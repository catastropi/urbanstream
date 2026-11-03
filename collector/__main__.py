"""CLI:  python -m collector {once|run|status}

  once   --source <이름> [--dry-run]   대상을 1사이클씩만 호출하고 결과를 요약한다 (처음 시험할 때)
  run    [--only a,b] [--dry-run] [--sequential]
                                        스케줄에 따라 계속 수집한다 (소스별 스레드, Ctrl+C 로 종료, 현재 사이클을 마치고 flush)
  status                                오늘 호출 예산 사용량

--dry-run 은 Kafka 에 보내지 않고 메모리에 모아 요약만 보여준다 (원본 저장(data/raw)은 그대로 한다).
키는 .env 에서 읽는다. 출력·로그에는 키 값이 나오지 않는다.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))  # python -m collector 를 pip install 없이 실행하기 위해

from urbanstream.contracts.common import KST  # noqa: E402

from .config import ConfigError, load_config  # noqa: E402
from .fetch import HttpFetcher, Redactor, normalize_datago_key  # noqa: E402
from .publisher import KafkaPublisher, MemoryPublisher  # noqa: E402
from .rawstore import RawStore  # noqa: E402
from .requests_spec import SEOUL_KEY_SOURCES  # noqa: E402
from .resilience import DailyBudget  # noqa: E402
from .runner import Collector, Scheduler, run_forever, run_forever_parallel  # noqa: E402


def _clock() -> datetime:
    return datetime.now(KST)


def _load_keys(env: dict) -> dict:
    return {"seoul": env.get("SEOUL_API_KEY", "").strip(),
            "datago": normalize_datago_key(env.get("DATA_GO_KR_KEY", ""))}


def _check_keys(sources, keys: dict) -> None:
    missing = []
    for s in sources:
        need = ("seoul", "SEOUL_API_KEY") if s.name in SEOUL_KEY_SOURCES else ("datago", "DATA_GO_KR_KEY")
        if not keys[need[0]] and need[1] not in missing:
            missing.append(need[1])
    if missing:
        raise ConfigError(f".env 에 {', '.join(missing)} 가 없습니다")


def build(args, dry_run: bool):
    load_dotenv(ROOT / ".env", encoding="utf-8-sig")
    env = dict(os.environ)
    cfg = load_config(env=env)
    only = [s.strip() for s in (getattr(args, "only", "") or getattr(args, "source", "") or "").split(",") if s.strip()]
    sources = cfg.enabled_sources(only or None)
    if not sources:
        raise ConfigError("실행할 소스가 없습니다 (enabled 와 --only 를 확인하세요)")
    keys = _load_keys(env)
    _check_keys(sources, keys)
    redact = Redactor([keys["seoul"], keys["datago"], env.get("DATA_GO_KR_KEY", "")])
    budget = DailyBudget(ROOT / "data" / "collector" / "budget.json",
                         {s.name: s.daily_budget for s in cfg.sources.values()}, _clock)
    publisher = MemoryPublisher() if dry_run else KafkaPublisher(env.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    collector = Collector(cfg, HttpFetcher(cfg.timeout, redact, total_timeout=cfg.total_timeout_sec), publisher, RawStore(ROOT / "data" / "raw", ROOT),
                          budget, keys, redact, _clock, time.sleep)
    return cfg, sources, collector, publisher, budget


def cmd_once(args) -> int:
    cfg, sources, collector, publisher, budget = build(args, args.dry_run)
    bad = 0
    for src in sources:
        for target in src.targets:
            rep = collector.run_target(target)
            print(f"{target.key:<40} {rep.outcome:<18} attempts={rep.attempts} records={rep.n_records} "
                  f"rejects={rep.n_rejects} published={rep.published} {rep.reason or ''}")
            bad += rep.outcome not in ("ok",)
    publisher.flush(10.0)
    if args.dry_run:
        counts = Counter(m.topic for m in publisher.messages)
        print("\n[dry-run] 발행했을 메시지 수:", dict(counts) or "없음")
    else:
        print(f"\nKafka 전달 성공 {publisher.delivered}건 / 실패 {publisher.errors}건")
    return 1 if bad or getattr(publisher, "errors", 0) else 0


def cmd_run(args) -> int:
    cfg, sources, collector, publisher, budget = build(args, args.dry_run)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    sched = Scheduler(sources, _clock())
    logging.getLogger("collector").info(
        "수집 시작: %s (종료: Ctrl+C)", ", ".join(f"{s.name}×{len(s.targets)}/{s.interval_sec}s" for s in sources))
    (run_forever if args.sequential else run_forever_parallel)(collector, sched, stop, _clock)
    print("종료했습니다. 오늘 호출 예산:", {k: f"{v['used']}/{v['limit']}" for k, v in budget.report().items()})
    return 0


def cmd_status(args) -> int:
    load_dotenv(ROOT / ".env", encoding="utf-8-sig")
    cfg = load_config()
    budget = DailyBudget(ROOT / "data" / "collector" / "budget.json",
                         {s.name: s.daily_budget for s in cfg.sources.values()}, _clock)
    print(f"오늘({_clock():%Y-%m-%d}) 호출 예산 사용량")
    for name, v in budget.report().items():
        s = cfg.sources[name]
        print(f"  {name:<16} {v['used']:>5} / {v['limit']:<6} ({v['pct']}%)  계획 {s.planned_calls_per_day}회/일, "
              f"{s.interval_sec}s 간격, 한도 근거={s.budget_basis}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m collector", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("once", help="대상별 1사이클")
    p.add_argument("--source", default="", help="쉼표로 구분한 소스 이름 (기본: 전체)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(fn=cmd_once)
    p = sub.add_parser("run", help="계속 수집")
    p.add_argument("--only", default="")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sequential", action="store_true", help="소스별 스레드 대신 한 줄로 순서대로 호출 (문제 조사용)")
    p.set_defaults(fn=cmd_run)
    sub.add_parser("status", help="오늘 호출 예산").set_defaults(fn=cmd_status)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    try:
        return args.fn(args)
    except ConfigError as e:
        print(f"설정 오류: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
