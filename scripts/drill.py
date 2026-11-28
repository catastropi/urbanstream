"""장애 훈련 (STEP 3B, 설계안 7-4). 일부러 고장을 내고, 알림이 오는지·되돌린 뒤 회복하는지 보고, 결과를 기록한다.

언제 하나: 수집을 끊는 훈련(kafka-stop, docker-restart)은 STEP 2 무거운 측정이 끝난 뒤(12-16 이후 예상)에만 한다.
          수집 2주(~12-08)와 측정 기간에는 하지 않는다. 그때는 check 와 webhook-test(recovery_drill.sh)만 한다.

  python scripts/drill.py list                       훈련 목록 (최대 시간, 수집 공백 여부)
  python scripts/drill.py check                      실행 전 검사만 (STEP 2 의 measure_guard 와 같은 검사)
  python scripts/drill.py run monitor-stop           검사 → 무엇을 멈추고 언제 되돌리는지 보여 주고 yes → 실행 → 되돌림 → 회복 확인 → 기록
  python scripts/drill.py restore monitor-stop       되돌리는 명령만 (훈련이 중간에 끊겼을 때)
  python scripts/drill.py record --scenario R8 --start "11-24 05:00" --end "11-24 09:10" --result "실패 기록" --memo "..."
                                                     일부러 일으키지 않은 장애('자연')를 같은 표에 적는다

안전장치
  - 실행 전 검사는 measure_guard.gate_checks 그대로다(12개 healthy, 메모리 여유, CPU, gold-runner 비어 있음, 금지 시간대, 전원, 최근 공백).
  - 측정과 같은 잠금(data/measure/.lock)을 잡는다. 측정과 훈련은 절대 겹치지 않는다.
  - 훈련마다 최대 시간이 있다(config/recovery.yaml drills). 넘거나 Ctrl-C 를 누르거나 Enter 를 치면 바로 되돌린다.
  - 되돌린 뒤 5분 연속 모두 healthy 여야 '통과'. 15분 안에 안 되면 '되돌림 확인 실패'로 기록하고 diagnose 를 안내한다.
  - 기록: data/drills/drills.jsonl (원시) + docs/recovery-drills.md 표 한 줄 (저장소에 커밋, 결정 12).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hostlib as H  # noqa: E402

DRILL_DIR = H.ROOT / "data" / "drills"
DOC = H.ROOT / "docs" / "recovery-drills.md"
TABLE_MARK = "<!-- 기록 끝 -->"


@dataclass(frozen=True)
class Drill:
    name: str
    scenario: str
    what: str            # 무엇을 하는가 (사람이 읽는 문장)
    expect: str          # 기대 결과
    breaks: list         # 고장 내는 명령들 (빈 목록 = 사람이 직접 한다)
    restores: list       # 되돌리는 명령들
    manual: str = ""     # 사람이 직접 할 일 (docker-restart)


DRILLS: dict[str, Drill] = {d.name: d for d in [
    Drill("monitor-stop", "R12", "monitor 컨테이너를 멈춘다. 수집·처리는 계속된다",
          "10~11분 안에 host_watch 가 '감시자(monitor)가 멈췄습니다'(error), 시작 뒤 '해소'",
          [["docker", "stop", "urbanstream-monitor"]], [["docker", "start", "urbanstream-monitor"]]),
    Drill("spark-silver-stop", "R3", "spark-silver 를 멈춘다. Bronze 는 계속 쓰이고 Silver 만 늦어진다",
          "15~20분 뒤 monitor 가 Silver 적재 정체(error), 시작 뒤 따라잡고 해소. Silver 중복 0(다음 silver_reconcile)",
          [["docker", "stop", "urbanstream-spark-silver"]], [["docker", "start", "urbanstream-spark-silver"]]),
    Drill("spark-bronze-kill", "R2", "spark-bronze 를 docker kill 로 죽인다(배치 도중일 수 있다). 메시지는 Kafka 에 쌓인다",
          "재시작 뒤 offset 이 이어진다. 중복·유실 0 (bronze_verify)",
          [["docker", "kill", "urbanstream-spark-bronze"]], [["docker", "start", "urbanstream-spark-bronze"]]),
    Drill("kafka-stop", "R4", "Kafka 를 멈춘다. 수집 공백이 생긴다(멈춘 시간 + 약 30초)",
          "3분 안에 monitor critical 'Kafka 감사 토픽을 읽지 못합니다', 시작 뒤 회복. 잃은 건수는 수집기 audit 의 발행 실패",
          [["docker", "stop", "urbanstream-kafka"]], [["docker", "start", "urbanstream-kafka"]]),
    Drill("docker-restart", "R6", "Docker Desktop 을 끄고 다시 켠 뒤 resume 으로 모두 다시 시작한다. 수집 공백이 생긴다",
          "3분 안에 host_watch critical 'Docker 가 응답하지 않습니다', resume 으로 모두 healthy 까지 걸린 시간",
          [], [[sys.executable, str(H.ROOT / "scripts" / "recover.py"), "resume", "--in-drill"]],
          manual="1) 이 터미널은 그대로 두고 다른 터미널에서: osascript -e 'quit app \"Docker\"'\n"
                 "2) host_watch 알림(약 3분)을 기다린다\n3) open -a Docker → 고래 아이콘이 running 이 되면 이 터미널에서 Enter"),
    Drill("recover-rehearsal", "R1", "시험 토픽(test.recover.*)으로 OffsetOutOfRange 를 만들고 복구 도구로 고친다. 운영 토픽·체크포인트는 건드리지 않는다",
          "tests/spark/test_bronze_recover_kafka.py 통과",
          [["docker", "compose", "--profile", "core", "--profile", "streaming", "run", "--rm", "--no-deps", "-T",
            "-e", "URBANSTREAM_TEST_KAFKA_BOOTSTRAP=kafka:19092", "-v", f"{H.ROOT / 'tests'}:/app/tests:ro",
            "--entrypoint", "urbanstream-pytest", "spark-bronze", "-q", "/app/tests/spark/test_bronze_recover_kafka.py"]], []),
]}
GAP_DRILLS = ("kafka-stop", "docker-restart")


# ------------------------------------------------------------------ 기록

def gap_minutes_today(path: Path, today: str) -> float:
    total = 0.0
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("date") == today and r.get("kind") == "훈련":
                total += float(r.get("gap_min") or 0)
    return total


def doc_row(r: dict) -> str:
    cells = [r.get("date", ""), r.get("kind", ""), f"{r.get('scenario', '')} {r.get('name', '')}".strip(),
             f"{r.get('start', '')}~{r.get('end', '')}", r.get("alert_at") or "없음", r.get("recover") or "-",
             str(r.get("gap") or "0"), str(r.get("lost") if r.get("lost") is not None else "-"), r.get("result", ""), r.get("memo", "")]
    return "| " + " | ".join(str(c).replace("|", "/").replace("\n", " ") for c in cells) + " |"


def append_record(r: dict, *, drill_dir: Optional[Path] = None, doc: Optional[Path] = None) -> None:
    drill_dir, doc = drill_dir or DRILL_DIR, doc or DOC
    drill_dir.mkdir(parents=True, exist_ok=True)
    with open(drill_dir / "drills.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
    if doc.exists():
        text = doc.read_text(encoding="utf-8")
        if TABLE_MARK in text:
            text = text.replace(TABLE_MARK, doc_row(r) + "\n" + TABLE_MARK, 1)
            doc.write_text(text, encoding="utf-8")
            return
    print("docs/recovery-drills.md 에 표 끝 표시가 없어 원시 기록(data/drills/drills.jsonl)에만 남겼다. 표에 붙일 줄:\n" + doc_row(r))


# ------------------------------------------------------------------ 실행

def gate(expected_sec: float) -> tuple[bool, list]:
    import measure_guard as G
    cfg = G.load_config()
    checks = G.run_check(cfg, expected_sec, 0)
    G.print_checks(checks)
    return all(c.ok for c in checks), checks


def run_cmds(cmds: list, runner=subprocess.run, timeout: float = 180) -> list[tuple[list, Optional[int], str]]:
    out = []
    for c in cmds:
        rc, o, e = H.run(c, timeout=timeout, runner=runner)
        out.append((c, rc, (e or o).strip()[-300:]))
        print(f"  $ {' '.join(c[:6])}{' …' if len(c) > 6 else ''} → {'성공' if rc == 0 else f'실패({rc}) {(e or o).strip()[-200:]}'}", flush=True)
    return out


def wait_for_restore(max_sec: float, *, wait_input: Callable[[float], bool], clock_fn=time.monotonic) -> str:
    """Enter·Ctrl-C·최대 시간 중 먼저 오는 것. 반환 = 이유."""
    t0 = clock_fn()
    try:
        while clock_fn() - t0 < max_sec:
            if wait_input(min(5.0, max_sec - (clock_fn() - t0))):
                return "Enter"
        return "최대 시간"
    except KeyboardInterrupt:
        return "Ctrl-C"


def stdin_ready(timeout: float) -> bool:
    import select
    r, _, _ = select.select([sys.stdin], [], [], max(0.0, timeout))
    if r:
        sys.stdin.readline()
        return True
    return False


def settle(cfg: dict, *, runner=subprocess.run, sleep=time.sleep, clock_fn=time.monotonic,
           expected: Optional[list[str]] = None) -> tuple[bool, float]:
    """되돌린 뒤 모두 healthy 가 settle_healthy_sec 동안 이어지는지. (통과, 되돌림부터 처음 모두 healthy 까지 초)."""
    d = cfg["drills"]
    exp = expected or H.expected_services()
    t0 = clock_fn()
    first_ok: Optional[float] = None
    while clock_fn() - t0 < d["settle_max_sec"]:
        rows = H.containers(10, runner=runner) or []
        svc = H.by_service(rows)
        if all(H.healthy(svc.get(s)) for s in exp):
            first_ok = clock_fn() if first_ok is None else first_ok
            if clock_fn() - first_ok >= d["settle_healthy_sec"]:
                return True, round(first_ok - t0, 1)
        else:
            first_ok = None
        sleep(10)
    return False, round(clock_fn() - t0, 1)


def run_drill(name: str, cfg: dict, *, yes: bool = False, runner=subprocess.run, ask: Callable[[str], str] = input,
              wait_input: Callable[[float], bool] = stdin_ready, gate_fn: Callable[[float], tuple] = gate,
              sleep=time.sleep, clock_fn=time.monotonic, expected: Optional[list[str]] = None,
              drill_dir: Optional[Path] = None, doc: Optional[Path] = None, lock_path: Optional[Path] = None) -> int:
    import measure_guard as G
    if name not in DRILLS:
        print(f"모르는 훈련: {name} (가능: {', '.join(DRILLS)})")
        return 2
    dr, spec = DRILLS[name], cfg["drills"][name]
    max_sec = float(spec["max_min"]) * 60
    today = H.now_kst().strftime("%Y-%m-%d")
    drill_dir = drill_dir or DRILL_DIR
    if spec.get("gap"):
        used = gap_minutes_today(drill_dir / "drills.jsonl", today)
        if used + spec["max_min"] > cfg["drills"]["gap_budget_min"]:
            print(f"오늘 훈련 공백이 이미 {used:.0f}분이다. 이 훈련(최대 {spec['max_min']}분)을 더하면 하루 상한 "
                  f"{cfg['drills']['gap_budget_min']}분을 넘는다. 다른 날 한다.")
            return 2
    print(f"훈련 {name} ({dr.scenario}): {dr.what}\n기대: {dr.expect}\n최대 {spec['max_min']}분 뒤 자동으로 되돌린다. "
          f"수집 공백: {'있음' if spec.get('gap') else '없음'}\n")
    print("실행 전 검사:")
    ok, _ = gate_fn(max_sec)
    if not ok:
        print("\n검사를 통과하지 못해 시작하지 않는다.")
        return 1
    if not yes and ask("\n시작하려면 yes: ").strip() != "yes":
        print("취소했다.")
        return 0
    lp = lock_path or H.measure_lock_path()
    with G.Lock(lp, note=f"drill:{name}"):
        rows0 = H.containers(10, runner=runner) or []
        restarts0 = {r["service"]: r["restarts"] for r in rows0}
        start = time.time()
        print(f"\n[{H.now_kst():%H:%M:%S}] 시작", flush=True)
        broke = run_cmds(dr.breaks, runner=runner, timeout=max_sec if dr.name == "recover-rehearsal" else 180)
        broke_ok = all(rc == 0 for _, rc, _ in broke)
        if dr.manual:
            print(dr.manual, flush=True)
        if dr.breaks and not broke_ok:
            reason = "고장 내기 실패"
        elif dr.name == "recover-rehearsal":
            reason = "시험 끝"
        else:
            print(f"알림이 오는지 본다. 되돌리려면 Enter (최대 {spec['max_min']}분 뒤 자동).", flush=True)
            reason = wait_for_restore(max_sec, wait_input=wait_input, clock_fn=clock_fn)
        restored_at = time.time()
        print(f"[{H.now_kst():%H:%M:%S}] 되돌림 ({reason})", flush=True)
        run_cmds(dr.restores, runner=runner, timeout=600)
        passed, recover_sec = settle(cfg, runner=runner, sleep=sleep, clock_fn=clock_fn, expected=expected)
        end = time.time()
        rows1 = H.containers(10, runner=runner) or []
        bumped = {r["service"]: r["restarts"] - restarts0.get(r["service"], r["restarts"]) for r in rows1
                  if r["restarts"] > restarts0.get(r["service"], r["restarts"])}
    alert = "" if yes else ask("알림이 도착한 시각 (예: 14:56, 안 왔으면 Enter): ").strip()
    memo = "" if yes else ask("메모 (없으면 Enter): ").strip()
    if dr.name == "recover-rehearsal":
        result = "통과" if broke_ok and passed else "실패"
    else:
        result = "통과" if broke_ok and passed else ("되돌림 확인 실패" if broke_ok else "고장 내기 실패")
    rec = {"date": today, "kind": "훈련", "scenario": dr.scenario, "name": name,
           "start": datetime.fromtimestamp(start, H.KST).strftime("%H:%M"), "end": datetime.fromtimestamp(end, H.KST).strftime("%H:%M"),
           "alert_at": alert_cell(alert, start),
           "recover": f"{recover_sec / 60:.1f}분", "gap": f"{(restored_at - start) / 60 + 0.5:.0f}분" if spec.get("gap") else "0",
           "gap_min": round((restored_at - start) / 60 + 0.5, 1) if spec.get("gap") else 0, "lost": None,
           "result": result, "memo": memo or reason, "restarts": bumped, "ts": round(start, 1)}
    append_record(rec, drill_dir=drill_dir, doc=doc)
    print(f"\n결과: {result}. 기록: docs/recovery-drills.md, data/drills/drills.jsonl")
    if not passed:
        print("다음: python scripts/recover.py diagnose")
    return 0 if result == "통과" else 1


def alert_cell(alert: str, start: float) -> str:
    """'14:56' → '14:56 (11분)'. 시작 뒤 몇 분 만에 왔는지를 같이 적는다(완료 판정 칸)."""
    if not alert:
        return ""
    t = _hm_ts(alert, start)
    return alert if t is None else f"{alert} ({max(0, int((t - start) // 60))}분)"


def _hm_ts(hm: str, ref: float) -> Optional[float]:
    try:
        h, m = (int(x) for x in hm.split(":"))
    except ValueError:
        return None
    d = datetime.fromtimestamp(ref, H.KST).replace(hour=h, minute=m, second=0, microsecond=0)
    return d.timestamp()


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(H.CONFIG))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("check")
    r = sub.add_parser("run")
    r.add_argument("name", choices=list(DRILLS))
    r.add_argument("--yes", action="store_true", help="확인 질문 없이 (시험용)")
    rs = sub.add_parser("restore")
    rs.add_argument("name", choices=list(DRILLS))
    rc = sub.add_parser("record", help="자연 발생 장애 기록")
    rc.add_argument("--scenario", required=True)
    rc.add_argument("--date", default=H.now_kst().strftime("%Y-%m-%d"))
    rc.add_argument("--start", required=True)
    rc.add_argument("--end", default="")
    rc.add_argument("--alert-at", default="")
    rc.add_argument("--recover", default="-")
    rc.add_argument("--gap", default="-")
    rc.add_argument("--lost", default=None)
    rc.add_argument("--result", required=True)
    rc.add_argument("--memo", default="")
    a = ap.parse_args(argv)
    cfg = H.load_config(a.config)
    if a.cmd == "list":
        for n, d in DRILLS.items():
            s = cfg["drills"][n]
            print(f"{n:<18} {d.scenario:<4} 최대 {s['max_min']:>2}분  수집 공백 {'있음' if s['gap'] else '없음'}  {d.what}")
        return 0
    if a.cmd == "check":
        ok, _ = gate(15 * 60)
        print("결과: " + ("훈련을 시작해도 됨" if ok else "시작하지 않음"))
        return 0 if ok else 1
    if a.cmd == "restore":
        res = run_cmds(DRILLS[a.name].restores)
        return 0 if all(rc == 0 for _, rc, _ in res) else 1
    if a.cmd == "record":
        append_record({"date": a.date, "kind": "자연", "scenario": a.scenario, "name": "", "start": a.start, "end": a.end,
                       "alert_at": a.alert_at, "recover": a.recover, "gap": a.gap, "lost": a.lost, "result": a.result, "memo": a.memo,
                       "ts": round(time.time(), 1)})
        print("기록했다: docs/recovery-drills.md")
        return 0
    return run_drill(a.name, cfg, yes=a.yes)


if __name__ == "__main__":
    sys.exit(main())
