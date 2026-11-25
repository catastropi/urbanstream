"""STEP 2 측정 안전장치: 설정 읽기, 경로 가드, 금지 시간대, 실행 전 검사, 실행 중 감시 판정 (설계안 6장).

이 파일의 판정 함수는 순수하다(입력 = measure_sample.collect_state 가 만든 상태 dict). 그래서 Docker 없이 시험한다
(tests/unit/test_measure_guard.py). Docker·DB 를 실제로 읽는 것은 아래 `check` 명령뿐이다.

사용 (프로젝트 루트, 호스트 Python)
  python scripts/measure_guard.py check                       능동 측정을 시작해도 되는지 (모두 PASS 면 종료 코드 0)
  python scripts/measure_guard.py check --expected-sec 600 --expected-gib 2
  python scripts/measure_guard.py window                      지금이 금지 시간대인지, 다음 창은 언제인지
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9), "KST")
GIB = 1024 ** 3
CONFIG = ROOT / "config" / "measure.yaml"


class GuardError(RuntimeError):
    """측정 도구가 시작하면 안 되는 상태 (운영 경로·운영 DB·잘못된 날짜)."""


# ------------------------------------------------------------------ 설정

def load_config(path: Path | str = CONFIG) -> dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if cfg.get("version") != 1:
        raise ValueError("config/measure.yaml version 은 1 이어야 합니다")
    for k in ("paths", "synthetic", "gate", "watch", "forbidden", "repeat", "runner", "decide", "sample"):
        if k not in cfg:
            raise ValueError(f"config/measure.yaml 에 {k} 절이 없습니다")
    date.fromisoformat(cfg["collection_start"])
    if date.fromisoformat(cfg["synthetic"]["first_date"]) >= date.fromisoformat(cfg["synthetic"]["real_data_start"]):
        raise ValueError("synthetic.first_date 는 real_data_start 보다 앞이어야 합니다")
    for a, b in cfg["forbidden"]["hourly"]:
        if not (0 <= int(a) < int(b) <= 60):
            raise ValueError(f"forbidden.hourly 범위가 잘못됨: {a}~{b}")
    for a, b in cfg["forbidden"]["daily"]:
        _hm(a), _hm(b)
    return cfg


def heavy_start(cfg: dict) -> date:
    """무거운 측정(M5~M7)을 시작할 수 있는 첫 날."""
    return date.fromisoformat(cfg["collection_start"]) + timedelta(days=int(cfg["heavy_after_days"]))


# ------------------------------------------------------------------ 경로·DB 가드

def _resolve(p: Path | str, root: Path) -> Path:
    p = Path(p)
    return (p if p.is_absolute() else root / p).resolve()


def inside(base: Path | str, path: Path | str) -> bool:
    b, p = Path(base).resolve(), Path(path).resolve()
    return p == b or b in p.parents


def ensure_measure_path(path: Path | str, cfg: dict, root: Path = ROOT) -> Path:
    """출력 경로 가드: measure_root 아래가 아니거나 운영 경로 아래면 GuardError. 반환 = 절대 경로."""
    p = _resolve(path, root)
    mroot = _resolve(cfg["paths"]["measure_root"], root)
    for prot in cfg["paths"]["protected"]:
        if inside(_resolve(prot, root), p):
            raise GuardError(f"운영 경로에는 쓰지 않습니다: {p}")
    if not inside(mroot, p) or p == mroot:
        raise GuardError(f"측정 출력은 {mroot} 아래여야 합니다: {p}")
    return p


def ensure_synthetic_date(d: date, cfg: dict) -> date:
    first = date.fromisoformat(cfg["synthetic"]["first_date"])
    real = date.fromisoformat(cfg["synthetic"]["real_data_start"])
    if d < first or d >= real:
        raise GuardError(f"합성 날짜는 {first} 이상 {real} 미만이어야 합니다 (실제 데이터와 겹치지 않게): {d}")
    return d


def ensure_measure_db(dbname: str, cfg: dict, production: Optional[str] = None) -> str:
    """합성 Gold 를 넣을 DB 이름 가드. 운영 DB(POSTGRES_DB, 기본 urbanstream)면 거부한다."""
    prod = production or os.environ.get("POSTGRES_DB") or "urbanstream"
    want = cfg["synthetic"]["measure_db"]
    if dbname in (prod, "urbanstream", "airflow", "postgres", "template0", "template1"):
        raise GuardError(f"운영·시스템 DB 에는 측정 데이터를 넣지 않습니다: {dbname}")
    if dbname != want:
        raise GuardError(f"측정 DB 이름은 {want} 이어야 합니다: {dbname}")
    return dbname


# ------------------------------------------------------------------ 금지 시간대

def _hm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def forbidden_reason(t: datetime, cfg: dict) -> Optional[str]:
    """t(시각, tz 있음)가 금지 시간대면 이유 문자열, 아니면 None."""
    k = t.astimezone(KST)
    for a, b in cfg["forbidden"]["hourly"]:
        if int(a) <= k.minute < int(b):
            return f"매시 :{int(a):02d}~:{int(b):02d} (정시 DAG)"
    tt = k.time()
    for a, b in cfg["forbidden"]["daily"]:
        if _hm(a) <= tt < _hm(b):
            return f"{a}~{b} (새벽 DAG)"
    return None


def next_allowed(t: datetime, cfg: dict, step_sec: int = 60) -> datetime:
    """t 이후(포함) 처음으로 금지 시간대가 아닌 분."""
    k = t.astimezone(KST).replace(second=0, microsecond=0)
    if k < t.astimezone(KST):
        k += timedelta(minutes=1)
    for _ in range(24 * 60 + 1):
        if forbidden_reason(k, cfg) is None:
            return k
        k += timedelta(seconds=step_sec)
    raise RuntimeError("하루 종일 금지 시간대입니다. config/measure.yaml forbidden 을 확인하세요")


def free_until(t: datetime, cfg: dict) -> datetime:
    """t 가 허용 시간이면, 다음 금지 시간대가 시작하는 시각."""
    k = t.astimezone(KST).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(24 * 60 + 1):
        if forbidden_reason(k, cfg) is not None:
            return k
        k += timedelta(minutes=1)
    return k


def window_check(t: datetime, expected_sec: float, cfg: dict) -> tuple[bool, str]:
    r = forbidden_reason(t, cfg)
    if r:
        return False, f"금지 시간대 {r}. 다음 창 {next_allowed(t, cfg):%H:%M}"
    if expected_sec > float(cfg["forbidden"]["max_run_sec"]):
        return False, f"예상 소요 {int(expected_sec)}초 > 한 회 상한 {cfg['forbidden']['max_run_sec']}초 (나누어 잰다)"
    end = t + timedelta(seconds=expected_sec)
    stop = free_until(t, cfg)
    if end > stop:
        return False, f"예상 끝 {end.astimezone(KST):%H:%M} 이 다음 금지 시간대 {stop:%H:%M} 를 넘는다"
    return True, f"허용 창 (다음 금지 {stop:%H:%M})"


# ------------------------------------------------------------------ 상태 요약 (순수)

def mem_used(state: dict) -> int:
    return sum(int(c.get("mem_bytes") or 0) for c in state.get("containers", []) if c.get("state") == "running")


def mem_free(state: dict) -> Optional[int]:
    total = (state.get("vm") or {}).get("mem_total")
    return None if not total else int(total) - mem_used(state)


def cpu_sum(state: dict) -> float:
    return round(sum(float(c.get("cpu_pct") or 0.0) for c in state.get("containers", []) if c.get("state") == "running"), 1)


def by_service(state: dict) -> dict[str, dict]:
    return {c.get("service") or c.get("name"): c for c in state.get("containers", [])}


def container_ok(c: Optional[dict]) -> bool:
    """running 이고 healthcheck 가 없거나(health 빈 값) healthy."""
    return c is not None and c.get("state") == "running" and (c.get("health") or "") in ("", "healthy")


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


def _gib(n: Optional[float]) -> str:
    return "?" if n is None else f"{n / GIB:.2f} GiB"


def gate_checks(state: dict, cfg: dict, *, now: datetime, expected_sec: float = 0, expected_bytes: float = 0,
                cpu_avg: Optional[float] = None, lock_exists: bool = False) -> list[Check]:
    """실행 전 검사 (설계안 6-2). state 에는 power·db·runner_queue 가 있으면 함께 본다. 없는 값은 FAIL 로 본다(모르면 시작하지 않는다)."""
    g = cfg["gate"]
    out: list[Check] = []
    svc = by_service(state)
    bad = [s for s in g["expected_containers"] if not container_ok(svc.get(s))]
    probs = ", ".join("{}({}/{})".format(s, (svc.get(s) or {}).get("state", "없음"), (svc.get(s) or {}).get("health") or "-") for s in bad)
    out.append(Check("컨테이너", not bad, f"{len(g['expected_containers']) - len(bad)}/{len(g['expected_containers'])} 정상"
                     + (f", 문제: {probs}" if bad else "")))
    free = mem_free(state)
    out.append(Check("메모리 여유", free is not None and free >= g["min_free_mem_gib"] * GIB,
                     f"VM {_gib((state.get('vm') or {}).get('mem_total'))} - 사용 {_gib(mem_used(state))} = {_gib(free)} (기준 {g['min_free_mem_gib']} GiB)"))
    cpu = cpu_sum(state) if cpu_avg is None else cpu_avg
    out.append(Check("CPU", cpu < g["max_cpu_pct_1m"], f"컨테이너 합 평균 {cpu:.0f}% (기준 < {g['max_cpu_pct_1m']}%)"))
    q = state.get("runner_queue")
    db = state.get("db") or {}
    busy = []
    if q is None:
        busy.append("실행기 요청 폴더를 못 읽음")
    elif q.get("pending") or q.get("running"):
        busy.append(f"gold-runner 대기 {q.get('pending', 0)}·실행 {q.get('running', 0)}")
    if db.get("reprocess_running") is None:
        busy.append("ops.reprocess_run 을 못 읽음")
    elif db["reprocess_running"]:
        busy.append(f"진행 중 재처리 {db['reprocess_running']}건")
    out.append(Check("다른 무거운 작업", not busy, ", ".join(busy) or "없음"))
    ok, why = window_check(now, expected_sec, cfg)
    out.append(Check("시간대", ok, why))
    disk = state.get("disk_free")
    need = max(g["min_disk_free_gib"] * GIB, expected_bytes * g["disk_output_factor"])
    out.append(Check("디스크", disk is not None and disk >= need, f"여유 {_gib(disk)} (기준 {_gib(need)})"))
    p = state.get("power") or {}
    pw_bad = []
    if g.get("require_ac_power") and p.get("ac") is not True:
        pw_bad.append("전원 연결 아님" if p.get("ac") is False else "전원 상태 모름")
    if g.get("require_caffeinate") and p.get("caffeinate") is not True:
        pw_bad.append("caffeinate 없음")
    out.append(Check("전원", not pw_bad, ", ".join(pw_bad) or "전원 연결, caffeinate 실행 중"))
    out.append(Check("중복 실행", not lock_exists, "잠금 파일 있음 (다른 측정이 도는 중이거나 비정상 종료)" if lock_exists else "잠금 없음"))
    gaps = db.get("new_gaps")
    out.append(Check("수집", gaps == 0, "ops.data_gap 을 못 읽음" if gaps is None
                     else f"최근 {g['data_gap_lookback_min']}분 새 공백 {gaps}건"))
    return out


# ------------------------------------------------------------------ 실행 중 감시 (순수)

@dataclass
class Watchdog:
    """10초 표본을 차례로 넣으면 중단 이유를 돌려준다(None = 계속). targets = 측정 대상 컨테이너(서비스 이름)."""
    cfg: dict
    targets: Iterable[str] = ()
    _first: Optional[dict] = None
    _last_ts: Optional[float] = None
    _hot_since: Optional[float] = None
    reason: Optional[str] = None
    max_cpu: float = 0.0
    min_free: Optional[int] = None
    samples: int = 0
    target_peak: dict = field(default_factory=dict)

    def feed(self, state: dict) -> Optional[str]:
        w = self.cfg["watch"]
        ts = float(state["ts"])
        self.samples += 1
        svc = by_service(state)
        for t in self.targets:
            m = int((svc.get(t) or {}).get("mem_bytes") or 0)
            self.target_peak[t] = max(self.target_peak.get(t, 0), m)
        if self._first is None:
            self._first = svc
        cpu = cpu_sum(state)
        self.max_cpu = max(self.max_cpu, cpu)
        free = mem_free(state)
        if free is not None:
            self.min_free = free if self.min_free is None else min(self.min_free, free)
        reason = None
        if self._last_ts is not None and ts - self._last_ts > w["max_sample_gap_sec"]:
            reason = f"감시 표본 사이가 {int(ts - self._last_ts)}초 비었다 (Mac 잠듦으로 본다)"
        self._last_ts = ts
        targets = set(self.targets)
        if reason is None:
            for name, c in svc.items():
                if name in targets or name not in self._first:
                    continue
                if c.get("state") == "running" and c.get("health") == "unhealthy":
                    reason = f"{name} 가 unhealthy"
                    break
                if self._first[name].get("state") == "running" and c.get("state") != "running":
                    reason = f"{name} 가 멈춤 ({c.get('state')})"
                    break
        if reason is None:
            for name in w["restart_watch"]:
                a, b = (self._first.get(name) or {}).get("restarts"), (svc.get(name) or {}).get("restarts")
                if a is not None and b is not None and b > a:
                    reason = f"{name} 재시작 {a}→{b}"
                    break
        if reason is None and free is not None and free < w["min_free_mem_gib"] * GIB:
            reason = f"VM 메모리 여유 {_gib(free)} < {w['min_free_mem_gib']} GiB"
        if cpu > w["max_cpu_pct"]:
            self._hot_since = ts if self._hot_since is None else self._hot_since
            if reason is None and ts - self._hot_since >= w["max_cpu_sustain_sec"]:
                reason = f"컨테이너 CPU 합 {cpu:.0f}% > {w['max_cpu_pct']}% 가 {int(ts - self._hot_since)}초 이어짐"
        else:
            self._hot_since = None
        if reason and not self.reason:
            self.reason = reason
        return reason

    def summary(self) -> dict:
        return {"samples": self.samples, "max_cpu_pct": self.max_cpu, "min_free_mem_bytes": self.min_free,
                "target_peak_mem_bytes": dict(self.target_peak), "abort_reason": self.reason}


# ------------------------------------------------------------------ 잠금

class Lock:
    """data/measure/.lock. 이미 있으면 GuardError. 측정 하나가 끝나면 지운다."""

    def __init__(self, path: Path | str, note: str = ""):
        self.path, self.note = Path(path), note

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise GuardError(f"잠금 파일이 있습니다: {self.path} (다른 측정이 도는지 확인하고, 아니면 지운다)") from None
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()} {datetime.now(KST).isoformat(timespec='seconds')} {self.note}\n")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.path.unlink(missing_ok=True)


# ------------------------------------------------------------------ CLI

def print_checks(checks: list[Check]) -> None:
    for c in checks:
        print(f"{'PASS' if c.ok else 'FAIL'}  {c.name:<12} {c.detail}")


def run_check(cfg: dict, expected_sec: float, expected_bytes: float) -> list[Check]:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import measure_sample as S
    state, cpu_avg = S.collect_gate_state(cfg, ROOT)
    lock = _resolve(cfg["paths"]["lock"], ROOT).exists()
    return gate_checks(state, cfg, now=datetime.now(KST), expected_sec=expected_sec, expected_bytes=expected_bytes,
                       cpu_avg=cpu_avg, lock_exists=lock)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(CONFIG))
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="실행 전 검사")
    c.add_argument("--expected-sec", type=float, default=600)
    c.add_argument("--expected-gib", type=float, default=0.0)
    sub.add_parser("window", help="금지 시간대 확인")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    if a.cmd == "window":
        now = datetime.now(KST)
        r = forbidden_reason(now, cfg)
        print(f"지금 {now:%H:%M} KST: " + (f"금지 ({r}). 다음 창 {next_allowed(now, cfg):%H:%M}" if r
                                          else f"허용. 다음 금지 {free_until(now, cfg):%H:%M}"))
        print(f"무거운 측정 시작 가능일: {heavy_start(cfg)} (수집 시작 {cfg['collection_start']} + {cfg['heavy_after_days']}일)")
        return 0
    checks = run_check(cfg, a.expected_sec, a.expected_gib * GIB)
    print_checks(checks)
    ok = all(x.ok for x in checks)
    print("결과: " + ("시작해도 됨" if ok else "시작하지 않음"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
