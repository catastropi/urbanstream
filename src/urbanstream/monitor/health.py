"""소스 상태 계산 (순수 함수). 규칙은 ADR-0010 D8-8.

- 기대 간격 = interval ÷ 대상 수 (kma 는 override). 마지막 '성공' 이후 간격의 warn_factor 배 → degraded, down_factor 배 → down.
- 활성 시간대 밖 → idle. 데이터가 한 건도 없으면 → unknown (정상으로 치지 않는다).
- permanent_failure(인증 오류 등) → 기다리지 않고 down.
- 잠자기·정지 직후 유예(in_grace) 동안은 unknown — 소스가 아니라 수집기가 쉬었기 때문이다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .config import MonitorConfig, SourceCfg


@dataclass(frozen=True)
class Summary:
    last_ok_at: Optional[datetime] = None
    last_call_at: Optional[datetime] = None
    last_outcome: Optional[str] = None          # 마지막 최종 결과 (ok / failed / permanent_failure / skipped)
    last_reason: Optional[str] = None           # skipped 이유
    calls_1h: int = 0
    fail_1h: int = 0
    p95_latency_ms: Optional[int] = None
    budget_used: Optional[int] = None
    budget_limit: Optional[int] = None


@dataclass(frozen=True)
class StatusRow:
    source_system: str
    status: str
    reason: str
    last_call_at: Optional[datetime]
    last_ok_at: Optional[datetime]
    age_sec: Optional[float]
    expected_gap_sec: float
    calls_1h: int
    fail_1h: int
    p95_latency_ms: Optional[int]
    budget_used: Optional[int]
    budget_limit: Optional[int]


def source_status(scfg: SourceCfg, sm: Summary, now: datetime, cfg: MonitorConfig, *, in_grace: bool = False) -> StatusRow:
    gap = scfg.expected_gap_sec(cfg.expected_gap_override_sec.get(scfg.name))
    ref = sm.last_ok_at
    if scfg.in_window(now):                       # 창이 막 열렸을 때 어제의 마지막 성공을 기준으로 삼지 않는다
        ws = scfg.window_start(now)
        if ws <= now and (ref is None or ref < ws):
            ref = ws if sm.last_call_at is not None or sm.last_ok_at is not None else None
    age = max(0.0, (now - ref).total_seconds()) if ref else None

    def row(status: str, reason: str) -> StatusRow:
        return StatusRow(scfg.name, status, reason, sm.last_call_at, sm.last_ok_at, age, gap, sm.calls_1h, sm.fail_1h,
                         sm.p95_latency_ms, sm.budget_used, sm.budget_limit)

    if not scfg.in_window(now):
        return row("idle", "out_of_window")
    if in_grace:
        return row("unknown", "post_gap_grace")
    if sm.last_call_at is None and sm.last_ok_at is None:
        return row("unknown", "no_data")
    if sm.last_outcome == "permanent_failure":
        return row("down", "permanent_failure")
    if age is None:
        return row("unknown", "no_reference")
    warn, down = cfg.f("warn_factor", 2.5), cfg.f("down_factor", 5.0)
    if age >= down * gap:
        return row("down", f"no_success_{int(age)}s>={int(down * gap)}s")
    if age >= warn * gap:
        return row("degraded", f"no_success_{int(age)}s>={int(warn * gap)}s")
    if sm.calls_1h >= cfg.f("fail_rate_min_calls", 4) and sm.fail_1h / sm.calls_1h >= cfg.f("fail_rate_degraded", 0.5):
        return row("degraded", f"fail_rate_{sm.fail_1h}/{sm.calls_1h}")
    if sm.last_reason == "budget_exhausted":
        return row("degraded", "budget_exhausted")
    if sm.budget_limit and sm.budget_used is not None and sm.budget_used / sm.budget_limit >= cfg.f("budget_degraded_ratio", 0.8):
        return row("degraded", f"budget_{sm.budget_used}/{sm.budget_limit}")
    return row("ok", "")


ALERT_LEVEL = {"down": "error", "degraded": "warning"}
