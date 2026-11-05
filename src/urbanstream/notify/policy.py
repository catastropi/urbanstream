"""알림 정책 (config/alerts.yaml). 파일이 없거나 키가 빠져도 아래 기본값으로 동작한다. 숫자는 [가정] 시작값 (ADR-0010 D8-4)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

ROOT = Path(os.environ.get("URBANSTREAM_ROOT") or Path(__file__).resolve().parents[3])
LEVEL_ORDER = ("info", "warning", "error", "critical")


@dataclass(frozen=True)
class StallPolicy:
    min_slept_sec: float = 120.0
    blackout_min_sec: float = 600.0
    overlap_margin_sec: float = 600.0
    long_stall_sec: float = 21600.0
    group_window_sec: float = 1800.0
    post_gap_grace_sec: float = 600.0


@dataclass(frozen=True)
class Policy:
    push_levels: tuple[str, ...] = ("error", "critical")
    cooldown_sec: Mapping[str, float] = field(default_factory=lambda: {"critical": 1800.0, "error": 7200.0, "warning": 21600.0})
    escalate_warning_after_sec: float = 86400.0
    max_push_per_hour: int = 10
    pending_grace_sec: float = 60.0
    pending_flush_after_sec: float = 600.0
    digest_time_kst: str = "07:30"
    stall: StallPolicy = field(default_factory=StallPolicy)

    def cooldown(self, level: str) -> float:
        return float(self.cooldown_sec.get(level, self.cooldown_sec.get("error", 7200.0)))


def _num(d: Mapping[str, Any], k: str, default: float) -> float:
    v = d.get(k, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
        raise ValueError(f"alerts.yaml: {k} 는 0 이상의 숫자여야 합니다: {v!r}")
    return float(v)


def from_mapping(raw: Optional[Mapping[str, Any]]) -> Policy:
    raw = raw or {}
    base = Policy()
    push = tuple(raw.get("push_levels", base.push_levels))
    bad = [p for p in push if p not in LEVEL_ORDER]
    if bad:
        raise ValueError(f"alerts.yaml: push_levels 에 알 수 없는 수준 {bad}")
    cd = dict(base.cooldown_sec)
    for k, v in (raw.get("cooldown_sec") or {}).items():
        if k not in LEVEL_ORDER:
            raise ValueError(f"alerts.yaml: cooldown_sec 에 알 수 없는 수준 {k!r}")
        cd[k] = _num({k: v}, k, cd.get(k, 0))
    st_raw = raw.get("stall") or {}
    st_base = StallPolicy()
    stall = StallPolicy(**{k: _num(st_raw, k, getattr(st_base, k)) for k in st_base.__dataclass_fields__})
    digest = str(raw.get("digest_time_kst", base.digest_time_kst))
    hh, _, mm = digest.partition(":")
    if not (hh.isdigit() and mm.isdigit() and 0 <= int(hh) < 24 and 0 <= int(mm) < 60):
        raise ValueError(f"alerts.yaml: digest_time_kst 는 HH:MM 이어야 합니다: {digest!r}")
    return Policy(push_levels=push, cooldown_sec=cd,
                  escalate_warning_after_sec=_num(raw, "escalate_warning_after_sec", base.escalate_warning_after_sec),
                  max_push_per_hour=int(_num(raw, "max_push_per_hour", base.max_push_per_hour)),
                  pending_grace_sec=_num(raw, "pending_grace_sec", base.pending_grace_sec),
                  pending_flush_after_sec=_num(raw, "pending_flush_after_sec", base.pending_flush_after_sec),
                  digest_time_kst=digest, stall=stall)


def load_policy(path: Optional[Path] = None) -> Policy:
    p = Path(path or os.environ.get("ALERTS_CONFIG") or ROOT / "config" / "alerts.yaml")
    if not p.exists():
        return Policy()
    import yaml  # 지연 import
    return from_mapping(yaml.safe_load(p.read_text(encoding="utf-8")))
