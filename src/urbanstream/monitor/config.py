"""config/monitor.yaml 과 config/sources.yaml 을 읽는다 (소스 설정은 수집기 코드를 import 하지 않고 직접 읽는다 — 이미지에 collector 가 없다)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ROOT = Path(os.environ.get("URBANSTREAM_ROOT") or Path(__file__).resolve().parents[3])


@dataclass(frozen=True)
class SourceCfg:
    name: str
    interval_sec: float
    n_targets: int
    window: tuple[int, int]                  # 분 단위 [시작, 끝) — 끝 1440 = 24:00
    daily_budget: int

    def expected_gap_sec(self, override: Optional[float] = None) -> float:
        # 대상(링크 등)은 처음에 몇 초씩만 어긋나게 시작해 같은 간격으로 거의 동시에 호출된다(collector/runner.py Scheduler stagger).
        # 그래서 호출 사이의 빈 시간은 interval ÷ 대상 수가 아니라 interval 이다 (Mac 첫 실행: ÷4 로 계산해 seoul_traffic degraded 가 반복됐다, D-140).
        return float(override) if override else self.interval_sec

    def in_window(self, now: datetime) -> bool:
        k = now.astimezone(KST)
        m = k.hour * 60 + k.minute
        return self.window[0] <= m < self.window[1]

    def window_start(self, now: datetime) -> datetime:
        k = now.astimezone(KST)
        return datetime.combine(k.date(), dtime(self.window[0] // 60 % 24, self.window[0] % 60), tzinfo=KST)


@dataclass(frozen=True)
class MonitorConfig:
    tick_sec: float = 60.0
    kafka: Mapping[str, Any] = field(default_factory=dict)
    freshness: Mapping[str, Any] = field(default_factory=dict)
    expected_gap_override_sec: Mapping[str, float] = field(default_factory=dict)
    layers: Mapping[str, Any] = field(default_factory=dict)
    sources: Mapping[str, SourceCfg] = field(default_factory=dict)

    def f(self, key: str, default: float) -> float:
        return float(self.freshness.get(key, default))


def _hm(s: str) -> int:
    h, m = str(s).split(":")
    return int(h) * 60 + int(m)


def _n_targets(raw: Mapping[str, Any]) -> int:
    for k in ("link_ids", "route_ids", "grids"):
        if raw.get(k):
            return len(raw[k])
    return 1


def parse_sources(raw: Mapping[str, Any]) -> dict[str, SourceCfg]:
    out = {}
    for name, s in (raw.get("sources") or {}).items():
        if not s.get("enabled", True):
            continue
        w = s.get("active_window") or ["00:00", "24:00"]
        out[name] = SourceCfg(name, float(s["interval_sec"]), _n_targets(s), (_hm(w[0]), _hm(w[1])), int(s.get("daily_budget", 0)))
    return out


def load(root: Optional[Path] = None) -> MonitorConfig:
    import yaml
    base = Path(root) if root else ROOT
    m = yaml.safe_load((base / "config" / "monitor.yaml").read_text(encoding="utf-8")) or {}
    s = yaml.safe_load((base / "config" / "sources.yaml").read_text(encoding="utf-8")) or {}
    return MonitorConfig(tick_sec=float(m.get("tick_sec", 60)), kafka=m.get("kafka") or {}, freshness=m.get("freshness") or {},
                         expected_gap_override_sec=m.get("expected_gap_override_sec") or {}, layers=m.get("layers") or {},
                         sources=parse_sources(s))
