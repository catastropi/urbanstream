"""수집 공백 판정 (순수 함수). ADR-0010 D8-5 / D8-12.

host_stall        : 수집기가 깨어나며 직접 남긴 host_resume 이벤트(slept_sec)로 만든 구간. 가장 강한 증거.
collector_blackout: 'call' 이벤트가 stall 임계(기본 600초) 이상 끊겼던 구간. 원인은 모른다 (잠자기일 수도, 수집기 중단일 수도).
                    host_stall 과 겹치면 따로 기록하지 않는다 (같은 사건을 두 번 세지 않는다).
둘 다 수집기가 못 부른 시간이라 API 가 실시간 값만 줘서 복구할 수 없다 (recoverable=false, D-09).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional, Sequence


@dataclass(frozen=True)
class Gap:
    kind: str
    start: datetime
    end: datetime
    evidence: str = ""

    @property
    def duration_sec(self) -> int:
        return int((self.end - self.start).total_seconds())


def host_stalls(resumes: Iterable[tuple[datetime, float]], min_slept_sec: float) -> list[Gap]:
    """(깨어난 시각, 잠든 초) → 구간. 시작 시각은 초 단위로 맞춘다 (같은 이벤트를 다시 읽어도 같은 (kind,start) → UNIQUE 로 멱등)."""
    out = []
    for woke, slept in resumes:
        if slept is None or slept < min_slept_sec:
            continue
        start = (woke - timedelta(seconds=slept)).replace(microsecond=0)
        out.append(Gap("host_stall", start, woke, f"host_resume slept_sec={slept:.0f}"))
    return out


def silences(times: Sequence[datetime], min_sec: float) -> list[tuple[datetime, datetime]]:
    """정렬된 이벤트 시각 사이에서 min_sec 이상 벌어진 구간."""
    return [(a, b) for a, b in zip(times, times[1:]) if (b - a).total_seconds() >= min_sec]


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime, margin_sec: float = 0.0) -> bool:
    m = timedelta(seconds=margin_sec)
    return a_start - m <= b_end and b_start - m <= a_end


def blackouts(times: Sequence[datetime], stalls: Sequence[Gap], min_sec: float, margin_sec: float) -> list[Gap]:
    out = []
    for a, b in silences(times, min_sec):
        if any(overlaps(a, b, s.start, s.end, margin_sec) for s in stalls):
            continue
        out.append(Gap("collector_blackout", a.replace(microsecond=0), b, f"no call events for {int((b - a).total_seconds())}s"))
    return out


def ongoing_blackout(last_event_at: Optional[datetime], now: datetime, min_sec: float) -> Optional[float]:
    """지금 진행 중인 침묵 길이(초). 임계 미만이거나 기록이 없으면 None (기록이 없는 것은 침묵이 아니라 '모름')."""
    if last_event_at is None:
        return None
    d = (now - last_event_at).total_seconds()
    return d if d >= min_sec else None
