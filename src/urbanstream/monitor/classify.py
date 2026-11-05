"""DAG 실패 알림(pending) 분류. 증거와 겹치면 격하·묶고, 증거가 없으면 원래 수준을 유지한다 (fail-safe, ADR-0010 D8-5).

증거 우선순위: host_stall(수집기가 직접 알림) > collector_blackout(전 소스 침묵) > 없음.
- host_stall 과 겹침  → warning 으로 격하, dedup_key 'host_stall:<날짜>' 로 묶는다 (잠자기 한 번에 DAG 여러 개가 실패해도 1건).
- blackout 과만 겹침  → 수준은 그대로, 'collector_blackout_fail:<날짜>' 로 묶기만 한다 (원인을 모르니 격하하지 않는다).
- 증거 없음           → 그대로.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from ..notify.core import Decision
from .gaps import Gap, overlaps

KST = ZoneInfo("Asia/Seoul")


def _dt(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str) and v:
        try:
            d = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def window_of(row: Mapping[str, Any], fallback_sec: float = 600.0) -> tuple[datetime, datetime]:
    """실패한 작업이 실행된 시간대. meta 에 시작·끝이 있으면 그것을, 없으면 알림 생성 직전 fallback_sec 을 쓴다."""
    meta = row.get("meta") or {}
    end = _dt(meta.get("ended_at")) or _dt(row.get("created_at")) or datetime.now(timezone.utc)
    start = _dt(meta.get("started_at")) or (end - timedelta(seconds=fallback_sec))
    return min(start, end), end


def make_classifier(gaps: Sequence[Gap], *, margin_sec: float) -> Callable[[dict], Decision]:
    stalls = [g for g in gaps if g.kind == "host_stall"]
    blacks = [g for g in gaps if g.kind == "collector_blackout"]

    def classify(row: dict) -> Decision:
        a, b = window_of(row)
        for g in stalls:
            if overlaps(a, b, g.start, g.end, margin_sec):
                return Decision("warning", f"host_stall:{g.start.astimezone(KST).date()}", "host_stall")
        for g in blacks:
            if overlaps(a, b, g.start, g.end, margin_sec):
                return Decision(row["level"], f"collector_blackout_fail:{g.start.astimezone(KST).date()}", "collector_blackout_group")
        return Decision(row["level"], row.get("dedup_key"), None)

    return classify
