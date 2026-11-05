"""platform.audit 메시지 → ops.call_event 행. 모르는 필드는 무시하고, 모르는 event 종류도 버리지 않고 보존한다."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

COLUMNS = ["kafka_partition", "kafka_offset", "recorded_at", "event", "source_system", "target", "final", "outcome", "reason",
           "attempt", "will_retry", "status_kind", "http_status", "latency_ms", "n_records", "n_rejects", "delivery_errors",
           "budget_used", "budget_limit", "breaker_state", "slept_sec", "ingest_id"]
INSERT_SQL = (f"INSERT INTO ops.call_event ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))}) "
              "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING")


def _ts(v: Any) -> Optional[datetime]:
    if not isinstance(v, str):
        return None
    try:
        d = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _int(v: Any) -> Optional[int]:
    try:
        return None if v is None or isinstance(v, bool) else int(v)
    except (TypeError, ValueError):
        return None


def _float(v: Any) -> Optional[float]:
    try:
        return None if v is None or isinstance(v, bool) else float(v)
    except (TypeError, ValueError):
        return None


def _bool(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def parse(value: bytes | str, partition: int, offset: int) -> Optional[tuple]:
    """깨진 메시지는 None (건너뜀 — 감시가 메시지 하나 때문에 멈추면 안 된다)."""
    try:
        d = json.loads(value)
    except (ValueError, TypeError):
        return None
    if not isinstance(d, dict):
        return None
    rec = _ts(d.get("recorded_at"))
    if rec is None:
        return None
    br = d.get("breaker")
    state = br.get("state") if isinstance(br, dict) else (br if isinstance(br, str) else None)
    s = lambda k: (str(d[k]) if d.get(k) is not None else None)          # noqa: E731
    return (partition, offset, rec, str(d.get("event") or "call"), s("source_system"), s("target"), _bool(d.get("final")), s("outcome"),
            s("reason"), _int(d.get("attempt")), _bool(d.get("will_retry")), s("status_kind"), _int(d.get("http_status")),
            _int(d.get("latency_ms")), _int(d.get("n_records")), _int(d.get("n_rejects")), _int(d.get("delivery_errors")),
            _int(d.get("budget_used")), _int(d.get("budget_limit")), str(state) if state is not None else None,
            _float(d.get("slept_sec")), s("ingest_id"))
