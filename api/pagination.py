"""기간(window)과 키셋 커서 (D9-7)."""
from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, NamedTuple, Optional, Sequence

from .errors import ApiError
from .schemas import PageInfo

KST = timezone(timedelta(hours=9))
MAX_WINDOW_DAYS = 7


class Window(NamedTuple):
    start: datetime      # 포함
    end: datetime        # 제외


def kst_date(dt: datetime) -> date:
    return dt.astimezone(KST).date()


def resolve_window(start: Optional[datetime], end: Optional[datetime], now: datetime, *,
                   default_hours: int = 24, max_days: int = MAX_WINDOW_DAYS) -> Window:
    """[start, end). 시각에는 오프셋이 반드시 있어야 한다. 초과·역전은 잘라 주지 않고 422 로 알린다."""
    for name, v in (("from", start), ("to", end)):
        if v is not None and (v.tzinfo is None or v.utcoffset() is None):
            raise ApiError(422, "naive_datetime", f"'{name}' 에는 시간대 오프셋이 필요합니다 (예: 2026-11-05T09:00:00+09:00 — URL 에서는 + 를 %2B 로 씁니다)")
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=default_hours)
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if start >= end:
        raise ApiError(422, "invalid_window", "'from' 은 'to' 보다 앞이어야 합니다")
    if end - start > timedelta(days=max_days):
        raise ApiError(422, "window_too_large", f"기간은 최대 {max_days}일입니다 (요청: {(end - start).total_seconds() / 86400:.1f}일)")
    return Window(start, end)


def encode_cursor(values: Sequence[Any]) -> str:
    raw = json.dumps([v.isoformat() if isinstance(v, (datetime, date)) else str(v) for v in values], separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(raw: Optional[str], n: int) -> Optional[list]:
    """None 이면 첫 페이지. 모양이 틀리면 422 (커서 안의 값은 SQL 에 매개변수로만 들어간다)."""
    if not raw:
        return None
    try:
        vals = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode())
        if not (isinstance(vals, list) and len(vals) == n and all(isinstance(v, str) and len(v) <= 80 for v in vals)):
            raise ValueError
        return vals
    except Exception as e:                                             # noqa: BLE001 — 어떤 형태의 불량 입력이든 같은 오류로
        raise ApiError(422, "invalid_cursor", "cursor 값이 올바르지 않습니다 (응답의 page.next_cursor 를 그대로 쓰세요)") from e


def cursor_params(vals: Optional[list], n: int) -> dict:
    """SQL 의 %(c1)s.. 매개변수. 첫 페이지면 모두 None."""
    return {f"c{i + 1}": (vals[i] if vals else None) for i in range(n)}


def paginate(rows: list, limit: int, key_cols: Sequence[str]) -> tuple[list, PageInfo]:
    """limit+1 건을 읽어 온 결과를 자른다. 다음 커서는 마지막 반환 행의 key_cols 값."""
    more = len(rows) > limit
    rows = rows[:limit]
    nxt = encode_cursor([rows[-1][c] for c in key_cols]) if (more and rows) else None
    return rows, PageInfo(limit=limit, has_more=more, next_cursor=nxt)
