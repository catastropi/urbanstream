"""속도 이상 후보 (최근 7일). 읽는 곳: mart.v_anomaly_recent."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import respond, services
from ..pagination import cursor_params, decode_cursor, paginate, resolve_window
from ..schemas import Anomaly, PagedEnvelope
from ..security import guard

router = APIRouter(prefix="/v1", tags=["anomalies"], dependencies=[Depends(guard)])


@router.get("/anomalies", response_model=PagedEnvelope[Anomaly], summary="최근 속도 이상 (최신순, 최근 7일 안에서만)")
def list_anomalies(request: Request, response: Response, link_id: Optional[str] = Query(None, max_length=40),
                   status: Optional[Literal["anomaly", "candidate", "degenerate_baseline"]] = None,
                   direction: Optional[Literal["slow", "fast"]] = None,
                   start: Optional[datetime] = Query(None, alias="from"), end: Optional[datetime] = Query(None, alias="to"),
                   limit: int = Query(100, ge=1, le=500), cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now())
    c = decode_cursor(cursor, 2)

    def build():
        rows = svc.db.rows(
            "SELECT link_id, event_time, speed_kmh, baseline_p50, baseline_iqr, robust_z, direction, status, n_obs, provisional, detected_at "
            "FROM mart.v_anomaly_recent WHERE event_time >= %(s)s AND event_time < %(e)s AND (%(l)s::text IS NULL OR link_id = %(l)s) "
            "AND (%(st)s::text IS NULL OR status = %(st)s) AND (%(d)s::text IS NULL OR direction = %(d)s) "
            "AND (%(c1)s::timestamptz IS NULL OR (event_time, link_id) < (%(c1)s::timestamptz, %(c2)s::text)) "
            "ORDER BY event_time DESC, link_id DESC LIMIT %(n)s",
            {"s": w.start, "e": w.end, "l": link_id, "st": status, "d": direction, "n": limit + 1, **cursor_params(c, 2)})
        rows, page = paginate(rows, limit, ["event_time", "link_id"])
        notes = ["baseline_provisional"] if any(r["provisional"] for r in rows) else []
        return PagedEnvelope[Anomaly](data=rows, page=page, meta=svc.meta(max((r["event_time"] for r in rows), default=None),
                                                                        stale_after_sec=None, notes=notes, empty=not rows))
    return respond(request, response, 30, build)
