"""날씨 관측과 강수 등급별 도로 속도 효과(연관이지 인과가 아님)."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import respond, services
from ..pagination import cursor_params, decode_cursor, paginate, resolve_window
from ..schemas import Envelope, PagedEnvelope, WeatherEffect, WeatherObs
from ..security import guard

router = APIRouter(prefix="/v1", tags=["weather"], dependencies=[Depends(guard)])


@router.get("/weather/observations", response_model=PagedEnvelope[WeatherObs], summary="기간별 날씨 관측 (최대 7일)")
def weather_observations(request: Request, response: Response,
                         start: Optional[datetime] = Query(None, alias="from"), end: Optional[datetime] = Query(None, alias="to"),
                         limit: int = Query(500, ge=1, le=1000), cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now())
    c = decode_cursor(cursor, 3)

    def build():
        rows = svc.db.rows(
            "SELECT base_time, nx, ny, t1h, rn1, uuu, vvv, reh, pty, vec, wsd, value_unparsed_count, event_date, iso_dow, is_weekend "
            "FROM mart.v_weather_hourly WHERE base_time >= %(s)s AND base_time < %(e)s "
            "AND (%(c1)s::timestamptz IS NULL OR (base_time, nx, ny) > (%(c1)s::timestamptz, %(c2)s::int, %(c3)s::int)) "
            "ORDER BY base_time, nx, ny LIMIT %(n)s",
            {"s": w.start, "e": w.end, "n": limit + 1, **cursor_params(c, 3)})
        rows, page = paginate(rows, limit, ["base_time", "nx", "ny"])
        return PagedEnvelope[WeatherObs](data=rows, page=page, meta=svc.meta(max((r["base_time"] for r in rows), default=None),
                                                                           stale_after_sec=None, empty=not rows))
    return respond(request, response, 60, build)


@router.get("/weather/effect", response_model=Envelope[List[WeatherEffect]],
            summary="강수 등급별 속도 비(기준선 대비) — 연관이지 인과 아님, insufficient=true 면 읽지 말 것")
def weather_effect(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT precip_class, n_hours, n_links, ratio_p25, ratio_p50, ratio_p75, insufficient, provisional "
                           "FROM mart.v_weather_effect ORDER BY precip_class")
        notes = ["association_not_causation"] + (["baseline_provisional"] if any(r["provisional"] for r in rows) else [])
        return Envelope[List[WeatherEffect]](data=rows, meta=svc.meta(None, stale_after_sec=None, notes=notes, empty=not rows))
    return respond(request, response, 300, build)
