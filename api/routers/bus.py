"""버스. 주의: '지연'은 시간표 대비가 아니다 — 도착 예측 변동(arrival-drift)과 구간 통과 시간(section-slowness)이다."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import respond, services
from ..pagination import cursor_params, decode_cursor, kst_date, paginate, resolve_window
from ..schemas import ArrivalDrift, BusPosition, Envelope, PagedEnvelope, SectionSlowness
from ..security import guard

router = APIRouter(prefix="/v1", tags=["bus"], dependencies=[Depends(guard)])


@router.get("/bus/positions", response_model=Envelope[list[BusPosition]],
            summary="노선의 차량별 가장 최근 위치 (수집 창 밖에는 비어 있을 수 있음)")
def bus_positions(request: Request, response: Response, route_id: str = Query(..., max_length=40),
                  within_min: int = Query(15, ge=1, le=120, description="이 분 안에 관측된 차량만"),
                  limit: int = Query(300, ge=1, le=500)):
    svc = services(request)
    now = svc.now()

    def build():
        rows = svc.db.rows(
            "SELECT * FROM (SELECT DISTINCT ON (veh_id) veh_id, plain_no, event_time, lon, lat, section_id, sect_ord, last_stop_id, next_stop_id, "
            "sect_dist, full_sect_dist, congestion, stop_flag, is_running, is_last, is_full, dq_flags FROM fact.bus_position "
            "WHERE route_id = %(r)s AND event_time >= %(since)s AND event_time <= %(now)s ORDER BY veh_id, event_time DESC) p "
            "ORDER BY veh_id LIMIT %(n)s",
            {"r": route_id, "since": now - timedelta(minutes=within_min), "now": now, "n": limit})
        notes = [] if rows else ["outside_collection_window_or_no_data"]
        return Envelope[list[BusPosition]](data=rows, meta=svc.meta(max((r["event_time"] for r in rows), default=None), notes=notes,
                                                                    empty=not rows))
    return respond(request, response, 15, build)


@router.get("/bus/arrival-drift", response_model=PagedEnvelope[ArrivalDrift],
            summary="도착 예측 변동(시간·정류소별 중앙값/p90) — 시간표 대비 지연 아님")
def bus_arrival_drift(request: Request, response: Response, route_id: str = Query(..., max_length=40),
                      station_id: Optional[str] = Query(None, max_length=40),
                      start: Optional[datetime] = Query(None, alias="from"), end: Optional[datetime] = Query(None, alias="to"),
                      limit: int = Query(500, ge=1, le=1000), cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now(), default_hours=168)
    d0, d1 = kst_date(w.start), kst_date(w.end - timedelta(microseconds=1))
    c = decode_cursor(cursor, 3)

    def build():
        rows = svc.db.rows(
            "SELECT route_id, station_id, event_date, kst_hour, n_approaches, drift_p50_sec, drift_p90_sec, provisional "
            "FROM mart.v_bus_drift_daily WHERE route_id = %(r)s AND (%(st)s::text IS NULL OR station_id = %(st)s) "
            "AND event_date BETWEEN %(d0)s AND %(d1)s "
            "AND (%(c1)s::date IS NULL OR (event_date, kst_hour, station_id) > (%(c1)s::date, %(c2)s::smallint, %(c3)s::text)) "
            "ORDER BY event_date, kst_hour, station_id LIMIT %(n)s",
            {"r": route_id, "st": station_id, "d0": d0, "d1": d1, "n": limit + 1, **cursor_params(c, 3)})
        rows, page = paginate(rows, limit, ["event_date", "kst_hour", "station_id"])
        notes = ["daily_grain_kst_dates"] + (["provisional_small_sample"] if any(r["provisional"] for r in rows) else [])
        mx = max((r["event_date"] for r in rows), default=None)
        as_of = None if mx is None else datetime.combine(mx, datetime.min.time()).replace(tzinfo=w.start.tzinfo)
        return PagedEnvelope[ArrivalDrift](data=rows, page=page, meta=svc.meta(as_of, stale_after_sec=None, notes=notes,
                                                                             empty=not rows, with_loaded_at=True))
    return respond(request, response, 120, build)


@router.get("/bus/section-slowness", response_model=PagedEnvelope[SectionSlowness],
            summary="구간 통과 시간과 평소 대비 비 (±90초 오차)")
def bus_section_slowness(request: Request, response: Response, route_id: str = Query(..., max_length=40),
                         start: Optional[datetime] = Query(None, alias="from"), end: Optional[datetime] = Query(None, alias="to"),
                         min_ratio: Optional[float] = Query(None, ge=0, le=100, description="이 값 이상으로 느린 통과만"),
                         limit: int = Query(500, ge=1, le=1000), cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now(), default_hours=168)
    c = decode_cursor(cursor, 3)

    def build():
        rows = svc.db.rows(
            "SELECT route_id, veh_id, section_id, sect_ord, entered_at, traversal_sec, event_date, kst_hour, day_type, base_p50_sec, "
            "base_n_obs, base_provisional, ratio_to_base FROM mart.v_bus_section_slowness "
            "WHERE route_id = %(r)s AND entered_at >= %(s)s AND entered_at < %(e)s AND (%(mr)s::float8 IS NULL OR ratio_to_base >= %(mr)s) "
            "AND (%(c1)s::timestamptz IS NULL OR (entered_at, veh_id, section_id) > (%(c1)s::timestamptz, %(c2)s::text, %(c3)s::text)) "
            "ORDER BY entered_at, veh_id, section_id LIMIT %(n)s",
            {"r": route_id, "s": w.start, "e": w.end, "mr": min_ratio, "n": limit + 1, **cursor_params(c, 3)})
        rows, page = paginate(rows, limit, ["entered_at", "veh_id", "section_id"])
        notes = ["baseline_provisional"] if any(r["base_provisional"] for r in rows) else []
        return PagedEnvelope[SectionSlowness](data=rows, page=page, meta=svc.meta(
            max((r["entered_at"] for r in rows), default=None), stale_after_sec=None, notes=notes, empty=not rows))
    return respond(request, response, 120, build)
