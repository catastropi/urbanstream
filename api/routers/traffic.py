"""도로 링크·속도·혼잡. 읽는 곳: dim.dim_link, fact.traffic_speed, mart.v_traffic_level / v_traffic_latest / v_congestion_threshold_latest."""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import respond, services
from ..errors import ApiError
from ..pagination import cursor_params, decode_cursor, paginate, resolve_window
from ..schemas import Congestion, Envelope, Link, PagedEnvelope, SpeedPoint, Threshold
from ..security import guard

router = APIRouter(prefix="/v1", tags=["traffic"], dependencies=[Depends(guard)])

_LINK_COLS = "link_id, length_m, vertex_count, is_collected, source_note"


@router.get("/links", response_model=PagedEnvelope[Link], summary="도로 링크 목록 (link_id 순)")
def list_links(request: Request, response: Response, limit: int = Query(100, ge=1, le=500), cursor: Optional[str] = None,
               collected_only: bool = Query(False, description="true 면 실제로 수집 중인 링크만"),
               geometry: bool = Query(False, description="true 면 GeoJSON 형상 포함 (limit 최대 100)")):
    if geometry and limit > 100:
        raise ApiError(422, "limit_too_large_for_geometry", "geometry=true 일 때 limit 는 최대 100입니다")
    c = decode_cursor(cursor, 1)
    svc = services(request)

    def build():
        rows = svc.db.rows(
            f"SELECT {_LINK_COLS}, CASE WHEN %(geom)s THEN ST_AsGeoJSON(geom)::jsonb END AS geometry FROM dim.dim_link "
            "WHERE (%(collected)s = false OR is_collected) AND (%(c1)s::text IS NULL OR link_id > %(c1)s) "
            "ORDER BY link_id LIMIT %(n)s",
            {"geom": geometry, "collected": collected_only, "n": limit + 1, **cursor_params(c, 1)})
        rows, page = paginate(rows, limit, ["link_id"])
        return PagedEnvelope[Link](data=rows, page=page, meta=svc.meta(None, stale_after_sec=None, empty=not rows, with_loaded_at=False))
    return respond(request, response, 300, build)


@router.get("/links/{link_id}", response_model=Envelope[Link], summary="링크 1건 (형상 포함)",
            responses={404: {"description": "없는 링크"}})
def get_link(link_id: str, request: Request, response: Response):
    svc = services(request)
    if len(link_id) > 40:
        raise ApiError(404, "link_not_found", "없는 링크입니다")

    def build():
        row = svc.db.row(f"SELECT {_LINK_COLS}, ST_AsGeoJSON(geom)::jsonb AS geometry FROM dim.dim_link WHERE link_id = %(id)s", {"id": link_id})
        if row is None:
            raise ApiError(404, "link_not_found", "없는 링크입니다")
        return Envelope[Link](data=row, meta=svc.meta(None, stale_after_sec=None, with_loaded_at=False))
    return respond(request, response, 300, build)


# 성능(Step 9 측정): 뷰(v_traffic_level / v_traffic_latest)를 통째로 읽으면 DISTINCT ON 이 fact.traffic_speed 전체(150만 행 → 약 5초,
# 5초 제한에 걸림)를 정렬한다. 링크별로 LATERAL + LIMIT 1 로 부르면 (link_id, event_time) 인덱스로 링크당 1행만 읽는다(300링크 약 50ms).
# LIMIT 1 이 없으면 플래너가 뷰를 풀어서 다시 전체 정렬로 돌아간다 → 지우지 말 것(tests/api/test_api_pg.py::test_current_uses_per_link_index_path).
# 의미는 뷰와 같다. 단 dim.dim_link 에 없는 link_id 의 관측은 나오지 않는다(DEFERRED D-153).
_CURRENT_WITH_BASE = """
SELECT l.link_id, l.event_time, l.speed_kmh, l.level, l.day_type, l.kst_hour, l.threshold_n_obs, l.threshold_provisional,
       l.threshold_version, b.baseline_p50, b.baseline_provisional, b.ratio_to_baseline
FROM dim.dim_link d
CROSS JOIN LATERAL (SELECT * FROM mart.v_traffic_level x WHERE x.link_id = d.link_id LIMIT 1) l
LEFT JOIN LATERAL (SELECT * FROM mart.v_traffic_latest y WHERE y.link_id = d.link_id AND y.event_time = l.event_time LIMIT 1) b ON true
WHERE (%(link)s::text IS NULL OR d.link_id = %(link)s) AND (%(level)s::text IS NULL OR l.level = %(level)s)
  AND (%(c1)s::text IS NULL OR d.link_id > %(c1)s)
ORDER BY d.link_id LIMIT %(n)s"""

_CURRENT_NO_BASE = """
SELECT l.link_id, l.event_time, l.speed_kmh, l.level, l.day_type, l.kst_hour, l.threshold_n_obs, l.threshold_provisional,
       l.threshold_version
FROM dim.dim_link d
CROSS JOIN LATERAL (SELECT * FROM mart.v_traffic_level x WHERE x.link_id = d.link_id LIMIT 1) l
WHERE (%(link)s::text IS NULL OR d.link_id = %(link)s) AND (%(level)s::text IS NULL OR l.level = %(level)s)
  AND (%(c1)s::text IS NULL OR d.link_id > %(c1)s)
ORDER BY d.link_id LIMIT %(n)s"""


@router.get("/traffic/current", response_model=PagedEnvelope[Congestion],
            summary="링크별 가장 최근 속도·혼잡 단계·기준선 대비 (Gold 적재 기준, 실시간 아님)")
def traffic_current(request: Request, response: Response, link_id: Optional[str] = Query(None, max_length=40),
                    level: Optional[Literal["severe", "heavy", "slow", "normal"]] = None,
                    limit: int = Query(100, ge=1, le=1000), cursor: Optional[str] = None):
    c = decode_cursor(cursor, 1)
    svc = services(request)

    def build():
        # 기준선 MV 는 WITH NO DATA 로 만들어져 첫 새로고침 전에는 읽으면 오류 → 채워졌는지 보고 기준선 열 없이 답한다
        populated = svc.cached("mv_populated", 30, lambda: {"v": bool((svc.db.row(
            "SELECT ispopulated FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'mv_link_baseline'") or {}).get("ispopulated"))})["v"]
        rows = svc.db.rows(_CURRENT_WITH_BASE if populated else _CURRENT_NO_BASE,
                           {"link": link_id, "level": level, "n": limit + 1, **cursor_params(c, 1)})
        rows, page = paginate(rows, limit, ["link_id"])
        notes = [] if populated else ["baseline_unavailable"]
        if any(r.get("baseline_provisional") for r in rows):
            notes.append("baseline_provisional")
        as_of = max((r["event_time"] for r in rows), default=None)
        return PagedEnvelope[Congestion](data=rows, page=page, meta=svc.meta(as_of, notes=notes, empty=not rows))
    return respond(request, response, 30, build)


@router.get("/traffic/speeds", response_model=PagedEnvelope[SpeedPoint], summary="한 링크의 기간별 속도 (최대 7일)",
            responses={404: {"description": "없는 링크"}})
def traffic_speeds(request: Request, response: Response, link_id: str = Query(..., max_length=40),
                   start: Optional[datetime] = Query(None, alias="from", description="포함. 오프셋 필수(예: 2026-11-05T09:00:00%2B09:00)"),
                   end: Optional[datetime] = Query(None, alias="to", description="제외. 기본은 지금"),
                   limit: int = Query(500, ge=1, le=1000), cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now())
    c = decode_cursor(cursor, 1)

    def build():
        if svc.db.row("SELECT 1 AS x FROM dim.dim_link WHERE link_id = %(id)s", {"id": link_id}) is None:
            raise ApiError(404, "link_not_found", "없는 링크입니다")
        rows = svc.db.rows(
            "SELECT event_time, speed_kmh, travel_time_sec, dq_flags, fetched_at FROM fact.traffic_speed "
            "WHERE link_id = %(id)s AND event_time >= %(s)s AND event_time < %(e)s "
            "AND (%(c1)s::timestamptz IS NULL OR event_time > %(c1)s::timestamptz) ORDER BY event_time LIMIT %(n)s",
            {"id": link_id, "s": w.start, "e": w.end, "n": limit + 1, **cursor_params(c, 1)})
        rows, page = paginate(rows, limit, ["event_time"])
        return PagedEnvelope[SpeedPoint](data=rows, page=page, meta=svc.meta(max((r["event_time"] for r in rows), default=None),
                                                                           stale_after_sec=None, empty=not rows))
    return respond(request, response, 30, build)


@router.get("/traffic/thresholds", response_model=Envelope[List[Threshold]], summary="한 링크의 혼잡 임계값(최신 버전, 요일유형×시각)")
def traffic_thresholds(request: Request, response: Response, link_id: str = Query(..., max_length=40),
                       day_type: Optional[Literal["weekday", "sat", "sun", "holiday"]] = None):
    svc = services(request)

    def build():
        rows = svc.db.rows(
            "SELECT version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days, speed_p10, speed_p25, speed_p50, speed_p90, "
            "sufficient, provisional, computed_at FROM mart.v_congestion_threshold_latest "
            "WHERE link_id = %(id)s AND (%(dt)s::text IS NULL OR day_type = %(dt)s) ORDER BY day_type, kst_hour",
            {"id": link_id, "dt": day_type})
        notes = ["baseline_provisional"] if any(r["provisional"] for r in rows) else []
        return Envelope[List[Threshold]](data=rows, meta=svc.meta(max((r["computed_at"] for r in rows), default=None),
                                                                  stale_after_sec=192 * 3600, notes=notes, empty=not rows))
    return respond(request, response, 300, build)
