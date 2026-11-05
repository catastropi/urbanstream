"""돌발 상황과 속도 영향. 읽는 곳: mart.v_incident_episode, mart.incident_impact, mart.v_incident_impact_by_type."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import respond, services
from ..pagination import cursor_params, decode_cursor, paginate, resolve_window
from ..schemas import Envelope, ImpactByType, IncidentEpisode, IncidentImpact, IncidentImpactItem, PagedEnvelope
from ..security import guard

router = APIRouter(prefix="/v1", tags=["incidents"], dependencies=[Depends(guard)])

_EP = ("e.acc_id, e.occurred_at, e.first_seen_at, e.last_seen_at, e.absent_since, e.expected_clear_at, e.obs_count, e.is_long_term, "
       "e.still_listed, e.acc_type, e.link_id, e.lon, e.lat")
_IMP = ("i.duration_min AS i_duration_min, i.link_id AS i_link_id, i.match_kind AS i_match_kind, i.distance_m AS i_distance_m, "
        "i.n_in AS i_n_in, i.n_pre AS i_n_pre, i.ratio_in AS i_ratio_in, i.ratio_pre AS i_ratio_pre, i.delta AS i_delta, "
        "i.sufficient AS i_sufficient, i.provisional AS i_provisional, i.finalized_at AS i_finalized_at")
_EP_KEYS = ["acc_id", "occurred_at", "first_seen_at", "last_seen_at", "absent_since", "expected_clear_at", "obs_count", "is_long_term",
            "still_listed", "acc_type", "link_id", "lon", "lat"]


def _item(r: dict) -> IncidentImpactItem:
    ep = IncidentEpisode(**{k: r[k] for k in _EP_KEYS})
    imp = None
    if r["i_match_kind"] is not None:
        imp = IncidentImpact(**{k[2:]: v for k, v in r.items() if k.startswith("i_")})
    if r["still_listed"]:
        st = "listed"
    elif imp is not None:
        st = "finalized"
    elif r["is_long_term"]:
        st = "excluded_long_term"
    else:
        st = "pending"
    return IncidentImpactItem(status=st, episode=ep, impact=imp)


@router.get("/incidents/active", response_model=Envelope[List[IncidentEpisode]], summary="지금 목록에 있는 돌발 상황")
def incidents_active(request: Request, response: Response, include_long_term: bool = Query(False, description="장기 공사 포함"),
                     limit: int = Query(200, ge=1, le=500)):
    svc = services(request)

    def build():
        rows = svc.db.rows(
            f"SELECT {_EP} FROM mart.v_incident_episode e WHERE e.still_listed AND (%(lt)s OR NOT e.is_long_term) "
            "ORDER BY e.occurred_at DESC, e.acc_id LIMIT %(n)s", {"lt": include_long_term, "n": limit})
        return Envelope[List[IncidentEpisode]](data=rows, meta=svc.meta(max((r["last_seen_at"] for r in rows), default=None),
                                                                        empty=not rows))
    return respond(request, response, 30, build)


@router.get("/incidents", response_model=PagedEnvelope[IncidentImpactItem],
            summary="돌발 에피소드와 속도 영향(확정은 해제 24시간 뒤, 7일 넘는 장기 공사는 제외)")
def incidents_list(request: Request, response: Response,
                   start: Optional[datetime] = Query(None, alias="from"), end: Optional[datetime] = Query(None, alias="to"),
                   acc_type: Optional[str] = Query(None, max_length=40), limit: int = Query(100, ge=1, le=500),
                   cursor: Optional[str] = None):
    svc = services(request)
    w = resolve_window(start, end, svc.now())
    c = decode_cursor(cursor, 2)

    def build():
        rows = svc.db.rows(
            f"SELECT {_EP}, {_IMP} FROM mart.v_incident_episode e "
            "LEFT JOIN mart.incident_impact i ON i.acc_id = e.acc_id AND i.occurred_at = e.occurred_at "
            "WHERE e.occurred_at >= %(s)s AND e.occurred_at < %(e)s AND (%(t)s::text IS NULL OR e.acc_type = %(t)s) "
            "AND (%(c1)s::timestamptz IS NULL OR (e.occurred_at, e.acc_id) < (%(c1)s::timestamptz, %(c2)s::text)) "
            "ORDER BY e.occurred_at DESC, e.acc_id DESC LIMIT %(n)s",
            {"s": w.start, "e": w.end, "t": acc_type, "n": limit + 1, **cursor_params(c, 2)})
        rows, page = paginate(rows, limit, ["occurred_at", "acc_id"])
        items = [_item(r) for r in rows]
        notes = ["impact_pending_24h"] if any(i.status == "pending" for i in items) else []
        if any(i.impact and i.impact.provisional for i in items):
            notes.append("baseline_provisional")
        return PagedEnvelope[IncidentImpactItem](data=items, page=page, meta=svc.meta(
            max((r["last_seen_at"] for r in rows), default=None), stale_after_sec=None, notes=notes, empty=not rows))
    return respond(request, response, 60, build)


@router.get("/incidents/impact-by-type", response_model=Envelope[List[ImpactByType]], summary="돌발 유형별 속도 영향 요약")
def incidents_impact_by_type(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT acc_type, n_episodes, n_linked, n_sufficient, ratio_in_p50, delta_p50, provisional "
                           "FROM mart.v_incident_impact_by_type ORDER BY n_episodes DESC, acc_type")
        notes = ["baseline_provisional"] if any(r["provisional"] for r in rows) else []
        return Envelope[List[ImpactByType]](data=rows, meta=svc.meta(None, stale_after_sec=None, notes=notes, empty=not rows))
    return respond(request, response, 300, build)
