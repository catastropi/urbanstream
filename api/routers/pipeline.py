"""파이프라인·DQ 운영 상태. Step 8 의 ops.v_* 뷰만 읽는다(원본 ops 표 금지)."""
from __future__ import annotations

from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from ..deps import OPS_SNAPSHOT_STALE_SEC, respond, services
from ..schemas import (AlertRecent, DataGap, DqLatest, DqSummary, Envelope, LayerStatus, LoadRun, MartFreshness,
                       PipelineComponent, PipelineHealth, SourceStatus)
from ..security import guard

router = APIRouter(prefix="/v1", tags=["pipeline"], dependencies=[Depends(guard)])

_RANK = {"down": 4, "degraded": 3, "unknown": 2, "ok": 1}


def _latest(rows, key="checked_at"):
    vals = [r[key] for r in rows if r.get(key) is not None]
    return max(vals) if vals else None


@router.get("/pipeline/health", response_model=Envelope[PipelineHealth], summary="파이프라인 전체 건강(한 줄에 하나씩)")
def pipeline_health(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT component_kind, component, status, reason, last_ok_at, age_sec, checked_at "
                           "FROM ops.v_pipeline_health ORDER BY component_kind, component")
        counts: dict[str, int] = {}
        for r in rows:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        live = [r["status"] for r in rows if r["status"] != "idle"]
        overall = max(live, key=lambda s: _RANK.get(s, 2)) if live else ("idle" if rows else "unknown")
        snap = [r for r in rows if r["component_kind"] in ("source", "layer", "monitor")]
        data = PipelineHealth(overall=overall, counts=counts, components=[PipelineComponent(**r) for r in rows])
        return Envelope[PipelineHealth](data=data, meta=svc.meta(_latest(snap), stale_after_sec=OPS_SNAPSHOT_STALE_SEC, empty=not rows))
    return respond(request, response, 10, build)


@router.get("/pipeline/sources", response_model=Envelope[List[SourceStatus]], summary="수집 소스별 상태")
def pipeline_sources(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT source_system, status, reason, last_call_at, last_ok_at, age_sec, expected_gap_sec, calls_1h, fail_1h, "
                           "p95_latency_ms, budget_used, budget_limit, checked_at FROM ops.v_source_status ORDER BY source_system")
        return Envelope[List[SourceStatus]](data=rows, meta=svc.meta(_latest(rows), stale_after_sec=OPS_SNAPSHOT_STALE_SEC, empty=not rows))
    return respond(request, response, 10, build)


@router.get("/pipeline/layers", response_model=Envelope[List[LayerStatus]], summary="Bronze/Silver 층별 상태")
def pipeline_layers(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT layer, table_name, source_system, status, reason, last_write_at, age_sec, checked_at "
                           "FROM ops.v_layer_status ORDER BY layer, table_name")
        return Envelope[List[LayerStatus]](data=rows, meta=svc.meta(_latest(rows), stale_after_sec=OPS_SNAPSHOT_STALE_SEC, empty=not rows))
    return respond(request, response, 10, build)


@router.get("/pipeline/marts", response_model=Envelope[List[MartFreshness]], summary="마트 신선도")
def pipeline_marts(request: Request, response: Response):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT mart, last_computed_at, age_sec, max_age_hours, status FROM ops.v_mart_freshness ORDER BY mart")
        return Envelope[List[MartFreshness]](data=rows, meta=svc.meta(_latest(rows, "last_computed_at"), stale_after_sec=None, empty=not rows))
    return respond(request, response, 60, build)


@router.get("/pipeline/loads", response_model=Envelope[List[LoadRun]], summary="최근 Gold 적재 실행 (최대 100건)")
def pipeline_loads(request: Request, response: Response, limit: int = Query(20, ge=1, le=100)):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT run_id, job, dag_id, date_from, date_to, started_at, finished_at, status "
                           "FROM ops.v_load_recent ORDER BY run_id DESC LIMIT %(limit)s", {"limit": limit})
        return Envelope[List[LoadRun]](data=rows, meta=svc.meta(_latest(rows, "started_at"), stale_after_sec=None, empty=not rows))
    return respond(request, response, 30, build)


@router.get("/pipeline/gaps", response_model=Envelope[List[DataGap]], summary="최근 30일 수집 공백")
def pipeline_gaps(request: Request, response: Response, limit: int = Query(50, ge=1, le=200)):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT gap_id, kind, gap_start, gap_end, duration_sec, recoverable FROM ops.v_data_gap_recent "
                           "ORDER BY gap_start DESC LIMIT %(limit)s", {"limit": limit})
        return Envelope[List[DataGap]](data=rows, meta=svc.meta(_latest(rows, "gap_end"), stale_after_sec=None, empty=not rows))
    return respond(request, response, 60, build)


@router.get("/pipeline/alerts", response_model=Envelope[List[AlertRecent]], summary="최근 7일 알림")
def pipeline_alerts(request: Request, response: Response, limit: int = Query(50, ge=1, le=200),
                    level: Optional[Literal["info", "warning", "error", "critical"]] = None):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT alert_id, created_at, level, title, status, dedup_key FROM ops.v_alert_recent "
                           "WHERE (%(level)s::text IS NULL OR level = %(level)s) ORDER BY alert_id DESC LIMIT %(limit)s",
                           {"limit": limit, "level": level})
        return Envelope[List[AlertRecent]](data=rows, meta=svc.meta(_latest(rows, "created_at"), stale_after_sec=None, empty=not rows))
    return respond(request, response, 30, build)


# ------------------------------------------------------------------ DQ
@router.get("/dq/summary", response_model=Envelope[List[DqSummary]], summary="날짜별 데이터 품질 요약")
def dq_summary(request: Request, response: Response, days: int = Query(7, ge=1, le=60)):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT run_date, n_checks, n_pass, n_warn, n_fail, n_unknown, n_fail_error, checked_at "
                           "FROM ops.v_dq_summary ORDER BY run_date DESC LIMIT %(days)s", {"days": days})
        return Envelope[List[DqSummary]](data=rows, meta=svc.meta(_latest(rows), stale_after_sec=36 * 3600, empty=not rows))
    return respond(request, response, 60, build)


@router.get("/dq/latest", response_model=Envelope[List[DqLatest]], summary="규칙·대상별 가장 최근 DQ 결과")
def dq_latest(request: Request, response: Response,
              status: Optional[Literal["pass", "warn", "fail", "unknown", "skipped"]] = None,
              include: Optional[Literal["detail"]] = Query(None, description="detail 이면 규칙별 상세(JSON)를 포함")):
    svc = services(request)

    def build():
        rows = svc.db.rows("SELECT rule_id, kind, target, severity, action, status, shadow, run_date, checked_at, value, n_checked, threshold, "
                           "CASE WHEN %(detail)s THEN detail END AS detail FROM ops.v_dq_latest "
                           "WHERE (%(status)s::text IS NULL OR status = %(status)s) ORDER BY rule_id, target",
                           {"status": status, "detail": include == "detail"})
        return Envelope[List[DqLatest]](data=rows, meta=svc.meta(_latest(rows), stale_after_sec=36 * 3600, empty=not rows))
    return respond(request, response, 60, build)
