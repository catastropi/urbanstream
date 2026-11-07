"""대시보드 시험용 가짜 API. 실제 API 와 같은 {data, meta[, page]} 모양의 합성 데이터를 돌려준다.
(Postgres·PostGIS 가 없는 환경에서 화면 코드를 돌리기 위한 것이다. 값은 현실 데이터가 아니다.)
tests/support 에 두는 이유: tests 아래에 __init__.py 를 두면 패키지 이름을 가린다 (HANDOFF 6장)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from dashboard.client import ApiClient, FetchResponse
from dashboard.config import Settings

NOW = datetime(2026, 11, 5, 6, 20, 30, tzinfo=timezone.utc)      # 2026-11-05 15:20:30 KST
KEY = "k" * 43
LINKS = ["1220003800", "1110014200", "1120005200", "1190000700"]


def z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def meta(as_of=None, *, stale=False, notes=(), loaded_at=None, total=None):
    age = None if as_of is None else round((NOW - as_of).total_seconds(), 1)
    return {"as_of": None if as_of is None else z(as_of), "age_sec": age, "stale": None if as_of is None else stale,
            "loaded_at": z(loaded_at or NOW - timedelta(minutes=40)), "total": total, "notes": list(notes)}


def env(data, m=None, page=None):
    out = {"data": data, "meta": m or meta(NOW - timedelta(minutes=45))}
    if page is not None:
        out["page"] = page
    return out


def line(i):
    x, y = 126.97 + i * 0.01, 37.55 + i * 0.005
    return {"type": "LineString", "coordinates": [[x, y], [x + 0.004, y + 0.002], [x + 0.008, y + 0.001]]}


def comps():
    return [{"component": "seoul_traffic", "component_kind": "source", "status": "ok", "reason": None, "age_sec": 60.0, "last_ok_at": z(NOW - timedelta(minutes=1))},
            {"component": "silver_bus_arrival", "component_kind": "layer", "status": "degraded", "reason": "no_new_file", "age_sec": 900.0, "last_ok_at": z(NOW - timedelta(minutes=15))},
            {"component": "mart_bus_delay", "component_kind": "mart", "status": "future_status", "reason": None, "age_sec": None, "last_ok_at": None}]


def payload_for(path: str, params: dict) -> dict:
    ops = meta(NOW - timedelta(seconds=40))
    if path == "/v1/pipeline/health":
        return env({"overall": "degraded", "counts": {"ok": 1, "degraded": 1}, "components": comps()}, ops)
    if path == "/v1/pipeline/sources":
        return env([{"source_system": "seoul_traffic", "status": "ok", "age_sec": 60.0, "last_ok_at": z(NOW - timedelta(minutes=1)), "calls_1h": 8, "fail_1h": 0,
                     "p95_latency_ms": 310, "budget_used": 200, "budget_limit": 1000, "reason": None}], ops)
    if path == "/v1/pipeline/layers":
        return env([{"layer": "silver", "table_name": "silver_bus_arrival", "status": "degraded", "age_sec": 900.0, "last_write_at": z(NOW - timedelta(minutes=15)), "reason": "x"}], ops)
    if path == "/v1/pipeline/marts":
        return env([{"mart": "traffic_threshold", "status": "ok", "age_sec": 3600.0, "last_computed_at": z(NOW - timedelta(hours=1)), "max_age_hours": 30.0}], ops)
    if path == "/v1/pipeline/gaps":
        return env([{"kind": "host_sleep", "gap_start": z(NOW - timedelta(hours=5)), "gap_end": z(NOW - timedelta(hours=4)), "duration_sec": 3600, "recoverable": False}], ops)
    if path == "/v1/pipeline/alerts":
        return env([{"alert_id": 1, "created_at": z(NOW - timedelta(minutes=30)), "dedup_key": None, "level": "warning", "status": "digest", "title": "적재 정체"}], ops)
    if path == "/v1/dq/summary":
        return env([{"run_date": "2026-11-05", "n_checks": 24, "n_pass": 18, "n_warn": 3, "n_fail": 0, "n_fail_error": 0, "n_unknown": 3, "checked_at": z(NOW - timedelta(hours=10))}], ops)
    if path == "/v1/dq/latest":
        return env([{"rule_id": "fresh.traffic", "status": "unknown", "severity": "warning", "shadow": True, "value": None, "threshold": ">=0.9", "target": "fact.traffic_speed",
                     "checked_at": z(NOW - timedelta(hours=10)), "kind": "x", "action": "record", "run_date": "2026-11-05", "n_checked": None, "detail": None}], ops)
    if path == "/v1/links":
        geom = params.get("geometry") == "true"
        return env([{"link_id": l, "length_m": 120.0 + i, "is_collected": True, "geometry": line(i) if geom else None, "vertex_count": 3, "source_note": None}
                    for i, l in enumerate(LINKS)], meta(None), {"has_more": False, "limit": 100, "next_cursor": None})
    if path == "/v1/traffic/current":
        rows = [{"link_id": l, "event_time": z(NOW - timedelta(minutes=45)), "speed_kmh": 31.5 + i, "baseline_p50": None, "baseline_provisional": True, "ratio_to_baseline": None,
                 "level": None, "day_type": None, "kst_hour": None, "threshold_n_obs": 8, "threshold_provisional": True, "threshold_version": None} for i, l in enumerate(LINKS)]
        return env(rows, meta(NOW - timedelta(minutes=45), notes=["baseline_provisional"]), {"has_more": False, "limit": 200, "next_cursor": None})
    if path == "/v1/traffic/speeds":
        pts = [{"event_time": z(NOW - timedelta(minutes=450 * k // 60 + 7 * k)), "fetched_at": z(NOW), "speed_kmh": (None if k == 3 else 30.0 + k % 7), "travel_time_sec": 30, "dq_flags": []} for k in range(40)]
        return env(pts, meta(NOW - timedelta(minutes=45)), {"has_more": False, "limit": 1000, "next_cursor": None})
    if path == "/v1/traffic/thresholds":
        rows = [{"link_id": params.get("link_id"), "day_type": d, "kst_hour": h, "speed_p10": 20.0, "speed_p25": 25.0, "speed_p50": 32.0, "speed_p90": 40.0, "n_obs": 30,
                 "provisional": True, "sufficient": True, "computed_at": z(NOW), "n_days": 3, "span_days": 3, "version_date": "2026-11-05"}
                for d in ("weekday", "sat", "sun", "holiday") for h in range(24)]
        return env(rows, meta(NOW - timedelta(hours=2), notes=["baseline_provisional"]))
    if path == "/v1/incidents/active":
        base = {"acc_type": "공사", "occurred_at": z(NOW - timedelta(hours=3)), "expected_clear_at": z(NOW + timedelta(hours=2)), "link_id": None, "still_listed": True,
                "is_long_term": False, "last_seen_at": z(NOW - timedelta(minutes=2)), "first_seen_at": z(NOW - timedelta(hours=3)), "absent_since": None, "obs_count": 90}
        return env([{**base, "acc_id": "A1", "lat": 37.56, "lon": 126.98}, {**base, "acc_id": "A2", "lat": 37.57, "lon": 127.0}, {**base, "acc_id": "A3", "lat": None, "lon": None}],
                   meta(NOW - timedelta(minutes=2)))
    if path == "/v1/incidents":
        ep = {"acc_id": "A9", "acc_type": "사고", "occurred_at": z(NOW - timedelta(days=1)), "still_listed": False, "lat": 37.5, "lon": 127.0, "link_id": "1220003800",
              "first_seen_at": z(NOW), "last_seen_at": z(NOW), "absent_since": None, "expected_clear_at": None, "is_long_term": False, "obs_count": 3}
        im = {"delta": -4.2, "ratio_in": 0.8, "ratio_pre": 1.0, "n_in": 5, "n_pre": 6, "sufficient": True, "provisional": True, "match_kind": "link", "distance_m": 0.0,
              "duration_min": 60, "finalized_at": z(NOW), "link_id": "1220003800"}
        return env([{"episode": ep, "impact": im, "status": "finalized"}, {"episode": {**ep, "acc_id": "A10"}, "impact": None, "status": "listed"}], meta(NOW - timedelta(hours=1)),
                   {"has_more": False, "limit": 100, "next_cursor": None})
    if path == "/v1/incidents/impact-by-type":
        return env([{"acc_type": "사고", "n_episodes": 4, "n_linked": 2, "n_sufficient": 1, "delta_p50": -4.2, "ratio_in_p50": 0.8, "provisional": True},
                    {"acc_type": "공사", "n_episodes": 9, "n_linked": 0, "n_sufficient": 0, "delta_p50": None, "ratio_in_p50": None, "provisional": True}], meta(NOW - timedelta(hours=1)))
    if path == "/v1/bus/positions":
        b = {"event_time": z(NOW - timedelta(minutes=1)), "dq_flags": [], "veh_id": "V1", "plain_no": "서울74사1234", "section_id": "S1", "sect_ord": 3, "congestion": 3, "is_full": False,
             "is_running": True, "stop_flag": 0, "last_stop_id": None, "next_stop_id": None, "sect_dist": 1.0, "full_sect_dist": 2.0, "is_last": False}
        return env([{**b, "lat": 37.55, "lon": 126.99}, {**b, "veh_id": "V2", "lat": None, "lon": None}], meta(NOW - timedelta(minutes=1), notes=[]))
    if path == "/v1/bus/arrival-drift":
        rows = [{"event_date": "2026-11-04", "kst_hour": h, "route_id": "100100118", "station_id": s, "n_approaches": 5, "drift_p50_sec": 30.0 + h, "drift_p90_sec": 200.0 + h, "provisional": True}
                for s in ("S100", "S200") for h in (8, 9, 10)]
        return env(rows, meta(NOW - timedelta(hours=5), notes=["daily_grain_kst_dates"]), {"has_more": False, "limit": 500, "next_cursor": None})
    if path == "/v1/bus/section-slowness":
        return env([{"event_date": "2026-11-04", "kst_hour": 9, "route_id": "100100118", "section_id": "S1", "sect_ord": 3, "veh_id": "V1", "entered_at": z(NOW), "traversal_sec": 130,
                     "base_p50_sec": 100, "base_n_obs": 20, "base_provisional": True, "ratio_to_base": 1.3, "day_type": "weekday"}], meta(NOW - timedelta(hours=5)),
                   {"has_more": False, "limit": 200, "next_cursor": None})
    if path == "/v1/weather/effect":
        return env([{"precip_class": "none", "n_hours": 80, "n_links": 4, "ratio_p25": 0.9, "ratio_p50": 1.0, "ratio_p75": 1.05, "insufficient": False, "provisional": True},
                    {"precip_class": "light", "n_hours": 5, "n_links": 3, "ratio_p25": 0.7, "ratio_p50": 0.85, "ratio_p75": 0.95, "insufficient": True, "provisional": True}],
                   meta(NOW - timedelta(hours=2), notes=["association_not_causation", "baseline_provisional"]))
    if path == "/v1/weather/observations":
        rows = [{"base_time": z(NOW - timedelta(hours=k)), "event_date": "2026-11-05", "t1h": 8.0 - k * 0.1, "rn1": 0.0 if k else None, "pty": 0, "reh": 50, "wsd": 1.0, "nx": 60, "ny": 127,
                 "is_weekend": False, "iso_dow": 4, "uuu": None, "vec": None, "vvv": None, "value_unparsed_count": 0} for k in range(6)]
        return env(rows, meta(NOW - timedelta(hours=1)), {"has_more": False, "limit": 200, "next_cursor": None})
    if path == "/v1/anomalies":
        return env([{"link_id": LINKS[0], "event_time": z(NOW - timedelta(hours=3)), "detected_at": z(NOW - timedelta(hours=2)), "speed_kmh": 12.0, "baseline_p50": 32.0, "baseline_iqr": 5.0,
                     "robust_z": -4.2, "direction": "slow", "status": "candidate", "n_obs": 8, "provisional": True}], meta(NOW - timedelta(hours=2)),
                   {"has_more": False, "limit": 50, "next_cursor": None})
    raise KeyError(path)


_SNAPSHOT = None


def _validate_like_api(path: str, params: dict):
    """실제 API 처럼 OpenAPI 스냅샷의 필수 매개변수·정수 범위·최대 길이를 검사해 422 를 돌려준다.
    (Mac 에서 버스 route_id 누락이 422 로 드러났다 — 가짜 API 가 너그러워서 시험이 못 잡았다. D-176)"""
    global _SNAPSHOT
    if _SNAPSHOT is None:
        from pathlib import Path
        _SNAPSHOT = json.loads((Path(__file__).resolve().parents[1] / "contract" / "snapshots" / "openapi_v1.json").read_text(encoding="utf-8"))
    op = (_SNAPSHOT["paths"].get(path) or {}).get("get")
    if op is None:
        return FetchResponse(404, {}, b"{}")
    known = {q["name"]: q for q in op.get("parameters", []) if q.get("in") == "query"}
    errors = [f"unknown:{k}" for k in params if k not in known]
    errors += [f"missing:{n}" for n, q in known.items() if q.get("required") and n not in params]
    for k, v in params.items():
        sch = (known.get(k) or {}).get("schema") or {}
        if sch.get("type") == "integer":
            try:
                iv = int(v)
            except (TypeError, ValueError):
                errors.append(f"int:{k}")
                continue
            if ("minimum" in sch and iv < sch["minimum"]) or ("maximum" in sch and iv > sch["maximum"]):
                errors.append(f"range:{k}")
        if "maxLength" in sch and len(str(v)) > sch["maxLength"]:
            errors.append(f"length:{k}")
    if errors:
        body = {"type": "urn:urbanstream:error:validation_error", "code": "validation_error", "status": 422, "request_id": "t" * 32}
        return FetchResponse(422, {}, json.dumps(body).encode())
    return None


class FakeFetch:
    """client.fetch 대용. 호출을 (경로, 매개변수)로 기록하고 상태/예외를 바꿔 끼울 수 있다."""

    def __init__(self, handler: Optional[Callable[[str, dict], Optional[FetchResponse]]] = None):
        self.calls: list = []
        self.handler = handler
        self.last_headers: dict = {}

    def __call__(self, url, headers, params, timeout):
        self.last_headers = dict(headers)
        path = url.split("://", 1)[1].split("/", 1)[1]
        path = "/" + path
        self.calls.append((path, dict(params)))
        bad = _validate_like_api(path, dict(params))
        if bad is not None:
            return bad
        if self.handler is not None:
            r = self.handler(path, dict(params))
            if r is not None:
                return r
        return FetchResponse(200, {}, json.dumps(payload_for(path, dict(params))).encode())

    def paths(self):
        return [p for p, _ in self.calls]


def settings(**kw) -> Settings:
    base = dict(api_url="http://api:8000", api_key=KEY)
    base.update(kw)
    return Settings(**base)


class Clock:
    """monotonic 시계 + 현재 시각을 함께 움직이는 가짜 시계."""

    def __init__(self, now: datetime = NOW):
        self.t = 1000.0
        self.base = now
        self.sleeps: list = []

    def mono(self):
        return self.t

    def now(self):
        return self.base + timedelta(seconds=self.t - 1000.0)

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s

    def advance(self, s):
        self.t += s


def make_client(fetch: Optional[FakeFetch] = None, clock: Optional[Clock] = None, **kw):
    fetch = fetch or FakeFetch()
    clock = clock or Clock()
    on_budget = kw.pop("on_budget", "fail")
    return ApiClient(settings(**kw), fetch=fetch, clock=clock.mono, now=clock.now, sleep=clock.sleep, on_budget=on_budget), fetch, clock
