"""API ↔ 실제 Postgres. 모든 요청은 api_service 역할(`-c role=api_service`)로 나간다 → 권한이 모자라면 여기서 500/503 으로 드러난다.
빈 DB, 첫 새로고침 전의 기준선 MV, 값이 있는 DB, 경계, SQL 주입, 페이지 끝까지 따라가기."""
from __future__ import annotations

import os
from urllib.parse import quote
from datetime import date, datetime, timedelta, timezone

import pytest

# 기본 환경에 fastapi 가 없으면 수집 오류 대신 건너뛴다(설치: pip install -r requirements-api.txt)
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from api.config import load_settings
from api.main import create_app
from pg_support import pg  # noqa: F401

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
KEY = "k" * 32
H = {"X-API-Key": KEY}
NOW = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)          # 목요일 12:00 KST
MON = datetime(2026, 11, 2, 12, 0, tzinfo=KST)                  # 월요일 (기준선용)


@pytest.fixture
def make(pg):  # noqa: F811
    """make(clock=None, **env) -> TestClient. 끝나면 풀을 닫는다."""
    opened = []

    def _make(clock=None, **extra):
        env = {"API_KEYS": KEY, "API_DSN": os.environ["GOLD_TEST_DSN"], "API_DB_EXTRA_OPTIONS": "-c role=api_service",
               "API_RATE_BURST": "1000", "API_RATE_LIMIT_PER_MIN": "6000", **extra}
        app = create_app(load_settings(env), clock=clock or (lambda: NOW))
        c = TestClient(app, raise_server_exceptions=False, headers=H)
        c.__enter__()
        opened.append(c)
        return c
    yield _make
    for c in opened:
        c.__exit__(None, None, None)


# ---------------------------------------------------------------- seed helpers
def link(pg, lid, collected=True, lon0=127.0):
    pg.execute("INSERT INTO dim.dim_link (link_id, vertex_count, length_m, geom, is_collected) VALUES (%s,2,800,ST_GeomFromText(%s,4326),%s)",
               (lid, f"LINESTRING({lon0} 37.5, {lon0 + 0.01} 37.5)", collected))


def speed(pg, key, lid, t, sp, flags=()):
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, dq_flags, event_date) "
               "VALUES (%s,%s,%s,%s,%s,100,%s,%s)", (key, lid, t, t, sp, list(flags), t.astimezone(KST).date()))


def refresh(pg):
    pg.execute("SELECT mart.refresh_all()")


def threshold(pg, lid, day_type="weekday", hour=12, ver=date(2026, 11, 2), p=(10, 20, 30, 50), suff=True, prov=False):
    pg.execute("INSERT INTO mart.congestion_threshold (version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days, speed_p10, speed_p25, "
               "speed_p50, speed_p90, sufficient, provisional) VALUES (%s,%s,%s,%s,50,10,30,%s,%s,%s,%s,%s,%s)",
               (ver, lid, day_type, hour, *p, suff, prov))


ROUTES_NEEDING_PARAMS = {"/v1/traffic/speeds": "?link_id=L1", "/v1/traffic/thresholds": "?link_id=L1", "/v1/bus/positions": "?route_id=R1",
                         "/v1/bus/arrival-drift": "?route_id=R1", "/v1/bus/section-slowness": "?route_id=R1", "/v1/links/{link_id}": None}


def all_get_paths(c):
    return [p for p in c.get("/openapi.json").json()["paths"] if p not in ("/health", "/ready")]


# ---------------------------------------------------------------- 빈 DB
def test_empty_db_every_endpoint_answers_cleanly(make):
    c = make()
    for p in all_get_paths(c):
        if p == "/v1/links/{link_id}":
            r = c.get("/v1/links/NOPE")
            assert r.status_code == 404 and r.json()["code"] == "link_not_found"
            continue
        r = c.get(p + ROUTES_NEEDING_PARAMS.get(p, ""))
        if p in ("/v1/traffic/speeds",):
            assert r.status_code == 404, p                      # 링크가 없으면 404
            continue
        assert r.status_code == 200, (p, r.text)
        body = r.json()
        assert "meta" in body and "data" in body, p
        if p == "/v1/pipeline/marts":                            # 빈 DB 에서도 4행, 전부 unknown (신선도를 모른다 ≠ ok)
            assert {m["status"] for m in body["data"]} == {"unknown"} and body["meta"]["as_of"] is None
            continue
        if p != "/v1/pipeline/health":
            assert body["data"] == [], p
            assert "empty_result" in body["meta"]["notes"] or p in ("/v1/links",), p


def test_empty_pipeline_health_is_unknown_not_ok(make):
    d = make().get("/v1/pipeline/health").json()["data"]
    assert d["overall"] == "unknown" and d["counts"].get("ok", 0) == 0


def test_ready_ok_and_health(make):
    c = make()
    assert c.get("/ready", headers={}).json() == {"status": "ok", "db": "ok"}


# ---------------------------------------------------------------- 기준선 MV 가 아직 비어 있을 때
def test_current_works_before_first_baseline_refresh(make, pg):  # noqa: F811
    link(pg, "L1")
    speed(pg, "a", "L1", NOW - timedelta(minutes=5), 25.0)
    c = make()
    r = c.get("/v1/traffic/current")
    assert r.status_code == 200, r.text
    row = r.json()["data"][0]
    assert row["link_id"] == "L1" and row["speed_kmh"] == 25.0 and row["level"] is None and row["ratio_to_baseline"] is None
    assert "baseline_unavailable" in r.json()["meta"]["notes"]


def test_current_after_refresh_has_baseline_and_level(make, pg):  # noqa: F811
    link(pg, "L1")
    for i in range(4):                                         # 월요일 12시 기준선 (KST 월=1)
        speed(pg, f"b{i}", "L1", MON + timedelta(minutes=i), 40.0)
    t = datetime(2026, 11, 9, 12, 5, tzinfo=KST)                # 다음 월요일 12:05 의 현재값
    speed(pg, "now", "L1", t, 9.0)
    refresh(pg)
    threshold(pg, "L1", "weekday", 12)
    c = make(clock=lambda: t + timedelta(minutes=2))
    d = c.get("/v1/traffic/current?link_id=L1").json()
    row = d["data"][0]
    assert row["level"] == "severe" and row["baseline_p50"] == 40.0 and row["ratio_to_baseline"] == pytest.approx(9 / 40)
    assert row["baseline_provisional"] is True and "baseline_provisional" in d["meta"]["notes"]
    assert d["meta"]["as_of"].startswith("2026-11-09T03:05") and d["meta"]["age_sec"] == 120 and d["meta"]["stale"] is False


def test_level_boundaries_and_insufficient(make, pg):  # noqa: F811
    t = datetime(2026, 11, 4, 12, 0, tzinfo=KST)               # 수요일 12시
    for lid, sp in (("A", 10.0), ("B", 20.0), ("C", 30.0), ("D", 30.1), ("E", 5.0)):
        link(pg, lid)
        speed(pg, lid, lid, t, sp)
        threshold(pg, lid, "weekday", 12, suff=(lid != "E"))
    c = make()
    got = {r["link_id"]: r["level"] for r in c.get("/v1/traffic/current").json()["data"]}
    assert got == {"A": "severe", "B": "heavy", "C": "slow", "D": "normal", "E": None}      # 경계값 포함(<=), 표본 부족은 계산하지 않음
    only = c.get("/v1/traffic/current?level=heavy").json()["data"]
    assert [r["link_id"] for r in only] == ["B"]


def test_stale_flag(make, pg):  # noqa: F811
    link(pg, "L1")
    speed(pg, "a", "L1", NOW - timedelta(hours=4), 25.0)
    m = make().get("/v1/traffic/current").json()["meta"]
    assert m["stale"] is True and m["age_sec"] == pytest.approx(4 * 3600)
    m = make(API_STALE_AFTER_SEC="86400").get("/v1/traffic/current").json()["meta"]
    assert m["stale"] is False


# ---------------------------------------------------------------- 링크·페이지네이션
def test_pagination_follows_cursor_to_the_end(make, pg):  # noqa: F811
    for i in range(7):
        link(pg, f"L{i}", collected=(i % 2 == 0))
    c = make()
    seen, cur = [], None
    for _ in range(10):
        r = c.get("/v1/links", params={"limit": 3, **({"cursor": cur} if cur else {})}).json()
        seen += [x["link_id"] for x in r["data"]]
        assert r["page"]["limit"] == 3
        if not r["page"]["has_more"]:
            assert r["page"]["next_cursor"] is None
            break
        cur = r["page"]["next_cursor"]
    assert seen == [f"L{i}" for i in range(7)]
    assert [x["link_id"] for x in c.get("/v1/links?collected_only=true").json()["data"]] == ["L0", "L2", "L4", "L6"]
    exact = c.get("/v1/links?limit=7").json()["page"]
    assert exact["has_more"] is False                           # 정확히 limit 건이면 다음 쪽 없음


def test_links_geometry_rules(make, pg):  # noqa: F811
    link(pg, "L1")
    c = make()
    assert c.get("/v1/links").json()["data"][0]["geometry"] is None
    assert c.get("/v1/links?geometry=true").json()["data"][0]["geometry"]["type"] == "LineString"
    assert c.get("/v1/links?geometry=true&limit=101").json()["code"] == "limit_too_large_for_geometry"
    assert c.get("/v1/links/L1").json()["data"]["geometry"]["type"] == "LineString"


def test_bad_params_are_422(make):
    c = make()
    for q in ("/v1/links?limit=0", "/v1/links?limit=501", "/v1/links?cursor=!!!", "/v1/traffic/current?level=bogus", "/v1/traffic/speeds",
              "/v1/traffic/speeds?link_id=L1&from=2026-11-01T00:00:00", "/v1/anomalies?status=x"):
        r = c.get(q)
        assert r.status_code in (404, 422), q
    assert c.get("/v1/links?cursor=!!!").json()["code"] == "invalid_cursor"


# ---------------------------------------------------------------- 기간 경계
def test_speeds_window_boundaries(make, pg):  # noqa: F811
    link(pg, "L1")
    start = NOW - timedelta(hours=2)
    for i, dt in enumerate((start - timedelta(seconds=1), start, NOW - timedelta(seconds=1), NOW)):
        speed(pg, f"s{i}", "L1", dt, 30.0 + i)
    c = make()
    r = c.get("/v1/traffic/speeds", params={"link_id": "L1", "from": start.isoformat(), "to": NOW.isoformat()}).json()
    assert [p["speed_kmh"] for p in r["data"]] == [31.0, 32.0]  # from 포함, to 제외
    assert c.get("/v1/traffic/speeds", params={"link_id": "L1", "from": (NOW - timedelta(days=8)).isoformat()}).json()["code"] == "window_too_large"
    assert c.get("/v1/traffic/speeds", params={"link_id": "L1", "from": NOW.isoformat(), "to": start.isoformat()}).json()["code"] == "invalid_window"
    assert c.get("/v1/traffic/speeds", params={"link_id": "NOPE"}).json()["code"] == "link_not_found"


def test_speeds_zero_and_null_pass_through_with_flags(make, pg):  # noqa: F811
    link(pg, "L1")
    speed(pg, "z", "L1", NOW - timedelta(minutes=1), 0.0, flags=["speed_zero"])
    speed(pg, "n", "L1", NOW - timedelta(minutes=2), None)
    d = make().get("/v1/traffic/speeds?link_id=L1").json()["data"]
    assert d[0]["speed_kmh"] is None and d[1]["speed_kmh"] == 0.0 and d[1]["dq_flags"] == ["speed_zero"]


def test_speeds_pagination_by_time(make, pg):  # noqa: F811
    link(pg, "L1")
    for i in range(5):
        speed(pg, f"s{i}", "L1", NOW - timedelta(minutes=10 - i), 10.0 + i)
    c = make()
    a = c.get("/v1/traffic/speeds?link_id=L1&limit=2").json()
    b = c.get("/v1/traffic/speeds", params={"link_id": "L1", "limit": 2, "cursor": a["page"]["next_cursor"]}).json()
    d = c.get("/v1/traffic/speeds", params={"link_id": "L1", "limit": 2, "cursor": b["page"]["next_cursor"]}).json()
    assert [p["speed_kmh"] for p in a["data"] + b["data"] + d["data"]] == [10, 11, 12, 13, 14] and d["page"]["has_more"] is False


# ---------------------------------------------------------------- SQL 주입
@pytest.mark.parametrize("evil", ["'; DROP TABLE dim.dim_link; --", "L1' OR '1'='1", "%", "L1\x00", "‮", "x" * 39])
def test_injection_strings_are_just_values(make, pg, evil):  # noqa: F811
    link(pg, "L1")
    c = make()
    for url, params in (("/v1/traffic/speeds", {"link_id": evil}), ("/v1/traffic/current", {"link_id": evil}),
                        ("/v1/traffic/thresholds", {"link_id": evil}), ("/v1/bus/positions", {"route_id": evil}),
                        ("/v1/anomalies", {"link_id": evil}), ("/v1/incidents", {"acc_type": evil}), ("/v1/links/" + quote(evil, safe=""), {})):
        r = c.get(url, params=params)
        assert r.status_code in (200, 404, 422), (url, r.status_code, r.text[:200])
    assert pg.execute("SELECT count(*) FROM dim.dim_link").fetchone()[0] == 1


# ---------------------------------------------------------------- 돌발
def _episode(pg, acc, first, absent, *, long_term=False, typ="사고", link_id="L1"):
    pg.execute("INSERT INTO fact.incident_observation (dedup_key, ingest_id, acc_id, link_id, fetched_at, acc_type, lon, lat, observed_date) "
               "VALUES (%s,%s,%s,%s,%s,%s,127.0,37.5,%s)", (f"d-{acc}", f"i-{acc}", acc, link_id, first, typ, first.astimezone(KST).date()))
    pg.execute("INSERT INTO fact.incident_episode (acc_id, occurred_at, first_seen_at, last_seen_at, absent_since, obs_count, is_long_term) "
               "VALUES (%s,%s,%s,%s,%s,3,%s)", (acc, first, first, absent - timedelta(minutes=1) if absent else NOW, absent, long_term))


def _impact(pg, acc, first, **kw):
    pg.execute("INSERT INTO mart.incident_impact (acc_id, occurred_at, first_seen_at, absent_since, duration_min, acc_type, link_id, match_kind, "
               "n_in, n_pre, ratio_in, ratio_pre, delta, sufficient, provisional) VALUES (%s,%s,%s,%s,30,'사고','L1','link_id',5,5,0.5,0.9,-0.4,%s,%s)",
               (acc, first, first, first + timedelta(minutes=30), kw.get("sufficient", True), kw.get("provisional", False)))


def test_incident_statuses(make, pg):  # noqa: F811
    t = NOW - timedelta(hours=30)
    _episode(pg, "listed", t, None)
    _episode(pg, "final", t + timedelta(minutes=1), t + timedelta(minutes=40))
    _impact(pg, "final", t + timedelta(minutes=1))
    _episode(pg, "pending", t + timedelta(minutes=2), t + timedelta(minutes=50))
    _episode(pg, "longterm", t + timedelta(minutes=3), t + timedelta(minutes=60), long_term=True)
    c = make()
    items = {i["episode"]["acc_id"]: i for i in c.get("/v1/incidents", params={"from": (NOW - timedelta(days=2)).isoformat()}).json()["data"]}
    assert {k: v["status"] for k, v in items.items()} == {"listed": "listed", "final": "finalized", "pending": "pending", "longterm": "excluded_long_term"}
    assert items["final"]["impact"]["ratio_in"] == 0.5 and items["pending"]["impact"] is None
    m = c.get("/v1/incidents", params={"from": (NOW - timedelta(days=2)).isoformat()}).json()["meta"]
    assert "impact_pending_24h" in m["notes"]
    act = c.get("/v1/incidents/active").json()["data"]
    assert [e["acc_id"] for e in act] == ["listed"]
    assert c.get("/v1/incidents").json()["data"] == []         # 기본 24시간 창 밖
    by = c.get("/v1/incidents/impact-by-type").json()["data"]
    assert by[0]["acc_type"] == "사고" and by[0]["n_episodes"] == 1 and by[0]["provisional"] is True


def test_incident_pagination_desc_with_ties(make, pg):  # noqa: F811
    t = NOW - timedelta(hours=3)
    for k in "abcde":
        _episode(pg, f"x{k}", t, t + timedelta(minutes=20))        # 같은 occurred_at → acc_id 로 순서 보장
    c = make()
    out, cur = [], None
    for _ in range(6):
        r = c.get("/v1/incidents", params={"limit": 2, **({"cursor": cur} if cur else {})}).json()
        out += [i["episode"]["acc_id"] for i in r["data"]]
        cur = r["page"]["next_cursor"]
        if not r["page"]["has_more"]:
            break
    assert out == ["xe", "xd", "xc", "xb", "xa"]


# ---------------------------------------------------------------- 버스
def test_bus_positions_latest_per_vehicle_and_window(make, pg):  # noqa: F811
    def bp(key, veh, t, route="R1"):
        pg.execute("INSERT INTO fact.bus_position (dedup_key, route_id, veh_id, event_time, fetched_at, event_date, is_running) VALUES (%s,%s,%s,%s,%s,%s,true)",
                   (key, route, veh, t, t, t.astimezone(KST).date()))
    bp("1", "V1", NOW - timedelta(minutes=10))
    bp("2", "V1", NOW - timedelta(minutes=2))
    bp("3", "V2", NOW - timedelta(minutes=20))                  # 15분 창 밖
    bp("4", "V3", NOW - timedelta(minutes=1), route="R2")
    c = make()
    d = c.get("/v1/bus/positions?route_id=R1").json()
    assert [r["veh_id"] for r in d["data"]] == ["V1"] and d["data"][0]["event_time"].startswith("2026-11-05T02:58")
    assert [r["veh_id"] for r in c.get("/v1/bus/positions?route_id=R1&within_min=30").json()["data"]] == ["V1", "V2"]
    e = c.get("/v1/bus/positions?route_id=NONE").json()
    assert e["data"] == [] and "outside_collection_window_or_no_data" in e["meta"]["notes"]


def test_bus_drift_and_slowness(make, pg):  # noqa: F811
    for i in range(3):
        pg.execute("INSERT INTO mart.bus_arrival_drift (route_id, station_id, veh_id, first_event_time, last_event_time, event_date, kst_hour, n_pred, "
                   "first_arrive_in_sec, last_arrive_in_sec, first_eta, last_eta, drift_sec, eta_range_sec, sufficient) "
                   "VALUES ('R1','S1',%s,%s,%s,'2026-11-04',9,5,300,120,%s,%s,%s,60,true)",
                   (f"V{i}", NOW - timedelta(days=1, minutes=i), NOW - timedelta(days=1), NOW, NOW, 30 * (i + 1)))
    for i in range(12):
        pg.execute("INSERT INTO mart.bus_section_time (route_id, veh_id, section_id, sect_ord, entered_at, next_entered_at, traversal_sec, event_date, kst_hour, day_type) "
                   "VALUES ('R1',%s,'SEC1',3,%s,%s,%s,'2026-11-04',9,'weekday')",
                   (f"V{i}", NOW - timedelta(hours=20, minutes=i), NOW - timedelta(hours=19), 100 if i < 11 else 300))
    c = make()
    d = c.get("/v1/bus/arrival-drift?route_id=R1").json()
    assert len(d["data"]) == 1 and d["data"][0]["drift_p50_sec"] == 60 and d["data"][0]["provisional"] is True and d["data"][0]["n_approaches"] == 3
    assert c.get("/v1/bus/arrival-drift?route_id=R1&station_id=S9").json()["data"] == []
    s = c.get("/v1/bus/section-slowness?route_id=R1").json()
    ratios = [r["ratio_to_base"] for r in s["data"]]
    assert len(ratios) == 12 and max(ratios) == pytest.approx(3.0) and "baseline_provisional" in s["meta"]["notes"]
    slow = c.get("/v1/bus/section-slowness?route_id=R1&min_ratio=2").json()["data"]
    assert len(slow) == 1 and slow[0]["traversal_sec"] == 300


# ---------------------------------------------------------------- 날씨·이상
def test_weather(make, pg):  # noqa: F811
    t = NOW - timedelta(hours=1)
    pg.execute("INSERT INTO fact.weather_obs (base_time, nx, ny, t1h, rn1, event_date) VALUES (%s,60,127,12.5,0.0,%s)", (t, t.astimezone(KST).date()))
    for i in range(25):
        pg.execute("INSERT INTO mart.weather_traffic_hourly (link_id, kst_date, kst_hour, n_speed, speed_p50, baseline_p50, ratio, precip_class, sufficient, provisional) "
                   "VALUES (%s,'2026-11-04',%s,5,20,40,0.5,'light',true,true)", (f"L{i}", i % 24))
    c = make()
    o = c.get("/v1/weather/observations").json()
    assert o["data"][0]["t1h"] == 12.5 and o["data"][0]["iso_dow"] == 4 and o["data"][0]["rn1"] == 0.0
    e = c.get("/v1/weather/effect").json()
    assert e["data"][0]["precip_class"] == "light" and e["data"][0]["n_hours"] == 25 and e["data"][0]["insufficient"] is False
    assert "association_not_causation" in e["meta"]["notes"]


def test_anomalies_filters(make, pg):  # noqa: F811
    real_now = pg.execute("SELECT now()").fetchone()[0]
    for i, (st, d) in enumerate((("anomaly", "slow"), ("candidate", "fast"), ("degenerate_baseline", None))):
        pg.execute("INSERT INTO mart.anomaly_event (link_id, event_time, speed_kmh, baseline_p50, baseline_iqr, robust_z, direction, status, n_obs, provisional) "
                   "VALUES (%s,%s,5,40,4,%s,%s,%s,30,%s)", (f"L{i}", real_now - timedelta(hours=i + 1), None if d is None else -9.0, d, st, st != "anomaly"))
    c = make(clock=lambda: real_now)
    all_ = c.get("/v1/anomalies").json()["data"]
    assert [a["link_id"] for a in all_] == ["L0", "L1", "L2"] and all_[2]["robust_z"] is None
    assert [a["link_id"] for a in c.get("/v1/anomalies?status=anomaly").json()["data"]] == ["L0"]
    assert [a["link_id"] for a in c.get("/v1/anomalies?direction=fast").json()["data"]] == ["L1"]


# ---------------------------------------------------------------- 운영
def test_pipeline_and_dq(make, pg):  # noqa: F811
    pg.execute("INSERT INTO ops.source_status (source_system, status, reason, expected_gap_sec) VALUES ('traffic','ok',NULL,120),('bus','idle','window',NULL)")
    pg.execute("INSERT INTO ops.layer_status (layer, table_name, source_system, status) VALUES ('bronze','b_traffic','traffic','degraded')")
    pg.execute("INSERT INTO ops.monitor_state (component) VALUES ('monitor')")
    pg.execute("INSERT INTO ops.load_run (job, date_from, date_to, started_at, finished_at, status) VALUES ('gold_load','2026-11-05','2026-11-05',now()-interval '5 minutes',now()-interval '4 minutes','ok')")
    pg.execute("INSERT INTO ops.alert_event (level, title, status, dedup_key) VALUES ('warning','t','sent','k'),('error','e','sent','k2')")
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', now()-interval '2 hours', now()-interval '1 hour', 3600)")
    pg.execute("INSERT INTO ops.dq_result (run_date, rule_id, kind, target, severity, action, status, shadow, value, detail) "
               "VALUES ('2026-11-04','r1','k','t','error','alert','fail',false,1,'{\"a\":1}'),('2026-11-04','r2','k','','info','record','pass',true,0,'{}')")
    real_now = pg.execute("SELECT now()").fetchone()[0]
    c = make(clock=lambda: real_now)
    h = c.get("/v1/pipeline/health").json()
    assert h["data"]["overall"] == "degraded" and h["data"]["counts"]["idle"] == 1 and h["meta"]["stale"] is False
    assert h["meta"]["loaded_at"] is not None
    assert {s["source_system"]: s["status"] for s in c.get("/v1/pipeline/sources").json()["data"]} == {"traffic": "ok", "bus": "idle"}
    assert c.get("/v1/pipeline/layers").json()["data"][0]["status"] == "degraded"
    assert len(c.get("/v1/pipeline/marts").json()["data"]) == 4
    assert c.get("/v1/pipeline/loads").json()["data"][0]["status"] == "ok"
    assert c.get("/v1/pipeline/gaps").json()["data"][0]["duration_sec"] == 3600
    assert [a["level"] for a in c.get("/v1/pipeline/alerts?level=error").json()["data"]] == ["error"]
    s = c.get("/v1/dq/summary").json()["data"][0]
    assert (s["n_checks"], s["n_fail"], s["n_fail_error"]) == (2, 1, 1)
    lat = c.get("/v1/dq/latest").json()["data"]
    assert [r["rule_id"] for r in lat] == ["r1", "r2"] and all(r["detail"] is None for r in lat)
    assert c.get("/v1/dq/latest?include=detail").json()["data"][0]["detail"] == {"a": 1}
    assert [r["rule_id"] for r in c.get("/v1/dq/latest?status=pass").json()["data"]] == ["r2"]


# ---------------------------------------------------------------- 인증은 모든 경로에
def test_every_v1_route_requires_key(make):
    c = make()
    for p in all_get_paths(c):
        url = p.replace("{link_id}", "L1") + (ROUTES_NEEDING_PARAMS.get(p) or "")
        r = c.get(url, headers={"X-API-Key": ""})
        assert r.status_code == 401, p
        assert c.get(url, headers={"X-API-Key": "wrong"}).status_code == 401, p


# ---------------------------------------------------------------- DB 가 없을 때
def test_db_down_is_503_but_health_stays_up():
    env = {"API_KEYS": KEY, "API_DSN": "host=127.0.0.1 port=1 dbname=x user=x password=x connect_timeout=1"}
    with TestClient(create_app(load_settings(env), clock=lambda: NOW), raise_server_exceptions=False, headers=H) as c:
        assert c.get("/health").status_code == 200
        assert c.get("/ready").status_code == 503
        r = c.get("/v1/pipeline/health")
        assert r.status_code == 503 and r.json()["code"] == "database_unavailable" and "Retry-After" in r.headers
        assert "password" not in r.text and "127.0.0.1" not in r.text


# ---------------------------------------------------------------- 연결 옵션 (역할 설정과 별개로 앱이 직접 건다)
def test_connection_options_are_applied_by_the_app(make, pg):  # noqa: F811
    from api.db import Database
    st = load_settings({"API_KEYS": KEY, "API_DSN": os.environ["GOLD_TEST_DSN"], "API_DB_EXTRA_OPTIONS": "-c role=api_service"})
    db = Database(st)
    db.open()
    try:
        got = {k: db.row(f"SHOW {k}")[k] for k in ("statement_timeout", "lock_timeout", "default_transaction_read_only", "TimeZone")}
        assert got == {"statement_timeout": "5s", "lock_timeout": "1s", "default_transaction_read_only": "on", "TimeZone": "UTC"}
        assert db.row("SELECT current_user AS u")["u"] == "api_service"
        import psycopg
        with pytest.raises(Exception) as ei:
            with db.pool.connection() as c:
                c.execute("CREATE TABLE fact.evil (x int)")
        assert isinstance(ei.value, (psycopg.errors.ReadOnlySqlTransaction, psycopg.errors.InsufficientPrivilege))
    finally:
        db.close()


def test_current_uses_per_link_index_path(make, pg):  # noqa: F811
    """전체 정렬(DISTINCT ON 이 테이블 전체를 읽음)로 되돌아가면 실데이터에서 5초 제한에 걸린다 — 실행 계획으로 잡는다."""
    from api.routers.traffic import _CURRENT_WITH_BASE, _CURRENT_NO_BASE
    for i in range(30):
        link(pg, f"L{i}")
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, dq_flags, event_date) "
               "SELECT l.link_id||ts::text, l.link_id, ts, ts, 30, 100, '{}', '2026-11-04' FROM dim.dim_link l, "
               "generate_series(timestamptz '2026-11-04 00:00+09', timestamptz '2026-11-04 23:59+09', interval '1 minute') ts")
    refresh(pg)
    pg.execute("ANALYZE")
    p = {"link": None, "level": None, "c1": None, "n": 101}
    for q in (_CURRENT_WITH_BASE, _CURRENT_NO_BASE):
        plan = "\n".join(r[0] for r in pg.execute("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + q, p).fetchall())
        scanned = [int(x) for x in __import__("re").findall(r"traffic_speed_link_time_ix on traffic_speed \w+ \(actual rows=(\d+)", plan)]
        assert scanned and max(scanned) <= 1 and "Incremental Sort" not in plan, plan[:1500]     # 링크당 1행만 읽는다
    # 의미는 뷰와 같다
    c = make()
    rows = c.get("/v1/traffic/current?limit=1000").json()["data"]
    assert len(rows) == 30 and all(r["speed_kmh"] == 30 for r in rows)
    view = pg.execute("SELECT count(*) FROM mart.v_traffic_level").fetchone()[0]
    assert view == 30


def test_current_ignores_links_unknown_to_dim(make, pg):  # noqa: F811
    """알려진 제한(D-153): dim_link 에 없는 link_id 의 관측은 current 에 나오지 않는다."""
    link(pg, "L1")
    speed(pg, "a", "L1", NOW - timedelta(minutes=5), 25.0)
    speed(pg, "b", "GHOST", NOW - timedelta(minutes=5), 25.0)
    assert [r["link_id"] for r in make().get("/v1/traffic/current").json()["data"]] == ["L1"]


def test_large_responses_are_not_cached_small_ones_are(make, pg):  # noqa: F811
    for i in range(320):
        link(pg, f"L{i:03d}")
    c = make()
    c.get("/v1/links?limit=500")
    assert len(c.app.state.services.cache) == 0          # 320 행 > 300 → 저장하지 않음
    c.get("/v1/links?limit=10")
    assert len(c.app.state.services.cache) == 1
