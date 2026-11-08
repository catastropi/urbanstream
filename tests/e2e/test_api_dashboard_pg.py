"""e2e: 진짜 uvicorn API ↔ 진짜 Postgres ↔ 대시보드 클라이언트 ↔ Tableau 내보내기 (Step 11, ADR-0013 D11-6).

왜 만들었나: Step 10 에서 대시보드의 '가짜 API' 가 너그러워서 버스 엔드포인트의 필수 매개변수(route_id) 누락이 Mac 에서야 HTTP 422 로 드러났다(D-176).
가짜를 엄격하게 고쳤지만, 같은 종류의 어긋남을 CI 가 먼저 잡도록 '진짜 서버' 에 대한 시험을 하나 둔다.

무엇을 확인하나
1. dashboard/contract.py 의 엔드포인트 21개를 대시보드 클라이언트로 전부 불러 오류 없이 답을 받는다(HTTP 4xx/5xx·검증 오류 없음).
2. 값이 있는 엔드포인트는 화면이 읽겠다고 선언한 필드가 실제 응답 행에 있다.
3. scripts/export_tableau.py 의 collect→write_all 이 진짜 API 로 끝까지 돌고 CSV 5개 + manifest 가 나온다.
4. 틀린 키는 401 → 클라이언트가 인증 오류로 구분한다(키를 오류 문구에 싣지 않는다).

안 하는 것: Kafka·Spark·Collector 구간(이미지 빌드가 무거워 CI 에 두지 않는다 — DEFERRED).
요구: GOLD_TEST_DSN(테스트 전용 DB. 이 시험은 dim/fact/mart/ops 스키마를 지운다), fastapi·uvicorn·requests·psycopg.
"""
from __future__ import annotations

import csv
import json
import os
import socket
import threading
import time
from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")
pytest.importorskip("requests")
pytest.importorskip("psycopg")
import uvicorn  # noqa: E402

from api.config import load_settings  # noqa: E402
from api.main import create_app  # noqa: E402
from dashboard import contract as C  # noqa: E402
from dashboard.client import ApiClient, ApiError  # noqa: E402
from dashboard.config import Settings  # noqa: E402
from pg_support import pg  # noqa: E402,F401
from scripts import export_tableau as ET  # noqa: E402

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
KEY = "e" * 40
NOW = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)      # 목요일 12:00 KST
LID = "L-E2E-1"
ROUTE = "100100118"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _seed(pg):
    pg.execute("INSERT INTO dim.dim_link (link_id, vertex_count, length_m, geom, is_collected) VALUES (%s,2,800,ST_GeomFromText(%s,4326),true)",
               (LID, "LINESTRING(127.0 37.5, 127.01 37.5)"))
    for i in range(6):
        t = NOW - timedelta(minutes=10 * (i + 1))        # API 의 기간 끝(to)은 배타적이라 'now' 정각 값은 기본 창에서 빠진다
        pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, dq_flags, event_date) "
                   "VALUES (%s,%s,%s,%s,%s,100,%s,%s)", (f"k{i}", LID, t, t, 20 + i, [], t.astimezone(KST).date()))
    pg.execute("SELECT mart.refresh_all()")
    pg.execute("INSERT INTO mart.congestion_threshold (version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days, speed_p10, speed_p25, "
               "speed_p50, speed_p90, sufficient, provisional) VALUES (%s,%s,'weekday',12,50,10,30,10,20,30,50,true,false)",
               (date(2026, 11, 2), LID))


@pytest.fixture
def live(pg):  # noqa: F811
    """진짜 uvicorn 서버(스레드). api_service 역할(최소 권한)로 DB 에 붙는다. 끝나면 서버를 내린다."""
    _seed(pg)
    env = {"API_KEYS": KEY, "API_DSN": os.environ["GOLD_TEST_DSN"], "API_DB_EXTRA_OPTIONS": "-c role=api_service",
           "API_RATE_BURST": "1000", "API_RATE_LIMIT_PER_MIN": "6000"}
    app = create_app(load_settings(env), clock=lambda: NOW)
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False))
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    deadline = time.time() + 20
    while not server.started and time.time() < deadline:
        time.sleep(0.05)
    assert server.started, "uvicorn 이 20초 안에 시작하지 않았다"
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    th.join(timeout=10)


def _client(url: str, key: str = KEY) -> ApiClient:
    st = Settings(api_url=url, api_key=key, ttl_scale=0.0, budget_per_min=1000, bus_route_id=ROUTE)
    return ApiClient(st, on_budget="wait")


def _params(ep: C.Endpoint) -> dict:
    if ep.name in ("traffic_speeds", "traffic_thresholds"):
        return {"link_id": LID}
    if ep.name in ("bus_positions", "bus_arrival_drift", "bus_section_slowness"):
        return {"route_id": ROUTE}
    return {}


def test_every_dashboard_endpoint_answers_on_the_real_server(live):
    c = _client(live)
    failures = []
    for ep in C.ALL_ENDPOINTS:
        try:
            res = c.get(ep, _params(ep))
        except ApiError as e:
            failures.append(f"{ep.name}: {e.short()}")
            continue
        assert res.error is None, (ep.name, res.error)
    assert failures == [], "실제 서버가 거부한 엔드포인트:\n" + "\n".join(failures)


def test_seeded_endpoints_return_the_fields_the_screens_read(live):
    c = _client(live)
    for ep, minimum in ((C.LINKS, 1), (C.TRAFFIC_CURRENT, 1), (C.TRAFFIC_SPEEDS, 6), (C.TRAFFIC_THRESHOLDS, 1)):
        rows = c.get(ep, _params(ep) or ({"limit": 100} if ep is C.LINKS else {})).rows
        assert len(rows) >= minimum, (ep.name, len(rows))
        (schema, fields), = ep.reads
        missing = [f for f in fields if f not in rows[0]]
        assert missing == [], f"{ep.name} 응답에 화면이 읽는 필드가 없다: {missing}"


def test_wrong_key_is_an_auth_error_that_never_echoes_the_key(live):
    bad = "x" * 40
    c = _client(live, key=bad)
    with pytest.raises(ApiError) as ei:
        c.get(C.PIPELINE_HEALTH, {})
    assert ei.value.kind == "auth"
    assert bad not in str(ei.value) and bad not in ei.value.short()


def test_tableau_export_runs_end_to_end_against_the_real_api(live, tmp_path):
    c = _client(live)
    rc = ET.main(["--out", str(tmp_path), "--days", "1"], client=c, now=NOW)
    assert rc == 0
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["complete"] is True and manifest["contains_vehicle_identifiers"] is False
    assert {f["file"] for f in manifest["files"]} == {f"{n}.csv" for n in ET.COLUMNS}
    for name, cols in ET.COLUMNS.items():
        with (tmp_path / f"{name}.csv").open(encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh))
        assert header == cols, name
        assert not (set(header) & ET.FORBIDDEN_COLUMNS), name
    speed_rows = next(f["rows"] for f in manifest["files"] if f["file"] == "link_speed_hourly.csv")
    assert speed_rows >= 1                                       # 시드한 속도 6건이 시간별 집계가 되어 나온다
