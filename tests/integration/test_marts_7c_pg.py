"""Step 7c (batch/marts.py 7c 부분, batch/maintenance.py, sql/007) — 실제 Postgres 에서 값·멱등·확정 규칙·권한을 확인한다."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from batch import marts as M, maintenance as MT
from batch.jobs import JobError

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
MON1 = datetime(2026, 11, 2, 8, 0, tzinfo=KST)         # 기준선용 월요일
MON2 = datetime(2026, 11, 9, 8, 0, tzinfo=KST)         # 돌발 날 월요일


def conn_of(pg):
    class C:
        def __enter__(self): return pg
        def __exit__(self, *a): return False
    return lambda: C()


def _speed(pg, key, link, t, sp):
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, event_date) "
               "VALUES (%s,%s,%s,%s,%s,100,%s)", (key, link, t, t, sp, t.astimezone(KST).date()))


def _link(pg, link_id, collected=True, lon0=127.00):
    pg.execute("INSERT INTO dim.dim_link (link_id, vertex_count, length_m, geom, is_collected) "
               "VALUES (%s, 2, 800, ST_GeomFromText(%s, 4326), %s)",
               (link_id, f"LINESTRING({lon0} 37.5, {lon0 + 0.01} 37.5)", collected))


def _episode(pg, acc, first, absent, *, link="L1", lon=None, lat=None, long_term=False, typ="사고"):
    pg.execute("INSERT INTO fact.incident_observation (dedup_key, ingest_id, acc_id, link_id, fetched_at, acc_type, lon, lat, observed_date) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (f"d-{acc}", f"i-{acc}", acc, link, first, typ, lon, lat, first.astimezone(KST).date()))
    pg.execute("INSERT INTO fact.incident_episode (acc_id, occurred_at, first_seen_at, last_seen_at, absent_since, obs_count, is_long_term) "
               "VALUES (%s,%s,%s,%s,%s,3,%s)", (acc, first, first, absent - timedelta(minutes=1) if absent else first, absent, long_term))


def _base(pg):
    """L1 월요일 08시·09시 기준선: 40 km/h × 30개씩. 그 뒤 mv 를 채운다."""
    for h in (0, 1):
        for i in range(30):
            _speed(pg, f"b{h}-{i}", "L1", MON1 + timedelta(hours=h, minutes=i), 40.0)


def _refresh(pg):
    pg.execute("SELECT mart.refresh_all()")


# ------------------------------------------------------------------ 문제 B

def test_incident_impact_values_matching_finalization_and_immutability(pg):
    _link(pg, "L1"); _link(pg, "LX", collected=False, lon0=127.5)
    _base(pg)
    first, absent = MON2 + timedelta(minutes=10), MON2 + timedelta(minutes=40)
    for i, m in enumerate((0, 3, 6)):                                   # 직전: 40 (비율 1.0)
        _speed(pg, f"p{i}", "L1", MON2 + timedelta(minutes=m), 40.0)
    for i, m in enumerate((12, 20, 30, 35)):                            # 돌발 중: 20 (비율 0.5)
        _speed(pg, f"i{i}", "L1", MON2 + timedelta(minutes=m), 20.0)
    _refresh(pg)
    _episode(pg, "E1", first, absent, link="L1")                                           # link_id 일치
    _episode(pg, "E2", first, absent, link=None, lon=127.005, lat=37.5015)                 # 약 167 m → distance
    _episode(pg, "E3", first, absent, link="ZZ", lon=127.005, lat=37.51)                   # 약 1.1 km → no_link
    _episode(pg, "E4", first, absent, link="L1", long_term=True)                           # 장기 → 제외
    _episode(pg, "E5", first, None, link="L1")                                             # 아직 목록에 있음
    _episode(pg, "E6", first, absent, link="LX")                                           # 수집 안 하는 링크, 좌표 없음 → no_link
    asof = absent + timedelta(hours=23)
    out = M.build_incident_impact(as_of=asof, connect=conn_of(pg))                         # 23시간 → 아직 대기
    assert out["new"] == 0 and out["waiting"] == 4 and out["long_term_skipped"] == 1 and out["still_listed"] == 1
    out = M.build_incident_impact(as_of=absent + timedelta(hours=25), connect=conn_of(pg))
    assert out["new"] == 4 and out["long_term_skipped"] == 1 and out["still_listed"] == 1
    r = {x[0]: x[1:] for x in pg.execute(
        "SELECT acc_id, match_kind, link_id, n_in, n_pre, ratio_in, ratio_pre, delta, sufficient, duration_min, provisional "
        "FROM mart.incident_impact").fetchall()}
    assert set(r) == {"E1", "E2", "E3", "E6"}
    assert r["E1"][:5] == ("link_id", "L1", 4, 3, 0.5) and r["E1"][5] == 1.0 and abs(r["E1"][6] + 0.5) < 1e-9
    assert r["E1"][7] is True and r["E1"][8] == 30 and r["E1"][9] is True            # 기준선 4주 미만 → provisional
    assert r["E2"][0] == "distance" and r["E2"][1] == "L1" and r["E2"][7] is True
    assert r["E3"][0] == "no_link" and r["E3"][1] is None and r["E3"][7] is False and r["E3"][2] == 0
    assert r["E6"][0] == "no_link"
    # 확정은 다시 계산하지 않는다: 속도를 바꾸고 다시 돌려도 그대로, 새 에피소드만 추가
    pg.execute("UPDATE fact.traffic_speed SET speed_kmh = 5 WHERE dedup_key LIKE 'i%'")
    _refresh(pg)
    again = M.build_incident_impact(as_of=absent + timedelta(hours=26), connect=conn_of(pg))
    assert again["new"] == 0
    assert pg.execute("SELECT ratio_in FROM mart.incident_impact WHERE acc_id='E1'").fetchone()[0] == 0.5
    v = {x[0]: x[1:] for x in pg.execute("SELECT acc_type, n_episodes, n_linked, n_sufficient, provisional FROM mart.v_incident_impact_by_type").fetchall()}
    assert v["사고"] == (4, 2, 2, True)


def test_incident_impact_insufficient_when_few_observations(pg):
    _link(pg, "L1"); _base(pg)
    first, absent = MON2 + timedelta(minutes=10), MON2 + timedelta(minutes=40)
    _speed(pg, "i0", "L1", MON2 + timedelta(minutes=15), 20.0)
    _refresh(pg)
    _episode(pg, "E1", first, absent)
    M.build_incident_impact(as_of=absent + timedelta(hours=30), connect=conn_of(pg))
    row = pg.execute("SELECT n_in, ratio_in, delta, sufficient FROM mart.incident_impact").fetchone()
    assert row[0] >= 1 and row[1] is None and row[2] is None and row[3] is False


# ------------------------------------------------------------------ 문제 D

def _wx(pg, t, rn1, pty):
    pg.execute("INSERT INTO fact.weather_obs (base_time, nx, ny, rn1, pty, t1h, event_date) VALUES (%s,60,127,%s,%s,10,%s)",
               (t, rn1, pty, t.astimezone(KST).date()))


def test_weather_traffic_ratio_classes_sufficiency_and_idempotency(pg):
    _link(pg, "L1"); _base(pg)
    for i in range(5):
        _speed(pg, f"h8-{i}", "L1", MON2 + timedelta(minutes=i * 5), 20.0)              # 08시: 비율 0.5
        _speed(pg, f"h9-{i}", "L1", MON2 + timedelta(hours=1, minutes=i * 5), 40.0)     # 09시: 비율 1.0
    for h in (10, 11, 12, 13, 14):                                                       # 날씨 등급 확인용: 지난주(MON1)에도 같은 속도 → 기준선 2일, 비율 1.0
        for i in range(3):
            _speed(pg, f"x{h}-{i}", "L1", MON2 + timedelta(hours=h - 8, minutes=i), 30.0)
            _speed(pg, f"y{h}-{i}", "L1", MON1 + timedelta(hours=h - 8, minutes=i), 30.0)
    _refresh(pg)
    _wx(pg, MON2, 12.0, 1)                                    # heavy
    _wx(pg, MON2 + timedelta(hours=1), 0.0, 0)                # none
    # 10시 날씨 없음 → NULL
    _wx(pg, MON2 + timedelta(hours=3), 0.0, 3)                # snow_mix
    _wx(pg, MON2 + timedelta(hours=4), 5.0, 1)                # moderate
    _wx(pg, MON2 + timedelta(hours=5), 0.5, 1)                # light
    d = date(2026, 11, 9)
    out = M.build_weather_traffic(d, d, connect=conn_of(pg))
    assert out["rows"] == 7 and out["sufficient"] == 5 and out["no_weather"] == 2
    r = {x[0]: x[1:] for x in pg.execute("SELECT kst_hour, ratio, precip_class, sufficient, n_speed FROM mart.weather_traffic_hourly").fetchall()}
    assert r[8] == (0.5, "heavy", True, 5) and r[9] == (1.0, "none", True, 5)
    assert r[10][1] is None and r[10][2] is False
    assert (r[11][1], r[12][1], r[13][1]) == ("snow_mix", "moderate", "light") and r[11][2] is True
    e = {x[0]: x[1:] for x in pg.execute("SELECT precip_class, n_hours, ratio_p50, insufficient FROM mart.v_weather_effect").fetchall()}
    assert e["heavy"] == (1, 0.5, True) and e["none"] == (1, 1.0, True)                  # 표본 20 미만 → insufficient
    M.build_weather_traffic(d, d, connect=conn_of(pg))                                   # 멱등
    assert pg.execute("SELECT count(*) FROM mart.weather_traffic_hourly").fetchone()[0] == 7
    with pytest.raises(ValueError):
        M.build_weather_traffic(date(2026, 11, 10), d, connect=conn_of(pg))


def test_weather_ratio_is_not_sufficient_when_baseline_is_a_single_day(pg):
    """기준선이 비교 대상 날 하나뿐이면 비율은 항상 1.0(자기 자신과 비교)이다 → sufficient=false (D-178)."""
    _link(pg, "L1")
    for i in range(5):
        _speed(pg, f"s{i}", "L1", MON2 + timedelta(minutes=i * 5), 33.0)
    _refresh(pg)
    _wx(pg, MON2, 0.0, 0)
    d = date(2026, 11, 9)
    out = M.build_weather_traffic(d, d, connect=conn_of(pg))
    r = pg.execute("SELECT ratio, sufficient FROM mart.weather_traffic_hourly").fetchone()
    assert r[0] == 1.0 and r[1] is False and out["sufficient"] == 0
    M.build_weather_traffic(d, d, min_baseline_days=1, connect=conn_of(pg))              # 기준을 낮추면 다시 충분 → 조건이 실제로 작동
    assert pg.execute("SELECT sufficient FROM mart.weather_traffic_hourly").fetchone()[0] is True


# ------------------------------------------------------------------ 창고 관리

def test_maintenance_analyze_snapshot_prune_and_privileges(pg):
    pg.execute("INSERT INTO ops.load_run (job,date_from,date_to,started_at,finished_at,status) VALUES "
               "('gold_load','2025-01-01','2025-01-02', now()-interval '400 days', now()-interval '400 days','ok'),"
               "('gold_load','2025-01-01','2025-01-02', now()-interval '400 days', NULL,'running'),"
               "('gold_load','2026-11-03','2026-11-04', now()-interval '2 hours', now()-interval '1 hour','ok')")
    pg.execute("INSERT INTO ops.call_audit (ingest_id, source_system, fetched_at, ok, event_date) VALUES "
               "('old','s', now()-interval '400 days', true, (now() - interval '400 days')::date),('new','s', now(), true, (now() AT TIME ZONE 'Asia/Seoul')::date)")
    pg.execute("SET ROLE gold_writer")
    try:
        assert MT.analyze_warehouse(connect=conn_of(pg)) > 10
        assert MT.snapshot_table_stats(date(2026, 11, 8), connect=conn_of(pg)) > 10
        n1 = pg.execute("SELECT count(*) FROM ops.table_stats").fetchone()[0]
        MT.snapshot_table_stats(date(2026, 11, 8), connect=conn_of(pg))                  # 같은 날 = 교체
        assert pg.execute("SELECT count(*) FROM ops.table_stats").fetchone()[0] == n1
        out = MT.prune_ops(connect=conn_of(pg))
        assert out["load_run"] == 1 and out["call_audit"] == 1
        assert pg.execute("SELECT count(*) FROM ops.load_run").fetchone()[0] == 2        # running + 최근 ok 는 남음
        with pytest.raises(ValueError):
            MT.prune_ops(retention_days=7, connect=conn_of(pg))
    finally:
        pg.execute("RESET ROLE")
    pg.execute("SET ROLE api_reader")
    try:
        with pytest.raises(Exception):
            pg.execute("SELECT ops.analyze_warehouse()")                                 # 읽기 역할은 실행 불가
    finally:
        pg.execute("RESET ROLE")


def test_7c_roles(pg):
    for role in ("api_reader", "bi_reader"):
        pg.execute(f"SET ROLE {role}")
        try:
            for rel in ("incident_impact", "weather_traffic_hourly", "v_incident_impact_by_type", "v_weather_effect"):
                pg.execute(f"SELECT count(*) FROM mart.{rel}").fetchone()
            with pytest.raises(Exception):
                pg.execute("DELETE FROM mart.incident_impact")
        finally:
            pg.execute("RESET ROLE")
    pg.execute("SET ROLE gold_writer")
    try:
        pg.execute("DELETE FROM mart.weather_traffic_hourly")
    finally:
        pg.execute("RESET ROLE")


def test_unpopulated_baseline_fails_with_clear_message_and_changes_nothing(pg):
    _link(pg, "L1"); _base(pg)                       # _refresh 를 하지 않음 → mv 비어 있음
    d = date(2026, 11, 9)
    with pytest.raises(JobError, match="mv_link_baseline"):
        M.build_weather_traffic(d, d, connect=conn_of(pg))
    with pytest.raises(JobError, match="mv_link_baseline"):
        M.build_incident_impact(as_of=MON2, connect=conn_of(pg))
    assert pg.execute("SELECT count(*) FROM mart.weather_traffic_hourly").fetchone()[0] == 0


def test_every_builder_runs_as_gold_writer(pg):
    """Mac 에서 발견: 샌드박스 테스트가 관리자 계정이라 gold_writer 의 권한 누락(mv_link_baseline 읽기)을 못 잡았다.
    모든 마트 빌더와 선행 검사를 실제 DAG 가 쓰는 역할(gold_writer)로 돌린다."""
    _link(pg, "L1"); _base(pg); _refresh(pg)
    first, absent = MON2 + timedelta(minutes=10), MON2 + timedelta(minutes=40)
    _episode(pg, "E1", first, absent)
    _wx(pg, MON2, 1.0, 1)
    for i in range(3):
        _speed(pg, f"gw{i}", "L1", MON2 + timedelta(minutes=i), 30.0)
    pg.execute("INSERT INTO ops.load_run (job,date_from,date_to,started_at,finished_at,status) VALUES "
               "('gold_load','2026-11-08','2026-11-10', now()-interval '2 hours', now()-interval '1 hour','ok')")
    d = date(2026, 11, 9)
    pg.execute("SET ROLE gold_writer")
    try:
        M.require_recent_load(date(2026, 11, 8), date(2026, 11, 10), connect=conn_of(pg))
        M.build_congestion_threshold(date(2026, 11, 10), connect=conn_of(pg))
        M.build_bus_drift(d, d, connect=conn_of(pg))
        M.build_bus_section_time(d, d, connect=conn_of(pg))
        M.build_weather_traffic(d, d, connect=conn_of(pg))
        out = M.build_incident_impact(as_of=absent + timedelta(hours=30), connect=conn_of(pg))
        MT.analyze_warehouse(connect=conn_of(pg)); MT.snapshot_table_stats(connect=conn_of(pg)); MT.prune_ops(connect=conn_of(pg))
    finally:
        pg.execute("RESET ROLE")
    assert out["new"] == 1
    assert pg.execute("SELECT count(*) FROM mart.weather_traffic_hourly").fetchone()[0] > 0


def test_hour_without_baseline_cell_is_insufficient_not_null(pg):
    """Mac 에서 발견: 기준선 MV 는 매시 갱신이라 가장 최근 시간대에는 셀이 없다 → sufficient 가 NULL 이 되어 NOT NULL 위반."""
    _link(pg, "L1"); _base(pg); _refresh(pg)                    # 기준선은 월요일 08·09시만
    for i in range(4):                                          # 기준선이 없는 시각(23시)의 새 관측
        _speed(pg, f"new{i}", "L1", MON2 + timedelta(hours=15, minutes=i), 30.0)
    _wx(pg, MON2 + timedelta(hours=15), 0.0, 0)
    d = date(2026, 11, 9)
    out = M.build_weather_traffic(d, d, connect=conn_of(pg))
    assert out["rows"] == 1 and out["sufficient"] == 0
    assert pg.execute("SELECT ratio, baseline_p50, sufficient, provisional, precip_class FROM mart.weather_traffic_hourly").fetchone() \
        == (None, None, False, True, "none")
