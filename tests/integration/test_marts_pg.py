"""Step 7b 마트(batch/marts.py + sql/006) — 실제 Postgres 에서 값·멱등·권한을 확인한다."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from batch import marts as M
from batch.jobs import JobError

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
MON = datetime(2026, 11, 2, 8, 0, tzinfo=KST)          # 월요일 08:00 KST


def conn_of(pg):
    class C:                                              # marts 가 `with connect() as conn` 으로 쓰므로 닫지 않는 감싸개
        def __enter__(self): return pg
        def __exit__(self, *a): return False
    return lambda: C()


def _speed(pg, key, link, t, sp):
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, event_date) "
               "VALUES (%s,%s,%s,%s,%s,100,%s)", (key, link, t, t, sp, t.astimezone(KST).date()))


def _arr(pg, key, veh, t, in_sec, code=0, station="S1"):
    pg.execute("INSERT INTO fact.bus_arrival (dedup_key, route_id, station_id, station_order, slot, veh_id, event_time, fetched_at, "
               "arrive_in_sec, arrive_code, event_date) VALUES (%s,'R1',%s,1,1,%s,%s,%s,%s,%s,%s)",
               (key, station, veh, t, t, in_sec, code, t.astimezone(KST).date()))


def _pos(pg, key, veh, t, section, ord_, route="R1"):
    pg.execute("INSERT INTO fact.bus_position (dedup_key, route_id, veh_id, event_time, fetched_at, section_id, sect_ord, event_date) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (key, route, veh, t, t, section, ord_, t.astimezone(KST).date()))


# ------------------------------------------------------------------ 임계값 (문제 A)

def _seed_speeds(pg):
    for i in range(25):                                   # L1: 08:00~08:24, 속도 1..25 → 최신 관측 25
        _speed(pg, f"a{i}", "L1", MON + timedelta(minutes=i), float(i + 1))
    for i in range(5):                                    # L2: 표본 5개 → 부족
        _speed(pg, f"b{i}", "L2", MON + timedelta(minutes=i), 30.0)
    for i in range(25):                                   # L3: 속도 25..1 → 최신 관측 1
        _speed(pg, f"c{i}", "L3", MON + timedelta(minutes=i), float(25 - i))
    _speed(pg, "sat", "L1", datetime(2026, 11, 7, 8, 0, tzinfo=KST), 50.0)   # 토요일은 다른 칸


def test_threshold_quantiles_sufficiency_day_type_and_idempotency(pg):
    _seed_speeds(pg)
    v = date(2026, 11, 4)
    out = M.build_congestion_threshold(v, connect=conn_of(pg))
    assert out["cells"] == 4 and out["sufficient"] == 2
    rows = {(r[0], r[1], r[2]): r[3:] for r in pg.execute(
        "SELECT link_id, day_type, kst_hour, n_obs, n_days, speed_p10, speed_p50, sufficient, provisional "
        "FROM mart.congestion_threshold WHERE version_date=%s", (v,)).fetchall()}
    n, d, p10, p50, ok, prov = rows[("L1", "weekday", 8)]
    assert (n, d, ok, prov) == (25, 1, True, True) and p50 == 13.0 and abs(p10 - 3.4) < 1e-9
    assert rows[("L2", "weekday", 8)][4] is False and rows[("L2", "weekday", 8)][2] is None       # 표본 부족 → 분위수 비움
    assert ("L1", "sat", 8) in rows                                                                  # 요일유형 분리
    M.build_congestion_threshold(v, connect=conn_of(pg))                                            # 같은 날 재실행 = 교체
    assert pg.execute("SELECT count(*) FROM mart.congestion_threshold").fetchone()[0] == 4
    M.build_congestion_threshold(date(2026, 11, 5), connect=conn_of(pg))                            # 새 판은 누적
    assert pg.execute("SELECT count(*) FROM mart.congestion_threshold").fetchone()[0] == 8
    assert pg.execute("SELECT DISTINCT version_date FROM mart.v_congestion_threshold_latest").fetchall() == [(date(2026, 11, 5),)]


def test_traffic_level_view_classifies_and_leaves_insufficient_null(pg):
    _seed_speeds(pg)
    M.build_congestion_threshold(date(2026, 11, 4), connect=conn_of(pg))
    # v_traffic_level 은 링크별 '최신' 관측을 본다 → L1 은 토요일 관측이 최신. 월요일 칸 기준을 보려고 토요일 행을 지운다.
    pg.execute("DELETE FROM fact.traffic_speed WHERE dedup_key='sat'")
    lv = {r[0]: r[1:] for r in pg.execute("SELECT link_id, speed_kmh, level, threshold_provisional FROM mart.v_traffic_level").fetchall()}
    assert lv["L1"] == (25.0, "normal", True)             # 25 > p50(13)
    assert lv["L3"] == (1.0, "severe", True)              # 1 <= p10(3.4)
    assert lv["L2"][1] is None                            # 표본 부족 → 단계 계산 안 함


# ------------------------------------------------------------------ 도착 예측 드리프트 (문제 C-i)

def test_drift_values_splits_approaches_and_skips_waiting_rows(pg):
    t0 = datetime(2026, 11, 2, 9, 0, tzinfo=KST)
    for i, (dt, sec) in enumerate([(0, 300), (90, 240), (180, 200), (270, 60)]):
        _arr(pg, f"v1a{i}", "V1", t0 + timedelta(seconds=dt), sec)
    for i, (dt, sec) in enumerate([(0, 300), (90, 210), (180, 120)]):                 # 1시간 뒤 두 번째 접근: 드리프트 0
        _arr(pg, f"v1b{i}", "V1", t0 + timedelta(hours=1, seconds=dt), sec)
    for i, (dt, sec) in enumerate([(0, 100), (90, 10)]):                              # 예측 2개뿐 → 부족
        _arr(pg, f"v2{i}", "V2", t0 + timedelta(seconds=dt), sec)
    _arr(pg, "w0", "V3", t0, 600, code=2); _arr(pg, "w1", "V3", t0 + timedelta(seconds=90), 500, code=2)   # 출발대기 → 제외
    d = date(2026, 11, 2)
    out = M.build_bus_drift(d, d, connect=conn_of(pg))
    assert out["approaches"] == 3 and out["sufficient"] == 2
    r = pg.execute("SELECT veh_id, n_pred, drift_sec, eta_range_sec, sufficient, kst_hour FROM mart.bus_arrival_drift "
                   "ORDER BY first_event_time, veh_id").fetchall()
    assert r[0] == ("V1", 4, 30, 80, True, 9)             # eta: +300, +330, +380, +330 → 드리프트 30, 범위 80
    assert ("V1", 3, 0, 0, True, 10) in r                 # 두 번째 접근
    assert ("V2", 2, 0, 0, False, 9) in r                 # 예측 2개뿐 → sufficient=false (eta 는 +100, +100)
    assert not [x for x in r if x[0] == "V3"]
    before = pg.execute("SELECT count(*) FROM mart.bus_arrival_drift").fetchone()[0]
    M.build_bus_drift(d, d, connect=conn_of(pg))                                      # 멱등
    assert pg.execute("SELECT count(*) FROM mart.bus_arrival_drift").fetchone()[0] == before


def test_drift_window_and_midnight_crossing(pg):
    base = datetime(2026, 11, 2, 23, 58, tzinfo=KST)       # 자정 직전에 시작해 넘어가는 접근
    for i, sec in enumerate([200, 140, 80, 20]):
        _arr(pg, f"m{i}", "V1", base + timedelta(seconds=90 * i), sec)
    M.build_bus_drift(date(2026, 11, 3), date(2026, 11, 3), connect=conn_of(pg))      # 11-03 창: 시작이 11-02 → 포함 안 됨
    assert pg.execute("SELECT count(*) FROM mart.bus_arrival_drift").fetchone()[0] == 0
    M.build_bus_drift(date(2026, 11, 2), date(2026, 11, 3), connect=conn_of(pg))
    row = pg.execute("SELECT event_date, n_pred FROM mart.bus_arrival_drift").fetchall()
    assert row == [(date(2026, 11, 2), 4)]                 # 자정을 넘어도 한 접근, 시작 날짜 기준
    with pytest.raises(ValueError):
        M.build_bus_drift(date(2026, 11, 3), date(2026, 11, 2), connect=conn_of(pg))


# ------------------------------------------------------------------ 구간 통과 시간 (문제 C-ii)

def test_section_time_pairs_consecutive_sections_only(pg):
    t0 = datetime(2026, 11, 2, 9, 0, tzinfo=KST)
    seq = [(0, "A", 1), (90, "A", 1), (180, "B", 2), (270, "B", 2), (360, "C", 3)]
    for i, (dt, sec, o) in enumerate(seq):
        _pos(pg, f"v{i}", "V1", t0 + timedelta(seconds=dt), sec, o)
    _pos(pg, "w0", "V2", t0, "A", 1); _pos(pg, "w1", "V2", t0 + timedelta(seconds=90), "C", 3)             # 구간을 건너뜀 → 버림
    _pos(pg, "g0", "V3", t0, "A", 1); _pos(pg, "g1", "V3", t0 + timedelta(minutes=40), "B", 2)             # 40분 공백 → 버림
    d = date(2026, 11, 2)
    out = M.build_bus_section_time(d, d, connect=conn_of(pg))
    assert out["traversals"] == 2
    r = pg.execute("SELECT veh_id, section_id, traversal_sec, kst_hour, day_type FROM mart.bus_section_time ORDER BY section_id").fetchall()
    assert r == [("V1", "A", 180, 9, "weekday"), ("V1", "B", 180, 9, "weekday")]
    M.build_bus_section_time(d, d, connect=conn_of(pg))
    assert pg.execute("SELECT count(*) FROM mart.bus_section_time").fetchone()[0] == 2                        # 멱등


def test_section_slowness_ratio_needs_enough_baseline(pg):
    t = datetime(2026, 11, 2, 9, 0, tzinfo=KST)
    for i in range(12):
        pg.execute("INSERT INTO mart.bus_section_time VALUES ('R1',%s,'A',1,%s,%s,%s,'2026-11-02',9,'weekday')",
                   (f"V{i}", t + timedelta(minutes=i), t + timedelta(minutes=i, seconds=100), 100))
    pg.execute("INSERT INTO mart.bus_section_time VALUES ('R1','VX','A',1,%s,%s,200,'2026-11-02',9,'weekday')",
               (t + timedelta(hours=0, minutes=30), t + timedelta(minutes=33)))
    pg.execute("INSERT INTO mart.bus_section_time VALUES ('R1','VY','B',2,%s,%s,300,'2026-11-02',9,'weekday')",
               (t, t + timedelta(minutes=5)))
    r = {x[0]: x[1:] for x in pg.execute("SELECT veh_id, traversal_sec, ratio_to_base, base_n_obs, base_provisional "
                                         "FROM mart.v_bus_section_slowness WHERE veh_id IN ('VX','VY')").fetchall()}
    assert r["VX"] == (200, 2.0, 13, True)                 # 기준 중앙값 100 → 2배
    assert r["VY"][1] is None                              # 구간 B 는 표본 1개 → 비율 계산 안 함


# ------------------------------------------------------------------ 선행 조건 · 권한

def _load(pg, status="ok", d0=date(2026, 11, 3), d1=date(2026, 11, 4), age_h=1):
    pg.execute("INSERT INTO ops.load_run (job, date_from, date_to, started_at, finished_at, status) "
               "VALUES ('gold_load', %s, %s, now() - make_interval(hours => %s), now() - make_interval(hours => %s), %s)",
               (d0, d1, age_h + 1, age_h, status))


def test_require_recent_load(pg):
    w = (date(2026, 11, 3), date(2026, 11, 4))
    with pytest.raises(JobError, match="성공한 적재"):
        M.require_recent_load(*w, connect=conn_of(pg))                                   # 기록 없음
    _load(pg, status="failed")
    _load(pg, d0=date(2026, 11, 4), d1=date(2026, 11, 4))                                # 창을 덮지 못함
    _load(pg, age_h=48)                                                                  # 너무 오래됨
    with pytest.raises(JobError):
        M.require_recent_load(*w, connect=conn_of(pg))
    _load(pg)
    assert isinstance(M.require_recent_load(*w, connect=conn_of(pg)), int)


def test_roles_can_read_marts_and_only_writer_writes(pg):
    pg.execute("INSERT INTO mart.congestion_threshold (version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days, sufficient, provisional) "
               "VALUES ('2026-11-04','L1','weekday',8,1,1,1,false,true)")
    try:
        for role in ("api_reader", "bi_reader"):
            pg.execute(f"SET ROLE {role}")
            for rel in ("congestion_threshold", "bus_arrival_drift", "bus_section_time", "v_congestion_threshold_latest",
                        "v_traffic_level", "v_bus_drift_daily", "v_bus_section_slowness"):
                pg.execute(f"SELECT count(*) FROM mart.{rel}").fetchone()
            with pytest.raises(Exception):
                pg.execute("DELETE FROM mart.congestion_threshold")
            pg.execute("RESET ROLE")
        pg.execute("SET ROLE bi_reader")
        with pytest.raises(Exception):                                                    # BI 는 fact 직접 접근 불가(기존 규칙 유지)
            pg.execute("SELECT 1 FROM fact.traffic_speed LIMIT 1")
        pg.execute("RESET ROLE")
        pg.execute("SET ROLE gold_writer")
        pg.execute("DELETE FROM mart.congestion_threshold")
        pg.execute("RESET ROLE")
    finally:
        pg.execute("RESET ROLE")
