from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pg_support import pg  # noqa: F401

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))


def _ins(pg, key, link, t, speed):
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, event_date) "
               "VALUES (%s,%s,%s,%s,%s,100,%s)", (key, link, t, t, speed, t.astimezone(KST).date()))


def test_baseline_median_is_per_link_dow_and_kst_hour_and_provisional(pg):
    # 2026-11-02 월(ISO 1) KST 08시: 속도 10,20,30 → 중앙값 20 / 같은 시각 다른 링크·다른 시간대는 섞이지 않는다
    for i, sp in enumerate([10, 20, 30]):
        _ins(pg, f"a{i}", "L1", datetime(2026, 11, 2, 8, 10 * i, tzinfo=KST), sp)
    _ins(pg, "b", "L1", datetime(2026, 11, 2, 9, 0, tzinfo=KST), 99)
    _ins(pg, "c", "L2", datetime(2026, 11, 2, 8, 0, tzinfo=KST), 77)
    _ins(pg, "n", "L1", datetime(2026, 11, 2, 8, 40, tzinfo=KST), None)       # 속도 NULL 은 제외
    pg.execute("SELECT mart.refresh_all()")
    r = pg.execute("SELECT link_id, iso_dow, kst_hour, n_obs, speed_p50, provisional FROM mart.mv_link_baseline ORDER BY 1,3").fetchall()
    assert r == [("L1", 1, 8, 3, 20.0, True), ("L1", 1, 9, 1, 99.0, True), ("L2", 1, 8, 1, 77.0, True)]
    pg.execute("SELECT mart.refresh_all()")        # 두 번째는 CONCURRENTLY 경로


def test_kst_hour_boundary_uses_korea_time_not_utc(pg):
    _ins(pg, "x", "L1", datetime(2026, 11, 2, 23, 30, tzinfo=timezone.utc), 40)    # UTC 23:30 = KST 다음날(화) 08:30
    pg.execute("SELECT mart.refresh_all()")
    assert pg.execute("SELECT iso_dow, kst_hour FROM mart.mv_link_baseline").fetchone() == (2, 8)


def test_latest_view_ratio_and_episode_view(pg):
    for wk in range(3):                                   # 3주 같은 월요일 08시: 40 km/h
        _ins(pg, f"w{wk}", "L1", datetime(2026, 10, 12 + 7 * wk, 8, 0, tzinfo=KST), 40)
    _ins(pg, "now", "L1", datetime(2026, 11, 2, 8, 0, tzinfo=KST), 20)
    pg.execute("SELECT mart.refresh_all()")
    link, ratio, prov = pg.execute("SELECT link_id, ratio_to_baseline, baseline_provisional FROM mart.v_traffic_latest").fetchone()
    assert (link, prov) == ("L1", True) and 0.4 < ratio < 0.6          # 기준선에 자기 자신도 포함되어 (40*3+20)/4 가 아니라 중앙값 40 → 0.5
    pg.execute("INSERT INTO fact.incident_episode VALUES ('A', now(), now(), now(), NULL, NULL, 1, false), ('B', now(), now(), now(), now(), NULL, 1, false)")
    got = dict(pg.execute("SELECT acc_id, still_listed FROM mart.v_incident_episode").fetchall())
    assert got == {"A": True, "B": False}


def test_traffic_range_query_uses_link_time_index(pg):
    pg.execute("SET enable_seqscan = off")
    plan = "\n".join(r[0] for r in pg.execute("EXPLAIN SELECT * FROM fact.traffic_speed WHERE link_id='L1' AND event_time > now() - interval '1 day'").fetchall())
    assert "traffic_speed_link_time_ix" in plan
