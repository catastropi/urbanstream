"""Step 8c: 로버스트 z 이상 탐지를 실제 Postgres 에서 확인한다 (기준선 칸 없음·IQR=0·잠정 기준선·표본 부족·재실행·권한)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from batch import anomaly as A
from batch.jobs import JobError

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
UNTIL = datetime(2026, 11, 4, 9, 0, tzinfo=KST)           # 수요일 09:00 → 08시대 관측을 본다


def conn_of(pg):
    class X:
        def __enter__(s): return pg
        def __exit__(s, *a): return False
    return lambda: X()


def put(pg, link, day, hour, minute, speed):
    ev = datetime(day.year, day.month, day.day, hour, minute, tzinfo=KST)
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, event_date) VALUES (%s,%s,%s,%s,%s,60,%s)",
               (f"{link}-{ev.isoformat()}", link, ev, ev, speed, day))


WEDS_MATURE = [date(2026, 9, 30), date(2026, 10, 7), date(2026, 10, 14), date(2026, 10, 21), date(2026, 10, 28)]     # span 29일 → 잠정 아님
WEDS_SHORT = [date(2026, 10, 21), date(2026, 10, 28)]


def history(pg, link, days, speeds, per_day):
    for d in days:
        for i in range(per_day):
            put(pg, link, d, 8, i * 5, speeds[i % len(speeds)])


def build(pg):
    history(pg, "L1", WEDS_MATURE, [38, 40, 42, 44, 36], 5)       # 25건, p25=38 p50=40 p75=42 → IQR 4
    history(pg, "L2", WEDS_SHORT, [38, 40, 42, 44, 36], 12)       # 24건이지만 22일 미만 span → provisional
    history(pg, "L3", WEDS_MATURE, [30], 5)                       # 전부 30 → IQR 0
    history(pg, "L5", [WEDS_MATURE[0], WEDS_MATURE[-1]], [38, 40, 42, 44, 36], 5)   # 10건 (<20) 이지만 span 29일
    pg.execute("SELECT mart.refresh_all()")


def obs(pg):
    for link, sp in (("L1", 20.0), ("L2", 20.0), ("L3", 25.0), ("L4", 20.0), ("L5", 20.0)):
        put(pg, link, date(2026, 11, 4), 8, 30, sp)


def scan(pg, sent=None, **kw):
    sent = sent if sent is not None else []
    out = A.scan(UNTIL, connect=conn_of(pg), send=lambda *a, **k: sent.append((a, k)), now=UNTIL, **kw)
    return out, sent


def test_statuses_for_each_baseline_situation(pg):
    build(pg); obs(pg)
    out, sent = scan(pg)
    rows = {r[0]: r[1:] for r in pg.execute("SELECT link_id, status, direction, robust_z, n_obs, provisional FROM mart.anomaly_event ORDER BY link_id")}
    assert rows["L1"][0] == "anomaly" and rows["L1"][1] == "slow" and rows["L1"][2] == pytest.approx((20 - 40) / (4 / 1.349), abs=0.01)
    assert rows["L2"][0] == "candidate" and rows["L2"][4] is True                 # 잠정 기준선 → 알림 후보 아님
    assert rows["L3"][0] == "degenerate_baseline" and rows["L3"][2] is None       # IQR=0 → z 없음
    assert rows["L5"][0] == "candidate" and rows["L5"][3] == 10                   # 표본 부족
    assert "L4" not in rows                                                       # 기준선 칸 없음 → 행 없음
    assert out["scanned"] == 5 and out["no_baseline"] == 1
    assert out["new"] == {"anomaly": 1, "candidate": 2, "degenerate_baseline": 1}
    assert len(sent) == 1 and sent[0][0][0] == "warning" and "L1" in sent[0][0][2] and sent[0][1]["dedup_key"] == "anomaly:traffic_speed"


def test_rerun_is_idempotent_and_does_not_realert(pg):
    build(pg); obs(pg)
    scan(pg)
    out, sent = scan(pg)
    assert out["new"] == {"anomaly": 0, "candidate": 0, "degenerate_baseline": 0} and sent == []
    assert pg.execute("SELECT count(*) FROM mart.anomaly_event").fetchone()[0] == 4


def test_normal_values_make_no_rows(pg):
    build(pg)
    for link in ("L1", "L2", "L5"):
        put(pg, link, date(2026, 11, 4), 8, 30, 41.0)
    put(pg, "L3", date(2026, 11, 4), 8, 30, 30.0)               # IQR=0 이어도 중앙값과 같으면 이상이 아님
    out, sent = scan(pg)
    assert out["scanned"] == 4 and pg.execute("SELECT count(*) FROM mart.anomaly_event").fetchone()[0] == 0 and sent == []


def test_fast_direction_and_sigma_floor(pg):
    build(pg)
    put(pg, "L1", date(2026, 11, 4), 8, 30, 70.0)                # 평소 40 → 매우 빠름
    put(pg, "L1", date(2026, 11, 4), 8, 40, 43.0)                # 가벼운 이탈 z≈1 → 이상 아님
    out, _ = scan(pg)
    assert pg.execute("SELECT direction, status FROM mart.anomaly_event").fetchall() == [("fast", "anomaly")]


def test_empty_fact_and_unpopulated_baseline(pg):
    with pytest.raises(JobError):
        scan(pg)                                                  # 기준선이 한 번도 채워지지 않음
    history(pg, "L1", WEDS_MATURE, [38, 40, 42, 44, 36], 5)
    pg.execute("SELECT mart.refresh_all()")
    out, sent = scan(pg)                                          # 최근 관측이 없음 → 오류 없이 0건
    assert out["scanned"] == 0 and sent == []


def test_old_observations_outside_window_are_ignored(pg):
    build(pg)
    put(pg, "L1", date(2026, 11, 4), 5, 0, 5.0)                   # 5시 관측 — 3시간 창(06:00~09:00) 밖
    out, _ = scan(pg)
    assert out["scanned"] == 0


def test_null_speed_is_skipped(pg):
    build(pg)
    ev = datetime(2026, 11, 4, 8, 30, tzinfo=KST)
    pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, event_date) VALUES ('n','L1',%s,%s,NULL,%s)", (ev, ev, date(2026, 11, 4)))
    out, _ = scan(pg)
    assert out["scanned"] == 0


def test_roles_writer_runs_scan_readers_read_only(pg):
    build(pg); obs(pg)
    pg.execute("SET ROLE gold_writer")
    try:
        scan(pg)
    finally:
        pg.execute("RESET ROLE")
    for role in ("api_reader", "bi_reader"):
        pg.execute(f"SET ROLE {role}")
        try:
            assert pg.execute("SELECT count(*) FROM mart.v_anomaly_recent").fetchone()[0] in (0, 4)     # 7일 창: 합성 날짜가 과거라 0 일 수 있다
            assert pg.execute("SELECT count(*) FROM mart.anomaly_event").fetchone()[0] == 4
            with pytest.raises(Exception):
                pg.execute("DELETE FROM mart.anomaly_event")
        finally:
            pg.execute("RESET ROLE")


def test_sigma_floor_prevents_tiny_iqr_from_flagging_small_deviations(pg):
    history(pg, "L6", WEDS_MATURE, [40, 40, 40, 40.5, 40.5], 5)       # IQR = 0.5 (아주 좁음)
    pg.execute("SELECT mart.refresh_all()")
    put(pg, "L6", date(2026, 11, 4), 8, 30, 45.0)                     # 5km/h 이탈: 하한(2.0) 덕에 z=2.5 → 이상 아님 (하한이 없으면 z≈13)
    out, sent = scan(pg)
    assert out["scanned"] == 1 and pg.execute("SELECT count(*) FROM mart.anomaly_event").fetchone()[0] == 0 and sent == []
