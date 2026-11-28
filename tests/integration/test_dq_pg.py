"""Step 8b: DQ 검사·기록·알림·게이트를 실제 Postgres 에서 확인한다 (합성 데이터에 빈 경우 포함, 역할은 SET ROLE)."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from urbanstream.dq import checks as C, engine, rules as R
from batch import dq_jobs
from batch.jobs import JobError, no_lock

pytestmark = pytest.mark.pg
KST = timezone(timedelta(hours=9))
D = date(2026, 11, 4)


def conn_of(pg):
    class X:
        def __enter__(s): return pg
        def __exit__(s, *a): return False
    return lambda: X()


def traffic(pg, d=D, hours=range(24), per_hour=6, speed=30.0, null_every=0, future_sec=0, link="L1"):
    n = 0
    for h in hours:
        for i in range(per_hour):
            ev = datetime(d.year, d.month, d.day, h, i * 5, tzinfo=KST)
            sp = None if (null_every and n % null_every == 0) else speed
            pg.execute("INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, speed_kmh, travel_time_sec, dq_flags, event_date) "
                       "VALUES (%s,%s,%s,%s,%s,60,%s,%s)", (f"{link}-{d}-{h}-{i}-{future_sec}", link, ev + timedelta(seconds=future_sec), ev, sp,
                                                           ["speed_zero"] if sp == 0 else [], d))
            n += 1
    return n


def one(rule_dict, d=D):
    base = {"severity": "warning", "action": "alert"}
    rs = R.parse({"shadow_until": "2020-01-01", "rules": [{**base, **rule_dict}]})
    return rs


def run(pg, rule_dict, d=D, today=date(2026, 11, 20)):
    rs = one(rule_dict)
    return engine.run_rules(pg, rs, d, today=today)


NULL = {"id": "r", "kind": "null_rate", "table": "fact.traffic_speed", "date_col": "event_date", "column": "speed_kmh", "max_rate": 0.1, "min_rows": 50}


def test_empty_table_is_unknown_not_pass_for_every_kind(pg):
    cases = [NULL, {"id": "a", "kind": "range", "table": "fact.traffic_speed", "date_col": "event_date", "column": "speed_kmh", "min": 0, "max": 150, "max_out_rate": 0, "min_rows": 10},
             {"id": "b", "kind": "flag_rate", "table": "fact.traffic_speed", "date_col": "event_date", "flag": "speed_zero", "max_rate": 0.3, "min_rows": 10},
             {"id": "c", "kind": "future_time", "table": "fact.traffic_speed", "date_col": "event_date", "ts_col": "event_time", "fetched_col": "fetched_at", "margin_sec": 600, "max_count": 0},
             {"id": "d", "kind": "hour_coverage", "table": "fact.traffic_speed", "date_col": "event_date", "ts_col": "event_time", "hours": [0, 23], "min_hours": 22},
             {"id": "e", "kind": "volume_drift", "table": "fact.traffic_speed", "date_col": "event_date", "baseline_days": 7, "min_baseline_days": 3, "low": 0.5, "high": 2},
             {"id": "f", "kind": "ref_integrity", "child_table": "fact.bus_position", "child_col": "next_stop_id", "parent_table": "fact.bus_arrival", "parent_col": "station_id", "date_col": "event_date", "max_orphan_rate": 0.2, "min_rows": 5},
             {"id": "g", "kind": "mart_reliability", "mart_table": "mart.v_congestion_threshold_latest", "flag_col": "provisional", "source_table": "fact.traffic_speed", "source_date_col": "event_date", "mature_after_days": 28},
             {"id": "h", "kind": "load_health", "dag_id": "gold_intraday", "max_age_hours": 3, "stale_running_hours": 2},
             {"id": "i", "kind": "call_fail_rate", "max_rate": 0.3, "min_calls": 5},
             {"id": "j", "kind": "incident_link_rate", "min_rows": 5},
             {"id": "k", "kind": "table_bloat", "max_ratio": 0.2, "min_live_rows": 100}]
    for c in cases:
        rows = run(pg, c)
        assert rows and all(x.result.status in ("unknown", "pass") for x in rows), c["id"]
    # 데이터에 의존하는 종류는 빈 경우에 pass 가 하나도 없어야 한다 (load_health 의 stale_running=0 은 '자료 없음'과 무관한 사실이라 예외)
    for c in cases:
        if c["kind"] in ("load_health",):
            continue
        assert all(x.result.status == "unknown" for x in run(pg, c)), c["id"]


def test_null_rate_pass_fail_and_min_rows(pg):
    traffic(pg, null_every=0)
    assert run(pg, NULL)[0].result.status == "pass"
    pg.execute("DELETE FROM fact.traffic_speed")
    traffic(pg, null_every=4)                                    # 25% 결측
    r = run(pg, NULL)[0].result
    assert r.status == "fail" and r.value == pytest.approx(0.25, abs=0.01)
    pg.execute("DELETE FROM fact.traffic_speed")
    traffic(pg, hours=[1], per_hour=3, null_every=1)             # 3행 전부 결측이어도 표본 부족 → unknown
    assert run(pg, NULL)[0].result.status == "unknown"


def test_range_flag_and_future_time(pg):
    traffic(pg, speed=30.0)
    rng = {"id": "rg", "kind": "range", "table": "fact.traffic_speed", "date_col": "event_date", "column": "speed_kmh", "min": 0, "max": 150, "max_out_rate": 0, "min_rows": 10}
    assert run(pg, rng)[0].result.status == "pass"
    pg.execute("UPDATE fact.traffic_speed SET speed_kmh = 999 WHERE dedup_key LIKE 'L1-%-5-0-0'")
    assert run(pg, rng)[0].result.status == "fail"
    fut = {"id": "ft", "kind": "future_time", "table": "fact.traffic_speed", "date_col": "event_date", "ts_col": "event_time", "fetched_col": "fetched_at", "margin_sec": 600, "max_count": 0}
    assert run(pg, fut)[0].result.status == "pass"
    traffic(pg, hours=[3], per_hour=2, future_sec=3600, link="L2")      # 한 시간 미래
    r = run(pg, fut)[0].result
    assert r.status == "fail" and r.value == 2
    traffic(pg, d=D, hours=[4], per_hour=2, future_sec=300, link="L3")  # 5분 미래는 허용 오차(600초) 안
    assert run(pg, fut)[0].result.value == 2


def test_hour_coverage_explained_by_data_gap_is_warn_not_fail(pg):
    cov = {"id": "cv", "kind": "hour_coverage", "table": "fact.traffic_speed", "date_col": "event_date", "ts_col": "event_time", "hours": [0, 23], "min_hours": 22}
    traffic(pg, hours=[h for h in range(24) if h not in (10, 11, 12)])             # 3시간 빠짐
    assert run(pg, cov)[0].result.status == "fail"
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', %s, %s, 10800)",
               (datetime(2026, 11, 4, 10, 0, tzinfo=KST), datetime(2026, 11, 4, 13, 0, tzinfo=KST)))
    r = run(pg, cov)[0].result
    assert r.status == "warn" and r.detail["explained_by_data_gap"] is True
    # 겹치는 공백을 두 번 세지 않는다
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('collector_blackout', %s, %s, 3600)",
               (datetime(2026, 11, 4, 10, 30, tzinfo=KST), datetime(2026, 11, 4, 11, 30, tzinfo=KST)))
    assert C.gap_hours_on(pg, D) == pytest.approx(3.0)
    # 전날 밤부터 이어진 공백은 그 날짜에 걸친 부분만 센다
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', %s, %s, 7200)",
               (datetime(2026, 11, 3, 23, 0, tzinfo=KST), datetime(2026, 11, 4, 1, 0, tzinfo=KST)))
    assert C.gap_hours_on(pg, D) == pytest.approx(4.0)


def test_volume_drift_short_baseline_unknown_then_pass_and_fail(pg):
    vd = {"id": "vd", "kind": "volume_drift", "table": "fact.traffic_speed", "date_col": "event_date", "baseline_days": 7, "min_baseline_days": 3, "low": 0.5, "high": 2.0}
    traffic(pg, d=D)
    assert run(pg, vd)[0].result.status == "unknown"             # 기준 일수 부족
    for k in (1, 2, 3):
        traffic(pg, d=D - timedelta(days=k))
    assert run(pg, vd)[0].result.status == "pass"
    pg.execute("DELETE FROM fact.traffic_speed WHERE event_date = %s AND extract(hour FROM event_time AT TIME ZONE 'Asia/Seoul') >= 6", (D,))
    r = run(pg, vd)[0].result
    assert r.status == "fail" and r.value < 0.5                  # 공백 기록이 없으면 fail
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', %s, %s, 64800)",
               (datetime(2026, 11, 4, 6, 0, tzinfo=KST), datetime(2026, 11, 5, 0, 0, tzinfo=KST)))
    assert run(pg, vd)[0].result.status == "warn"


def test_ref_integrity_and_empty_parent(pg):
    ri = {"id": "ri", "kind": "ref_integrity", "child_table": "fact.bus_position", "child_col": "next_stop_id", "parent_table": "fact.bus_arrival", "parent_col": "station_id", "date_col": "event_date", "max_orphan_rate": 0.2, "min_rows": 5}
    for i in range(10):
        pg.execute("INSERT INTO fact.bus_position (dedup_key, veh_id, event_time, fetched_at, next_stop_id, event_date) VALUES (%s,'v',%s,%s,%s,%s)",
                   (f"p{i}", datetime(2026, 11, 4, 9, i, tzinfo=KST), datetime(2026, 11, 4, 9, i, tzinfo=KST), "S1" if i < 6 else "SX", D))
    assert run(pg, ri)[0].result.status == "unknown"             # 부모가 비어 있음
    pg.execute("INSERT INTO fact.bus_arrival (dedup_key, route_id, station_id, station_order, slot, veh_id, event_time, fetched_at, event_date) "
               "VALUES ('a','R','S1',1,1,'v',%s,%s,%s)", (datetime(2026, 11, 4, 9, tzinfo=KST), datetime(2026, 11, 4, 9, tzinfo=KST), D))
    r = run(pg, ri)[0].result
    assert r.status == "fail" and r.value == pytest.approx(0.4)   # 10건 중 4건이 고아


def test_mart_reliability_expected_state(pg):
    mr = {"id": "mr", "kind": "mart_reliability", "mart_table": "mart.v_congestion_threshold_latest", "flag_col": "provisional", "source_table": "fact.traffic_speed", "source_date_col": "event_date", "mature_after_days": 28}
    traffic(pg, d=D)
    pg.execute("INSERT INTO mart.congestion_threshold (version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days, sufficient, provisional) VALUES ('2026-11-04','L1','weekday',8,1,1,1,false,true)")
    r = run(pg, mr)[0].result
    assert r.status == "pass" and r.detail["expected_state"] == "provisional"      # 1일치 → provisional 이 정상
    traffic(pg, d=D - timedelta(days=30), hours=[1], per_hour=1)
    r = run(pg, mr)[0].result
    assert r.status == "warn" and r.detail["expected_state"] == "mature"           # 31일치인데 아직 provisional → 경고


def test_load_health_and_stale_running(pg):
    lh = {"id": "lh", "kind": "load_health", "dag_id": "gold_intraday", "max_age_hours": 3, "stale_running_hours": 2}
    pg.execute("INSERT INTO ops.load_run (job, date_from, date_to, dag_id, status, started_at, finished_at) VALUES ('gold_load', %s, %s, 'gold_intraday', 'ok', now()-interval '1 hour', now()-interval '1 hour')", (D, D))
    st = {x.result.target: x.result.status for x in run(pg, lh)}
    assert st == {"last_ok": "pass", "stale_running": "pass"}
    pg.execute("INSERT INTO ops.load_run (job, date_from, date_to, dag_id, status, started_at) VALUES ('gold_load', %s, %s, 'gold_intraday', 'running', now()-interval '5 hours')", (D, D))
    pg.execute("UPDATE ops.load_run SET finished_at = now() - interval '6 hours' WHERE status='ok'")
    st = {x.result.target: x.result.status for x in run(pg, lh)}
    assert st == {"last_ok": "fail", "stale_running": "fail"}


def test_call_fail_rate_per_source_and_table_bloat(pg):
    cf = {"id": "cf", "kind": "call_fail_rate", "max_rate": 0.3, "min_calls": 5}
    for i in range(10):
        pg.execute("INSERT INTO ops.call_event (kafka_partition, kafka_offset, recorded_at, event, source_system, final, outcome) VALUES (0,%s,%s,'call','a',true,%s)",
                   (i, datetime(2026, 11, 4, 9, i, tzinfo=KST), "failed" if i < 5 else "ok"))
        pg.execute("INSERT INTO ops.call_event (kafka_partition, kafka_offset, recorded_at, event, source_system, final, outcome) VALUES (0,%s,%s,'call','b',true,'ok')",
                   (100 + i, datetime(2026, 11, 4, 9, i, tzinfo=KST)))
    pg.execute("INSERT INTO ops.call_event (kafka_partition, kafka_offset, recorded_at, event, source_system, final, outcome) VALUES (0,500,%s,'call','c',true,'failed')", (datetime(2026, 11, 4, 9, tzinfo=KST),))
    st = {x.result.target: x.result.status for x in run(pg, cf)}
    assert st == {"a": "fail", "b": "pass", "c": "unknown"}      # c 는 호출 1건뿐 → 표본 부족
    tb = {"id": "tb", "kind": "table_bloat", "max_ratio": 0.2, "min_live_rows": 1000}
    pg.execute("INSERT INTO ops.table_stats (snapshot_date, schema_name, table_name, n_live_est, n_dead_est, total_bytes) VALUES ('2026-11-01','fact','t',5000,2000,1),('2026-11-01','fact','small',10,10,1),('2026-11-01','fact','ok',5000,100,1)")
    st = {x.result.target: x.result.status for x in run(pg, tb)}
    assert st == {"fact.t": "fail", "fact.ok": "pass"}


def test_incident_link_rate_is_info_pass_with_enough_rows(pg):
    il = {"id": "il", "kind": "incident_link_rate", "min_rows": 3}
    for i in range(4):
        pg.execute("INSERT INTO mart.incident_impact (acc_id, occurred_at, first_seen_at, absent_since, duration_min, match_kind, sufficient, provisional) VALUES (%s, now(), now(), now(), 5, %s, false, true)",
                   (f"a{i}", "no_link" if i else "link_id"))
    r = run(pg, il)[0].result
    assert r.status == "pass" and r.value == pytest.approx(0.25)


def test_one_broken_rule_does_not_stop_others_and_records_unknown(pg):
    rs = R.parse({"shadow_until": "2020-01-01", "rules": [
        {"id": "broken", "kind": "null_rate", "severity": "warning", "action": "alert", "table": "fact.no_such_table", "date_col": "event_date", "column": "x", "max_rate": 0.1, "min_rows": 1},
        {**NULL, "severity": "warning", "action": "alert"}]})
    traffic(pg)
    rows = engine.run_rules(pg, rs, D, today=date(2026, 11, 20))
    st = {x.rule.id: x.result for x in rows}
    assert st["broken"].status == "unknown" and st["broken"].detail["reason"] == "check_error" and st["r"].status == "pass"


def test_record_is_idempotent_and_alert_rules(pg):
    rs = R.parse({"shadow_until": "2026-11-12", "rules": [
        {**NULL, "id": "w", "severity": "warning", "action": "alert"},
        {**NULL, "id": "e", "severity": "error", "action": "alert"},
        {**NULL, "id": "rec", "severity": "error", "action": "record"},
        {**NULL, "id": "info", "severity": "info", "action": "alert"}]})
    traffic(pg, null_every=2)                                     # 50% 결측 → 전부 fail
    sent = []
    send = lambda level, title, body, dedup_key=None: sent.append((level, dedup_key))
    shadow_rows = engine.run_rules(pg, rs, D, today=date(2026, 11, 10))                 # 관찰 기간
    engine.record(pg, shadow_rows); engine.record(pg, shadow_rows)
    assert engine.alert(shadow_rows, send) == 0 and sent == []                          # 관찰 중에는 알리지 않는다
    assert pg.execute("SELECT count(*), bool_and(shadow) FROM ops.dq_result").fetchone() == (4, True)
    live_rows = engine.run_rules(pg, rs, D, today=date(2026, 11, 20))
    engine.record(pg, live_rows)
    assert pg.execute("SELECT count(*), bool_or(shadow) FROM ops.dq_result").fetchone() == (4, False)      # 같은 날 재실행 = 교체
    assert engine.alert(live_rows, send) == 2
    assert sorted(sent) == [("error", "dq:e:"), ("warning", "dq:w:")]                   # record·info 는 알리지 않는다


def test_gate_blocks_only_enforced_block_mart_failures(pg):
    shadow_fail = ("INSERT INTO ops.dq_result (run_date, rule_id, kind, target, severity, action, status, shadow) VALUES (%s,%s,'future_time','t','error','block_mart','fail',%s)")
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0                 # 결과 없음 → 통과
    pg.execute(shadow_fail, (date.today(), "traffic_future_event", True))
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0                 # 관찰 기간 → 통과
    pg.execute("DELETE FROM ops.dq_result")
    pg.execute(shadow_fail, (date.today(), "traffic_future_event", False))
    with pytest.raises(JobError) as e:
        dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg))
    assert "traffic_future_event" in str(e.value)
    pg.execute("INSERT INTO ops.dq_result (run_date, rule_id, kind, target, severity, action, status, shadow) VALUES (%s,'other','null_rate','','error','alert','fail',false)", (date.today(),))
    pg.execute("DELETE FROM ops.dq_result WHERE rule_id = 'traffic_future_event'")
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0                 # block_mart 가 아닌 fail 은 막지 않는다
    pg.execute(shadow_fail, (date.today() - timedelta(days=5), "traffic_future_event", False))
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0                 # 오래된 결과는 막지 않는다
    pg.execute("DELETE FROM ops.dq_result")
    pg.execute(shadow_fail, (date.today() - timedelta(days=1), "traffic_future_event", False))
    pg.execute("INSERT INTO ops.dq_result (run_date, rule_id, kind, target, severity, action, status, shadow) VALUES (%s,'traffic_future_event','future_time','t','error','block_mart','pass',false)", (date.today(),))
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0                 # 고쳐져서 최근 결과가 pass 면 풀린다


def test_gate_is_inert_when_table_missing(pg):
    pg.execute("DROP VIEW ops.v_dq_summary; DROP VIEW ops.v_dq_latest; DROP TABLE ops.dq_result")
    assert dq_jobs.require_dq_ok("bus_delay", connect=conn_of(pg)) == 0


def test_run_daily_report_end_to_end_as_gold_writer(pg):
    traffic(pg)
    sent = []
    pg.execute("SET ROLE gold_writer")
    try:
        out = dq_jobs.run_daily_report(D, connect=conn_of(pg), send=lambda *a, **k: sent.append(a), today=date(2026, 11, 20))
    finally:
        pg.execute("RESET ROLE")
    assert out["checks"] >= 20 and out["pass"] >= 3 and out["unknown"] >= 5 and out["shadow"] is False
    assert pg.execute("SELECT count(*) FROM ops.dq_result WHERE run_date = %s", (D,)).fetchone()[0] == out["checks"]


def test_readers_see_dq_views_not_raw_table(pg):
    pg.execute("INSERT INTO ops.dq_result (run_date, rule_id, kind, severity, action, status, shadow) VALUES (%s,'x','null_rate','info','record','pass',true)", (D,))
    for role in ("api_reader", "bi_reader"):
        pg.execute(f"SET ROLE {role}")
        try:
            assert pg.execute("SELECT count(*) FROM ops.v_dq_latest").fetchone()[0] == 1
            assert pg.execute("SELECT n_pass FROM ops.v_dq_summary").fetchone()[0] == 1
            with pytest.raises(Exception):
                pg.execute("SELECT 1 FROM ops.dq_result")
        finally:
            pg.execute("RESET ROLE")


def test_silver_reconcile_records_per_table_and_alerts_on_fail(pg):
    class P:
        returncode = 1
        stdout = "silver_traffic_speed         OK   Bronze 10행\nsilver_weather               FAIL Bronze 3행 유실 2\n"
        stderr = ""
    sent = []
    out = dq_jobs.run_silver_reconcile(lock=no_lock, mode="inline", connect=conn_of(pg), runner=lambda *a, **k: P(), send=lambda *a, **k: sent.append(a), run_date=D)
    assert out["pass"] == 1 and out["fail"] == 1
    assert pg.execute("SELECT target, status FROM ops.dq_result WHERE rule_id='silver_reconcile' ORDER BY target").fetchall() == [("silver_traffic_speed", "pass"), ("silver_weather", "fail")]
    assert len(sent) == 1 and sent[0][0] == "error"
    P.stdout, P.returncode = "", 2
    with pytest.raises(JobError):
        dq_jobs.run_silver_reconcile(lock=no_lock, mode="inline", connect=conn_of(pg), runner=lambda *a, **k: P(), send=lambda *a, **k: None, run_date=D)


def test_every_real_rule_runs_as_gold_writer_without_check_error(pg):
    """실제 config/dq_rules.yaml 의 모든 규칙을 gold_writer 로 돌려, 권한 오류가 'unknown' 으로 묻히지 않는지 본다 (Mac 에서 발견: v_congestion_threshold_latest 권한)."""
    traffic(pg)
    pg.execute("SET ROLE gold_writer")
    try:
        rows = engine.run_rules(pg, R.load(), D, today=date(2026, 11, 20))
    finally:
        pg.execute("RESET ROLE")
    errs = [(x.rule.id, x.result.detail.get("error")) for x in rows if x.result.detail.get("reason") == "check_error"]
    assert errs == []


def test_gold_writer_can_read_every_mart_view(pg):
    """새 mart 뷰를 추가하고 gold_writer 권한을 빠뜨리면 여기서 잡힌다."""
    views = [r[0] for r in pg.execute("SELECT viewname FROM pg_views WHERE schemaname = 'mart'").fetchall()]
    assert views
    pg.execute("REFRESH MATERIALIZED VIEW mart.mv_link_baseline")      # 비어 있는 MV 는 읽기 자체가 실패하므로 권한 검사 전에 채운다
    pg.execute("SET ROLE gold_writer")
    try:
        for v in views:
            pg.execute(f"SELECT count(*) FROM mart.{v}").fetchone()
    finally:
        pg.execute("RESET ROLE")


def test_silver_reconcile_in_runner_mode_reads_table_lines_from_the_runner_output(pg, tmp_path):
    """STEP 3B: 기본(runner) 모드에서는 scheduler 가 Spark 를 띄우지 않고 gold-runner 요청만 남긴다. 대조 실패(코드 1)도 결과로 기록된다."""
    from batch import stage_runner as SR
    asked = []

    def request(kind, d0, d1, sd, *, args, base, timeout_sec):
        asked.append((kind, d0, args, sd))
        req = SR.requests_dir(base)
        req.mkdir(parents=True, exist_ok=True)
        SR.marker(req, "v1", "out").write_text("silver_traffic_speed         OK   Bronze 10행\nsilver_weather               FAIL 유실 2\n",
                                               encoding="utf-8")
        sd.mkdir(parents=True)
        return {"returncode": 1, "stdout_tail": "", "_id": "v1", "_stage_dir": str(sd)}
    out = dq_jobs.run_silver_reconcile(lock=no_lock, mode="runner", request=request, base=tmp_path, connect=conn_of(pg),
                                       send=lambda *a, **k: None, run_date=D)
    assert asked[0][0] == "silver_verify" and asked[0][2] == {"lookback_days": "1"}
    assert out["pass"] == 1 and out["fail"] == 1 and out["returncode"] == 1
    assert not asked[0][3].exists(), "빈 스테이징 폴더는 정리한다"
