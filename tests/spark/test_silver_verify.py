"""Silver 행 수 등식 검증(`silver_verify.compare`) 시험. Bronze/Silver 를 작은 배치 표로 만들어 일부러 어긋나게 한다."""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from streaming import silver as S  # noqa: E402
from streaming import silver_verify as V  # noqa: E402

from test_silver import at, bronze, table_of  # noqa: E402


def rows(n, *, t0=0, prefix="k", ingest="c"):
    return [{"dedup_key": f"{prefix}{i}", "event_time": at(t0 + i), "fetched_at": at(t0 + i + 5), "ingest_id": f"{ingest}{i}"} for i in range(n)]


def tail(prefix="tail"):
    """비교 기준 시각(Silver 마지막 fetched_at − 15분)을 앞으로 당기는 뒤쪽 행."""
    return {"dedup_key": prefix, "event_time": at(9000), "fetched_at": at(9000), "ingest_id": prefix}


def silver_of(b, source):
    t = table_of(source)
    return S.transform(b, t, S.SilverConfig()).dropDuplicates(list(t.dedup_keys))


def test_consistent_tables_pass_and_parse_failures_are_counted_by_reason(spark):
    b = bronze(spark, "bus_position", rows(4) + [{"dedup_key": "late", "event_time": at(5000), "fetched_at": at(5000), "ingest_id": "z"}],
               raw=["not json", "{}"])
    r = V.compare(b, silver_of(b, "bus_position"), table_of("bus_position"))
    assert r["ok"] and r["missing"] == 0 and r["extra"] == 0
    assert r["bronze_total"] == 7 and r["bronze_ok"] == 5 and sum(r["parse_failed"].values()) == 2 and r["bronze_total_check"]


def test_missing_key_in_silver_is_a_failure(spark):
    b = bronze(spark, "bus_position", rows(5) + [{"dedup_key": "tail", "event_time": at(9000), "fetched_at": at(9000), "ingest_id": "t"}])
    s = silver_of(b, "bus_position").filter(F.col("dedup_key") != "k2")
    r = V.compare(b, s, table_of("bus_position"))
    assert not r["ok"] and r["missing"] == 1 and r["extra"] == 0


def test_extra_key_in_silver_is_a_failure(spark):
    b = bronze(spark, "bus_position", rows(5) + [{"dedup_key": "tail", "event_time": at(9000), "fetched_at": at(9000), "ingest_id": "t"}])
    ghost = bronze(spark, "bus_position", [{"dedup_key": "ghost", "event_time": at(3), "fetched_at": at(8), "ingest_id": "g"}])
    s = silver_of(b, "bus_position").unionByName(silver_of(ghost, "bus_position"))
    r = V.compare(b, s, table_of("bus_position"))
    assert not r["ok"] and r["extra"] == 1 and r["missing"] == 0


def test_rows_newer_than_the_cutoff_are_not_expected_in_silver_yet(spark):
    old = rows(3)
    new = [{"dedup_key": "new", "event_time": at(3000), "fetched_at": at(3005), "ingest_id": "n"}]     # Silver 가 아직 처리 안 한 최신 행
    b = bronze(spark, "bus_position", old + new)
    s = silver_of(b, "bus_position").filter(F.col("dedup_key") != "new")
    assert V.compare(b, s, table_of("bus_position"))["ok"]


def test_readmitted_duplicate_is_info_for_watermark_tables_but_failure_for_incident(spark):
    pos = bronze(spark, "bus_position", rows(3) + [{"dedup_key": "k1", "event_time": at(1), "fetched_at": at(6), "ingest_id": "c1"}, tail()])
    s_pos = S.transform(pos, table_of("bus_position"), S.SilverConfig())                 # 중복 제거 없음 = k1 두 번
    r = V.compare(pos, s_pos, table_of("bus_position"))
    assert r["ok"] and r["dup_rows"] == 1

    inc = bronze(spark, "seoul_incident", rows(3) + [{"dedup_key": "k1", "event_time": at(1), "fetched_at": at(6), "ingest_id": "c1"}, tail()])
    s_inc = S.transform(inc, table_of("seoul_incident"), S.SilverConfig())
    r = V.compare(inc, s_inc, table_of("seoul_incident"))
    assert not r["ok"] and r["dup_rows"] == 1


def test_incident_same_incident_in_different_calls_is_not_a_duplicate(spark):
    inc = bronze(spark, "seoul_incident", [{"dedup_key": "acc", "event_time": at(0), "fetched_at": at(10 + 120 * i), "ingest_id": f"call{i}"} for i in range(3)] + [tail()])
    r = V.compare(inc, silver_of(inc, "seoul_incident"), table_of("seoul_incident"))
    assert r["ok"] and r["silver_rows"] == 3 and r["dup_rows"] == 0


def test_empty_silver_fails_only_when_bronze_has_good_rows(spark):
    b = bronze(spark, "bus_position", rows(2))
    empty = silver_of(b, "bus_position").limit(0)
    assert not V.compare(b, empty, table_of("bus_position"))["ok"]
    assert not V.compare(b, None, table_of("bus_position"))["ok"]
    only_bad = bronze(spark, "bus_position", [], raw=["not json"])
    assert V.compare(only_bad, None, table_of("bus_position"))["ok"]


def test_format_line_mentions_failure(spark):
    b = bronze(spark, "bus_position", rows(3) + [{"dedup_key": "tail", "event_time": at(9000), "fetched_at": at(9000), "ingest_id": "t"}])
    s = silver_of(b, "bus_position").filter(F.col("dedup_key") != "k1")
    line = V.format_line(V.compare(b, s, table_of("bus_position")))
    assert "FAIL" in line and "유실 1" in line


def test_time_checks_find_future_event_and_per_vehicle_reversal(spark):
    ok = bronze(spark, "bus_position", [
        {"dedup_key": "a1", "payload": {"veh_id": "A"}, "event_time": at(0), "fetched_at": at(5), "ingest_id": "1"},
        {"dedup_key": "a2", "payload": {"veh_id": "A"}, "event_time": at(60), "fetched_at": at(65), "ingest_id": "2"},
        {"dedup_key": "b1", "payload": {"veh_id": "B"}, "event_time": at(10), "fetched_at": at(15), "ingest_id": "1"}])
    assert V.time_checks(silver_of(ok, "bus_position"), "bus_position") == {"future_event": 0, "reversal": 0}
    bad = bronze(spark, "bus_position", [
        {"dedup_key": "a1", "payload": {"veh_id": "A"}, "event_time": at(60), "fetched_at": at(65), "ingest_id": "1"},
        {"dedup_key": "a2", "payload": {"veh_id": "A"}, "event_time": at(0), "fetched_at": at(125), "ingest_id": "2"},      # 같은 차량, 시각이 거꾸로
        {"dedup_key": "f", "payload": {"veh_id": "C"}, "event_time": at(500), "fetched_at": at(100), "ingest_id": "3"}])    # 미래 시각
    assert V.time_checks(silver_of(bad, "bus_position"), "bus_position") == {"future_event": 1, "reversal": 1}


def test_time_checks_do_not_count_other_vehicles_or_equal_times_as_reversal(spark):
    b = bronze(spark, "bus_position", [
        {"dedup_key": "a1", "payload": {"veh_id": "A"}, "event_time": at(60), "fetched_at": at(65), "ingest_id": "1"},
        {"dedup_key": "b1", "payload": {"veh_id": "B"}, "event_time": at(0), "fetched_at": at(70), "ingest_id": "2"},       # 다른 차량의 더 이른 시각
        {"dedup_key": "a1x", "payload": {"veh_id": "A"}, "event_time": at(60), "fetched_at": at(125), "ingest_id": "3"}])    # 같은 시각 반복
    assert V.time_checks(silver_of(b, "bus_position"), "bus_position")["reversal"] == 0


def test_incident_sample_lists_longest_first_with_converted_coordinates(spark):
    d = 86400
    b = bronze(spark, "seoul_incident", [
        {"dedup_key": "s", "payload": {"acc_id": "short", "occurred_at": at(0), "expected_clear_at": at(3600), "tm_x": 198000.0, "tm_y": 451500.0}, "ingest_id": "1"},
        {"dedup_key": "l", "payload": {"acc_id": "long", "occurred_at": at(0), "expected_clear_at": at(30 * d), "tm_x": 198000.0, "tm_y": 451500.0}, "ingest_id": "1"}])
    sample = V.incident_sample(silver_of(b, "seoul_incident"), n=2)
    assert [x["acc_id"] for x in sample] == ["long", "short"] and sample[0]["long"] is True
    assert abs(sample[0]["lat"] - 37.56303) < 1e-4


# ---------------------------------------------------------------- 비교 구간 (D-258)

def _old_ghost_case(spark):
    """11-20 의 정상 행들 + 11-03 에만 있는 Silver 초과 행(Bronze 의 복구 전 구간이 안 보이는 상황을 흉내)."""
    new = [{"dedup_key": f"n{i}", "event_time": at(i, day=20), "fetched_at": at(i + 5, day=20), "ingest_id": f"n{i}"} for i in range(4)]
    new.append({"dedup_key": "tail", "event_time": at(9000, day=20), "fetched_at": at(9000, day=20), "ingest_id": "t"})
    b = bronze(spark, "bus_position", new)
    ghost = bronze(spark, "bus_position", [{"dedup_key": "old", "event_time": at(0), "fetched_at": at(5), "ingest_id": "o"}])
    return b, silver_of(b, "bus_position").unionByName(silver_of(ghost, "bus_position"))


def test_window_excludes_old_silver_only_rows_that_fail_a_full_comparison(spark):
    from datetime import datetime
    b, s = _old_ghost_case(spark)
    t = table_of("bus_position")
    full = V.compare(b, s, t)
    assert not full["ok"] and full["extra"] == 1 and full["since"] is None
    win = V.compare(b, s, t, since=datetime(2026, 11, 10))
    assert win["ok"] and win["extra"] == 0 and win["bronze_total"] == 5 and win["since"] == datetime(2026, 11, 10)


def test_window_still_catches_a_missing_row_inside_the_window(spark):
    from datetime import datetime
    b, s = _old_ghost_case(spark)
    r = V.compare(b, s.filter(F.col("dedup_key") != "n2"), table_of("bus_position"), since=datetime(2026, 11, 10))
    assert not r["ok"] and r["missing"] == 1


def test_format_line_says_which_window_was_compared(spark):
    from datetime import datetime
    b, s = _old_ghost_case(spark)
    t = table_of("bus_position")
    assert "구간 전체 이력" in V.format_line(V.compare(b, s, t))
    assert "구간 fetched_at ≥ 2026-11-10 00:00:00Z" in V.format_line(V.compare(b, s, t, since=datetime(2026, 11, 10)))


# ---------------------------------------------------------------- 한 번 읽고 재사용 + 시간 검사에도 구간 적용 (D-258, 2026-11-19 2차)

def test_prewindowed_compare_gives_the_same_result_as_windowing_inside_compare(spark):
    from datetime import datetime
    b, s = _old_ghost_case(spark)
    t = table_of("bus_position")
    since = datetime(2026, 11, 19, 15, 0, 0)
    a = V.compare(b, s, t, since=since)
    cb, cs = V.cached(V.windowed(b, "event_date", since), since), V.cached(V.windowed(s, "event_date", since), since)
    try:
        c = V.compare(cb, cs, t, since=since, prewindowed=True)
    finally:
        cb.unpersist(), cs.unpersist()
    keys = ("ok", "bronze_total", "bronze_ok", "silver_rows", "missing", "extra", "dup_rows", "cutoff")
    assert {k: a[k] for k in keys} == {k: c[k] for k in keys}


def test_cached_is_a_noop_without_a_window(spark):
    b = bronze(spark, "bus_position", rows(2))
    assert V.cached(b, None) is b and V.cached(None, None) is None


def _write_tables(spark, tmp_path):
    """Bronze/Silver 를 파티션 폴더로 쓴다. 11-03 에는 '미래 시각' 위반이 있고 11-20 에는 정상 행만 있다."""
    old = [{"dedup_key": "o", "event_time": at(500), "fetched_at": at(100), "ingest_id": "o"}]                 # 11-03, 미래 시각 위반
    new = [{"dedup_key": f"n{i}", "event_time": at(i, day=20), "fetched_at": at(i + 5, day=20), "ingest_id": f"n{i}"} for i in range(4)]
    new.append({"dedup_key": "tail", "event_time": at(9000, day=20), "fetched_at": at(9000, day=20), "ingest_id": "t"})
    b = bronze(spark, "bus_position", old + new)
    b.write.mode("overwrite").partitionBy("event_date").parquet(str(tmp_path / "bronze" / "bus_position"))
    silver_of(b, "bus_position").write.mode("overwrite").partitionBy("event_date").parquet(str(tmp_path / "silver" / "silver_bus_position"))


def test_main_applies_the_window_to_time_checks_too(spark, tmp_path, monkeypatch, capsys):
    """구간을 주면 구간 밖(옛 이력)의 시간 위반은 보지 않고, 구간을 안 주면 본다. 이전에는 time_checks 가 Silver 전체를 읽었다."""
    _write_tables(spark, tmp_path)
    from pathlib import Path
    monkeypatch.setenv("SILVER_BRONZE_ROOT", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_ROOT", str(tmp_path / "silver"))
    monkeypatch.setenv("SILVER_CONFIG_DIR", str(Path(__file__).resolve().parents[2] / "config"))
    monkeypatch.delenv("SILVER_VERIFY_LOOKBACK_DAYS", raising=False)
    monkeypatch.setenv("SILVER_VERIFY_SINCE", "2026-11-19")
    assert V.main() == 0
    out = capsys.readouterr().out
    assert "silver_bus_position" in out and "미래 시각 0" in out and "구간 fetched_at ≥" in out
    monkeypatch.delenv("SILVER_VERIFY_SINCE")
    assert V.main() == 1
    assert "미래 시각 1" in capsys.readouterr().out


def test_incident_bronze_is_not_pruned_by_event_date_because_old_incidents_keep_being_observed(spark):
    """돌발 Bronze 의 event_date 는 발생 시각 날짜다. 오래된 장기 돌발도 최근에 관측되므로, 이를 파티션으로 거르면 Silver 쪽만 남아 '초과'로 보인다."""
    from datetime import datetime
    obs = [{"dedup_key": "long", "event_time": at(0, day=3), "fetched_at": at(10 + 120 * i, day=20), "ingest_id": f"call{i}"} for i in range(3)]   # 11-03 에 발생, 11-20 에 관측
    obs.append({"dedup_key": "tail", "event_time": at(9000, day=20), "fetched_at": at(9000, day=20), "ingest_id": "t"})
    b = bronze(spark, "seoul_incident", obs)
    t = table_of("seoul_incident")
    r = V.compare(b, silver_of(b, "seoul_incident"), t, since=datetime(2026, 11, 19, 15, 0, 0))
    assert r["ok"] and r["extra"] == 0 and r["missing"] == 0 and r["silver_rows"] == 3
    assert V.bronze_partition_col(t) is None and V.bronze_partition_col(table_of("bus_position")) == "event_date"
