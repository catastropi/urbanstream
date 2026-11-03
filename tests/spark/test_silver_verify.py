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
