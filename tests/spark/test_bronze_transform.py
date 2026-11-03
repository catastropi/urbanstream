"""Bronze 변환 테스트 (실제 Spark, 배치 DataFrame). 변환 함수는 스트리밍 DataFrame 에서도 같은 코드가 돈다."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from streaming.bronze import BronzeTable, to_bronze, to_bronze_raw, to_bronze_typed  # noqa: E402
from urbanstream.contracts import SOURCES  # noqa: E402
from urbanstream.contracts.common import KST, to_jsonable  # noqa: E402
from urbanstream.contracts.registry import schema_of  # noqa: E402

from spark_helpers import SAMPLE_FILES, kafka_rows, message_json, parse_sample  # noqa: E402

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def micros(dt: datetime) -> int:
    return (dt - EPOCH) // timedelta(microseconds=1)


@pytest.fixture(scope="module", params=sorted(SAMPLE_FILES))
def case(request, spark):
    """소스 하나: (소스명, 원본 Record 목록, 변환 결과 행들(오프셋 순))"""
    source = request.param
    records = parse_sample(source)
    out = to_bronze_typed(kafka_rows(spark, [message_json(r) for r in records], topic="topic-" + source, start_offset=100),
                          source)
    return source, records, out.orderBy("_kafka_offset").collect()


def test_every_message_becomes_exactly_one_ok_row(case):
    source, records, rows = case
    assert len(rows) == len(records)                       # 행 수 = 메시지 수 (아무것도 버리지 않는다)
    assert all(r["_parse_ok"] and r["_parse_error"] is None for r in rows)


def test_envelope_values_round_trip(case):
    source, records, rows = case
    for rec, row in zip(records, rows):
        env = rec.envelope
        assert row["event_id"] == env.event_id and row["dedup_key"] == env.dedup_key
        assert row["source_system"] == source and row["schema_version"] == SOURCES[source].schema_version
        assert row["http_status"] == env.http_status and row["ingest_id"] == env.ingest_id
        assert row["event_time"] is not None and row["fetched_at"] is not None   # 같은 '순간'인지는 아래 테스트가 마이크로초까지 비교


def test_timestamps_are_the_same_instant(spark, case):
    source, records, _ = case
    df = to_bronze_typed(kafka_rows(spark, [message_json(r) for r in records]), source)
    got = {r["event_id"]: (r["et"], r["fa"]) for r in
           df.select("event_id", F.unix_micros("event_time").alias("et"), F.unix_micros("fetched_at").alias("fa")).collect()}
    for rec in records:
        assert got[rec.envelope.event_id] == (micros(rec.envelope.event_time), micros(rec.envelope.fetched_at))


def test_payload_fields_round_trip(case):
    source, records, rows = case
    ts_fields = {f["name"] for f in schema_of(source)["payload"] if f["type"] == "timestamp"}
    for rec, row in zip(records, rows):
        expected = to_jsonable(rec.payload)
        actual = row["payload"].asDict()
        assert set(actual) == set(expected)
        for k, v in expected.items():
            if k in ts_fields:
                assert actual[k] is not None if v is not None else actual[k] is None
            else:
                assert actual[k] == v, f"{source}.payload.{k}: {actual[k]!r} != {v!r}"


def test_ingested_at_is_filled_by_bronze(case):
    source, records, rows = case
    assert all(r.envelope.ingested_at is None for r in records)   # 수집기는 비워 둔다 (HANDOFF 7-4)
    assert all(row["ingested_at"] is not None for row in rows)


def test_existing_ingested_at_is_preserved(spark):
    rec = parse_sample("bus_position")[0]
    d = rec.to_dict()
    d["ingested_at"] = "2026-01-02T03:04:05+00:00"
    row = to_bronze_typed(kafka_rows(spark, [json.dumps(d)]), "bus_position").select(
        F.date_format("ingested_at", "yyyy-MM-dd'T'HH:mm:ss").alias("s")).first()
    assert row["s"] == "2026-01-02T03:04:05"


def test_event_date_is_the_kst_date_of_event_time(case):
    source, records, rows = case
    for rec, row in zip(records, rows):
        assert row["event_date"] == rec.envelope.event_time.astimezone(KST).date()


def test_lineage_columns_keep_kafka_position_and_raw_text(case):
    source, records, rows = case
    for i, (rec, row) in enumerate(zip(records, rows)):
        assert row["_raw_value"] == message_json(rec)          # 원문 그대로
        assert (row["_kafka_topic"], row["_kafka_partition"], row["_kafka_offset"]) == ("topic-" + source, 0, 100 + i)
        assert row["_kafka_key"] == "k"


# ---- 깨진 입력은 버리지 않고 표시한다 ----

def bad_rows(spark, messages, source="bus_position"):
    ts = datetime(2026, 11, 3, 15, 30, 0, tzinfo=timezone.utc)   # KST 로는 11-04 00:30 → 날짜가 바뀌는 경계
    out = to_bronze_typed(kafka_rows(spark, messages, ts=ts), source).orderBy("_kafka_offset").collect()
    assert len(out) == len(messages)                              # 행 수 보존
    return out


def test_unparseable_json_is_kept_and_flagged(spark):
    rows = bad_rows(spark, ["이건 JSON 이 아님", "[1,2,3]", '"문자열"', ""])
    assert [r["_parse_error"] for r in rows] == ["unparseable"] * 4
    assert all(not r["_parse_ok"] for r in rows)
    assert rows[0]["_raw_value"] == "이건 JSON 이 아님"            # 원문 보존
    assert all(r["event_id"] is None and r["payload"] is None for r in rows)


def test_truncated_json_and_wrong_types_are_flagged_unparseable(spark):
    rec = parse_sample("seoul_traffic")[0]
    d = rec.to_dict()
    full = json.dumps(d, ensure_ascii=False)
    wrong_type = json.loads(full)
    wrong_type["payload"]["speed_kmh"] = "빠름"            # 숫자 자리에 문자열
    rows = bad_rows(spark, [full[:-5], json.dumps(wrong_type, ensure_ascii=False)], "seoul_traffic")
    assert [r["_parse_error"] for r in rows] == ["unparseable", "unparseable"]
    assert rows[0]["_raw_value"] == full[:-5]                # 원문은 그대로 남는다


def test_missing_required_fields_are_flagged(spark):
    rec = parse_sample("bus_position")[0]
    no_dedup = rec.to_dict()
    del no_dedup["dedup_key"]
    no_payload_key = rec.to_dict()
    no_payload_key["payload"].pop("veh_id")
    rows = bad_rows(spark, ["{}", json.dumps(no_dedup), json.dumps(no_payload_key)])
    assert [r["_parse_error"] for r in rows] == ["missing_required"] * 3


def test_other_schema_version_is_flagged_not_misread(spark):
    d = parse_sample("bus_position")[0].to_dict()
    d["schema_version"] = "bus_position_v2"
    (row,) = bad_rows(spark, [json.dumps(d)])
    assert row["_parse_error"] == "schema_version_mismatch" and row["_raw_value"] == json.dumps(d)


def test_event_date_of_bad_rows_comes_from_kafka_timestamp_in_kst(spark):
    rows = bad_rows(spark, ["깨짐"])
    assert str(rows[0]["event_date"]) == "2026-11-04"             # 15:30Z = 다음 날 00:30 KST


def test_good_and_bad_rows_mix_in_one_batch(spark):
    good = message_json(parse_sample("bus_position")[0])
    rows = bad_rows(spark, [good, "깨짐", good])
    assert [r["_parse_ok"] for r in rows] == [True, False, True]


# ---- 계약 스키마가 없는 토픽 (dlq, audit) ----

def test_raw_only_tables_keep_text_and_position(spark):
    ts = datetime(2026, 11, 3, 15, 30, 0, tzinfo=timezone.utc)
    msgs = ['{"source":"x","키":"값"}', "json 아님"]
    rows = to_bronze_raw(kafka_rows(spark, msgs, topic="platform.audit", ts=ts)).orderBy("_kafka_offset").collect()
    assert [r["_raw_value"] for r in rows] == msgs
    assert set(rows[0].asDict()) == {"_raw_value", "_kafka_topic", "_kafka_partition", "_kafka_offset",
                                     "_kafka_timestamp", "_kafka_key", "ingested_at", "event_date"}
    assert all(r["ingested_at"] is not None and str(r["event_date"]) == "2026-11-04" for r in rows)


def test_to_bronze_dispatches_on_table_kind(spark):
    df = kafka_rows(spark, [message_json(parse_sample("bus_position")[0])])
    typed = to_bronze(df, BronzeTable("bus_position", "t", "bus_position"))
    raw = to_bronze(df, BronzeTable("dlq", "t", None))
    assert "payload" in typed.columns and "payload" not in raw.columns
