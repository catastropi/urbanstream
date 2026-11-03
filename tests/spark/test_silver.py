"""Silver 시험 (Step 5). 정답을 아는 작은 Bronze 표를 실제 Spark 로 변환한다.

- 변환(`transform_*`)은 배치 DataFrame 으로, 중복 제거와 재시작은 파일 스트림(availableNow)으로 확인한다.
- Bronze 표는 실제 샘플 응답을 `to_bronze_typed` 에 통과시켜 만든다 (스키마를 손으로 흉내 내지 않는다).
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from streaming import silver as S  # noqa: E402
from streaming.bronze import to_bronze_typed  # noqa: E402
from streaming.silver_config import SilverConfig, load_silver_tables  # noqa: E402

from spark_helpers import kafka_rows, parse_sample  # noqa: E402

CONFIG_DIR = str(Path(__file__).resolve().parents[2] / "config")


def at(sec: int, hour: int = 15, day: int = 3) -> str:
    """KST 2026-11-<day> <hour>:00:00 + sec 초 (며칠을 넘어가도 올바른 ISO 문자열)."""
    base = datetime(2026, 11, day, hour, 0, 0, tzinfo=timezone(timedelta(hours=9)))
    return (base + timedelta(seconds=sec)).isoformat()


def bronze(spark, source, rows, *, raw=None):
    """rows: 행마다 envelope 덮어쓰기 값(+ 'payload': {...}). raw: 읽을 수 없는 원문 목록(정상 행 뒤에 붙는다)."""
    base = parse_sample(source)[0].to_dict()
    msgs = []
    for i, ov in enumerate(rows):
        d = copy.deepcopy(base)
        ov = dict(ov)
        d["payload"].update(ov.pop("payload", {}))
        d.update(ov)
        d.setdefault("ingest_id", f"call-{i}")
        msgs.append(json.dumps(d, ensure_ascii=False))
    msgs += list(raw or [])
    return to_bronze_typed(kafka_rows(spark, msgs, topic="t-" + source), source)


def cfg_for(tmp_path, **kw):
    return SilverConfig(bronze_root=str(tmp_path / "bronze"), silver_root=str(tmp_path / "silver"),
                        checkpoint_root=str(tmp_path / "ckpt"), config_dir=CONFIG_DIR, **kw)


def table_of(source):
    return next(t for t in load_silver_tables(CONFIG_DIR) if t.source == source)


def rows_of(df, *cols):
    return [tuple(r[c] for c in cols) for r in df.collect()]


# ---------------------------------------------------------------- 변환: 공통

@pytest.mark.parametrize("source,fn", [
    ("seoul_traffic", S.transform_traffic), ("bus_position", S.transform_position),
    ("bus_arrival", S.transform_arrival), ("kma_ncst", S.transform_weather),
])
def test_output_is_flat_and_carries_lineage_and_flags(spark, source, fn):
    out = fn(bronze(spark, source, [{"fetched_at": at(10), "event_time": at(0)}]))
    cols = set(out.columns)
    assert "payload" not in cols and "_raw_value" not in cols and "_parse_ok" not in cols
    assert {"event_id", "dedup_key", "ingest_id", "raw_uri", "event_time", "fetched_at", "ingested_at",
            "_kafka_partition", "_kafka_offset", "silver_processed_at", "dq_flags", "event_date"} <= cols
    r = out.collect()[0]
    assert r["silver_processed_at"] is not None and isinstance(r["dq_flags"], list)


def test_rows_that_could_not_be_parsed_are_left_out_but_counted_by_difference(spark):
    b = bronze(spark, "bus_position", [{"fetched_at": at(10), "event_time": at(0)}], raw=["not json", "{}"])
    assert b.count() == 3                                          # Bronze 는 전부 가지고 있다
    assert S.transform_position(b).count() == 1                    # Silver 는 정상 행만 → 차이 2 = _parse_error 행 수 (D-08 등식)


def test_ingest_clock_skew_flag_when_ingested_before_fetched(spark):
    rows = [{"fetched_at": at(100), "event_time": at(0), "ingested_at": at(0), "dedup_key": "a"},
            {"fetched_at": at(100), "event_time": at(0), "ingested_at": at(200), "dedup_key": "b"}]
    out = {r["dedup_key"]: r["dq_flags"] for r in S.transform_position(bronze(spark, "bus_position", rows)).collect()}
    assert "ingest_clock_skew" in out["a"] and "ingest_clock_skew" not in out["b"]


# ---------------------------------------------------------------- 변환: 소스별 규칙

def test_traffic_zero_and_missing_are_kept_and_only_flagged(spark):
    rows = [{"dedup_key": "z", "payload": {"speed_kmh": 0.0, "travel_time_sec": 0}},
            {"dedup_key": "n", "payload": {"speed_kmh": None, "travel_time_sec": 55}},
            {"dedup_key": "ok", "payload": {"speed_kmh": 30.0, "travel_time_sec": 55}}]
    out = {r["dedup_key"]: r for r in S.transform_traffic(bronze(spark, "seoul_traffic", rows)).collect()}
    assert out["z"]["speed_kmh"] == 0.0 and set(out["z"]["dq_flags"]) == {"speed_zero", "travel_time_zero"}   # 0 을 바꾸지 않는다
    assert out["n"]["speed_kmh"] is None and out["n"]["dq_flags"] == ["speed_null"]
    assert out["ok"]["dq_flags"] == []


def test_arrival_zero_and_null_flags(spark):
    rows = [{"dedup_key": "z", "payload": {"arrive_in_sec": 0}}, {"dedup_key": "n", "payload": {"arrive_in_sec": None}},
            {"dedup_key": "ok", "payload": {"arrive_in_sec": 120}}]
    out = {r["dedup_key"]: r for r in S.transform_arrival(bronze(spark, "bus_arrival", rows)).collect()}
    assert out["z"]["dq_flags"] == ["arrive_zero"] and out["z"]["arrive_in_sec"] == 0
    assert out["n"]["dq_flags"] == ["arrive_null"] and out["ok"]["dq_flags"] == []


def test_weather_keeps_raw_text_and_flags_unparsed_value(spark):
    rows = [{"dedup_key": "u", "payload": {"value_raw": "강수없음", "value": None}},
            {"dedup_key": "ok", "payload": {"value_raw": "0.5", "value": 0.5}},
            {"dedup_key": "both_null", "payload": {"value_raw": None, "value": None}}]
    out = {r["dedup_key"]: r for r in S.transform_weather(bronze(spark, "kma_ncst", rows)).collect()}
    assert out["u"]["value_raw"] == "강수없음" and out["u"]["dq_flags"] == ["value_unparsed"]    # 행은 버려지지 않는다
    assert out["ok"]["dq_flags"] == []
    assert "both_null" not in out            # 원문(value_raw)이 없으면 계약 위반(missing_required) → Bronze 에는 남고 Silver 에서는 제외
    assert "category" in S.transform_weather(bronze(spark, "kma_ncst", rows)).columns            # 긴 형태 유지


def test_incident_duration_and_long_term_threshold_boundary(spark):
    d = 86400
    rows = [{"dedup_key": "exactly7", "payload": {"occurred_at": at(0), "expected_clear_at": at(7 * d)}},
            {"dedup_key": "7d1s", "payload": {"occurred_at": at(0), "expected_clear_at": at(7 * d + 1)}},
            {"dedup_key": "hours", "payload": {"occurred_at": at(0), "expected_clear_at": at(3600)}},
            {"dedup_key": "noclear", "payload": {"occurred_at": at(0), "expected_clear_at": None}},
            {"dedup_key": "neg", "payload": {"occurred_at": at(100), "expected_clear_at": at(0)}}]
    out = {r["dedup_key"]: r for r in S.transform_incident(bronze(spark, "seoul_incident", rows), 7.0).collect()}
    assert out["exactly7"]["is_long_term"] is False and out["7d1s"]["is_long_term"] is True      # 7일 "초과"
    assert abs(out["hours"]["planned_duration_days"] - 1 / 24) < 1e-9 and out["hours"]["is_long_term"] is False
    assert out["noclear"]["planned_duration_days"] is None and out["noclear"]["is_long_term"] is False
    assert "no_expected_clear" in out["noclear"]["dq_flags"] and "negative_duration" in out["neg"]["dq_flags"]


def test_incident_threshold_comes_from_the_argument_not_a_constant(spark):
    rows = [{"dedup_key": "k", "payload": {"occurred_at": at(0), "expected_clear_at": at(2 * 86400)}}]
    b = bronze(spark, "seoul_incident", rows)
    assert S.transform_incident(b, 1.0).collect()[0]["is_long_term"] is True
    assert S.transform_incident(b, 30.0).collect()[0]["is_long_term"] is False


def test_incident_coordinates_converted_and_missing_tm_is_flagged(spark):
    rows = [{"dedup_key": "c", "payload": {"tm_x": 198000.0, "tm_y": 451500.0}},
            {"dedup_key": "m", "payload": {"tm_x": None, "tm_y": 451500.0}}]
    out = {r["dedup_key"]: r for r in S.transform_incident(bronze(spark, "seoul_incident", rows), 7.0).collect()}
    assert abs(out["c"]["lon"] - 126.97736303425938) < 1e-9 and abs(out["c"]["lat"] - 37.56303076860721) < 1e-9
    assert out["m"]["lon"] is None and out["m"]["lat"] is None and "no_coordinates" in out["m"]["dq_flags"]


def test_incident_observed_date_is_kst_date_of_fetched_at_and_no_event_date(spark):
    rows = [{"dedup_key": "late", "fetched_at": at(23 * 3600 + 30 * 60, hour=0), "payload": {}},       # 11-03 23:30 KST
            {"dedup_key": "next", "fetched_at": at(10 * 60, hour=0, day=4), "payload": {}}]           # 11-04 00:10 KST (= 11-03 15:10 UTC)
    out = {r["dedup_key"]: str(r["observed_date"]) for r in S.transform_incident(bronze(spark, "seoul_incident", rows), 7.0).collect()}
    assert out == {"late": "2026-11-03", "next": "2026-11-04"}
    assert "event_date" not in S.transform_incident(bronze(spark, "seoul_incident", rows), 7.0).columns


# ---------------------------------------------------------------- 스트림: 중복 제거·재시작

def write_bronze(df, tmp_path, source):
    df.write.partitionBy("event_date").mode("append").parquet(str(tmp_path / "bronze" / source))


def run(spark, tmp_path, source):
    cfg = cfg_for(tmp_path)
    t = table_of(source)
    q = S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"availableNow": True})
    q.awaitTermination()
    return spark.read.parquet(str(tmp_path / "silver" / t.name))


def test_stream_drops_repeated_dedup_key_across_files_within_watermark(spark, tmp_path):
    a = [{"dedup_key": "v1", "event_time": at(0), "fetched_at": at(10), "ingest_id": "c1"},
         {"dedup_key": "v1", "event_time": at(0), "fetched_at": at(10), "ingest_id": "c1"},      # 같은 호출 안 재전송
         {"dedup_key": "v2", "event_time": at(30), "fetched_at": at(40), "ingest_id": "c1"}]
    write_bronze(bronze(spark, "bus_position", a), tmp_path, "bus_position")
    out = run(spark, tmp_path, "bus_position")
    assert sorted(r["dedup_key"] for r in out.collect()) == ["v1", "v2"]

    b = [{"dedup_key": "v1", "event_time": at(0), "fetched_at": at(100), "ingest_id": "c2"},     # 다음 호출에 같은 GPS 시각 (정지한 차량)
         {"dedup_key": "v3", "event_time": at(90), "fetched_at": at(100), "ingest_id": "c2"}]
    write_bronze(bronze(spark, "bus_position", b), tmp_path, "bus_position")
    out = run(spark, tmp_path, "bus_position")
    assert sorted(r["dedup_key"] for r in out.collect()) == ["v1", "v2", "v3"]                    # v1 이 다시 들어오지 않았다


def test_stream_restart_without_new_input_adds_nothing(spark, tmp_path):
    rows = [{"dedup_key": f"k{i}", "event_time": at(i), "fetched_at": at(i + 5), "ingest_id": "c"} for i in range(5)]
    write_bronze(bronze(spark, "seoul_traffic", rows), tmp_path, "seoul_traffic")
    assert run(spark, tmp_path, "seoul_traffic").count() == 5
    assert run(spark, tmp_path, "seoul_traffic").count() == 5


def test_stream_incident_keeps_repeated_observations_but_not_resent_ones(spark, tmp_path):
    def obs(call, t, n=1):
        return [{"dedup_key": "acc-1", "ingest_id": call, "event_time": at(0), "fetched_at": at(t),
                 "payload": {"occurred_at": at(0), "expected_clear_at": at(3600)}}] * n
    rows = obs("c1", 10) + obs("c2", 130) + obs("c3", 250, n=2)          # c3 은 같은 호출이 두 번 전달됨
    write_bronze(bronze(spark, "seoul_incident", rows), tmp_path, "seoul_incident")
    out = run(spark, tmp_path, "seoul_incident")
    assert sorted(r["ingest_id"] for r in out.collect()) == ["c1", "c2", "c3"]   # 관측 3번, 재전송 1건만 제거
    assert {str(r["observed_date"]) for r in out.collect()} == {"2026-11-03"}


def test_stream_output_is_partitioned_by_the_declared_column(spark, tmp_path):
    for source, col in [("bus_position", "event_date"), ("seoul_incident", "observed_date")]:
        write_bronze(bronze(spark, source, [{"dedup_key": "x", "event_time": at(0), "fetched_at": at(5)}]), tmp_path, source)
        run(spark, tmp_path, source)
        names = [p.name for p in (tmp_path / "silver" / table_of(source).name).iterdir()]
        assert any(n.startswith(col + "=") for n in names), (source, names)
