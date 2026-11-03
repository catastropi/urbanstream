"""Bronze 프로파일링(Step 5-0) 시험. 정답을 아는 작은 Bronze 표를 만들어서 숫자가 맞는지 본다 (실제 Spark, 배치 DataFrame).

Bronze 표는 실제 샘플 응답을 `to_bronze_typed` 에 통과시켜 만들고, 시각·키·값만 시험 목적에 맞게 바꾼다 (스키마를 손으로 흉내 내지 않는다).
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from streaming.bronze import to_bronze_typed  # noqa: E402
from streaming.bronze_config import BronzeConfig  # noqa: E402
from streaming import profile_bronze as P  # noqa: E402

from spark_helpers import kafka_rows, parse_sample  # noqa: E402

T0 = "2026-11-03T15:00:00+09:00"


def at(sec: int, base_hour: int = 15) -> str:
    """KST base_hour:00:00 + sec 초 (ISO 문자열)."""
    h, rem = divmod(base_hour * 3600 + sec, 3600)
    m, s = divmod(rem, 60)
    return f"2026-11-03T{h:02d}:{m:02d}:{s:02d}+09:00"


def bronze(spark, source, rows):
    """rows: 행마다 envelope 덮어쓰기 값(+ 'payload': {...}). 반환: Bronze 모양 DataFrame."""
    base = parse_sample(source)[0].to_dict()
    msgs = []
    for i, ov in enumerate(rows):
        d = copy.deepcopy(base)
        ov = dict(ov)
        d["payload"].update(ov.pop("payload", {}))
        d.update(ov)
        d.setdefault("ingest_id", f"call-{i}")
        msgs.append(json.dumps(d, ensure_ascii=False))
    return to_bronze_typed(kafka_rows(spark, msgs, topic="t-" + source), source)


# ---- lateness: 워터마크의 근거


def test_lateness_counts_how_far_behind_the_running_max(spark):
    rows = [  # fetched_at 순서대로 event_time: 0, +60, +30(늦음 30초), +120
        {"fetched_at": at(10), "event_time": at(0), "ingest_id": "a"},
        {"fetched_at": at(70), "event_time": at(60), "ingest_id": "b"},
        {"fetched_at": at(130), "event_time": at(30), "ingest_id": "c"},
        {"fetched_at": at(190), "event_time": at(120), "ingest_id": "d"},
    ]
    r = P.lateness(bronze(spark, "bus_position", rows))
    assert r["rows_late_gt_0"] == 1 and r["max"] == 30.0 and r["n"] == 4


def test_lateness_is_zero_when_events_arrive_in_order(spark):
    rows = [{"fetched_at": at(10 + 60 * i), "event_time": at(60 * i), "ingest_id": str(i)} for i in range(5)]
    r = P.lateness(bronze(spark, "seoul_traffic", rows))
    assert r["rows_late_gt_0"] == 0 and r["max"] == 0.0


def test_lateness_ignores_when_bronze_processed_the_rows(spark):
    # 처리 시각(ingested_at)이 몇 시간 늦어도(백필) 도착 순서는 fetched_at 으로 본다 → 늦음 0
    rows = [{"fetched_at": at(10 + 60 * i), "event_time": at(60 * i), "ingested_at": at(0, 20), "ingest_id": str(i)} for i in range(3)]
    assert P.lateness(bronze(spark, "seoul_traffic", rows))["max"] == 0.0


def test_lateness_rows_of_the_same_call_are_ties(spark):
    # 한 호출의 차량들은 dataTm 이 20초 차이 → 동률 구간 안의 차이가 '늦음' 20초로 보인다 (모듈 설명에 적은 동작)
    rows = [{"fetched_at": at(10), "event_time": at(0), "ingest_id": "x"},
            {"fetched_at": at(10), "event_time": at(-20), "ingest_id": "x"}]
    assert P.lateness(bronze(spark, "bus_position", rows))["max"] == 20.0


# ---- 공통 측정


def test_duplicates_separate_same_call_from_cross_call(spark):
    # 두 값이 서로 달라야(2 vs 1) 둘을 바꿔 계산하는 실수를 잡을 수 있다 (변이 시험 M2 에서 둘 다 1 이라 살아남았던 것을 고침)
    rows = [{"dedup_key": "K1", "ingest_id": "a"}, {"dedup_key": "K1", "ingest_id": "a"}, {"dedup_key": "K1", "ingest_id": "a"},  # 같은 호출 안 중복 2건
            {"dedup_key": "K2", "ingest_id": "a"}, {"dedup_key": "K2", "ingest_id": "b"},                                       # 다른 호출에서 또 봄 1건
            {"dedup_key": "K3", "ingest_id": "b"}]
    r = P.duplicates(bronze(spark, "seoul_traffic", rows))
    assert r == {"rows": 6, "distinct_keys": 3, "same_call_dups": 2, "cross_call_dups": 1}


def test_time_anomalies_are_counted_separately(spark):
    rows = [{"fetched_at": at(0), "event_time": at(0), "ingested_at": at(5)},                 # 정상
            {"fetched_at": at(0), "event_time": at(120), "ingested_at": at(5)},               # 2분 미래
            {"fetched_at": at(0), "event_time": "2026-11-01T15:00:00+09:00", "ingested_at": at(5)},   # 이틀 전 (하루 넘게 전)
            {"fetched_at": at(30), "event_time": at(0), "ingested_at": at(0)}]               # fetched 가 ingested 보다 30초 뒤(시계 어긋남)
    r = P.time_anomalies(bronze(spark, "seoul_traffic", rows))
    assert r == {"future_event": 1, "event_after_ingest": 1, "fetched_after_ingest": 1, "event_older_than_1d": 1}


def test_hourly_uses_kst_and_shows_gaps(spark):
    rows = [{"fetched_at": at(10, 15)}, {"fetched_at": at(20, 15)}, {"fetched_at": at(10, 18)}]   # 16, 17시 없음
    assert P.hourly(bronze(spark, "seoul_traffic", rows)) == {"2026-11-03 15": 2, "2026-11-03 18": 1}


def test_pipeline_delay_buckets(spark):
    rows = [{"fetched_at": at(0), "ingested_at": at(60)},        # 60초
            {"fetched_at": at(0), "ingested_at": at(300)},       # 5분
            {"fetched_at": at(0), "ingested_at": at(7200)}]      # 2시간 (백필)
    r = P.pipeline_delay(bronze(spark, "seoul_traffic", rows))
    assert (r["le_2min"], r["2_to_10min"], r["gt_10min"]) == (1, 1, 1)


def test_coverage_reports_failed_rows_and_period_in_kst(spark):
    df = bronze(spark, "seoul_traffic", [{"fetched_at": at(0)}, {"fetched_at": at(3600)}])
    r = P.coverage(df)
    assert r["rows"] == 2 and r["parse_failed"] == 0
    assert r["fetched_first_kst"] == "2026-11-03 15:00:00" and r["fetched_last_kst"] == "2026-11-03 16:00:00"


# ---- 소스별


def test_traffic_counts_zero_and_null_speed_without_interpreting(spark):
    rows = [{"payload": {"speed_kmh": 0.0}}, {"payload": {"speed_kmh": 22.5}}, {"payload": {"speed_kmh": None}}]
    r = P.traffic(bronze(spark, "seoul_traffic", rows))
    assert (r["rows"], r["speed_zero"], r["speed_null"]) == (3, 1, 1)


def test_position_detects_regress_and_same_pairs(spark):
    v = {"veh_id": "V1"}
    rows = [{"fetched_at": at(0), "event_time": at(-5), "payload": v},
            {"fetched_at": at(90), "event_time": at(85), "payload": v},     # 정상 전진
            {"fetched_at": at(180), "event_time": at(84), "payload": v},    # 1초 거꾸로
            {"fetched_at": at(270), "event_time": at(84), "payload": v}]    # 제자리(같은 dataTm)
    r = P.position(bronze(spark, "bus_position", rows))
    assert r["consecutive_pairs"] == 3 and r["regress_pairs"] == 1 and r["same_event_time_pairs"] == 1
    assert r["min_delta_sec"] == -1.0 and r["vehicles"] == 1


def test_position_pairs_are_per_vehicle(spark):
    rows = [{"fetched_at": at(0), "event_time": at(0), "payload": {"veh_id": "A"}},
            {"fetched_at": at(10), "event_time": at(-500), "payload": {"veh_id": "B"}}]   # 다른 차량이면 '거꾸로'가 아니다
    r = P.position(bronze(spark, "bus_position", rows))
    assert r["consecutive_pairs"] == 0 and r["regress_pairs"] == 0


def test_arrival_zero_seconds_are_shown_with_their_message(spark):
    rows = [{"payload": {"arrive_in_sec": 0, "arrival_msg": "곧 도착", "arrive_code": 0}},
            {"payload": {"arrive_in_sec": 0, "arrival_msg": "출발대기", "arrive_code": 2}},
            {"payload": {"arrive_in_sec": 0, "arrival_msg": "곧 도착", "arrive_code": 0}},
            {"payload": {"arrive_in_sec": 116, "arrival_msg": "1분56초후[1번째 전]", "arrive_code": 0}},
            {"payload": {"arrive_in_sec": None, "arrival_msg": "운행종료", "arrive_code": 0}}]      # NULL 은 0 과 따로 센다 (변이 시험 M11)
    r = P.arrival(bronze(spark, "bus_arrival", rows))
    assert r["arrive_in_sec_zero"] == 3 and r["arrive_in_sec_null"] == 1
    assert r["arrival_msg_when_zero_top"] == {"곧 도착": 2, "출발대기": 1}
    assert r["arrive_code"] == {"0": 4, "2": 1}


def test_arrival_flags_a_call_with_two_snapshot_times(spark):
    rows = [{"ingest_id": "c", "event_time": at(0)}, {"ingest_id": "c", "event_time": at(20)}]
    assert P.arrival(bronze(spark, "bus_arrival", rows))["max_distinct_mkTm_in_a_call"] == 2


def test_incident_versions_duration_and_age(spark):
    def inc(acc, h, occ, clr, call, fetched=at(0)):
        return {"ingest_id": call, "fetched_at": fetched,
                "payload": {"acc_id": acc, "content_hash": h, "occurred_at": occ, "expected_clear_at": clr, "acc_type": "A04"}}
    rows = [inc("1", "h1", "2026-04-07T10:00:00+09:00", "2027-03-01T00:00:00+09:00", "c1"),    # 장기 공사 (>30일), 발생 후 >30일
            inc("1", "h1", "2026-04-07T10:00:00+09:00", "2027-03-01T00:00:00+09:00", "c2"),
            inc("2", "a", "2026-11-03T14:00:00+09:00", "2026-11-03T18:00:00+09:00", "c1"),      # 4시간 (<=1일)
            inc("2", "b", "2026-11-03T14:00:00+09:00", "2026-11-03T19:00:00+09:00", "c2")]      # 내용이 바뀜 (해제 예정 연장)
    r = P.incident(bronze(spark, "seoul_incident", rows))
    assert r["distinct_incidents"] == 2 and r["observations"] == 4
    assert r["incidents_with_changed_content"] == 1 and r["max_content_versions_of_one_incident"] == 2
    assert r["planned_duration"] == {">30일": 1, "<=1일": 1}
    assert r["age_at_first_seen"] == {">30일": 1, "<=1일": 1}
    assert r["by_acc_type"] == {"A04": 2}


def test_weather_shows_unreadable_values_and_rn1_shapes(spark):
    p = lambda cat, raw, val: {"payload": {"category": cat, "value_raw": raw, "value": val}}
    rows = [p("T1H", "12.3", 12.3), p("RN1", "강수없음", None), p("RN1", "1.0mm", None), p("RN1", "강수없음", None)]
    r = P.weather(bronze(spark, "kma_ncst", rows))
    assert r["unreadable_value_rows"] == 3 and r["RN1_raw_values"] == {"강수없음": 2, "1.0mm": 1}
    assert set(r["unreadable_examples"]) == {"RN1=강수없음", "RN1=1.0mm"}
    assert r["categories"]["T1H"] == {"n": 1, "min": 12.3, "max": 12.3}


# ---- 조립: 오류 격리, 읽기 전용, 저장


def test_a_failing_section_does_not_hide_the_others(spark):
    df = bronze(spark, "seoul_traffic", [{}])
    out = P.profile_table(df, "bus_arrival")          # 도로소통 표에 버스도착 측정을 억지로 적용 → source_specific 만 실패해야 한다
    assert "error" in out["source_specific"]
    assert out["coverage"]["rows"] == 1 and "error" not in out["duplicates"]


def test_parse_failed_rows_are_excluded_from_the_measures_but_reported(spark):
    good = json.dumps(parse_sample("seoul_traffic")[0].to_dict(), ensure_ascii=False)
    df = to_bronze_typed(kafka_rows(spark, [good, "이건 JSON 이 아님"], topic="t"), "seoul_traffic")
    out = P.profile_table(df, "seoul_traffic")
    assert out["coverage"]["rows"] == 2 and out["coverage"]["parse_failed"] == 1
    assert out["duplicates"]["rows"] == 1


def test_profile_all_reads_but_never_writes_bronze(spark, tmp_path):
    root = tmp_path / "bronze"
    bronze(spark, "seoul_traffic", [{"fetched_at": at(0)}, {"fetched_at": at(120)}]).write.partitionBy("event_date").parquet(str(root / "seoul_traffic"))
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    cfg = BronzeConfig(bronze_root=str(root), checkpoint_root=str(tmp_path / "ck"))
    rep = P.profile_all(spark, cfg)
    after = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    assert before == after                                         # 읽기 전용
    assert rep["tables"]["seoul_traffic"]["coverage"]["rows"] == 2
    assert rep["tables"]["bus_position"] == {"rows": 0}             # 아직 표가 없는 소스는 빈 것으로 보고
    assert "dlq" not in rep["tables"] and "audit" not in rep["tables"]


def test_save_json_only_when_the_folder_exists(tmp_path):
    rep = {"tables": {"x": {"rows": 1}}}
    assert P.save_json(rep, str(tmp_path / "없는폴더")) is None
    p = P.save_json(rep, str(tmp_path))
    assert p and json.loads(Path(p).read_text(encoding="utf-8")) == rep


def test_format_report_is_readable_and_keeps_numbers():
    txt = P.format_report({"coverage": {"rows": 5}, "lateness_sec": {"p95": 1.5, "max": 3.0}, "x": {"a": {"b": 1}}})
    assert "coverage: rows=5" in txt and "lateness_sec: p95=1.5, max=3.0" in txt and "  a: b=1" in txt
