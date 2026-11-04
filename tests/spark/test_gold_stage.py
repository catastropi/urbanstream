"""Gold 스테이징 변환 테스트. Silver/audit 를 작은 DataFrame 으로 직접 만든다 (실제 Silver 스키마와 같은 열 이름)."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from batch import gold_stage as G
from batch.gold_config import GoldConfig

T0 = datetime(2026, 11, 3, 6, 0, 0, tzinfo=timezone.utc)      # KST 15:00, 같은 날짜
cfg = GoldConfig()


def u(x):
    """collect() 는 timestamp 를 프로세스 지역 시각의 naive datetime 으로 준다 → UTC aware 로 되돌려 비교한다."""
    return x.astimezone(timezone.utc) if x is not None else None


def at(sec: int) -> datetime:
    return T0 + timedelta(seconds=sec)


def test_traffic_dedup_keeps_first_seen_and_filters_by_date(spark):
    rows = [("k1", "L1", at(0), at(5), 30.0, 100, [], date(2026, 11, 3), 7),
            ("k1", "L1", at(0), at(300), 99.0, 100, [], date(2026, 11, 3), 9),      # 재통과: 버려야 함
            ("k2", "L1", at(450), at(455), 28.0, 110, ["speed_zero"], date(2026, 11, 4), 11)]
    df = spark.createDataFrame(rows, "dedup_key string, link_id string, event_time timestamp, fetched_at timestamp, "
                                     "speed_kmh double, travel_time_sec long, dq_flags array<string>, event_date date, _kafka_offset long")
    out = {r["dedup_key"]: r for r in G.stage_traffic(df, cfg).collect()}
    assert out["k1"]["speed_kmh"] == 30.0 and len(out) == 2
    only3 = G.stage_traffic(df, GoldConfig(date_from=date(2026, 11, 3), date_to=date(2026, 11, 3))).collect()
    assert [r["dedup_key"] for r in only3] == ["k1"]


def test_weather_pivot_to_one_row_per_time_and_grid(spark):
    d = date(2026, 11, 3)
    rows = [("a", at(0), at(60), 60, 127, "T1H", "12.3", 12.3, [], d, 1), ("b", at(0), at(60), 60, 127, "RN1", "0", 0.0, [], d, 2),
            ("c", at(0), at(60), 60, 127, "REH", "x", None, ["value_unparsed"], d, 3),
            ("d", at(3600), at(3660), 60, 127, "T1H", "13.0", 13.0, [], d, 4)]
    df = spark.createDataFrame(rows, "dedup_key string, event_time timestamp, fetched_at timestamp, nx long, ny long, category string, "
                                     "value_raw string, value double, dq_flags array<string>, event_date date, _kafka_offset long")
    out = {u(r["base_time"]): r for r in G.stage_weather(df, cfg).collect()}
    r0 = out[at(0)]
    assert (r0["t1h"], r0["rn1"], r0["reh"], r0["uuu"], r0["value_unparsed_count"]) == (12.3, 0.0, None, None, 1)
    assert len(out) == 2


def _audit_df(spark, msgs):
    return spark.createDataFrame([(json.dumps(m),) for m in msgs], "_raw_value string")


def _call(ingest, t, outcome="ok", truncated=False, source="seoul_incident", final=True, with_id=True):
    m = {"event": "call", "source_system": source, "target": source, "final": final, "outcome": outcome,
         "fetched_at": at(t).isoformat(), "http_status": 200, "latency_ms": 50, "status_kind": "ok", "n_records": 3,
         "truncated": truncated}
    if with_id:
        m["ingest_id"] = ingest
    return m


def test_parse_audit_keeps_only_final_calls_with_ingest_id(spark):
    a = G.parse_audit(_audit_df(spark, [_call("i1", 0), _call("i1", 0), _call("i2", 10, final=False),
                                        _call("x", 20, with_id=False), {"event": "other"}, _call("i3", 30, outcome="failed")]))
    rows = {r["ingest_id"]: r for r in a.collect()}
    assert set(rows) == {"i1", "i3"} and rows["i1"]["ok"] and not rows["i3"]["ok"]       # i1 중복 제거
    assert u(rows["i1"]["fetched_at"]) == at(0)


def _obs(spark, items):
    return spark.createDataFrame(items, "acc_id string, occurred_at timestamp, fetched_at timestamp, ingest_id string, "
                                        "expected_clear_at timestamp, is_long_term boolean")


def test_episode_absent_since_uses_good_incident_calls_not_wall_clock(spark):
    occ = at(-1000)
    obs = _obs(spark, [("A", occ, at(0), "c0", at(5000), False), ("A", occ, at(120), "c1", at(6000), False),
                       ("B", occ, at(0), "c0", None, True), ("B", occ, at(120), "c1", None, True),
                       ("B", occ, at(240), "c2", None, True), ("N", None, at(0), "c0", None, False)])
    audit = G.parse_audit(_audit_df(spark, [
        _call("c0", 0), _call("c1", 120),
        _call("c2", 240),                                   # B 가 마지막으로 보인 호출
        _call("c3", 360, truncated=True),                   # 잘린 호출: 사라짐 증거 아님
        _call("c4", 480, outcome="failed"),                 # 실패 호출: 증거 아님
        _call("t1", 500, source="seoul_traffic"),           # 다른 소스: 증거 아님
        _call("c5", 600),                                   # A 는 여기서 처음 '없음'을 확인 (A 마지막 관측은 120)
    ]))
    ep = {r["acc_id"]: r for r in G.stage_episodes(obs, audit).collect()}
    assert "N" not in ep                                    # occurred_at 없는 행은 수명 계산에서 제외
    a, b = ep["A"], ep["B"]
    assert (u(a["first_seen_at"]), u(a["last_seen_at"]), a["obs_count"], u(a["expected_clear_at"])) == (at(0), at(120), 2, at(6000))
    assert u(a["absent_since"]) == at(240)                     # A: 120 이후 첫 성공·비절단 호출은 c2(240)
    assert u(b["absent_since"]) == at(600) and b["is_long_term"] is True   # B: 240 이후 truncated/failed/타소스 건너뛰고 600


def test_episode_still_listed_when_no_later_good_call(spark):
    obs = _obs(spark, [("A", at(-5), at(0), "c0", None, False)])
    audit = G.parse_audit(_audit_df(spark, [_call("c0", 0)]))
    assert G.stage_episodes(obs, audit).collect()[0]["absent_since"] is None


def test_run_end_to_end_is_idempotent_and_writes_manifest(spark, tmp_path):
    silver, bronze, stage = tmp_path / "silver", tmp_path / "bronze", tmp_path / "stage"
    d = date(2026, 11, 3)
    spark.createDataFrame([("k1", "L1", at(0), at(5), 30.0, 100, [], d, 1)],
                          "dedup_key string, link_id string, event_time timestamp, fetched_at timestamp, speed_kmh double, "
                          "travel_time_sec long, dq_flags array<string>, event_date date, _kafka_offset long"
                          ).write.parquet(str(silver / "silver_traffic_speed"))
    _audit_df(spark, [_call("t1", 0, source="seoul_traffic")]).write.parquet(str(bronze / "audit"))
    c = GoldConfig(silver_root=str(silver), bronze_root=str(bronze), stage_root=str(stage), date_from=d, date_to=d)
    m1 = G.run(spark, c)
    m2 = G.run(spark, c)
    assert m1["row_counts"] == m2["row_counts"]
    assert m1["row_counts"]["traffic_speed"] == 1 and m1["row_counts"]["call_audit"] == 1 and m1["row_counts"]["bus_position"] == 0
    assert json.loads((stage / "manifest.json").read_text())["audit_available"] is True
