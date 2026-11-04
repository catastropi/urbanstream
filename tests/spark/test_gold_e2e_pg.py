"""Spark 스테이징 → 호스트 로더 → Postgres 전 구간 테스트 (열 이름·타입이 서로 맞는지가 핵심).
GOLD_TEST_DSN 이 없으면 건너뛴다. 같은 컨테이너에서 Spark 와 DB 를 같이 쓰지 않는 운영 구조와 달리, 테스트는 한 프로세스에서 둘 다 돈다."""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pg_support import pg  # noqa: F401
from batch import gold_load as L
from batch import gold_stage as G
from batch.gold_config import GoldConfig

pytestmark = pytest.mark.pg
ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 11, 3, 6, 0, 0, tzinfo=timezone.utc)
D = date(2026, 11, 3)


def at(s): return T0 + timedelta(seconds=s)


def _write_silver(spark, root: Path):
    common = "dedup_key string, event_time timestamp, fetched_at timestamp, _kafka_offset long, dq_flags array<string>"
    spark.createDataFrame([("t1", at(0), at(5), 1, [], "L1", 30.0, 100, D), ("t2", at(450), at(455), 2, ["speed_zero"], "L1", 0.0, 90, D)],
                          common + ", link_id string, speed_kmh double, travel_time_sec long, event_date date"
                          ).write.parquet(str(root / "silver_traffic_speed"))
    spark.createDataFrame([("p1", at(0), at(5), 1, [], "753", "V1", "서울70", 126.9, 37.5, "S1", 3, "A", "B", 10.0, 20.0, 5.0, 2, 0, True, False, False, D)],
                          common + ", route_id string, veh_id string, plain_no string, lon double, lat double, section_id string, sect_ord long, "
                                   "last_stop_id string, next_stop_id string, sect_dist double, full_sect_dist double, rt_dist double, "
                                   "congestion long, stop_flag long, is_running boolean, is_last boolean, is_full boolean, event_date date"
                          ).write.parquet(str(root / "silver_bus_position"))
    spark.createDataFrame([("a1", at(0), at(5), 1, [], "753", "ST1", 4, 1, "V1", "서울70", "곧 도착", 0, 30, 31, 32, 33, 0, False, D)],
                          common + ", route_id string, station_id string, station_order long, slot long, veh_id string, plain_no string, "
                                   "arrival_msg string, stops_away long, arrive_in_sec long, pred_exps_sec long, pred_kals_sec long, pred_neus_sec long, "
                                   "arrive_code long, is_last_bus boolean, event_date date"
                          ).write.parquet(str(root / "silver_bus_arrival"))
    occ = at(-1000)
    spark.createDataFrame([("i-h", at(0), at(0), 1, [], "c0", "ACC1", occ, at(9000), "A04", None, "1220003800", "공사", "R1", 0.2, False, 126.97, 37.56, D),
                           ("i-h", at(120), at(120), 2, [], "c1", "ACC1", occ, at(9000), "A04", None, "1220003800", "공사", "R1", 0.2, False, 126.97, 37.56, D)],
                          common + ", ingest_id string, acc_id string, occurred_at timestamp, expected_clear_at timestamp, acc_type string, "
                                   "acc_detail_type string, link_id string, description string, road_code string, planned_duration_days double, "
                                   "is_long_term boolean, lon double, lat double, observed_date date"
                          ).write.parquet(str(root / "silver_incident_observation"))
    spark.createDataFrame([("w1", at(0), at(60), 1, [], 60, 127, "T1H", "12.3", 12.3, D), ("w2", at(0), at(60), 2, [], 60, 127, "RN1", "0", 0.0, D)],
                          common + ", nx long, ny long, category string, value_raw string, value double, event_date date"
                          ).write.parquet(str(root / "silver_weather"))


def _audit(i, t, source="seoul_incident", ok=True):
    return (json.dumps({"event": "call", "source_system": source, "target": source, "final": True, "outcome": "ok" if ok else "failed",
                        "ingest_id": i, "fetched_at": at(t).isoformat(), "http_status": 200, "latency_ms": 40,
                        "status_kind": "ok", "n_records": 1, "truncated": False}),)


def test_stage_then_load_matches_and_is_idempotent(spark, tmp_path, pg):
    _write_silver(spark, tmp_path / "silver")
    spark.createDataFrame([_audit("c0", 0), _audit("c1", 120), _audit("c2", 240), _audit("t1", 5, "seoul_traffic")],
                          "_raw_value string").write.parquet(str(tmp_path / "bronze" / "audit"))
    cfg = GoldConfig(silver_root=str(tmp_path / "silver"), bronze_root=str(tmp_path / "bronze"),
                     stage_root=str(tmp_path / "stage"), date_from=D, date_to=D)
    G.run(spark, cfg)
    res = L.load_all(pg, tmp_path / "stage", refresh_mart=True)
    q = lambda sql: pg.execute(sql).fetchall()
    assert res["row_counts"] == {"traffic_speed": 2, "bus_position": 1, "bus_arrival": 1, "incident_observation": 2,
                                 "weather_obs": 1, "incident_episode": 1, "call_audit": 4}
    assert q("select speed_kmh, dq_flags from fact.traffic_speed order by event_time") == [(30.0, []), (0.0, ["speed_zero"])]
    assert q("select event_time = '2026-11-03 06:00:00+00' from fact.traffic_speed where dedup_key='t1'") == [(True,)]     # 시간대 정확
    assert q("select ST_X(geom), ST_Y(geom) from fact.incident_observation limit 1")[0] == (126.97, 37.56)
    assert q("select t1h, rn1, reh from fact.weather_obs") == [(12.3, 0.0, None)]
    assert q("select obs_count, absent_since = '2026-11-03 06:04:00+00' from fact.incident_episode") == [(2, True)]
    assert q("select status from ops.load_run") == [("ok",)]
    assert q("select n_days, provisional from mart.mv_link_baseline") == [(1, True)]
    # 두 번째 적재: 행 수 불변, 수명은 갱신 가능
    before = {t: q(f"select count(*) from {t}")[0][0] for t in
              ("fact.traffic_speed", "fact.bus_position", "fact.bus_arrival", "fact.incident_observation", "fact.weather_obs", "fact.incident_episode", "ops.call_audit")}
    L.load_all(pg, tmp_path / "stage", refresh_mart=True)
    after = {t: q(f"select count(*) from {t}")[0][0] for t in before}
    assert before == after and q("select count(*) from ops.load_run")[0][0] == 2


def test_load_failure_is_recorded_and_other_tables_stay_consistent(spark, tmp_path, pg):
    _write_silver(spark, tmp_path / "silver")
    G.run(spark, GoldConfig(silver_root=str(tmp_path / "silver"), bronze_root=str(tmp_path / "nobronze"), stage_root=str(tmp_path / "stage")))
    bad = L.TableSpec("bus_position", "fact.no_such_table", ("dedup_key",), False)
    with pytest.raises(Exception):
        L.load_all(pg, tmp_path / "stage", specs=(L.SPECS[0], bad))
    assert pg.execute("select count(*) from fact.traffic_speed").fetchone()[0] == 2          # 앞 표는 끝나 있음
    st, err = pg.execute("select status, error from ops.load_run").fetchone()
    assert st == "failed" and err
