"""Bronze 파일 sink(`_spark_metadata` 포함) → Silver 스트림 → 검증까지 이어서 시험한다 (운영과 같은 폴더 모양).

Kafka 는 쓰지 않는다: 정답을 아는 Bronze 표를 파일 스트림으로 한 번 더 써서 Bronze 와 같은 sink 폴더를 만든다.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from streaming import silver as S  # noqa: E402
from streaming import silver_verify as V  # noqa: E402
from streaming.verify_bronze import read_bronze  # noqa: E402

from test_silver import at, bronze, cfg_for, table_of  # noqa: E402


def feed_bronze_sink(spark, tmp_path, source, df, batch):
    """df 를 스테이징 폴더에 쓰고, 그 폴더를 스트림으로 읽어 Bronze 폴더(파일 sink + 체크포인트)에 append 한다."""
    # 스테이징 폴더와 체크포인트는 소스마다 하나로 고정한다 (운영 Bronze 도 같은 체크포인트로 이어간다. 체크포인트를 바꾸면 sink 로그의 배치 번호가 겹쳐 새 배치가 건너뛰어진다)
    stage = tmp_path / "stage" / source
    df.write.mode("append").partitionBy("event_date").parquet(str(stage))
    q = (spark.readStream.schema(df.schema).parquet(str(stage))
         .writeStream.format("parquet").outputMode("append").partitionBy("event_date")
         .option("path", str(tmp_path / "bronze" / source))
         .option("checkpointLocation", str(tmp_path / "bronze_ckpt" / source))
         .trigger(availableNow=True).start())
    q.awaitTermination()


def run_silver(spark, tmp_path, source):
    cfg, t = cfg_for(tmp_path), table_of(source)
    S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"availableNow": True}).awaitTermination()
    return t, cfg


def pos_rows(start, n, ingest):
    return [{"dedup_key": f"v{start + i}", "event_time": at(60 * (start + i)), "fetched_at": at(60 * (start + i) + 5), "ingest_id": f"{ingest}{start + i}"}
            for i in range(n)]


def test_bronze_sink_to_silver_end_to_end_with_incremental_batches_and_verification(spark, tmp_path):
    src = "bus_position"
    b1 = bronze(spark, src, pos_rows(0, 40, "a"), raw=["not json"])                       # 40분 분량 + 읽을 수 없는 행 1개
    feed_bronze_sink(spark, tmp_path, src, b1, 1)
    t, cfg = run_silver(spark, tmp_path, src)
    silver = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    assert silver.count() == 40 and "payload" not in silver.columns
    r = V.compare(read_bronze(spark, f"{cfg.bronze_root}/{src}"), silver, t)
    assert r["ok"] and r["bronze_total"] == 41 and r["parse_failed"] == {"unparseable": 1}, r

    # 두 번째 Bronze 배치: 새 행 + 이미 본 키(정지한 차량) 한 개
    b2 = bronze(spark, src, pos_rows(40, 20, "b") + [{"dedup_key": "v39", "event_time": at(60 * 39), "fetched_at": at(60 * 60 + 5), "ingest_id": "b-again"}])
    feed_bronze_sink(spark, tmp_path, src, b2, 2)
    run_silver(spark, tmp_path, src)
    silver = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    assert silver.count() == 60                                                          # v39 는 다시 들어오지 않았다
    r = V.compare(read_bronze(spark, f"{cfg.bronze_root}/{src}"), silver, t)
    assert r["ok"] and r["missing"] == 0 and r["extra"] == 0 and r["dup_rows"] == 0, r

    run_silver(spark, tmp_path, src)                                                      # 입력이 없는 재시작
    assert read_bronze(spark, f"{cfg.silver_root}/{t.name}").count() == 60


def test_incident_end_to_end_keeps_each_observation_and_partitions_by_observed_date(spark, tmp_path):
    src = "seoul_incident"
    obs = [{"dedup_key": "acc-1", "ingest_id": f"call{i}", "event_time": at(0), "fetched_at": at(120 * i + 5),
            "payload": {"occurred_at": at(0), "expected_clear_at": at(30 * 86400), "tm_x": 198000.0, "tm_y": 451500.0}} for i in range(12)]
    feed_bronze_sink(spark, tmp_path, src, bronze(spark, src, obs), 1)
    t, cfg = run_silver(spark, tmp_path, src)
    silver = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    rows = silver.collect()
    assert len(rows) == 12 and all(r["is_long_term"] and abs(r["lat"] - 37.56303) < 1e-4 for r in rows)
    assert {str(r["observed_date"]) for r in rows} == {"2026-11-03"}
    assert V.compare(read_bronze(spark, f"{cfg.bronze_root}/{src}"), silver, t)["silver_rows"] >= 0
