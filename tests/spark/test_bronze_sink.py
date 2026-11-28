"""Bronze 쓰기 테스트: 파일 sink + 체크포인트. Kafka 없이 `rate` 소스를 Kafka 열 모양으로 바꿔서 같은 쓰기 코드를 돌린다.

여기서 확인하는 것: 폴더 구조(event_date 분할), 체크포인트 재시작 때 중복·유실 없음, 읽기 설정.
배치 도중 프로세스가 kill -9 로 죽는 경우와 브로커 재시작은 실제 Kafka 로 test_crash_restart_kafka.py 가 본다 (STEP 3B R2·R4).
"""
from __future__ import annotations

import os
import time
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from streaming.bronze import BronzeConfig, BronzeTable, to_bronze, write_bronze  # noqa: E402

from spark_helpers import message_json, parse_sample  # noqa: E402

TABLE = BronzeTable("bus_position", "urban.test.v1", "bus_position")


def fake_kafka_stream(spark, message: str):
    """rate 소스(timestamp, value)를 Kafka 소스의 열 구성으로 바꾼다. offset = rate 의 value (증가하는 번호)."""
    rate = spark.readStream.format("rate").option("rowsPerSecond", 20).load()
    return rate.select(
        F.lit("k").cast("binary").alias("key"), F.lit(message).cast("binary").alias("value"),
        F.lit("urban.test.v1").alias("topic"), F.lit(0).alias("partition"),
        F.col("value").alias("offset"), F.col("timestamp"), F.lit(0).alias("timestampType"),
    )


@pytest.fixture
def cfg(tmp_path):
    return replace(BronzeConfig(), bronze_root=str(tmp_path / "bronze"), checkpoint_root=str(tmp_path / "ckpt"))


def run_until(spark, cfg, message, min_rows, timeout=90):
    """쿼리를 켜서 min_rows 행 이상 처리되면 stop() 으로 끊는다 (= 잡이 살았다가 죽는 것).
    stop() 은 진행 중인 배치를 중간에 끊을 수도 있다 → 재시작 때 같은 구간을 다시 처리하게 되는 상황까지 시험한다."""
    df = to_bronze(fake_kafka_stream(spark, message), TABLE)
    q = write_bronze(df, TABLE, cfg, trigger={"processingTime": "1 second"})
    deadline = time.time() + timeout
    while time.time() < deadline and q.isActive:
        if sum(p["numInputRows"] for p in q.recentProgress) >= min_rows:
            break
        time.sleep(0.5)
    q.stop()
    assert q.exception() is None


def test_layout_is_partitioned_by_event_date_with_sink_metadata(spark, cfg):
    msg = message_json(parse_sample("bus_position")[0])
    run_until(spark, cfg, msg, min_rows=20)
    out = Path(cfg.bronze_root) / "bus_position"
    assert (out / "_spark_metadata").is_dir()                      # 파일 sink 의 커밋 기록 (읽을 때 커밋된 파일만 보게 한다)
    assert (Path(cfg.checkpoint_root) / "bus_position").is_dir()
    parts = [p.name for p in out.iterdir() if p.name.startswith("event_date=")]
    assert parts, "event_date=YYYY-MM-DD 폴더가 있어야 합니다"
    df = spark.read.parquet(str(out))
    assert "event_date" in df.columns and "payload" in df.columns and "_raw_value" in df.columns
    assert df.filter(~F.col("_parse_ok")).count() == 0


def test_restart_resumes_from_checkpoint_without_duplicates_or_gaps(spark, cfg):
    msg = message_json(parse_sample("bus_position")[0])
    run_until(spark, cfg, msg, min_rows=40)
    n1 = spark.read.parquet(f"{cfg.bronze_root}/bus_position").count()
    assert n1 > 0

    time.sleep(2)                                                  # 죽어 있는 동안 rate 소스는 계속 행을 만든다 (= Kafka 에 쌓이는 메시지)
    run_until(spark, cfg, msg, min_rows=40)                        # 같은 체크포인트로 '재시작'
    offsets = sorted(r[0] for r in spark.read.parquet(f"{cfg.bronze_root}/bus_position").select("_kafka_offset").collect())

    assert len(offsets) > n1, "재시작 뒤에 새 데이터가 이어서 쓰여야 합니다"
    assert len(offsets) == len(set(offsets)), "재시작으로 같은 offset 이 두 번 쓰이면 안 됩니다 (중복)"
    assert offsets == list(range(offsets[0], offsets[0] + len(offsets))), "offset 사이에 빈 구간이 있으면 안 됩니다 (유실)"


@pytest.mark.skipif(os.environ.get("URBANSTREAM_EXPECT_KAFKA_CONNECTOR") != "1",
                    reason="Kafka 커넥터 jar 는 컨테이너 이미지에만 있다 (scripts/spark_test.sh 가 이 변수를 켠다)")
def test_kafka_connector_is_baked_into_the_image(spark):
    """jar 가 이미지에 들어 있으면 옵션 없이 읽을 때 '토픽 옵션을 지정하라'는 오류가 나고, 없으면 'Failed to find data source' 가 난다."""
    with pytest.raises(Exception) as e:
        spark.read.format("kafka").load()
    msg = str(e.value)
    assert "Failed to find data source" not in msg and "kafka" not in msg.split("data source")[0].lower() or "subscribe" in msg
