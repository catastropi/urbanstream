"""Bronze 변환과 스트리밍 쿼리 조립 (Step 4, docs/adr/0005-bronze-spark.md).

Bronze 는 "정제하지 않고 원래 모양 그대로" 쌓는 층이다. 이 모듈이 하는 일은 세 가지뿐이다.
  1) Kafka 메시지(JSON)를 계약 스키마로 **읽어 두되**, 읽을 수 없는 행도 버리지 않는다 (`_parse_ok`/`_parse_error` 로 표시).
  2) 원문(`_raw_value`)과 Kafka 위치(`_kafka_topic/partition/offset/timestamp/key`)를 함께 남긴다 (재처리·lineage).
  3) envelope 의 `ingested_at`(수집기가 비워 둔 값)을 Bronze 가 채운다.

중복 제거·시간 보정·0값 해석 같은 정제는 하지 않는다 (Silver, Step 5).

변환 함수(`to_bronze_typed`, `to_bronze_raw`)는 배치/스트리밍 DataFrame 둘 다에서 똑같이 동작한다.
그래서 테스트는 배치 DataFrame 으로 변환을, 파일 sink 로 쓰기·재시작을 따로 확인한다.
"""
from __future__ import annotations

import functools
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from urbanstream.contracts import SOURCES
from urbanstream.contracts.registry import schema_of
from urbanstream.contracts.spark_schema import ddl_of, required_paths

from .bronze_config import BronzeConfig, BronzeTable, load_tables  # noqa: F401  (다른 모듈이 bronze 에서 가져다 쓴다)

KST = "Asia/Seoul"
CORRUPT_COL = "_corrupt"   # from_json 이 읽기 실패한 행의 원문을 담는 임시 열 (최종 출력에는 넣지 않는다)


# ---------------------------------------------------------------- 변환

def _kafka_columns() -> list[Column]:
    """Kafka 소스가 주는 열 → Bronze 의 lineage 열. value 는 UTF-8 문자열로 보존한다."""
    return [
        F.col("value").cast("string").alias("_raw_value"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.col("key").cast("string").alias("_kafka_key"),
    ]


def _kst_date(ts: Column) -> Column:
    # 세션 시간대는 UTC 로 고정한다 (bronze_writer). from_utc_timestamp 로 KST 벽시계 시각으로 옮긴 뒤 날짜를 뗀다.
    return F.to_date(F.from_utc_timestamp(ts, KST))


def to_bronze_raw(kafka_df: DataFrame) -> DataFrame:
    """계약 스키마가 없는 토픽(dlq, audit): 원문 + Kafka 위치 + 적재 시각만 남긴다."""
    return kafka_df.select(
        *_kafka_columns(),
        F.current_timestamp().alias("ingested_at"),
        _kst_date(F.col("timestamp")).alias("event_date"),
    )


def to_bronze_typed(kafka_df: DataFrame, source_system: str) -> DataFrame:
    """계약 스키마가 있는 토픽: envelope 열(최상위) + payload(구조체) + 원문 + Kafka 위치 + 파싱 결과.

    `_parse_error`: NULL 이면 정상. 'unparseable'(JSON 이 아님/객체가 아님/잘림/계약 타입과 안 맞음) / 'schema_version_mismatch' / 'missing_required'(계약상
    NULL 이면 안 되는 필드가 비어 있음). 행은 어떤 경우에도 버리지 않는다 → Bronze 행 수 = Kafka 메시지 수.
    """
    expected_version = SOURCES[source_system].schema_version
    env_fields = [f["name"] for f in schema_of(source_system)["envelope"]]

    # PERMISSIVE 로 읽되, 읽기에 실패한 행의 원문을 `_corrupt` 에 담게 한다 (실측: JSON 이 아님/객체가 아님/잘림/계약 타입 불일치는
    # 구조체가 NULL 이 아니라 '필드가 전부 NULL 인 구조체'로 나오므로, `_corrupt` 없이는 `{}`(필드 누락)와 구분할 수 없다).
    parsed = kafka_df.select(*_kafka_columns()).withColumn(
        "_p",
        F.from_json(F.col("_raw_value"), ddl_of(source_system) + f", {CORRUPT_COL} STRING",
                    {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COL}),
    )

    missing = functools.reduce(lambda a, b: a | b, [F.col(f"_p.{p}").isNull() for p in required_paths(source_system)])
    error = (
        F.when(F.col("_p").isNull() | F.col(f"_p.{CORRUPT_COL}").isNotNull(), F.lit("unparseable"))
        .when(F.col("_p.schema_version").isNotNull() & (F.col("_p.schema_version") != expected_version),
              F.lit("schema_version_mismatch"))
        .when(missing, F.lit("missing_required"))
    )
    parsed = parsed.withColumn("_parse_error", error).withColumn("_parse_ok", F.col("_parse_error").isNull())

    # event_date: 정상 행은 사건 시각(event_time), 아닌 행은 Kafka 메시지 시각 (둘 다 KST 날짜)
    event_ts = F.when(F.col("_parse_ok"), F.col("_p.event_time")).otherwise(F.col("_kafka_timestamp"))

    envelope = [
        # 수집기는 ingested_at 을 비워 둔다 → Bronze 가 채운다. 이미 값이 있으면 보존.
        F.coalesce(F.col("_p.ingested_at"), F.current_timestamp()).alias("ingested_at") if n == "ingested_at"
        else F.col(f"_p.{n}").alias(n)
        for n in env_fields
    ]
    return parsed.select(
        *envelope,
        F.col("_p.payload").alias("payload"),
        F.col("_raw_value"), F.col("_kafka_topic"), F.col("_kafka_partition"), F.col("_kafka_offset"),
        F.col("_kafka_timestamp"), F.col("_kafka_key"),
        F.col("_parse_ok"), F.col("_parse_error"),
        _kst_date(event_ts).alias("event_date"),
    )


def to_bronze(kafka_df: DataFrame, table: BronzeTable) -> DataFrame:
    return to_bronze_raw(kafka_df) if table.source_system is None else to_bronze_typed(kafka_df, table.source_system)


# ---------------------------------------------------------------- 스트리밍 쿼리

def read_kafka(spark: SparkSession, table: BronzeTable, cfg: BronzeConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", table.topic)
        .option("startingOffsets", cfg.starting_offsets)
        # true(기본): 보관 기간이 지나 읽기도 전에 지워진 구간이 있으면 조용히 건너뛰지 않고 쿼리를 실패시킨다 (유실을 숨기지 않는다)
        .option("failOnDataLoss", "true")
    )
    if cfg.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
    return reader.load()


def write_bronze(df: DataFrame, table: BronzeTable, cfg: BronzeConfig, *, trigger: Optional[dict] = None):
    """파일 sink(Parquet, append) + 체크포인트. 파일 sink 는 체크포인트와 `_spark_metadata` 로 재시작 때 중복 없이 이어 쓴다.

    foreachBatch 를 쓰지 않는 이유: 배치가 재시도되면 같은 행이 두 번 쓰일 수 있어서 (ADR-0005 결정 4).
    """
    writer = (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{cfg.bronze_root}/{table.name}")
        .option("checkpointLocation", f"{cfg.checkpoint_root}/{table.name}")
        .partitionBy("event_date")
        .queryName(f"bronze_{table.name}")
    )
    return writer.trigger(**(trigger or {"processingTime": f"{cfg.trigger_sec} seconds"})).start()


def start_all(spark: SparkSession, cfg: BronzeConfig) -> list:
    queries = []
    for table in load_tables(cfg.config_dir):
        df = to_bronze(read_kafka(spark, table, cfg), table)
        queries.append(write_bronze(df, table, cfg))
    return queries
