"""Silver 변환과 스트리밍 쿼리 조립 (Step 5, docs/adr/0006-step5-pre-decisions.md, 0007-silver-rules.md).

Silver 는 Bronze 를 읽어 "분석할 수 있는 모양"으로 만든다:
  - 읽을 수 없는 행(`_parse_ok` 가 거짓)은 제외한다 (Bronze 에 남아 있고, 행 수 등식으로 대조한다 — D-08).
  - 같은 사건의 재전송을 제거한다 (워터마크 안에서 `dedup_key`). 돌발은 관측 로그라서 (dedup_key, ingest_id) 로만 제거한다.
  - 0·결측은 **바꾸지 않고** `dq_flags` 에 이름만 단다 (ADR-0007 2-2).
  - 돌발 TM 좌표를 위경도로 바꾸고, 계획 기간·장기 여부를 계산한다.
  - 기상은 긴 형태(행 = 관측 항목 1개)를 유지한다 (스트리밍 pivot 불가).

변환 함수(`transform_*`)는 상태가 없어서 배치 DataFrame 으로 시험할 수 있다. 중복 제거(`dedup`)만 스트리밍에서 상태를 가진다.
"""
from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .bronze import _kst_date, to_bronze_typed
from .geo_tm import tm5181_to_wgs84
from .silver_config import SilverConfig, SilverTable, load_silver_tables

_KAFKA_SCHEMA = T.StructType([
    T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType()),
    T.StructField("timestampType", T.IntegerType()),
])

_COORD = T.StructType([T.StructField("lon", T.DoubleType()), T.StructField("lat", T.DoubleType())])
_tm_udf = F.udf(lambda x, y: tuple(tm5181_to_wgs84(x, y)), _COORD)


# ---------------------------------------------------------------- 입력 (Bronze 표)

def bronze_schema(spark: SparkSession, source: str) -> T.StructType:
    """Silver 의 입력 계약 = Bronze 스키마. Bronze 변환 함수에서 직접 얻는다 (손으로 복사하지 않는다)."""
    return to_bronze_typed(spark.createDataFrame([], _KAFKA_SCHEMA), source).schema


def read_bronze_stream(spark: SparkSession, source: str, cfg: SilverConfig) -> DataFrame:
    reader = spark.readStream.schema(bronze_schema(spark, source))
    if cfg.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", cfg.max_files_per_trigger)
    return reader.parquet(f"{cfg.bronze_root}/{source}")


# ---------------------------------------------------------------- 변환 (상태 없음)

def _flags(**conds: Column) -> Column:
    """조건이 참인 이름만 모은 배열 (없으면 빈 배열). NULL 조건은 거짓으로 본다."""
    return F.array_compact(F.array(*[F.when(c, F.lit(n)) for n, c in conds.items()]))


def _base(df: DataFrame) -> DataFrame:
    return df.filter(F.col("_parse_ok"))


def _common_cols() -> list[Column]:
    return [F.col(c) for c in ("event_id", "source_system", "dedup_key", "ingest_id", "raw_uri",
                               "event_time", "fetched_at", "ingested_at")]


def _lineage_cols() -> list[Column]:
    return [F.col("_kafka_partition"), F.col("_kafka_offset"), F.current_timestamp().alias("silver_processed_at")]


def _skew() -> Column:
    # Bronze 적재 시각이 수집 시각보다 앞서는 것은 불가능 → 컨테이너 시계가 어긋났던 행 (D-44). 정보용 표시일 뿐 판단에는 쓰지 않는다.
    return F.col("ingested_at") < F.col("fetched_at")


def transform_traffic(bronze: DataFrame) -> DataFrame:
    p = F.col("payload")
    flags = _flags(speed_zero=p["speed_kmh"] == 0, speed_null=p["speed_kmh"].isNull(),
                   travel_time_zero=p["travel_time_sec"] == 0, ingest_clock_skew=_skew())
    return _base(bronze).select(*_common_cols(), "payload.*", *_lineage_cols(), flags.alias("dq_flags"), "event_date")


def transform_position(bronze: DataFrame) -> DataFrame:
    flags = _flags(ingest_clock_skew=_skew())
    return _base(bronze).select(*_common_cols(), "payload.*", *_lineage_cols(), flags.alias("dq_flags"), "event_date")


def transform_arrival(bronze: DataFrame) -> DataFrame:
    p = F.col("payload")
    flags = _flags(arrive_zero=p["arrive_in_sec"] == 0, arrive_null=p["arrive_in_sec"].isNull(), ingest_clock_skew=_skew())
    return _base(bronze).select(*_common_cols(), "payload.*", *_lineage_cols(), flags.alias("dq_flags"), "event_date")


def transform_weather(bronze: DataFrame) -> DataFrame:
    p = F.col("payload")
    flags = _flags(value_unparsed=p["value_raw"].isNotNull() & p["value"].isNull(), ingest_clock_skew=_skew())
    return _base(bronze).select(*_common_cols(), "payload.*", *_lineage_cols(), flags.alias("dq_flags"), "event_date")


def transform_incident(bronze: DataFrame, long_term_days: float) -> DataFrame:
    p = F.col("payload")
    duration = (F.unix_seconds(p["expected_clear_at"]) - F.unix_seconds(p["occurred_at"])) / F.lit(86400.0)
    flags = _flags(
        no_expected_clear=p["expected_clear_at"].isNull(),
        no_occurred_at=p["occurred_at"].isNull(),
        negative_duration=duration < 0,
        no_coordinates=p["tm_x"].isNull() | p["tm_y"].isNull(),
        ingest_clock_skew=_skew(),
    )
    return (
        _base(bronze)
        .withColumn("_ll", _tm_udf(p["tm_x"], p["tm_y"]))
        .select(
            *_common_cols(), "payload.*",
            F.col("_ll.lon").alias("lon"), F.col("_ll.lat").alias("lat"),
            duration.alias("planned_duration_days"),
            F.coalesce(duration > F.lit(float(long_term_days)), F.lit(False)).alias("is_long_term"),
            *_lineage_cols(), flags.alias("dq_flags"),
            _kst_date(F.col("fetched_at")).alias("observed_date"),
        )
    )


def transform(bronze: DataFrame, table: SilverTable, cfg: SilverConfig) -> DataFrame:
    fn: dict[str, Callable[[], DataFrame]] = {
        "seoul_traffic": lambda: transform_traffic(bronze),
        "seoul_incident": lambda: transform_incident(bronze, cfg.long_term_days),
        "bus_position": lambda: transform_position(bronze),
        "bus_arrival": lambda: transform_arrival(bronze),
        "kma_ncst": lambda: transform_weather(bronze),
    }
    return fn[table.source]()


# ---------------------------------------------------------------- 중복 제거 (상태 있음, 스트리밍 전용)

def dedup(df: DataFrame, table: SilverTable) -> DataFrame:
    """워터마크 안에서 같은 키를 한 번만 통과시킨다. 워터마크가 지난 키는 상태에서 지워지므로 그 뒤에 다시 오면 통과한다 (ADR-0006 7-B)."""
    return df.withWatermark(table.watermark_col, table.watermark).dropDuplicatesWithinWatermark(list(table.dedup_keys))


# ---------------------------------------------------------------- 스트리밍 쿼리

def write_silver(df: DataFrame, table: SilverTable, cfg: SilverConfig, *, trigger: Optional[dict] = None):
    """파일 sink(Parquet, append) + 체크포인트. Bronze 와 같은 이유로 foreachBatch 를 쓰지 않는다 (재시도 시 중복)."""
    writer = (
        df.writeStream.format("parquet").outputMode("append")
        .option("path", f"{cfg.silver_root}/{table.name}")
        .option("checkpointLocation", f"{cfg.checkpoint_root}/{table.name}")
        .partitionBy(table.partition_col)
        .queryName(table.name)
    )
    return writer.trigger(**(trigger or {"processingTime": f"{cfg.trigger_sec} seconds"})).start()


def build(spark: SparkSession, table: SilverTable, cfg: SilverConfig) -> DataFrame:
    return dedup(transform(read_bronze_stream(spark, table.source, cfg), table, cfg), table)


def start_all(spark: SparkSession, cfg: SilverConfig) -> list:
    return [write_silver(build(spark, t, cfg), t, cfg) for t in load_silver_tables(cfg.config_dir)]
