"""Bronze 검증 잡 (Step 4 완료 기준 1·2) — Bronze 에 쌓인 것이 Kafka 와 일치하는지 대조한다.

토픽·파티션마다 다음을 본다 (모두 Bronze 의 `_kafka_partition/_kafka_offset` 열 = lineage 덕분에 가능):
  - 중복   : 행 수 - 서로 다른 offset 수 (0 이어야 함)
  - 유실   : (최대 offset - 최소 offset + 1) - 서로 다른 offset 수 (0 이어야 함. 이 수집기는 Kafka 트랜잭션을 안 쓰므로 offset 이 연속이다)
  - 대조   : Kafka 토픽 전체(모든 파티션)를 배치로 읽어, Bronze 의 [최소, 최대] offset 범위 안의 메시지 수 == Bronze 의 서로 다른 offset 수
  - 참고   : Bronze 시작 전에 Kafka 에 있던 메시지 수(= 백필 대상), 아직 Bronze 에 안 들어온 메시지 수(= 지연)
실패하면 종료 코드 1. 잡이 도는 중에도 실행할 수 있다 (Bronze 파일 sink 는 커밋된 파일만 읽힌다).

실행: scripts/bronze_verify.sh   (docs/runbooks/step4-bronze.md)
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.bronze_config import BronzeConfig, BronzeTable, load_tables


@dataclass(frozen=True)
class PartitionCheck:
    rows: int
    distinct: int
    lo: int
    hi: int

    @property
    def duplicates(self) -> int:
        return self.rows - self.distinct

    @property
    def gaps(self) -> int:
        return (self.hi - self.lo + 1) - self.distinct


def kafka_partition_view(spark: SparkSession, cfg: BronzeConfig, topic: str) -> DataFrame:
    """토픽 전체(모든 파티션, earliest → latest)의 (partition, offset). 파티션을 일부만 지정하면 Spark 가 거부한다
    (키가 한 파티션에만 몰려 Bronze 에 그 파티션만 있어도 Kafka 에는 파티션이 더 있다 — 2026-11-03 첫 실행에서 실패한 지점)."""
    return (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap).option("subscribe", topic)
            .option("startingOffsets", "earliest").option("endingOffsets", "latest")
            .load().select("partition", "offset"))


def compare_with_kafka(spark: SparkSession, kafka_df: DataFrame, ranges: dict[int, tuple[int, int]]) -> dict[int, dict]:
    """Kafka 의 파티션별 메시지 수를 Bronze 의 [최소, 최대] offset 범위 기준으로 나눈다.

    inside = 범위 안(Bronze 와 1:1 로 대조할 구간), before = 범위 앞(Bronze 시작 전), after = 범위 뒤 또는 Bronze 에 파티션 자체가 없는 경우(뒤처짐).
    kafka_df 는 (partition, offset) 열만 있으면 되므로 Kafka 없이도 시험할 수 있다 (tests/spark/test_verify_bronze.py).
    """
    rng = spark.createDataFrame([(p, lo, hi) for p, (lo, hi) in ranges.items()], "partition INT, lo LONG, hi LONG")
    j = kafka_df.join(F.broadcast(rng), "partition", "left")
    inside = F.col("offset").between(F.col("lo"), F.col("hi"))
    rows = (j.groupBy("partition").agg(
        F.count("*").alias("total"),
        F.sum(F.when(inside, 1).otherwise(0)).alias("inside"),
        F.sum(F.when(F.col("lo").isNull() | (F.col("offset") > F.col("hi")), 1).otherwise(0)).alias("after"),
        F.sum(F.when(F.col("offset") < F.col("lo"), 1).otherwise(0)).alias("before"),
    ).collect())
    return {r["partition"]: {"total": int(r["total"]), "inside": int(r["inside"]),
                             "before": int(r["before"]), "after": int(r["after"])} for r in rows}


def read_bronze(spark: SparkSession, path: str) -> Optional[DataFrame]:
    """Bronze 표 읽기. 폴더는 있는데 커밋된 Parquet 이 하나도 없으면 None.
    (쿼리는 시작할 때 `_spark_metadata` 와 함께 폴더를 만든다 → 메시지가 아직 없는 토픽은 빈 폴더다. 2026-11-03 첫 검증에서 이 경우가 오류가 됐다.)"""
    if not Path(path).exists():
        return None
    try:
        return spark.read.parquet(path)
    except AnalysisException as e:
        if "Unable to infer schema" in str(e) or "UNABLE_TO_INFER_SCHEMA" in str(e):
            return None
        raise


def verify_table(spark: SparkSession, cfg: BronzeConfig, table: BronzeTable) -> tuple[bool, str]:
    path = f"{cfg.bronze_root}/{table.name}"
    df = read_bronze(spark, path)
    if df is None:
        n = kafka_partition_view(spark, cfg, table.topic).count()
        if n == 0:
            return True, f"{table.name:<15} OK   Bronze 비어 있음 / Kafka 토픽도 비어 있음 (아직 메시지가 없는 토픽)"
        return False, (f"{table.name:<15} FAIL Bronze 에 파일이 없는데 Kafka 에는 {n:,}건 있음 "
                       f"— 쓰기 잡이 안 돌거나 첫 배치 전일 수 있음 (1~2분 뒤 다시, 계속되면 로그 확인)")
    agg = (df.groupBy("_kafka_partition")
           .agg(F.count("*").alias("rows"), F.countDistinct("_kafka_offset").alias("d"),
                F.min("_kafka_offset").alias("lo"), F.max("_kafka_offset").alias("hi"))
           .collect())
    checks = {r["_kafka_partition"]: PartitionCheck(r["rows"], r["d"], r["lo"], r["hi"]) for r in agg}
    if not checks:
        return False, f"{table.name:<15} Bronze 가 비어 있음"

    k = compare_with_kafka(spark, kafka_partition_view(spark, cfg, table.topic),
                           {p: (c.lo, c.hi) for p, c in checks.items()})
    in_kafka = sum(v["inside"] for v in k.values())
    before = sum(v["before"] for v in k.values())
    after = sum(v["after"] for v in k.values())
    orphan = {p: v["total"] for p, v in k.items() if p not in checks}      # Kafka 에는 있는데 Bronze 에는 아직 없는 파티션

    rows = sum(c.rows for c in checks.values())
    distinct = sum(c.distinct for c in checks.values())
    dup = sum(c.duplicates for c in checks.values())
    gaps = sum(c.gaps for c in checks.values())
    bad = df.filter(~F.col("_parse_ok")).count() if "_parse_ok" in df.columns else 0

    ok = dup == 0 and gaps == 0 and in_kafka == distinct
    extra = f" · Bronze 에 없는 파티션 {orphan}" if orphan else ""
    line = (f"{table.name:<15} {'OK  ' if ok else 'FAIL'} 파티션 {len(checks)}개  Bronze {rows:>8,}행 (offset {distinct:,}개)  "
            f"Kafka 같은 구간 {in_kafka:>8,}건  중복 {dup}  유실 {gaps}  | 참고: 시작 전 {before:,}건 · 뒤처짐 {after:,}건 · 파싱불가 {bad:,}행{extra}")
    return ok, line


def main() -> int:
    cfg = BronzeConfig.from_env()
    spark = (SparkSession.builder.appName("urbanstream-bronze-verify")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    results = []
    for t in load_tables(cfg.config_dir):
        try:
            results.append(verify_table(spark, cfg, t))
        except Exception as e:  # 한 표의 오류가 다른 표의 결과를 가리지 않게 한다
            first = (str(e).strip().splitlines() or [type(e).__name__])[0][:200]
            results.append((False, f"{t.name:<15} FAIL 검증 중 오류: {type(e).__name__}: {first}"))
    print("\n=== Bronze 검증 결과 ===")
    for _, line in results:
        print(line)
    all_ok = all(ok for ok, _ in results)
    print("\n전체:", "통과" if all_ok else "실패 (위 FAIL 줄 확인)")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
