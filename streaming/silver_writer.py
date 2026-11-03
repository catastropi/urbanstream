"""Silver 쓰기 잡 — data/bronze 5개 표를 읽어 data/silver/<표>/ 에 Parquet 으로 쌓는다.

실행(컨테이너 안): spark-submit --master local[2] --driver-memory 1g /app/streaming/silver_writer.py   (docker-compose.yml 의 spark-silver 서비스)
쿼리는 표마다 1개(총 5개)이고 한 JVM 안에서 같이 돈다. 하나라도 실패하면 프로세스가 비정상 종료 → 컨테이너가 재시작되고 체크포인트에서 이어간다.
"""
from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from streaming.silver import start_all
from streaming.silver_config import SilverConfig, load_silver_tables

log = logging.getLogger("silver_writer")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("urbanstream-silver")
        .config("spark.sql.session.timeZone", "UTC")                     # Bronze 와 같은 설정 (날짜 계산에서만 KST)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = SilverConfig.from_env()
    for t in load_silver_tables(cfg.config_dir):
        log.info("  %-16s -> %s/%s  워터마크 %s(%s) 중복키 %s", t.source, cfg.silver_root, t.name,
                 t.watermark_col, t.watermark, list(t.dedup_keys))
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    start_all(spark, cfg)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
