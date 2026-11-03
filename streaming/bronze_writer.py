"""Bronze 쓰기 잡 — Kafka 토픽 7개를 읽어 data/bronze/<이름>/event_date=YYYY-MM-DD/ 에 Parquet 으로 쌓는다.

실행(컨테이너 안): spark-submit --master local[2] --driver-memory 1g /app/streaming/bronze_writer.py   (docker-compose.yml 의 spark-bronze 서비스)
설정은 환경변수 BRONZE_* (streaming/bronze.py 의 BronzeConfig). 키·비밀은 쓰지 않는다 (Kafka 에 인증이 없다).

쿼리는 토픽마다 1개(총 7개)이고 한 JVM 안에서 같이 돈다. 하나라도 실패하면 이 프로세스가 비정상 종료 → 컨테이너가 재시작되고
체크포인트에서 이어간다 (ADR-0005 결정 3).
"""
from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from streaming.bronze import start_all
from streaming.bronze_config import BronzeConfig, load_tables

log = logging.getLogger("bronze_writer")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("urbanstream-bronze")
        # 시각은 UTC 로 두고 날짜 계산에서만 KST 로 옮긴다 (bronze._kst_date). 서버 시간대에 따라 결과가 달라지지 않게 고정.
        .config("spark.sql.session.timeZone", "UTC")
        # 기본 200 은 단일 노드 local 모드에서 낭비다 (Bronze 는 셔플이 거의 없다). 메모리·작은 파일 억제용.
        .config("spark.sql.shuffle.partitions", "4")
        # SIGTERM(docker stop) 때 진행 중인 배치를 마치고 종료
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = BronzeConfig.from_env()
    tables = load_tables(cfg.config_dir)
    log.info("Bronze 시작: bootstrap=%s starting=%s trigger=%ss maxOffsets=%s 토픽 %d개",
             cfg.bootstrap, cfg.starting_offsets, cfg.trigger_sec, cfg.max_offsets_per_trigger, len(tables))
    for t in tables:
        log.info("  %-40s -> %s/%s  (%s)", t.topic, cfg.bronze_root, t.name,
                 "계약 스키마로 파싱" if t.source_system else "원문만 보존")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    start_all(spark, cfg)
    spark.streams.awaitAnyTermination()   # 쿼리 하나가 실패하면 예외가 올라온다 → 비정상 종료
    return 0


if __name__ == "__main__":
    sys.exit(main())
