"""Spark 테스트 공통 fixture. pyspark 나 Java 가 없으면(호스트 Mac) 이 폴더의 테스트는 건너뛴다.

실행 방법
  - 컨테이너 안(권장, 운영과 같은 Spark/Java):  scripts/spark_test.sh
  - Java 와 pyspark 가 있는 곳:                 python -m pytest tests/spark
"""
from __future__ import annotations

import os
import shutil

import pytest

pytest.importorskip("pyspark", reason="pyspark 없음 — scripts/spark_test.sh 로 컨테이너에서 실행하세요")

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if shutil.which("java") is None:
        pytest.skip("Java 없음 — scripts/spark_test.sh 로 컨테이너에서 실행하세요")
    b = (
        SparkSession.builder.master("local[2]").appName("urbanstream-tests")
        .config("spark.sql.session.timeZone", "UTC")          # bronze_writer 와 같은 설정
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    # 컨테이너 밖에서 Kafka 시험을 돌릴 때만: Kafka 커넥터를 내려받는다 (운영 이미지는 jar 가 이미 들어 있다. STEP 3B)
    # 예) URBANSTREAM_TEST_SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3
    if os.environ.get("URBANSTREAM_TEST_SPARK_PACKAGES"):
        b = b.config("spark.jars.packages", os.environ["URBANSTREAM_TEST_SPARK_PACKAGES"])
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap(tmp_path_factory):
    """실제 Kafka. URBANSTREAM_TEST_KAFKA_BOOTSTRAP 이 있으면 그것(예: 운영 이미지 안에서 kafka:19092, 시험 토픽만 쓴다),
    없고 URBANSTREAM_TEST_KAFKA_HOME 이 있으면 시험 전용 브로커를 띄운다. 둘 다 없으면 건너뛴다 (STEP 3B)."""
    if os.environ.get("URBANSTREAM_TEST_KAFKA_BOOTSTRAP"):
        yield os.environ["URBANSTREAM_TEST_KAFKA_BOOTSTRAP"]
        return
    if not os.environ.get("URBANSTREAM_TEST_KAFKA_HOME"):
        pytest.skip("URBANSTREAM_TEST_KAFKA_BOOTSTRAP·URBANSTREAM_TEST_KAFKA_HOME 이 없다 (실제 Kafka 가 필요한 시험)")
    pytest.importorskip("confluent_kafka")
    from kafka_broker import Broker
    b = Broker(tmp_path_factory.mktemp("kafka")).start()
    yield b.bootstrap
    b.stop()
