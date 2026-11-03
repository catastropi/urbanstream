"""Spark 테스트 공통 fixture. pyspark 나 Java 가 없으면(호스트 Mac) 이 폴더의 테스트는 건너뛴다.

실행 방법
  - 컨테이너 안(권장, 운영과 같은 Spark/Java):  scripts/spark_test.sh
  - Java 와 pyspark 가 있는 곳:                 python -m pytest tests/spark
"""
from __future__ import annotations

import shutil

import pytest

pytest.importorskip("pyspark", reason="pyspark 없음 — scripts/spark_test.sh 로 컨테이너에서 실행하세요")

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if shutil.which("java") is None:
        pytest.skip("Java 없음 — scripts/spark_test.sh 로 컨테이너에서 실행하세요")
    s = (
        SparkSession.builder.master("local[2]").appName("urbanstream-tests")
        .config("spark.sql.session.timeZone", "UTC")          # bronze_writer 와 같은 설정
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
