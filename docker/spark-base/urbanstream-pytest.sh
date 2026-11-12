#!/bin/sh
# 컨테이너 안에서 pytest 를 실행한다. 공식 Spark 이미지는 pyspark/py4j 를 /opt/spark/python 에 두므로 경로를 맞춰 준다.
set -eu
PY4J="$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n 1)"
# 이미지의 pytest 는 구버전이라 pytest.ini 의 `pythonpath` 를 무시한다(Unknown config option) → tests/support(pg_support)를 직접 넣는다.
export PYTHONPATH="/app/src:/app:/app/tests/support:/opt/spark/python:${PY4J}"
export URBANSTREAM_EXPECT_KAFKA_CONNECTOR=1
# 구버전 pytest 는 pytest.ini 의 `-p layers_plugin` 도 PYTHONPATH 적용 전에 import 하려다 실패한다. 컨테이너 시험에는 계층 마커가 필요 없어 addopts 를 덮어쓴다.
exec python3 -m pytest -p no:cacheprovider -o addopts=-ra "$@"
