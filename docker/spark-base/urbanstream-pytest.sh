#!/bin/sh
# 컨테이너 안에서 pytest 를 실행한다. 공식 Spark 이미지는 pyspark/py4j 를 /opt/spark/python 에 두므로 경로를 맞춰 준다.
set -eu
PY4J="$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n 1)"
export PYTHONPATH="/app/src:/app:/opt/spark/python:${PY4J}"
export URBANSTREAM_EXPECT_KAFKA_CONNECTOR=1
exec python3 -m pytest -p no:cacheprovider "$@"
