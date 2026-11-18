#!/usr/bin/env bash
# tests/spark (+ pyspark 가 필요한 tests/unit/test_bronze_verify.py)를 Spark 이미지 안에서 실행한다 (운영과 같은 Spark/Java/Python — Mac 에 Java 를 설치하지 않아도 된다).
#   scripts/spark_test.sh            # 전부
#   scripts/spark_test.sh -k restart # pytest 인자는 그대로 넘어간다
# 먼저 이미지가 있어야 한다: scripts/stack.sh up-streaming (또는 docker compose --profile streaming build spark-bronze)
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose --profile streaming run --rm --no-deps -T \
  -v "$PWD/tests:/app/tests:ro" --entrypoint urbanstream-pytest spark-bronze -q /app/tests/spark /app/tests/unit/test_bronze_verify.py "$@"
