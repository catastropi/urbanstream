#!/usr/bin/env bash
# Bronze 가 Kafka 와 일치하는지 대조한다 (중복·유실·행 수). 종료 코드 0 = 통과. 자세한 설명은 streaming/verify_bronze.py.
# Bronze 쓰기 잡이 돌고 있는 중에도 실행할 수 있다 (별도 JVM 이 하나 더 떠서 잠시 메모리를 더 쓴다 — 약 1 GiB 추정).
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose --profile streaming run --rm --no-deps -T spark-bronze \
  /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 768m /app/streaming/verify_bronze.py
