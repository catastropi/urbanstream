#!/usr/bin/env bash
# Bronze → Silver 행 수 등식·유실·초과를 대조한다. 종료 코드 0 = 통과. 자세한 설명은 streaming/silver_verify.py.
# 비교 구간: SILVER_VERIFY_LOOKBACK_DAYS=3 scripts/silver_verify.sh (없으면 전체 이력).
# Silver/Bronze 쓰기 잡이 돌고 있는 중에도 실행할 수 있다 (별도 JVM 이 잠시 메모리를 더 쓴다 — 약 1 GiB 추정).
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose --profile silver run --rm --no-deps -T -e SILVER_VERIFY_LOOKBACK_DAYS -e SILVER_VERIFY_SINCE spark-silver \
  /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 768m /app/streaming/silver_verify.py
