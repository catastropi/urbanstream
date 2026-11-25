#!/usr/bin/env bash
# Bronze → Silver 행 수 등식·유실·초과를 대조한다. 종료 코드 0 = 통과. 자세한 설명은 streaming/silver_verify.py.
# 비교 구간: SILVER_VERIFY_LOOKBACK_DAYS=3 scripts/silver_verify.sh (없으면 전체 이력).
# Silver/Bronze 쓰기 잡이 돌고 있는 중에도 실행할 수 있다 (별도 JVM 이 잠시 메모리를 더 쓴다 — 약 1 GiB 추정).
set -euo pipefail
cd "$(dirname "$0")/.."
# 재처리로 게시된 날짜는 data/silver_rebuild 의 버전을 읽어야 한다(streaming/silver_read.py). 스트리밍 서비스 자체에는 이 폴더를 주지 않으므로 이 실행에만 읽기 전용으로 붙인다.
mkdir -p data/silver_rebuild
exec docker compose --profile silver run --rm --no-deps -T -e SILVER_VERIFY_LOOKBACK_DAYS -e SILVER_VERIFY_SINCE \
  -v "$PWD/data/silver_rebuild:/data/silver_rebuild:ro" spark-silver \
  /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 768m /app/streaming/silver_verify.py
