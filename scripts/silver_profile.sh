#!/usr/bin/env bash
# Bronze 프로파일링 (Step 5-0) — Silver 규칙을 정할 근거 숫자를 읽기 전용으로 뽑는다. 설명은 streaming/profile_bronze.py, 절차는 docs/runbooks/step5-silver.md.
# Bronze 쓰기 잡이 돌고 있는 중에도 실행할 수 있다 (별도 JVM 이 하나 더 떠서 잠시 메모리를 더 쓴다 — 약 1 GiB 추정).
# 코드는 이미지에 구워져 있으므로, 이 파일이나 streaming/ 이 바뀐 뒤에는 먼저 이미지를 다시 만든다:
#   docker compose --profile streaming build spark-bronze      (돌고 있는 spark-bronze 는 재시작되지 않는다)
# 결과 JSON 은 data/profile/ 에 저장된다 (gitignore 대상).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/profile
chmod a+rwx data/profile      # 컨테이너가 비루트(uid 185)라서 쓸 수 있게 한다
exec docker compose --profile streaming run --rm --no-deps -T \
  -v "$PWD/data/profile:/data/profile" \
  spark-bronze /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 768m /app/streaming/profile_bronze.py
