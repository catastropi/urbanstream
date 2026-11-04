#!/usr/bin/env bash
# Gold: ① Spark 스테이징(컨테이너, 비밀 없음) → ② 호스트 Python 로더(COPY→upsert) → ③ (선택) 마트 새로고침.
# 사용:  scripts/gold_run.sh [YYYY-MM-DD [YYYY-MM-DD]] [--refresh-mart]
#   날짜 두 개 = KST 날짜 범위(양 끝 포함), 하나 = 그 하루, 없음 = 전체. 같은 범위를 다시 돌려도 안전하다(멱등).
# 사전: python scripts/migrate.py 가 끝나 있어야 하고, Silver 가 한 번이라도 돌았어야 한다.
set -euo pipefail
cd "$(dirname "$0")/.."
dates=(); refresh=()
for a in "$@"; do
  case "$a" in
    --refresh-mart) refresh=(--refresh-mart) ;;
    ????-??-??) dates+=("$a") ;;
    *) echo "알 수 없는 인자: $a" >&2; exit 2 ;;
  esac
done
args=()
if [ "${#dates[@]}" -ge 1 ]; then args+=(--from "${dates[0]}" --to "${dates[${#dates[@]}-1]}"); fi
mkdir -p data/gold_stage
docker compose --profile gold run --rm --no-deps -T spark-gold \
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g /app/batch/gold_stage.py ${args[@]+"${args[@]}"}
python3 -m batch.gold_load --stage data/gold_stage ${refresh[@]+"${refresh[@]}"}
