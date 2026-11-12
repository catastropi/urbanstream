#!/usr/bin/env bash
# VM 안에서 실행한다 (scripts/cloud.sh up 이 ssh 로 부른다). Mac 에서 직접 실행하지 않는다.  Step 12, ADR-0014
#   scripts/cloud_remote.sh <스냅샷.sql.gz>
# 순서: .env 만들기(init-env) → postgres → 빈 DB 에 마이그레이션 001~014(D-202) → api_service 로그인 → 스냅샷 복원 → api·dashboard → 확인.
# - 비밀은 .env(VM, 권한 600)에만 만든다. 이 스크립트는 비밀 값을 출력하지 않고 set -x 를 쓰지 않는다.
# - 호스트 Python 의존(D-207)은 일회성 컨테이너(api 이미지: psycopg 포함)로 푼다.
set -euo pipefail
cd "$(dirname "$0")/.."

SNAP="${1:?사용법: scripts/cloud_remote.sh <스냅샷.sql.gz>}"
[ -f "$SNAP" ] || { echo "스냅샷 파일이 없습니다: $SNAP"; exit 2; }

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.cloud.yml --profile core --profile api --profile dashboard"
API_IMAGE="urbanstream/api:${API_IMAGE_TAG:-1.0.0}"
SCHEMAS="'dim','fact','mart','ops'"

PSQL() { docker exec -i urbanstream-postgres sh -c 'psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB"'; }

echo "[1/6] .env 만들기 (이미 있는 값은 유지, 비밀은 새로 생성)"
python3 scripts/stack_env.py
chmod 600 .env

echo "[2/6] postgres 기동"
$COMPOSE up -d --wait postgres

echo "[3/6] api 이미지 빌드 + 마이그레이션 001~014 (빈 DB, 일회성 컨테이너)"
$COMPOSE build api
docker run --rm --network urbanstream_net --env-file .env \
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -v "$PWD":/work:ro -w /work "$API_IMAGE" python scripts/migrate.py

echo "[4/6] api_service 로그인 설정"
docker run --rm --network urbanstream_net --env-file .env \
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -v "$PWD":/work:ro -w /work "$API_IMAGE" python scripts/db_bootstrap.py --api

echo "[5/6] 스냅샷 복원 (마이그레이션이 넣은 기본 행은 비우고 Mac 의 값으로 대체, ops.schema_migrations 는 건드리지 않음)"
PSQL <<SQL
DO \$\$
DECLARE r record;
BEGIN
  FOR r IN SELECT format('%I.%I', schemaname, tablename) AS t FROM pg_tables
           WHERE schemaname IN (${SCHEMAS}) AND NOT (schemaname = 'ops' AND tablename = 'schema_migrations')
  LOOP
    EXECUTE 'TRUNCATE TABLE ' || r.t || ' CASCADE';
  END LOOP;
END \$\$;
SQL
gunzip -c "$SNAP" | docker exec -i urbanstream-postgres sh -c 'psql -v ON_ERROR_STOP=1 -q -1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null
echo "    복원 완료"

echo "[6/6] api · dashboard 기동"
$COMPOSE up -d --build --wait api dashboard
$COMPOSE ps postgres api dashboard

echo "--- 확인 (VM 안, 127.0.0.1) ---"
curl -fsS http://127.0.0.1:8010/health; echo
curl -fsS http://127.0.0.1:8010/ready; echo
KEY="$(grep -E '^DASHBOARD_API_KEY=' .env | head -1 | cut -d= -f2-)"
curl -fsS -H "X-API-Key: ${KEY}" http://127.0.0.1:8010/v1/pipeline/health | python3 -c 'import json,sys; d=json.load(sys.stdin); print("pipeline overall:", d.get("data",{}).get("overall"), "| stale:", d.get("meta",{}).get("stale"))'
echo "완료. Mac 에서: scripts/cloud.sh tunnel"
