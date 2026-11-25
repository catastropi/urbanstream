# Step 9 런북 — FastAPI 읽기 전용 API

결정 근거: `docs/adr/0011-step9-api.md`. 남은 것: `docs/DEFERRED.md` I장(D-146~D-160).
**사용자가 Mac 에서 실행하는 순서입니다.** 명령은 한 줄씩이고, 비밀 값(`.env` 의 `API_KEYS`, `API_DB_PASSWORD`)은 **채팅·git 에 붙이지 마세요.**
상태 표기: 샌드박스에서 확인한 것은 ADR 에, 아래 각 단계의 "기대 결과"는 **[Mac 미확인]** 입니다.

## 0. 압축 풀기

```zsh
unzip -o urbanstream_full.zip -d .
```

`.env` 와 `data/` 는 zip 에 없으므로 건드리지 않습니다. 이번에 바뀐 것: `sql/013_api_login.sql`(새), `api/`(새), `docker/api/`(새), `docker-compose.yml`(서비스 `api` 추가), `scripts/stack.sh`(`up-api`), `scripts/db_bootstrap.py`(`--api`), `scripts/stack_env.py`(키 생성), `requirements-api.txt`·`requirements-dev.txt`.

## 1. `.env` 에 API 항목 채우기

```zsh
scripts/stack.sh init-env
```

기대 결과: "빈 값을 채운 키: API_DB_PASSWORD, API_KEYS" 처럼 **이름만** 출력됩니다(값은 출력하지 않음). 이미 있는 값은 바뀌지 않습니다. `API_PORT=8010` 도 들어갑니다.
키를 여러 개 쓰려면 `.env` 의 `API_KEYS=` 를 쉼표로 이어 쓰세요(각 24자 이상). 클라이언트별로 키를 나누면 로그의 `key_id`(키의 해시 앞 8자)로 구분됩니다.

## 2. 마이그레이션 013 적용 + 기동

```zsh
scripts/stack.sh up-api
```

이 명령이 하는 일: 키가 비어 있으면 안내하고 멈춤 → Postgres 기동 → `migrate.py`(013 적용) → `db_bootstrap.py --api`(`api_service` 에 비밀번호·LOGIN 설정) → 이미지 빌드(첫 빌드는 몇 분) → `api` 기동 → healthy 대기.
기대 결과: `docker compose ps` 에서 `urbanstream-api` 가 `healthy`. **[Mac 미확인: 이미지 빌드, Compose v2.10.2]**

## 3. 점검 (curl)

키를 쉘 변수에 담습니다(화면에 출력하지 않음).

```zsh
export K="$(grep '^API_KEYS=' .env | cut -d= -f2 | cut -d, -f1)"
```

살아 있는지(인증 불필요):

```zsh
curl -s http://127.0.0.1:8010/health
```

기대: `{"status":"ok","version":"1.0.0"}`

DB 까지(인증 불필요):

```zsh
curl -s http://127.0.0.1:8010/ready
```

기대: `{"status":"ok","db":"ok"}`

키 없이 호출하면 거절돼야 합니다:

```zsh
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/v1/pipeline/health
```

기대: `401`

파이프라인 건강(키 필요):

```zsh
curl -s -H "X-API-Key: $K" http://127.0.0.1:8010/v1/pipeline/health
```

기대: `data.overall` 이 `ok`/`degraded`/… 중 하나, `meta.loaded_at` 에 마지막 Gold 적재 시각.

혼잡 현황(상위 3개 링크만 보기):

```zsh
curl -s -H "X-API-Key: $K" "http://127.0.0.1:8010/v1/traffic/current?limit=3"
```

기대: `meta.notes` 에 `baseline_unavailable`(기준선 MV 를 아직 새로고침하지 않았다면) 또는 `baseline_provisional`(기준선 28일 미만 — 2026-11-30 이전에는 정상). **값이 실시간이 아닙니다**: `meta.age_sec`/`stale` 을 보세요.

브라우저에서 `http://127.0.0.1:8010/docs` 를 열고 오른쪽 위 **Authorize** 에 키를 넣으면 모든 엔드포인트를 실행해 볼 수 있습니다.

## 4. 메모리 확인 (D-147)

```zsh
docker stats --no-stream urbanstream-api
```

샌드박스 측정은 부하 후 약 82 MiB 입니다(한도 256m). Mac 값이 한도의 70%를 넘으면 `.env` 에 `API_MEM_LIMIT=384m` 를 쓰고 `scripts/stack.sh up-api` 를 다시 실행하세요.

## 5. 오류 코드 (응답 `code`)

| code | 상태 | 뜻 / 할 일 |
|---|---|---|
| `missing_api_key` · `invalid_api_key` | 401 | 헤더 `X-API-Key` 확인 |
| `rate_limited` | 429 | `Retry-After` 초 뒤에 다시. 한도는 `.env` 의 `API_RATE_LIMIT_PER_MIN`/`API_RATE_BURST` |
| `validation_error` · `invalid_cursor` · `invalid_parameter` | 422 | 매개변수. `errors` 에 위치와 이유 |
| `naive_datetime` | 422 | `from`/`to` 에 오프셋이 없음. URL 에서는 `+` 를 `%2B` 로 (`2026-11-05T09:00:00%2B09:00`) |
| `window_too_large` · `invalid_window` | 422 | 기간은 최대 7일, `from` < `to` |
| `link_not_found` | 404 | 없는 링크 |
| `query_timeout` | 503 | 조회가 5초를 넘음. 기간·limit 를 줄이기 (반복되면 알려 주세요) |
| `data_not_ready` | 503 | 마트가 아직 채워지지 않음 (첫 `baseline_build` 전) |
| `database_unavailable` | 503 | DB 에 연결 불가. `docker compose ps` 로 postgres 확인 |
| `internal_error` | 500 | 응답의 `request_id` 로 `docker compose logs api` 에서 찾기 |

## 6. 테스트 (선택 — 코드를 고칠 때)

```zsh
pip install -r requirements-dev.txt
```

DB 없이 도는 것(핵심·구조 가드·OpenAPI 계약):

```zsh
python -m pytest -q tests/api
```

DB 가 필요한 것은 `GOLD_TEST_DSN` 이 있어야 돌고, 없으면 건너뜁니다. **이 시험은 해당 DB 의 `dim/fact/mart/ops` 스키마를 지웁니다** — 운영 DB(`urbanstream`)를 가리키지 말고 테스트용 DB 를 따로 만드세요(Step 6 런북 §6 과 같음).

```zsh
docker exec urbanstream-postgres sh -c 'psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE ut_test"'
```

```zsh
docker exec urbanstream-postgres sh -c 'psql -U "$POSTGRES_USER" -d ut_test -c "CREATE EXTENSION IF NOT EXISTS postgis"'
```

```zsh
GOLD_TEST_DSN="host=127.0.0.1 port=5433 dbname=ut_test user=urbanstream password=$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2)" python -m pytest -q tests/api
```

**주의 — `tests/integration/test_api_role.py` 는 기본으로 건너뜁니다.** 이 시험은 역할 `api_service` 를 지우고 다시 만드는데, 역할은 DB 가 아니라 **클러스터 전체**에 속하므로 운영 Postgres 와 같은 클러스터에서 돌리면 운영 API 의 비밀번호가 사라집니다(그러면 `scripts/stack.sh up-api` 를 다시 실행해야 복구). 돌리려면 일회용 Postgres 에서만 `ALLOW_ROLE_TESTS=1` 을 붙이세요.

OpenAPI 명세를 의도적으로 바꿨다면:

```zsh
python scripts/export_openapi.py
```

(스냅샷이 코드와 같은지만 보려면 `python scripts/export_openapi.py --check`.) 필드·열거값을 지우거나 이름을 바꾸는 것은 비호환 변경이므로 ADR-0011 D9-10 을 따릅니다.

변이 시험(코드를 일부러 망가뜨려 시험이 잡는지 확인, 약 5분, 위 `GOLD_TEST_DSN` 필요):

```zsh
GOLD_TEST_DSN="host=127.0.0.1 port=5433 dbname=ut_test user=urbanstream password=$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2)" python scripts/mutation_api.py
```

## 7. 키·비밀번호 바꾸기

- API 키 교체: `.env` 의 `API_KEYS=새키,옛키` 로 두 개를 잠시 같이 쓰고 → `scripts/stack.sh up-api` → 클라이언트를 새 키로 바꾼 뒤 → 옛 키를 지우고 다시 `up-api`.
- DB 비밀번호 교체: `.env` 의 `API_DB_PASSWORD` 를 바꾸고 `scripts/stack.sh up-api` (역할 비밀번호를 다시 설정하고 컨테이너를 새 값으로 재기동).

## 되돌리기 / 끄기
- API 만 끄기: `docker compose --profile core --profile api stop api`. 데이터·다른 서비스에는 영향이 없습니다.
- 문서 화면만 끄기: `.env` 에 `API_DOCS_ENABLED=false`.
- 캐시 끄기(문제 분리용): `.env` 에 `API_CACHE_TTL_SCALE=0`.
- 로그인 역할 제거(013 취소): psql 에서 `DROP ROLE api_service;` — 013 파일 자체는 수정하지 않습니다(이미 적용된 마이그레이션은 체크섬이 고정).
