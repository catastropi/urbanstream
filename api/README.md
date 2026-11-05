# api

읽기 전용 FastAPI 서빙 계층 (Step 9). 결정 근거 `docs/adr/0011-step9-api.md`, 실행 `docs/runbooks/step9-api.md`.

- `main.py` — `create_app()` 팩토리, 요청 ID·JSON 로그·CORS, 라우터 연결. 실행: `uvicorn api.main:app_factory --factory`
- `config.py` — 환경 변수 → `Settings`(키 24자 이상·중복 없음·비밀은 repr 에 안 나옴). 키·DB 비밀번호가 없으면 기동 거부
- `security.py` — `X-API-Key`(상수 시간 비교, 여러 키) + 키별 속도 제한. `/health`·`/ready` 외 모든 경로에 `Depends(guard)`
- `db.py` — 풀(`psycopg_pool`) + 오류 매핑(타임아웃·준비 전 마트·연결 불가·잘못된 값 → 안정된 problem 코드). **SQL 실행은 이 모듈만**
- `deps.py` — `Services`(설정·DB·캐시·속도 제한·시계), `meta()`(`as_of/age_sec/stale/loaded_at`), `respond()`(캐시 + Cache-Control)
- `pagination.py` — 기간 해석(오프셋 필수·최대 7일)·키셋 커서
- `errors.py` — RFC 9457 `application/problem+json`
- `schemas.py` — 응답 모델(열거값은 `Literal` — OpenAPI 스냅샷에 잠김)
- `routers/` — `system`(health/ready), `pipeline`(ops 뷰 + DQ), `traffic`, `incidents`, `bus`, `weather`, `anomalies`

규칙(시험이 지킨다 — `tests/api/test_guards.py`)
- SQL 은 고정 문자열 + 매개변수만(문자열 보간·연결 금지). 쓰기·DDL 키워드 없음.
- `ops` 는 `v_*` 뷰만. 새 뷰를 만들면 엔드포인트를 추가하거나 `EXCLUDED_VIEWS` 에 사유를 적는다.
- 응답 모양을 바꾸면 `python scripts/export_openapi.py` 로 스냅샷을 갱신하고 호환성을 ADR-0011 D9-10 대로 판단한다.
- 수동 주입 엔드포인트(POST 등)는 만들지 않는다 — 수집기가 유일한 쓰기 경로.

Blueprint 참고: P절(FastAPI Endpoint). 이전의 `models/`·`auth.py`·`dashboard/` 구상은 위 구조로 대체했다 (대시보드는 Step 10 에서 이 API 를 읽는다).
