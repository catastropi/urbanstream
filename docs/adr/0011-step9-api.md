# ADR-0011 — Step 9: FastAPI 읽기 전용 서빙

날짜: 2026-11-05. 상태: 구현됨(샌드박스 검증 · Mac 확인 대기, `docs/DEFERRED.md` I장).
사용자 승인: 2026-11-05 ("기본 기본 그리고 이전 이후 스텝이랑 대조 시 이상 없으면 동의" — 기본값 = 응답 시각 UTC, 포트 8010, 버스 엔드포인트 이름 변경. 대조에서 나온 조정 1건은 D9-11).
사실 표기: **[샌드박스]** 이 환경(Python 3.11, Postgres 16 + PostGIS)에서 실행해 확인 / **[Mac 미확인]** 이미지 빌드·컨테이너 기동·Compose v2.10.2 / **[추정]** 근거만 있고 확인 전.

## 맥락
Step 6~8 이 Gold 표, 마트 뷰, ops 운영 뷰를 만들었고 `api_reader` 역할이 뷰·표 SELECT 를 가진다. Step 9 는 이것을 HTTP 로 내보낸다. Step 10(대시보드)이 이 API 만 읽는다. 쓰기 경로는 없다(수집기가 유일한 쓰기 경로).

## 결정

| ID | 결정 | 이유 | 한계 / 되돌리기 |
|---|---|---|---|
| D9-1 | 네 덩어리로 구현: 9a 로그인 역할(013), 9b 핵심(설정·DB·인증·오류·캐시·속도 제한), 9c 엔드포인트, 9d 컨테이너·계약 | 각각 독립 시험. 9a 만으로도 역할은 쓸모 있다 | — |
| D9-2 | 경로는 `/v1/...` 25개(아래 표). 읽는 곳은 **뷰와 마트·fact 표뿐**, ops 는 `v_*` 뷰만 | `api_reader` 최소 권한(009 규칙). 구조 가드 시험이 SQL 을 AST 로 검사한다 | 뷰를 새로 만들면 엔드포인트를 추가하거나 `EXCLUDED_VIEWS` 에 사유를 적어야 시험이 통과한다 |
| D9-3 | 로그인 역할 `api_service` (마이그레이션 **013**): `NOLOGIN IN ROLE api_reader`, 연결 8개, `statement_timeout 5s`, `lock_timeout 1s`, `idle_in_transaction 10s`, `default_transaction_read_only=on`. 비밀번호·LOGIN 은 마이그레이션이 아니라 `db_bootstrap.py --api` 가 `.env` 의 `API_DB_PASSWORD` 로 설정 | 새 GRANT 없음 → 권한은 `api_reader` 와 정확히 같다(시험). 비밀번호가 SQL·git 에 들어가지 않는다 | 되돌리기: `DROP ROLE api_service`(013 은 이미 적용된 파일이라 수정 금지, 새 번호로 취소). 비밀번호 교체: `.env` 값을 바꾸고 `db_bootstrap.py --api` + 컨테이너 재시작 |
| D9-4 | 앱도 연결 옵션을 직접 건다(같은 타임아웃·읽기 전용·`TimeZone=UTC`). 풀 최대 5(역할 상한 8 아래), 풀이 없는 DB 라도 앱은 뜨고 요청이 503 `database_unavailable` | 역할 설정이 빠져도(예: 다른 계정으로 접속) 보호가 유지된다. DB 가 늦게 떠도 API 가 크래시 루프에 빠지지 않는다 | `API_POOL_MAX`(1~8), `API_DB_EXTRA_OPTIONS` |
| D9-5 | 인증 = 헤더 `X-API-Key`. 키는 `.env` 의 `API_KEYS`(쉼표로 여러 개 — 교체 때 새 키를 먼저 추가) 24자 이상·중복 없음. 상수 시간 비교. 쿼리 문자열 키는 받지 않는다. `/health`·`/ready`·`/docs` 만 열려 있다 | 포트폴리오 수준에서 충분하고 URL·로그에 키가 남지 않는다. 키가 없으면 앱이 **기동을 거부**(fail-closed) | 사용자별 권한·자동 회전 없음(D-148). 문서(`/docs`)를 끄려면 `API_DOCS_ENABLED=false` |
| D9-6 | 응답 봉투 `{data, meta}`. `meta`: `as_of`(데이터 시각), `age_sec`, `stale`(기본 3시간, 운영 스냅샷 5분), `loaded_at`(Gold 적재 마지막 성공), `total`, `notes`(해석 주의 코드). 빈 결과는 빈 `data` + `notes: ["empty_result"]` | Gold 는 매시 20분 적재 → "현재" 값은 **실시간이 아니고 최대 약 80분 늦다**. 이것을 숨기면 대시보드가 거짓을 말한다 | 필드 추가는 호환, 삭제·개명은 `/v2`(D9-10) |
| D9-7 | 기간 `from`/`to`(오프셋 필수, `from` 포함 `to` 제외, 기본 최근 24시간, 최대 7일, 일 단위 마트는 기본 7일). 초과·역전은 잘라 주지 않고 422. 목록은 **키셋 커서**(`page.next_cursor`, 불투명). `limit` 상한: 목록 500·시계열 1000 | 큰 조회가 DB 를 붙잡지 않게. 오프셋 페이지는 뒤로 갈수록 느리고 중복·누락이 생긴다 | 커서 사이에 새 행이 들어오면 새 행은 보이지 않을 수 있다(D-155). 커서 값은 SQL 에 매개변수로만 들어간다 |
| D9-8 | 오류 = RFC 9457 `application/problem+json` (`type: urn:urbanstream:error:<code>`, `code`, `request_id`). 입력값·예외 문자열·SQL 은 응답에 넣지 않는다. `X-Request-ID` 는 형식에 맞을 때만 받고 아니면 새로 만든다 | 기계가 읽는 안정된 코드. 내부 정보가 새지 않는다(시험) | 코드 목록은 런북 §5 |
| D9-9 | 속도 제한: 키별 토큰 버킷(기본 분당 120, 순간 40), 인증 통과 뒤에만 센다. 캐시: 프로세스 안 TTL(경로+정렬된 쿼리가 키, 엔드포인트별 10~300초), 성공만 저장, **행이 300 개를 넘는 응답은 저장하지 않음**, 항목 128 개 상한. **워커 1개** | 상태가 프로세스 안에 있어 외부 저장소(Redis)가 필요 없다. 측정: 큰 응답을 캐시하면 RSS 가 249 MiB 까지 올랐고 위 제한 뒤 82 MiB | 재시작하면 초기화, 수평 확장 불가(D-150). `API_CACHE_TTL_SCALE=0` 으로 캐시 끔 |
| D9-10 | OpenAPI 스냅샷 `tests/contract/snapshots/openapi_v1.json`(`scripts/export_openapi.py`, `--check`). 코드와 다르면 시험이 실패. `operationId` = 함수 이름. 열거값(혼잡 단계·돌발 상태·이상 상태·전체 상태)과 `meta` 필드는 별도 시험으로 잠금 | Step 10 이 필드 이름·열거값에 기댄다. 의도하지 않은 계약 변경이 CI(Step 11)에서 걸린다 | 필드 삭제·개명·열거값 삭제 = 비호환 → 경로를 `/v2` 로 올리고 이 ADR 에 이유를 남긴다 |
| D9-11 | 컨테이너 `api`: 이미지 `urbanstream/api`(python:3.12-slim-bookworm + `requirements-api.txt` 고정 버전), profile `api`, 포트 `127.0.0.1:8010`, `mem_limit` **256m**(측정 RSS 82 MiB 의 약 3배), 읽기 전용 파일시스템, `cap_drop ALL`, `no-new-privileges`, 비루트(uid 10001), healthcheck `/health`. 환경 변수는 `API_*` 와 DB 호스트만(관리자·gold_loader·Airflow·웹훅 값 없음). **`${API_KEYS:-}`** — `:?` 로 필수 처리하지 않는다 | 포트 8000 은 Airflow 앞단(8080)·Spark UI 와 겹치지 않게 8010. `:?` 를 쓰면 `stack.sh ps/down/stats`(`--profile '*'`)가 API 키 없이는 **Step 2~8 명령까지 전부** 깨진다(대조에서 발견). 대신 앱이 빈 키·비밀번호면 기동을 거부하고 `stack.sh up-api` 가 미리 안내 | Mac 에서 `docker stats` 로 확인 후 `API_MEM_LIMIT`(D-147) |
| D9-12 | `/v1/traffic/current` 는 뷰를 통째로 읽지 않고 `dim_link` 를 기준으로 **`LATERAL (… LIMIT 1)`** 로 링크마다 부른다(같은 두 뷰 사용) | 합성 데이터(300링크·7일·150만 행)에서 뷰 전체 읽기는 **약 4.6~5.0초**(= 5초 제한, 데이터가 늘면 503) → LATERAL 은 약 50~70ms. `LIMIT 1` 이 없으면 플래너가 다시 전체 정렬로 되돌아간다(실행 계획 시험이 잡는다) | `dim_link` 에 없는 link_id 의 관측은 나오지 않는다(D-153). 링크가 수만 개로 늘면 재측정(15,300개 링크에서도 73ms) |
| D9-13 | 시각은 모두 ISO 8601 UTC(`Z`) 로 응답. 일 단위 필드(`event_date`)는 KST 날짜 그대로(마트 정의). 클라이언트(대시보드)가 KST 로 바꿔 보여 준다 | 오프셋이 있는 시각은 모호하지 않다. 마트의 날짜 경계는 이미 KST(ADR-0008) | `TZ` 를 바꾸지 않는다 |

## 엔드포인트 ↔ 원천

| 경로 | 읽는 곳 | 비고 |
|---|---|---|
| `GET /health`, `/ready` | — / `SELECT 1` | 인증 없음. `/health` 는 DB 를 보지 않는다 |
| `/v1/pipeline/health` | `ops.v_pipeline_health` | 전체 상태 = 가장 나쁜 것(down>degraded>unknown>ok), idle 은 제외, 빈 DB 는 `unknown` |
| `/v1/pipeline/sources` · `layers` · `marts` · `loads` · `gaps` · `alerts` | `ops.v_source_status` · `v_layer_status` · `v_mart_freshness` · `v_load_recent` · `v_data_gap_recent` · `v_alert_recent` | 운영 스냅샷은 5분 넘으면 `stale` |
| `/v1/dq/summary` · `/v1/dq/latest` | `ops.v_dq_summary` · `v_dq_latest` | `shadow` 규칙 표시. `include=detail` 일 때만 상세 JSON |
| `/v1/links` · `/v1/links/{id}` | `dim.dim_link` | 형상은 요청할 때만(GeoJSON) |
| `/v1/traffic/current` | `mart.v_traffic_level` + `v_traffic_latest` | 기준선 MV 가 아직 비어 있으면(`WITH NO DATA`) 기준선 열 없이 답하고 `baseline_unavailable` |
| `/v1/traffic/speeds` | `fact.traffic_speed` | 0 km/h·결측·`dq_flags` 를 그대로 노출 |
| `/v1/traffic/thresholds` | `mart.v_congestion_threshold_latest` | |
| `/v1/incidents` · `/active` · `/impact-by-type` | `mart.v_incident_episode` + `incident_impact` / `v_incident_impact_by_type` | 상태: listed · pending(해제 24시간 전) · finalized · excluded_long_term(7일 초과) |
| `/v1/bus/positions` | `fact.bus_position` | 수집 창(04:00~) 밖에는 비어 있을 수 있음 |
| `/v1/bus/arrival-drift` | `mart.v_bus_drift_daily` | **시간표 대비 지연이 아니다**(도착 예측 변동). 처음 제안의 `/delays` 에서 이름 변경 |
| `/v1/bus/section-slowness` | `mart.v_bus_section_slowness` | 구간 통과 시간 ÷ 평소(±90초 오차) |
| `/v1/weather/observations` · `/effect` | `mart.v_weather_hourly` · `v_weather_effect` | 연관이지 인과가 아님, `insufficient` 면 읽지 말 것 |
| `/v1/anomalies` | `mart.v_anomaly_recent` | 최근 7일 안에서만 |

## 검증 (샌드박스, Python 3.11 · Postgres 16 + PostGIS)
- `tests/api` 71개(핵심 DB 없음 / 실제 Postgres 전 엔드포인트·빈 DB·기준선 MV 미생성·경계·SQL 주입·커서 끝까지 / 구조 가드 / OpenAPI 계약). 전체 회귀 443 passed · 8 skipped(pyspark·airflow·pyproj 없음 + 역할 시험 4개는 `ALLOW_ROLE_TESTS=1` 이 없으면 건너뜀; 켜면 447 passed · 4 skipped) — 기존 365 + 4 skipped 가 그대로 초록. `tests/api`·컴포즈 정책은 Python 3.11·3.12·3.13 모두 통과.
- 변이 시험 **30건 전부 검출** (`scripts/mutation_api.py`: 인증·속도 제한·기간 경계·캐시·오류 노출·CORS·SQL 보간·ops 원본·쓰기 구문·커서·stale·기준선 MV·읽기 전용·실행 계획 되돌림 …). 시험이 놓쳤던 것 3건(동치 변이 1, 정의 오류 2)은 변이를 고쳐서 다시 확인.
- 부하(합성 데이터: 링크 300 · 속도 150만 행 · 버스 위치 67만 행 · 구간 통과 10만 행, 실제 uvicorn 1 워커, 동시 20개 × 30요청): 141 rps · p50 102ms · p95 192ms · 오류 0 · **RSS 시작 62 → 부하 후 82 MiB**. 캐시 적중 2~7ms, 캐시 없는 첫 요청 5~70ms. (샌드박스는 CPU 2개를 Postgres 와 나눠 쓴다. Mac 값은 다를 수 있다 — D-159.)
- `docker compose config -q` 통과(전체 프로파일, 비어 있는 `.env` 에서 API 항목 때문에 실패하지 않음).
- **Mac 미확인**: 이미지 빌드(python:3.12-slim-bookworm, 의존성 휠), 컨테이너 기동·healthy, Compose v2.10.2 동작, `docker stats` 메모리, Mac 의 Python 에서의 시험(샌드박스는 3.11·3.12·3.13 통과).
