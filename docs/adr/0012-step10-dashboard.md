# ADR-0012 — Step 10: 대시보드(Streamlit)와 Tableau CSV

날짜: 2026-11-05. 상태: 구현됨(샌드박스 검증 · Mac 확인 대기, `docs/DEFERRED.md` J장).
사용자 승인: 2026-11-05 ("스트림릿, A, 승인" — 대시보드 = Streamlit, Tableau = 옵션 A(로컬 CSV 생성, 게시는 사용자가 라이선스·약관을 확인한 뒤 직접), 대시보드 전용 API 키 분리 승인).
사실 표기: **[샌드박스]** 이 환경(Python 3.12 venv, 가짜 API, 헤드리스 Chrome)에서 실행해 확인 / **[Mac 미확인]** 이미지 빌드·컨테이너 기동·실제 데이터·Compose v2.10.2 / **[추정]** 근거만 있고 확인 전.

## 맥락
Step 9 가 `/v1/...` 25개를 닫힌 계약(OpenAPI 스냅샷)으로 내놓았다. Step 10 은 이것**만** 읽어 화면을 만든다. 데이터는 링크 4개·버스 노선 1개(753)·날씨 격자 1개이고, 임계값(기준선)은 약 11-30 까지 잠정이다. 그래서 화면의 첫 번째 일은 "예쁘게"가 아니라 **"모르는 것을 모른다고 말하는 것"**이다.

## 결정

| ID | 결정 | 이유 | 한계 / 되돌리기 |
|---|---|---|---|
| D10-1 | 대시보드 = **Streamlit**(1.66.1, Python ≥3.11). 데이터 입구는 **FastAPI(`X-API-Key`) 하나뿐**, DB 직접 연결 없음 | 사용자 승인. Python 만으로 지도·차트·시험(AppTest)이 된다. DB 를 직접 보면 Step 9 의 계약·캐시·속도 제한·최소 권한을 우회한다. 구조 시험이 대시보드의 import 를 검사해 DB 드라이버·`psycopg`·`sqlalchemy` 를 막는다 | 화면이 느리거나 불편하면 프레임워크만 교체(`dashboard/client.py`·`shape.py` 는 Streamlit 을 import 하지 않는다 → 재사용) |
| D10-2 | 화면 7개, 각각 "질문 한 줄 + 이 화면에서 낼 수 있는 결론 한 줄"로 시작: 개요 · 지도 · 혼잡(A) · 돌발 영향(B) · 버스(C) · 날씨(D) · 이상·건강(E·F) | HANDOFF 의 분석 문제 A~F 와 1:1. 화면마다 "이걸로 무엇을 결론 내릴 수 있나"를 적어 두지 않으면 값만 나열된다 | 문제 ↔ 화면 표는 런북 §3 |
| D10-3 | **대시보드 전용 키** `DASHBOARD_API_KEY`. `init-env` 가 만들고 `API_KEYS` 목록 **끝에 덧붙인다**(추가 전용). 컨테이너는 `API_KEYS` 전체를 받지 않고 자기 키 하나만 받는다 | 로그의 `key_id` 로 대시보드 호출을 구분하고, 유출·회전 때 다른 클라이언트와 따로 다룬다. 사용자 승인된 **init-env 규칙의 유일한 예외**("이미 있는 값은 바꾸지 않는다" → 목록 끝에 `,키` 추가만, 기존 키는 한 글자도 안 바뀜, 두 번째 실행은 변화 없음, 값은 출력 안 함 — 시험) | 키 교체: `.env` 의 `DASHBOARD_API_KEY` 를 지우고 `init-env` → 옛 키는 `API_KEYS` 에서 직접 제거 |
| D10-4 | 클라이언트(`dashboard/client.py`): **호출 예산**이 설계의 중심. 자료 종류별 캐시 수명 `ops 60s · gold 300s · mart 900s · static 3600s`(API 자체 캐시 10~300s 와 맞춤), 프로세스 하나에 클라이언트 하나(`st.cache_resource`, 모든 탭·세션이 캐시 공유), 같은 키 동시 요청은 한 번만(single-flight), 429 는 `Retry-After ≤ 5s` 일 때 1회만 재시도, 클라이언트 쪽 상한 **분당 100**(서버 한도 120 아래), 실패하면 옛 값을 "갱신 실패" 표시와 함께 보여 줌(인증·4xx 는 가리지 않음) | 서버 한도는 키당 분당 120·순간 40. 개요를 60초마다 새로 고치는 화면이 여러 탭에서 그대로 API 를 때리면 한도에 걸린다 | 측정: 모든 화면 처음 열기 ≤25회, 안정 상태 ≤12회/분 [샌드박스]. `DASHBOARD_TTL_SCALE` 로 배율 |
| D10-4a | 조회 기간 `from`/`to` 는 **300초 단위로 내림**(`floor_to`) | 초 단위로 달라지는 `from`/`to` 는 캐시 키가 매번 바뀌어 **캐시가 한 번도 안 맞고** 예산만 탄다(설계 중 발견). 시험(`test_views_budget`)과 변이가 잡는다 | 데이터 시각은 5분 단위로 그려지므로 정보 손실 없음 |
| D10-5 | 시각: 응답은 UTC `Z`, 화면은 **고정 UTC+9(KST)**(`tzdata` 의존 없음). 오프셋 없는 시각은 거부. `event_date`·`kst_hour` 는 이미 KST 라 변환하지 않음(구조 시험). 나이(age)는 API 의 `age_sec` 가 아니라 **지금 − as_of 로 다시 계산**, 신선도 판정은 API `meta.stale` 이 **기준**(null 이면 판정하지 않음) | 캐시된 응답의 `age_sec` 는 받은 순간 값이라 화면에 오래 떠 있으면 거짓이 된다. 한국은 일광절약시간이 없다 | stale 판정 지연: 캐시 수명(최대 15분)만큼 늦게 바뀔 수 있다(J장) |
| D10-6 | **정직한 표시 규칙**: ① 단계(`level`)가 null 이면 "◇ 판정 불가(표본 부족)" — 원활·0 이 아니다 ② Gold "현재"는 **최대 약 80분 늦다**는 안내를 표시 ③ `meta.notes` 코드를 한글로 풀어 표시(모르는 코드는 원문 그대로) ④ 결측 속도는 선을 끊고 0 으로 그리지 않음 ⑤ 날씨 = **연관이지 인과 아님**, 표본 부족 구간은 해석 금지 ⑥ 버스 드리프트 = 마지막−처음 예측 도착(양수=늦어짐), **시간표 지연 아님** ⑦ 상태 색(good/warning/serious/critical)은 항상 기호+글자와 함께, 이중 축 금지, 범주 색 blue #2a78d6 · orange #eb6834 ⑧ 차량번호·차량 ID 는 어느 화면에도 없음(구조 시험이 `plain_no`·`veh_id` 사용을 막는다) | 데이터가 적을 때 가장 쉬운 거짓말은 "문제없음"이다 | 문구는 `dashboard/freshness.py`·`shape.py` 한 곳 |
| D10-7 | 컨테이너 `dashboard`: 이미지 `urbanstream/dashboard`(python:3.12-slim-bookworm + `requirements-dashboard.txt` 정확한 `==` 고정), profile `dashboard`, 포트 `127.0.0.1:8501`, `mem_limit` **512m [추정]**, 읽기 전용 파일시스템 + `/tmp` tmpfs, `HOME=/tmp`, `cap_drop ALL`, `no-new-privileges`, 비루트(uid 10001), healthcheck `/_stcore/health`, `depends_on api: service_healthy`. 환경 변수는 `DASHBOARD_*` 와 `TZ` 뿐(DB·Airflow·웹훅·`API_KEYS` 없음). **`${DASHBOARD_API_KEY:-}`**(`:?` 금지) | `:?` 면 `stack.sh ps/down/stats`(`--profile '*'`)가 키 없이 **Step 2~9 명령까지** 깨진다(D-157 과 같은 이유). 대신 앱이 키가 비면 기동을 거부(fail-closed)하고 `stack.sh up-dashboard` 가 미리 안내 | 512m 는 Streamlit+pandas+pydeck 에 대한 **추정** — Mac `docker stats` 로 확인 후 `DASHBOARD_MEM_LIMIT` |
| D10-8 | Tableau = **옵션 A**: `scripts/export_tableau.py` 가 API 에서 **집계 CSV 5개 + manifest.json** 을 `tableau/export/`(git 제외)에 쓴다. **게시(업로드)는 하지 않는다.** 사용자가 라이선스·약관·공개 범위를 확인한 뒤 직접 올린다 | 사용자는 Tableau Desktop 이 없고 Tableau Public 은 DB 에 붙을 수 없다. Tableau Public 게시물은 **누구나 볼 수 있다**(비공개 없음) → 원천 데이터 이용 조건 확인 전에는 자동 게시가 위험하다. 이 스크립트는 `127.0.0.1` API 만 호출한다(그 밖의 주소는 거부) | 열: `link_speed_hourly`(링크×KST 날짜×시, 결측 제외 평균)·`congestion_threshold`·`incident_impact_by_type`·`weather_effect`·`bus_arrival_drift_hourly`. 버스는 마트 값(p50/p90)을 다시 집계하지 않고 **시간 단위 그대로**(백분위를 평균 내면 틀리므로). 차량·운영 지표는 고정 열 목록과 시험으로 배제 |
| D10-9 | 검증: 단위·구조·AppTest 111개(`tests/dashboard`) + 내보내기 19개 + compose/env/gitignore 시험 추가분 + **변이 31건**(`scripts/mutation_dashboard.py`, 약 15초, Postgres·Docker 불필요) | HANDOFF 검증 습관 ②. 변이가 의도한 시험에서 실패하는지(엉뚱한 import 오류가 아닌지) 일부 직접 확인 | CI(Step 11)에 `tests/dashboard` + `mutation_dashboard.py` 를 넣는다 |

| D10-10 | **버스 엔드포인트는 `route_id` 필수** → 설정 `DASHBOARD_BUS_ROUTE_ID`(기본 `100100118` = 753번; compose 는 `.env` 의 `SEOUL_BUS_ROUTE_ID` 를 그대로 전달, 비밀 아님). 화면(버스·지도)과 내보내기가 같은 값을 쓴다. 가짜 API 는 이제 **OpenAPI 스냅샷의 필수·정수 범위·최대 길이·모르는 매개변수를 실제 API 처럼 검사**해 422 를 돌려준다 | **Mac 실제 API 에서 지도·버스 화면과 내보내기가 422 `validation_error`** 로 실패해 발견(11-06). 처음 가짜 API 는 너그러워 `route_id` 누락을 못 잡았고, 계약 시험은 "필드 이름"만 대조하고 "필수 매개변수"는 대조하지 않았다 | 노선을 늘리려면 API 에 노선 목록이 필요하다(지금은 노선 1개). 변이 3건이 이 누락을 잡는다 |

## 호환성 대조 (Step 2~9)
- `docker-compose.yml`: `dashboard` 서비스만 추가. 정확한 서비스 집합 시험·`--profile '*'` 정책(`:-`) 유지. [샌드박스: 시험 통과 · Mac `docker compose config` 미확인]
- API: **변경 없음**(OpenAPI 스냅샷 `export_openapi.py --check` 통과). 대시보드가 읽는 필드는 `dashboard/contract.py` 에 선언하고 스냅샷과 대조하는 시험이 있다 — API 가 필드를 바꾸면 대시보드 시험이 먼저 실패한다.
- `.env.example` 스택 구간에 `DASHBOARD_PORT`·`DASHBOARD_API_KEY` 추가(init-env 시험 갱신). `.dockerignore` 는 그대로(`docs/`·`tests/`·`.env`·`data/` 제외 — 이미지는 `dashboard/` 만 복사).
- `.gitignore`: `tableau/export/` 추가(시험).
