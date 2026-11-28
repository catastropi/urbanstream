# 테스트 구성과 실행 기록

이 문서의 숫자는 손으로 적지 않는다. 표 두 개(계층별 수, 필요한 환경별 수)는 `scripts/test_inventory.py readme --write` 가 코드에서 세어 쓰고,
CI 의 hygiene job 이 `readme --check` 로 코드와 같은지 확인한다. 실행 결과(passed/skipped)는 환경마다 다르므로 아래 기록 표에 날짜·환경과 함께 남긴다.

## 1. 계층별 테스트 함수 수

`def test_` 개수다(파라미터 확장 전). 설치된 패키지와 상관없이 같은 값이 나온다.

<!-- tests:testing:start -->
| 계층 | 테스트 함수 |
|---|---:|
| unit | 525 |
| contract | 50 |
| integration | 93 |
| spark | 123 |
| e2e | 4 |
| recovery | 47 |
| api | 59 |
| dashboard | 75 |
| 합계 | 976 |

`data` 표시가 붙은 테스트 55개는 위 계층에 이미 들어 있다(다시 분류한 것이라 합계에 더하지 않는다).

| 필요한 것 (없으면 skip) | 테스트 함수 |
|---|---:|
| 없음 | 660 |
| PostgreSQL | 183 |
| Spark | 102 |
| Spark+PostgreSQL | 16 |
| Kafka | 10 |
| Spark+Kafka | 5 |

| 뮤테이션 스크립트 | 항목 |
|---|---:|
| `scripts/mutation_api.py` | 30 |
| `scripts/mutation_dashboard.py` | 33 |
| `scripts/mutation_ci.py` | 65 |
| `scripts/mutation_data.py` | 27 |
<!-- tests:testing:end -->

"필요한 것"은 파일 단위로 판단한다. `pg_support` 나 `GOLD_TEST_DSN` 을 쓰면 PostgreSQL, `tests/spark/` 아래면 Spark, 시험용 Kafka 브로커를 쓰면 Kafka 다.
그 환경이 없으면 해당 테스트는 실패하지 않고 skip 된다. 그래서 Mac 기본 환경에서 돌린 숫자와 CI 숫자는 다르다.

## 2. 계층마다 실제로 확인하는 것

| 계층 | 위치 | 진짜로 쓰는 것 | 흉내 내는 것 | 확인하는 것 |
|---|---|---|---|---|
| unit | `tests/unit/` | 파이썬 코드 | 시계, 파일 시스템(tmp), HTTP 응답, Gold 실행기 요청 파일 | 설정 해석, 수집기 흐름, DAG 정책, 증분 진행 기록, 재처리 포인터, compose·워크플로 정책 |
| contract | `tests/contract/` | 실제 API 응답을 녹화한 파일 | 외부 API 호출 | 소스 5종 응답이 계약(스키마)과 맞는지 |
| integration | `tests/integration/` | PostgreSQL 17 + PostGIS (CI), 마이그레이션 001~015 | 없음 | Gold 적재, 범위 교체, DQ 규칙, 마트, 역할 권한, 작업 잠금 |
| spark | `tests/spark/` | Spark 4.1.3 (local), 일부는 PostgreSQL, 일부는 시험용 Kafka 3.9.2 | 운영 Kafka 대신 시험 토픽 | Bronze·Silver 변환과 중복 제거, 재처리 빌드, Gold 스테이징, 재시작 뒤 이어 쓰기 |
| recovery | `tests/recovery/` | 복구 스크립트 | `docker` 명령(가짜 docker), Kafka 는 weekly 에서만 진짜 | `recover.py`, `host_watch.py`, `drill.py` 의 판단과 순서 |
| api | `tests/api/` | FastAPI TestClient, 일부는 PostgreSQL | 없음 | 인증, 속도 제한, 페이지, 경계값, SQL 주입 방어, OpenAPI 스냅샷 |
| dashboard | `tests/dashboard/` | Streamlit AppTest | API 응답 | 화면 구성, 호출 예산, 시간 표시 |
| e2e | `tests/e2e/` | 실제 API 서버 + PostgreSQL + 대시보드 클라이언트 | 없음 | 세 부분이 함께 동작하는지 |

`data` 표시는 데이터 품질과 마트 값을 보는 테스트를 다시 분류한 것이다(`tests/support/layers_plugin.py` 의 `DATA_FILES`). 다른 계층에 이미 들어 있다.

## 3. 뮤테이션 테스트

코드를 일부러 한 곳씩 망가뜨린 사본에서 지정한 테스트를 돌려, 테스트가 실패하면 '잡힘'이다. 원본은 건드리지 않는다.

| 스크립트 | 대상 | 언제 도는지 |
|---|---|---|
| `scripts/mutation_data.py` | 재처리(Silver 포인터, 실행 이력, Gold 범위 교체), 중복 제거, 평소 적재의 불변성, DQ 판정, 증분 스테이징, 작업 잠금 | CI: 데이터 코드가 바뀐 push(`mutation-data`, Spark 항목 제외). weekly: Spark 포함 전체 |
| `scripts/mutation_api.py` | API 인증·제한·페이지·SQL | CI: API 가 바뀐 push. weekly: 전체 |
| `scripts/mutation_dashboard.py` | 대시보드 | CI: 매 push |
| `scripts/mutation_ci.py` | 워크플로, gitleaks, compose, Terraform 같은 CI 안전장치 | weekly (STEP 5 전에는 손으로만 돌렸다) |

`mutation_ci.py` 의 항목은 CI 설정을 지키는 장치이고 데이터 처리 로직이 아니다. 데이터 로직은 `mutation_data.py` 가 맡는다.

`mutation_data.py` 를 처음 만들었을 때(2026-11-28) 27건 중 2건을 테스트가 잡지 못했다. 둘 다 테스트를 보강했다.

| 못 잡은 뮤테이션 | 원인 | 보강 |
|---|---|---|
| 평소 적재의 사실 표를 `DO UPDATE` 로 바꿈 | 불변성을 확인하는 테스트는 있었는데(`test_replace_deletes_only_the_range…`), 뮤테이션이 다른 파일을 가리켰다 | 뮤테이션의 대상 테스트를 고침 |
| DQ `volume_drift` 기준선에 검사하는 날을 포함 | 기준 일수가 정확히 모자라는 경우를 보는 테스트가 없었다 | `test_volume_drift_baseline_does_not_count_the_checked_day` |

## 4. 실행 기록

새로 돌릴 때마다 한 줄씩 위에 더한다. CI 실행은 Actions 실행 링크를 적는다(각 job 의 실행 요약에 계층별 표가 있다).
로컬 실행은 `python -m pytest -q --junitxml=/tmp/r.xml` 뒤 `python scripts/test_inventory.py summarize /tmp/r.xml` 출력을 옮긴다.

| 날짜 | 어디서 | 명령 | 결과 | 비고 |
|---|---|---|---|---|
| 2026-11-28 | 클라우드 Linux, Python 3.12, PostgreSQL 16 + PostGIS 3, Spark 4.1.3 (local, Java 21) | `pytest -m "not spark"` + `pytest tests/spark` (Kafka 시험 제외) | 1,059 passed / 3 skipped, Spark 151 passed / 6 skipped, 실패 0 | STEP 5 적용본. PG 16 이라 CI(17)와 다르다 |
| 2026-11-28 | 같은 환경 | `scripts/mutation_data.py` | 잡힘 27 / 못 잡음 0 | 보강 전 첫 실행은 25 / 2 |
| 2026-11-28 | 같은 환경 | `scripts/mutation_api.py`, `scripts/mutation_dashboard.py` | 잡힘 30 / 30, 33 / 33 | |
| 2026-11-28 | 같은 환경 | `scripts/mutation_ci.py` | 잡힘 65 / 못 잡음 0 | ci.yml 이 바뀌어 정의 2건을 고친 뒤 |
