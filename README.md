# UrbanStream

서울시가 공개하는 **실시간 도로 소통·돌발·버스 위치·버스 도착·기상** Open API를 수집해
Kafka → Bronze → Silver → Gold(PostgreSQL/PostGIS)로 흘리고, 데이터 품질 검사·이상 탐지·모니터링을 거쳐
FastAPI와 대시보드로 보여 주는 **End-to-End 데이터 엔지니어링 포트폴리오 프로젝트**입니다.
(첫 번째 프로젝트 Olist 이커머스 배치 파이프라인의 뼈대를 이어받고, 정적 데이터·배치 전용이라는 한계를 보완하는 것이 목표.)

> **상태: Step 0~12 완료.** 남은 일은 포트폴리오 마무리(다이어그램·수치·데모)뿐입니다. 진행 이력은 `HANDOFF.md`, 미룬 일은 `docs/DEFERRED.md`.

## 아키텍처

```
공공 API 5종 ─▶ Collector ─▶ Kafka(KRaft) ─▶ Spark Streaming ─▶ Bronze(Parquet) ─▶ Silver(정제·중복제거·좌표변환)
(서울 도로·돌발·버스2, 기상청)   (재시도·회로차단·호출예산)   토픽 7개            ↓
                                                                      Airflow(DAG 11개) + Spark Batch
                                                                                   ↓
                              Tableau CSV ◀─ 대시보드(Streamlit) ◀─ FastAPI(25개, 읽기 전용) ◀─ Gold/Mart (PostgreSQL 17 + PostGIS)
                                                       DQ · 이상 탐지 · 모니터링 · Discord 알림
```

| 영역 | 구성 |
|---|---|
| 수집 | `collector/` — 소스별 스레드, 재시도·회로차단, 호출 예산, 원본 보관, DLQ·audit 토픽 |
| 스트리밍 | `streaming/` — Spark Structured Streaming(Kafka→Bronze, Bronze→Silver), 체크포인트 복구 |
| 배치 | `batch/`, `dags/` — Airflow(LocalExecutor) DAG 11개, 백필, 마트 갱신 |
| 저장 | `sql/` 마이그레이션 001~014 — Star Schema, 마트 뷰, 역할/권한(`api_service` 읽기 전용) |
| 품질·감시 | `src/urbanstream/{dq,monitor,notify}`, `config/{dq_rules,monitor,alerts}.yaml` — DQ 규칙, 이상 탐지, Discord 알림 |
| 서빙 | `api/` — FastAPI 25개 엔드포인트, `X-API-Key` 인증, 속도 제한, OpenAPI 스냅샷 잠금 |
| 화면 | `dashboard/` Streamlit, `tableau/` CSV 내보내기 |
| CI/CD | `.github/workflows/` — ci · weekly · release, 액션 SHA 고정, gitleaks, Terraform 검증, GHCR 이미지 |
| 클라우드 | `infra/terraform/` — Azure 증거 배포(VM 1대·NSG SSH 전용), 시연 후 destroy |

설계 결정은 `docs/adr/` (ADR-0001 ~ 0014)에 날짜·이유와 함께 남겼습니다.

## 검증 결과 (2026-11-17, 클린 환경 재현)

| 항목 | 결과 |
|---|---|
| pytest (Postgres 16 + PostGIS 연결) | **727 passed, 9 skipped** (skip: pyspark·airflow·pyproj 미설치, 클러스터 역할 시험 4건, zip 해제본의 git 검사) |
| 변이 시험 `scripts/mutation_ci.py` | 65개 변이 전부 잡힘 |
| ruff 게이트 | 통과 |
| OpenAPI 스냅샷 | 코드와 일치 (`scripts/export_openapi.py --check`) |
| 수집 실측 (11-03) | 호출 실패·재시도 0, 발행 수 = 레코드 수, 약 175건/분 |
| Azure 증거 배포 | 5일 가동, 비용 ₩17,587.77, apply → plan(No changes) → destroy → `az group exists` = false, 재배포 확인 |

Azure 증거 캡처 19장은 `docs/evidence/step12/` (구독 ID·메일·IP는 가림 처리).

## 시작하기

환경: Mac(Apple Silicon) · Docker · Conda · Python 3.11(CI는 3.12).

```zsh
conda create -n urbanstream python=3.11 -y
conda activate urbanstream
pip install -r requirements-dev.txt
cp .env.example .env            # SEOUL_API_KEY, DATA_GO_KR_KEY 를 채운다 (절대 커밋 금지)
scripts/stack.sh init-env       # 나머지 비밀 값 자동 생성
scripts/stack.sh up-dev         # PostGIS + Kafka + Kafka UI
python scripts/create_topics.py
python -m collector once        # 한 사이클 확인 → 상시: caffeinate -i python -m collector run
scripts/stack.sh up-silver      # Bronze·Silver
scripts/stack.sh up-airflow     # Airflow
scripts/stack.sh up-api         # http://127.0.0.1:8010/docs
scripts/stack.sh up-dashboard   # http://127.0.0.1:8501
scripts/stack.sh check          # 점검
```

시험: `python -m pytest` (네트워크·Docker 없이 대부분 통과). Postgres 시험은 `GOLD_TEST_DSN` 을 지정합니다.
`ALLOW_ROLE_TESTS=1` 은 일회용 DB에서만 쓰세요(클러스터 역할을 지웁니다).
Mac 전원·종료 절차는 `docs/runbooks/mac-on-off.md`.

## 디렉터리

```
src/urbanstream/  공통 라이브러리      collector/  API 수집·Kafka 발행     streaming/ batch/  Spark 잡
dags/             Airflow DAG          api/        FastAPI                  dashboard/  Streamlit
sql/              마이그레이션 001~014  config/     소스·토픽·DQ·알림 설정    monitoring/ 감시 안내(코드는 src/urbanstream/monitor)
scripts/          운영·CI·Terraform 도구  infra/terraform/  Azure IaC       docker/  이미지 정의
tests/            unit·integration·contract·e2e·recovery·spark    docs/  ADR·런북·증거·DEFERRED
```

## 알려진 한계

- 서울 열린데이터광장 호출 한도는 가정치(1,000건)이며 포털 통계와 대조하지 않았습니다. 수집 대상은 링크 4개·버스 노선 1·기상 격자 1로 의도적으로 작게 잡았습니다.
- 24/7 운영은 하지 않습니다. 클라우드는 "써 봤다"는 증거용 단기 배포였고, 평소 실행은 Mac입니다(Mac 잠자기 시 수집 중단 → 런북 참고).
- fact 테이블 파티션 없음(D-70), Olist 잔여물(D-227), Gold `gold_daily` 최신성(D-259) 등은 `docs/DEFERRED.md` 에 기록했습니다.
- 과거 커밋에 `.env.bak` 이 올라간 이력(D-195)이 남아 있습니다. **저장소를 공개하기 전에 키 폐기와 이력 정리를 결정해야 합니다.**

## 문서 안내

| 문서 | 용도 |
|---|---|
| `HANDOFF.md` | 전체 진행 이력과 규칙(새 채팅 인수인계) |
| `docs/DEFERRED.md` | 미룬 일·미확인 장부 |
| `docs/adr/` | 설계 결정 14건 |
| `docs/runbooks/` | 단계별 실행·복구 절차 |
| `docs/api-catalog/` | API 5종 필드·코드 실측 기록 |
| `docs/erd/gold.md` | Gold 스키마 |
| `docs/evidence/step12/` | Azure 배포 증거 |
