# UrbanStream

> 실제 운영 중인 서울 도시 교통·대중교통·기상 Open API에서 발생하는 데이터를 Kafka 기반
> Streaming Pipeline과 Airflow/Spark 기반 Batch Pipeline으로 처리하고, Bronze/Silver/Gold
> 데이터 계층과 PostgreSQL Data Warehouse에 저장한 뒤 Data Quality, Anomaly Detection,
> Pipeline Monitoring, FastAPI Serving Layer 및 Tableau/실시간 Dashboard를 통해 도시 이동
> 문제를 탐지·분석하는 End-to-End Data Engineering Platform.

두 번째 포트폴리오 프로젝트. 첫 번째 프로젝트([ecommerce-pipeline](https://github.com/catastropi))의
기술적 뼈대를 계승하면서, 정적 데이터셋·Batch-only 구조라는 한계를 보완하는 것이 목표.

전체 설계는 별도 Architecture Blueprint 문서(대화 세션에서 작성, docs/ 에 아직 옮기지 않음) 기준이며,
이 저장소는 그 설계를 단계별로 구현하는 공간이다. **지금은 Step 3(Collector) 코드 완료 — Mac 에서 실제 한 사이클 확인 대기. Step 0 마무리 측정(갱신 주기)도 대기.**

## 지금 상태 (2026-11-02)

- [x] Architecture Blueprint 확정
- [x] 인증키 확보 — 서울 열린데이터광장(`SEOUL_API_KEY`), 공공데이터포털(`DATA_GO_KR_KEY`)
- [x] **Step 0 호출 검증** — 5개 소스 PASS: 서울 도로소통·돌발(XML), 서울 버스위치·도착(`ws.bus.go.kr`), 기상청 초단기실황
- [ ] Step 0 마무리 (사용자 Mac 에서 실행): 갱신 주기 `cadence`, 기상청 `kma-lag`, 호출 한도, 링크 지오메트리 → [런북](docs/runbooks/step0-closeout.md)
- [x] **Step 1** — [API 카탈로그](docs/api-catalog/README.md), envelope + 소스별 스키마 v1 + 파서(`src/urbanstream/contracts/`), 계약 테스트 → [ADR-0001](docs/adr/0001-envelope-event-time-dedup.md)
- [x] **Step 2** — Docker Compose 인프라 (PostGIS, Kafka KRaft, Kafka UI): [런북](docs/runbooks/step2-stack.md), [ADR-0003](docs/adr/0003-compose-infra.md). Mac 에서 기동·점검 8/8 통과, 메모리 실측 완료
- [x] **Step 3 (코드)** — Kafka 토픽 정의 + Collector(재시도·회로차단·호출 예산·원본 저장·DLQ·audit): [런북](docs/runbooks/step3-collector.md), [ADR-0004](docs/adr/0004-collector-topics.md). *실제 API→Kafka 한 사이클은 Mac 에서 확인 필요*
- [ ] 다음: Step 4 Raw/Bronze (Spark Structured Streaming)
- [ ] 이후 미착수: Bronze/Silver, DDL, Airflow/Spark Batch, DQ·모니터링, FastAPI, 대시보드, CI/CD

테스트: `pip install -r requirements-dev.txt && python -m pytest` (네트워크·Docker 없이 통과. 기록된 응답 샘플과 compose 규칙을 검증)

인프라 기동: `scripts/stack.sh init-env && scripts/stack.sh up && scripts/stack.sh check`

수집: `python scripts/create_topics.py && python -m collector once --dry-run` → `python -m collector once` → `python -m collector run`

## 환경

- Mac + PyCharm + Conda
- Python 3.11 (Step 0은 requests/python-dotenv만 필요. Kafka/Spark/Airflow 등은 이후 단계에서 추가)

## 시작하는 법

```zsh
conda create -n urbanstream python=3.11 -y
conda activate urbanstream
pip install -r requirements.txt
cp .env.example .env   # 발급받은 키를 채운다
```

자세한 절차는 **[docs/step0-guide.md](docs/step0-guide.md)** 를 그대로 따라가면 된다.

## 디렉터리 구조

```
urbanstream/
├── src/urbanstream/   # 공통 라이브러리 (contracts, dq, geo, lineage, logging, notify, config)
├── collector/         # API 폴링 + Kafka 발행
├── streaming/         # Spark Structured Streaming Job
├── batch/             # Spark Batch Job
├── dags/              # Airflow DAG (로직은 batch/ 호출)
├── api/               # FastAPI Serving Layer
├── sql/               # PostgreSQL DDL / View
├── config/            # 설정 값 파일 (sources.yaml, dq_rules.yaml, thresholds/)
├── data/              # raw / bronze / silver / gold (git에는 구조만, 내용은 .gitignore)
├── monitoring/         # Prometheus/Grafana 설정
├── tableau/            # BI 워크북
├── tests/              # unit / spark / contract / data / integration / e2e / recovery
├── docker/             # 이미지별 Dockerfile
├── docs/               # ADR, api-catalog, runbooks, 이 문서들
├── scripts/            # smoke_test.py 등 유틸리티
├── .github/workflows/  # CI/CD (아직 비어 있음)
├── docker-compose.yml  # 서비스 목록만 주석으로 적어둔 스켈레톤
├── requirements.txt
└── .env.example
```

각 폴더 안의 `README.md`가 그 폴더의 책임과 Blueprint 참고 절을 설명한다.
빈 폴더는 `.gitkeep`만 있으며, 코드는 "~부터 만들어" 식으로 하나씩 요청해서 채워나간다.
