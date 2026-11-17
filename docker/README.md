# docker

서비스별 Dockerfile (docker-compose.yml 의 build 컨텍스트는 프로젝트 루트).

- `spark-base/` ✅ Step 4 — 공식 `apache/spark` 이미지 + Kafka 커넥터 jar(빌드 때 내장) + 우리 코드. 비루트 실행, `.env`·`data/` 는 `.dockerignore` 로 제외. 결정 근거 `docs/adr/0005-bronze-spark.md`
- `airflow/` ✅ Step 7 — 공식 Airflow 3.3.0 이미지 + JDK + pyspark 4.1.3 + psycopg (Spark 는 컨테이너 안 서브프로세스). `monitor` 서비스도 이 이미지를 쓴다
- `api/` ✅ Step 9 — FastAPI, 비루트, 버전 고정(`requirements-api.txt`)
- `dashboard/` ✅ Step 10 — Streamlit, 읽기 전용 파일시스템·uid 10001(`requirements-dashboard.txt`)
- `collector/` — 만들지 않았다. 수집기는 호스트(Mac)에서 `caffeinate -i python -m collector run` 으로 실행한다(ADR-0004)

Step 2 는 외부 이미지(PostGIS, Kafka, Kafka UI)만 써서 Dockerfile 이 없었다.

Blueprint 참고: W절(Docker Compose 서비스)
