# docker

서비스별 Dockerfile (docker-compose.yml 의 build 컨텍스트는 프로젝트 루트).

- `spark-base/` ✅ Step 4 — 공식 `apache/spark` 이미지 + Kafka 커넥터 jar(빌드 때 내장) + 우리 코드. 비루트 실행, `.env`·`data/` 는 `.dockerignore` 로 제외. 결정 근거 `docs/adr/0005-bronze-spark.md`
- `airflow/` (Step 7), `collector/`(후반, 지금은 호스트 실행), `api/` (Step 9) — 만드는 단계에서 추가

Step 2 는 외부 이미지(PostGIS, Kafka, Kafka UI)만 써서 Dockerfile 이 없었다.

Blueprint 참고: W절(Docker Compose 서비스)
