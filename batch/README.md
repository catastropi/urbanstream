# batch

Airflow DAG(`dags/`)가 호출하는 Gold·마트·품질·이상 탐지 로직. DAG 는 얇게, 로직은 여기에 둔다. 결정 근거 `docs/adr/0008-gold-postgres.md`, `0009-step7-airflow.md`, `0010-step8-dq-monitoring.md`.

| 파일 | 역할 |
|---|---|
| `gold_config.py` | Gold 적재 설정(경로·표 목록) |
| `gold_stage.py` | Spark 스테이징: Silver → Parquet(비밀 없음) |
| `gold_load.py` | 호스트 COPY → `INSERT … ON CONFLICT` 적재 |
| `seed_dim_link.py` | `dim_link` 시드(LINK_VERTEX 엑셀) |
| `jobs.py` | DAG 가 부르는 함수(KST 윈도우, 실행별 스테이징, 행 수 검증, 정리) |
| `marts.py` | 마트 SQL: 혼잡 임계값, 버스 드리프트·구간, 돌발 영향, 날씨 효과 |
| `anomaly.py` | robust z-score 이상 탐지 |
| `dq_jobs.py` | DQ 규칙 실행·`silver_reconcile` |
| `maintenance.py` | ANALYZE·크기 기록·보관 기간 정리 |
| `db.py` | 연결·공통 SQL 헬퍼 |
