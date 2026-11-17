# lineage (예약된 자리 — 코드 없음)

출처 추적용 공통 헬퍼를 둘 계획이었으나 별도 모듈은 만들지 않았다. 출처 추적은 데이터 열로 구현되어 있다.

- Bronze/Silver 열: `event_id`, `raw_uri`, `_kafka_topic/partition/offset`, `ingest_id`, `silver_processed_at` (ADR-0001, ADR-0005)
- Gold: `ops.load_run`(적재 실행 기록, `dag_id` 포함), `ops.call_audit` (ADR-0008, `sql/001_base.sql`·`009_ops_monitor.sql`)

`pipeline_version`(git sha)·`run_registry` 는 구현하지 않았다.
