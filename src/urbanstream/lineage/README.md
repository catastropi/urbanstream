# lineage

모든 레코드에 붙는 출처 추적 컬럼을 다루는 헬퍼.

- source_system, raw_uri, kafka_topic/partition/offset, bronze_batch_id, pipeline_run_id, pipeline_version(git sha), schema_version 등을 채워주는 공통 함수
- ops.run_registry 에 run_id -> 코드 버전/설정 해시/입력 범위를 기록

Blueprint 참고: I절, 원칙 8(lineage 추적)
