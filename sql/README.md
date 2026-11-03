# sql

PostgreSQL DDL과 View.

- ddl/dim.sql, ddl/fact.sql, ddl/ops.sql — 스키마별로 분리
- partitions.sql — fact 테이블 선언적 파티션 생성/관리
- views.sql — mart.v_* (Tableau용 wide view, 기존 mart_views.sql 패턴 계승)
- roles.sql — streaming_writer / batch / api_reader(SELECT 전용) / bi_reader 최소 권한

Blueprint 참고: J절(Warehouse ERD), K절(Fact grain), 보안 요약
