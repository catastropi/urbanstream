# sql

PostgreSQL 17 + PostGIS DDL. **번호 순서 마이그레이션**이며 `scripts/migrate.py` 가 적용한다 (ADR-0008 ⑦).

| 파일 | 내용 |
|---|---|
| 001_base.sql | postgis, 스키마 dim/fact/mart/ops, `ops.load_run` |
| 002_dims.sql | `dim_link`(선, 4326), `dim_date`(공휴일은 전부 false — 데이터 미확보), `dim_weather_category` |
| 003_facts.sql | fact 6개(PK = Silver `dedup_key` 등 자연키) + `ops.call_audit` |
| 004_mart.sql | `mv_link_baseline`(링크×요일×KST시각), 읽기 뷰 3개, `mart.refresh_all()` |
| 005_roles.sql | gold_writer / api_reader / bi_reader (NOLOGIN — 비밀번호는 운영자가 지정) |
| 006_mart_7b.sql | `congestion_threshold`(+`v_traffic_level`), `bus_arrival_drift`(+`v_bus_drift_daily`), `bus_section_time`(+`v_bus_section_slowness`) 와 GRANT (ADR-0009 §5) |
| 007_mart_7c.sql | `incident_impact`(+`v_incident_impact_by_type`), `weather_traffic_hourly`(+`v_weather_effect`), `ops.table_stats`, `ops.analyze_warehouse()` 와 GRANT (ADR-0009 §6) |
| 008_grant_baseline.sql | `gold_writer` 에게 `mart.mv_link_baseline` 읽기 권한 (7c 빌더가 기준선을 읽음) |
| 009_ops_monitor.sql | Step 8a: `ops.load_run.dag_id`, 감사 이벤트 거울·소스/레이어 상태·수집 공백·알림 표, 읽기 뷰(원본 표는 reader 에게 주지 않음) |
| 010_dq.sql | Step 8b: `ops.dq_result` + `v_dq_latest`·`v_dq_summary` |
| 011_anomaly.sql | Step 8c: `mart.anomaly_event` + `v_anomaly_recent` |
| 012_grant_mart_views.sql | Mac 에서 발견: `gold_writer` 가 mart 뷰를 읽지 못해 DQ 규칙 1개가 실패 → 기존 mart 뷰 전부 SELECT 부여 |

규칙: 이미 적용된 파일은 **수정하지 않는다**(체크섬 검사로 중단). 고칠 때는 다음 번호 파일을 추가한다.
`initdb.d` 는 쓰지 않는다. 이미 떠 있는 DB 에도 같은 명령으로 적용된다.

```zsh
python scripts/migrate.py --dry-run
```
