# dags

Airflow DAG. 로직은 여기 두지 않고 batch/ 의 함수를 얇게 호출만 한다
(기존 ecommerce-pipeline의 ecommerce_dag.py 패턴 계승 — pyspark 등 무거운 의존성은
태스크 콜러블 내부에서 import).

DAG 목록 (Blueprint M절): reference_data_sync, silver_reconcile_hourly, baseline_build,
gold_daily, incident_impact_finalize, weather_traffic_analysis, dq_report_daily,
backfill, replay_failed, warehouse_maintenance, platform_supervisor

Blueprint 참고: M절(Airflow DAG)

## 구현 상태 (2026-11-05, 11개)
gold_intraday(:20) · gold_daily(01:00) · backfill(수동) · bus_delay(02:00) · baseline_build(일 02:30) · incident_impact_finalize(03:00) · weather_traffic_analysis(03:30) · warehouse_maintenance(일 04:00) · **silver_reconcile_daily(04:30, pool gold_io)** · **dq_report_daily(05:00)** · **anomaly_scan(:35)** · **reprocess(수동, STEP 1: Bronze→Silver 재생성 + Gold 범위 교체, `docs/runbooks/reprocess.md`)**.
아직 없음: reference_data_sync(D-101), replay_failed, platform_supervisor(→ `monitor` 컨테이너로 대체, ADR-0010). `silver_reconcile_hourly` 는 일 단위(`silver_reconcile_daily`)로 바꿨다(Spark 비용).
