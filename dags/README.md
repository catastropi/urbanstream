# dags

Airflow DAG. 로직은 여기 두지 않고 batch/ 의 함수를 얇게 호출만 한다
(기존 ecommerce-pipeline의 ecommerce_dag.py 패턴 계승 — pyspark 등 무거운 의존성은
태스크 콜러블 내부에서 import).

DAG 목록 (Blueprint M절): reference_data_sync, silver_reconcile_hourly, baseline_build,
gold_daily, incident_impact_finalize, weather_traffic_analysis, dq_report_daily,
backfill, replay_failed, warehouse_maintenance, platform_supervisor

Blueprint 참고: M절(Airflow DAG)
