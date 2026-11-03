# batch

Spark Batch Job (Blueprint N절 기준). Airflow DAG(dags/)가 이 모듈의 함수를 호출한다.

- bronze_to_silver.py
- build_baseline.py — (링크, 요일유형, 시간대) baseline
- calibrate_thresholds.py — 혼잡 단계 임계값 버전 산출
- compute_congestion_history.py
- compute_incident_impact.py
- compute_bus_delay.py
- weather_traffic_join.py
- mart_refresh.py
- dq_profile.py

기존 ecommerce-pipeline의 dags/scripts/spark_transformations.py, spark_aggregations.py,
dw_loader.py 패턴(로직과 I/O 분리, upsert/truncate 구분)을 계승한다.

Blueprint 참고: N절(Spark Batch Job)
