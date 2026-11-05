-- Step 8 Mac 적용 중 발견: DQ 규칙(mart_reliability)이 gold_writer 로 mart.v_congestion_threshold_latest 를 읽다가
-- "permission denied for view" (006 이 뷰 권한을 api_reader/bi_reader 에게만 줬다). 7c 의 D-114 와 같은 종류.
-- mart 의 기존 뷰 전부를 gold_writer 가 읽을 수 있게 한다 (읽기만; 쓰기는 기존대로 표에만).
GRANT SELECT ON mart.v_congestion_threshold_latest, mart.v_traffic_level, mart.v_bus_drift_daily, mart.v_bus_section_slowness,
                mart.v_traffic_latest, mart.v_weather_hourly, mart.v_incident_episode, mart.v_incident_impact_by_type,
                mart.v_weather_effect, mart.v_anomaly_recent
      TO gold_writer;
