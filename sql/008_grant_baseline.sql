-- 008: 7c 빌더(incident_impact_finalize, weather_traffic_analysis)가 기준선 MV 를 읽는다.
-- 005 는 gold_writer 에게 dim/fact/ops 만 주었고 mart.mv_link_baseline 읽기는 빠져 있었다 (Mac 에서 permission denied 로 발견, 샌드박스 테스트는 관리자 계정이라 못 잡음).
-- 007 은 이미 적용된 파일이라 고치지 않고 새 마이그레이션으로 둔다.
GRANT SELECT ON mart.mv_link_baseline TO gold_writer;
