# config (값)

실제 설정 "값" 파일들. 코드(src/urbanstream/config/)가 이 파일들을 읽는다.

- sources.yaml — 소스별 폴링 간격, 호출 예산, 엔드포인트
- dq_rules.yaml — Data Quality 규칙 정의
- thresholds/ — 혼잡 단계 임계값 버전 (dim_congestion_level)

기존 ecommerce-pipeline의 config.yaml(+ .env 분리) 패턴을 계승한다.
