# config (값)

실제 설정 "값" 파일들. 코드(src/urbanstream/config/)가 이 파일들을 읽는다.

- sources.yaml — 소스별 폴링 간격, 호출 예산, 엔드포인트
- dq_rules.yaml — Data Quality 규칙 정의
- thresholds/ — 혼잡 단계 임계값 버전 (dim_congestion_level)
- recovery.yaml — STEP 3B 복구·감시·훈련: resume 이 켜는 순서, host_watch 기준(Docker 응답·컨테이너·monitor 박동·시계·Airflow 메모리), diagnose 기준, 훈련별 최대 시간·수집 공백 상한 (scripts/recover.py·host_watch.py·drill.py 가 읽는다)
- measure.yaml — STEP 2 성능 측정: 경로, 합성 데이터 날짜·규모, 실행 전 검사·실행 중 감시 기준, 금지 시간대, 반복 횟수, 판단 기준 (scripts/measure_*.py 가 읽는다)

기존 ecommerce-pipeline의 config.yaml(+ .env 분리) 패턴을 계승한다.
