# config (코드)

최상위 config/ 디렉터리(설정 "값" 파일들: sources.yaml, dq_rules.yaml, thresholds/)를
읽어서 파이썬 객체로 만들어주는 로더 코드.

기존 ecommerce-pipeline의 dags/config/pipeline_config.py 패턴을 계승하되,
collector/streaming/batch/api 가 공통으로 import 할 수 있도록 여기로 옮긴다.

Blueprint 참고: D-1(계승할 요소 - 설정 분리)
