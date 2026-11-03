# logging

구조화 로깅 공통 모듈. (주의: 표준 라이브러리 logging과 이름이 겹치므로
이 패키지 안에서 표준 logging을 쓸 때는 `import logging as stdlib_logging` 처럼 구분할 것)

- get_logger(name) — JSON 구조화 로그 포맷 (기존 ecommerce-pipeline의 logging_utils.py 계승)
- log_duration 데코레이터
- 인증키 등 민감정보를 로그에서 자동 마스킹 (scripts/smoke_test.py의 redact() 로직을 공통화)

Blueprint 참고: D-1(계승할 요소 - 로거 + log_duration)
