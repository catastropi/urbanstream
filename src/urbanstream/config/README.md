# config (코드, 예약된 자리 — 코드 없음)

설정 "값" 파일은 최상위 `config/` 에 있다(`sources.yaml`, `topics.yaml`, `silver.yaml`, `dq_rules.yaml`, `monitor.yaml`, `alerts.yaml`).
이 값을 읽는 로더는 공통 모듈이 아니라 사용하는 쪽에 있다.

- 수집기: `collector/config.py`
- Bronze/Silver: `streaming/bronze_config.py`, `streaming/silver_config.py`
- Gold/배치: `batch/gold_config.py`
- API: `api/config.py`, 대시보드: `dashboard/config.py`

공통 로더로 합치는 일은 하지 않았다(소스마다 환경 변수와 검증 규칙이 달라서).
