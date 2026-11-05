# dq (Data Quality)

팩트·마트·운영 표의 품질 검사. 규칙은 `config/dq_rules.yaml`, 결과는 `ops.dq_result`. 결정 근거 `docs/adr/0010-step8-dq-monitoring.md` (D8-9, D8-10).

- `rules.py` — 규칙 파일 읽기와 린터 (모르는 kind, 잘못된 표·열 이름, `block_mart` 남용을 막는다)
- `checks.py` — 검사 12종: null_rate · range · flag_rate · future_time · hour_coverage · volume_drift · ref_integrity · mart_reliability · load_health · call_fail_rate · incident_link_rate · table_bloat
- `engine.py` — 실행·기록·알림, 마트 차단 게이트(`blocking_failures`)

원칙: 빈 표·표본 부족은 pass 가 아니라 `unknown`. 수집 공백(`ops.data_gap`)으로 설명되는 부족은 fail 이 아니라 `warn`. 관찰(shadow) 기간(`shadow_until`)에는 기록만 한다. 마트를 막을 수 있는 규칙은 `future_time` 뿐이다.
실행: DAG `dq_report_daily`(05:00 KST) · `silver_reconcile_daily`(04:30 KST), 호출 함수는 `batch/dq_jobs.py`.
