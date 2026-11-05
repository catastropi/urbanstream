# monitor

상시 감시 프로세스 (`python -m urbanstream.monitor`, compose 서비스 `monitor`). 결정 근거 `docs/adr/0010-step8-dq-monitoring.md`.

매 60초: Kafka `platform.audit` → `ops.call_event` → 소스 상태(`ops.source_status`) → 수집 공백(`ops.data_gap`) → 레이어 정체(`ops.layer_status`) → DAG 실패 분류 → 알림 → (07:30) 아침 요약.

| 파일 | 역할 | 순수/입출력 |
|---|---|---|
| `health.py` | 소스 상태 판정 (기대 간격 ×2.5 degraded, ×5 down) | 순수 |
| `gaps.py` | host_stall(깨어남 알림) · collector_blackout(침묵 600초) | 순수 |
| `classify.py` | DAG 실패(pending)를 잠자기 증거와 대조해 격하·묶기 (증거 없으면 error 유지) | 순수 |
| `layers.py` | Bronze/Silver 최신 parquet mtime (원천 소스가 ok 일 때만 알림) | 파일 읽기 |
| `audit.py` | audit JSON → `call_event` 행 | 순수 |
| `kafkatail.py` | assign+seek 로 읽기 (offset 은 `ops.monitor_state`) | Kafka |
| `db.py` | 감시 SQL | DB |
| `loop.py` | `Monitor.tick()` · `run()` | 조립 |
