# notify

알림. 결정 근거 `docs/adr/0010-step8-dq-monitoring.md` (D8-3, D8-4, D8-5).

| 파일 | 역할 |
|---|---|
| `__init__.py` | 공개 함수: `send`(고정 시그니처) · `enqueue`(DAG 실패 → pending) · `sync`/`resolve`(상태형) · `get_notifier`/`set_notifier` |
| `core.py` | `Notifier`: 중복 억제(쿨다운)·시간당 상한·warning 승격·pending 분류·아침 요약·해소 알림 |
| `policy.py` / `config/alerts.yaml` | 수준별 푸시/요약, 쿨다운, 상한, 잠자기 판정 숫자 |
| `transport.py` | 웹훅 전송(Discord `content` / Slack `text`), 1회 재시도, 오류문 마스킹 |
| `store.py` | `PgStore`(ops.alert_event · ops.alert_state) / `MemoryStore`(DB 없을 때·시험) |
| `_mask.py` | 비밀 가리기: `password=…` 류, Discord·Slack 웹훅 URL, `.env` 의 URL 그대로 |

규칙: 어떤 경우에도 호출자에게 예외를 던지지 않는다(잘못된 level 만 ValueError). error·critical 만 푸시, warning·info 는 07:30 아침 요약. 웹훅 URL 은 `.env` 의 `ALERT_WEBHOOK_URL` 에만 둔다.
