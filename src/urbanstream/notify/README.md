# notify

장애/품질 이슈 알림 채널 추상화.

- Slack 웹훅 (기존 ecommerce-pipeline의 notifier.py 계승)
- 심각도(INFO/WARN/CRIT), 쿨다운/중복 억제, 해소 알림
- ops.alert_event 에 기록

Blueprint 참고: T절(Monitoring Metrics), 알림 정책
