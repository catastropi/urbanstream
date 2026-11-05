# monitoring

**Step 8 에서는 Prometheus/Grafana 를 만들지 않았다** (ADR-0010 D8-7, DEFERRED D-129). Docker VM 메모리 여유가 작아서, 건강 상태를 Postgres 표(`ops.source_status`, `ops.layer_status`, `ops.source_health`, `ops.data_gap`, `ops.v_pipeline_health` …)에 두고 `monitor` 컨테이너(`src/urbanstream/monitor/`)가 채운다.

- Grafana 가 필요해지면(Step 10) 이 Postgres 표·뷰를 데이터 소스로 바로 읽으면 된다.
- 알림은 `src/urbanstream/notify/`, 점검 절차는 `docs/runbooks/step8-recovery.md`.
- 아래 항목은 원래 계획(Blueprint T절)으로, 아직 없다: prometheus.yml · kafka-exporter · postgres-exporter · Grafana 대시보드.
