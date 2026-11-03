# monitoring

Prometheus/Grafana 설정 (docker compose 의 monitoring profile 용).

- prometheus.yml — scrape 대상 (kafka-exporter, postgres-exporter, FastAPI 미들웨어)
- grafana/dashboards/ — Kafka lag, Spark 처리 지연, API 응답시간 등

Blueprint 참고: T절(Monitoring Metrics)
