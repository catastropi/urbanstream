"""파이프라인 실시간 감시 (Step 8a, ADR-0010). `python -m urbanstream.monitor` 로 상시 실행한다 (docker compose profile `monitor`).

순수 계산(health·gaps·classify·layers)과 입출력(audit·kafkatail·loop)을 나눠 놓았다 — 앞쪽은 DB·Kafka 없이 시험한다.
이 패키지는 import 시점에 psycopg·confluent_kafka 를 부르지 않는다.
"""
