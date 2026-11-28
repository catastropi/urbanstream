"""R4 생산자 쪽, 실제 브로커 (STEP 3B, 설계안 7-3. 단위판은 tests/unit/test_publisher_outage.py): Kafka 브로커가 잠깐 죽었을 때 수집기 발행기(collector/publisher.KafkaPublisher)가
(1) 짧은 정전(message.timeout.ms 안)이면 쌓아 두었다가 모두 한 번씩 전달하고, (2) 긴 정전이면 조용히 버리지 않고 실패를 센다
(collector/runner.py 가 그 수를 감사 기록의 delivery_errors 와 오류 로그로 남기고, 원본은 raw 에 있어 재처리할 수 있다).

시험 전용 브로커가 필요하다: URBANSTREAM_TEST_KAFKA_HOME (tests/spark/kafka_broker.py). 없으면 건너뛴다.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if not os.environ.get("URBANSTREAM_TEST_KAFKA_HOME"):
    pytest.skip("URBANSTREAM_TEST_KAFKA_HOME(시험 전용 Kafka 배포판)이 없다", allow_module_level=True)
ck = pytest.importorskip("confluent_kafka")
sys.path.insert(0, str(ROOT / "tests" / "spark"))
from kafka_broker import Broker  # noqa: E402

from collector.publisher import KafkaPublisher, kafka_config  # noqa: E402


@pytest.fixture
def broker(tmp_path):
    b = Broker(tmp_path / "kafka").start()
    yield b
    b.stop()


def make_topic(bs: str) -> str:
    from confluent_kafka.admin import AdminClient, NewTopic
    name = f"test.publisher.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bs})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(30)
    return name


def read_all(bs: str, topic: str, expect: int, wait: float = 60) -> list[bytes]:
    c = ck.Consumer({"bootstrap.servers": bs, "group.id": uuid.uuid4().hex, "auto.offset.reset": "earliest",
                     "broker.address.family": "v4"})
    c.subscribe([topic])
    got, t0 = [], time.time()
    while len(got) < expect and time.time() - t0 < wait:
        m = c.poll(1.0)
        if m is not None and m.error() is None:
            got.append(m.value())
    t1 = time.time()
    while time.time() - t1 < 3:                             # 더 오는 것(중복)이 없는지 잠깐 더 본다
        m = c.poll(0.5)
        if m is not None and m.error() is None:
            got.append(m.value())
    c.close()
    return got


def send(pub: KafkaPublisher, topic: str, start: int, n: int) -> None:
    for i in range(start, start + n):
        pub.produce(topic, b"k", str(i).encode(), {"ingest_id": str(i)}, None)


def test_short_broker_outage_delivers_everything_once(broker):
    bs = broker.bootstrap
    topic = make_topic(bs)
    pub = KafkaPublisher(bs)
    send(pub, topic, 0, 20)
    assert pub.flush(30) == 0
    broker.kill9()
    send(pub, topic, 20, 20)                                 # 브로커가 없는 동안 낸 메시지는 생산자 안에 쌓인다
    time.sleep(3)
    broker.start()
    assert pub.flush(60) == 0
    assert pub.errors == 0 and pub.delivered == 40
    got = read_all(bs, topic, 40)
    assert sorted(int(v) for v in got) == list(range(40)), "멱등 생산자라 다시 보내도 한 번씩만 있어야 한다"


def test_long_broker_outage_is_counted_not_hidden(broker):
    bs = broker.bootstrap
    topic = make_topic(bs)
    from confluent_kafka import Producer
    pub = KafkaPublisher(bs, producer=Producer({**kafka_config(bs), "message.timeout.ms": 4000}))   # 운영 30초를 4초로 줄여 재현
    send(pub, topic, 0, 10)
    assert pub.flush(30) == 0
    broker.kill9()
    send(pub, topic, 10, 15)
    pub.flush(15)
    assert pub.errors == 15 and pub.delivered == 10
    assert pub.last_error and "timed out" in pub.last_error.lower()
