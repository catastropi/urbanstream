"""Kafka 발행. 실제(KafkaPublisher)와 테스트·dry-run 용(MemoryPublisher)이 같은 인터페이스를 쓴다."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol


class Publisher(Protocol):
    delivered: int
    errors: int
    last_error: Optional[str]

    def produce(self, topic: str, key: bytes, value: bytes, headers: dict[str, str],
                timestamp_ms: Optional[int]) -> None: ...

    def flush(self, timeout: float) -> int: ...


@dataclass
class Message:
    topic: str
    key: bytes
    value: bytes
    headers: dict
    timestamp_ms: Optional[int]


@dataclass
class MemoryPublisher:
    """메모리에 쌓는 발행기. --dry-run 과 테스트에서 쓴다."""
    messages: list[Message] = field(default_factory=list)
    delivered: int = 0
    errors: int = 0
    last_error: Optional[str] = None
    fail_all: bool = False  # 테스트: 전달 실패 흉내

    def produce(self, topic, key, value, headers, timestamp_ms=None) -> None:
        if self.fail_all:
            self.errors += 1
            self.last_error = "simulated delivery failure"
            return
        self.messages.append(Message(topic, key, value, dict(headers), timestamp_ms))
        self.delivered += 1

    def flush(self, timeout: float) -> int:
        return 0

    def by_topic(self, topic: str) -> list[Message]:
        return [m for m in self.messages if m.topic == topic]


def kafka_config(bootstrap: str) -> dict:
    """생산자 설정. 멱등 + acks=all: 재시도해도 중복·유실이 없게 한다 (단일 브로커여도 설정은 운영과 같게)."""
    return {
        "bootstrap.servers": bootstrap,
        "broker.address.family": "v4",  # localhost 가 IPv6(::1)로 먼저 풀려 접속 실패 로그가 쏟아지는 것을 막는다 (Docker 는 127.0.0.1 에만 연다)
        "client.id": "urbanstream-collector",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
        "message.timeout.ms": 30000,
    }


class KafkaPublisher:
    def __init__(self, bootstrap: str, producer=None):
        if producer is None:
            from confluent_kafka import Producer  # 지연 import: dry-run/테스트에는 필요 없다
            producer = Producer(kafka_config(bootstrap))
        self._p = producer
        self.delivered = 0
        self.errors = 0
        self.last_error: Optional[str] = None

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.errors += 1
            self.last_error = str(err)
        else:
            self.delivered += 1

    def produce(self, topic, key, value, headers, timestamp_ms=None) -> None:
        kwargs = dict(key=key, value=value, headers=list(headers.items()), on_delivery=self._on_delivery)
        if timestamp_ms:
            kwargs["timestamp"] = int(timestamp_ms)
        try:
            self._p.produce(topic, **kwargs)
        except BufferError:  # 내부 큐가 가득 찼다: 비우고 한 번 더
            self._p.poll(1.0)
            self._p.produce(topic, **kwargs)
        self._p.poll(0)

    def flush(self, timeout: float) -> int:
        return self._p.flush(timeout)
