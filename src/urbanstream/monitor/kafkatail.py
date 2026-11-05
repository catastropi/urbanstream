"""platform.audit 를 읽는다. 컨슈머 그룹 커밋을 쓰지 않고 offset 을 ops.monitor_state 에 직접 저장한다 (assign + seek).
그래서 컨테이너를 다시 만들어도 이어 읽고, 같은 메시지를 다시 읽어도 call_event 의 PK 가 중복을 막는다.
confluent_kafka 는 쓸 때만 import 한다."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


class KafkaUnavailable(RuntimeError):
    pass


class KafkaTail:
    def __init__(self, bootstrap: str, topic: str, *, poll_timeout_sec: float = 2.0, consumer_factory=None) -> None:
        self.bootstrap, self.topic, self.poll_timeout = bootstrap, topic, poll_timeout_sec
        self._factory, self._c = consumer_factory, None

    def _consumer(self):
        if self._c is None:
            if self._factory:
                self._c = self._factory()
            else:
                from confluent_kafka import Consumer
                self._c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "urbanstream-monitor", "enable.auto.commit": False,
                                    "enable.partition.eof": True, "session.timeout.ms": 10000, "socket.timeout.ms": 10000,
                                    "allow.auto.create.topics": False})
        return self._c

    def read(self, offsets: Mapping[str, int], *, max_messages: int, start_hours_back: float, now: datetime) -> list[tuple[int, int, bytes]]:
        """offsets: {"0": 다음에 읽을 offset}. 없으면 start_hours_back 시간 전부터. (partition, offset, value) 목록."""
        from confluent_kafka import TopicPartition
        c = self._consumer()
        try:
            md = c.list_topics(self.topic, timeout=10)
        except Exception as e:                                               # noqa: BLE001
            self._c = None
            raise KafkaUnavailable(f"{type(e).__name__}: {e}") from e
        t = md.topics.get(self.topic)
        if t is None or t.error is not None or not t.partitions:
            raise KafkaUnavailable(f"토픽 {self.topic} 메타데이터 없음")
        tps = []
        for p in sorted(t.partitions):
            if str(p) in offsets:
                tps.append(TopicPartition(self.topic, p, int(offsets[str(p)])))
            else:
                since = int((now - timedelta(hours=start_hours_back)).timestamp() * 1000)
                got = c.offsets_for_times([TopicPartition(self.topic, p, since)], timeout=10)[0]
                tps.append(TopicPartition(self.topic, p, got.offset if got.offset >= 0 else 0))
        c.assign(tps)
        out: list[tuple[int, int, bytes]] = []
        eof: set[int] = set()
        deadline = time.monotonic() + 30
        while len(out) < max_messages and len(eof) < len(tps) and time.monotonic() < deadline:
            m = c.poll(self.poll_timeout)
            if m is None:
                break
            if m.error():
                if m.error().code() == -191:                                 # _PARTITION_EOF
                    eof.add(m.partition())
                    continue
                self._c = None
                raise KafkaUnavailable(str(m.error()))
            out.append((m.partition(), m.offset(), m.value()))
        return out

    def close(self) -> None:
        if self._c is not None:
            try:
                self._c.close()
            except Exception:                                                # noqa: BLE001
                pass
            self._c = None
