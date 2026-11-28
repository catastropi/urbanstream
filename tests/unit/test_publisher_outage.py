"""R4 생산자 쪽 단위 시험 (STEP 3B): 브로커 없이 전달 콜백만 흉내 낸다. 실제 브로커 판은 tests/recovery/test_publisher_kafka.py.

확인: 전달 실패를 errors 로 세고 last_error 를 남긴다(조용히 버리지 않는다), 내부 큐가 가득 차면 비우고 한 번 더 넣는다,
collector/runner 가 한 주기의 실패 수를 '주기 전후 errors 차이' 로 구하므로 errors 는 줄지 않는다.
"""
from __future__ import annotations

from collector.publisher import KafkaPublisher, kafka_config


class FakeProducer:
    def __init__(self, fail: bool = False, full_once: bool = False):
        self.fail, self.full_once = fail, full_once
        self.pending: list = []
        self.sent: list = []
        self.polls = 0

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None, timestamp=None):
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        self.pending.append((topic, value, on_delivery))

    def poll(self, timeout):
        self.polls += 1
        return 0

    def flush(self, timeout):
        for topic, value, cb in self.pending:
            if self.fail:
                cb("KafkaError{code=_MSG_TIMED_OUT,val=-192,str=\"Local: Message timed out\"}", None)
            else:
                self.sent.append(value)
                cb(None, object())
        self.pending.clear()
        return 0


def test_delivery_failures_are_counted_and_last_error_kept():
    p = KafkaPublisher("unused:1", producer=FakeProducer(fail=True))
    for i in range(5):
        p.produce("t", b"k", str(i).encode(), {}, None)
    p.flush(1)
    assert (p.delivered, p.errors) == (0, 5)
    assert "timed out" in p.last_error.lower()
    before = p.errors
    p._p.fail = False
    p.produce("t", b"k", b"x", {}, None)
    p.flush(1)
    assert p.errors - before == 0 and p.delivered == 1 and p.errors == 5     # 성공해도 실패 수는 줄지 않는다


def test_full_queue_is_drained_and_retried_once():
    fp = FakeProducer(full_once=True)
    p = KafkaPublisher("unused:1", producer=fp)
    p.produce("t", b"k", b"v", {"a": "b"}, 1700000000000)
    p.flush(1)
    assert fp.sent == [b"v"] and p.delivered == 1 and fp.polls >= 2


def test_producer_settings_survive_short_outages_without_duplicates():
    c = kafka_config("kafka:19092")
    assert c["enable.idempotence"] is True and c["acks"] == "all"
    assert c["message.timeout.ms"] >= 30000                                   # 30초 안의 브로커 재시작은 쌓아 두었다 보낸다
