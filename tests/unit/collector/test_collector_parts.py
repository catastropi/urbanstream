"""Collector 부품 단위 테스트: 설정, 요청 명세, 재시도·회로차단·예산, 원본 저장, 마스킹, 발행기, 스케줄러, 토픽 스크립트."""
import gzip
import importlib.util
import json
import random
import sys
from datetime import datetime, timedelta

import pytest
import yaml
from collector_helpers import KEYS, ROOT, T0, Clock

from collector.config import (BreakerConfig, ConfigError, RetryConfig, expand_env, load_config, load_topics,
                              parse_hhmm)
from collector.fetch import Redactor, normalize_datago_key
from collector.publisher import KafkaPublisher, MemoryPublisher, kafka_config
from collector.rawstore import RawStore
from collector.requests_spec import build_request, kma_base
from collector.resilience import CircuitBreaker, DailyBudget, RetryPolicy, classify
from collector.runner import Scheduler, in_window, next_window_start
from urbanstream.contracts.common import KST


# ------------------------------------------------------------------ 설정
def test_real_config_loads_and_plans_fit_budgets():
    cfg = load_config(env={})
    assert set(cfg.sources) == {"seoul_traffic", "seoul_incident", "bus_position", "bus_arrival", "kma_ncst"}
    for s in cfg.sources.values():
        assert s.planned_calls_per_day <= s.daily_budget * 0.8, s.name
        assert s.topic in cfg.topics.topics
    assert cfg.sources["bus_position"].targets[0].params == {"route_id": "100100118"}  # 환경변수 없을 때의 기본값
    assert cfg.sources["kma_ncst"].targets[0].params == {"nx": 60, "ny": 127}
    assert cfg.topics.dlq_topic in cfg.topics.topics and cfg.topics.audit_topic in cfg.topics.topics


def test_env_overrides_and_expansion():
    assert expand_env("${A:-x}", {}) == "x" and expand_env("${A:-x}", {"A": "y"}) == "y"
    assert expand_env({"k": ["${A:-1}", 2]}, {}) == {"k": ["1", 2]}
    cfg = load_config(env={"SEOUL_BUS_ROUTE_ID": "999", "KMA_NX": "55", "KMA_NY": "124"})
    assert cfg.sources["bus_arrival"].targets[0].params["route_id"] == "999"
    assert cfg.sources["kma_ncst"].targets[0].params == {"nx": 55, "ny": 124}


def _write(tmp_path, mutate):
    raw = yaml.safe_load((ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"))
    mutate(raw)
    p = tmp_path / "sources.yaml"
    p.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
    return p


def test_plan_over_budget_refuses_to_start(tmp_path):
    p = _write(tmp_path, lambda r: r["sources"]["bus_position"].update(interval_sec=60))  # 1200회/일 > 800
    with pytest.raises(ConfigError, match="daily_budget"):
        load_config(p, env={})


@pytest.mark.parametrize("mutate,match", [
    (lambda r: r["sources"]["bus_position"].update(interval_sec=5), "10 이상"),
    (lambda r: r["sources"]["bus_position"].update(topic="nope.topic"), "topics.yaml"),
    (lambda r: r["sources"]["bus_position"].update(active_window=["10:00", "09:00"]), "active_window"),
    (lambda r: r["sources"]["bus_position"].update(route_ids=[]), "비어 있습니다"),
    (lambda r: r["sources"]["bus_position"].update(budget_basis="guess"), "budget_basis"),
    (lambda r: r["sources"].update(mystery={"interval_sec": 60}), "알 수 없는 소스"),
])
def test_invalid_configs_are_rejected(tmp_path, mutate, match):
    with pytest.raises(ConfigError, match=match):
        load_config(_write(tmp_path, mutate), env={})


def test_topics_config_is_consistent():
    topics = load_topics()
    assert topics.topics["urban.bus.position.v1"].partitions == 3
    assert topics.topics["platform.audit"].retention_ms == 604800000
    assert parse_hhmm("24:00") == 1440 and parse_hhmm("04:30") == 270
    with pytest.raises(ConfigError):
        parse_hhmm("25:00")


# ------------------------------------------------------------------ 요청 명세
def test_request_specs_match_verified_endpoints():
    now = datetime(2026, 11, 2, 10, 50, tzinfo=KST)
    r = build_request("seoul_traffic", {"link_id": "1220003800"}, "K/e y", now)
    assert r.url == "http://openapi.seoul.go.kr:8088/K%2Fe%20y/xml/TrafficInfo/1/5/1220003800" and r.params is None
    r = build_request("seoul_incident", {"page_size": 200}, "KEY", now)
    assert r.url == "http://openapi.seoul.go.kr:8088/KEY/xml/AccInfo/1/200/"
    r = build_request("bus_position", {"route_id": "100100118"}, "KEY", now)
    assert r.url == "http://ws.bus.go.kr/api/rest/buspos/getBusPosByRtid"
    assert r.params == {"ServiceKey": "KEY", "busRouteId": "100100118"}
    r = build_request("bus_arrival", {"route_id": "100100118"}, "KEY", now)
    assert r.url.endswith("/arrive/getArrInfoByRouteAll") and r.params["busRouteId"] == "100100118"
    r = build_request("kma_ncst", {"nx": 60, "ny": 127}, "KEY", now)
    assert r.params["base_date"] == "20261102" and r.params["base_time"] == "1000" and r.base == ("20261102", "1000")
    assert r.params["dataType"] == "JSON" and r.params["nx"] == 60


def test_kma_base_uses_previous_hour_until_available():
    at = lambda h, m: datetime(2026, 11, 2, h, m, tzinfo=KST)
    assert kma_base(at(10, 44)) == ("20261102", "0900") and kma_base(at(10, 45)) == ("20261102", "1000")
    assert kma_base(at(0, 10)) == ("20261101", "2300")                        # 자정을 넘는 경우 전날
    assert kma_base(at(10, 20), available_after_min=15) == ("20261102", "1000")  # 측정 후 값 조정 가능


def test_datago_key_is_normalised_to_decoded_form():
    assert normalize_datago_key("abc%2Bdef%3D") == "abc+def=" and normalize_datago_key(" plain ") == "plain"


# ------------------------------------------------------------------ 재시도 / 회로차단 / 예산
def test_classification():
    assert classify("ok") == classify("empty") == "ok"
    assert {classify(k) for k in ("auth", "param", "no_service")} == {"permanent"}
    assert {classify(k) for k in ("server_error", "rate_limited", "malformed", "unknown", "network_error",
                                  "timeout", "http_error")} == {"retryable"}


def test_backoff_grows_and_is_capped():
    p = RetryPolicy(RetryConfig(3, 1.0, 10.0), random.Random(0))
    d = [p.delay(n) for n in (1, 2, 3, 4, 5, 6)]
    assert 1.0 <= d[0] <= 1.25 and 2.0 <= d[1] <= 2.5 and 4.0 <= d[2] <= 5.0
    assert all(x <= 12.5 for x in d) and 10.0 <= d[5] <= 12.5


def test_circuit_breaker_state_machine():
    clock = Clock()
    b = CircuitBreaker(BreakerConfig(failure_threshold=3, cooldown_sec=100, permanent_cooldown_sec=500), clock)
    assert b.allow()
    b.record_failure(); b.record_failure()
    assert b.state == "closed" and b.allow()
    b.record_failure()
    assert b.state == "open" and not b.allow()
    clock.advance(99)
    assert not b.allow()
    clock.advance(2)
    assert b.allow() and b.state == "half_open"          # cooldown 뒤 시험 호출 1회
    b.record_failure()                                     # 시험 실패 -> 곧바로 다시 open
    assert b.state == "open" and not b.allow()
    clock.advance(101); assert b.allow()
    b.record_success()
    assert b.state == "closed" and b.failures == 0
    b.record_failure(permanent=True)                       # 영구 실패는 한 번에 open, 더 길게 쉰다
    assert b.state == "open"; clock.advance(400); assert not b.allow(); clock.advance(101); assert b.allow()


def test_budget_persists_across_restarts_and_rolls_over(tmp_path):
    clock, path = Clock(), tmp_path / "b.json"
    b = DailyBudget(path, {"x": 3}, clock)
    assert b.try_consume("x") and b.try_consume("x") and b.used("x") == 2
    b2 = DailyBudget(path, {"x": 3}, clock)                # 프로세스 재시작을 흉내
    assert b2.used("x") == 2 and b2.try_consume("x") and not b2.try_consume("x")
    clock.now = datetime(2026, 11, 3, 0, 0, 1, tzinfo=KST)  # KST 날짜가 바뀜
    assert b2.used("x") == 0 and b2.try_consume("x")
    assert b2.report()["x"] == {"used": 1, "limit": 3, "pct": 33.3}


def test_budget_survives_corrupted_state_file(tmp_path):
    path = tmp_path / "b.json"
    path.write_text("{not json", encoding="utf-8")
    b = DailyBudget(path, {"x": 5}, Clock())
    assert b.recovered_from_corruption and b.try_consume("x")


# ------------------------------------------------------------------ 원본 저장 / 마스킹
def test_rawstore_layout_roundtrip_and_determinism(tmp_path):
    store = RawStore(tmp_path / "data" / "raw", tmp_path)
    t = datetime(2026, 11, 2, 9, 5, 7, tzinfo=KST)
    uri = store.write("bus_position", t, "abcdef12-0000", b"<x>\xed\x95\x9c</x>")
    assert uri == "data/raw/bus_position/2026/11/02/090507_abcdef12.xml.gz"
    assert gzip.decompress((tmp_path / uri).read_bytes()) == b"<x>\xed\x95\x9c</x>"
    assert store.write("kma_ncst", t, "aaaaaaaa", b'{"a":1}').endswith(".json.gz")
    assert store.write("x", t, "bbbbbbbb", b"plain").endswith(".txt.gz")
    store.write("bus_position", t, "abcdef12-0000", b"<x>\xed\x95\x9c</x>")
    assert (tmp_path / uri).read_bytes() == gzip.compress(b"<x>\xed\x95\x9c</x>", mtime=0)


def test_redactor_masks_all_encodings():
    r = Redactor(["ab/c+d=e%f", "plainkey123"])
    for s in ("ab/c+d=e%f", "ab%2Fc%2Bd%3De%25f", "plainkey123"):
        assert "***KEY***" in r(f"https://x/{s}?q=1") and s not in r(f"https://x/{s}?q=1")
    assert r(None) is None and r("no secret here") == "no secret here"


# ------------------------------------------------------------------ 발행기
def test_kafka_config_is_idempotent_and_acks_all():
    cfg = kafka_config("localhost:9092")
    assert cfg["enable.idempotence"] is True and cfg["acks"] == "all" and cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["broker.address.family"] == "v4"       # localhost 가 IPv6 로 먼저 풀리는 문제 방지


class FakeProducer:
    def __init__(self, fail_first=False):
        self.calls, self.fail_first, self.polled = [], fail_first, 0

    def produce(self, topic, **kw):
        if self.fail_first:
            self.fail_first = False
            raise BufferError("queue full")
        self.calls.append((topic, kw))

    def poll(self, t):
        self.polled += 1

    def flush(self, timeout):
        return 0


def test_kafka_publisher_retries_on_full_queue_and_counts_deliveries():
    fp = FakeProducer(fail_first=True)
    pub = KafkaPublisher("x", producer=fp)
    pub.produce("t", b"k", b"v", {"h": "1"}, 1700000000000)
    topic, kw = fp.calls[0]
    assert topic == "t" and kw["key"] == b"k" and kw["headers"] == [("h", "1")] and kw["timestamp"] == 1700000000000
    kw["on_delivery"](None, object()); kw["on_delivery"]("boom", object())
    assert pub.delivered == 1 and pub.errors == 1 and pub.last_error == "boom"


def test_memory_publisher_basics():
    m = MemoryPublisher()
    m.produce("a", b"k", b"v", {}, None); m.produce("b", b"k", b"v", {}, None)
    assert len(m.by_topic("a")) == 1 and m.delivered == 2 and m.flush(1) == 0


# ------------------------------------------------------------------ 스케줄러
def test_scheduler_staggers_start_and_respects_windows():
    cfg = load_config(env={})
    srcs = [cfg.sources[n] for n in ("seoul_traffic", "bus_position", "kma_ncst")]
    s = Scheduler(srcs, T0, stagger_sec=3.0, rng=random.Random(0))
    assert [t.source for t in s.pop_ready(T0)] == ["seoul_traffic"]            # 처음에는 하나만
    assert {t.source for t in s.pop_ready(T0 + timedelta(seconds=6))} == {"seoul_traffic", "bus_position", "kma_ncst"}
    night = datetime(2026, 11, 2, 2, 0, tzinfo=KST)                              # 버스 활성 시간(04:00~) 밖
    s2 = Scheduler([cfg.sources["bus_position"]], night, rng=random.Random(0))
    assert s2.pop_ready(night) == []
    assert s2.due["bus_position:100100118"] == datetime(2026, 11, 2, 4, 0, tzinfo=KST)


def test_window_helpers_and_reschedule_jitter():
    cfg = load_config(env={})
    bus = cfg.sources["bus_position"]
    assert in_window(bus, datetime(2026, 11, 2, 4, 0, tzinfo=KST)) and not in_window(bus, datetime(2026, 11, 2, 3, 59, tzinfo=KST))
    late = datetime(2026, 11, 2, 23, 59, tzinfo=KST)
    assert in_window(bus, late) and next_window_start(bus, late) == datetime(2026, 11, 3, 4, 0, tzinfo=KST)
    s = Scheduler([bus], T0, jitter=0.05, rng=random.Random(3))
    target = bus.targets[0]
    for _ in range(50):
        s.reschedule(target, T0)
        assert 90 * 0.95 <= (s.due[target.key] - T0).total_seconds() <= 90 * 1.05
    s.reschedule(target, datetime(2026, 11, 2, 23, 59, 30, tzinfo=KST))        # 다음 호출이 자정을 넘으면 다음 날 04:00
    assert s.due[target.key] == datetime(2026, 11, 3, 4, 0, tzinfo=KST)


# ------------------------------------------------------------------ 토픽 스크립트
def test_create_topics_plan_lists_every_topic(capsys):
    spec = importlib.util.spec_from_file_location("create_topics", ROOT / "scripts" / "create_topics.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["create_topics"] = mod
    spec.loader.exec_module(mod)
    assert mod.main(["--plan"]) == 0
    out = capsys.readouterr().out
    for name in load_topics().topics:
        assert name in out
    assert mod.topic_config(load_topics().topics["urban.dlq.v1"]) == {"retention.ms": "604800000"}
