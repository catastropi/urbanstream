"""Collector 한 사이클의 전체 흐름 (가짜 호출기·가짜 발행기). 성공, 재시도, 영구 실패, 예산, 키 유출까지."""
import gzip
import json
import random
from datetime import timedelta

from collector_helpers import (KEYS, T0, Clock, FakeFetcher, body, fixture_bytes, make_collector, net_error, ok,
                               router_for_fixtures, target)

from collector.config import load_config
from collector.publisher import MemoryPublisher
from collector.runner import KEY_FUNCS, Scheduler, run_forever

CFG = load_config(env={})
POS, ARR, TRAF, INC, KMA = (target(CFG, n) for n in
                            ("bus_position", "bus_arrival", "seoul_traffic", "seoul_incident", "kma_ncst"))


def audits(pub):
    return [json.loads(m.value) for m in pub.by_topic("platform.audit")]


def test_success_publishes_keyed_timestamped_messages_and_saves_raw(tmp_path):
    c, pub, fetcher, clock, _ = make_collector(tmp_path, [ok("bus_position/ok_20261101T215320.xml")])
    rep = c.run_target(POS)
    assert (rep.outcome, rep.attempts, rep.n_records, rep.n_rejects, rep.published) == ("ok", 1, 13, 0, 13)
    msgs = pub.by_topic("urban.bus.position.v1")
    assert len(msgs) == 13
    first = json.loads(msgs[0].value)
    assert msgs[0].key == first["payload"]["veh_id"].encode()
    assert msgs[0].timestamp_ms == int(__import__("datetime").datetime.fromisoformat(first["event_time"]).timestamp() * 1000)
    assert msgs[0].headers["schema_version"] == "bus_position_v1" and msgs[0].headers["event_id"] == first["event_id"]
    assert first["payload"]["route_id"] == "100100118"
    a = audits(pub)[-1]
    assert (a["final"], a["outcome"], a["n_records"], a["published"], a["budget_used"], a["budget_limit"]) == \
           (True, "ok", 13, 13, 1, 1000)
    assert first["raw_uri"] == a["raw_uri"] and a["raw_uri"].startswith("raw/bus_position/2026/11/02/")
    assert gzip.decompress((tmp_path / a["raw_uri"]).read_bytes()) == ok("bus_position/ok_20261101T215320.xml").body


def test_arrival_and_other_sources_use_their_keys(tmp_path):
    for source, rel, topic, n in [("bus_arrival", "bus_arrival/ok_20261101T215321.xml", "urban.bus.arrival.v1", 193),
                                  ("seoul_traffic", "seoul_traffic/ok_20261101T180753.xml", "urban.traffic.speed.v1", 1),
                                  ("seoul_incident", "seoul_incident/ok_20261101T180755.xml", "urban.traffic.incident.v1", 10),
                                  ("kma_ncst", "kma_ncst/ok_20261028T150951.json", "urban.weather.observation.v1", 8)]:
        c, pub, *_ = make_collector(tmp_path / source, [ok(rel)])
        rep = c.run_target(target(CFG, source))
        assert rep.outcome == "ok" and len(pub.by_topic(topic)) == n, source
    c, pub, *_ = make_collector(tmp_path / "k", [ok("bus_arrival/ok_20261101T215321.xml")])
    c.run_target(ARR)
    arr = pub.by_topic("urban.bus.arrival.v1")
    assert all(m.key == f"100100118|{json.loads(m.value)['payload']['station_id']}".encode() for m in arr)
    stations = {json.loads(m.value)['payload']['station_id'] for m in arr}
    assert len({m.key for m in arr}) == len(stations) and len(arr) == 193 > len(stations)   # 슬롯 1·2는 같은 키
    assert set(KEY_FUNCS) == set(CFG.sources)


def test_bad_rows_go_to_dlq_without_losing_good_rows(tmp_path):
    raw = ok("bus_position/ok_20261101T215320.xml").body.decode("utf-8").replace("<vehId>111049702</vehId>", "", 1)
    c, pub, *_ = make_collector(tmp_path, [body(raw)])
    rep = c.run_target(POS)
    assert rep.outcome == "ok" and rep.n_records == 12 and rep.n_rejects == 1
    dlq = [json.loads(m.value) for m in pub.by_topic("urban.dlq.v1")]
    assert len(dlq) == 1 and "vehId" in dlq[0]["reason"] and dlq[0]["raw_row"]["plainNo"] and dlq[0]["raw_uri"]


def test_empty_result_is_ok_with_zero_records(tmp_path):
    xml = "<AccInfo><list_total_count>0</list_total_count><RESULT><CODE>INFO-200</CODE><MESSAGE>없음</MESSAGE></RESULT></AccInfo>"
    c, pub, *_ = make_collector(tmp_path, [body(xml)])
    rep = c.run_target(INC)
    assert rep.outcome == "ok" and rep.n_records == 0 and not pub.by_topic("urban.traffic.incident.v1")
    assert audits(pub)[-1]["status_kind"] == "empty"


def test_truncated_incident_list_is_flagged(tmp_path):
    xml = ("<AccInfo><list_total_count>250</list_total_count><RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>"
           "<row><acc_id>1</acc_id><occr_date>20261101</occr_date><occr_time>1000</occr_time><acc_type>A01</acc_type></row></AccInfo>")
    c, pub, *_ = make_collector(tmp_path, [body(xml)])
    c.run_target(INC)
    assert audits(pub)[-1]["truncated"] is True


# ------------------------------------------------------------------ 재시도
def test_transient_failures_are_retried_with_backoff_then_succeed(tmp_path):
    c, pub, fetcher, clock, sleeps = make_collector(
        tmp_path, [body("Service Unavailable", 503), body("Bad Gateway", 502), ok("bus_position/ok_20261101T215320.xml")])
    rep = c.run_target(POS)
    assert (rep.outcome, rep.attempts) == ("ok", 3) and len(fetcher.calls) == 3
    assert len(sleeps) == 2 and sleeps[0] < sleeps[1]                         # 지수 백오프
    a = audits(pub)
    assert [(x["attempt"], x["final"], x["status_kind"]) for x in a] == [(1, False, "server_error"), (2, False, "server_error"), (3, True, "ok")]
    assert c.budget.used("bus_position") == 3 and c._breaker(POS.key).state == "closed"


def test_network_error_and_timeout_are_retried(tmp_path):
    c, pub, fetcher, *_ = make_collector(tmp_path, [net_error(), net_error("ReadTimeout", "timeout"),
                                                    ok("bus_position/ok_20261101T215320.xml")])
    assert c.run_target(POS).outcome == "ok" and len(fetcher.calls) == 3
    assert [x["status_kind"] for x in audits(pub)][:2] == ["network_error", "timeout"]


def test_all_attempts_fail_marks_failed_and_publishes_dlq_for_unparseable_body(tmp_path):
    c, pub, fetcher, *_ = make_collector(tmp_path, [body("<html>oops</html>")])
    rep = c.run_target(POS)
    assert rep.outcome == "failed" and rep.attempts == 3 and rep.reason == "malformed"
    dlq = [json.loads(m.value) for m in pub.by_topic("urban.dlq.v1")]
    assert len(dlq) == 1 and dlq[0]["reason"] == "status:malformed" and dlq[0]["raw_uri"]
    assert not pub.by_topic("urban.bus.position.v1")


# ------------------------------------------------------------------ 영구 실패 / 회로차단
def test_auth_failure_is_not_retried_and_opens_breaker_then_recovers(tmp_path):
    auth = body('{"error":"Unauthorized","message":"유효하지 않은 서비스키입니다: 등록되지 않은 서비스키","status":401}', 401)
    c, pub, fetcher, clock, sleeps = make_collector(tmp_path, [auth])
    rep = c.run_target(POS)
    assert (rep.outcome, rep.attempts, rep.reason) == ("permanent_failure", 1, "auth") and sleeps == []
    n_calls = len(fetcher.calls)
    assert c.run_target(POS).reason == "circuit_open" and len(fetcher.calls) == n_calls      # 호출하지 않는다
    assert audits(pub)[-1]["reason"] == "circuit_open"
    clock.advance(CFG.breaker.permanent_cooldown_sec + 1)
    fetcher.responses = [ok("bus_position/ok_20261101T215320.xml")]
    assert c.run_target(POS).outcome == "ok" and c._breaker(POS.key).state == "closed"


def test_repeated_transient_failures_open_the_breaker(tmp_path):
    c, pub, fetcher, clock, _ = make_collector(tmp_path, [body("down", 503)])
    for _ in range(CFG.breaker.failure_threshold):
        assert c.run_target(POS).outcome == "failed"
    calls = len(fetcher.calls)
    assert c.run_target(POS).reason == "circuit_open" and len(fetcher.calls) == calls


# ------------------------------------------------------------------ 예산
def test_budget_exhaustion_stops_retries_and_then_skips(tmp_path):
    c, pub, fetcher, *_ = make_collector(tmp_path, [body("down", 503)], limits={**{s.name: s.daily_budget for s in CFG.sources.values()}, "bus_position": 2})
    rep = c.run_target(POS)
    assert rep.outcome == "failed" and len(fetcher.calls) == 2 and audits(pub)[-1]["budget_stop"] is True
    c._breaker(POS.key).record_success()  # 회로차단과 무관하게 예산만 시험
    rep = c.run_target(POS)
    assert (rep.outcome, rep.reason) == ("skipped", "budget_exhausted") and len(fetcher.calls) == 2
    assert audits(pub)[-1]["reason"] == "budget_exhausted"


# ------------------------------------------------------------------ 기상청 중복 호출 방지
def test_weather_is_not_refetched_for_the_same_base_hour(tmp_path):
    c, pub, fetcher, clock, _ = make_collector(tmp_path, [ok("kma_ncst/ok_20261028T150951.json")])
    assert c.should_skip(KMA, clock()) is None
    assert c.run_target(KMA).outcome == "ok"
    clock.advance(600)
    assert c.should_skip(KMA, clock()) == "base_already_fetched"       # 10:10 -> 여전히 09시 발표분
    clock.advance(40 * 60)                                              # 10:50 -> 10시 발표분이 생김
    assert c.should_skip(KMA, clock()) is None


# ------------------------------------------------------------------ 키 유출 방지
def test_keys_are_used_in_requests_but_never_appear_in_any_output(tmp_path):
    leak = f"HTTPConnectionPool: Max retries exceeded url: /x?ServiceKey={KEYS['datago']}&k={KEYS['seoul']}"
    c, pub, fetcher, *_ = make_collector(tmp_path, [net_error(leak), net_error(leak), net_error(leak)])
    c.run_target(POS)
    assert fetcher.calls[0][1]["ServiceKey"] == KEYS["datago"]                 # 요청에는 실제 키가 쓰인다
    blob = b"".join(m.key + m.value + json.dumps(m.headers).encode() for m in pub.messages)
    assert KEYS["datago"].encode() not in blob and KEYS["seoul"].encode() not in blob
    assert b"***KEY***" in blob
    c2, pub2, *_ = make_collector(tmp_path / "ok", [ok("bus_position/ok_20261101T215320.xml")])
    c2.run_target(POS)
    assert KEYS["datago"].encode() not in b"".join(m.value for m in pub2.messages)
    assert not any(KEYS["datago"].encode() in p.read_bytes() for p in (tmp_path / "ok").rglob("*") if p.is_file())


def test_seoul_sources_use_the_seoul_key(tmp_path):
    c, pub, fetcher, *_ = make_collector(tmp_path, [ok("seoul_traffic/ok_20261101T180753.xml")])
    c.run_target(TRAF)
    assert KEYS["seoul"] in fetcher.calls[0][0] and KEYS["datago"] not in fetcher.calls[0][0]


# ------------------------------------------------------------------ Kafka 전달 실패
def test_kafka_delivery_failure_is_reported_but_does_not_trip_source_breaker(tmp_path):
    pub = MemoryPublisher(fail_all=True)
    c, pub, *_ = make_collector(tmp_path, [ok("bus_position/ok_20261101T215320.xml")], publisher=pub)
    rep = c.run_target(POS)
    assert rep.outcome == "ok" and rep.published == 0
    assert c._breaker(POS.key).state == "closed"      # 소스는 멀쩡하다. 문제는 Kafka 쪽이다
    assert pub.errors >= 13


# ------------------------------------------------------------------ 스케줄 통합
class FakeStop:
    def __init__(self, clock, loops):
        self.clock, self.loops, self.n = clock, loops, 0

    def is_set(self):
        return self.n >= self.loops

    def wait(self, seconds):
        self.n += 1
        self.clock.advance(seconds)


def test_run_forever_respects_intervals_and_weather_dedup(tmp_path):
    clock = Clock()
    c, pub, fetcher, _, _ = make_collector(tmp_path, router=router_for_fixtures, clock=clock)
    sched = Scheduler(list(CFG.sources.values()), clock(), rng=random.Random(0))
    stop = FakeStop(clock, loops=240)     # 대기 최대 5초 × 240 = 최대 20분의 가상 시간
    run_forever(c, sched, stop, clock)
    by = lambda kw: sum(1 for url, _ in fetcher.calls if kw in url)
    elapsed = (clock.now - T0).total_seconds()
    assert 600 <= elapsed <= 1250
    assert by("getUltraSrtNcst") == 1                                          # 같은 발표 시각은 한 번만
    iv = lambda name: CFG.sources[name].interval_sec        # 간격은 sources.yaml 값을 따른다 (측정으로 바뀔 수 있음)
    n = lambda name: len(CFG.sources[name].targets)          # 대상(링크·노선) 수도 설정을 따른다 — 호출 수는 대상마다 따로 센다 (2026-11-04: 링크 4개)
    # 대상마다 시작할 때 1회 + 간격마다 1회. 시작 시각 어긋남(스태거·지터)만큼 오차를 허용한다.
    assert abs(by("TrafficInfo") - n("seoul_traffic") * (elapsed / iv("seoul_traffic") + 1)) <= n("seoul_traffic")
    assert abs(by("getBusPosByRtid") - n("bus_position") * (elapsed / iv("bus_position") + 1)) <= n("bus_position")
    assert len(audits(pub)) == len(fetcher.calls)                              # 호출마다 audit 1건


def test_audit_carries_parser_stats_and_odd_flag_values_do_not_cause_dlq(tmp_path):
    raw = ok("bus_arrival/ok_20261101T215321.xml").body.decode("utf-8").replace("<isArrive1>0</isArrive1>", "<isArrive1>2</isArrive1>")
    c, pub, *_ = make_collector(tmp_path, [body(raw)])
    rep = c.run_target(ARR)
    assert rep.outcome == "ok" and rep.n_rejects == 0 and not pub.by_topic("urban.dlq.v1")
    a = audits(pub)[-1]
    assert a["parse_stats"]["empty_slots"] == 15 and a["parse_stats"]["unexpected_flag_values"] == 0


# ------------------------------------------------------------------ 빈 응답은 실패가 아니다 (2026-11-04)
def test_bus_position_no_result_is_ok_without_retry_and_never_trips_the_breaker(tmp_path):
    empty = body(fixture_bytes("bus_position/empty_code4_20261104T024101.xml"))
    c, pub, fetcher, clock, sleeps = make_collector(tmp_path, [empty])
    for _ in range(8):                                  # 연속 8번 (회로차단 임계값 5 보다 많이)
        rep = c.run_target(POS)
        assert rep.outcome == "ok" and rep.n_records == 0 and rep.attempts == 1
        clock.advance(90)
    assert len(fetcher.calls) == 8 and not sleeps      # 호출 1회/사이클, 백오프 대기 없음
    last = audits(pub)[-1]
    assert last["status_kind"] == "empty" and last["status_code"] == "4" and last["outcome"] == "ok"
    assert not pub.by_topic("urban.dlq.v1") and not pub.by_topic("urban.bus.position.v1")
    assert c.budget.used("bus_position") == 8


def test_weather_no_data_is_retried_next_cycle_not_marked_as_fetched(tmp_path):
    nodata = body(fixture_bytes("kma_ncst/nodata_03_20261103T160200.json"))
    good = ok("kma_ncst/ok_20261028T150951.json")
    c, pub, fetcher, clock, sleeps = make_collector(tmp_path, [nodata, good])
    rep = c.run_target(KMA)
    assert rep.outcome == "ok" and rep.n_records == 0 and not sleeps         # 재시도·대기 없음
    assert c.should_skip(KMA, clock()) is None                               # 발표 시각을 '받음'으로 기록하지 않았다 → 다음 주기에 다시 부른다
    assert audits(pub)[-1]["status_kind"] == "empty" and not pub.by_topic("urban.dlq.v1")
    clock.advance(600)
    assert c.run_target(KMA).n_records == 8                                  # 이번엔 데이터가 있다
    assert c.should_skip(KMA, clock()) == "base_already_fetched"             # 이제서야 중복 호출 방지가 걸린다
