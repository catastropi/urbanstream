"""서울 도로소통(traffic_speed_v1)과 돌발(incident_v1) 계약."""
from conftest import MANIFEST, read_fixture, run


# ------------------------------------------------------------------ 도로소통
def test_traffic_ok(traffic_ok):
    assert traffic_ok.ok and traffic_ok.status.code == "INFO-000"
    assert len(traffic_ok.records) == 1 and not traffic_ok.rejects
    rec = traffic_ok.records[0]
    assert rec.payload.link_id == "1220003800"
    assert rec.payload.speed_kmh == 16.0 and rec.payload.travel_time_sec == 675


def test_traffic_event_time_is_fetched_at_floored_to_minute(traffic_ok):
    env = traffic_ok.records[0].envelope
    # 소스에 event_time 이 없다 → 수집 시각(18:07:53)을 분 단위로 절삭
    assert env.event_time.isoformat() == "2026-11-01T18:07:00+09:00"
    assert env.fetched_at.isoformat() == "2026-11-01T18:07:53+09:00"
    assert env.dedup_key == "1220003800|202611011807"


def test_traffic_same_minute_gives_same_dedup_key_and_event_id():
    a = run("seoul_traffic/ok_20261101T180753.xml").records[0].envelope
    # 같은 분(18:07:59)에 다시 호출한 경우를 흉내: manifest 의 시각만 바꿔서 직접 실행
    from datetime import datetime
    from urbanstream.contracts import SOURCES, CallContext
    b = SOURCES["seoul_traffic"].parse(
        read_fixture("seoul_traffic/ok_20261101T180753.xml"),
        CallContext(fetched_at=datetime.fromisoformat("2026-11-01T18:07:59+09:00"))).records[0].envelope
    assert a.dedup_key == b.dedup_key and a.event_id == b.event_id
    assert a.ingest_id != b.ingest_id  # 호출은 다르다


def test_traffic_error_bodies_become_status_not_exceptions():
    # 관측한 서로 다른 오류 모양: 루트가 <RESULT> 인 것(INFO-100), <response> 로 감싼 것(ERROR-301)
    for rel in ("seoul_traffic/err_info100_20261028T150949.xml", "seoul_traffic/err_error301_20261028T151706.xml"):
        res = run(rel)
        assert not res.ok and res.status.kind == MANIFEST[rel]["expect_kind"]
        assert res.records == [] and res.status.message


def test_traffic_row_without_link_id_is_rejected_not_fatal():
    body = ("<TrafficInfo><list_total_count>2</list_total_count><RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>"
            "<row><prcs_spd>10</prcs_spd></row>"
            "<row><link_id>1</link_id><prcs_spd>abc</prcs_spd></row>"
            "<row><link_id>2</link_id><prcs_spd>0</prcs_spd><prcs_trv_time>30</prcs_trv_time></row></TrafficInfo>")
    res = run("seoul_traffic/ok_20261101T180753.xml", body=body)
    assert [r.payload.link_id for r in res.records] == ["2"]
    assert res.records[0].payload.speed_kmh == 0.0  # 0 은 결측으로 바꾸지 않고 보존한다
    assert len(res.rejects) == 2 and res.total_count == 2


def test_broken_xml_is_malformed():
    res = run("seoul_traffic/ok_20261101T180753.xml", body=b"<TrafficInfo><row>")
    assert res.status.kind == "malformed" and not res.ok


def test_http_error_is_classified_before_parsing():
    res = run("seoul_traffic/ok_20261101T180753.xml", body=b"Service Unavailable", http_status=503)
    assert res.status.kind == "server_error" and res.status.code == "503"
    assert run("seoul_traffic/ok_20261101T180753.xml", body=b"", http_status=429).status.kind == "rate_limited"


# ------------------------------------------------------------------ 돌발
def test_incident_ok(incident_ok):
    assert incident_ok.ok and len(incident_ok.records) == 10 and not incident_ok.rejects
    assert incident_ok.total_count == 10
    keys = [r.envelope.dedup_key for r in incident_ok.records]
    assert len(set(keys)) == 10
    assert len({r.payload.acc_id for r in incident_ok.records}) == 10


def test_incident_handles_mixed_time_lengths(incident_ok):
    by_id = {r.payload.acc_id: r for r in incident_ok.records}
    assert by_id["1107757"].payload.occurred_at.isoformat() == "2026-11-01T10:06:00+09:00"  # HHMM
    assert by_id["1107793"].payload.occurred_at.isoformat() == "2026-11-01T15:22:00+09:00"  # HHMMSS
    assert by_id["1107802"].envelope.event_time == by_id["1107802"].payload.occurred_at
    assert by_id["1107802"].payload.acc_type == "A01"  # 사고


def test_incident_blank_road_code_is_none_and_crlf_normalized(incident_ok):
    by_id = {r.payload.acc_id: r for r in incident_ok.records}
    assert by_id["1107757"].payload.road_code is None  # 빈 문자열로 오던 값
    first = by_id["1071788"].payload
    assert "\r" not in first.description and "\n" in first.description
    assert first.expected_clear_at.isoformat() == "2027-03-01T00:00:00+09:00"


def test_incident_content_change_creates_new_dedup_key_but_keeps_identity():
    raw = read_fixture("seoul_incident/ok_20261101T180755.xml").decode("utf-8")
    assert raw.count("<exp_clr_time>200000</exp_clr_time>") >= 1
    changed = raw.replace("<exp_clr_time>200000</exp_clr_time>", "<exp_clr_time>210000</exp_clr_time>", 1)
    a = run("seoul_incident/ok_20261101T180755.xml").records
    b = run("seoul_incident/ok_20261101T180755.xml", body=changed).records
    diff = [(x, y) for x, y in zip(a, b) if x.envelope.dedup_key != y.envelope.dedup_key]
    assert len(diff) == 1                                   # 바뀐 1건만 새 키
    x, y = diff[0]
    assert x.payload.acc_id == y.payload.acc_id             # acc_id 는 같다 → Silver 에서 같은 돌발의 변경 이력으로 묶는다
    assert x.envelope.event_time == y.envelope.event_time   # 발생 시각은 그대로


def test_incident_row_with_bad_time_is_rejected():
    body = ("<AccInfo><list_total_count>1</list_total_count><RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>"
            "<row><acc_id>9</acc_id><occr_date>20261101</occr_date><occr_time>99</occr_time><acc_type>A01</acc_type></row></AccInfo>")
    res = run("seoul_incident/ok_20261101T180755.xml", body=body)
    assert res.records == [] and len(res.rejects) == 1 and "시각" in res.rejects[0].reason
