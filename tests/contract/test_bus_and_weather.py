"""서울 버스위치(bus_position_v1), 버스도착(bus_arrival_v1), 기상청(weather_observation_v1) 계약."""
from conftest import MANIFEST, read_fixture, run


# ------------------------------------------------------------------ 버스위치
def test_position_ok(position_ok):
    assert position_ok.ok and len(position_ok.records) == 13 and not position_ok.rejects
    p = position_ok.records[0].payload
    assert p.route_id == "100100118"              # 호출 인자에서 받은 값
    assert 126 < p.lon < 128 and 37 < p.lat < 38  # 서울 부근
    assert isinstance(p.is_running, bool)


def test_position_event_time_is_per_vehicle_gps_time(position_ok):
    times = {r.envelope.event_time for r in position_ok.records}
    assert len(times) > 1  # 차량마다 보고 시각이 다르다 (한 호출 안에서 약 20초 폭)
    spread = (max(times) - min(times)).total_seconds()
    assert 0 < spread < 60
    for r in position_ok.records:
        assert r.envelope.event_time <= r.envelope.fetched_at  # 미래 시각 없음


def test_position_dedup_key_unique_and_stable(position_ok):
    keys = [r.envelope.dedup_key for r in position_ok.records]
    assert len(set(keys)) == 13
    again = run("bus_position/ok_20261101T215320.xml")
    assert [r.envelope.event_id for r in again.records] == [r.envelope.event_id for r in position_ok.records]


def test_position_plain_no_whitespace_removed():
    raw = read_fixture("bus_position/ok_20261101T215320.xml").decode("utf-8")
    body = raw.replace("<plainNo>서울74사4573</plainNo>", "<plainNo> 서울74사 4573 </plainNo>", 1)
    res = run("bus_position/ok_20261101T215320.xml", body=body)
    assert res.records[0].payload.plain_no == "서울74사4573"


def test_position_row_without_vehicle_is_rejected():
    raw = read_fixture("bus_position/ok_20261101T215320.xml").decode("utf-8")
    body = raw.replace("<vehId>111049702</vehId>", "", 1)
    res = run("bus_position/ok_20261101T215320.xml", body=body)
    assert len(res.records) == 12 and len(res.rejects) == 1 and "vehId" in res.rejects[0].reason


# ------------------------------------------------------------------ 버스도착
def test_arrival_unpivots_slots_and_skips_empty_ones(arrival_ok):
    # 샘플: 104행, vehId1 != 0 인 행 102, vehId2 != 0 인 행 91 → 레코드 193, 빈 슬롯 15
    assert arrival_ok.ok and arrival_ok.stats["rows"] == 104
    assert len(arrival_ok.records) == 193 and not arrival_ok.rejects
    assert arrival_ok.stats["empty_slots"] == 15
    assert len(arrival_ok.records) + arrival_ok.stats["empty_slots"] == 104 * 2
    assert {r.payload.slot for r in arrival_ok.records} == {1, 2}


def test_arrival_dedup_key_unique_and_includes_slot(arrival_ok):
    keys = [r.envelope.dedup_key for r in arrival_ok.records]
    assert len(set(keys)) == len(keys)
    assert all(k.startswith("100100118|") and k.endswith("|20261101215300") for k in keys)
    assert {r.envelope.event_time.isoformat() for r in arrival_ok.records} == {"2026-11-01T21:53:00+09:00"}


def test_arrival_blank_values_and_derived_fields(arrival_ok):
    for r in arrival_ok.records:
        assert r.payload.plain_no is None or " " not in r.payload.plain_no
        assert r.payload.veh_id != "0"
    msgs = {r.payload.arrival_msg: r.payload for r in arrival_ok.records}
    p = next(r.payload for r in arrival_ok.records if r.payload.arrival_msg == "1분56초후[1번째 전]")
    assert p.stops_away == 1 and p.arrive_in_sec == 116   # 메시지의 시간(116초)과 traTime 이 일치
    soon = next(r.payload for r in arrival_ok.records if r.payload.arrival_msg == "곧 도착")
    assert soon.stops_away is None
    assert msgs  # 비어 있지 않음


def test_arrival_malformed_first_last_tm_is_tolerated_and_counted(arrival_ok):
    # 노선 첫 정류소 행의 firstTm/lastTm 이 10자리('2026110100')로 온다 → 행을 버리지 않고 None + 통계
    assert arrival_ok.stats["malformed_first_last_tm"] == 1
    first_row = [r for r in arrival_ok.records if r.payload.station_order == 1]
    assert all(r.payload.first_bus_time is None and r.payload.last_bus_time is None for r in first_row)
    others = [r for r in arrival_ok.records if r.payload.station_order > 1]
    assert all(r.payload.first_bus_time is not None for r in others)


def test_arrival_row_without_mktm_is_rejected_whole_row():
    raw = read_fixture("bus_arrival/ok_20261101T215321.xml").decode("utf-8")
    body = raw.replace("<mkTm>2026-11-01 21:53:00.0</mkTm>", "", 1)
    res = run("bus_arrival/ok_20261101T215321.xml", body=body)
    assert len(res.rejects) == 1 and "mkTm" in res.rejects[0].reason


def _replace_nth(text: str, old: str, new: str, n: int) -> str:
    import re
    idx = [m.start() for m in re.finditer(re.escape(old), text)][n]
    return text[:idx] + new + text[idx + len(old):]


def test_arrive_code_is_a_code_not_a_flag_and_odd_flags_do_not_drop_rows():
    # 2026-11-02 12:31 실제 사례: '출발대기' 행들에서 isArrive1 = 2. 예전에는 슬롯이 reject 됐다
    raw = read_fixture("bus_arrival/ok_20261101T215321.xml").decode("utf-8")
    raw = _replace_nth(raw, "<isArrive1>0</isArrive1>", "<isArrive1>2</isArrive1>", 3)
    raw = _replace_nth(raw, "<isLast1>0</isLast1>", "<isLast1>9</isLast1>", 3)
    res = run("bus_arrival/ok_20261101T215321.xml", body=raw)
    assert not res.rejects and len(res.records) == 193                       # 한 건도 잃지 않는다
    assert [r.payload.arrive_code for r in res.records if r.payload.arrive_code == 2] == [2]
    odd = [r for r in res.records if r.payload.is_last_bus is None]
    assert len(odd) == 1 and res.stats["unexpected_flag_values"] == 1        # 값은 None, 건수는 기록


def test_position_odd_flags_are_counted_not_rejected():
    raw = read_fixture("bus_position/ok_20261101T215320.xml").decode("utf-8").replace(
        "<isrunyn>1</isrunyn>", "<isrunyn>7</isrunyn>", 2)
    res = run("bus_position/ok_20261101T215320.xml", body=raw)
    assert len(res.records) == 13 and not res.rejects and res.stats["unexpected_flag_values"] == 2


# ------------------------------------------------------------------ 위치 ↔ 도착 (소스 간 계약)
def test_position_and_arrival_join_on_stop_and_vehicle(position_ok, arrival_ok):
    stations = {r.payload.station_id for r in arrival_ok.records}
    next_stops = {r.payload.next_stop_id for r in position_ok.records}
    assert next_stops <= stations                       # nextStId ↔ stId (12/12)
    arrival_veh = {r.payload.veh_id for r in arrival_ok.records}
    position_veh = {r.payload.veh_id for r in position_ok.records}
    assert position_veh <= arrival_veh                  # vehId 로도 조인된다 (13/13)
    assert {r.payload.route_id for r in arrival_ok.records} == {r.payload.route_id for r in position_ok.records}


# ------------------------------------------------------------------ 기상
def test_weather_ok(weather_ok):
    assert weather_ok.ok and len(weather_ok.records) == 8 and not weather_ok.rejects
    by_cat = {r.payload.category: r for r in weather_ok.records}
    assert set(by_cat) == {"PTY", "REH", "RN1", "T1H", "UUU", "VEC", "VVV", "WSD"}
    assert by_cat["T1H"].payload.value == 14.1 and by_cat["T1H"].payload.unit == "℃"
    assert by_cat["T1H"].envelope.event_time.isoformat() == "2026-10-28T14:00:00+09:00"  # 호출(15:09)이 아니라 발표 시각
    assert by_cat["T1H"].envelope.dedup_key == "60|127|T1H|202610281400"
    assert len({r.envelope.dedup_key for r in weather_ok.records}) == 8


def test_weather_unknown_category_and_missing_value_are_kept():
    body = ('{"response":{"header":{"resultCode":"00","resultMsg":"NORMAL_SERVICE"},"body":{"items":{"item":['
            '{"baseDate":"20261028","baseTime":"1400","category":"ZZZ","nx":60,"ny":127,"obsrValue":"1"},'
            '{"baseDate":"20261028","baseTime":"1400","category":"T1H","nx":60,"ny":127,"obsrValue":"-999"}'
            ']}, "totalCount":2}}}')
    res = run("kma_ncst/ok_20261028T150951.json", body=body)
    assert len(res.records) == 2 and not res.rejects
    zzz, t1h = res.records[0].payload, res.records[1].payload
    assert zzz.category_name is None and zzz.value == 1.0   # 새 카테고리를 버리지 않는다
    assert t1h.value is None and t1h.value_raw == "-999"    # 결측 표기는 None, 원문은 보존


def test_weather_single_item_object_and_empty_items():
    one = ('{"response":{"header":{"resultCode":"00","resultMsg":"x"},"body":{"items":{"item":'
           '{"baseDate":"20261028","baseTime":"1400","category":"T1H","nx":60,"ny":127,"obsrValue":"3"}}}}}')
    assert len(run("kma_ncst/ok_20261028T150951.json", body=one).records) == 1
    none = '{"response":{"header":{"resultCode":"00","resultMsg":"x"},"body":{"items":"","totalCount":0}}}'
    res = run("kma_ncst/ok_20261028T150951.json", body=none)
    assert res.status.kind == "empty" and res.ok and res.records == []


def test_weather_non_00_code_is_unknown_not_guessed():
    body = '{"response":{"header":{"resultCode":"99","resultMsg":"SOMETHING"},"body":{}}}'
    res = run("kma_ncst/ok_20261028T150951.json", body=body)
    assert res.status.kind == "unknown" and res.status.code == "99" and not res.ok


def test_weather_non_json_is_malformed():
    assert run("kma_ncst/ok_20261028T150951.json", body="<html>oops</html>").status.kind == "malformed"


# ------------------------------------------------------------------ 게이트웨이 오류 (공통)
def test_gateway_errors_are_recognised():
    for rel in ("gateway/err_no_service_12_20261013T190221.json", "gateway/err_401_20261101T180914.json"):
        res = run(rel)
        assert res.status.kind == MANIFEST[rel]["expect_kind"] and res.records == []
    assert run("gateway/err_no_service_12_20261013T190221.json").status.code == "12"
    # 401 JSON 은 HTTP 가 200 으로 기록돼 있어도 본문의 status 로 인증 오류를 알아본다
    assert run("gateway/err_401_20261101T180914.json", http_status=200).status.kind == "auth"


def test_arrival_key_distinguishes_snapshots_within_the_same_minute():
    # 서버는 약 20초마다 새 스냅샷을 만든다 (mkTm 21:53:02 → 21:53:19 → 21:53:39 관측). 분 단위 키였다면 셋이 한 키로 합쳐진다.
    raw = read_fixture("bus_arrival/ok_20261101T215321.xml").decode("utf-8")
    assert raw.count("<mkTm>2026-11-01 21:53:00.0</mkTm>") == 104
    keys = {}
    for sec in ("02", "19", "39"):
        res = run("bus_arrival/ok_20261101T215321.xml",
                  body=raw.replace("21:53:00.0</mkTm>", f"21:53:{sec}.0</mkTm>"))
        assert res.ok and not res.rejects
        keys[sec] = {r.envelope.dedup_key for r in res.records}
    assert keys["02"].isdisjoint(keys["19"]) and keys["19"].isdisjoint(keys["39"]) and keys["02"].isdisjoint(keys["39"])
    again = run("bus_arrival/ok_20261101T215321.xml", body=raw.replace("21:53:00.0</mkTm>", "21:53:19.0</mkTm>"))
    assert {r.envelope.dedup_key for r in again.records} == keys["19"]     # 같은 스냅샷을 다시 받으면 같은 키


# ------------------------------------------------------------------ 빈 응답 분류 (2026-11-04, ADR-0008 결정 9)
def test_bus_position_header_code_4_no_result_is_empty_not_unknown():
    res = run("bus_position/empty_code4_20261104T024101.xml")
    assert res.status.kind == "empty" and res.status.code == "4" and res.ok and res.records == []
    assert "결과가 없습니다" in res.status.message


def test_bus_position_other_nonzero_codes_stay_unknown():
    xml = run.__globals__["read_fixture"]("bus_position/empty_code4_20261104T024101.xml").decode("utf-8")
    other = xml.replace("<headerCd>4</headerCd>", "<headerCd>5</headerCd>")
    res = run("bus_position/empty_code4_20261104T024101.xml", body=other)
    assert res.status.kind == "unknown" and res.status.code == "5" and not res.ok


def test_bus_position_code_4_with_rows_is_not_trusted_as_empty():
    # 코드가 4 인데 행이 있으면 (모양을 모르는 응답) 비어 있다고 단정하지 않는다
    ok_xml = run.__globals__["read_fixture"]("bus_position/ok_20261101T215320.xml").decode("utf-8")
    weird = ok_xml.replace("<headerCd>0</headerCd>", "<headerCd>4</headerCd>")
    res = run("bus_position/ok_20261101T215320.xml", body=weird)
    assert res.status.kind == "unknown"


def test_weather_result_code_03_no_data_is_empty_not_unknown():
    res = run("kma_ncst/nodata_03_20261103T160200.json")
    assert res.status.kind == "empty" and res.status.code == "03" and res.ok and res.records == []
