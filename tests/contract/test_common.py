import pytest

from urbanstream.contracts.common import (KST, ContractError, clean_str, content_hash, make_event_id,
                                          parse_dt14, parse_kst_date_time, parse_mk_tm, to_flag, to_int)


def test_clean_str_treats_blank_and_space_as_missing():
    # 버스도착 API 는 빈 칸을 ' ' 로 보낸다
    assert clean_str(" ") is None
    assert clean_str("") is None
    assert clean_str(None) is None
    assert clean_str(" 가 ") == "가"
    assert clean_str("a\r\nb\rc") == "a\nb\nc"


def test_occr_time_accepts_hhmm_and_hhmmss():
    # 2026-11-01 관측: 같은 응답 안에 '1006' 과 '152200' 이 섞여 있다
    assert parse_kst_date_time("20261101", "1006").isoformat() == "2026-11-01T10:06:00+09:00"
    assert parse_kst_date_time("20261101", "152200").isoformat() == "2026-11-01T15:22:00+09:00"
    assert parse_kst_date_time("20261101", "165000").second == 0


def test_time_24_is_next_day_midnight():
    assert parse_kst_date_time("20261101", "2400").isoformat() == "2026-11-02T00:00:00+09:00"


@pytest.mark.parametrize("d,t", [("2026110", "1000"), ("20261101", "10"), ("20261101", "2561"),
                                 ("20261301", "1000"), ("20261101", "10:00"), (None, "1000"), ("20261101", None)])
def test_bad_date_time_raises(d, t):
    with pytest.raises(ContractError):
        parse_kst_date_time(d, t)


def test_dt14_and_mk_tm():
    assert parse_dt14("20261101215259").isoformat() == "2026-11-01T21:52:59+09:00"
    assert parse_mk_tm("2026-11-01 21:53:00.0").isoformat() == "2026-11-01T21:53:00+09:00"
    assert parse_mk_tm("2026-11-01 21:53:00").tzinfo == KST
    with pytest.raises(ContractError):
        parse_dt14("2026110100")  # 버스도착 첫 정류소 행에서 관측된 깨진 10자리 값
    with pytest.raises(ContractError):
        parse_mk_tm("21:53")


def test_numbers_and_flags():
    assert to_int("12") == 12 and to_int("12.0") == 12 and to_int(" ") is None
    with pytest.raises(ContractError):
        to_int("1.5")
    assert to_flag("1") is True and to_flag("0") is False and to_flag(None) is None
    with pytest.raises(ContractError):
        to_flag("2")


def test_ids_are_deterministic():
    assert make_event_id("bus_position", "a|b") == make_event_id("bus_position", "a|b")
    assert make_event_id("bus_position", "a|b") != make_event_id("bus_arrival", "a|b")
    assert content_hash("a", None) == content_hash("a", "")
    assert content_hash("a", "b") != content_hash("ab", "")
