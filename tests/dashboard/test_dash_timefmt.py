"""KST 변환·경과 표시·기간 계산 (D-152). 자정 경계와 이중 변환이 핵심."""
from datetime import date, datetime, timezone

import pytest

from dashboard import timefmt as T


def test_parse_accepts_z_and_offsets_and_rejects_naive():
    assert T.parse_utc("2026-11-05T06:20:30Z") == datetime(2026, 11, 5, 6, 20, 30, tzinfo=timezone.utc)
    assert T.parse_utc("2026-11-05T15:20:30+09:00") == datetime(2026, 11, 5, 6, 20, 30, tzinfo=timezone.utc)
    assert T.parse_utc("2026-11-05T06:20:30.123456Z").microsecond == 123456
    assert T.parse_utc(None) is None and T.parse_utc("") is None
    with pytest.raises(ValueError):
        T.parse_utc("2026-11-05T06:20:30")          # 오프셋 없음 → 추측하지 않는다
    with pytest.raises(ValueError):
        T.parse_utc("어제")


def test_kst_conversion_and_midnight_boundary():
    assert T.fmt_kst("2026-11-05T06:20:30Z") == "11-05 15:20 KST"
    assert T.fmt_kst("2026-11-04T14:59:59Z") == "11-04 23:59 KST"       # 아직 같은 날
    assert T.fmt_kst("2026-11-04T15:00:00Z") == "11-05 00:00 KST"       # 자정 넘김
    assert T.fmt_kst("2026-12-31T15:00:00Z", year=True) == "2027-01-01 00:00 KST"
    assert T.fmt_kst("2026-11-05T06:20:30Z", seconds=True) == "11-05 15:20:30 KST"


def test_fmt_kst_never_leaks_or_crashes_on_bad_values():
    for bad in (None, "", "garbage", "2026-11-05T06:20:30"):
        assert T.fmt_kst(bad) == T.DASH


def test_utc_z_and_window_are_exact_and_use_z():
    now = datetime(2026, 11, 5, 6, 20, 30, tzinfo=timezone.utc)
    assert T.utc_z(now) == "2026-11-05T06:20:30Z"
    assert T.utc_z(datetime(2026, 11, 5, 15, 0, tzinfo=T.KST)) == "2026-11-05T06:00:00Z"
    f, t = T.window(now, hours=24)
    assert (f, t) == ("2026-11-04T06:20:30Z", "2026-11-05T06:20:30Z") and "+" not in f + t
    assert T.window(now, days=7)[0] == "2026-10-29T06:20:30Z"
    with pytest.raises(ValueError):
        T.window(now)


def test_floor_to_makes_query_windows_stable_within_a_bucket():
    a = datetime(2026, 11, 5, 6, 20, 1, tzinfo=timezone.utc)
    b = datetime(2026, 11, 5, 6, 24, 59, tzinfo=timezone.utc)
    c = datetime(2026, 11, 5, 6, 25, 0, tzinfo=timezone.utc)
    assert T.window(T.floor_to(a, 300), hours=24) == T.window(T.floor_to(b, 300), hours=24)
    assert T.window(T.floor_to(a, 300), hours=24) != T.window(T.floor_to(c, 300), hours=24)
    with pytest.raises(ValueError):
        T.floor_to(a, 0)


def test_age_and_humanize():
    now = datetime(2026, 11, 5, 6, 20, 30, tzinfo=timezone.utc)
    assert T.age_seconds("2026-11-05T05:20:30Z", now) == 3600.0
    assert T.age_seconds("2026-11-05T07:00:00Z", now) == 0.0           # 미래(시계 차이)는 0 으로
    assert T.age_seconds(None, now) is None
    assert [T.humanize_age(x) for x in (None, 0, 59, 60, 3599, 3600, 3660, 86399, 86400, 90000)] == \
        ["—", "1분 미만", "1분 미만", "1분", "59분", "1시간", "1시간 1분", "23시간 59분", "1일", "1일 1시간"]


def test_day_type_guess_and_native_kst_fields_declared():
    assert T.day_type_guess(date(2026, 11, 5)) == "weekday"      # 목
    assert T.day_type_guess(date(2026, 11, 7)) == "sat"
    assert T.day_type_guess(date(2026, 11, 8)) == "sun"
    assert T.KST_NATIVE_FIELDS == ("event_date", "kst_hour")
    assert T.kst_hour_of("2026-11-04T15:00:00Z") == 0 and T.kst_date_of("2026-11-04T15:00:00Z") == date(2026, 11, 5)


def test_day_type_guess_knows_2026_holidays_and_holiday_wins_over_weekend():
    assert T.day_type_guess(date(2026, 12, 25)) == "holiday"      # 금, 공휴일
    assert T.day_type_guess(date(2026, 5, 24)) == "holiday"       # 일요일이지만 공휴일 → 마트(_DAY_TYPE)와 같은 우선순위
    assert T.day_type_guess(date(2027, 1, 1)) == "weekday"        # 목록 밖 연도는 근사(평일) — 모른다는 것을 숨기지 않는다 (D-180)


def test_holiday_list_matches_migration_014():
    import re
    from pathlib import Path
    from dashboard.holidays import KR_HOLIDAYS_2026
    sql = (Path(__file__).resolve().parents[2] / "sql" / "014_holidays_2026.sql").read_text(encoding="utf-8")
    assert {date.fromisoformat(d) for d in re.findall(r"\('(\d{4}-\d\d-\d\d)'::date", sql)} == set(KR_HOLIDAYS_2026)
