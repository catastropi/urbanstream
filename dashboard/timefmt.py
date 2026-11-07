"""시각 변환·경과 시간 표시 (ADR-0012 D10-5, D-152). 순수 함수 — streamlit·requests 를 import 하지 않는다.

규칙
- API 의 시각 필드(event_time, as_of, loaded_at, checked_at …)는 UTC 'Z' 이다 → 표시할 때만 KST 로 바꾼다.
- 일 단위 필드 event_date·kst_hour 는 이미 KST 다(마트 정의, ADR-0008) → **다시 바꾸지 않는다** (KST_NATIVE_FIELDS).
- KST 는 고정 오프셋 UTC+9 다. 한국은 서머타임이 없어 tzdata 의존성이 필요 없다. 되돌리려면 zoneinfo("Asia/Seoul") 로 KST 만 바꾸면 된다.
- 오프셋이 없는 시각은 해석하지 않는다(UTC 인지 KST 인지 모르므로 추측하지 않는다).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from .holidays import is_known_holiday

UTC = timezone.utc
KST = timezone(timedelta(hours=9), "KST")
DASH = "—"

# 이미 KST 로 오는 필드. 화면 코드가 이 필드에 fmt_kst/to_kst 를 쓰면 구조 가드 시험이 실패한다.
KST_NATIVE_FIELDS = ("event_date", "kst_hour")


def parse_utc(value: Any) -> Optional[datetime]:
    """ISO 8601 문자열/datetime → UTC datetime. None·빈 문자열은 None. 오프셋 없는 값은 ValueError."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if s[-1:] in ("Z", "z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            raise ValueError("시각 형식을 해석할 수 없습니다") from None
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("오프셋이 없는 시각은 해석하지 않습니다")
    return dt.astimezone(UTC)


def to_kst(value: Any) -> Optional[datetime]:
    dt = parse_utc(value)
    return None if dt is None else dt.astimezone(KST)


def fmt_kst(value: Any, *, year: bool = False, seconds: bool = False) -> str:
    """'11-05 15:20 KST'. None → '—'. 해석 못 하는 값은 '—' 로 두고 원문을 흘리지 않는다."""
    try:
        dt = to_kst(value)
    except ValueError:
        return DASH
    if dt is None:
        return DASH
    pattern = ("%Y-" if year else "") + "%m-%d %H:%M" + (":%S" if seconds else "")
    return dt.strftime(pattern) + " KST"


def utc_z(value: datetime) -> str:
    """API 로 보내는 시각. 항상 UTC 'Z' (URL 에서 '+' 인코딩 문제를 피한다)."""
    dt = parse_utc(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # type: ignore[union-attr]


def window(now: datetime, *, hours: Optional[float] = None, days: Optional[float] = None) -> tuple[str, str]:
    """(from, to) UTC 'Z' 문자열. API 는 from 포함·to 제외, 최대 7일."""
    end = parse_utc(now)
    span = timedelta(hours=hours or 0) + timedelta(days=days or 0)
    if span <= timedelta(0):
        raise ValueError("기간은 0 보다 커야 합니다")
    return utc_z(end - span), utc_z(end)


def age_seconds(as_of: Any, now: datetime) -> Optional[float]:
    dt = parse_utc(as_of)
    if dt is None:
        return None
    return max(0.0, (parse_utc(now) - dt).total_seconds())  # type: ignore[operator]


def humanize_age(sec: Optional[float]) -> str:
    if sec is None:
        return DASH
    s = int(max(0, sec))
    if s < 60:
        return "1분 미만"
    m = s // 60
    if m < 60:
        return f"{m}분"
    h, mm = divmod(m, 60)
    if h < 24:
        return f"{h}시간" + (f" {mm}분" if mm else "")
    d, hh = divmod(h, 24)
    return f"{d}일" + (f" {hh}시간" if hh else "")


def kst_date_of(value: Any) -> Optional[date]:
    dt = to_kst(value)
    return None if dt is None else dt.date()


def day_type_guess(kst_day: date) -> str:
    """평일/토/일/공휴일. 공휴일은 dashboard/holidays.py 목록(2026, 미확인 D-180)에 있는 날만 안다 — 목록에 없는 연도는 평일로 근사한다.
    마트(`_DAY_TYPE`)와 같은 순서: 공휴일이 토·일보다 우선."""
    if is_known_holiday(kst_day):
        return "holiday"
    wd = kst_day.weekday()
    return "sat" if wd == 5 else "sun" if wd == 6 else "weekday"


def kst_hour_of(value: Any) -> Optional[int]:
    dt = to_kst(value)
    return None if dt is None else dt.hour


def floor_to(value: Any, seconds: int) -> datetime:
    """시각을 seconds 단위로 내림(UTC). **조회 기간(from/to)을 만들 때 반드시 쓴다** — 초 단위로 달라지는 from/to 는 캐시 키가
    매번 달라져 캐시가 한 번도 맞지 않고 API 호출 예산을 태운다 (ADR-0012 D10-4, 시험: test_views_budget)."""
    if seconds <= 0:
        raise ValueError("seconds 는 양수")
    dt = parse_utc(value)
    ts = int(dt.timestamp())  # type: ignore[union-attr]
    return datetime.fromtimestamp(ts - ts % seconds, UTC)
