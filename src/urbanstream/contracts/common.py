"""contracts 공통 유틸: 시각, 문자열, 숫자 파싱과 해시.

표준 라이브러리만 쓴다. (이유: docs/adr/0002-contracts-stdlib-dataclasses.md)

규칙
- 시각은 항상 timezone-aware datetime 이다. 소스가 주는 시각은 모두 KST(UTC+9)로 해석한다.
- 행(row) 하나가 잘못됐을 때는 ContractError 를 던진다. 파서가 이를 잡아 Reject 로 바꾼다.
  (한 행 때문에 호출 전체를 버리지 않는다.)
"""
from __future__ import annotations

import dataclasses
import hashlib
import math
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

KST = timezone(timedelta(hours=9), "KST")

_DATE8 = re.compile(r"^\d{8}$")
_DT14 = re.compile(r"^\d{14}$")
_MK_TM = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$")

# event_id 를 결정적으로 만들기 위한 고정 네임스페이스 (바꾸면 모든 event_id 가 바뀐다)
_EVENT_NS = uuid.uuid5(uuid.NAMESPACE_URL, "urn:urbanstream:event")


class ContractError(ValueError):
    """행 단위 계약 위반 (필수 값 누락, 형식 오류 등)."""


# --------------------------------------------------------------------------- 문자열/숫자
def clean_str(v: Any) -> Optional[str]:
    """None/빈 문자열/공백뿐인 문자열 -> None. CR/LF 는 \\n 으로 통일하고 앞뒤 공백을 제거한다.

    버스도착 API 는 빈 칸을 ' '(공백 1자)로 보내므로 공백뿐인 값도 결측이다.
    """
    if v is None:
        return None
    s = str(v).replace("\r\n", "\n").replace("\r", "\n").strip()
    return s or None


def require(row: dict, key: str) -> str:
    v = clean_str(row.get(key))
    if v is None:
        raise ContractError(f"필수 필드 누락: {key}")
    return v


def to_flag_lenient(v: Any, stats: dict, key: str = "unexpected_flag_values") -> Optional[bool]:
    """to_flag 와 같지만 예상 밖의 값('2' 등)이면 None 으로 두고 stats[key] 를 1 올린다.

    이유: 부가 플래그 하나가 예상 밖이라고 해서 그 행의 나머지 정상 값까지 버리지 않는다 (원칙 '데이터는 틀릴 수 있다').
    대신 건수를 세어 audit 에 남기므로 소스가 바뀌는 것(drift)은 눈에 보인다.
    """
    try:
        return to_flag(v)
    except ContractError:
        stats[key] = stats.get(key, 0) + 1
        return None


def to_int(v: Any) -> Optional[int]:
    s = clean_str(v)
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        f = float(s)
    except ValueError as e:
        raise ContractError(f"정수가 아님: {s!r}") from e
    if not math.isfinite(f) or not f.is_integer():
        raise ContractError(f"정수가 아님: {s!r}")
    return int(f)


def to_float(v: Any) -> Optional[float]:
    s = clean_str(v)
    if s is None:
        return None
    try:
        f = float(s)
    except ValueError as e:
        raise ContractError(f"실수가 아님: {s!r}") from e
    if not math.isfinite(f):
        raise ContractError(f"유한한 실수가 아님: {s!r}")
    return f


def to_flag(v: Any) -> Optional[bool]:
    """'0'/'1', 'N'/'Y' 형태의 플래그."""
    s = clean_str(v)
    if s is None:
        return None
    if s in ("1", "Y", "y"):
        return True
    if s in ("0", "N", "n"):
        return False
    raise ContractError(f"플래그가 아님: {s!r}")


# --------------------------------------------------------------------------- 시각
def parse_kst_date_time(date8: Any, time_str: Any) -> datetime:
    """'20261101' + 'HHMM' 또는 'HHMMSS' -> KST datetime.

    서울 돌발 API 의 occr_time 은 4자리(HHMM)와 6자리(HHMMSS)가 섞여 온다 (2026-11-01 관측).
    기상청 baseTime('1400')도 4자리다. 'HH=24'는 다음날 00시로 해석한다.
    """
    d = clean_str(date8)
    t = clean_str(time_str)
    if d is None or not _DATE8.match(d):
        raise ContractError(f"날짜 형식 오류(YYYYMMDD): {date8!r}")
    if t is None or not t.isdigit() or len(t) not in (4, 6):
        raise ContractError(f"시각 형식 오류(HHMM/HHMMSS): {time_str!r}")
    hh, mm = int(t[0:2]), int(t[2:4])
    ss = int(t[4:6]) if len(t) == 6 else 0
    try:
        base = datetime.strptime(d, "%Y%m%d").replace(tzinfo=KST)
    except ValueError as e:
        raise ContractError(f"존재하지 않는 날짜: {d!r}") from e
    if hh == 24 and mm == 0 and ss == 0:
        return base + timedelta(days=1)
    if hh > 23 or mm > 59 or ss > 59:
        raise ContractError(f"시각 범위 오류: {time_str!r}")
    return base.replace(hour=hh, minute=mm, second=ss)


def parse_dt14(v: Any) -> datetime:
    """'20261101215259'(YYYYMMDDHHMMSS) -> KST datetime. 버스위치 dataTm, 버스도착 firstTm/lastTm."""
    s = clean_str(v)
    if s is None or not _DT14.match(s):
        raise ContractError(f"14자리 시각 형식 오류: {v!r}")
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError as e:
        raise ContractError(f"존재하지 않는 시각: {s!r}") from e


def parse_mk_tm(v: Any) -> datetime:
    """'2026-11-01 21:53:00.0' -> KST datetime. 버스도착 mkTm."""
    s = clean_str(v)
    m = _MK_TM.match(s) if s else None
    if not m:
        raise ContractError(f"mkTm 형식 오류: {v!r}")
    y, mo, d, h, mi, se = (int(x) for x in m.groups())
    try:
        return datetime(y, mo, d, h, mi, se, tzinfo=KST)
    except ValueError as e:
        raise ContractError(f"존재하지 않는 시각: {s!r}") from e


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def minute_key(dt: datetime) -> str:
    """분 단위 키 'YYYYMMDDHHMM' (KST 기준)."""
    return dt.astimezone(KST).strftime("%Y%m%d%H%M")


def second_key(dt: datetime) -> str:
    """초 단위 키 'YYYYMMDDHHMMSS' (KST 기준). 소스가 분보다 자주 갱신될 때 스냅샷을 구분한다 (버스도착 mkTm)."""
    return dt.astimezone(KST).strftime("%Y%m%d%H%M%S")


# --------------------------------------------------------------------------- 해시/ID
def content_hash(*parts: Any) -> str:
    """값들의 안정적인 해시(앞 16자). None 은 빈 문자열로 취급한다."""
    joined = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def make_event_id(source_system: str, dedup_key: str) -> str:
    """같은 (source_system, dedup_key) 는 항상 같은 event_id. 재처리해도 ID 가 바뀌지 않는다."""
    return str(uuid.uuid5(_EVENT_NS, f"{source_system}|{dedup_key}"))


# --------------------------------------------------------------------------- 직렬화
def to_jsonable(obj: Any) -> Any:
    """dataclass/datetime 이 섞인 객체를 json.dumps 가능한 값으로. datetime 은 오프셋 포함 ISO-8601."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_jsonable(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    return obj
