"""weather_observation_v1 — 기상청 초단기실황 (getUltraSrtNcst). 격자(nx, ny) 1곳 호출 → 카테고리마다 1행 (관측: 8행).

- event_time = baseDate + baseTime (KST). 발표 시각이지 호출 시각이 아니다.
- dedup_key = nx|ny|category|YYYYMMDDHHMM.
- 응답이 JSON 이어도 인증 오류는 XML 로 올 수 있다고 알려져 있으나 이 프로젝트에서는 아직 관측하지 못했다.
  그래서 게이트웨이 XML/JSON 오류를 모두 인식하고, 인식 못 하는 본문은 malformed 로 처리한다.
- value 는 문자열 그대로(value_raw)와 숫자(value)를 함께 둔다. 숫자가 아니거나 -999(결측 표기, 문서상)면 value 는 None.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from .base import ApiStatus, Body, CallContext, ParseResult, Reject, early_exit, malformed, to_text
from .common import ContractError, clean_str, minute_key, parse_kst_date_time, require, to_float, to_int

SOURCE_SYSTEM = "kma_ncst"
SCHEMA_VERSION = "weather_observation_v1"

# 초단기실황 카테고리 (기상청 활용가이드 기준). 2026-10-28 샘플에서 8개 모두 관측했다.
CATEGORIES: dict[str, tuple[str, str]] = {
    "T1H": ("기온", "℃"),
    "RN1": ("1시간 강수량", "mm"),
    "UUU": ("동서바람성분", "m/s"),
    "VVV": ("남북바람성분", "m/s"),
    "REH": ("습도", "%"),
    "PTY": ("강수형태", "code"),
    "VEC": ("풍향", "deg"),
    "WSD": ("풍속", "m/s"),
}
# PTY 코드표(문서 기준, 관측은 0 뿐): 0 없음, 1 비, 2 비/눈, 3 눈, 5 빗방울, 6 빗방울눈날림, 7 눈날림
_MISSING = {"-999", "-999.0"}


@dataclass(frozen=True)
class WeatherObservation:
    nx: int
    ny: int
    category: str                  # T1H, RN1, ... (CATEGORIES 참고)
    value_raw: str                 # 응답의 obsrValue 문자열 그대로
    value: Optional[float]         # 숫자로 읽을 수 있고 결측 표기가 아닐 때만
    category_name: Optional[str]   # CATEGORIES 에 있으면 한글 이름, 없으면 None (새 카테고리는 reject 하지 않는다)
    unit: Optional[str]


def dedup_key(nx: int, ny: int, category: str, event_time) -> str:
    return f"{nx}|{ny}|{category}|{minute_key(event_time)}"


def _items(response_body: dict) -> list[dict]:
    items = response_body.get("items")
    if not items:  # 데이터가 없으면 "" 로 오는 경우가 있다
        return []
    item = items.get("item") if isinstance(items, dict) else None
    if item is None:
        return []
    if isinstance(item, dict):  # 1건이면 목록이 아니라 객체로 오는 경우가 있다
        return [item]
    return list(item)


def parse(body: Body, ctx: CallContext) -> ParseResult:
    pre = early_exit(SOURCE_SYSTEM, SCHEMA_VERSION, ctx, body)
    if pre is not None:
        return pre
    try:
        data = json.loads(to_text(body))
        response = data["response"]
        header = response["header"]
    except (ValueError, KeyError, TypeError):
        return malformed(SOURCE_SYSTEM, SCHEMA_VERSION, "기상청 JSON 응답 구조가 아님")

    code = clean_str(header.get("resultCode"))
    message = clean_str(header.get("resultMsg"))
    if code != "00":
        # 00 이외의 코드는 아직 관측하지 못했다 → unknown 으로 두고 그대로 기록
        return ParseResult(SOURCE_SYSTEM, SCHEMA_VERSION, ApiStatus("unknown", code, message))

    resp_body = response.get("body") or {}
    items = _items(resp_body)
    kind = "ok" if items else "empty"
    try:
        total = to_int(resp_body.get("totalCount"))
    except ContractError:
        total = None
    res = ParseResult(SOURCE_SYSTEM, SCHEMA_VERSION, ApiStatus(kind, code, message), total_count=total)
    res.stats["rows"] = len(items)

    for i, item in enumerate(items):
        try:
            event_time = parse_kst_date_time(item.get("baseDate"), item.get("baseTime"))
            nx, ny = to_int(require(item, "nx")), to_int(require(item, "ny"))
            category = require(item, "category")
            raw = require(item, "obsrValue")
            try:
                value = None if raw in _MISSING else to_float(raw)
            except ContractError:
                value = None
            name, unit = CATEGORIES.get(category, (None, None))
            payload = WeatherObservation(nx=nx, ny=ny, category=category, value_raw=raw, value=value,
                                         category_name=name, unit=unit)
            res.records.append(ctx.record(SOURCE_SYSTEM, SCHEMA_VERSION, event_time,
                                          dedup_key(nx, ny, category, event_time), payload))
        except ContractError as e:
            res.rejects.append(Reject(reason=str(e), raw=item, index=i))
    return res
