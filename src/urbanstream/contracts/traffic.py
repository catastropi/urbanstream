"""traffic_speed_v1 — 서울 실시간 도로 소통 정보 (TrafficInfo).

소스가 event_time 을 주지 않는다 (관측). 그래서 event_time = 수집 시각을 분 단위로 절삭한 값으로 정한다.
→ 같은 분에 같은 링크를 두 번 호출하면 같은 dedup_key 가 되어 중복으로 걸러진다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from . import seoul
from .base import Body, CallContext, ParseResult, Reject, early_exit, malformed
from .common import ContractError, floor_minute, minute_key, require, to_float, to_int

SOURCE_SYSTEM = "seoul_traffic"
SCHEMA_VERSION = "traffic_speed_v1"


@dataclass(frozen=True)
class TrafficSpeed:
    link_id: str                       # 도로 링크 ID (prcs_spd 가 속한 구간)
    speed_kmh: Optional[float]         # prcs_spd: 구간 속도(km/h). 0 의 의미(정체 vs 결측)는 미확인 → 그대로 보존
    travel_time_sec: Optional[int]     # prcs_trv_time: 구간 통과 소요시간(초)


def dedup_key(link_id: str, event_time) -> str:
    return f"{link_id}|{minute_key(event_time)}"


def parse(body: Body, ctx: CallContext) -> ParseResult:
    pre = early_exit(SOURCE_SYSTEM, SCHEMA_VERSION, ctx, body)
    if pre is not None:
        return pre
    try:
        status, rows, total = seoul.read(body)
    except ContractError as e:
        return malformed(SOURCE_SYSTEM, SCHEMA_VERSION, str(e))

    res = ParseResult(SOURCE_SYSTEM, SCHEMA_VERSION, status, total_count=total)
    res.stats["rows"] = len(rows)
    if not status.ok:
        return res

    event_time = floor_minute(ctx.fetched_at)
    for i, row in enumerate(rows):
        try:
            link_id = require(row, "link_id")
            payload = TrafficSpeed(
                link_id=link_id,
                speed_kmh=to_float(row.get("prcs_spd")),
                travel_time_sec=to_int(row.get("prcs_trv_time")),
            )
            res.records.append(ctx.record(SOURCE_SYSTEM, SCHEMA_VERSION, event_time,
                                          dedup_key(link_id, event_time), payload))
        except ContractError as e:
            res.rejects.append(Reject(reason=str(e), raw=row, index=i))
    return res
