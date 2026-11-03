"""bus_position_v1 — 서울 버스위치 (getBusPosByRtid). 노선 1개 호출 → 운행 중인 차량마다 1행.

- event_time = dataTm (차량의 GPS 보고 시각, KST). 차량마다 다르다: 한 호출 안에서 최대 약 20초 차이 (2026-11-01 관측).
- dedup_key = vehId + dataTm (13건 모두 유일함을 샘플로 확인).
- 응답에는 노선 ID 가 없다. 호출 인자의 busRouteId 를 route_id 로 받아 payload 에 넣는다.
- gpsX/gpsY 는 WGS84 경도/위도, posX/posY 는 TM 좌표(EPSG:5181, 13점 비교 최대 오차 약 6 m).
- 값의 의미를 아직 확인하지 못한 필드(lastStTm, nextStTm, sectDist, fullSectDist, rtDist, congetion, stopFlag, trnstnid)는
  원래 이름을 snake_case 로만 바꿔 담는다. 의미를 확정하면 api-catalog 에 기록하고 필요하면 v2 에서 이름을 바꾼다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from . import seoulbus
from .base import Body, CallContext, ParseResult, Reject, early_exit, malformed
from .common import (ContractError, clean_str, parse_dt14, require, to_flag_lenient, to_float, to_int)

SOURCE_SYSTEM = "bus_position"
SCHEMA_VERSION = "bus_position_v1"


@dataclass(frozen=True)
class BusPosition:
    route_id: Optional[str]        # 호출 인자(busRouteId). 응답에는 없다
    veh_id: str                    # vehId
    plain_no: Optional[str]        # 차량 번호판. 공백을 모두 제거한다
    lon: Optional[float]           # gpsX
    lat: Optional[float]           # gpsY
    tm_x: Optional[float]          # posX
    tm_y: Optional[float]          # posY
    section_id: Optional[str]      # sectionId
    sect_ord: Optional[int]        # sectOrd: 구간 순번
    last_stop_id: Optional[str]    # lastStnId: 직전 정류소
    next_stop_id: Optional[str]    # nextStId: 다음 정류소 (버스도착 stId 와 조인)
    turn_stop_id: Optional[str]    # trnstnid (의미 미확인)
    last_st_tm: Optional[int]      # lastStTm (의미 미확인)
    next_st_tm: Optional[int]      # nextStTm (의미 미확인)
    sect_dist: Optional[float]     # sectDist (의미 미확인)
    full_sect_dist: Optional[float]  # fullSectDist (의미 미확인)
    rt_dist: Optional[float]       # rtDist (의미 미확인)
    bus_type: Optional[int]        # busType
    congestion: Optional[int]      # congetion (API 철자 그대로 오타). 값의 의미 미확인
    stop_flag: Optional[int]       # stopFlag (의미 미확인)
    is_running: Optional[bool]     # isrunyn
    is_last: Optional[bool]        # islastyn
    is_full: Optional[bool]        # isFullFlag


def dedup_key(veh_id: str, data_tm: str) -> str:
    return f"{veh_id}|{data_tm}"


def parse(body: Body, ctx: CallContext, *, route_id: Optional[str] = None) -> ParseResult:
    pre = early_exit(SOURCE_SYSTEM, SCHEMA_VERSION, ctx, body)
    if pre is not None:
        return pre
    try:
        status, rows, _root = seoulbus.read(body)
    except ContractError as e:
        return malformed(SOURCE_SYSTEM, SCHEMA_VERSION, str(e))

    res = ParseResult(SOURCE_SYSTEM, SCHEMA_VERSION, status)
    res.stats.update(rows=len(rows), unexpected_flag_values=0)
    if not status.ok:
        return res

    for i, row in enumerate(rows):
        try:
            veh_id = require(row, "vehId")
            data_tm = require(row, "dataTm")
            event_time = parse_dt14(data_tm)
            plain = clean_str(row.get("plainNo"))
            payload = BusPosition(
                route_id=route_id,
                veh_id=veh_id,
                plain_no="".join(plain.split()) if plain else None,
                lon=to_float(row.get("gpsX")),
                lat=to_float(row.get("gpsY")),
                tm_x=to_float(row.get("posX")),
                tm_y=to_float(row.get("posY")),
                section_id=clean_str(row.get("sectionId")),
                sect_ord=to_int(row.get("sectOrd")),
                last_stop_id=clean_str(row.get("lastStnId")),
                next_stop_id=clean_str(row.get("nextStId")),
                turn_stop_id=clean_str(row.get("trnstnid")),
                last_st_tm=to_int(row.get("lastStTm")),
                next_st_tm=to_int(row.get("nextStTm")),
                sect_dist=to_float(row.get("sectDist")),
                full_sect_dist=to_float(row.get("fullSectDist")),
                rt_dist=to_float(row.get("rtDist")),
                bus_type=to_int(row.get("busType")),
                congestion=to_int(row.get("congetion")),
                stop_flag=to_int(row.get("stopFlag")),
                is_running=to_flag_lenient(row.get("isrunyn"), res.stats),
                is_last=to_flag_lenient(row.get("islastyn"), res.stats),
                is_full=to_flag_lenient(row.get("isFullFlag"), res.stats),
            )
            res.records.append(ctx.record(SOURCE_SYSTEM, SCHEMA_VERSION, event_time,
                                          dedup_key(veh_id, data_tm), payload))
        except ContractError as e:
            res.rejects.append(Reject(reason=str(e), raw=row, index=i))
    return res
