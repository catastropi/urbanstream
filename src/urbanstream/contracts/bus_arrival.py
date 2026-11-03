"""bus_arrival_v1 — 서울 버스도착 (getArrInfoByRouteAll). 노선 1개 호출 → 정류소마다 1행.

응답은 정류소 1행에 '첫 번째 버스'와 '두 번째 버스'가 접미사 1/2 로 붙어 있다 (arrmsg1/arrmsg2, vehId1/vehId2 ...).
계약에서는 이것을 (정류소, 슬롯) 단위 레코드로 펼친다(unpivot). 이유: Silver 이후 모든 단계가 '버스 1대의 도착 예측'
단위로 일하고, 버스위치와 vehId 로 직접 조인되기 때문이다 (13/13 일치 확인).

- 빈 슬롯: vehId{n} 이 '0' 이거나 없으면 그 슬롯에는 버스가 없다. 레코드도 reject 도 만들지 않고 stats["empty_slots"]에 센다.
- event_time = mkTm (초 단위 — 서버가 약 20초마다 새 스냅샷을 만든다, ADR-0001 정정 2. 한 호출의 모든 행은 같은 값).
  dedup_key = 노선|정류소|정류소순번|슬롯|mkTm(초). 같은 스냅샷을 두 번 받으면 같은 키, 다른 스냅샷은 다른 키.
- 응답 컬럼은 88개다. v1 payload 는 의미를 확인했거나 분석(문제 C)에 바로 쓰는 필드만 담는다.
  넣지 않은 컬럼: 이 샘플에서 항상 0 인 10개(avgCf, kalCf, neuCf 등)와, 값은 변하지만 의미를 확인하지 못한
  약 45개(nmain*, nstn*, goal, nextBus, routeType, term 등), 값이 2021년에 멈춘 repTm1, 공백뿐인 dir.
  원본은 Bronze 에 그대로 남으므로, 의미를 확인하면 v2 에서 추가한다. (상수 0 이라는 판단은 샘플 1건 기준이라 단정하지 않는다.)
- 0 을 결측으로 바꾸지 않는다: traTime 0 은 '곧 도착'일 수 있어서 의미를 단정할 수 없다. 해석은 Silver 에서 arrival_msg 와 함께 한다.
- 정정(2026-11-02): isArrive{n} 은 0/1 플래그가 아니라 코드값이다 (실제 호출에서 '2' 관측 → 처음엔 슬롯 8개가 reject 됐다).
  그래서 arrive_code(int)로 두고, 부가 플래그(isLast)는 예상 밖 값이면 행을 버리지 않고 None + stats 로 센다.
- 시간 필드 의미(샘플 근거): traTime{n} 은 arrmsg 와 일치 (116 = '1분56초후', 207 = '3분27초후'). 따라서 도착까지 남은 초로 본다.
  exps/kals/neus 는 같은 값을 다른 방식으로 예측한 초 단위 값으로 보이며(112/93/117 등) 정확한 모델 이름은 미확인.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from . import seoulbus
from .base import Body, CallContext, ParseResult, Reject, early_exit, malformed
from .common import (ContractError, clean_str, parse_dt14, parse_mk_tm, require, second_key, to_flag_lenient, to_int)

SOURCE_SYSTEM = "bus_arrival"
SCHEMA_VERSION = "bus_arrival_v1"

_STOPS_AWAY = re.compile(r"\[(\d+)번째 전\]")


@dataclass(frozen=True)
class BusArrivalPrediction:
    route_id: str                       # busRouteId
    route_name: Optional[str]           # rtNm (예: '753')
    station_id: str                     # stId (버스위치의 nextStId/lastStnId 와 같은 ID 체계)
    station_ars_id: Optional[str]       # arsId
    station_name: Optional[str]         # stNm
    station_order: int                  # staOrd: 노선 안에서의 정류소 순번
    slot: int                           # 1 = 첫 번째 도착 버스, 2 = 두 번째
    veh_id: str                         # vehId{n}
    plain_no: Optional[str]             # plainNo{n}, 공백 제거
    arrival_msg: Optional[str]          # arrmsg{n} 예: '1분56초후[1번째 전]', '곧 도착', '출발대기'
    stops_away: Optional[int]           # 파생값: arrival_msg 의 '[N번째 전]'. 없으면 None
    arrive_in_sec: Optional[int]        # traTime{n}: 도착까지 남은 초
    pred_exps_sec: Optional[int]        # exps{n}
    pred_kals_sec: Optional[int]        # kals{n}
    pred_neus_sec: Optional[int]        # neus{n}
    speed: Optional[int]                # traSpd{n} (단위 미확인, 값 범위 14~26)
    arrive_code: Optional[int]          # isArrive{n}: 불리언이 아니라 코드값. 0 과 2 관측(2 = 노선 시작점 '출발대기' 행들, 2026-11-02). 의미 미확인
    is_last_bus: Optional[bool]         # isLast{n}. 예상 밖 값이면 None (stats[unexpected_flag_values])
    bus_type: Optional[int]             # busType{n}
    bus_station_name: Optional[str]     # stationNm{n}: 버스가 있는 정류소 이름으로 추정 (일부 행에만 존재, 스키마 진화 대상)
    first_bus_time: Optional[datetime]  # firstTm: 형식이 깨진 값(10자리)이 오면 None
    last_bus_time: Optional[datetime]   # lastTm: 위와 같음


def dedup_key(route_id: str, station_id: str, station_order: int, slot: int, mk_tm: datetime) -> str:
    return f"{route_id}|{station_id}|{station_order}|{slot}|{second_key(mk_tm)}"


def _tolerant_dt14(v) -> tuple[Optional[datetime], bool]:
    """firstTm/lastTm 은 간혹 10자리('2026110100')로 온다 (노선 첫 정류소 행, 관측). (값, 깨졌는지)."""
    s = clean_str(v)
    if s is None:
        return None, False
    try:
        return parse_dt14(s), False
    except ContractError:
        return None, True


def parse(body: Body, ctx: CallContext) -> ParseResult:
    pre = early_exit(SOURCE_SYSTEM, SCHEMA_VERSION, ctx, body)
    if pre is not None:
        return pre
    try:
        status, rows, _root = seoulbus.read(body)
    except ContractError as e:
        return malformed(SOURCE_SYSTEM, SCHEMA_VERSION, str(e))

    res = ParseResult(SOURCE_SYSTEM, SCHEMA_VERSION, status)
    res.stats.update(rows=len(rows), empty_slots=0, malformed_first_last_tm=0, unexpected_flag_values=0)
    if not status.ok:
        return res

    for i, row in enumerate(rows):
        try:
            route_id = require(row, "busRouteId")
            station_id = require(row, "stId")
            station_order = to_int(require(row, "staOrd"))
            mk_tm = parse_mk_tm(row.get("mkTm"))
            first_tm, bad1 = _tolerant_dt14(row.get("firstTm"))
            last_tm, bad2 = _tolerant_dt14(row.get("lastTm"))
        except ContractError as e:
            res.rejects.append(Reject(reason=str(e), raw=row, index=i))
            continue
        if bad1 or bad2:
            res.stats["malformed_first_last_tm"] += 1

        for slot in (1, 2):
            veh_id = clean_str(row.get(f"vehId{slot}"))
            if veh_id is None or veh_id == "0":
                res.stats["empty_slots"] += 1
                continue
            try:
                plain = clean_str(row.get(f"plainNo{slot}"))
                msg = clean_str(row.get(f"arrmsg{slot}"))
                m = _STOPS_AWAY.search(msg) if msg else None
                payload = BusArrivalPrediction(
                    route_id=route_id,
                    route_name=clean_str(row.get("rtNm")),
                    station_id=station_id,
                    station_ars_id=clean_str(row.get("arsId")),
                    station_name=clean_str(row.get("stNm")),
                    station_order=station_order,
                    slot=slot,
                    veh_id=veh_id,
                    plain_no="".join(plain.split()) if plain else None,
                    arrival_msg=msg,
                    stops_away=int(m.group(1)) if m else None,
                    arrive_in_sec=to_int(row.get(f"traTime{slot}")),
                    pred_exps_sec=to_int(row.get(f"exps{slot}")),
                    pred_kals_sec=to_int(row.get(f"kals{slot}")),
                    pred_neus_sec=to_int(row.get(f"neus{slot}")),
                    speed=to_int(row.get(f"traSpd{slot}")),
                    arrive_code=to_int(row.get(f"isArrive{slot}")),
                    is_last_bus=to_flag_lenient(row.get(f"isLast{slot}"), res.stats),
                    bus_type=to_int(row.get(f"busType{slot}")),
                    bus_station_name=clean_str(row.get(f"stationNm{slot}")),
                    first_bus_time=first_tm,
                    last_bus_time=last_tm,
                )
                res.records.append(ctx.record(SOURCE_SYSTEM, SCHEMA_VERSION, mk_tm,
                                              dedup_key(route_id, station_id, station_order, slot, mk_tm), payload))
            except ContractError as e:
                res.rejects.append(Reject(reason=f"slot{slot}: {e}", raw=row, index=i))
    return res
