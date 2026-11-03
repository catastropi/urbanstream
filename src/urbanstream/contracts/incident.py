"""incident_v1 — 서울 실시간 돌발 정보 (AccInfo).

- event_time = occr_date + occr_time (KST). occr_time 은 4자리(HHMM)와 6자리(HHMMSS)가 섞여 온다 (2026-11-01 관측).
- 돌발은 같은 acc_id 가 시간이 지나며 내용(해제 예정 시각, 설명)이 바뀔 수 있다.
  그래서 dedup_key = acc_id + 내용 해시. 내용이 바뀌면 새 레코드로 남기고(이력 보존),
  '현재 유효한 최신 상태'를 고르는 일은 Silver 에서 한다.
- 장기 공사(acc_type A04)도 같이 내려온다 (예: 2030년까지). 이것을 '진행 중 돌발'로 볼지는 분석 단계의 판단이다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from . import seoul
from .base import Body, CallContext, ParseResult, Reject, early_exit, malformed
from .common import (ContractError, clean_str, content_hash, parse_kst_date_time, require, to_float)

SOURCE_SYSTEM = "seoul_incident"
SCHEMA_VERSION = "incident_v1"


@dataclass(frozen=True)
class Incident:
    acc_id: str
    occurred_at: datetime                    # 발생 시각 (= envelope.event_time)
    expected_clear_at: Optional[datetime]    # 해제 예정 시각 (exp_clr_date/time). 시각이 비면 00:00:00
    acc_type: str                            # 돌발 유형 대분류 (관측: A01 사고, A04 공사, A10 행사/통제, A11 버스우회·기타)
    acc_detail_type: Optional[str]           # acc_dtype 세부 유형
    link_id: Optional[str]                   # 도로 링크 ID (도로소통의 link_id 와 같은 체계로 추정 — 조인 전 검증 필요)
    tm_x: Optional[float]                    # grs80tm_x. EPSG:5181 (버스 gps↔pos 13쌍 오차 ≤6 m, 돌발 10건은 설명 속 지명과 변환 좌표 일치 확인)
    tm_y: Optional[float]
    description: Optional[str]               # acc_info. CR/LF 는 \n 으로 통일
    road_code: Optional[str]                 # acc_road_code. 빈 값이 오면 None
    content_hash: str                        # 내용 변경 감지용 해시 (dedup_key 의 일부)


def dedup_key(acc_id: str, chash: str) -> str:
    return f"{acc_id}|{chash[:8]}"


def _expected_clear(row: dict) -> Optional[datetime]:
    d = clean_str(row.get("exp_clr_date"))
    if d is None:
        return None
    return parse_kst_date_time(d, clean_str(row.get("exp_clr_time")) or "000000")


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

    for i, row in enumerate(rows):
        try:
            acc_id = require(row, "acc_id")
            occurred_at = parse_kst_date_time(row.get("occr_date"), row.get("occr_time"))
            description = clean_str(row.get("acc_info"))
            chash = content_hash(row.get("exp_clr_date"), row.get("exp_clr_time"), row.get("acc_type"),
                                 row.get("acc_dtype"), row.get("link_id"), description)
            payload = Incident(
                acc_id=acc_id,
                occurred_at=occurred_at,
                expected_clear_at=_expected_clear(row),
                acc_type=require(row, "acc_type"),
                acc_detail_type=clean_str(row.get("acc_dtype")),
                link_id=clean_str(row.get("link_id")),
                tm_x=to_float(row.get("grs80tm_x")),
                tm_y=to_float(row.get("grs80tm_y")),
                description=description,
                road_code=clean_str(row.get("acc_road_code")),
                content_hash=chash,
            )
            res.records.append(ctx.record(SOURCE_SYSTEM, SCHEMA_VERSION, occurred_at,
                                          dedup_key(acc_id, chash), payload))
        except ContractError as e:
            res.rejects.append(Reject(reason=str(e), raw=row, index=i))
    return res
