"""소스 목록: 이름, 스키마 버전, payload 클래스, 파서, 그리고 event_time / dedup_key 규칙(사람이 읽는 설명).

규칙 설명 문자열은 api-catalog 문서와 같은 내용이어야 한다 (tests/contract/test_catalog_sync.py 가 일부를 검사한다).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from . import bus_arrival, bus_position, incident, traffic, weather
from .base import ParseResult
from .envelope import Envelope
from .schema import describe


@dataclass(frozen=True)
class SourceSpec:
    source_system: str
    schema_version: str
    payload_cls: type
    parse: Callable[..., ParseResult]
    event_time_rule: str
    dedup_rule: str


SOURCES: dict[str, SourceSpec] = {
    s.source_system: s
    for s in (
        SourceSpec(traffic.SOURCE_SYSTEM, traffic.SCHEMA_VERSION, traffic.TrafficSpeed, traffic.parse,
                   "수집 시각(fetched_at)을 분 단위로 절삭 — 소스가 시각을 주지 않음",
                   "link_id | YYYYMMDDHHMM"),
        SourceSpec(incident.SOURCE_SYSTEM, incident.SCHEMA_VERSION, incident.Incident, incident.parse,
                   "occr_date + occr_time (KST, HHMM 또는 HHMMSS)",
                   "acc_id | 내용해시 앞 8자 (해제시각·설명 등이 바뀌면 새 키)"),
        SourceSpec(bus_position.SOURCE_SYSTEM, bus_position.SCHEMA_VERSION, bus_position.BusPosition,
                   bus_position.parse,
                   "dataTm (차량별 GPS 보고 시각, KST)",
                   "vehId | dataTm"),
        SourceSpec(bus_arrival.SOURCE_SYSTEM, bus_arrival.SCHEMA_VERSION, bus_arrival.BusArrivalPrediction,
                   bus_arrival.parse,
                   "mkTm (초 단위, KST)",
                   "busRouteId | stId | staOrd | slot | YYYYMMDDHHMMSS"),
        SourceSpec(weather.SOURCE_SYSTEM, weather.SCHEMA_VERSION, weather.WeatherObservation, weather.parse,
                   "baseDate + baseTime (발표 시각, KST)",
                   "nx | ny | category | YYYYMMDDHHMM"),
    )
}


def schema_of(source_system: str) -> dict:
    """envelope + payload 스키마 (스냅샷 비교/문서/Spark 스키마 생성의 단일 출처)."""
    spec = SOURCES[source_system]
    return {
        "source_system": spec.source_system,
        "schema_version": spec.schema_version,
        "envelope": describe(Envelope),
        "payload": describe(spec.payload_cls),
    }
