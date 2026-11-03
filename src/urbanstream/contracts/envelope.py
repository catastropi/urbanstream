"""모든 소스가 공통으로 쓰는 envelope.

Blueprint(contracts/README)의 필드 이름을 따르고, 파이프라인 자체 감시(문제 F)에 필요한
호출 단위 정보(ingest_id, http_status, latency_ms)를 더했다. 근거: docs/adr/0001-envelope-event-time-dedup.md

필드 의미
- event_id       : sha(source_system|dedup_key) 기반 결정적 UUID. 같은 사건은 몇 번 수집해도 같은 ID.
- source_system  : seoul_traffic / seoul_incident / bus_position / bus_arrival / kma_ncst
- schema_version : payload 스키마 이름과 버전 (예: traffic_speed_v1)
- event_time     : 사건이 일어난 시각 (소스가 알려준 시각). 소스에 없으면 규칙에 따라 정한다. (소스별 규칙: registry.py)
- fetched_at     : 우리가 API 응답을 받은 시각
- ingested_at    : 파이프라인이 저장소/Kafka 에 넣은 시각. 파서는 모르므로 None 이고, 수집기/Bronze 가 채운다.
- ingest_id      : API 호출 1회를 가리키는 ID. 한 호출에서 나온 레코드는 모두 같다. (lineage 용)
- raw_uri        : 원본 응답 위치. 형식은 Step 3(Collector)에서 확정한다.
- dedup_key      : 같은 사건을 판별하는 키 (source_system 안에서 유일)
- http_status    : 호출의 HTTP 상태
- latency_ms     : 호출 지연
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .common import ContractError, to_jsonable


@dataclass(frozen=True)
class Envelope:
    event_id: str
    source_system: str
    schema_version: str
    event_time: datetime
    fetched_at: datetime
    ingested_at: Optional[datetime]
    ingest_id: str
    raw_uri: Optional[str]
    dedup_key: str
    http_status: int
    latency_ms: Optional[int]

    def __post_init__(self) -> None:
        for name in ("event_time", "fetched_at"):
            v = getattr(self, name)
            if v.tzinfo is None:
                raise ContractError(f"{name} 은 timezone-aware 여야 합니다")
        if self.ingested_at is not None and self.ingested_at.tzinfo is None:
            raise ContractError("ingested_at 은 timezone-aware 여야 합니다")
        if not self.dedup_key:
            raise ContractError("dedup_key 가 비어 있습니다")


@dataclass(frozen=True)
class Record:
    """envelope + payload 한 쌍. Kafka 메시지 한 건에 해당한다."""

    envelope: Envelope
    payload: object

    def to_dict(self) -> dict:
        d = to_jsonable(self.envelope)
        d["payload"] = to_jsonable(self.payload)
        return d
