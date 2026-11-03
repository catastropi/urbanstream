"""UrbanStream 계약(contracts): API 응답 -> envelope + payload 레코드.

사용 예
    from urbanstream.contracts import CallContext, SOURCES
    ctx = CallContext(fetched_at=datetime.now(KST), http_status=200, latency_ms=66, raw_uri="...")
    result = SOURCES["bus_position"].parse(body_bytes, ctx, route_id="100100118")
    if result.ok:
        for rec in result.records:
            kafka_message = rec.to_dict()
    # result.rejects 는 dead-letter / DQ 로, result.status 는 파이프라인 감시(문제 F)로 보낸다.
"""
from .base import ApiStatus, CallContext, ParseResult, Reject
from .common import KST, ContractError
from .envelope import Envelope, Record
from .registry import SOURCES, SourceSpec, schema_of

__all__ = [
    "ApiStatus", "CallContext", "ParseResult", "Reject", "KST", "ContractError",
    "Envelope", "Record", "SOURCES", "SourceSpec", "schema_of",
]
