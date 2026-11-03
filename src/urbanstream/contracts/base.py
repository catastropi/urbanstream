"""파서 공통 구조: 호출 맥락, API 상태, 파싱 결과, XML/게이트웨이 오류 처리.

설계 원칙 (원 요청서 25장)
- 외부 API 는 장애가 난다고 전제한다: HTTP 오류, 인증 오류, 게이트웨이 오류, 깨진 본문을 모두 ApiStatus 로 표현한다.
- 데이터는 틀릴 수 있다고 전제한다: 잘못된 행은 예외로 전체를 죽이지 않고 Reject 로 모은다. (DQ/dead-letter 의 입력)
- 확인 안 된 코드는 추측해서 분류하지 않는다: 관측하지 못한 코드는 kind="unknown" 으로 두고 code/message 를 그대로 남긴다.
"""
from __future__ import annotations

import json
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Union

from .common import ContractError, clean_str, make_event_id, to_int
from .envelope import Envelope, Record

Body = Union[bytes, str]

# ApiStatus.kind 값
#   ok            정상
#   empty         정상이지만 데이터 0건 (ok 로 취급)
#   auth          인증 실패 (키 오류, HTTP 401/403)
#   param         요청 인자 오류
#   no_service    게이트웨이가 서비스를 찾지 못함 (잘못된 경로/미승인)
#   rate_limited  호출 제한 (HTTP 429)
#   server_error  서버 오류 (HTTP 5xx)
#   http_error    그 밖의 HTTP 오류
#   malformed     본문을 해석할 수 없음
#   unknown       처음 보는 코드 (관측 후 분류를 추가한다)
OK_KINDS = frozenset({"ok", "empty"})


@dataclass(frozen=True)
class ApiStatus:
    kind: str
    code: Optional[str] = None
    message: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.kind in OK_KINDS


@dataclass(frozen=True)
class Reject:
    """계약을 통과하지 못한 행. 원본을 함께 남겨 나중에 재처리/조사할 수 있게 한다."""

    reason: str
    raw: Any = None
    index: Optional[int] = None


@dataclass
class ParseResult:
    source_system: str
    schema_version: str
    status: ApiStatus
    records: list[Record] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)
    total_count: Optional[int] = None  # API 가 알려준 전체 건수 (없으면 None)
    stats: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status.ok


@dataclass
class CallContext:
    """API 호출 1회의 맥락. 파서는 이것으로 envelope 를 채운다."""

    fetched_at: datetime
    http_status: int = 200
    latency_ms: Optional[int] = None
    raw_uri: Optional[str] = None
    ingest_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        if self.fetched_at.tzinfo is None:
            raise ContractError("fetched_at 은 timezone-aware 여야 합니다")

    def record(self, source_system: str, schema_version: str, event_time: datetime,
               dedup_key: str, payload: object) -> Record:
        env = Envelope(
            event_id=make_event_id(source_system, dedup_key),
            source_system=source_system,
            schema_version=schema_version,
            event_time=event_time,
            fetched_at=self.fetched_at,
            ingested_at=None,
            ingest_id=self.ingest_id,
            raw_uri=self.raw_uri,
            dedup_key=dedup_key,
            http_status=self.http_status,
            latency_ms=self.latency_ms,
        )
        return Record(envelope=env, payload=payload)


# --------------------------------------------------------------------------- 본문/XML
def to_text(body: Body) -> str:
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace").lstrip("﻿")
    return body.lstrip("﻿")


def load_xml(body: Body) -> ET.Element:
    """XML 본문 -> 루트 요소. 서울 API 는 XML 선언이 없거나 루트가 <RESULT> 인 응답도 준다 (관측)."""
    raw = body if isinstance(body, bytes) else body.encode("utf-8")
    raw = raw.lstrip(b"\xef\xbb\xbf \r\n\t")
    try:
        return ET.fromstring(raw)
    except ET.ParseError as e:
        raise ContractError(f"XML 해석 실패: {e}") from e


def element_rows(root: ET.Element, tag: str) -> list[dict]:
    """root 아래의 모든 <tag> 요소를 {자식태그: 텍스트} 사전으로."""
    return [{c.tag: c.text for c in el} for el in root.iter(tag)]


# --------------------------------------------------------------------------- 상태 판정
def _message_from_body(body: Body) -> Optional[str]:
    text = to_text(body).strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            return text[:200]
        if isinstance(data, dict):
            for key in ("message", "errMsg", "resultMsg", "error"):
                if isinstance(data.get(key), str):
                    return data[key]
    return text[:200]


def status_from_http(http_status: int, body: Body) -> Optional[ApiStatus]:
    """2xx 가 아니면 ApiStatus, 2xx 면 None(본문을 계속 해석)."""
    if 200 <= http_status < 300:
        return None
    msg = _message_from_body(body)
    if http_status in (401, 403):
        kind = "auth"
    elif http_status == 429:
        kind = "rate_limited"
    elif http_status >= 500:
        kind = "server_error"
    else:
        kind = "http_error"
    return ApiStatus(kind=kind, code=str(http_status), message=msg)


def gateway_status(body: Body) -> Optional[ApiStatus]:
    """공공데이터포털 게이트웨이 오류 본문을 인식한다. 인식하지 못하면 None.

    관측한 형태
    - JSON {"OpenAPI_ServiceResponse": {"cmmMsgHeader": {"errMsg", "returnAuthMsg", "returnReasonCode": "12"}}}
      (returnReasonCode 12 = NO_OPENAPI_SERVICE_ERROR, 잘못된 경로)
    - JSON {"error": "Unauthorized", "message": "...", "status": 401}  (등록되지 않은 서비스키)
    XML 로 오는 같은 오류(<OpenAPI_ServiceResponse>)도 같은 방식으로 읽는다. (XML 형태는 아직 미관측)
    """
    text = to_text(body).strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            return None
        if not isinstance(data, dict):
            return None
        wrapper = data.get("OpenAPI_ServiceResponse")
        if isinstance(wrapper, dict):
            hdr = wrapper.get("cmmMsgHeader") or {}
            return _gateway_from_header(hdr.get("returnReasonCode"), hdr.get("errMsg"), hdr.get("returnAuthMsg"))
        st = data.get("status")
        if isinstance(st, int) and st >= 400:
            return status_from_http(st, body)
        return None
    if text.startswith("<"):
        try:
            root = load_xml(text)
        except ContractError:
            return None
        if root.tag == "OpenAPI_ServiceResponse":
            hdr = root.find("cmmMsgHeader")
            if hdr is None:
                return None
            return _gateway_from_header(hdr.findtext("returnReasonCode"), hdr.findtext("errMsg"),
                                        hdr.findtext("returnAuthMsg"))
    return None


def _gateway_from_header(code: Any, err_msg: Any, auth_msg: Any) -> ApiStatus:
    code_s = clean_str(code)
    msg = " / ".join(x for x in (clean_str(err_msg), clean_str(auth_msg)) if x) or None
    kind = "no_service" if code_s == "12" else "unknown"
    return ApiStatus(kind=kind, code=code_s, message=msg)


def early_exit(source_system: str, schema_version: str, ctx: CallContext, body: Body) -> Optional[ParseResult]:
    """HTTP 오류/게이트웨이 오류면 레코드 없는 ParseResult 를 돌려준다. 정상 후보면 None."""
    st = status_from_http(ctx.http_status, body)
    if st is None:
        st = gateway_status(body)
    if st is None:
        return None
    return ParseResult(source_system=source_system, schema_version=schema_version, status=st)


def malformed(source_system: str, schema_version: str, message: str) -> ParseResult:
    return ParseResult(source_system=source_system, schema_version=schema_version,
                       status=ApiStatus(kind="malformed", message=message))


def total_count_of(root: ET.Element) -> Optional[int]:
    try:
        return to_int(root.findtext("list_total_count"))
    except ContractError:
        return None
