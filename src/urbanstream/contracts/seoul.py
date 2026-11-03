"""서울 열린데이터광장(openapi.seoul.go.kr) XML 응답 공통 처리.

관측한 응답 형태 (2026-10-28, 2026-11-01)
- 정상      : <TrafficInfo>|<AccInfo> 아래에 <list_total_count>, <RESULT><CODE>INFO-000</CODE>..., <row>...
- 인증 오류 : XML 선언도 루트도 없이 <RESULT><CODE>INFO-100</CODE><MESSAGE><![CDATA[...]]></MESSAGE></RESULT>
- 인자 오류 : <response><RESULT><CODE>ERROR-301</CODE>...</RESULT></response>
→ 루트 모양에 기대지 않고 문서 어디에 있든 <RESULT> 를 찾는다.
"""
from __future__ import annotations

from typing import Optional

from .base import ApiStatus, Body, element_rows, load_xml, total_count_of
from .common import ContractError, clean_str

# 관측한 코드만 분류한다. 그 밖의 코드는 unknown 으로 두고 code/message 를 그대로 남긴다.
_OBSERVED = {
    "INFO-000": "ok",
    "INFO-100": "auth",    # 인증키가 유효하지 않습니다 (2026-10-28 관측)
    "ERROR-301": "param",  # 파일타입(TYPE) 값 누락/유효하지 않음 (2026-10-28 관측)
}
# 문서상 '데이터 없음' 코드로 알려져 있으나 이 프로젝트에서는 아직 관측하지 못했다.
# 실제로 관측되면 api-catalog 를 갱신하고 이 주석을 지운다.
_DOCUMENTED_UNOBSERVED = {"INFO-200": "empty"}


def classify(code: Optional[str]) -> str:
    if code is None:
        return "malformed"
    return _OBSERVED.get(code) or _DOCUMENTED_UNOBSERVED.get(code) or "unknown"


def read(body: Body, row_tag: str = "row") -> tuple[ApiStatus, list[dict], Optional[int]]:
    """본문 -> (상태, 행 사전 목록, API 가 알려준 전체 건수).

    XML 자체가 깨졌으면 ContractError 를 그대로 던진다 (호출자가 malformed 로 바꾼다).
    """
    root = load_xml(body)
    result = next(root.iter("RESULT"), None)
    if result is None:
        raise ContractError("RESULT 요소가 없습니다")
    code = clean_str(result.findtext("CODE"))
    message = clean_str(result.findtext("MESSAGE"))
    kind = classify(code)
    rows = element_rows(root, row_tag)
    if kind == "ok" and not rows:
        kind = "empty"
    return ApiStatus(kind=kind, code=code, message=message), rows, total_count_of(root)
