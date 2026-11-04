"""서울 버스(ws.bus.go.kr) XML 응답 공통 처리.

관측한 정상 응답:
<ServiceResult><comMsgHeader/><msgHeader><headerCd>0</headerCd><headerMsg>정상적으로 처리되었습니다.</headerMsg>
<itemCount>0</itemCount></msgHeader><msgBody><itemList>...</itemList>...</msgBody></ServiceResult>

주의: msgHeader/itemCount 는 실제 건수와 무관하게 0 으로 온다 (13건/104건 응답에서 모두 0). 건수는 itemList 를 센다.
headerCd 가 0 이 아닌 경우: 4(결과 없음, 2026-11-04 관측)만 empty 로 분류하고, 그 밖의 코드는 아직 관측하지 못했다 → unknown 으로 두고 code/message 를 남긴다.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

from .base import ApiStatus, Body, element_rows, load_xml
from .common import ContractError, clean_str


def read(body: Body) -> tuple[ApiStatus, list[dict], ET.Element]:
    root = load_xml(body)
    header = root.find("msgHeader")
    if header is None:
        raise ContractError("msgHeader 요소가 없습니다")
    code = clean_str(header.findtext("headerCd"))
    message = clean_str(header.findtext("headerMsg"))
    rows = element_rows(root, "itemList")
    if code == "0":
        kind = "ok" if rows else "empty"
    elif code == "4" and not rows:
        # 2026-11-04 02:41 관측(운행 종료 시간대): HTTP 200, headerCd=4, headerMsg="결과가 없습니다." → 차량이 없는 정상 응답.
        # 이전에는 unknown 이라 재시도 3회 + 회로차단 실패로 처리됐다 (첫차 전 구간에서 호출 예산 낭비). 다른 비0 코드는 여전히 unknown.
        kind = "empty"
    else:
        kind = "malformed" if code is None else "unknown"
    return ApiStatus(kind=kind, code=code, message=message), rows, root
