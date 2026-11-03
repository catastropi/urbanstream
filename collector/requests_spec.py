"""소스별 호출 URL·파라미터 (순수 함수). 엔드포인트는 scripts/smoke_test.py 로 실제 호출에 성공한 것과 같다.

키는 인자로 받아 URL/파라미터에만 넣는다. 로그·audit 에 URL 을 쓰지 않는다 (키가 들어 있으므로).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote

SEOUL_OPENAPI = "http://openapi.seoul.go.kr:8088"
SEOUL_BUS_BASE = "http://ws.bus.go.kr/api/rest"
KMA_NCST = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

SEOUL_KEY_SOURCES = {"seoul_traffic", "seoul_incident"}  # SEOUL_API_KEY. 나머지는 DATA_GO_KR_KEY


@dataclass(frozen=True)
class RequestSpec:
    url: str
    params: Optional[dict]
    base: Optional[tuple[str, str]] = None   # 기상청 발표 시각 (YYYYMMDD, HHMM)


def kma_base(now: datetime, available_after_min: int = 45) -> tuple[str, str]:
    """조회할 초단기실황 발표 시각. 정시 + available_after_min 분이 지나야 그 시각 자료가 있다고 본다."""
    base = now.replace(minute=0, second=0, microsecond=0)
    if now.minute < available_after_min:
        base -= timedelta(hours=1)
    return base.strftime("%Y%m%d"), base.strftime("%H%M")


def build_request(source: str, params: dict, key: str, now: datetime, *,
                  kma_available_after_min: int = 45) -> RequestSpec:
    if source == "seoul_traffic":
        return RequestSpec(f"{SEOUL_OPENAPI}/{quote(key, safe='')}/xml/TrafficInfo/1/5/{params['link_id']}", None)
    if source == "seoul_incident":
        end = int(params.get("page_size", 200))
        return RequestSpec(f"{SEOUL_OPENAPI}/{quote(key, safe='')}/xml/AccInfo/1/{end}/", None)
    if source == "bus_position":
        return RequestSpec(f"{SEOUL_BUS_BASE}/buspos/getBusPosByRtid",
                           {"ServiceKey": key, "busRouteId": params["route_id"]})
    if source == "bus_arrival":
        return RequestSpec(f"{SEOUL_BUS_BASE}/arrive/getArrInfoByRouteAll",
                           {"ServiceKey": key, "busRouteId": params["route_id"]})
    if source == "kma_ncst":
        d, t = kma_base(now, kma_available_after_min)
        return RequestSpec(KMA_NCST, {"serviceKey": key, "pageNo": 1, "numOfRows": 100, "dataType": "JSON",
                                      "base_date": d, "base_time": t, "nx": params["nx"], "ny": params["ny"]},
                           base=(d, t))
    raise ValueError(f"알 수 없는 소스: {source}")
