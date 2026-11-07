"""환경 변수 → Settings. 키가 없거나 약하면 시작을 거부한다(안전한 쪽으로 실패, API 와 같은 규칙).

비밀(DASHBOARD_API_KEY)은 repr 에 나오지 않는다. 컨테이너는 .env 전체가 아니라 compose 가 넘긴 값만 받는다.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Mapping, Optional

MIN_KEY_LEN = 24  # API 쪽 MIN_KEY_LEN 과 같다 (api/config.py)

# 자료 종류별 캐시 수명(초). 근거는 ADR-0012 D10-4:
#  ops    운영 스냅샷 — monitor 가 60초마다 계산한다
#  gold   Gold 적재(매시 20분) 기준 "현재" 값 — API 자체 캐시도 최대 300초라 더 자주 불러도 달라지지 않는다
#  mart   일 단위 마트(영향·드리프트·날씨 효과·임계값)
#  static 링크 지오메트리 같은 참조 자료
#  none   캐시 안 함(내보내기 스크립트용)
TTL_SECONDS = {"ops": 60, "gold": 300, "mart": 900, "static": 3600, "none": 0}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    api_url: str
    api_key: str = field(repr=False)
    ttl_scale: float = 1.0               # 0 이면 캐시 끔(시험·점검용)
    timeout_sec: float = 8.0
    budget_per_min: int = 100            # 클라이언트 쪽 호출 상한 — 서버 한도(분당 120)보다 낮게
    max_entries: int = 64
    refresh_min_interval_sec: int = 30   # "지금 새로고침" 연타 방지
    bus_route_id: str = "100100118"      # 버스 API 는 route_id 가 필수(753번 = 100100118). Mac 실제 API 가 422 로 알려 줌 (D-176)


def _num(env: Mapping[str, str], name: str, default: float, lo: float, hi: float, cast=float):
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        v = cast(raw)
    except ValueError as e:
        raise ConfigError(f"{name} 는 숫자여야 합니다") from e
    if not lo <= v <= hi:
        raise ConfigError(f"{name} 는 {lo}~{hi} 사이여야 합니다")
    return v


def _route_id(raw: Optional[str]) -> str:
    v = (raw or "").strip() or "100100118"
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", v):
        raise ConfigError("DASHBOARD_BUS_ROUTE_ID 는 영문·숫자·-·_ 1~40자여야 합니다")
    return v


def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    e = os.environ if env is None else env
    url = (e.get("DASHBOARD_API_URL") or "http://api:8000").strip().rstrip("/")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ConfigError("DASHBOARD_API_URL 은 http:// 또는 https:// 로 시작해야 합니다")
    key = (e.get("DASHBOARD_API_KEY") or "").strip()
    if not key:
        raise ConfigError("DASHBOARD_API_KEY 가 비어 있습니다. scripts/stack.sh init-env 로 만든 뒤 다시 시작하세요 (키가 없으면 대시보드는 시작하지 않습니다)")
    if len(key) < MIN_KEY_LEN or any(c.isspace() for c in key) or "," in key:
        raise ConfigError(f"DASHBOARD_API_KEY 는 공백·쉼표 없이 {MIN_KEY_LEN}자 이상이어야 합니다")
    return Settings(
        api_url=url, api_key=key,
        ttl_scale=_num(e, "DASHBOARD_TTL_SCALE", 1.0, 0.0, 10.0),
        timeout_sec=_num(e, "DASHBOARD_TIMEOUT_SEC", 8.0, 1.0, 60.0),
        budget_per_min=int(_num(e, "DASHBOARD_BUDGET_PER_MIN", 100, 1, 1000, int)),
        max_entries=int(_num(e, "DASHBOARD_MAX_ENTRIES", 64, 8, 1024, int)),
        refresh_min_interval_sec=int(_num(e, "DASHBOARD_REFRESH_MIN_SEC", 30, 0, 3600, int)),
        bus_route_id=_route_id(e.get("DASHBOARD_BUS_ROUTE_ID")),
    )
