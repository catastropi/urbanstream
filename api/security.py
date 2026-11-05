"""인증(X-API-Key) + 키별 속도 제한 (D9-5, D9-9). 인증 실패는 속도 제한을 거치지 않고 바로 401."""
from __future__ import annotations

import hashlib
import hmac

from fastapi import Request

from .errors import ApiError

HEADER = "X-API-Key"


def key_id(key: str) -> str:
    """로그·버킷용 짧은 지문. 키 자체는 어디에도 남기지 않는다."""
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def match_key(candidate: str, valid: tuple[str, ...]) -> str | None:
    """모든 키와 상수 시간 비교(조기 종료 없음)."""
    found = None
    cb = candidate.encode()
    for k in valid:
        if hmac.compare_digest(cb, k.encode()):
            found = k
    return found


def guard(request: Request) -> None:
    from .deps import services
    svc = services(request)
    given = request.headers.get(HEADER)
    if not given:
        raise ApiError(401, "missing_api_key", f"{HEADER} 헤더가 필요합니다", headers={"WWW-Authenticate": "ApiKey"})
    hit = match_key(given, svc.settings.api_keys)
    if hit is None:
        raise ApiError(401, "invalid_api_key", "API 키가 올바르지 않습니다", headers={"WWW-Authenticate": "ApiKey"})
    kid = key_id(hit)
    request.state.key_id = kid
    wait = svc.limiter.check(kid)
    if wait:
        raise ApiError(429, "rate_limited", f"요청이 너무 많습니다. {wait}초 뒤에 다시 시도하세요", headers={"Retry-After": str(wait)})
