"""요청 간 공유 객체와 응답 조립 도우미."""
from __future__ import annotations

import time
from urllib.parse import urlencode
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fastapi import Request, Response

from .cache import TTLCache
from .config import Settings
from .db import Database
from .ratelimit import RateLimiter
from .schemas import Meta

CACHE_MAX_ROWS = 300               # 이보다 행이 많은 응답은 캐시하지 않는다 (RSS 보호, ADR-0011 D9-9)
OPS_SNAPSHOT_STALE_SEC = 300       # 운영 스냅샷(source/layer status)은 감시 루프가 1분마다 쓴다 → 5분 넘으면 stale


class Services:
    def __init__(self, settings: Settings, db: Database, clock: Optional[Callable[[], datetime]] = None,
                 mono: Callable[[], float] = time.monotonic):
        self.settings = settings
        self.db = db
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.cache = TTLCache(clock=mono)
        self.limiter = RateLimiter(settings.rate_per_min, settings.rate_burst, clock=mono)

    def now(self) -> datetime:
        return self._clock()

    def cached(self, key: str, ttl: float, build: Callable[[], Any], store_if: Callable[[Any], bool] = lambda v: True) -> Any:
        return self.cache.get_or_build(key, ttl * self.settings.cache_ttl_scale, build, store_if)

    def loaded_at(self) -> Optional[datetime]:
        """Gold 적재가 마지막으로 성공한 시각(ops.v_pipeline_health 의 load 행)."""
        def build():
            r = self.db.row("SELECT last_ok_at FROM ops.v_pipeline_health WHERE component_kind = 'load' AND component = 'gold_load'")
            return {"v": r["last_ok_at"] if r else None}
        return self.cached("loaded_at", 30, build)["v"]

    def meta(self, as_of: Optional[datetime], *, stale_after_sec: Optional[float] = -1, total: Optional[int] = None,
             notes: Optional[list[str]] = None, empty: bool = False, with_loaded_at: bool = True) -> Meta:
        """stale_after_sec: -1 = 설정 기본값, None = 기준 없음(stale=null), 숫자 = 그 기준."""
        limit = self.settings.stale_after_sec if stale_after_sec == -1 else stale_after_sec
        age = None if as_of is None else max(0.0, (self.now() - as_of).total_seconds())
        stale = None if (age is None or limit is None) else age > limit
        n = list(notes or [])
        if empty and "empty_result" not in n:
            n.append("empty_result")
        return Meta(as_of=as_of, age_sec=None if age is None else round(age, 1), stale=stale,
                    loaded_at=self.loaded_at() if with_loaded_at else None, total=total, notes=n)


def services(request: Request) -> Services:
    return request.app.state.services


def set_cache_header(response: Response, ttl: float, scale: float = 1.0) -> None:
    t = int(ttl * scale)
    response.headers["Cache-Control"] = f"private, max-age={t}" if t > 0 else "no-store"


def _small_enough(envelope: Any) -> bool:
    data = getattr(envelope, "data", None)
    return not isinstance(data, list) or len(data) <= CACHE_MAX_ROWS


def respond(request: Request, response: Response, ttl: float, build: Callable[[], Any]) -> Any:
    """캐시(경로+정렬된 쿼리) + Cache-Control. 오류는 캐시하지 않는다. 키(인증)는 캐시 키에 넣지 않는다 — 데이터가 키별로 다르지 않다."""
    svc = services(request)
    key = request.url.path + "?" + urlencode(sorted(request.query_params.multi_items()))
    out = svc.cached(key, ttl, build, store_if=_small_enough)
    set_cache_header(response, ttl, svc.settings.cache_ttl_scale)
    return out
