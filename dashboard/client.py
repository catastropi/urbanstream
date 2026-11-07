"""FastAPI 클라이언트 — 대시보드가 데이터를 얻는 유일한 길 (ADR-0012 D10-1, D10-4).

하는 일
- X-API-Key 헤더로 인증한다. 키는 로그·오류 문구·repr 어디에도 나오지 않는다.
- 자료 종류별 TTL 캐시(프로세스 안, 모든 브라우저 탭이 공유) → 보는 사람 수와 상관없이 API 호출 수가 일정하다.
- 같은 요청이 동시에 오면 한 번만 부른다(single-flight).
- 429 는 Retry-After(최대 5초)만큼 기다려 **1회만** 다시 시도한다. 그래도 안 되면 마지막 값을 '갱신 실패' 표시와 함께 돌려준다.
- 클라이언트 쪽 호출 예산(기본 분당 100 < 서버 120). 넘으면 부르지 않고 마지막 값(없으면 오류)을 쓴다.
- 인증 오류(401/403)와 우리 쪽 요청 오류(4xx)는 마지막 값으로 덮지 않는다 — 설정/버그가 보이도록.

streamlit 을 import 하지 않는다(순수 파이썬). 시험에서는 fetch 를 가짜로 바꿔 끼운다.
"""
from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

from .config import TTL_SECONDS, Settings
from .contract import Endpoint

MAX_RETRY_AFTER_SEC = 5
FALLBACK_KINDS = ("server", "network", "rate_limited", "budget", "bad_response")  # 마지막 값으로 대체해도 되는 오류


class ApiError(Exception):
    """kind: auth | client | rate_limited | server | network | bad_response | budget. 메시지에 키·입력값·SQL 이 없다."""

    def __init__(self, kind: str, message: str, *, status: Optional[int] = None, code: Optional[str] = None,
                 request_id: Optional[str] = None):
        super().__init__(message)
        self.kind, self.status, self.code, self.request_id = kind, status, code, request_id

    def short(self) -> str:
        bits = [self.kind] + ([self.code] if self.code else []) + ([f"HTTP {self.status}"] if self.status else [])
        return " / ".join(bits) + (f" (요청 {self.request_id})" if self.request_id else "")


@dataclass(frozen=True)
class FetchResponse:
    status: int
    headers: Mapping[str, str]   # 키는 소문자
    body: bytes


Fetch = Callable[[str, Mapping[str, str], Mapping[str, str], float], FetchResponse]


def requests_fetch(url: str, headers: Mapping[str, str], params: Mapping[str, str], timeout: float) -> FetchResponse:
    """기본 전송. requests 의 예외는 OSError 계열이라 클라이언트가 network 오류로 묶는다."""
    import requests  # 늦게 import: 시험·내보내기가 이 모듈만 읽어도 requests 없이 가짜 fetch 를 쓸 수 있다

    r = requests.get(url, headers=dict(headers), params=dict(params), timeout=timeout, allow_redirects=False)
    return FetchResponse(r.status_code, {k.lower(): v for k, v in r.headers.items()}, r.content)


@dataclass(frozen=True)
class ApiResult:
    data: Any
    meta: dict
    page: Optional[dict]
    fetched_at: datetime            # 이 값을 API 에서 받은 시각(UTC)
    from_cache: bool = False
    error: Optional[str] = None     # None 이 아니면 '갱신에 실패해 fetched_at 의 값을 보여 주는 중'
    truncated: bool = False         # get_all 이 max_pages 에서 멈췄고 더 있다

    @property
    def rows(self) -> list:
        return self.data if isinstance(self.data, list) else []


@dataclass
class _Entry:
    expires: float
    fetched_at: datetime
    payload: dict


class ApiClient:
    def __init__(self, settings: Settings, *, fetch: Fetch = requests_fetch,
                 clock: Callable[[], float] = time.monotonic,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 sleep: Callable[[float], None] = time.sleep, on_budget: str = "fail"):
        if on_budget not in ("fail", "wait"):
            raise ValueError("on_budget 은 fail 또는 wait")
        self._s, self._fetch, self._clock, self._now, self._sleep, self._on_budget = settings, fetch, clock, now, sleep, on_budget
        self._cache: "OrderedDict[tuple, _Entry]" = OrderedDict()
        self._lock = threading.Lock()
        self._key_locks: dict[tuple, threading.Lock] = {}
        self._calls: deque = deque()                 # 최근 60초의 HTTP 시도 시각(monotonic)
        self.call_log: deque = deque(maxlen=500)     # (경로, 상태) — 시험·사이드바 표시용. 키·매개변수 값은 담지 않는다
        self._last_refresh = -1e9

    @property
    def bus_route_id(self) -> str:
        """버스 엔드포인트는 route_id 가 필수다 — 화면·내보내기가 같은 값을 쓴다."""
        return self._s.bus_route_id

    def __repr__(self) -> str:
        return f"ApiClient(base={self._s.api_url!r}, entries={len(self._cache)})"

    # ---- 공개 -------------------------------------------------------------------------------------------------
    def get(self, ep: Endpoint, params: Optional[Mapping[str, Any]] = None) -> ApiResult:
        qs = self._query(ep, params)
        key = (ep.path, tuple(sorted(qs.items())))
        ttl = TTL_SECONDS[ep.ttl] * self._s.ttl_scale
        with self._key_lock(key):
            with self._lock:
                entry = self._cache.get(key)
            if entry is not None and ttl > 0 and entry.expires > self._clock():
                return self._result(entry.payload, entry.fetched_at, from_cache=True)
            try:
                payload = self._request(ep.path, qs)
            except ApiError as e:
                if entry is not None and e.kind in FALLBACK_KINDS:
                    return self._result(entry.payload, entry.fetched_at, from_cache=True, error=e.short())
                raise
            fetched = self._now()
            if ttl > 0:
                with self._lock:
                    self._cache[key] = _Entry(self._clock() + ttl, fetched, payload)
                    self._cache.move_to_end(key)
                    while len(self._cache) > self._s.max_entries:
                        self._cache.popitem(last=False)
            return self._result(payload, fetched)

    def get_all(self, ep: Endpoint, params: Optional[Mapping[str, Any]] = None, *, max_pages: int = 5) -> ApiResult:
        """키셋 커서를 따라가며 data 를 이어 붙인다. meta 는 첫 쪽 것, 더 있는데 max_pages 에서 멈추면 truncated."""
        base = dict(params or {})
        first = self.get(ep, base)
        rows, last, errors = list(first.rows), first, first.error
        pages = 1
        while last.page and last.page.get("has_more") and last.page.get("next_cursor") and pages < max_pages:
            last = self.get(ep, {**base, "cursor": last.page["next_cursor"]})
            rows.extend(last.rows)
            errors = errors or last.error
            pages += 1
        more = bool(last.page and last.page.get("has_more"))
        return ApiResult(rows, first.meta, last.page, first.fetched_at, first.from_cache and last.from_cache, errors, truncated=more)

    def refresh(self) -> bool:
        """캐시를 만료시킨다(값은 '갱신 실패' 대비로 남긴다). 연타 방지: 최소 간격 안에서는 False."""
        now = self._clock()
        with self._lock:
            if now - self._last_refresh < self._s.refresh_min_interval_sec:
                return False
            self._last_refresh = now
            for e in self._cache.values():
                e.expires = 0.0
        return True

    def calls_last_minute(self) -> int:
        with self._lock:
            self._purge(self._clock())
            return len(self._calls)

    # ---- 내부 -------------------------------------------------------------------------------------------------
    @staticmethod
    def _query(ep: Endpoint, params: Optional[Mapping[str, Any]]) -> dict:
        qs = {}
        for k, v in (params or {}).items():
            if k not in ep.params:
                raise ValueError(f"{ep.name}: 선언되지 않은 쿼리 매개변수 {k!r} (dashboard/contract.py 에 먼저 추가)")
            if v is None:
                continue
            qs[k] = ("true" if v else "false") if isinstance(v, bool) else str(v)
        return qs

    def _key_lock(self, key: tuple) -> threading.Lock:
        with self._lock:
            if len(self._key_locks) > 512:
                self._key_locks = {k: l for k, l in self._key_locks.items() if l.locked()}
            return self._key_locks.setdefault(key, threading.Lock())

    def _purge(self, now: float) -> None:
        while self._calls and now - self._calls[0] >= 60.0:
            self._calls.popleft()

    def _reserve_call(self) -> None:
        """호출 예산 확인 후 시도 1회를 기록한다."""
        while True:
            with self._lock:
                now = self._clock()
                self._purge(now)
                if len(self._calls) < self._s.budget_per_min:
                    self._calls.append(now)
                    return
                wait = 60.0 - (now - self._calls[0])
            if self._on_budget == "fail":
                raise ApiError("budget", "호출 예산(분당)을 넘어 요청하지 않았습니다")
            self._sleep(max(0.05, wait))

    def _once(self, path: str, qs: Mapping[str, str]) -> FetchResponse:
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("경로는 '/' 로 시작해야 합니다")
        self._reserve_call()
        try:
            resp = self._fetch(self._s.api_url + path, {"X-API-Key": self._s.api_key, "Accept": "application/json"},
                               qs, self._s.timeout_sec)
        except OSError:
            self.call_log.append((path, "network"))
            raise ApiError("network", "API 에 연결하지 못했습니다") from None
        self.call_log.append((path, resp.status))
        return resp

    def _request(self, path: str, qs: Mapping[str, str]) -> dict:
        resp = self._once(path, qs)
        if resp.status == 429:
            wait = _retry_after(resp)
            if wait is None or wait > MAX_RETRY_AFTER_SEC:
                raise self._error(resp)
            self._sleep(wait)
            resp = self._once(path, qs)              # 재시도는 1회뿐
        if resp.status != 200:
            raise self._error(resp)
        try:
            payload = json.loads(resp.body)
        except ValueError:
            raise ApiError("bad_response", "API 응답이 JSON 이 아닙니다", status=resp.status) from None
        if not isinstance(payload, dict) or "data" not in payload or not isinstance(payload.get("meta"), dict):
            raise ApiError("bad_response", "API 응답에 data/meta 가 없습니다", status=resp.status)
        return payload

    @staticmethod
    def _error(resp: FetchResponse) -> ApiError:
        code = rid = None
        try:
            body = json.loads(resp.body)
            if isinstance(body, dict):
                code = body.get("code") if isinstance(body.get("code"), str) else None
                rid = body.get("request_id") if isinstance(body.get("request_id"), str) else None
        except ValueError:
            pass
        s = resp.status
        kind = "auth" if s in (401, 403) else "rate_limited" if s == 429 else "server" if s >= 500 else "client"
        msg = {"auth": "API 키가 거부되었습니다(DASHBOARD_API_KEY 가 API_KEYS 에 있는지 확인)",
               "rate_limited": "API 호출 한도에 걸렸습니다", "server": "API 서버 오류입니다",
               "client": "API 가 요청을 거부했습니다"}[kind]
        return ApiError(kind, msg, status=s, code=code, request_id=rid)

    @staticmethod
    def _result(payload: dict, fetched_at: datetime, *, from_cache: bool = False, error: Optional[str] = None) -> ApiResult:
        return ApiResult(payload.get("data"), payload.get("meta") or {}, payload.get("page"), fetched_at, from_cache, error)


def _retry_after(resp: FetchResponse) -> Optional[float]:
    raw = resp.headers.get("retry-after")
    try:
        v = float(raw) if raw is not None else None
    except ValueError:
        return None
    return None if v is None or v < 0 else v
