"""HTTP 호출. 키가 로그·오류 메시지·audit 에 새지 않도록 모든 문자열을 마스킹한다."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol
from urllib.parse import quote, quote_plus, unquote

import requests


@dataclass
class FetchResult:
    http_status: Optional[int]        # 응답을 받지 못했으면 None
    body: bytes
    latency_ms: int
    error: Optional[str] = None       # 네트워크 예외 설명 (마스킹됨)
    error_kind: Optional[str] = None  # 'timeout' | 'network_error'


class Fetcher(Protocol):
    def get(self, url: str, params: Optional[dict]) -> FetchResult: ...


class Redactor:
    """키 값(원본, URL 인코딩된 형태들)을 ***KEY*** 로 바꾼다."""

    def __init__(self, secrets: Iterable[str]):
        variants: set[str] = set()
        for s in secrets:
            if s and len(s) >= 4:
                variants.update({s, quote(s, safe=""), quote_plus(s), quote(s, safe="/"), unquote(s)})
        self._variants = sorted((v for v in variants if len(v) >= 4), key=len, reverse=True)

    def __call__(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return text
        for v in self._variants:
            text = text.replace(v, "***KEY***")
        return text


def normalize_datago_key(raw: str) -> str:
    """공공데이터포털은 Encoding/Decoding 두 가지 키를 준다. requests 가 다시 인코딩하므로 Decoding 형태로 통일."""
    raw = raw.strip()
    return unquote(raw) if "%" in raw else raw


class HttpFetcher:
    """호출 1회의 총 제한시간(total_timeout)을 지킨다.

    requests 의 timeout=(connect, read) 는 '바이트 사이 간격'만 막아서, 조금씩 느리게 오는 응답은 총 시간이
    얼마든 길어질 수 있다 (2026-11-02 버스도착 26.2초). 그래서 본문을 조각으로 읽으면서 총 시간을 확인한다.
    한계: 조각 사이를 확인하므로 최악은 total + read 이다 (아무 바이트도 안 오는 정체는 read 가 막는다).

    Connection: close — 호출 간격이 60초 이상이라 연결을 재사용해 얻는 것이 없다. 60초 간격 측정에서 버스 서버만
    RemoteDisconnected 가 40회 중 4번 났고, 15~20초 간격에서는 55회 중 0번이었다 (서버가 놀고 있는 연결을 닫는다는
    가설, 표본이 작아 약한 증거). 매번 새 연결을 쓰면 이 가능성 자체가 사라진다. 되돌리려면 아래 헤더 한 줄을 지운다.
    """

    CHUNK = 16384

    def __init__(self, timeout: tuple[float, float], redact: Redactor, session: Optional[requests.Session] = None,
                 total_timeout: float = 15.0):
        self.timeout, self.redact, self.total_timeout = timeout, redact, total_timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "urbanstream-collector/0.1", "Connection": "close"})

    def _elapsed_ms(self, t0: float) -> int:
        return int((time.perf_counter() - t0) * 1000)

    def get(self, url: str, params: Optional[dict]) -> FetchResult:
        t0 = time.perf_counter()
        resp = None
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout, stream=True)
            chunks = []
            for chunk in resp.iter_content(chunk_size=self.CHUNK):
                chunks.append(chunk)
                if time.perf_counter() - t0 > self.total_timeout:
                    return FetchResult(None, b"", self._elapsed_ms(t0),
                                       f"TotalTimeout: 총 {self.total_timeout:g}초 제한 초과 (응답 일부만 수신)", "timeout")
            if time.perf_counter() - t0 > self.total_timeout:   # 헤더가 늦게 와서 본문이 한 조각이었던 경우도 같은 기준
                return FetchResult(None, b"", self._elapsed_ms(t0),
                                   f"TotalTimeout: 총 {self.total_timeout:g}초 제한 초과", "timeout")
            return FetchResult(resp.status_code, b"".join(chunks), self._elapsed_ms(t0))
        except requests.Timeout as e:
            return FetchResult(None, b"", self._elapsed_ms(t0), self.redact(f"{type(e).__name__}: {e}"), "timeout")
        except requests.RequestException as e:
            return FetchResult(None, b"", self._elapsed_ms(t0), self.redact(f"{type(e).__name__}: {e}"),
                               "network_error")
        finally:
            if resp is not None:
                resp.close()
