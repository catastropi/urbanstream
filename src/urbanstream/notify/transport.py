"""웹훅 전송 (Discord / Slack). 표준 라이브러리만 쓴다. 실패해도 예외를 던지지 않고 결과를 돌려준다 — 알림 때문에 DAG·감시가 죽으면 안 된다.

비밀: 웹훅 URL 은 .env 에만 있다. 오류 문장에 URL 이 섞여 나오므로(urllib 은 URL 을 오류에 담는다) 돌려주기 전에 반드시 가린다.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

MAX_TEXT = {"discord": 1900, "slack": 3000}
KINDS = tuple(MAX_TEXT)


@dataclass(frozen=True)
class SendResult:
    ok: bool
    status: Optional[int] = None
    error: str = ""


def build_payload(kind: str, text: str) -> bytes:
    if kind not in KINDS:
        raise ValueError(f"ALERT_WEBHOOK_KIND 는 {KINDS} 중 하나여야 합니다: {kind!r}")
    text = text[: MAX_TEXT[kind]]
    return json.dumps({"content": text} if kind == "discord" else {"text": text}, ensure_ascii=False).encode("utf-8")


def post_webhook(kind: str, url: str, text: str, *, timeout: float = 5.0, retries: int = 1,
                 opener: Optional[Callable[..., object]] = None, scrub: Callable[[str], str] = lambda s: s) -> SendResult:
    """JSON POST. 일시 오류(5xx, 429, 네트워크)는 retries 번 더 시도한다. 4xx(URL 오류 등)는 다시 해도 소용없다."""
    if not url:
        return SendResult(False, None, "웹훅 URL 이 설정되지 않았습니다")
    data = build_payload(kind, text)
    open_ = opener or urllib.request.urlopen
    last = SendResult(False, None, "시도하지 않음")
    for _ in range(retries + 1):
        req = urllib.request.Request(url, data=data, method="POST",
                                     headers={"Content-Type": "application/json", "User-Agent": "urbanstream-notify/1.0"})
        try:
            with open_(req, timeout=timeout) as resp:                       # type: ignore[operator]
                status = getattr(resp, "status", 200)
            if 200 <= status < 300:
                return SendResult(True, status)
            last = SendResult(False, status, f"HTTP {status}")
        except urllib.error.HTTPError as e:
            last = SendResult(False, e.code, scrub(f"HTTP {e.code}"))
            if e.code != 429 and e.code < 500:
                return last
        except Exception as e:                                              # noqa: BLE001 - 네트워크·시간 초과 모두 같은 처리
            last = SendResult(False, None, scrub(f"{type(e).__name__}: {e}"))
    return last


def config_from_env(env: Optional[Mapping[str, str]] = None) -> tuple[bool, str, str]:
    """(켜짐 여부, kind, url). ALERTS_ENABLED=false 이거나 URL 이 비어 있으면 전송하지 않고 기록만 한다."""
    e = os.environ if env is None else env
    enabled = str(e.get("ALERTS_ENABLED", "true")).strip().lower() not in ("0", "false", "no", "off")
    return enabled, (e.get("ALERT_WEBHOOK_KIND") or "discord").strip().lower(), (e.get("ALERT_WEBHOOK_URL") or "").strip()
