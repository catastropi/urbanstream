"""알림 (Step 7 스텁 → Step 8 에서 웹훅/이메일로 확장).

지금은 로그로만 남긴다. **호출 모양(`send`)은 Step 7 에서 고정**했다 — Step 8 은 이 함수의 내부만 바꾸고 호출하는 쪽(DAG)은
고치지 않는다. 비밀(비밀번호·키·토큰)이 본문에 섞여 들어오면 가린다.
"""
from __future__ import annotations

import logging
import re

_LOG = logging.getLogger("urbanstream.notify")
LEVELS = {"info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR, "critical": logging.CRITICAL}
_MAX_BODY = 1500
_SECRET = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key|authkey|servicekey)(\s*[=:]\s*)([^\s,;&'\"]+)")


def mask(text: str) -> str:
    """`password=abc` 같은 조각의 값을 가린다. 길이도 제한한다."""
    return _SECRET.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)[:_MAX_BODY]


def send(level: str, title: str, body: str = "", *, dedup_key: str | None = None) -> bool:
    """알림 1건. 반환값 True = 전달 경로(지금은 로그)에 기록됨.

    dedup_key: 같은 문제가 반복될 때 합치기 위한 키 (Step 8 에서 빈도 제한에 쓴다. 지금은 로그에 붙이기만 한다).
    """
    if level not in LEVELS:
        raise ValueError(f"level 은 {sorted(LEVELS)} 중 하나여야 합니다: {level!r}")
    _LOG.log(LEVELS[level], "[notify:%s] %s | %s | dedup=%s", level, mask(title), mask(body), dedup_key)
    return True
