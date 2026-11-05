"""비밀 가리기. 로그·DB·웹훅 본문에 들어가기 전에 모든 문장이 거친다.

가리는 것: password/secret/token/api_key/authkey/servicekey = 값 조각, Discord·Slack 웹훅 URL, 긴 토큰이 든 URL 경로, .env 로 설정한 웹훅 URL 그대로.
"""
from __future__ import annotations

import os
import re

MAX_BODY = 1500
_SECRET = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key|authkey|servicekey)(\s*[=:]\s*)([^\s,;&'\"]+)")
_WEBHOOK = re.compile(r"https?://(?:[\w-]+\.)?discord(?:app)?\.com/api/(?:v\d+/)?webhooks/\S+|https?://hooks\.slack\.com/services/\S+", re.I)
_LONG_PATH_TOKEN = re.compile(r"(https?://[^\s/]+/)\S*[A-Za-z0-9_\-]{30,}\S*")


def mask(text: str) -> str:
    text = str(text)
    url = (os.environ.get("ALERT_WEBHOOK_URL") or "").strip()
    if url:
        text = text.replace(url, "***WEBHOOK***")
    text = _WEBHOOK.sub("***WEBHOOK***", text)
    text = _LONG_PATH_TOKEN.sub(lambda m: m.group(1) + "***", text)
    return _SECRET.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)[:MAX_BODY]
