"""streamlit 쪽 접착제: 클라이언트 싱글톤(모든 세션 공유), 현재 시각. 시험에서는 set_client_for_tests 로 가짜를 끼운다."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import streamlit as st

from .client import ApiClient
from .config import load_settings

_override: Optional[ApiClient] = None


def set_client_for_tests(c: Optional[ApiClient]) -> None:
    global _override
    _override = c


@st.cache_resource(show_spinner=False)
def _build() -> ApiClient:
    return ApiClient(load_settings())


def client() -> ApiClient:
    return _override if _override is not None else _build()


def now() -> datetime:
    return datetime.now(timezone.utc)
