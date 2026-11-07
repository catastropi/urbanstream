"""공통 화면 부품 (streamlit). 판단 규칙은 freshness·shape 에 있고 여기서는 그리기만 한다."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import streamlit as st

from .client import ApiError, ApiResult
from .freshness import describe, explain_notes


def page_header(title: str, question: str, conclusion: str) -> None:
    st.title(title)
    st.caption(question)
    st.info(f"**이 화면에서 낼 수 있는 결론:** {conclusion}")


def freshness_line(name: str, res: Optional[ApiResult], now: datetime) -> None:
    """패널 하나의 기준 시각·경과·적재 시각과 '오래됨/갱신 실패' 표시."""
    if res is None:
        return
    f = describe(res.meta, now=now, fetched_at=res.fetched_at, error=res.error)
    text = f"**{name}** — {f.text}"
    if f.level == "stale" or res.error:
        st.warning(text)
    else:
        st.caption(text)
    for n in explain_notes(res.meta.get("notes")):
        st.caption(f"ℹ︎ {n}")
    if res.truncated:
        st.caption("ℹ︎ 결과가 더 있지만 화면 상한에서 잘렸습니다.")


def api_error(e: ApiError) -> None:
    """API 를 못 읽었을 때. 빈 화면 대신 이유와 확인 방법을 보여 준다(키·내부 정보는 담지 않는다)."""
    st.error(f"데이터를 불러오지 못했습니다: {e.short()}")
    if e.kind == "auth":
        st.caption(str(e))
    else:
        st.caption("잠시 뒤 '지금 새로고침' 을 누르거나, 개요 페이지에서 파이프라인 상태를 확인하세요.")


def empty(msg: str = "표시할 데이터가 없습니다.") -> None:
    st.caption(f"○ {msg}")
