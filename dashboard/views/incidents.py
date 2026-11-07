"""돌발 영향 — 문제 B: 사고·공사가 주변 도로 속도에 얼마나 영향을 줬나."""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ..timefmt import floor_to, window
from ._common import ACTIVE_PARAMS, QUANT_SEC


def load(client, now):
    f, t = window(floor_to(now, QUANT_SEC), days=7)
    return {"active": client.get(C.INCIDENTS_ACTIVE, ACTIVE_PARAMS),
            "by_type": client.get(C.IMPACT_BY_TYPE),
            "episodes": client.get_all(C.INCIDENTS, {"from": f, "to": t, "limit": 100}, max_pages=2)}


def render(d, now):
    st.metric("진행 중 돌발", shape.fmt_int(len(d["active"].rows)))
    ui.freshness_line("진행 중 돌발", d["active"], now)

    st.subheader("유형별 영향(링크에 연결된 에피소드만)")
    rows = d["by_type"].rows
    usable = [r for r in rows if r.get("delta_p50") is not None and (r.get("n_sufficient") or 0) > 0]
    if usable:
        df = pd.DataFrame([{"유형": shape.acc_type_text(r["acc_type"]), "속도 변화 중앙값(km/h)": r["delta_p50"], "충분한 표본": r["n_sufficient"]} for r in usable])
        ch = alt.Chart(df).mark_bar(color="#2a78d6", cornerRadiusEnd=4).encode(
            x=alt.X("유형:N", sort="-y", title=None), y=alt.Y("속도 변화 중앙값(km/h):Q", scale=alt.Scale(zero=True)),
            tooltip=["유형", "속도 변화 중앙값(km/h)", "충분한 표본"])
        st.altair_chart(ch, width="stretch")
        st.caption("값이 음수면 돌발 중 속도가 사고 전보다 낮았다는 뜻입니다. 막대 위 표본 수가 작으면 읽지 마세요.")
    else:
        ui.empty("영향을 읽을 만큼 표본이 충분한 유형이 아직 없습니다(수집 링크 4개라 연결되는 돌발이 적습니다).")
    if rows:
        st.dataframe(pd.DataFrame(shape.impact_type_rows(rows)), hide_index=True, width="stretch")
    ui.freshness_line("유형별 영향", d["by_type"], now)

    st.subheader("최근 7일 에피소드")
    ep = shape.incident_rows(d["episodes"].rows)
    if ep:
        st.dataframe(pd.DataFrame(ep), hide_index=True, width="stretch")
    else:
        ui.empty("최근 7일 에피소드가 없습니다.")
    ui.freshness_line("에피소드", d["episodes"], now)


def page():
    ui.page_header("돌발 영향", "사고·공사가 주변 도로 속도에 얼마나 영향을 줬나?",
                   "돌발 유형별로, 연결된 수집 링크의 속도가 돌발 전보다 얼마나 달라졌는지 중앙값으로 본다. '충분한 표본' 이 0 이면 결론을 내릴 수 없다.")
    st.info(GOLD_LAG_NOTICE)
    try:
        render(load(runtime.client(), runtime.now()), runtime.now())
    except ApiError as e:
        ui.api_error(e)
