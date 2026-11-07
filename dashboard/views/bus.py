"""버스 — 문제 C: 이 버스가 평소보다 늦나? (시간표 대비 지연이 아니다 — ADR-0009 ③')"""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ..timefmt import floor_to, window
from ._common import BUS_PARAMS, QUANT_SEC


def load(client, now):
    f, t = window(floor_to(now, QUANT_SEC), days=7)
    r = client.bus_route_id
    return {"bus": client.get(C.BUS_POSITIONS, {**BUS_PARAMS, "route_id": r}),
            "drift": client.get(C.BUS_DRIFT, {"route_id": r, "from": f, "to": t, "limit": 500}),
            "section": client.get(C.BUS_SECTION, {"route_id": r, "from": f, "to": t, "limit": 200})}


def drift_chart(rows):
    df = pd.DataFrame(rows).melt(id_vars=["일시(KST)"], value_vars=["p50(초)", "p90(초)"], var_name="분위", value_name="초")
    return alt.Chart(df).mark_line(point=True, strokeWidth=2).encode(
        x=alt.X("일시(KST):N", title=None, sort=None), y=alt.Y("초:Q", title="예측 도착 시각 변동(초)"),
        color=alt.Color("분위:N", scale=alt.Scale(domain=["p50(초)", "p90(초)"], range=["#2a78d6", "#eb6834"])),
        tooltip=["일시(KST)", "분위", "초"])


def render(d, now):
    pts, nocoord = shape.bus_points(d["bus"].rows)
    st.metric("최근 15분 위치 관측 버스", len(pts), help="수집 창(04:00~) 밖에는 비어 있을 수 있다")
    ui.freshness_line("버스 위치", d["bus"], now)

    st.subheader("도착 예측 변동")
    drift = shape.drift_series(d["drift"].rows)
    if drift:
        stations = sorted({r["정류소"] for r in drift if r["정류소"]})
        pick = st.selectbox("정류소", stations) if stations else None
        sel = [r for r in drift if r["정류소"] == pick] if pick else drift
        if sel:
            st.altair_chart(drift_chart(sel), width="stretch")
        st.caption("같은 버스·정류소의 **예측 도착 시각**이 접근하는 동안 얼마나 밀렸는지(마지막 − 처음)입니다. "
                   "양수 = 뒤로 밀림. **시간표 대비 지연이 아닙니다**(시간표가 없음). 파랑 = 중앙값, 주황 = 90분위. 일시는 KST.")
        with st.expander("원자료 표"):
            st.dataframe(pd.DataFrame(sel), hide_index=True, width="stretch")
    else:
        ui.empty("최근 7일 도착 예측 변동 값이 없습니다.")
    ui.freshness_line("도착 예측 변동", d["drift"], now)

    st.subheader("구간 통과 시간")
    sec = shape.section_rows(d["section"].rows)
    if sec:
        st.dataframe(pd.DataFrame(sec), hide_index=True, width="stretch")
        st.caption("구간 통과 시간 ÷ 같은 시간대 평소 중앙값. 관측 주기 때문에 ±90초 오차가 있습니다.")
    else:
        ui.empty("최근 7일 구간 통과 값이 없습니다.")
    ui.freshness_line("구간 통과", d["section"], now)


def page():
    ui.page_header("버스", "이 노선 버스가 평소보다 늦게 도착하나?",
                   "도착 예측이 접근 중 얼마나 밀리는지, 구간을 지나는 데 평소보다 얼마나 걸리는지를 본다. 시간표 대비 지연은 시간표가 없어서 계산하지 않는다(노선 1개).")
    st.info(GOLD_LAG_NOTICE)
    try:
        render(load(runtime.client(), runtime.now()), runtime.now())
    except ApiError as e:
        ui.api_error(e)
