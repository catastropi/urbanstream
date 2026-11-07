"""혼잡 — 문제 A: 지금 속도가 같은 요일 구분·시간대의 평소와 얼마나 다른가."""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ..timefmt import floor_to, window
from ._common import CURRENT_PARAMS, LINKS_PARAMS, QUANT_SEC

SPEED_BLUE, BAND_BLUE, MEDIAN_GRAY = "#2a78d6", "#9ec5f4", "#52514e"


def load_base(client, now):
    return {"links": client.get(C.LINKS, LINKS_PARAMS), "current": client.get(C.TRAFFIC_CURRENT, CURRENT_PARAMS)}


def load_link(client, now, link_id):
    """선택한 링크의 최근 24시간 속도와 기준선. 기간은 QUANT_SEC 로 내려서 캐시 키가 안정되게 한다."""
    f, t = window(floor_to(now, QUANT_SEC), hours=24)
    return {"speeds": client.get_all(C.TRAFFIC_SPEEDS, {"link_id": link_id, "from": f, "to": t, "limit": 1000}, max_pages=3),
            "thresholds": client.get(C.TRAFFIC_THRESHOLDS, {"link_id": link_id})}


def chart(rows, has_band):
    df = pd.DataFrame(rows)
    x = alt.X("t:T", title="시각(KST)")
    line = alt.Chart(df).mark_line(color=SPEED_BLUE, strokeWidth=2).encode(
        x=x, y=alt.Y("speed:Q", title="속도(km/h)", scale=alt.Scale(zero=True)),
        tooltip=[alt.Tooltip("t:T", title="KST", format="%m-%d %H:%M"), alt.Tooltip("speed:Q", title="속도", format=".1f")])
    if not has_band:
        return line
    band = alt.Chart(df).mark_area(color=BAND_BLUE, opacity=0.45, interpolate="step-after").encode(x=x, y="p10:Q", y2="p90:Q")
    med = alt.Chart(df).mark_line(color=MEDIAN_GRAY, strokeDash=[4, 3], strokeWidth=1.5, interpolate="step-after").encode(x=x, y="p50:Q")
    return band + med + line


def render_base(d, now):
    counts = shape.level_counts(d["current"].rows)
    st.write(" · ".join(f"{shape.level_text(k)} {counts[k]}" for k in shape.LEVEL_ORDER) + f" · {shape.level_text(None)} {counts['unknown_level']}")
    rows = shape.traffic_rows(d["current"].rows)
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        ui.empty("관측된 링크가 없습니다.")
    ui.freshness_line("현재 속도", d["current"], now)


def render_link(link_id, d, now):
    series = shape.speed_series(d["speeds"].rows)
    if not series:
        ui.empty("최근 24시간 속도 관측이 없습니다.")
    else:
        series, has_band = shape.attach_baseline(series, d["thresholds"].rows)
        st.altair_chart(chart(series, has_band), width="stretch")
        st.caption("파란 실선 = 관측 속도 · 회색 점선 = 같은 요일 구분·시간대의 평소 중앙값 · 연한 파란 띠 = 평소 10~90% 범위. "
                   "요일 구분은 평일/토/일이며 **공휴일은 평일로 근사**합니다.")
        if not has_band:
            st.caption("◇ 기준선 표본이 아직 부족해 평소 범위를 그리지 않았습니다(표본이 쌓이면 나타납니다).")
        zeros = sum(1 for r in series if r["speed"] == 0)
        if zeros:
            st.caption(f"ℹ︎ 속도 0 km/h 관측 {zeros}건 포함 — 결측이 0 으로 들어온 값일 수 있습니다(품질 표시는 원자료 표 참고).")
        with st.expander("원자료 표"):
            st.dataframe(pd.DataFrame(series).drop(columns=["kst_hour"]), hide_index=True, width="stretch")
    ui.freshness_line("속도 시계열", d["speeds"], now)
    ui.freshness_line("평소 기준선", d["thresholds"], now)


def page():
    ui.page_header("혼잡", "이 도로가 평소 이 시간대보다 얼마나 막히나?",
                   "각 링크의 지금 속도를 같은 요일 구분·시간대의 평소 중앙값과 비교한다. 기준선 표본이 쌓이기 전(약 11월 말까지)에는 단계가 '판정 불가'로 나온다.")
    st.info(GOLD_LAG_NOTICE)
    try:
        c, now = runtime.client(), runtime.now()
        base = load_base(c, now)
        render_base(base, now)
        ids = [l.get("link_id") for l in base["links"].rows if l.get("link_id")]
        if not ids:
            ui.empty("수집 링크 목록이 비어 있습니다.")
            return
        link_id = st.selectbox("링크", ids)
        render_link(link_id, load_link(c, now, link_id), now)
    except ApiError as e:
        ui.api_error(e)
