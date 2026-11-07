"""날씨 — 문제 D: 비 오면 느려지나? (연관이지 인과가 아니다)"""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ..timefmt import floor_to, to_kst, window
from ._common import QUANT_SEC

BLUE, GRAY = "#2a78d6", "#8a8a85"


def load(client, now):
    f, t = window(floor_to(now, QUANT_SEC), hours=48)
    return {"effect": client.get(C.WEATHER_EFFECT),
            "obs": client.get_all(C.WEATHER_OBS, {"from": f, "to": t, "limit": 200}, max_pages=2)}


def effect_chart(rows):
    df = pd.DataFrame([{"강수": r["강수"], "p25": r["p25"], "p50": r["p50"], "p75": r["p75"],
                        "구분": "표본 부족(읽지 말 것)" if r["표본 부족"] else "표본 충분", "시간 수": r["시간 수"]}
                       for r in rows if r["p50"] is not None])
    if df.empty:
        return None
    order = [r["강수"] for r in rows]
    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4).encode(
        x=alt.X("강수:N", sort=order, title=None), y=alt.Y("p50:Q", title="평소 대비 속도 비율 (1.0 = 평소)", scale=alt.Scale(zero=True)),
        color=alt.Color("구분:N", scale=alt.Scale(domain=["표본 충분", "표본 부족(읽지 말 것)"], range=[BLUE, GRAY])),
        tooltip=["강수", "p25", "p50", "p75", "시간 수", "구분"])
    err = alt.Chart(df).mark_rule(color="#52514e").encode(x=alt.X("강수:N", sort=order), y="p25:Q", y2="p75:Q")
    one = alt.Chart(pd.DataFrame({"y": [1.0]})).mark_rule(strokeDash=[4, 3], color="#52514e").encode(y="y:Q")
    return bars + err + one


def render(d, now):
    st.subheader("강수 구분별 속도 비율")
    rows = shape.weather_effect_rows(d["effect"].rows)
    ch = effect_chart(rows)
    if ch is not None:
        st.altair_chart(ch, width="stretch")
        st.caption("막대 = 중앙값, 세로선 = 25~75분위, 점선 = 1.0(평소). 회색 막대는 시간 수가 20 미만이라 읽지 않습니다. "
                   "**연관이지 인과가 아닙니다** — 비 오는 시간대에 다른 요인(퇴근 시간 등)이 겹칠 수 있습니다.")
        st.dataframe(pd.DataFrame([{k: v for k, v in r.items() if k != "_key"} for r in rows]), hide_index=True, width="stretch")
    else:
        ui.empty("강수 구분별 비율을 계산할 표본이 아직 없습니다.")
    ui.freshness_line("날씨 효과", d["effect"], now)

    st.subheader("최근 48시간 기상 관측(격자 1개)")
    obs = []
    for o in d["obs"].rows:
        t = to_kst(o.get("base_time"))
        if t is not None:
            obs.append({"t": t.replace(tzinfo=None), "기온(°C)": o.get("t1h"), "강수량(mm)": o.get("rn1")})
    if obs:
        df = pd.DataFrame(sorted(obs, key=lambda r: r["t"]))
        c1, c2 = st.columns(2)   # 단위가 다른 두 값은 이중 축이 아니라 차트 두 개로 그린다
        for col, name in ((c1, "기온(°C)"), (c2, "강수량(mm)")):
            col.altair_chart(alt.Chart(df).mark_line(color=BLUE, strokeWidth=2, point=True).encode(
                x=alt.X("t:T", title="발표 기준 시각(KST)"), y=alt.Y(f"{name}:Q", title=name),
                tooltip=[alt.Tooltip("t:T", format="%m-%d %H:%M"), f"{name}:Q"]), width="stretch")
    else:
        ui.empty("최근 48시간 기상 관측이 없습니다.")
    ui.freshness_line("기상 관측", d["obs"], now)


def page():
    ui.page_header("날씨", "비 오면 도로가 느려지나?",
                   "강수 구분별로 평소 대비 속도 비율이 어떻게 다른지 연관을 본다. 표본이 20시간 미만인 구분은 회색이며 결론을 내리지 않는다.")
    st.info(GOLD_LAG_NOTICE)
    try:
        render(load(runtime.client(), runtime.now()), runtime.now())
    except ApiError as e:
        ui.api_error(e)
