"""이상·건강 — 문제 E(실시간 이상 탐지)와 F(파이프라인 자체 이상)."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..timefmt import fmt_kst, humanize_age
from ._common import ALERT_PARAMS


def load(client, now):
    return {"anomalies": client.get(C.ANOMALIES, {"limit": 50}),
            "health": client.get(C.PIPELINE_HEALTH),
            "layers": client.get(C.PIPELINE_LAYERS),
            "marts": client.get(C.PIPELINE_MARTS),
            "gaps": client.get(C.PIPELINE_GAPS, {"limit": 20}),
            "dq": client.get(C.DQ_LATEST),
            "alerts": client.get(C.PIPELINE_ALERTS, ALERT_PARAMS)}


def render(d, now):
    st.subheader("이상 탐지(최근 7일)")
    an = d["anomalies"].rows
    n_real = sum(1 for r in an if r.get("status") == "anomaly")
    st.metric("이상", n_real, help="후보(candidate)와 기준선 퇴화는 '이상'에 세지 않는다 — 표본 부족일 수 있음")
    if an:
        st.dataframe(pd.DataFrame(shape.anomaly_rows(an)), hide_index=True, width="stretch")
        st.caption("robust z = (속도 − 평소 중앙값) / max(IQR/1.349, 2 km/h), |z| ≥ 3.5 를 이상으로 봅니다(가정값, D-121). "
                   "표본이 20 미만이거나 기준선이 잠정이면 '후보' 입니다.")
    else:
        ui.empty("최근 7일 이상 후보가 없습니다.")
    ui.freshness_line("이상 탐지", d["anomalies"], now)

    st.subheader("파이프라인 구성요소")
    comps = (d["health"].data or {}).get("components") or []
    if comps:
        st.dataframe(pd.DataFrame(shape.component_rows(comps, now)), hide_index=True, width="stretch")
    else:
        ui.empty("구성요소 상태가 없습니다.")
    ui.freshness_line("구성요소", d["health"], now)

    st.subheader("계층·마트 신선도")
    lay = [{"계층": r.get("layer"), "표": r.get("table_name"), "상태": shape.status_text(r.get("status")),
            "마지막 쓰기(KST)": fmt_kst(r.get("last_write_at")), "경과": humanize_age(r.get("age_sec")), "사유": r.get("reason") or "—"}
           for r in d["layers"].rows]
    if lay:
        st.dataframe(pd.DataFrame(lay), hide_index=True, width="stretch")
    ui.freshness_line("계층", d["layers"], now)
    mar = [{"마트": r.get("mart"), "상태": shape.status_text(r.get("status")), "마지막 계산(KST)": fmt_kst(r.get("last_computed_at")),
            "경과": humanize_age(r.get("age_sec")), "허용(시간)": shape.fmt_num(r.get("max_age_hours"), 0)} for r in d["marts"].rows]
    if mar:
        st.dataframe(pd.DataFrame(mar), hide_index=True, width="stretch")
    ui.freshness_line("마트", d["marts"], now)

    st.subheader("수집 공백")
    gaps = [{"종류": g.get("kind"), "시작(KST)": fmt_kst(g.get("gap_start")), "끝(KST)": fmt_kst(g.get("gap_end")),
             "길이": humanize_age(g.get("duration_sec")), "복구 가능": "예" if g.get("recoverable") else "아니오"} for g in d["gaps"].rows]
    if gaps:
        st.dataframe(pd.DataFrame(gaps), hide_index=True, width="stretch")
    else:
        ui.empty("기록된 수집 공백이 없습니다.")
    ui.freshness_line("수집 공백", d["gaps"], now)

    st.subheader("데이터 품질 규칙(최신 결과)")
    dq = [{"규칙": r.get("rule_id"), "결과": r.get("status"), "심각도": r.get("severity"),
           "섀도": "예(알림만)" if r.get("shadow") else "아니오", "값": shape.fmt_num(r.get("value"), 3),
           "임계": r.get("threshold") or "—", "대상": r.get("target")} for r in d["dq"].rows]
    if dq:
        st.dataframe(pd.DataFrame(dq), hide_index=True, width="stretch")
        st.caption("'unknown' 은 데이터가 없어 판단하지 못했다는 뜻이며 통과가 아닙니다. 섀도 규칙은 마트를 막지 않습니다.")
    else:
        ui.empty("DQ 결과가 없습니다.")
    ui.freshness_line("DQ 규칙", d["dq"], now)


def page():
    ui.page_header("이상·건강", "평소와 다른 값이 나오나? 파이프라인 어딘가 멈췄나?",
                   "이상 후보와 파이프라인 각 단계의 마지막 정상 시각·공백·DQ 결과로 '어느 단계가 왜 늦는지'를 좁힐 수 있다(문제 E·F).")
    try:
        render(load(runtime.client(), runtime.now()), runtime.now())
    except ApiError as e:
        ui.api_error(e)
