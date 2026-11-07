"""개요 — 문제 F 일부: 전체 상태와 소스 신선도. 이 화면만 60초마다 스스로 갱신한다(운영 스냅샷 캐시 60초)."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ..timefmt import fmt_kst, humanize_age
from ._common import ALERT_PARAMS


def load(client, now):
    return {
        "health": client.get(C.PIPELINE_HEALTH),
        "sources": client.get(C.PIPELINE_SOURCES),
        "dq": client.get(C.DQ_SUMMARY, {"days": 7}),
        "alerts": client.get(C.PIPELINE_ALERTS, ALERT_PARAMS),
    }


def render(d, now):
    h = d["health"]
    overall = (h.data or {}).get("overall")
    comps = (h.data or {}).get("components") or []
    c1, c2, c3 = st.columns(3)
    c1.metric("전체 상태", shape.status_text(overall))
    counts = shape.count_by_status(comps)
    c2.metric("구성요소", f"{len(comps)}개", help="정상 / 저하 / 중단 / 대기 / 알 수 없음 개수는 아래 표")
    c3.write(" · ".join(f"{shape.status_text(k)} {v}" for k, v in sorted(counts.items(), key=lambda kv: str(kv[0]))) or "—")
    ui.freshness_line("파이프라인 상태", h, now)

    st.subheader("소스별 신선도")
    rows = [{"소스": s.get("source_system"), "상태": shape.status_text(s.get("status")),
             "마지막 정상(KST)": fmt_kst(s.get("last_ok_at")), "경과": humanize_age(s.get("age_sec")),
             "호출/1h": shape.fmt_int(s.get("calls_1h")), "실패/1h": shape.fmt_int(s.get("fail_1h")),
             "p95 지연(ms)": shape.fmt_int(s.get("p95_latency_ms")),
             "오늘 예산": f"{shape.fmt_int(s.get('budget_used'))}/{shape.fmt_int(s.get('budget_limit'))}",
             "사유": s.get("reason") or "—"} for s in d["sources"].rows]
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        ui.empty("소스 상태가 아직 없습니다(monitor 가 켜져 있는지 확인).")
    ui.freshness_line("소스 상태", d["sources"], now)

    st.subheader("데이터 품질(최근 7일)")
    dq = sorted(d["dq"].rows, key=lambda r: str(r.get("run_date")))
    if dq:
        last = dq[-1]
        a, b, c, e, f = st.columns(5)
        a.metric("통과", shape.fmt_int(last.get("n_pass")))
        b.metric("경고", shape.fmt_int(last.get("n_warn")))
        c.metric("실패", shape.fmt_int(last.get("n_fail")))
        e.metric("판정 불가", shape.fmt_int(last.get("n_unknown")), help="데이터가 없어 판단하지 못한 규칙 — 통과가 아니다")
        f.metric("검사일(KST)", str(last.get("run_date")))
        st.caption("규칙별 shadow(알림만, 마트를 막지 않음) 여부는 '이상·건강' 페이지에 있습니다.")
    else:
        ui.empty("DQ 결과가 아직 없습니다.")
    ui.freshness_line("DQ 요약", d["dq"], now)

    st.subheader("최근 알림")
    al = [{"시각(KST)": fmt_kst(a.get("created_at")), "수준": a.get("level"), "상태": a.get("status"), "제목": a.get("title")}
          for a in d["alerts"].rows]
    if al:
        st.dataframe(pd.DataFrame(al), hide_index=True, width="stretch")
    else:
        ui.empty("최근 알림이 없습니다.")


def page():
    st.info(GOLD_LAG_NOTICE)

    @st.fragment(run_every="60s")
    def body():
        ui.page_header("개요", "파이프라인이 지금 돌고 있나? 어느 소스가 멈췄나?",
                       "전체 상태와 소스별 마지막 정상 시각으로 '수집이 멈췄는지'를 바로 판단할 수 있다(문제 F).")
        try:
            render(load(runtime.client(), runtime.now()), runtime.now())
        except ApiError as e:
            ui.api_error(e)

    body()
