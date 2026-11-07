"""지도 — 문제 A·B·C를 한 장에: 수집 링크(혼잡 단계 색), 돌발 위치, 버스 위치."""
from __future__ import annotations

import pydeck as pdk
import streamlit as st

from .. import contract as C
from .. import runtime, shape, ui
from ..client import ApiError
from ..freshness import GOLD_LAG_NOTICE
from ._common import ACTIVE_PARAMS, BUS_PARAMS, CURRENT_PARAMS, LINKS_PARAMS

SEOUL = (37.5665, 126.9780)
INCIDENT_RGB = [235, 104, 52, 230]   # 범주 팔레트 2번(주황)
BUS_RGB = [42, 120, 214, 230]        # 범주 팔레트 1번(파랑)


def load(client, now):
    return {
        "links": client.get(C.LINKS, LINKS_PARAMS),
        "current": client.get(C.TRAFFIC_CURRENT, CURRENT_PARAMS),
        "incidents": client.get(C.INCIDENTS_ACTIVE, ACTIVE_PARAMS),
        "bus": client.get(C.BUS_POSITIONS, {**BUS_PARAMS, "route_id": client.bus_route_id}),
    }


def build_layers(d):
    """(레이어 목록, 중심 좌표 후보, 요약 숫자). streamlit 없이 시험할 수 있게 분리."""
    level_by_link = {r.get("link_id"): r.get("level") for r in d["current"].rows}
    paths, skipped_links = shape.link_paths(d["links"].rows, level_by_link)
    inc, inc_nocoord = shape.incident_points(d["incidents"].rows)
    bus, bus_nocoord = shape.bus_points(d["bus"].rows)
    layers = []
    if paths:
        layers.append(pdk.Layer("PathLayer", data=paths, get_path="path", get_color="color", width_min_pixels=6, pickable=True))
    if inc:
        layers.append(pdk.Layer("ScatterplotLayer", data=inc, get_position="[lon, lat]", get_fill_color=INCIDENT_RGB,
                                get_radius=90, radius_min_pixels=5, pickable=True, stroked=True, get_line_color=[255, 255, 255, 255],
                                line_width_min_pixels=2))
    if bus:
        layers.append(pdk.Layer("ScatterplotLayer", data=bus, get_position="[lon, lat]", get_fill_color=BUS_RGB,
                                get_radius=70, radius_min_pixels=5, pickable=True, stroked=True, get_line_color=[255, 255, 255, 255],
                                line_width_min_pixels=2))
    coords = [p for x in paths for p in x["path"]] + [[x["lon"], x["lat"]] for x in inc + bus]
    return layers, coords, {"links": len(paths), "links_skipped": skipped_links, "incidents": len(inc),
                            "incidents_nocoord": inc_nocoord, "bus": len(bus), "bus_nocoord": bus_nocoord,
                            "levels": shape.level_counts(d["current"].rows)}


def view_state(coords):
    if not coords:
        return pdk.ViewState(latitude=SEOUL[0], longitude=SEOUL[1], zoom=10.5)
    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return pdk.ViewState(latitude=lat, longitude=lon, zoom=11.5)


def render(d, now):
    layers, coords, s = build_layers(d)
    lv = s["levels"]
    cols = st.columns(5)
    for col, key in zip(cols, shape.LEVEL_ORDER):
        col.metric(shape.level_text(key), lv[key])
    cols[4].metric(shape.level_text(None), lv["unknown_level"], help="기준선 표본이 아직 적어 단계를 판정하지 못한 링크. 원활이 아니다.")
    deck = pdk.Deck(layers=layers, initial_view_state=view_state(coords), map_provider="carto", map_style="light",
                    tooltip={"text": "{link_id}{acc_id}\n{단계}{유형}"})
    st.pydeck_chart(deck, height=480)
    st.caption(f"선 = 수집 링크 {s['links']}개(색 = 혼잡 단계, 회색 = 판정 불가) · 주황 점 = 진행 중 돌발 {s['incidents']}건 · "
               f"파랑 점 = 버스 {s['bus']}대. 지도 바탕 타일은 브라우저가 인터넷에서 불러옵니다(막히면 바탕만 비어 보입니다).")
    miss = []
    if s["links_skipped"]:
        miss.append(f"형상이 없는 링크 {s['links_skipped']}개")
    if s["incidents_nocoord"]:
        miss.append(f"좌표가 없는 돌발 {s['incidents_nocoord']}건")
    if s["bus_nocoord"]:
        miss.append(f"좌표가 없는 버스 {s['bus_nocoord']}대")
    if miss:
        st.caption("지도에서 제외: " + ", ".join(miss))
    ui.freshness_line("링크 단계", d["current"], now)
    ui.freshness_line("돌발", d["incidents"], now)
    ui.freshness_line("버스 위치", d["bus"], now)


def page():
    ui.page_header("지도", "지금 어느 도로가 막히고, 어디에 돌발이 있고, 버스는 어디 있나?",
                   "수집 링크의 혼잡 단계(평소 같은 시간대 대비), 진행 중 돌발, 버스 위치를 한 장에서 겹쳐 볼 수 있다. 수집 대상은 링크 4개·노선 1개뿐이다.")
    st.info(GOLD_LAG_NOTICE)
    try:
        render(load(runtime.client(), runtime.now()), runtime.now())
    except ApiError as e:
        ui.api_error(e)
