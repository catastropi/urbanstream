"""UrbanStream 대시보드 진입점:  streamlit run dashboard/app.py

데이터는 FastAPI(X-API-Key)에서만 읽는다. DB 에 직접 붙지 않는다 (ADR-0012).
"""
import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:      # `streamlit run dashboard/app.py` 는 스크립트 폴더만 경로에 넣는다 → 저장소 루트를 추가
    sys.path.insert(0, _ROOT)

import streamlit as st  # noqa: E402

from dashboard import runtime  # noqa: E402
from dashboard.config import ConfigError, load_settings  # noqa: E402
from dashboard.timefmt import fmt_kst  # noqa: E402
from dashboard.views import bus, congestion, health, incidents, map_view, overview, weather  # noqa: E402


def main() -> None:
    st.set_page_config(page_title="UrbanStream", page_icon="🚦", layout="wide")
    if runtime._override is None:       # 시험에서는 가짜 클라이언트가 이미 있다
        try:
            load_settings()
        except ConfigError as e:
            st.error(f"대시보드 설정 오류: {e}")
            st.stop()
    pages = [
        st.Page(overview.page, title="개요", icon="🧭", url_path="overview", default=True),
        st.Page(map_view.page, title="지도", icon="🗺️", url_path="map"),
        st.Page(congestion.page, title="혼잡 (A)", icon="🚗", url_path="congestion"),
        st.Page(incidents.page, title="돌발 영향 (B)", icon="🚧", url_path="incidents"),
        st.Page(bus.page, title="버스 (C)", icon="🚌", url_path="bus"),
        st.Page(weather.page, title="날씨 (D)", icon="🌧️", url_path="weather"),
        st.Page(health.page, title="이상·건강 (E·F)", icon="🩺", url_path="health"),
    ]
    nav = st.navigation(pages)
    with st.sidebar:
        c = runtime.client()
        st.caption(f"지금 {fmt_kst(runtime.now())}")
        if st.button("지금 새로고침", help="캐시를 만료시킵니다. 연타 방지로 30초에 한 번만 동작합니다."):
            st.toast("캐시를 만료시켰습니다." if c.refresh() else "방금 새로고침했습니다. 잠시 뒤에 다시 눌러 주세요.")
        st.caption(f"API 호출(최근 1분): {c.calls_last_minute()}회")
        st.caption("데이터는 FastAPI 에서만 읽습니다. 시각은 모두 KST.")
    nav.run()


main()
