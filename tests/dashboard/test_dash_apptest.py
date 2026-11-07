"""Streamlit AppTest 스모크: 가짜 API 로 모든 페이지를 실제로 실행해 예외 없이 그려지는지, 핵심 문구가 나오는지 본다.
(지도·차트의 '눈으로 본 모양'은 이 시험이 보장하지 않는다 — 런북의 화면 점검 참고.)"""
import json

import pytest

# Mac 의 기본 환경에는 streamlit 등이 없을 수 있다 → 수집 오류 대신 건너뛴다(설치: pip install -r requirements-dashboard.txt)
for _m in ("streamlit", "pandas", "altair", "pydeck"):
    pytest.importorskip(_m)
from streamlit.testing.v1 import AppTest

import dashboard_support as D
from dashboard import runtime
from dashboard.client import FetchResponse

PAGES = {"overview": "overview", "map_view": "map_view", "congestion": "congestion", "incidents": "incidents",
         "bus": "bus", "weather": "weather", "health": "health"}


def script(mod):
    return f"from dashboard.views import {mod}\n{mod}.page()\n"


@pytest.fixture
def install(monkeypatch):
    monkeypatch.setattr(runtime, "now", lambda: D.NOW)          # 가짜 데이터의 시각(11-05 15:20 KST)과 맞춘 고정 시계

    def _install(fetch=None):
        c, f, k = D.make_client(fetch)
        runtime.set_client_for_tests(c)
        return c, f
    yield _install
    runtime.set_client_for_tests(None)


def texts(at):
    out = []
    for group in (at.title, at.caption, at.markdown, at.info, at.warning, at.error, at.metric, at.subheader):
        for e in group:
            out.append(str(getattr(e, "label", "")) + " " + str(e.value))
    return "\n".join(out)


@pytest.mark.parametrize("mod", sorted(PAGES))
def test_every_page_renders_without_exception_and_never_shows_the_key(mod, install):
    install()
    at = AppTest.from_string(script(mod), default_timeout=30).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    body = texts(at)
    assert "이 화면에서 낼 수 있는 결론" in body
    assert D.KEY not in body


def test_overview_shows_status_symbols_lag_notice_and_dq_unknown_is_not_a_pass(install):
    install()
    at = AppTest.from_string(script("overview"), default_timeout=30).run()
    assert not at.exception
    body = texts(at)
    assert "실시간이 아닙니다" in body and "▲ 저하" in body
    assert "판정 불가 3" in body                                      # DQ unknown 은 통과(18)에 합치지 않는다
    assert "통과 18" in body
    assert "기준 11-05 15:19 KST · 경과 1분 미만" in body             # 40초 전 스냅샷, KST, 고정 시계 기준 경과


def test_congestion_says_undetermined_not_normal_and_notes_holiday_approximation(install):
    install()
    at = AppTest.from_string(script("congestion"), default_timeout=30).run()
    assert not at.exception
    body = texts(at)
    assert "◇ 판정 불가(표본 부족) 4" in body and "● 원활 0" in body
    assert "● 원활 4" not in body
    assert "공휴일은 평일로 근사" in body
    assert "기준 11-05 14:35 KST · 경과 45분" in body                 # 45분 전 관측을 KST 로
    assert len(at.dataframe) >= 1 and at.get("vega_lite_chart")
    assert "기준선 표본이 아직 부족" not in body                      # 가짜 임계값이 있으면 띠를 그린다


def test_map_page_builds_a_deck_and_reports_skipped_items(install):
    install()
    at = AppTest.from_string(script("map_view"), default_timeout=30).run()
    assert not at.exception
    assert at.get("deck_gl_json_chart"), "지도 요소가 없다"
    body = texts(at)
    assert "좌표가 없는 돌발 1건" in body and "좌표가 없는 버스 1대" in body


def test_incidents_page_charts_only_types_with_sufficient_samples(install):
    install()
    at = AppTest.from_string(script("incidents"), default_timeout=30).run()
    assert not at.exception and len(at.dataframe) >= 2
    assert len(at.get("vega_lite_chart")) == 1                  # 표본 충분한 '사고' 만 막대, '공사'(n_sufficient=0)는 표에만
    assert "연결되는 돌발이 적습니다" not in texts(at)


def test_weather_page_has_association_not_causation_and_two_separate_charts(install):
    install()
    at = AppTest.from_string(script("weather"), default_timeout=30).run()
    assert not at.exception
    body = texts(at)
    assert "인과가 아닙니다" in body
    assert len(at.get("vega_lite_chart")) >= 3                  # 효과 막대 1 + 기온 + 강수량(이중 축 아님)


def test_api_failure_shows_message_not_a_crash_and_auth_error_hides_key(install):
    install(D.FakeFetch(lambda p, q: FetchResponse(500, {}, b"{}")))
    at = AppTest.from_string(script("health"), default_timeout=30).run()
    assert not at.exception and at.error and "데이터를 불러오지 못했습니다" in str(at.error[0].value)
    install(D.FakeFetch(lambda p, q: FetchResponse(401, {}, json.dumps({"code": "unauthorized", "request_id": "r1"}).encode())))
    at = AppTest.from_string(script("health"), default_timeout=30).run()
    body = texts(at)
    assert not at.exception and "auth" in body and D.KEY not in body and "API_KEYS" in body


def test_stale_data_gets_a_text_warning(install):
    def stale(path, params):
        if path == "/v1/traffic/current":
            p = D.payload_for(path, params)
            p["meta"]["stale"] = True
            return FetchResponse(200, {}, json.dumps(p).encode())
    install(D.FakeFetch(stale))
    at = AppTest.from_string(script("map_view"), default_timeout=30).run()
    assert any("오래됨" in str(w.value) for w in at.warning)


def test_whole_app_with_navigation_runs(install):
    install()
    at = AppTest.from_file(str(__import__("pathlib").Path(__file__).resolve().parents[2] / "dashboard/app.py"), default_timeout=40).run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("API 호출(최근 1분)" in str(c.value) for c in at.sidebar.caption)
