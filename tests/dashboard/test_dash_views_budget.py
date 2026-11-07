"""화면별 API 호출 수. 서버 한도(분당 120, 순간 40)와 클라이언트 예산(100)을 넘지 않는지, 캐시 키가 안정적인지 센다."""
import pytest

# Mac 의 기본 환경에는 streamlit 등이 없을 수 있다 → 수집 오류 대신 건너뛴다(설치: pip install -r requirements-dashboard.txt)
for _m in ("streamlit", "pandas", "altair", "pydeck"):
    pytest.importorskip(_m)

import dashboard_support as D
from dashboard import contract as C
from dashboard.client import ApiError
from dashboard.views import bus, congestion, health, incidents, map_view, overview, weather

LOADERS = {"overview": overview.load, "map": map_view.load, "incidents": incidents.load, "bus": bus.load,
           "weather": weather.load, "health": health.load, "congestion": congestion.load_base}
MAX_PER_PAGE = {"overview": 4, "map": 4, "incidents": 3, "bus": 3, "weather": 2, "health": 7, "congestion": 2}


@pytest.mark.parametrize("name", sorted(LOADERS))
def test_each_page_cold_load_is_small(name):
    c, f, k = D.make_client()
    LOADERS[name](c, k.now())
    if name == "congestion":
        congestion.load_link(c, k.now(), D.LINKS[0])
    assert 0 < len(f.calls) <= MAX_PER_PAGE[name] + (2 if name == "congestion" else 0), (name, f.paths())


def test_visiting_every_page_in_a_minute_stays_under_burst_and_shares_cache():
    c, f, k = D.make_client()
    for name, fn in LOADERS.items():
        fn(c, k.now())
    congestion.load_link(c, k.now(), D.LINKS[0])
    assert len(f.calls) <= 25, f.paths()                 # 순간 한도 40 안, 분당 120 의 1/4 이하
    assert len(f.calls) == len(set((p, tuple(sorted(q.items()))) for p, q in f.calls)), "같은 요청을 두 번 불렀다 — 캐시 공유 깨짐"
    assert f.paths().count("/v1/traffic/current") == 1 and f.paths().count("/v1/links") == 1   # 지도·혼잡 화면이 공유
    assert f.paths().count("/v1/pipeline/health") == 1 and f.paths().count("/v1/pipeline/alerts") == 1


def test_time_windows_use_stable_cache_keys_within_a_bucket():
    """from/to 가 초마다 달라지면 캐시가 한 번도 맞지 않는다 (floor_to 로 막은 결함의 회귀 시험)."""
    c, f, k = D.make_client()
    for fn in (incidents.load, bus.load, weather.load):
        fn(c, k.now())
    congestion.load_link(c, k.now(), D.LINKS[0])
    n = len(f.calls)
    k.advance(100)                                       # 5분 버킷 안에서 시각만 흐른다
    for fn in (incidents.load, bus.load, weather.load):
        fn(c, k.now())
    congestion.load_link(c, k.now(), D.LINKS[0])
    assert len(f.calls) == n, f.paths()[n:]


def test_steady_state_rate_is_far_below_server_limit():
    """10분간 1분마다 모든 페이지를 보는 사용자(최악)의 분당 호출 수."""
    c, f, k = D.make_client()
    for minute in range(10):
        for fn in LOADERS.values():
            fn(c, k.now())
        congestion.load_link(c, k.now(), D.LINKS[0])
        k.advance(60)
    per_min = len(f.calls) / 10
    assert per_min <= 12, per_min                         # 서버 한도 120 의 10%
    assert c.calls_last_minute() <= 40


def test_two_sessions_share_one_client_so_viewers_do_not_multiply_calls():
    c, f, k = D.make_client()
    for _ in range(5):                                    # 탭 5개가 같은 화면을 연다
        map_view.load(c, k.now())
    assert len(f.calls) == 4


# ---------------------------------------------------------------- Step 10 Mac 발견(D-176): 버스 route_id 필수
def test_every_view_loads_against_a_fake_api_that_validates_like_the_real_one():
    """가짜 API 가 OpenAPI 스냅샷의 필수 매개변수·범위·길이를 검사한다. 어떤 화면이든 load 가 422 를 맞으면 여기서 실패한다."""
    import dashboard.views.bus as bus
    import dashboard.views.congestion as cong
    import dashboard.views.health as health
    import dashboard.views.incidents as inc
    import dashboard.views.map_view as mp
    import dashboard.views.overview as ov
    import dashboard.views.weather as wx
    loaders = [(m.__name__, m.load) for m in (ov, mp, inc, bus, wx, health)] + [("congestion", cong.load_base),
               ("congestion_link", lambda c, n: cong.load_link(c, n, D.LINKS[0]))]
    for name, fn in loaders:
        c, f, _ = D.make_client()
        data = fn(c, D.NOW)
        errors = [k for k, v in data.items() if getattr(v, "error", None)] if isinstance(data, dict) else []
        assert not errors, (name, errors)
        assert not [q for q in f.calls if q[0].startswith("/v1/bus/") and not q[1].get("route_id")], name


def test_fake_api_rejects_missing_required_and_out_of_range_params():
    c, f, _ = D.make_client()
    with pytest.raises(ApiError) as e:
        c.get(C.BUS_POSITIONS, {"within_min": 15, "limit": 300})            # route_id 필수
    assert e.value.kind == "client" and e.value.status == 422
    with pytest.raises(ApiError):
        c.get(C.BUS_POSITIONS, {"route_id": "100100118", "limit": 501})     # limit ≤ 500
