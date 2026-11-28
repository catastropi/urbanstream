"""API 클라이언트: 캐시·429·예산·폴백·키 비노출. 호출 수가 곧 속도 제한 안전성이라 숫자로 단언한다."""
import json
import threading

import pytest

import dashboard_support as D
from dashboard import contract as C
from dashboard.client import ApiError, FetchResponse


def resp(status, body=None, headers=None):
    return FetchResponse(status, headers or {}, json.dumps(body if body is not None else {}).encode())


def test_cache_hit_within_ttl_and_refetch_after_expiry():
    c, f, k = D.make_client()
    r1 = c.get(C.TRAFFIC_CURRENT, {"limit": 200})
    r2 = c.get(C.TRAFFIC_CURRENT, {"limit": 200})
    assert len(f.calls) == 1 and not r1.from_cache and r2.from_cache
    k.advance(299)
    c.get(C.TRAFFIC_CURRENT, {"limit": 200})
    assert len(f.calls) == 1
    k.advance(2)                                      # gold TTL 300초 경과
    c.get(C.TRAFFIC_CURRENT, {"limit": 200})
    assert len(f.calls) == 2


def test_ttl_classes_differ_and_scale_zero_disables_cache():
    c, f, k = D.make_client()
    c.get(C.PIPELINE_HEALTH); c.get(C.WEATHER_EFFECT)
    k.advance(61)
    c.get(C.PIPELINE_HEALTH); c.get(C.WEATHER_EFFECT)           # ops 60초는 만료, mart 900초는 유지
    assert f.paths().count("/v1/pipeline/health") == 2 and f.paths().count("/v1/weather/effect") == 1
    c0, f0, _ = D.make_client(ttl_scale=0.0)
    c0.get(C.PIPELINE_HEALTH); c0.get(C.PIPELINE_HEALTH)
    assert len(f0.calls) == 2


def test_different_params_are_different_cache_entries_and_order_does_not_matter():
    c, f, _ = D.make_client()
    c.get(C.TRAFFIC_THRESHOLDS, {"link_id": "A"}); c.get(C.TRAFFIC_THRESHOLDS, {"link_id": "B"})
    c.get(C.ANOMALIES, {"limit": 5, "status": "anomaly"}); c.get(C.ANOMALIES, {"status": "anomaly", "limit": 5})
    assert len(f.calls) == 3


def test_undeclared_param_is_rejected_none_dropped_bool_lowercase():
    c, f, _ = D.make_client()
    with pytest.raises(ValueError):
        c.get(C.PIPELINE_HEALTH, {"limit": 1})
    c.get(C.LINKS, {"geometry": True, "collected_only": False, "limit": 100, "cursor": None})
    assert f.calls[-1][1] == {"geometry": "true", "collected_only": "false", "limit": "100"}


def test_key_is_sent_in_header_only_and_never_leaks():
    def boom(path, params):
        return resp(500, {"code": "internal_error", "request_id": "abc123"})
    c, f, _ = D.make_client(D.FakeFetch(boom))
    with pytest.raises(ApiError) as ei:
        c.get(C.PIPELINE_HEALTH)
    assert f.last_headers["X-API-Key"] == D.KEY
    assert all(D.KEY not in str(x) for x in (ei.value, ei.value.short(), repr(c), list(c.call_log), f.calls))
    assert "abc123" in ei.value.short()


def test_429_retries_once_after_retry_after_then_succeeds():
    state = {"n": 0}
    def h(path, params):
        state["n"] += 1
        return resp(429, {"code": "rate_limited"}, {"retry-after": "2"}) if state["n"] == 1 else None
    c, f, k = D.make_client(D.FakeFetch(h))
    r = c.get(C.TRAFFIC_CURRENT)
    assert len(f.calls) == 2 and k.sleeps == [2.0] and r.error is None


def test_429_twice_stops_after_one_retry_and_falls_back_to_last_value():
    c, f, k = D.make_client()
    good = c.get(C.TRAFFIC_CURRENT)
    k.advance(400)
    n0 = len(f.calls)
    c._fetch = D.FakeFetch(lambda p, q: resp(429, {}, {"retry-after": "1"}))
    r = c.get(C.TRAFFIC_CURRENT)
    assert len(c._fetch.calls) == 2, "재시도는 정확히 1회"
    assert r.rows == good.rows and r.from_cache and "rate_limited" in r.error and len(f.calls) == n0


def test_429_without_prior_value_raises_and_long_retry_after_is_not_waited():
    c, f, k = D.make_client(D.FakeFetch(lambda p, q: resp(429, {}, {"retry-after": "60"})))
    with pytest.raises(ApiError) as ei:
        c.get(C.TRAFFIC_CURRENT)
    assert ei.value.kind == "rate_limited" and len(f.calls) == 1 and k.sleeps == []


def test_auth_and_client_errors_are_not_masked_by_stale_values():
    c, f, k = D.make_client()
    c.get(C.TRAFFIC_CURRENT)
    k.advance(400)
    c._fetch = D.FakeFetch(lambda p, q: resp(401, {"code": "unauthorized"}))
    with pytest.raises(ApiError) as ei:
        c.get(C.TRAFFIC_CURRENT)
    assert ei.value.kind == "auth"
    c._fetch = D.FakeFetch(lambda p, q: resp(422, {"code": "invalid_parameter"}))
    with pytest.raises(ApiError) as ei:
        c.get(C.TRAFFIC_CURRENT)
    assert ei.value.kind == "client"


@pytest.mark.parametrize("handler", [lambda p, q: resp(503, {"code": "database_unavailable"}),
                                     lambda p, q: (_ for _ in ()).throw(ConnectionError("x")),
                                     lambda p, q: FetchResponse(200, {}, b"<html>proxy</html>"),
                                     lambda p, q: resp(200, {"nope": 1})])
def test_server_network_and_bad_responses_fall_back_to_last_value(handler):
    c, f, k = D.make_client()
    good = c.get(C.PIPELINE_HEALTH)
    k.advance(61)
    c._fetch = D.FakeFetch(handler)
    r = c.get(C.PIPELINE_HEALTH)
    assert r.rows == good.rows and r.data == good.data and r.from_cache and r.error
    assert r.fetched_at == good.fetched_at                       # 받은 시각은 옛 값 그대로 — 새 값처럼 보이지 않는다
    c2, _, _ = D.make_client(D.FakeFetch(handler))
    with pytest.raises(ApiError):
        c2.get(C.PIPELINE_HEALTH)


def test_budget_blocks_calls_and_uses_stale_or_waits():
    c, f, k = D.make_client(budget_per_min=3)
    for p in ("a", "b", "c"):
        c.get(C.TRAFFIC_THRESHOLDS, {"link_id": p})
    with pytest.raises(ApiError) as ei:
        c.get(C.TRAFFIC_THRESHOLDS, {"link_id": "d"})
    assert ei.value.kind == "budget" and len(f.calls) == 3
    assert c.calls_last_minute() == 3
    k.advance(61)
    c.get(C.TRAFFIC_THRESHOLDS, {"link_id": "d"})
    assert len(f.calls) == 4
    cw, fw, kw = D.make_client(budget_per_min=2, on_budget="wait")
    for p in ("a", "b", "c"):
        cw.get(C.TRAFFIC_THRESHOLDS, {"link_id": p})
    assert len(fw.calls) == 3 and kw.sleeps and sum(kw.sleeps) >= 59


def test_budget_exhaustion_still_serves_stale_value():
    c, f, k = D.make_client(budget_per_min=1)
    c.get(C.PIPELINE_HEALTH)
    k.advance(61)
    c.get(C.WEATHER_EFFECT)                                      # 이 분의 예산을 소진
    r = c.get(C.PIPELINE_HEALTH)                                 # 만료됐지만 예산 없음 → 마지막 값
    assert r.error and "budget" in r.error and r.from_cache


def test_single_flight_concurrent_requests_hit_api_once():
    gate, started = threading.Event(), threading.Event()
    def slow(path, params):
        started.set(); gate.wait(5); return None
    c, f, _ = D.make_client(D.FakeFetch(slow))
    out = []
    ts = [threading.Thread(target=lambda: out.append(c.get(C.TRAFFIC_CURRENT))) for _ in range(6)]
    for t in ts: t.start()
    started.wait(5); gate.set()
    for t in ts: t.join(10)
    assert len(f.calls) == 1 and len(out) == 6


def test_get_all_follows_cursor_merges_and_flags_truncation():
    pages = {None: (["r1", "r2"], "c1"), "c1": (["r3"], "c2"), "c2": (["r4"], None)}
    def h(path, params):
        rows, nxt = pages[params.get("cursor")]
        return resp(200, D.env(rows, D.meta(D.NOW), {"has_more": nxt is not None, "limit": 2, "next_cursor": nxt}))
    c, f, _ = D.make_client(D.FakeFetch(h))
    r = c.get_all(C.ANOMALIES, {"limit": 2}, max_pages=5)
    assert r.rows == ["r1", "r2", "r3", "r4"] and not r.truncated and len(f.calls) == 3
    assert f.calls[1][1]["cursor"] == "c1"
    c2, f2, _ = D.make_client(D.FakeFetch(h))
    r2 = c2.get_all(C.ANOMALIES, {"limit": 2}, max_pages=2)
    assert r2.rows == ["r1", "r2", "r3"] and r2.truncated and len(f2.calls) == 2


def test_refresh_expires_but_keeps_values_and_is_rate_limited():
    c, f, k = D.make_client()
    c.get(C.TRAFFIC_CURRENT)
    assert c.refresh() is True and c.refresh() is False
    k.advance(31)
    assert c.refresh() is True
    c.get(C.TRAFFIC_CURRENT)
    assert len(f.calls) == 2                                      # 만료돼서 다시 불렀다
    c._fetch = D.FakeFetch(lambda p, q: resp(500, {}))
    k.advance(31); c.refresh()
    assert c.get(C.TRAFFIC_CURRENT).error                         # 새로고침 뒤에 실패해도 값은 남아 있다


def test_cache_is_bounded():
    c, f, _ = D.make_client(max_entries=8)
    for i in range(30):
        c.get(C.TRAFFIC_THRESHOLDS, {"link_id": f"L{i}"})
    assert len(c._cache) == 8


def test_path_must_be_absolute():
    c, _, _ = D.make_client()
    with pytest.raises(ValueError):
        c._once("//evil.example/x", {})
    with pytest.raises(ValueError):
        c._once("v1/x", {})
