"""API 핵심(DB 없음): 인증·오류 형식·속도 제한·캐시·커서·기간·설정."""
from datetime import datetime, timedelta, timezone

import pytest

# 기본 환경에 fastapi 가 없으면 수집 오류 대신 건너뛴다(설치: pip install -r requirements-api.txt)
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from api.cache import TTLCache
from api.config import ConfigError, load_settings
from api.errors import ApiError
from api.main import create_app
from api.pagination import decode_cursor, encode_cursor, resolve_window
from api.ratelimit import RateLimiter

KEY = "k" * 32
KEY2 = "j" * 32
NOW = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)


class FakeDb:
    def __init__(self, ok=True):
        self.ok = ok

    def ping(self):
        return self.ok

    def rows(self, sql, params=None):
        return []

    def row(self, sql, params=None):
        return None


def env(**kw):
    e = {"API_KEYS": f"{KEY},{KEY2}", "API_DB_PASSWORD": "pw" * 8}
    e.update(kw)
    return e


def client(db=None, **kw):
    s = load_settings(env(**kw))
    return TestClient(create_app(s, db or FakeDb(), clock=lambda: NOW), raise_server_exceptions=False)


def test_health_open_ready_checks_db():
    c = client()
    assert c.get("/health").json() == {"status": "ok", "version": "1.0.0"}
    assert c.get("/ready").status_code == 200
    r = client(FakeDb(ok=False)).get("/ready")
    assert r.status_code == 503 and r.json()["code"] == "database_unavailable" and r.headers["retry-after"] == "5"


def test_problem_json_and_request_id():
    r = client().get("/nope", headers={"X-Request-ID": "abc-12345678"})
    assert r.status_code == 404 and r.headers["content-type"].startswith("application/problem+json")
    b = r.json()
    assert b["code"] == "not_found" and b["type"] == "urn:urbanstream:error:not_found" and b["request_id"] == "abc-12345678"
    assert r.headers["x-request-id"] == "abc-12345678" and r.headers["cache-control"] == "no-store"


def test_bad_request_id_replaced():
    r = client().get("/health", headers={"X-Request-ID": "x y\n"})
    assert len(r.headers["x-request-id"]) == 32


def test_validation_error_does_not_echo_input():
    app = create_app(load_settings(env()), FakeDb(), clock=lambda: NOW)

    @app.get("/_t")
    def t(n: int):
        return n
    r = TestClient(app).get("/_t?n=SECRET-VALUE")
    assert r.status_code == 422 and r.json()["code"] == "validation_error" and "SECRET-VALUE" not in r.text


def test_unhandled_error_is_500_without_internals():
    app = create_app(load_settings(env()), FakeDb(), clock=lambda: NOW)

    @app.get("/_boom")
    def boom():
        raise RuntimeError("password=hunter2")
    r = TestClient(app, raise_server_exceptions=False).get("/_boom")
    assert r.status_code == 500 and r.json()["code"] == "internal_error" and "hunter2" not in r.text


def test_guard_variants():
    from fastapi import Depends
    from api.security import guard
    app = create_app(load_settings(env(API_RATE_BURST="3", API_RATE_LIMIT_PER_MIN="60")), FakeDb(), clock=lambda: NOW)

    @app.get("/_p", dependencies=[Depends(guard)])
    def p():
        return {"ok": 1}
    c = TestClient(app)
    r = c.get("/_p")
    assert r.status_code == 401 and r.json()["code"] == "missing_api_key" and r.headers["www-authenticate"] == "ApiKey"
    for bad in ("", "x", KEY + "x", KEY[:-1], KEY.upper()):
        r = c.get("/_p", headers={"X-API-Key": bad})
        assert r.status_code == 401, bad
    assert c.get("/_p", headers={"X-API-Key": KEY}).status_code == 200
    assert c.get("/_p", headers={"X-API-Key": KEY2}).status_code == 200      # 두 번째 키(회전)
    assert c.get("/_p?api_key=" + KEY).status_code == 401                    # 쿼리 문자열 키는 받지 않는다
    for _ in range(3):
        c.get("/_p", headers={"X-API-Key": KEY})
    r = c.get("/_p", headers={"X-API-Key": KEY})
    assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1 and r.json()["code"] == "rate_limited"
    assert c.get("/_p", headers={"X-API-Key": KEY2}).status_code == 200      # 다른 키는 영향 없음
    assert c.get("/_p", headers={"X-API-Key": "bad"}).status_code == 401     # 실패는 429 가 아니라 401


def test_docs_toggle():
    assert client().get("/docs").status_code == 200
    c = client(API_DOCS_ENABLED="false")
    assert c.get("/docs").status_code == 404 and c.get("/openapi.json").status_code == 404


def test_cors():
    c = client(API_CORS_ORIGINS="http://localhost:8501")
    r = c.options("/health", headers={"Origin": "http://localhost:8501", "Access-Control-Request-Method": "GET"})
    assert r.headers.get("access-control-allow-origin") == "http://localhost:8501"
    r = c.options("/health", headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in r.headers


@pytest.mark.parametrize("bad", [
    {"API_KEYS": ""}, {"API_KEYS": "short"}, {"API_KEYS": f"{KEY},{KEY}"}, {"API_KEYS": "a b" + "c" * 30},
    {"API_DB_PASSWORD": ""}, {"API_CORS_ORIGINS": "*"}, {"API_POOL_MAX": "99"}, {"API_RATE_BURST": "0"},
])
def test_settings_rejects(bad):
    e = env(**bad)
    if bad.get("API_DB_PASSWORD") == "":
        e.pop("API_DB_PASSWORD")
    with pytest.raises(ConfigError):
        load_settings(e)


def test_settings_hide_secrets_in_repr():
    s = load_settings(env())
    assert KEY not in repr(s) and "pwpw" not in repr(s)


def test_rate_limiter_refill():
    t = [0.0]
    rl = RateLimiter(60, 2, clock=lambda: t[0])
    assert rl.check("a") == 0 and rl.check("a") == 0
    assert rl.check("a") == 1
    t[0] += 1.0
    assert rl.check("a") == 0


def test_cache_ttl_and_errors_not_cached():
    t = [0.0]
    c, calls = TTLCache(clock=lambda: t[0]), []

    def build():
        calls.append(1)
        return len(calls)
    assert c.get_or_build("k", 10, build) == 1 and c.get_or_build("k", 10, build) == 1
    t[0] = 11
    assert c.get_or_build("k", 10, build) == 2
    assert c.get_or_build("z", 0, build) == 3 and c.get_or_build("z", 0, build) == 4   # ttl 0 = 우회

    def boom():
        raise ApiError(503, "x")
    with pytest.raises(ApiError):
        c.get_or_build("e", 10, boom)
    assert c.get_or_build("e", 10, build) == 5


def test_cursor_roundtrip_and_rejects():
    cur = encode_cursor([NOW, "L1"])
    assert decode_cursor(cur, 2) == [NOW.isoformat(), "L1"] and decode_cursor(None, 2) is None
    for bad in ("!!!", "e30", encode_cursor(["a"]), encode_cursor(["x" * 200, "b"])):
        with pytest.raises(ApiError) as ei:
            decode_cursor(bad, 2)
        assert ei.value.code == "invalid_cursor"


def test_window():
    w = resolve_window(None, None, NOW)
    assert w.end == NOW and w.start == NOW - timedelta(hours=24)
    assert resolve_window(NOW - timedelta(days=7), NOW, NOW).start == NOW - timedelta(days=7)      # 경계 포함
    for args, code in [((NOW - timedelta(days=7, seconds=1), NOW), "window_too_large"), ((NOW, NOW), "invalid_window"),
                       ((NOW, NOW - timedelta(hours=1)), "invalid_window"), ((datetime(2026, 1, 1), NOW), "naive_datetime")]:
        with pytest.raises(ApiError) as ei:
            resolve_window(*args, NOW)
        assert ei.value.code == code


def test_db_error_mapping(monkeypatch):
    """DB 예외 → 안정된 오류 코드. 내부 문자열은 새지 않는다."""
    import psycopg
    from psycopg_pool import PoolTimeout
    from api.db import Database
    db = Database(load_settings(env()))

    class Boom:
        def __init__(self, exc):
            self.exc = exc

        def connection(self):
            raise self.exc
    cases = [(psycopg.errors.QueryCanceled("secret sql"), 503, "query_timeout"),
             (psycopg.errors.ObjectNotInPrerequisiteState("mv x"), 503, "data_not_ready"),
             (psycopg.errors.DataError("NUL"), 422, "invalid_parameter"),
             (PoolTimeout("t"), 503, "database_unavailable"),
             (psycopg.OperationalError("host=10.0.0.5 password=x"), 503, "database_unavailable")]
    for exc, status, code in cases:
        db.pool = Boom(exc)
        with pytest.raises(ApiError) as ei:
            db.rows("SELECT 1")
        assert (ei.value.status, ei.value.code) == (status, code)
        assert "10.0.0.5" not in ei.value.detail and "secret" not in ei.value.detail
    db.pool = Boom(RuntimeError("x"))
    with pytest.raises(RuntimeError):          # 모르는 예외는 삼키지 않는다(→ 500 처리기)
        db.rows("SELECT 1")


def test_cache_is_bounded():
    c = TTLCache(max_entries=3, clock=lambda: 0.0)
    for i in range(20):
        c.get_or_build(f"k{i}", 100, lambda i=i: i)
    assert len(c) <= 3


def test_cache_skips_oversized_responses():
    c, calls = TTLCache(clock=lambda: 0.0), []

    def build():
        calls.append(1)
        return list(range(500))
    for _ in range(2):
        c.get_or_build("big", 100, build, store_if=lambda v: len(v) <= 300)
    assert len(calls) == 2 and len(c) == 0
