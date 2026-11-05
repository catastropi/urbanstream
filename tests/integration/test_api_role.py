"""sql/013 (api_service) 과 scripts/db_bootstrap.py --api. 실제 로그인까지 확인한다 — SET ROLE 로는 역할 설정(타임아웃·읽기 전용)이 적용되지 않기 때문이다."""
from __future__ import annotations

from pathlib import Path

import os

import pytest

from pg_support import pg  # noqa: F401

pytestmark = pytest.mark.pg
ROOT = Path(__file__).resolve().parents[2]
SQL_013 = ROOT / "sql" / "013_api_login.sql"
PW = "T3st-only-pw-0123456789"


@pytest.fixture
def fresh(pg):  # noqa: F811
    """역할은 클러스터 전체에 남아 이전 시험의 LOGIN 상태가 섞인다 → 역할을 지우고 013 을 다시 적용해 '마이그레이션 직후' 상태를 만든다."""
    if os.environ.get("ALLOW_ROLE_TESTS") != "1":
        pytest.skip("이 시험은 클러스터 전체 역할 api_service 를 지우고 다시 만든다(비밀번호·LOGIN 도 사라짐). 운영 Postgres 와 같은 "
                    "클러스터에서는 실행하지 말고, 일회용 Postgres 에서만 ALLOW_ROLE_TESTS=1 로 실행한다 (docs/runbooks/step9-api.md §6)")
    pg.execute("DROP ROLE IF EXISTS api_service")
    pg.execute(SQL_013.read_text(encoding="utf-8"))
    return pg


def _login(dsn: str, password: str):
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    d = conninfo_to_dict(dsn)
    return psycopg.connect(host=d.get("host", "127.0.0.1"), port=d.get("port", "5432"), dbname=d["dbname"],
                           user="api_service", password=password, autocommit=True, connect_timeout=5)


def test_013_creates_nologin_read_only_role_that_inherits_api_reader(fresh):
    can_login, limit = fresh.execute("SELECT rolcanlogin, rolconnlimit FROM pg_roles WHERE rolname='api_service'").fetchone()
    assert can_login is False and limit == 8, "비밀번호 설정 전에는 접속할 수 없어야 한다(안전한 쪽으로 실패)"
    assert fresh.execute("SELECT pg_has_role('api_service','api_reader','member')").fetchone()[0]
    cfg = fresh.execute("SELECT s.setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid=s.setrole "
                        "WHERE r.rolname='api_service' AND s.setdatabase=0").fetchone()[0]
    assert {"statement_timeout=5s", "lock_timeout=1s", "idle_in_transaction_session_timeout=10s",
            "default_transaction_read_only=on"} <= set(cfg)


def test_013_is_idempotent_and_does_not_reset_login(fresh):
    from scripts.db_bootstrap import ensure_api_login
    assert ensure_api_login(fresh, PW) == "ok"
    fresh.execute(SQL_013.read_text(encoding="utf-8"))          # 다시 적용해도 오류 없음
    assert fresh.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname='api_service'").fetchone()[0] is True, \
        "CREATE 가드가 이미 있는 역할을 건드리면 안 된다"


def test_013_adds_no_privileges_beyond_api_reader(fresh):
    """api_service 와 api_reader 가 읽을 수 있는 객체가 정확히 같아야 한다 (새 GRANT 없음)."""
    q = ("SELECT n.nspname||'.'||c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
         "WHERE n.nspname IN ('dim','fact','mart','ops') AND c.relkind IN ('r','v','m','p') AND has_table_privilege(%s, c.oid, 'SELECT') ORDER BY 1")
    a = [r[0] for r in fresh.execute(q, ("api_service",)).fetchall()]
    b = [r[0] for r in fresh.execute(q, ("api_reader",)).fetchall()]
    assert a == b and a
    assert not [x for x in a if x.startswith("ops.") and not x.startswith("ops.v_")], "ops 는 뷰만 읽을 수 있어야 한다"
    for t in ("fact.traffic_speed", "mart.anomaly_event", "dim.dim_link"):
        assert not fresh.execute("SELECT has_table_privilege('api_service', %s, 'INSERT') OR has_table_privilege('api_service', %s, 'UPDATE') "
                                 "OR has_table_privilege('api_service', %s, 'DELETE')", (t, t, t)).fetchone()[0]


def test_bootstrap_without_013_reports_no_role_and_rejects_short_password(pg):  # noqa: F811
    from scripts.db_bootstrap import ensure_api_login
    pg.execute("DROP ROLE IF EXISTS api_service")
    assert ensure_api_login(pg, PW) == "no_role"
    pg.execute(SQL_013.read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="짧"):
        ensure_api_login(pg, "short")
    assert pg.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname='api_service'").fetchone()[0] is False


def test_real_login_applies_role_settings_and_enforces_read_only_least_privilege(fresh):
    import os
    import psycopg
    from scripts.db_bootstrap import ensure_api_login
    assert ensure_api_login(fresh, PW) == "ok"
    assert ensure_api_login(fresh, PW) == "ok"                  # 멱등
    dsn = os.environ["GOLD_TEST_DSN"]
    with _login(dsn, PW) as c:
        assert c.execute("SHOW statement_timeout").fetchone()[0] == "5s"
        assert c.execute("SHOW default_transaction_read_only").fetchone()[0] == "on"
        assert c.execute("SELECT count(*) FROM ops.v_pipeline_health").fetchone()[0] >= 0
        for stmt in ("SELECT 1 FROM ops.alert_event", "SELECT 1 FROM ops.call_event", "SELECT 1 FROM ops.load_run",
                     "INSERT INTO fact.traffic_speed (dedup_key, link_id, event_time, fetched_at, event_date) VALUES ('x','x',now(),now(),current_date)",
                     "CREATE TABLE mart.should_not_exist (a int)", "DELETE FROM dim.dim_link"):
            with pytest.raises(psycopg.errors.Error):
                c.execute(stmt)
        with pytest.raises(psycopg.errors.QueryCanceled):
            c.execute("SELECT pg_sleep(7)")                     # 역할 설정 statement_timeout=5s
    with pytest.raises(psycopg.OperationalError):
        _login(dsn, PW + "x").close()
