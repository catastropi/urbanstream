"""작업 잠금(batch/jobs.job_lock, STEP 3B 4장)의 흐름. 실제 PostgreSQL 동작은 tests/integration/test_job_lock_pg.py."""
from __future__ import annotations

import pytest

from batch import jobs as J
from batch import dq_jobs


class FakeConn:
    """pg_try_advisory_lock 이 free_after 번째 시도부터 True 를 돌려준다(None 이면 끝까지 False)."""
    def __init__(self, free_after=0, holder=("uslock:gold_facts_incremental:gold_intraday#1:pid7:r", 7, "11-26 01:20")):
        self.free_after, self.tries, self.sql, self.closed, self.holder = free_after, 0, [], False, holder

    def execute(self, sql, params=()):
        self.sql.append(sql)
        conn = self

        class Cur:
            def fetchone(self_inner):
                if "pg_try_advisory_lock" in sql:
                    conn.tries += 1
                    return (conn.free_after is not None and conn.tries > conn.free_after,)
                if "pg_locks" in sql:
                    return conn.holder
                return (True,)
        return Cur()

    def close(self):
        self.closed = True


def lock_with(conn, **kw):
    labels = []

    def connect(label):
        labels.append(label)
        return conn
    return J.job_lock("gold_facts_incremental", connect=connect, sleep=lambda s: None, **kw), labels


def test_lock_runs_the_body_and_releases(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "gold_intraday")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__2026-11-26T01:20")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "2")
    c = FakeConn()
    cm, labels = lock_with(c)
    with cm:
        ran = True
    assert ran and c.closed
    assert any("pg_advisory_unlock" in s for s in c.sql)
    assert labels[0].startswith("uslock:gold_facts_incremental:gold_intraday#2:pid") and len(labels[0]) <= 63


def test_lock_waits_then_proceeds_when_the_holder_finishes():
    c = FakeConn(free_after=3)
    t = {"now": 0.0}
    cm, _ = lock_with(c, clock=lambda: t["now"], wait_sec=600, poll_sec=30)
    with cm:
        pass
    assert c.tries == 4


def test_lock_gives_up_after_wait_with_the_holder_in_the_message():
    c = FakeConn(free_after=None)
    t = {"now": 0.0}

    def clock():
        t["now"] += 100
        return t["now"]
    cm, _ = lock_with(c, clock=clock, wait_sec=600)
    with pytest.raises(J.LockBusy, match="이전 시도가 아직 실행 중.*pid 7"):
        with cm:
            pytest.fail("잠금을 못 잡았는데 본문이 돌면 안 된다")
    assert c.closed
    assert not any("pg_advisory_unlock" in s for s in c.sql), "잡지 않은 잠금은 풀지 않는다"


def test_lock_is_released_when_the_body_raises():
    c = FakeConn()
    cm, _ = lock_with(c)
    with pytest.raises(RuntimeError):
        with cm:
            raise RuntimeError("spark died")
    assert any("pg_advisory_unlock" in s for s in c.sql) and c.closed


def test_lock_key_is_stable_and_differs_per_job():
    assert J.lock_key("gold_window") == J.lock_key("gold_window")
    names = [J.LOCK_INCREMENTAL, J.LOCK_WINDOW, J.LOCK_INLINE, J.LOCK_RECONCILE]
    assert len({J.lock_key(n) for n in names}) == 4
    assert all(0 <= J.lock_key(n) < 2 ** 31 for n in names)


def test_every_spark_path_takes_its_lock(tmp_path, monkeypatch):
    """process_incremental·process_window·run_stage_inline·run_silver_reconcile 이 모두 잠금 안에서 돈다."""
    taken = []

    from contextlib import contextmanager

    @contextmanager
    def spy(name, **kw):
        taken.append(name)
        yield
    monkeypatch.setattr(J, "job_lock", spy)
    from datetime import date
    d = date(2026, 11, 4)

    def stage(d0, d1, sd, **kw):
        sd.mkdir(parents=True)
        return {"date_from": d0.isoformat(), "date_to": d1.isoformat(), "created_at": "2099-01-01T00:00:00+00:00",
                "audit_available": True, "row_counts": {"traffic_speed": 1}, "files_until": 10.0}
    J.process_window(d, d, dag_id="b", run_id="r", base=tmp_path / "w", stage=stage, load=lambda *a, **k: {}, verify=lambda *a: {})
    J.process_incremental(d, d, dag_id="g", run_id="r", base=tmp_path / "i", stage=stage, load=lambda *a, **k: {},
                          verify=lambda *a: {}, wall=lambda: J.kst_midnight_epoch(d) + 60)

    class P:
        returncode, stdout, stderr = 0, "{}", ""
    (tmp_path / "s").mkdir()
    with pytest.raises(J.JobError):        # manifest 가 없어서 실패하지만 잠금은 먼저 잡는다
        J.run_stage_inline(d, d, tmp_path / "s" / "a__b__try1", runner=lambda *a, **k: P())
    with pytest.raises(Exception):
        dq_jobs.run_silver_reconcile(mode="inline", runner=lambda *a, **k: P(), connect=lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    assert taken == [J.LOCK_WINDOW, J.LOCK_INCREMENTAL, J.LOCK_INLINE, J.LOCK_RECONCILE]


def test_dq_block_fails_fast_outside_airflow_as_a_job_error():
    e = dq_jobs.fail_fast("막힘")
    assert isinstance(e, (dq_jobs.DqBlocked, Exception))
    try:
        import airflow  # noqa: F401
    except ImportError:
        assert isinstance(e, dq_jobs.DqBlocked) and isinstance(e, J.JobError)
    else:
        assert type(e).__name__ == "AirflowFailException"


def test_timeout_message_keeps_the_last_progress_lines():
    import subprocess

    def boom(*a, **k):
        raise subprocess.TimeoutExpired(a[0], 5, stderr=b"[  10.0s] traffic_speed: read\n[1790.0s] weather: compare\n")
    with pytest.raises(J.JobError, match=r"1790\.0s\] weather"):
        dq_jobs.run_silver_reconcile(mode="inline", lock=J.no_lock, runner=boom, timeout_sec=5)
