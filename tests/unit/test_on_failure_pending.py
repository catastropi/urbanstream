import logging
from datetime import datetime, timezone

import pytest

from batch import jobs as J
from urbanstream import notify
from urbanstream.notify import Notifier
from urbanstream.notify.policy import from_mapping
from urbanstream.notify.store import MemoryStore


class TI:
    dag_id, task_id, try_number, run_id = "gold_daily", "load", 2, "scheduled__x"
    start_date = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)
    end_date = datetime(2026, 11, 5, 3, 1, tzinfo=timezone.utc)


@pytest.fixture
def store():
    st = MemoryStore()
    notify.set_notifier(Notifier(st, from_mapping(None), enabled=False))
    yield st
    notify.set_notifier(None)


def test_on_failure_records_pending_with_task_times_and_masks(store):
    J.on_failure({"ti": TI(), "run_id": "scheduled__x", "exception": RuntimeError("fail password=pw123 https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz0123456789")})
    [e] = store.events
    assert e["status"] == "pending" and e["level"] == "error" and e["dedup_key"] == "airflow:gold_daily.load"
    assert e["meta"]["dag_id"] == "gold_daily" and e["meta"]["started_at"].startswith("2026-11-05T03:00") and e["meta"]["ended_at"].startswith("2026-11-05T03:01")
    assert "pw123" not in e["body"] and "abcdefghij" not in e["body"]


def test_on_failure_falls_back_to_send_when_enqueue_fails(monkeypatch, caplog):
    class Boom:
        def enqueue(self, *a, **k): raise RuntimeError("db down")
        def emit(self, *a, **k): self.emitted = True
    b = Boom()
    notify.set_notifier(b)
    try:
        J.on_failure({"ti": TI(), "exception": RuntimeError("x")})
        assert getattr(b, "emitted", False)                      # 알림이 사라지지 않고 즉시 경로로 대체된다
    finally:
        notify.set_notifier(None)


def test_on_failure_never_raises_even_if_everything_breaks(monkeypatch):
    class Dead:
        def enqueue(self, *a, **k): raise RuntimeError("a")
        def emit(self, *a, **k): raise RuntimeError("b")
    notify.set_notifier(Dead())
    try:
        J.on_failure({"ti": TI()})
        J.on_failure({})
    finally:
        notify.set_notifier(None)


def test_run_load_passes_dag_id_only_when_given(tmp_path, monkeypatch):
    import batch.gold_load as GL
    seen = []
    monkeypatch.setattr(GL, "load_all", lambda conn, stage, refresh_mart=False, **kw: seen.append(kw) or {"run_id": 1})

    class C:
        def __enter__(s): return s
        def __exit__(s, *a): return False
    J.run_load(tmp_path, connect=lambda: C())
    J.run_load(tmp_path, connect=lambda: C(), dag_id="gold_intraday")
    assert seen == [{}, {"dag_id": "gold_intraday"}]


def test_process_window_passes_dag_id_to_load(tmp_path):
    from datetime import date
    from tests.unit.test_batch_jobs import _manifest
    got = {}
    d = date(2026, 11, 4)

    def stage(a, b, sd):
        sd.mkdir(parents=True, exist_ok=True)
        return _manifest(a, b)
    J.process_window(d, d, dag_id="gold_intraday", run_id="r", base=tmp_path, refresh_mart=False, stage=stage,
                     load=lambda sd, refresh_mart=False, dag_id=None: got.update(dag_id=dag_id) or {"run_id": 1},
                     verify=lambda *a: {})
    assert got["dag_id"] == "gold_intraday"
