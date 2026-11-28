"""Step 8a: sql/009 + PgStore + Notifier 를 실제 Postgres(역할 포함)에서 확인한다."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from urbanstream.notify import Decision, Notifier
from urbanstream.notify.policy import from_mapping
from urbanstream.notify.store import PgStore
from urbanstream.notify.transport import SendResult

pytestmark = pytest.mark.pg
T0 = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)
URL = "https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz0123456789"


def store_of(pg):
    class C:                                              # 닫지 않는 감싸개 (PgStore 는 `with` 로 쓴다)
        def __enter__(self): return pg
        def __exit__(self, *a): return False
    return PgStore("", connect=lambda: C())


class Clk:
    t = T0

    def __call__(self): return self.t


def notifier(pg, calls):
    def post(kind, url, text, **kw):
        calls.append(text)
        return SendResult(True, 204, "")
    return Notifier(store_of(pg), from_mapping(None), enabled=True, kind="discord", url=URL, clock=Clk(), post=post)


def test_notifier_on_real_tables_dedup_pending_digest(pg):
    calls: list[str] = []
    n = notifier(pg, calls)
    n.emit("error", "소스 down password=hunter2", f"{URL}", "src:a")
    n.emit("error", "소스 down", "", "src:a")
    assert len(calls) == 1
    row = pg.execute("SELECT status, count, suppressed_count FROM ops.alert_state WHERE dedup_key='src:a'").fetchone()
    assert row == ("open", 2, 1)
    blob = " ".join(str(r) for r in pg.execute("SELECT title, body, reason FROM ops.alert_event").fetchall())
    assert "hunter2" not in blob and "abcdefghij" not in blob

    n.enqueue("error", "dag 실패", "", "fail:x", meta={"dag_id": "x", "try_number": 1})
    n.clock.t = T0 + timedelta(seconds=61)
    assert n.process_pending(lambda r: Decision("warning", "host_stall:d", "host_stall")) == 1
    assert pg.execute("SELECT level, original_level, status, meta->>'dag_id' FROM ops.alert_event WHERE dedup_key='host_stall:d'").fetchone() == ("warning", "error", "digest", "x")

    assert n.send_digest() is True
    assert pg.execute("SELECT count(*) FROM ops.alert_event WHERE status='digested'").fetchone()[0] == 1
    assert n.resolve("src:a", "소스 down") is True
    assert pg.execute("SELECT status FROM ops.alert_state WHERE dedup_key='src:a'").fetchone()[0] == "resolved"


def test_empty_tables_views_return_no_rows_not_errors(pg):
    for v in ("v_source_status", "v_layer_status", "v_load_recent", "v_alert_recent", "v_data_gap_recent", "v_mart_freshness", "v_pipeline_health"):
        pg.execute(f"SELECT count(*) FROM ops.{v}").fetchone()
    s = store_of(pg)
    assert s.get_state("none") is None and s.pending(T0) == [] and s.digest_pending() == []
    assert s.counts_since_last_push() == {"failed": 0, "rate_limited": 0}


def test_gold_writer_can_write_new_tables(pg):
    pg.execute("SET ROLE gold_writer")
    try:
        s = store_of(pg)
        eid = s.add_event(created_at=T0, level="info", original_level=None, title="t", body="", dedup_key=None, status="pending", reason=None, sent_at=None, meta={})
        s.update_event(eid, status="digest")
        s.save_state("k", level="error", title="t", status="open", first_at=T0, last_at=T0, count=1, suppressed_count=0)
        pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', %s, %s, 3600)", (T0, T0 + timedelta(hours=1)))
        pg.execute("INSERT INTO ops.monitor_state (component) VALUES ('monitor') ON CONFLICT DO NOTHING")
        pg.execute("INSERT INTO ops.call_event (kafka_partition, kafka_offset, recorded_at, event, source_system) VALUES (0, 1, %s, 'call', 'x')", (T0,))
    finally:
        pg.execute("RESET ROLE")


@pytest.mark.parametrize("role", ["api_reader", "bi_reader"])
def test_readers_see_views_only_never_raw_tables(pg, role):
    pg.execute("INSERT INTO ops.alert_event (created_at, level, title, body, status) VALUES (now(), 'error', 't', 'SECRET-BODY', 'sent')")
    pg.execute(f"SET ROLE {role}")
    try:
        for v in ("v_source_status", "v_layer_status", "v_load_recent", "v_alert_recent", "v_data_gap_recent", "v_mart_freshness", "v_pipeline_health"):
            pg.execute(f"SELECT count(*) FROM ops.{v}").fetchone()
        assert "body" not in [d.name for d in pg.execute("SELECT * FROM ops.v_alert_recent").description]
        for raw in ("alert_event", "alert_state", "call_event", "monitor_state", "data_gap", "source_status", "layer_status", "source_health"):
            with pytest.raises(Exception):
                pg.execute(f"SELECT 1 FROM ops.{raw} LIMIT 1")
            pg.execute("RESET ROLE"); pg.execute(f"SET ROLE {role}")           # 오류 뒤 상태 정리
        with pytest.raises(Exception):
            pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec) VALUES ('host_stall', now(), now(), 0)")
    finally:
        pg.execute("RESET ROLE")


def test_source_status_check_rejects_unknown_value(pg):
    with pytest.raises(Exception):
        pg.execute("INSERT INTO ops.source_status (source_system, status) VALUES ('x', 'bogus')")


def test_load_all_records_dag_id_and_keeps_job_name(pg, tmp_path):
    """009 의 dag_id 열: job='gold_load' 는 그대로여야 마트 선행조건(require_recent_load)이 계속 통과한다."""
    import json
    from batch import gold_load as GL
    (tmp_path / "manifest.json").write_text(json.dumps({"date_from": "2026-11-04", "date_to": "2026-11-04"}))
    r1 = GL.load_all(pg, tmp_path, specs=(), dag_id="gold_intraday")
    r2 = GL.load_all(pg, tmp_path, specs=())                                  # 옛 호출 모양 (dag_id 없음)
    rows = pg.execute("SELECT job, dag_id, status FROM ops.load_run ORDER BY run_id").fetchall()
    assert rows == [("gold_load", "gold_intraday", "ok"), ("gold_load", None, "ok")]
    assert r1["run_id"] != r2["run_id"]
    assert pg.execute("SELECT count(*) FROM ops.v_load_recent").fetchone()[0] == 2


def test_prune_removes_old_monitor_rows_as_gold_writer_and_keeps_open_alerts(pg):
    from batch import maintenance as MT
    pg.execute("INSERT INTO ops.call_event (kafka_partition, kafka_offset, recorded_at, event) VALUES (0,1, now()-interval '40 days','call'),(0,2, now(),'call')")
    pg.execute("INSERT INTO ops.source_health (source_system, bucket_start, n_calls, n_ok, n_failed, n_skipped, n_retry) VALUES ('s', now()-interval '100 days',1,1,0,0,0),('s', now(),1,1,0,0,0)")
    pg.execute("INSERT INTO ops.alert_event (created_at, level, title, status) VALUES (now()-interval '200 days','info','old','digested'),(now(),'info','new','digest')")
    pg.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec, detected_at) VALUES ('host_stall', now()-interval '201 days', now()-interval '200 days', 86400, now()-interval '200 days')")
    pg.execute("INSERT INTO ops.alert_state (dedup_key, level, title, status, first_at, last_at) VALUES "
               "('open','error','t','open', now()-interval '300 days', now()-interval '300 days'),('done','error','t','resolved', now()-interval '300 days', now()-interval '300 days')")

    class C:
        def __enter__(s): return pg
        def __exit__(s, *a): return False
    pg.execute("SET ROLE gold_writer")
    try:
        out = MT.prune_ops(connect=lambda: C())
    finally:
        pg.execute("RESET ROLE")
    assert (out["call_event"], out["source_health"], out["alert_event"], out["data_gap"], out["alert_state"]) == (1, 1, 1, 1, 1)
    assert pg.execute("SELECT dedup_key FROM ops.alert_state").fetchall() == [("open",)]        # 열린 알림 상태는 오래돼도 남는다


def test_webhook_5xx_is_recorded_failed_and_carried_into_the_next_success(pg):
    """R13 (STEP 3B): 웹훅이 5xx 를 주면 예외 없이 status='failed' 로 남고, 다음 전송이 성공할 때 '이전 전달 실패 N건' 이 붙는다.
    (알림 경로가 끊긴 동안 알림이 사라지지 않았다는 것을 사람이 알 수 있게 한다.) 웹훅 주소는 본문에도 남지 않는다."""
    calls: list[str] = []
    ok = {"v": False}

    def post(kind, url, text, **kw):
        calls.append(text)
        return SendResult(True, 204, "") if ok["v"] else SendResult(False, 503, f"503 from {URL}")

    n = Notifier(store_of(pg), from_mapping(None), enabled=True, kind="discord", url=URL, clock=Clk(), post=post)
    n.emit("error", "끊긴 동안 1", "", "r13:a")
    n.emit("error", "끊긴 동안 2", "", "r13:b")
    rows = pg.execute("SELECT status, reason FROM ops.alert_event WHERE dedup_key LIKE 'r13:%' ORDER BY alert_id").fetchall()
    assert [r[0] for r in rows] == ["failed", "failed"]
    assert all("abcdefghij" not in (r[1] or "") for r in rows), "실패 사유에 웹훅 주소가 남으면 안 된다"
    ok["v"] = True
    n.emit("error", "다시 연결됨", "", "r13:c")
    assert "이전 전달 실패 2건" in calls[-1]
    n.emit("error", "그다음", "", "r13:d")
    assert "이전 전달 실패" not in calls[-1], "한 번 알린 실패는 다시 세지 않는다"
