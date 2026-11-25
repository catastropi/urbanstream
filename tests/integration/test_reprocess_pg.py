"""STEP 1 재처리의 DB 쪽 (Spark 없이): 마이그레이션 015, ops.reprocess_run 상태 전이, gold_load 범위 교체의 가드·잠금·범위 밖 불변.
설계안 5-3 의 P4~P7 과 5-1 의 U7. 스테이징 Parquet 은 pyarrow 로 직접 만든다. Spark 가 만든 스테이징으로 하는 시험은 tests/spark/test_reprocess_e2e_pg.py."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pg_support import pg  # noqa: F401
from batch import gold_load as L
from batch import reprocess as RP
from batch.jobs import JobError
from scripts.migrate import discover, migrate

pytestmark = pytest.mark.pg
ROOT = Path(__file__).resolve().parents[2]
D = date(2026, 11, 3)
T0 = datetime(2026, 11, 3, 3, 0, tzinfo=timezone.utc)


def stage_traffic(stage: Path, rows: list[tuple], d0: date = D, d1: date = D) -> Path:
    """rows: (dedup_key, speed, event_date). 다른 표는 비워 둔다."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    stage.mkdir(parents=True, exist_ok=True)
    if rows:
        t = pa.table({
            "dedup_key": [r[0] for r in rows], "link_id": ["L1"] * len(rows),
            "event_time": pa.array([T0 + timedelta(minutes=i) for i in range(len(rows))], pa.timestamp("us", tz="UTC")),
            "fetched_at": pa.array([T0 + timedelta(minutes=i, seconds=5) for i in range(len(rows))], pa.timestamp("us", tz="UTC")),
            "speed_kmh": [float(r[1]) for r in rows], "travel_time_sec": pa.array([100] * len(rows), pa.int32()),
            "dq_flags": pa.array([[] for _ in rows], pa.list_(pa.string())), "event_date": pa.array([r[2] for r in rows], pa.date32()),
        })
        (stage / "traffic_speed").mkdir(exist_ok=True)
        pq.write_table(t, stage / "traffic_speed" / "part-0.parquet")
    (stage / "manifest.json").write_text(json.dumps({"date_from": d0.isoformat(), "date_to": d1.isoformat(),
                                                     "row_counts": {"traffic_speed": len(rows)}, "audit_available": True}))
    return stage


def traffic_rows(conn, d0=None, d1=None):
    q = "SELECT dedup_key, speed_kmh, event_date FROM fact.traffic_speed"
    if d0:
        return conn.execute(q + " WHERE event_date BETWEEN %s AND %s ORDER BY dedup_key", (d0, d1)).fetchall()
    return conn.execute(q + " ORDER BY dedup_key").fetchall()


# ------------------------------------------------------------------ P7: 마이그레이션 015

def test_015_applies_on_top_of_001_to_014_and_on_empty_db(pg, tmp_path):
    assert pg.execute("SELECT count(*) FROM ops.schema_migrations WHERE name='015_reprocess_run.sql'").fetchone()[0] == 1
    pg.execute("DROP SCHEMA IF EXISTS dim, fact, mart, ops CASCADE")
    old = tmp_path / "sql"
    old.mkdir()
    for n, p, _ in discover(ROOT / "sql"):
        if n < "015":
            (old / n).write_text(p.read_text())
    assert migrate(pg, old)[-1] == "014_holidays_2026.sql"
    assert migrate(pg, ROOT / "sql") == ["015_reprocess_run.sql"]               # 운영 DB 처럼 014 까지 있는 상태에서 015 만
    assert migrate(pg, ROOT / "sql") == []
    grants = {r[0] for r in pg.execute("SELECT privilege_type FROM information_schema.role_table_grants "
                                       "WHERE table_schema='ops' AND table_name='reprocess_run' AND grantee='gold_writer'")}
    assert grants == {"SELECT", "INSERT", "UPDATE"}                            # DELETE 는 주지 않는다 (이력은 지우지 않음)


def _insert(conn, key, d0, d1, status="running"):
    conn.execute("INSERT INTO ops.reprocess_run (request_key, scope, tables, date_from, date_to, reason, status) "
                 "VALUES (%s, 'silver', '{silver_weather}', %s, %s, 't', %s)", (key, d0, d1, status))


def test_unfinished_reprocess_runs_cannot_overlap(pg):
    import psycopg
    _insert(pg, "a", date(2026, 11, 10), date(2026, 11, 12))
    with pytest.raises(psycopg.errors.ExclusionViolation):
        _insert(pg, "b", date(2026, 11, 12), date(2026, 11, 14))
    _insert(pg, "c", date(2026, 11, 13), date(2026, 11, 14))                  # 안 겹치면 된다
    _insert(pg, "d", date(2026, 11, 10), date(2026, 11, 10), status="ok")     # 끝난 행은 막지 않는다
    with pytest.raises(psycopg.errors.CheckViolation):
        pg.execute("INSERT INTO ops.reprocess_run (request_key, scope, tables, date_from, date_to, reason) "
                   "VALUES ('e', 'silver', '{x}', '2026-11-20', '2026-11-20', '  ')")   # 사유 없음
    assert pg.execute("SELECT count(*) FROM ops.v_reprocess_recent").fetchone()[0] == 3


# ------------------------------------------------------------------ U7: 상태 전이 · 재시도

def test_status_transitions_and_retry_reuse_the_same_row(pg):
    kw = dict(scope="silver_gold", tables=["silver_weather"], d0=D, d1=D, reason="규칙 변경", dag_id="reprocess", run_id="r1")
    row = RP.start_run(pg, "k1", try_number=1, **kw)
    rid = row["reprocess_id"]
    assert row["status"] == "running"
    with pytest.raises(JobError, match="허용되지 않는"):
        RP.set_status(pg, rid, "ok")                                            # running → ok 는 건너뛰기
    RP.set_status(pg, rid, "silver_built", silver_version="r0001t1", row_counts={"silver_weather": {"added": 1}})
    RP.set_status(pg, rid, "failed", error="boom")
    again = RP.start_run(pg, "k1", try_number=2, **kw)                         # Airflow 재시도: 같은 행을 running 으로
    assert again["reprocess_id"] == rid and again["status"] == "running" and again["try_number"] == 2
    RP.set_status(pg, rid, "silver_built", silver_version="r0001t2", row_counts={"x": 1})
    RP.set_status(pg, rid, "silver_published", previous_version={"silver_weather": {"2026-11-03": None}})
    resumed = RP.start_run(pg, "k1", try_number=3, **kw)                       # 게시 뒤 재시도: 상태를 유지하고 이어 간다
    assert resumed["status"] == "silver_published" and resumed["silver_version"] == "r0001t2"
    RP.set_status(pg, rid, "gold_loaded", load_run_id=5)
    RP.set_status(pg, rid, "ok")
    st, counts, prev, fin, err = pg.execute("SELECT status, row_counts, previous_version, finished_at, error FROM ops.reprocess_run "
                                            "WHERE reprocess_id=%s", (rid,)).fetchone()
    assert st == "ok" and counts == {"silver_weather": {"added": 1}, "x": 1} and prev == {"silver_weather": {"2026-11-03": None}}
    assert fin is not None and err is None
    RP.set_status(pg, rid, "rolled_back")
    with pytest.raises(JobError, match="되돌린"):
        RP.start_run(pg, "k1", try_number=4, **kw)


def test_mark_stale_fails_only_pre_publish_rows(pg):
    for i, st in enumerate(("running", "silver_built", "silver_published", "gold_loaded")):
        _insert(pg, f"s{i}", date(2026, 11, 1) + timedelta(days=i), date(2026, 11, 1) + timedelta(days=i), status=st)
    pg.execute("UPDATE ops.reprocess_run SET updated_at = now() - interval '5 hours'")
    out = RP.mark_stale(pg, older_than_minutes=180)
    assert out["failed"] == 2 and len(out["needs_attention"]) == 2
    assert sorted(r[0] for r in pg.execute("SELECT status FROM ops.reprocess_run")) == ["failed", "failed", "gold_loaded", "silver_published"]


# ------------------------------------------------------------------ P4 · P6: 범위 교체 가드, 범위 밖 불변

def test_replace_deletes_only_the_range_and_removes_rows_missing_from_stage(pg, tmp_path):
    L.load_all(pg, stage_traffic(tmp_path / "s0", [("a", 10, D - timedelta(days=1)), ("b", 20, D), ("c", 30, D), ("d", 40, D + timedelta(days=1))],
                                 D - timedelta(days=1), D + timedelta(days=1)))
    res = L.load_all(pg, stage_traffic(tmp_path / "s1", [("b", 21, D)]), mode="replace", replace={"traffic_speed"}, d0=D, d1=D)
    assert res["deleted"] == {"traffic_speed": 2} and res["row_counts"]["traffic_speed"] == 1
    assert traffic_rows(pg) == [("a", 10.0, D - timedelta(days=1)), ("b", 21.0, D), ("d", 40.0, D + timedelta(days=1))]
    # 같은 입력을 평소처럼(upsert) 넣으면 사라져야 할 c 가 남고 값도 안 바뀐다 → 재처리에 범위 교체가 필요한 이유 (결정 (b))
    L.load_all(pg, stage_traffic(tmp_path / "s2", [("b", 20, D), ("c", 30, D)]), mode="replace", replace={"traffic_speed"}, d0=D, d1=D)
    L.load_all(pg, stage_traffic(tmp_path / "s3", [("b", 21, D)]))
    assert [r[:2] for r in traffic_rows(pg, D, D)] == [("b", 20.0), ("c", 30.0)]
    assert pg.execute("SELECT row_counts->>'_mode' FROM ops.load_run WHERE run_id=%s", (res["run_id"],)).fetchone()[0] == "replace"


def test_replace_guards(pg, tmp_path):
    L.load_all(pg, stage_traffic(tmp_path / "s0", [("b", 20, D)]))
    with pytest.raises(L.ReplaceGuardError, match="0행인데"):
        L.load_all(pg, stage_traffic(tmp_path / "empty", []), mode="replace", replace={"traffic_speed"}, d0=D, d1=D)
    with pytest.raises(L.ReplaceGuardError, match="범위"):
        L.load_all(pg, stage_traffic(tmp_path / "wide", [("b", 20, D)], D, D + timedelta(days=1)), mode="replace", d0=D, d1=D)
    with pytest.raises(L.ReplaceGuardError, match="할 수 없는 표"):
        L.load_all(pg, stage_traffic(tmp_path / "s1", [("b", 20, D)]), mode="replace", replace={"call_audit"}, d0=D, d1=D)
    assert traffic_rows(pg) == [("b", 20.0, D)]
    assert [r[0] for r in pg.execute("SELECT status FROM ops.load_run ORDER BY run_id")] == ["ok", "failed", "failed", "failed"]
    L.load_all(pg, stage_traffic(tmp_path / "empty2", []), mode="replace", replace={"traffic_speed"}, d0=D, d1=D, allow_empty=True)
    assert traffic_rows(pg) == []                                              # 의도한 비우기만 허용


def test_key_outside_the_range_makes_replace_fail_loudly_and_roll_back(pg, tmp_path):
    L.load_all(pg, stage_traffic(tmp_path / "s0", [("x", 5, D - timedelta(days=1)), ("b", 20, D)], D - timedelta(days=1), D))
    with pytest.raises(Exception, match="duplicate key|unique"):
        L.load_all(pg, stage_traffic(tmp_path / "s1", [("x", 6, D), ("b", 21, D)]), mode="replace", replace={"traffic_speed"}, d0=D, d1=D)
    assert traffic_rows(pg) == [("b", 20.0, D), ("x", 5.0, D - timedelta(days=1))]     # DELETE 도 되돌아감


# ------------------------------------------------------------------ P5: advisory lock

def test_loads_wait_for_each_other(pg, tmp_path):
    import psycopg
    stage = stage_traffic(tmp_path / "s", [("b", 20, D)])
    dsn = os.environ["GOLD_TEST_DSN"]
    holder = psycopg.connect(dsn, autocommit=True)
    try:
        with holder.transaction():
            holder.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (L.LOCK_KEY,))   # 다른 적재가 잠금을 잡고 있다
            done = threading.Event()

            def other():
                with psycopg.connect(dsn, autocommit=True) as c:
                    L.load_all(c, stage, mode="replace", replace={"traffic_speed"}, d0=D, d1=D, allow_empty=True)
                done.set()
            th = threading.Thread(target=other, daemon=True)
            th.start()
            time.sleep(1.5)
            assert not done.is_set(), "잠금을 기다리지 않았다"
        th.join(30)
        assert done.is_set()
    finally:
        holder.close()
    assert traffic_rows(pg) == [("b", 20.0, D)]
