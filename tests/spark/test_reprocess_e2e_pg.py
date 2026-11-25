"""재처리 끝에서 끝 (STEP 1): Bronze sink → 스트리밍 Silver → 평소 Gold → 재처리 DAG 함수(process_reprocess_day) → Silver 게시 → Gold 범위 교체 → 마트 → DQ.
설계안 5-3 의 P1(2회 동일, Gold) · P2(트랜잭션 실패 주입) · P3(정정 반영) · P8(끝에서 끝). GOLD_TEST_DSN 이 없으면 건너뛴다.

운영에서는 Spark 단계가 gold-runner 에서 돌지만, 여기서는 같은 함수를 이 프로세스의 Spark 로 부른다(build·stage 인자). 나머지(게시, 적재, 검증,
마트, 상태 기록)는 운영 코드 그대로다."""
from __future__ import annotations

import functools
import json
import os
from datetime import date, timedelta
from pathlib import Path

import pytest

from pg_support import pg  # noqa: F401

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from batch import gold_load as L  # noqa: E402
from batch import gold_stage as G  # noqa: E402
from batch import jobs as J  # noqa: E402
from batch import reprocess as RP  # noqa: E402
from batch import silver_rebuild as SR  # noqa: E402
from batch.gold_config import GoldConfig  # noqa: E402
from streaming import silver as S  # noqa: E402
from streaming import silver_pointer as P  # noqa: E402

from test_silver import at  # noqa: E402
from test_silver_rebuild import D, bronze_df, feed, pos, setup, stream_silver, traffic  # noqa: E402

pytestmark = pytest.mark.pg
FACTS = {"fact.traffic_speed": "event_date", "fact.bus_position": "event_date", "fact.bus_arrival": "event_date",
         "fact.incident_observation": "observed_date", "fact.weather_obs": "event_date"}


def audit_rows(n):
    return [(json.dumps({"event": "call", "source_system": "seoul_incident", "target": "seoul_incident", "final": True, "outcome": "ok",
                         "ingest_id": f"call{i}", "fetched_at": at(600 * i), "http_status": 200, "latency_ms": 40,
                         "status_kind": "ok", "n_records": 1, "truncated": False}),) for i in range(n)]


@pytest.fixture
def world(spark, tmp_path, pg, monkeypatch):
    """5개 소스의 Bronze(sink) + 스트리밍 Silver + audit, 그리고 평소 경로(upsert)로 쌓은 Gold."""
    cfg, rb = setup(tmp_path)
    monkeypatch.setenv("GOLD_SILVER_REBUILD_ROOT", str(rb))
    monkeypatch.delenv("GOLD_STAGE_MODE", raising=False)
    monkeypatch.delenv(SR.FAIL_ENV, raising=False)
    srcs = {
        "seoul_traffic": traffic([30, 0, 25, 0, 40, 33]),
        "bus_position": pos(0, 12),
        "bus_arrival": [{"dedup_key": f"a{i}", "event_time": at(60 * i), "fetched_at": at(60 * i + 4), "ingest_id": f"ar{i}",
                         "payload": {"arrive_in_sec": 120 - 10 * i}} for i in range(6)],
        "kma_ncst": [{"dedup_key": f"w{i}", "event_time": at(3600 * i), "fetched_at": at(3600 * i + 60), "ingest_id": f"wr{i}"}
                     for i in range(3)],
        "seoul_incident": [{"dedup_key": "acc-1", "ingest_id": f"call{i}", "event_time": at(0, day=1), "fetched_at": at(600 * i + 5),
                            "payload": {"occurred_at": at(0, day=1), "expected_clear_at": at(30 * 86400), "tm_x": 198000.0,
                                        "tm_y": 451500.0}} for i in range(4)],
    }
    for s, rows in srcs.items():
        feed(spark, tmp_path, s, bronze_df(spark, s, rows))
        stream_silver(spark, cfg, s)
    spark.createDataFrame(audit_rows(6), "_raw_value string").write.parquet(f"{cfg.bronze_root}/audit")
    dsn = os.environ["GOLD_TEST_DSN"]

    def connect():
        import psycopg
        return psycopg.connect(dsn, autocommit=True)

    def gold_cfg(stage_root, d0=D, d1=D):
        return GoldConfig(silver_root=cfg.silver_root, bronze_root=cfg.bronze_root, stage_root=str(stage_root), rebuild_root=str(rb),
                          date_from=d0, date_to=d1)
    m = G.run(spark, gold_cfg(tmp_path / "first_stage"))
    assert m["silver_pointer_sha"] == "none"                                   # 포인터가 없으면 예전과 같다
    L.load_all(pg, tmp_path / "first_stage", dag_id="gold_daily")
    return {"cfg": cfg, "rb": rb, "connect": connect, "gold_cfg": gold_cfg, "tmp": tmp_path, "pg": pg, "spark": spark}


def run_day(w, run_id, tables="all", **kw):
    spark, cfg, rb, connect = w["spark"], w["cfg"], w["rb"], w["connect"]

    def build(d0, d1, sd, *, tables, version, mode, allow_shrink):
        rep = SR.build(spark, cfg, tables, [d0], rebuild_root=str(rb), version=version, mode=mode, allow_shrink=allow_shrink)
        if not rep["ok"]:
            raise J.JobError(rep["error"])
        return rep

    def stage(d0, d1, sd):
        return G.run(spark, w["gold_cfg"](sd, d0, d1))
    return RP.process_reprocess_day(
        D, tables=RP.parse_tables(tables), reason="시험: 규칙 변경", dag_id="reprocess", run_id=run_id, base=w["tmp"] / "gold_stage",
        connect=connect, build=build, stage=stage, load=functools.partial(J.run_load, connect=connect),
        verify=functools.partial(J.verify_counts, connect=connect), marts=functools.partial(RP.rebuild_marts, connect=connect), **kw)


def gold_fingerprint(pg, d0=D, d1=D):
    out = {}
    for t, col in FACTS.items():
        n, h = pg.execute(f"SELECT count(*), md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) FROM {t} x "
                          f"WHERE {col} BETWEEN %s AND %s", (d0, d1)).fetchone()
        out[t] = (n, h)
    return out


def silver_fingerprint(w):
    from streaming.silver_read import read_silver
    from test_silver_rebuild import rows_of
    out = {}
    for name, part in G.SILVER_TABLES.items():
        df = read_silver(w["spark"], w["cfg"].silver_root, str(w["rb"]), name, part)
        out[name] = rows_of(df.filter(F.col(part) == F.lit(D)))
    return out


# ------------------------------------------------------------------ P1 · P8

def test_reprocess_twice_gives_identical_silver_and_gold_and_passes_reconcile_and_dq(world):
    pg = world["pg"]
    gold0, silver0 = gold_fingerprint(pg), silver_fingerprint(world)
    r1 = run_day(world, "run1")
    assert r1["status"] == "ok" and r1["silver_version"].startswith("r") and r1["silver_version"].endswith("t1")
    gold1, silver1 = gold_fingerprint(pg), silver_fingerprint(world)
    r2 = run_day(world, "run2")
    gold2, silver2 = gold_fingerprint(pg), silver_fingerprint(world)
    assert r2["silver_version"] != r1["silver_version"]
    assert silver1 == silver2 and gold1 == gold2                              # 같은 범위 2회 재처리: 행 수와 값이 같다
    assert silver1 == silver0 and gold1 == gold0                              # 규칙이 같으면 재처리 전과도 같다
    # 기록
    rows = pg.execute("SELECT reprocess_id, status, silver_version, previous_version, load_run_id, row_counts FROM ops.reprocess_run "
                      "ORDER BY reprocess_id").fetchall()
    assert [r[1] for r in rows] == ["ok", "ok"]
    assert rows[1][3]["silver_traffic_speed"] == {D.isoformat(): r1["silver_version"]}       # 두 번째 게시 전 = 첫 번째 버전
    lr = pg.execute("SELECT status, row_counts->>'_mode' FROM ops.load_run WHERE run_id=%s", (rows[1][4],)).fetchone()
    assert lr == ("ok", "replace")
    assert rows[1][5]["silver_traffic_speed"]["changed"] == 0 and "_marts" in rows[1][5]
    # 재처리 뒤 Silver 대조(silver_verify 와 같은 등식) — 각 표의 in-job 대조가 이미 0 이어야 게시된다
    assert all(rows[1][5][t]["missing"] == 0 and rows[1][5][t]["extra"] == 0 for t in G.SILVER_TABLES)
    # 재처리 뒤 DQ: 마트를 막는 규칙(미래 시각) 통과, fail 이 재처리 전보다 늘지 않음
    from urbanstream.dq import engine, rules
    rs = rules.load(None)
    after = {r.rule.id: r.result.status for r in engine.run_rules(pg, rs, D, today=D + timedelta(days=1))}
    assert all(after[i] == "pass" for i in ("traffic_future_event", "bus_position_future_event", "bus_arrival_future_event")), after
    assert engine.blocking_failures(pg) == []


# ------------------------------------------------------------------ P3: 정정 반영

def test_rule_change_reaches_gold_and_removed_rows_disappear(world, monkeypatch):
    pg = world["pg"]
    assert pg.execute("SELECT count(*) FROM fact.traffic_speed WHERE speed_kmh = 0").fetchone()[0] == 2
    orig = S.transform_traffic
    monkeypatch.setattr(S, "transform_traffic", lambda b: orig(b).filter(F.col("speed_kmh") != 0))
    with pytest.raises(J.JobError, match="shrink_blocked|키 .* < 현재"):
        run_day(world, "blocked", tables="silver_traffic_speed")
    st = pg.execute("SELECT status, error FROM ops.reprocess_run").fetchone()
    assert st[0] == "failed" and st[1]
    assert P.active_dates(world["rb"], "silver_traffic_speed") == {}          # 게시되지 않았다
    r = run_day(world, "allowed", tables="silver_traffic_speed", allow_shrink=True)
    assert r["status"] == "ok"
    assert pg.execute("SELECT count(*) FROM fact.traffic_speed WHERE speed_kmh = 0").fetchone()[0] == 0
    assert pg.execute("SELECT count(*) FROM fact.traffic_speed WHERE event_date=%s", (D,)).fetchone()[0] == 4
    # 교체 대상이 아닌 표는 그대로(upsert)
    assert pg.execute("SELECT count(*) FROM fact.bus_position").fetchone()[0] == 12
    assert P.active_dates(world["rb"], "silver_bus_position") == {}


# ------------------------------------------------------------------ P2: 트랜잭션 실패 주입 · 게시 뒤 재시도

def test_gold_failure_rolls_back_whole_day_and_retry_resumes_after_publish(world, monkeypatch):
    pg = world["pg"]
    orig = S.transform_traffic
    monkeypatch.setattr(S, "transform_traffic", lambda b: orig(b).withColumn("dq_flags", F.array(F.lit("rule_v2"))))
    before = gold_fingerprint(pg)
    real = L.load_table
    calls = {"n": 0}

    def flaky(conn, spec, stage, *, conflict=True):
        calls["n"] += 1
        if spec.name == "bus_arrival":                                         # 세 번째 표에서 죽는다 (앞 두 표는 이미 DELETE+INSERT)
            raise RuntimeError("주입된 실패")
        return real(conn, spec, stage, conflict=conflict)
    monkeypatch.setattr(L, "load_table", flaky)
    with pytest.raises(RuntimeError, match="주입된 실패"):
        run_day(world, "run1")
    assert gold_fingerprint(pg) == before                                     # 앞 표의 DELETE 까지 되돌아갔다
    st, err, ver = pg.execute("SELECT status, error, silver_version FROM ops.reprocess_run").fetchone()
    assert st == "silver_published" and "주입된 실패" in err                    # Silver 는 이미 새 버전
    assert pg.execute("SELECT status FROM ops.load_run ORDER BY run_id DESC LIMIT 1").fetchone()[0] == "failed"
    assert P.active_dates(world["rb"], "silver_traffic_speed") == {D.isoformat(): ver}
    monkeypatch.setattr(L, "load_table", real)
    r = run_day(world, "run1", try_number=2)                                  # Airflow 재시도: 같은 실행
    assert r["status"] == "ok" and r["silver_version"] == ver                  # 새 버전을 만들지 않고 Gold 부터 이어 간다
    assert pg.execute("SELECT count(*) FROM ops.reprocess_run").fetchone()[0] == 1
    flags = {tuple(r[0]) for r in pg.execute("SELECT dq_flags FROM fact.traffic_speed WHERE event_date=%s", (D,)).fetchall()}
    assert flags == {("rule_v2",)}


def test_rollback_restores_previous_silver_and_gold_follows_with_scope_gold(world, monkeypatch):
    pg = world["pg"]
    before = gold_fingerprint(pg)
    orig = S.transform_traffic
    monkeypatch.setattr(S, "transform_traffic", lambda b: orig(b).withColumn("dq_flags", F.array(F.lit("rule_v2"))))
    r = run_day(world, "run1", tables="silver_traffic_speed")
    assert gold_fingerprint(pg) != before
    monkeypatch.setattr(S, "transform_traffic", orig)
    # 되돌리기 (런북 절차): 파일 → DB 기록 → Gold 를 지금 Silver 로 범위 교체
    rid = r["reprocess_id"]
    h = P.find_publish(world["rb"], "silver_traffic_speed", rid)
    SR.rollback_all(str(world["rb"]), {"silver_traffic_speed": h["previous"]}, expect_version=h["version"], reprocess_id=rid)
    RP.set_status(pg, rid, "rolled_back")
    assert P.active_dates(world["rb"], "silver_traffic_speed") == {}
    g = run_day(world, "undo", tables="silver_traffic_speed", scope="gold")
    assert g["status"] == "ok" and gold_fingerprint(pg) == before
