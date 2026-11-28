"""batch/jobs.process_window 를 가짜 없이 끝까지 돌린다: 실제 서브프로세스(gold_stage, 비밀 없는 환경) → 실제 Spark → 실제 Postgres.
Airflow DAG 의 태스크가 부르는 바로 그 함수다. GOLD_TEST_DSN 과 Java/pyspark 가 있어야 돈다."""
from __future__ import annotations

import os
from datetime import date

import pytest

from pg_support import pg  # noqa: F401
from batch import jobs as J
from test_gold_e2e_pg import D, _audit, _write_silver

pytestmark = pytest.mark.pg


def test_process_window_end_to_end_is_idempotent_and_cleans_up(spark, tmp_path, pg, monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ["GOLD_TEST_DSN"]
    connect = lambda: psycopg.connect(dsn, autocommit=True)
    silver, bronze, base = tmp_path / "silver", tmp_path / "bronze", tmp_path / "stage"
    _write_silver(spark, silver)
    spark.createDataFrame([_audit("c0", 0), _audit("c1", 120), _audit("c2", 240), _audit("t1", 5, "seoul_traffic")],
                          "_raw_value string").write.parquet(str(bronze / "audit"))
    monkeypatch.setenv("GOLD_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GOLD_BRONZE_ROOT", str(bronze))
    monkeypatch.setenv("POSTGRES_PASSWORD", "must-not-leak")          # 서브프로세스 환경에 들어가면 안 되는 값
    monkeypatch.setenv("GOLD_DSN", dsn)                               # 작업 잠금(job_lock)도 실제 PG 로 잡는다 (STEP 3B)

    def go(run_id, try_number=1):
        return J.process_window(
            D, D, dag_id="gold_intraday", run_id=run_id, try_number=try_number, refresh_mart=True, base=base,
            load=lambda sd, refresh_mart=False, dag_id=None: J.run_load(sd, refresh_mart=refresh_mart, connect=connect, dag_id=dag_id),
            verify=lambda m, a, b: J.verify_counts(m, a, b, connect=connect))

    first = go("scheduled__1")
    assert first["counts"]["traffic_speed"] == (2, 2) and first["counts"]["call_audit"] == (4, 4)
    assert first["warnings"] == []
    assert list(base.iterdir()) == []                                      # 성공하면 스테이징 폴더가 남지 않는다
    q = lambda sql: pg.execute(sql).fetchall()
    counts = {t: q(f"select count(*) from {t}")[0][0] for t in
              ("fact.traffic_speed", "fact.bus_position", "fact.incident_observation", "fact.incident_episode", "ops.call_audit")}
    assert counts == {"fact.traffic_speed": 2, "fact.bus_position": 1, "fact.incident_observation": 2,
                      "fact.incident_episode": 1, "ops.call_audit": 4}
    assert q("select n_days, provisional from mart.mv_link_baseline") == [(1, True)]

    second = go("scheduled__2")                                            # 같은 윈도우를 다른 실행으로 다시: 결과 불변
    assert second["counts"] == first["counts"]
    again = {t: q(f"select count(*) from {t}")[0][0] for t in counts}
    assert again == counts
    assert q("select count(*) from ops.load_run where status='ok'") == [(2,)]
    assert J.mark_stale_runs(connect=connect) == 0


def test_process_window_fails_fast_when_silver_is_missing(tmp_path, pg, monkeypatch):
    monkeypatch.setenv("GOLD_DSN", os.environ["GOLD_TEST_DSN"])
    monkeypatch.setenv("GOLD_SILVER_ROOT", str(tmp_path / "nope"))
    monkeypatch.setenv("GOLD_BRONZE_ROOT", str(tmp_path / "nope2"))
    with pytest.raises(J.JobError):
        J.process_window(date(2026, 11, 3), date(2026, 11, 3), dag_id="gold_intraday", run_id="r", base=tmp_path / "stage")
    assert len(list((tmp_path / "stage").iterdir())) == 1                   # 실패 흔적은 디버깅용으로 남긴다
