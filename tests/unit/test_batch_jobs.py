"""batch/jobs.py (Airflow 가 부르는 함수) 와 notify 스텁의 단위 테스트. Airflow·Spark·Postgres 없이 돈다."""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from batch import jobs as J
from urbanstream import notify

ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


# ------------------------------------------------------------------ 윈도우 (KST)

def test_kst_date_boundaries():
    assert J.kst_date(datetime(2026, 11, 4, 14, 59, 59, tzinfo=UTC)) == date(2026, 11, 4)    # KST 23:59:59
    assert J.kst_date(datetime(2026, 11, 4, 15, 0, 0, tzinfo=UTC)) == date(2026, 11, 5)       # KST 00:00:00
    assert J.kst_date(datetime(2026, 11, 4, 15, 0, 0)) == date(2026, 11, 5)                   # 시간대 없으면 UTC 로 본다


def test_windows_intraday_is_today_and_daily_is_yesterday_to_today():
    a = datetime(2026, 11, 4, 16, 0, tzinfo=UTC)      # KST 11-05 01:00 — gold_daily 가 도는 시각
    assert J.window_intraday(a) == (date(2026, 11, 5), date(2026, 11, 5))
    assert J.window_daily(a) == (date(2026, 11, 4), date(2026, 11, 5))
    # 월 경계
    b = datetime(2026, 11, 30, 16, 0, tzinfo=UTC)     # KST 12-01 01:00
    assert J.window_daily(b) == (date(2026, 11, 30), date(2026, 12, 1))


def test_pick_anchor_takes_first_non_none_and_falls_back_to_now():
    t1, t2 = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)
    assert J.pick_anchor(None, t1, t2) is t1
    assert abs((J.pick_anchor(None, None) - datetime.now(UTC)).total_seconds()) < 5


def test_days_between_validates_order_and_length():
    assert J.days_between(date(2026, 11, 1), date(2026, 11, 3)) == [date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)]
    assert J.days_between(date(2026, 11, 1), date(2026, 11, 1)) == [date(2026, 11, 1)]
    with pytest.raises(ValueError, match="늦습니다"):
        J.days_between(date(2026, 11, 3), date(2026, 11, 1))
    with pytest.raises(ValueError, match="31일"):
        J.days_between(date(2026, 1, 1), date(2026, 3, 1))
    assert len(J.days_between(date(2026, 1, 1), date(2026, 1, 31))) == 31                 # 경계 포함


@pytest.mark.parametrize("bad", ["2026/11/04", "yesterday", "", "2026-13-01"])
def test_parse_day_rejects_bad_input(bad):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        J.parse_day(bad)


def test_parse_day_accepts_iso_and_dates():
    assert J.parse_day("2026-11-04") == date(2026, 11, 4)
    assert J.parse_day(date(2026, 11, 4)) == date(2026, 11, 4)
    assert J.parse_day(datetime(2026, 11, 4, 12, 0)) == date(2026, 11, 4)


# ------------------------------------------------------------------ 스테이징 폴더

def test_stage_dir_is_unique_per_run_and_try_and_stays_inside_base(tmp_path):
    a = J.stage_dir_for(tmp_path, "gold_daily", "scheduled__2026-11-04T16:00:00+00:00", 1)
    b = J.stage_dir_for(tmp_path, "gold_daily", "scheduled__2026-11-04T16:00:00+00:00", 2)
    c = J.stage_dir_for(tmp_path, "gold_intraday", "scheduled__2026-11-04T16:00:00+00:00", 1)
    assert len({a, b, c}) == 3
    for p in (a, b, c):
        assert p.parent == tmp_path and ":" not in p.name and "+" not in p.name and "__" in p.name
    evil = J.stage_dir_for(tmp_path, "../../etc", "x/../..", 1)
    assert evil.parent == tmp_path and "/" not in evil.name


def test_cleanup_stage_deletes_only_inside_base(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    d = J.stage_dir_for(base, "dag", "run", 1)
    d.mkdir()
    (d / "x.parquet").write_text("x")
    assert J.cleanup_stage(base, d) is True and not d.exists()
    assert J.cleanup_stage(base, d) is False                                   # 이미 없음
    with pytest.raises(J.JobError):
        J.cleanup_stage(base, base)                                           # base 자신
    outside = tmp_path / "outside__x"
    outside.mkdir()
    with pytest.raises(J.JobError):
        J.cleanup_stage(base, outside)                                        # base 밖
    assert outside.exists()
    plain = base / "plain"                                                    # 이름 규칙 위반
    plain.mkdir()
    with pytest.raises(J.JobError):
        J.cleanup_stage(base, plain)
    assert plain.exists()


def test_cleanup_stage_refuses_symlink_escaping_base(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    target = tmp_path / "precious__data"
    target.mkdir()
    (target / "keep.txt").write_text("keep")
    link = base / "dag__run__try1"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(J.JobError):
        J.cleanup_stage(base, link)
    assert (target / "keep.txt").exists()


def test_prune_old_stages_removes_only_old_matching_dirs(tmp_path):
    old, new, other = tmp_path / "dag__run__try1", tmp_path / "dag__run2__try1", tmp_path / "keepme"
    for d in (old, new, other):
        d.mkdir()
    afile = tmp_path / "a__file"
    afile.write_text("x")
    long_ago = time.time() - 100 * 3600
    os.utime(old, (long_ago, long_ago))
    os.utime(other, (long_ago, long_ago))
    assert J.prune_old_stages(tmp_path, keep_hours=72) == ["dag__run__try1"]
    assert not old.exists() and new.exists() and other.exists() and afile.exists()
    assert J.prune_old_stages(tmp_path / "nope") == []


# ------------------------------------------------------------------ Spark 서브프로세스 환경

SECRET_ENV = {"PATH": "/usr/bin", "HOME": "/home/x", "JAVA_HOME": "/jvm", "POSTGRES_PASSWORD": "pgpw",
              "GOLD_WRITER_PASSWORD": "wpw", "AIRFLOW__API_AUTH__JWT_SECRET": "jwt", "AIRFLOW__CORE__FERNET_KEY": "fk",
              "SEOUL_API_KEY": "seoulkey", "DATA_GO_KR_KEY": "dkey", "GOLD_DSN": "host=x password=y"}


def test_stage_env_is_a_whitelist_and_leaks_no_secrets(tmp_path):
    env = J.stage_env(tmp_path / "s", silver_root="/data/silver", bronze_root="/data/bronze", source_env=SECRET_ENV)
    blob = json.dumps(env)
    for secret in ("pgpw", "wpw", "jwt", "fk", "seoulkey", "dkey", "password=y"):
        assert secret not in blob
    assert set(env) <= set(J._ENV_PASS) | {"GOLD_SILVER_ROOT", "GOLD_BRONZE_ROOT", "GOLD_STAGE_ROOT", "PYTHONPATH",
                                            "PYTHONDONTWRITEBYTECODE", "PYSPARK_SUBMIT_ARGS"}
    assert env["PATH"] == "/usr/bin" and env["JAVA_HOME"] == "/jvm"
    assert env["GOLD_STAGE_ROOT"] == str(tmp_path / "s")
    assert env["PYSPARK_SUBMIT_ARGS"] == "--master local[2] --driver-memory 1g pyspark-shell"


class FakeProc:
    def __init__(self, rc=0, err=""):
        self.returncode, self.stderr, self.stdout = rc, err, ""


def _manifest(d0, d1, audit=True, counts=None, created=None):
    return {"created_at": (created or datetime.now(UTC)).isoformat(), "date_from": d0.isoformat(), "date_to": d1.isoformat(),
            "row_counts": counts if counts is not None else {"traffic_speed": 3, "call_audit": 5}, "audit_available": audit}


def test_run_stage_calls_gold_stage_with_clean_env_and_returns_manifest(tmp_path, monkeypatch):
    for k, v in SECRET_ENV.items():
        monkeypatch.setenv(k, v)
    seen = {}
    d0, d1 = date(2026, 11, 4), date(2026, 11, 5)

    def runner(cmd, **kw):
        seen.update(cmd=cmd, **kw)
        Path(kw["env"]["GOLD_STAGE_ROOT"], "manifest.json").write_text(json.dumps(_manifest(d0, d1)))
        return FakeProc()

    stage = tmp_path / "s__r__try1"
    m = J.run_stage(d0, d1, stage, silver_root="/s", bronze_root="/b", runner=runner, python="PY")
    assert seen["cmd"] == ["PY", "-m", "batch.gold_stage", "--from", "2026-11-04", "--to", "2026-11-05"]
    assert seen["cwd"] == str(J.ROOT) and seen["env"]["GOLD_STAGE_ROOT"] == str(stage)
    assert "pgpw" not in json.dumps(seen["env"]) and "jwt" not in json.dumps(seen["env"])
    assert m["row_counts"] == {"traffic_speed": 3, "call_audit": 5}


def test_run_stage_failure_modes(tmp_path):
    d = date(2026, 11, 4)
    with pytest.raises(J.JobError, match="(?s)종료 코드 3.*boom"):
        J.run_stage(d, d, tmp_path / "a__b__try1", runner=lambda c, **k: FakeProc(3, "x\nboom"))
    with pytest.raises(J.JobError, match="manifest.json 이 없습니다"):
        J.run_stage(d, d, tmp_path / "a__b__try2", runner=lambda c, **k: FakeProc(0))

    def slow(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, 1)

    with pytest.raises(J.JobError, match="초 안에 끝나지"):
        J.run_stage(d, d, tmp_path / "a__b__try3", runner=slow, timeout_sec=1)
    with pytest.raises(ValueError):
        J.run_stage(date(2026, 11, 5), d, tmp_path / "a__b__try4")


# ------------------------------------------------------------------ manifest 검사

def test_check_manifest_accepts_good_and_rejects_bad():
    d0 = d1 = date(2026, 11, 4)
    assert J.check_manifest(_manifest(d0, d1), d0, d1) == []
    with pytest.raises(J.JobError, match="날짜 범위"):
        J.check_manifest(_manifest(date(2026, 11, 3), d1), d0, d1)                # 옛 manifest
    with pytest.raises(J.JobError, match="audit"):
        J.check_manifest(_manifest(d0, d1, audit=False), d0, d1)                  # absent_since 가 전부 NULL 이 되는 상태
    assert J.check_manifest(_manifest(d0, d1, audit=False), d0, d1, require_audit=False) == []
    with pytest.raises(J.JobError, match="row_counts"):
        J.check_manifest(_manifest(d0, d1, counts={}), d0, d1)
    w = J.check_manifest(_manifest(d0, d1, counts={"a": 0, "b": 0}), d0, d1)
    assert len(w) == 1 and "0" in w[0]


def test_check_manifest_rejects_manifest_older_than_this_run():
    d = date(2026, 11, 4)
    now = datetime.now(UTC)
    old = _manifest(d, d, created=now - timedelta(hours=1))
    with pytest.raises(J.JobError, match="이전에 만들어졌습니다"):
        J.check_manifest(old, d, d, started_at=now)
    assert J.check_manifest(_manifest(d, d, created=now), d, d, started_at=now) == []


# ------------------------------------------------------------------ DB 를 부르는 함수 (가짜 연결)

class FakeConn:
    def __init__(self, counts=None, rowcount=0):
        self.counts, self.rowcount, self.sql = counts or {}, rowcount, []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        outer = self

        class Cur:
            rowcount = outer.rowcount

            def fetchone(self_inner):
                table = sql.split("FROM ")[1].split(" ")[0]
                return (outer.counts.get(table, 0),)
        return Cur()

    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_verify_counts_requires_db_at_least_staged():
    d = date(2026, 11, 4)
    counts = {"traffic_speed": 10, "bus_position": 5, "bus_arrival": 7, "incident_observation": 3, "weather_obs": 1, "call_audit": 9}
    m = {"row_counts": {**counts, "incident_episode": 4}}
    ok = FakeConn({"fact.traffic_speed": 10, "fact.bus_position": 6, "fact.bus_arrival": 7, "fact.incident_observation": 3,
                   "fact.weather_obs": 1, "ops.call_audit": 9})
    res = J.verify_counts(m, d, d, connect=lambda: ok)
    assert res["bus_position"] == (5, 6) and "incident_episode" not in res               # DB 가 더 많은 것은 정상
    short = FakeConn({"fact.traffic_speed": 9, "fact.bus_position": 5, "fact.bus_arrival": 7, "fact.incident_observation": 3,
                      "fact.weather_obs": 1, "ops.call_audit": 9})
    with pytest.raises(J.JobError, match="traffic_speed: 스테이징 10 > DB 9"):
        J.verify_counts(m, d, d, connect=lambda: short)
    assert all(p == (d, d) for _, p in ok.sql)                                            # 날짜 범위를 파라미터로 묶는다


def test_mark_stale_runs_updates_only_old_running_rows():
    c = FakeConn(rowcount=2)
    assert J.mark_stale_runs(older_than_minutes=90, connect=lambda: c) == 2
    sql, params = c.sql[0]
    assert "status='running'" in sql.replace(" ", "") and params == (90,)


def test_run_load_passes_stage_and_refresh_flag(tmp_path, monkeypatch):
    import batch.gold_load as GL
    seen = {}
    monkeypatch.setattr(GL, "load_all", lambda conn, stage, refresh_mart=False: seen.update(conn=conn, stage=stage, r=refresh_mart) or {"run_id": 7})
    c = FakeConn()
    assert J.run_load(tmp_path, refresh_mart=True, connect=lambda: c) == {"run_id": 7}
    assert seen == {"conn": c, "stage": tmp_path, "r": True}


# ------------------------------------------------------------------ process_window 순서·정리

def test_process_window_order_and_cleanup(tmp_path):
    d = date(2026, 11, 4)
    calls = []

    def stage(d0, d1, sd):
        calls.append("stage")
        sd.mkdir(parents=True)
        return _manifest(d0, d1)

    def load(sd, refresh_mart=False):
        calls.append(f"load:{refresh_mart}")
        assert sd.exists()
        return {"run_id": 11}

    def verify(m, d0, d1):
        calls.append("verify")
        return {"traffic_speed": (3, 3)}

    out = J.process_window(d, d, dag_id="backfill", run_id="manual__x", try_number=1, refresh_mart=True, base=tmp_path,
                           stage=stage, load=load, verify=verify)
    assert calls == ["stage", "load:True", "verify"] and out["run_id"] == 11
    assert list(tmp_path.iterdir()) == []                                                  # 성공하면 스테이징 폴더를 지운다


def test_process_window_keeps_stage_dir_when_load_fails(tmp_path):
    d = date(2026, 11, 4)

    def stage(d0, d1, sd):
        sd.mkdir(parents=True)
        return _manifest(d0, d1)

    def load(sd, refresh_mart=False):
        raise RuntimeError("db down")

    with pytest.raises(RuntimeError):
        J.process_window(d, d, dag_id="a", run_id="b", base=tmp_path, stage=stage, load=load, verify=lambda *a: {})
    assert len(list(tmp_path.iterdir())) == 1                                              # 디버깅용으로 남긴다


def test_process_window_does_not_load_when_audit_missing(tmp_path):
    d = date(2026, 11, 4)
    loaded = []
    stage = lambda d0, d1, sd: (sd.mkdir(parents=True), _manifest(d0, d1, audit=False))[1]
    with pytest.raises(J.JobError, match="audit"):
        J.process_window(d, d, dag_id="a", run_id="b", base=tmp_path, stage=stage,
                         load=lambda sd, refresh_mart=False: loaded.append(1), verify=lambda *a: {})
    assert loaded == []


# ------------------------------------------------------------------ 알림 · 실패 콜백

def test_notify_send_validates_level_and_masks_secrets(caplog):
    with caplog.at_level(logging.INFO, logger="urbanstream.notify"):
        assert notify.send("error", "제목 password=hunter2", "token: abc123 api_key=K1 ok", dedup_key="k") is True
    text = caplog.text
    assert "hunter2" not in text and "abc123" not in text and "K1" not in text and "***" in text and "dedup=k" in text
    with pytest.raises(ValueError):
        notify.send("fatal", "x")
    assert len(notify.mask("a" * 5000)) == 1500


class FakeTI:
    dag_id, task_id, try_number, run_id = "gold_daily", "load", 2, "scheduled__x"


def test_on_failure_calls_notify_with_dedup_key_and_masked_exception(caplog):
    with caplog.at_level(logging.ERROR, logger="urbanstream.notify"):
        J.on_failure({"ti": FakeTI(), "run_id": "scheduled__x", "exception": RuntimeError("connect failed password=pw123")})
    assert "gold_daily.load" in caplog.text and "pw123" not in caplog.text and "airflow:gold_daily.load" in caplog.text
    J.on_failure({})                                                                        # 컨텍스트가 비어 있어도 죽지 않는다


# ------------------------------------------------------------------ 가볍게 import 되는가

def test_jobs_module_does_not_import_heavy_dependencies():
    code = ("import sys; sys.path[:0] = [%r, %r]; import batch.jobs, batch.marts, urbanstream.notify; "
            "bad = [m for m in ('pyspark', 'psycopg', 'pyarrow', 'airflow') if m in sys.modules]; "
            "sys.exit(1 if bad else 0)") % (str(ROOT), str(ROOT / "src"))
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, "DAG 파싱이 무거워집니다: batch.jobs 가 pyspark/psycopg/pyarrow/airflow 를 import 합니다"
