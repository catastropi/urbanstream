"""Gold 스테이징 실행기(batch/stage_runner.py)와 Airflow 쪽 요청 함수(batch/jobs.request_stage).

Spark 없이 시험한다. Spark 실행은 가짜 runner 가 manifest.json 을 쓰는 것으로 대신한다. 실제 Spark 실행은 tests/spark/test_jobs_e2e.py.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import date
from pathlib import Path

import pytest

from batch import jobs as J
from batch import stage_runner as R

D = date(2026, 11, 20)


class Proc:
    """subprocess.Popen 대신. polls 번 poll() 이 None(실행 중)을 돌려준 뒤 rc 로 끝난다. 출력은 넘겨받은 파일에 쓴다."""
    def __init__(self, rc=0, out="", err="", polls=0, stdout=None, stderr=None):
        self.rc, self.left, self.pid = rc, polls, -1
        if stdout:
            stdout.write(out)
        if stderr:
            stderr.write(err)

    def poll(self):
        if self.left > 0:
            self.left -= 1
            return None
        return self.rc


def launcher(rc=0, out="", err="", polls=0, on_start=None, seen=None):
    def start(cmd, env, stdout=None, stderr=None, start_new_session=False, **kw):
        assert start_new_session, "Spark 는 새 프로세스 그룹으로 띄워야 그룹째 끌 수 있다"
        if seen is not None:
            seen.append((cmd, env["GOLD_STAGE_ROOT"]))
        if on_start:
            on_start(cmd, env)
        return Proc(rc, out, err, polls, stdout, stderr)
    return start


def _write_manifest(cmd, env):
    d0, d1 = cmd[cmd.index("--from") + 1], cmd[cmd.index("--to") + 1]
    Path(env["GOLD_STAGE_ROOT"], "manifest.json").write_text(json.dumps({"date_from": d0, "date_to": d1}), encoding="utf-8")


def fake_spark(calls, polls=0):
    """spark-submit 대신: GOLD_STAGE_ROOT 에 manifest.json 을 쓰고 성공한다."""
    return launcher(0, "ok", polls=polls, on_start=_write_manifest, seen=calls)


def put_request(base: Path, rid: str, key: str, stage_name: str, created: float) -> Path:
    """created 가 작은 수(1.0, 2.0 …)면 '조금 전'의 상대 순서로 본다(줄 선 시간 한도에 걸리지 않게)."""
    if created < 1e9:
        created = time.time() - 60 + created
    req = R.requests_dir(base)
    req.mkdir(parents=True, exist_ok=True)
    R.write_json_atomic(req / f"{rid}.json", {"id": rid, "key": key, "date_from": "2026-11-20", "date_to": "2026-11-20",
                                              "stage_dir": str(base / stage_name), "created_at": created})
    return req


# ------------------------------------------------------------------ 실행기

def test_runner_executes_one_request_and_marks_done(tmp_path):
    calls = []
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    assert R.process_pending(tmp_path, launcher=fake_spark(calls), env={}) == [("1__a__r__try1", "done")]
    assert (tmp_path / "a__r__try1" / "manifest.json").exists()
    assert (req / "1__a__r__try1.done").exists() and not (req / "1__a__r__try1.running").exists()
    cmd, root = calls[0]
    assert cmd[0].endswith("spark-submit") and cmd[-4:] == ["--from", "2026-11-20", "--to", "2026-11-20"]
    assert cmd[cmd.index("--driver-memory") + 1] == "1g"
    assert root == str(tmp_path / "a__r__try1")


def test_orphan_retry_duplicates_run_spark_only_once(tmp_path):
    """고아 판정으로 try1 과 try2 요청이 같이 쌓이면 최신(try2)만 실행하고 try1 은 superseded (D-212)."""
    calls = []
    req = put_request(tmp_path, "1__g__r__try1", "g__r", "g__r__try1", 1.0)
    put_request(tmp_path, "2__g__r__try2", "g__r", "g__r__try2", 2.0)
    out = R.process_pending(tmp_path, launcher=fake_spark(calls), env={})
    assert out == [("2__g__r__try2", "done")] and len(calls) == 1
    assert json.loads((req / "1__g__r__try1.superseded").read_text())["by"] == "2__g__r__try2"


def test_different_runs_of_the_same_window_are_not_collapsed(tmp_path):
    """gold_intraday 와 backfill 이 같은 날을 요청하면 둘 다 실행한다 (각자 자기 폴더를 적재한다)."""
    calls = []
    put_request(tmp_path, "1__gold_intraday__r1__try1", "gold_intraday__r1", "gold_intraday__r1__try1", 1.0)
    put_request(tmp_path, "2__backfill__r9__try1", "backfill__r9", "backfill__r9__try1", 2.0)
    assert [k for _, k in R.process_pending(tmp_path, launcher=fake_spark(calls), env={})] == ["done", "done"]


def test_failed_spark_is_reported_with_stderr_tail(tmp_path):
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    R.process_pending(tmp_path, launcher=launcher(3, "", "line1\nboom"), env={})
    body = json.loads((req / "1__a__r__try1.failed").read_text())
    assert body["returncode"] == 3 and "boom" in body["stderr_tail"]


def test_timeout_kills_the_process_group_and_is_a_failure(tmp_path):
    killed = []
    t = iter(range(0, 10_000, 10))
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", time.time())
    r = R.Request.load(req / "1__a__r__try1.json")
    out = R.run_one(r, req, tmp_path, launcher=launcher(polls=10_000), env={}, timeout_sec=25, sleep=lambda s: None,
                    clock=lambda: float(next(t)), kill=killed.append)
    assert out == "failed" and len(killed) == 1
    body = json.loads((req / "1__a__r__try1.failed").read_text())
    assert body["returncode"] is None and "끝나지 않았다" in body["stderr_tail"]
    assert not (req / "1__a__r__try1.running").exists()


def test_heartbeat_is_refreshed_while_spark_runs(tmp_path):
    """D-274: 예전에는 요청 사이에만 박동해서 Spark 가 3분을 넘으면 그동안의 요청이 '응답 없음'으로 즉시 실패했다."""
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", time.time())
    alive = req / R.ALIVE
    seen = []

    def sleep(_):
        seen.append(alive.exists() and alive.stat().st_mtime)
        old = time.time() - 600                              # 박동을 일부러 오래된 것으로 되돌린다
        if alive.exists():
            os.utime(alive, (old, old))
    r = R.Request.load(req / "1__a__r__try1.json")
    assert R.run_one(r, req, tmp_path, launcher=fake_spark([], polls=3), env={}, sleep=sleep) == "done"
    assert len(seen) == 3 and all(m and time.time() - m < 60 for m in seen), "poll 마다 박동이 새로 찍혀야 한다"


def test_cancel_while_running_stops_spark(tmp_path):
    killed = []
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", time.time())

    def sleep(_):
        (req / "1__a__r__try1.cancel").write_text("timeout")
    r = R.Request.load(req / "1__a__r__try1.json")
    assert R.run_one(r, req, tmp_path, launcher=launcher(polls=10_000), env={}, sleep=sleep, kill=killed.append) == "failed"
    assert killed and "취소" in json.loads((req / "1__a__r__try1.failed").read_text())["stderr_tail"]


def test_request_that_waited_too_long_in_the_queue_is_skipped(tmp_path):
    calls = []
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", time.time() - 7200)
    assert R.process_pending(tmp_path, launcher=fake_spark(calls), env={}) == [("1__a__r__try1", "skipped")]
    assert not calls and "줄을 선 지" in json.loads((req / "1__a__r__try1.failed").read_text())["stderr_tail"]


def test_kill_group_escalates_to_sigkill(monkeypatch):
    import signal
    import subprocess
    sent = []
    monkeypatch.setattr(R.os, "killpg", lambda pid, sig: sent.append(sig))

    class Stubborn:
        pid = 4242
        def wait(self, timeout):
            if len(sent) < 2:
                raise subprocess.TimeoutExpired("x", timeout)
    R.kill_group(Stubborn(), grace_sec=0)
    assert sent == [signal.SIGTERM, signal.SIGKILL]


def test_kill_group_really_stops_child_processes(tmp_path):
    """실제 프로세스: 자식이 손자를 띄운 상태에서 그룹째 꺼진다 (spark-submit JVM → python 드라이버와 같은 모양)."""
    import subprocess
    import sys
    pidfile = tmp_path / "grandchild.pid"
    code = ("import subprocess,sys,time;"
            f"p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']);open({str(pidfile)!r},'w').write(str(p.pid));"
            "time.sleep(60)")
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    for _ in range(100):
        if pidfile.exists() and pidfile.read_text():
            break
        time.sleep(0.05)
    gpid = int(pidfile.read_text())
    R.kill_group(proc, grace_sec=5)
    assert proc.poll() is not None
    for _ in range(100):
        try:
            os.kill(gpid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        pytest.fail("손자 프로세스가 남았다")


def test_runner_restart_reruns_a_request_left_running(tmp_path):
    """실행기가 Spark 도중 죽으면 .running 만 남는다. 다시 뜨면 그 요청을 다시 실행한다(스테이징은 폴더를 덮어써서 안전)."""
    calls = []
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)      # 오래됐어도 .running 이 있으면 다시 실행
    (req / "1__a__r__try1.running").write_text("123")
    assert R.process_pending(tmp_path, launcher=fake_spark(calls), env={}) == [("1__a__r__try1", "done")]


def test_cancelled_request_is_skipped(tmp_path):
    calls = []
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    (req / "1__a__r__try1.cancel").write_text("timeout")
    assert R.process_pending(tmp_path, launcher=fake_spark(calls), env={}) == [("1__a__r__try1", "skipped")]
    assert not calls


def test_stage_dir_outside_base_is_refused(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.write_json_atomic(req / "1__x.json", {"id": "1__x", "key": "x", "date_from": "2026-11-20", "date_to": "2026-11-20",
                                            "stage_dir": str(tmp_path.parent / "elsewhere"), "created_at": 1.0})
    calls = []
    assert R.process_pending(tmp_path, launcher=fake_spark(calls), env={}) == [("1__x", "failed")] and not calls


def test_broken_request_file_is_marked_failed_and_does_not_stop_the_loop(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    (req / "1__bad.json").write_text("{not json")
    put_request(tmp_path, "2__a__r__try1", "a__r", "a__r__try1", 2.0)
    out = R.process_pending(tmp_path, launcher=fake_spark([]), env={})
    assert out == [("2__a__r__try1", "done")] and (req / "1__bad.failed").exists()


def test_prune_removes_only_old_finished_requests(tmp_path):
    req = put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    put_request(tmp_path, "2__b__r__try1", "b__r", "b__r__try1", 2.0)
    R.write_json_atomic(req / "1__a__r__try1.done", {})
    old = time.time() - 100 * 3600
    os.utime(req / "1__a__r__try1.done", (old, old))
    assert R.prune(req) == 1
    assert not (req / "1__a__r__try1.json").exists() and (req / "2__b__r__try1.json").exists()


def test_requests_dir_is_not_mistaken_for_a_stage_dir_by_cleanup():
    assert "__" not in R.REQ_DIRNAME, "prune_old_stages 는 이름에 '__' 가 있는 폴더를 지운다"


# ------------------------------------------------------------------ Airflow 쪽

def test_stage_mode_defaults_to_inline_so_live_containers_keep_old_behavior():
    assert J.stage_mode({}) == "inline"
    assert J.stage_mode({"GOLD_STAGE_MODE": "runner"}) == "runner"
    with pytest.raises(J.JobError):
        J.stage_mode({"GOLD_STAGE_MODE": "docker"})


def test_request_key_groups_retries_of_one_run():
    assert J.request_key(Path("/b/gold_intraday__scheduled__2026-11-20T01_20__try1")) == \
        J.request_key(Path("/b/gold_intraday__scheduled__2026-11-20T01_20__try2"))


def test_request_fails_fast_when_runner_is_not_alive(tmp_path):
    with pytest.raises(J.JobError, match="gold-runner"):
        J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, sleep=lambda s: None)
    assert not list(R.requests_dir(tmp_path).glob("*.json")), "실행기가 없으면 요청을 남기지 않는다"


def test_request_and_runner_round_trip(tmp_path):
    """Airflow 쪽 요청 → 실행기 처리 → Airflow 쪽이 manifest 를 받는 전체 흐름."""
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    stop = threading.Event()

    def runner_loop():
        while not stop.is_set():
            R.process_pending(tmp_path, launcher=fake_spark([]), env={})
            time.sleep(0.05)
    t = threading.Thread(target=runner_loop, daemon=True)
    t.start()
    try:
        m = J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, poll_sec=0.05, timeout_sec=10)
    finally:
        stop.set()
        t.join(2)
    assert m == {"date_from": "2026-11-20", "date_to": "2026-11-20", "_stage_dir": str(tmp_path / "a__r__try1")}


def test_request_reports_runner_failure(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)

    def sleep(_):
        R.process_pending(tmp_path, launcher=launcher(1, "", "spark died"), env={})
    with pytest.raises(J.JobError, match="spark died"):
        J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, sleep=sleep)


def test_request_times_out_and_cancels(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    t = iter(range(0, 10_000, 100))
    with pytest.raises(J.JobError, match="끝나지 않았습니다"):
        J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, timeout_sec=250, sleep=lambda s: None,
                        clock=lambda: float(next(t)))
    assert list(req.glob("*.cancel"))


def test_request_refuses_stage_dir_outside_base(tmp_path):
    with pytest.raises(J.JobError, match="base 안이 아닙니다"):
        J.request_stage(D, D, tmp_path.parent / "x__y__try1", base=tmp_path)


def test_run_stage_dispatches_on_mode(tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(J, "request_stage", lambda d0, d1, sd, **kw: seen.setdefault("runner", sd) and {"ok": 1})
    monkeypatch.setenv("GOLD_STAGE_MODE", "runner")
    J.run_stage(D, D, tmp_path / "a__r__try1")
    assert seen["runner"] == tmp_path / "a__r__try1"


def test_retry_reattaches_to_the_previous_try_still_in_flight(tmp_path):
    """try1 이 Airflow 에서 죽었지만(execution_timeout 등) 요청은 실행기에 남아 있다 → try2 는 새 요청을 만들지 않고 그 결과를 쓴다."""
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    put_request(tmp_path, "1__g__r__try1", "g__r", "g__r__try1", time.time())
    calls = []

    def sleep(_):
        R.process_pending(tmp_path, launcher=fake_spark(calls), env={})
    m = J.request_stage(D, D, tmp_path / "g__r__try2", base=tmp_path, sleep=sleep)
    assert m["_stage_dir"] == str(tmp_path / "g__r__try1") and len(calls) == 1
    assert sorted(p.name for p in req.glob("*.json")) == ["1__g__r__try1.json"]


def test_retry_does_not_reattach_to_a_cancelled_request(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    put_request(tmp_path, "1__g__r__try1", "g__r", "g__r__try1", time.time())
    (req / "1__g__r__try1.cancel").write_text("timeout")

    def sleep(_):
        R.process_pending(tmp_path, launcher=fake_spark([]), env={})
    m = J.request_stage(D, D, tmp_path / "g__r__try2", base=tmp_path, sleep=sleep)
    assert m["_stage_dir"] == str(tmp_path / "g__r__try2")


def test_wait_restarts_the_clock_when_the_runner_starts_the_request(tmp_path):
    """줄 서기 시간은 실행 시간 한도에 들어가지 않는다: .running 이 보인 뒤부터 다시 센다."""
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    t = iter(range(0, 100_000, 100))
    state = {"n": 0}

    def sleep(_):
        state["n"] += 1
        R.touch_alive(req)
        rid = next(p.stem for p in req.glob("*.json"))
        if state["n"] == 2:                                   # 200초 줄을 선 뒤 실행 시작
            (req / f"{rid}.running").write_text("1")
        if state["n"] == 5:                                   # 시작 뒤 300초에 끝남 (전체 500초 > 한도 400초)
            _write_manifest(["--from", "2026-11-20", "--to", "2026-11-20"], {"GOLD_STAGE_ROOT": str(tmp_path / "a__r__try1")})
            R.write_json_atomic(req / f"{rid}.done", {"returncode": 0})
    (tmp_path / "a__r__try1").mkdir()
    m = J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, timeout_sec=400, sleep=sleep, clock=lambda: float(next(t)))
    assert m["date_from"] == "2026-11-20"


def test_wait_fails_fast_when_runner_heartbeat_stops(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)

    def sleep(_):
        old = time.time() - 600
        os.utime(req / R.ALIVE, (old, old))
    t = iter(range(0, 10_000, 10))
    with pytest.raises(J.JobError, match="박동"):
        J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, sleep=sleep, clock=lambda: float(next(t)))
    assert not list(req.glob("*.cancel")), "실행기가 다시 뜨면 이어서 처리하도록 취소를 남기지 않는다"


# ------------------------------------------------------------------ STEP 1: kind (silver_rebuild / silver_publish)

def put_kind(base: Path, rid: str, kind: str, args: dict, stage_name: str, created: float, key: str = "k") -> Path:
    if created < 1e9:                                          # put_request 와 같이 '조금 전'의 상대 순서
        created = time.time() - 60 + created
    req = R.requests_dir(base)
    req.mkdir(parents=True, exist_ok=True)
    R.write_json_atomic(req / f"{rid}.json", {"id": rid, "key": f"{kind}:{key}", "kind": kind, "args": args, "date_from": "2026-11-10",
                                              "date_to": "2026-11-10", "stage_dir": str(base / stage_name), "created_at": created})
    return req


def test_request_without_kind_is_gold_stage_exactly_as_in_3a(tmp_path):
    put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    r = R.pending(R.requests_dir(tmp_path))[0]
    assert r.kind == "gold_stage" and r.args == {}
    cmd = R.spark_command(r, driver_memory="1g", master="local[2]", spark_submit="spark-submit")
    assert cmd[-5].endswith("gold_stage.py")


def test_silver_rebuild_request_runs_the_rebuild_job_with_silver_paths(tmp_path):
    calls = []

    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append((cmd, env))
        return Proc(0, "{}", stdout=stdout, stderr=stderr)
    put_kind(tmp_path, "1__rb__try1", "silver_rebuild", {"tables": ["silver_weather", "silver_bus_arrival"], "version": "r0007t1",
                                                         "bronze_read_mode": "listing", "allow_shrink": True}, "rb__try1", 1.0)
    env = {"GOLD_BRONZE_ROOT": "/b", "GOLD_SILVER_ROOT": "/s", "GOLD_SILVER_REBUILD_ROOT": "/rb"}
    assert R.process_pending(tmp_path, launcher=run, env=env) == [("1__rb__try1", "done")]
    cmd, e = calls[0]
    i = cmd.index("build")
    assert cmd[i - 1].endswith("silver_rebuild.py")
    assert cmd[cmd.index("--tables") + 1] == "silver_weather,silver_bus_arrival" and cmd[cmd.index("--version") + 1] == "r0007t1"
    assert cmd[cmd.index("--rebuild-root") + 1] == "/rb" and "--allow-shrink" in cmd
    assert cmd[cmd.index("--report") + 1] == str(tmp_path / "rb__try1" / R.REBUILD_REPORT)
    assert (e["SILVER_BRONZE_ROOT"], e["SILVER_ROOT"], e["SILVER_REBUILD_ROOT"]) == ("/b", "/s", "/rb")
    assert "umask-mode=000" in " ".join(cmd)


def test_silver_publish_runs_in_process_without_spark(tmp_path):
    from streaming import silver_pointer as P
    rb = tmp_path / "rb"
    vd = P.version_dir(rb, "silver_weather", "r0007t1")
    (vd / "event_date=2026-11-10").mkdir(parents=True)
    P.write_json_atomic(vd / P.BUILD_FILE, {"status": "ok", "partition_col": "event_date", "dates": {"2026-11-10": 4}})
    put_kind(tmp_path, "2__pb__try1", "silver_publish", {"tables": ["silver_weather"], "version": "r0007t1", "reprocess_id": 7},
             "pb__try1", 2.0)

    def no_spark(*a, **k):
        raise AssertionError("silver_publish 는 Spark 를 띄우지 않는다")
    assert R.process_pending(tmp_path, launcher=no_spark, env={"GOLD_SILVER_REBUILD_ROOT": str(rb)}) == [("2__pb__try1", "done")]
    assert P.active_dates(rb, "silver_weather") == {"2026-11-10": "r0007t1"}
    rep = json.loads((tmp_path / "pb__try1" / R.PUBLISH_REPORT).read_text())
    assert rep["previous"] == {"silver_weather": {"2026-11-10": None}}


def test_publish_of_a_blocked_build_fails_and_leaves_pointer(tmp_path):
    from streaming import silver_pointer as P
    rb = tmp_path / "rb"
    vd = P.version_dir(rb, "silver_weather", "r0008t1")
    vd.mkdir(parents=True)
    P.write_json_atomic(vd / P.BUILD_FILE, {"status": "shrink_blocked", "partition_col": "event_date", "dates": {"2026-11-10": 1}})
    put_kind(tmp_path, "3__pb__try1", "silver_publish", {"tables": ["silver_weather"], "version": "r0008t1"}, "pb__try1", 3.0)
    assert R.process_pending(tmp_path, env={"GOLD_SILVER_REBUILD_ROOT": str(rb)}) == [("3__pb__try1", "failed")]
    body = json.loads((R.requests_dir(tmp_path) / "3__pb__try1.failed").read_text())
    assert "shrink_blocked" in body["stderr_tail"] and P.active_dates(rb, "silver_weather") == {}


def test_kinds_do_not_supersede_each_other_but_retries_of_one_kind_do(tmp_path):
    calls = []
    put_request(tmp_path, "1__x__try1", "x", "x__try1", 1.0)                                  # gold_stage, key "x"
    put_kind(tmp_path, "2__x__try1", "silver_rebuild", {"tables": ["silver_weather"], "version": "v1"}, "y__try1", 2.0, key="x")
    put_kind(tmp_path, "3__x__try2", "silver_rebuild", {"tables": ["silver_weather"], "version": "v2"}, "y__try2", 3.0, key="x")
    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append(cmd)
        return Proc(0, stdout=stdout, stderr=stderr)
    res = dict(R.process_pending(tmp_path, launcher=run, env={}))
    assert set(res) == {"1__x__try1", "3__x__try2"}                                          # gold_stage 와 rebuild 는 따로
    assert (R.requests_dir(tmp_path) / "2__x__try1.superseded").exists()


def test_unknown_kind_is_marked_failed(tmp_path):
    put_kind(tmp_path, "4__z__try1", "drop_everything", {}, "z__try1", 4.0)
    assert R.process_pending(tmp_path, env={}) == []
    assert (R.requests_dir(tmp_path) / "4__z__try1.failed").exists()


def test_request_runner_tags_key_with_kind_and_waits_for_done(tmp_path):
    base = tmp_path
    R.requests_dir(base).mkdir(parents=True)
    R.touch_alive(R.requests_dir(base))
    sd = base / "reprocess_rebuild_2026-11-10__run1__try1"

    def tick(_):
        reqs = list(R.requests_dir(base).glob("*.json"))
        body = json.loads(reqs[0].read_text())
        assert body["kind"] == "silver_rebuild" and body["key"] == "silver_rebuild:reprocess_rebuild_2026-11-10__run1"
        assert body["args"]["version"] == "r0001t1"
        R.write_json_atomic(R.marker(R.requests_dir(base), body["id"], "done"), {"returncode": 0, "elapsed_sec": 1})
    out = J.request_runner("silver_rebuild", date(2026, 11, 10), date(2026, 11, 10), sd, args={"version": "r0001t1"}, base=base,
                           sleep=tick, poll_sec=0)
    assert out["returncode"] == 0


def test_stage_progress_lines_reach_the_runner_log(tmp_path, caplog):
    """gold_stage 가 stderr 에 남기는 '[경과초] 표: 행, 초' 줄이 docker logs 로 보인다 (스테이징 폴더는 성공 뒤 지워진다)."""
    import logging
    put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    caplog.set_level(logging.INFO, logger="urbanstream.batch.stage_runner")
    R.process_pending(tmp_path, launcher=launcher(0, "ok", "WARN noise\n[3s] traffic_speed: 10행, 2.5초\n", on_start=_write_manifest),
                      env={})
    assert "[3s] traffic_speed: 10행, 2.5초" in caplog.text and "WARN noise" not in caplog.text


def test_wait_tolerates_a_short_stale_heartbeat_after_wake(tmp_path):
    """Mac 이 깨어난 직후: 실행기 박동이 잠깐 오래돼 보여도 곧 갱신되면 기다림을 이어 간다."""
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    t = iter(range(0, 10_000, 10))
    n = {"i": 0}

    def sleep(_):
        n["i"] += 1
        if n["i"] == 1:                                   # 깨어난 직후: 박동이 10분 전
            old = time.time() - 600
            os.utime(req / R.ALIVE, (old, old))
        elif n["i"] == 2:                                 # 실행기가 다시 박동하고 일을 끝낸다
            R.process_pending(tmp_path, launcher=fake_spark([]), env={})
    m = J.request_stage(D, D, tmp_path / "a__r__try1", base=tmp_path, sleep=sleep, clock=lambda: float(next(t)))
    assert m["date_from"] == "2026-11-20"


def _rebuild_cmd(tmp_path, env):
    calls = []

    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append(cmd)
        return Proc(0, "{}", stdout=stdout, stderr=stderr)
    put_kind(tmp_path, "1__rb__try1", "silver_rebuild", {"tables": ["silver_weather"], "version": "r0001t1"}, "rb__try1", 1.0)
    assert R.process_pending(tmp_path, launcher=run, env=env) == [("1__rb__try1", "done")]
    return calls[0]


def test_silver_rebuild_runs_at_low_cpu_priority_by_default(tmp_path):
    cmd = _rebuild_cmd(tmp_path, {})
    assert cmd[:3] == ["nice", "-n", str(R.DEFAULT_REBUILD_NICE)] and cmd[3].endswith("spark-submit")


def test_silver_rebuild_nice_can_be_turned_off(tmp_path):
    cmd = _rebuild_cmd(tmp_path, {"GOLD_RUNNER_REBUILD_NICE": "0"})
    assert cmd[0].endswith("spark-submit")


def test_gold_stage_is_not_niced(tmp_path):
    calls = []
    put_request(tmp_path, "1__a__r__try1", "a__r", "a__r__try1", 1.0)
    R.process_pending(tmp_path, launcher=fake_spark(calls), env={})
    assert calls[0][0][0].endswith("spark-submit")


def _killed_after(tmp_path, kind_req, env) -> int:
    """몇 초(가짜 시계)에 끊기는지."""
    t = iter(range(0, 100_000, 60))
    last = {}

    def clock():
        last["t"] = float(next(t))
        return last["t"]
    req, rid = kind_req
    r = R.Request.load(req / f"{rid}.json")
    out = R.run_one(r, req, tmp_path, launcher=launcher(polls=100_000), env=env, sleep=lambda s: None, clock=clock, kill=lambda p: None)
    assert out == "failed"
    return int(last["t"])


def test_rebuild_timeout_matches_airflow_wait_and_gold_stage_keeps_1800(tmp_path):
    rb = put_kind(tmp_path / "a", "1__rb__try1", "silver_rebuild", {"tables": ["silver_weather"], "version": "r0001t1"}, "rb__try1", 1.0)
    assert R.DEFAULT_REBUILD_TIMEOUT_SEC < _killed_after(tmp_path / "a", (rb, "1__rb__try1"), {}) <= R.DEFAULT_REBUILD_TIMEOUT_SEC + 120
    gs = put_request(tmp_path / "b", "1__a__r__try1", "a__r", "a__r__try1", time.time())
    assert R.DEFAULT_TIMEOUT_SEC < _killed_after(tmp_path / "b", (gs, "1__a__r__try1"), {}) <= R.DEFAULT_TIMEOUT_SEC + 120
    rb2 = put_kind(tmp_path / "c", "1__rb__try1", "silver_rebuild", {"tables": ["silver_weather"], "version": "r0001t1"}, "rb__try1", 1.0)
    assert _killed_after(tmp_path / "c", (rb2, "1__rb__try1"), {"GOLD_RUNNER_REBUILD_TIMEOUT_SEC": "600"}) <= 720


# ------------------------------------------------------------------ STEP 2: kind=measure

def put_measure(base: Path, rid: str, args: dict, stage_dir: Path, created: float = 1.0) -> Path:
    req = R.requests_dir(base)
    req.mkdir(parents=True, exist_ok=True)
    R.write_json_atomic(req / f"{rid}.json", {"id": rid, "key": f"measure__{rid}", "date_from": "2026-01-05", "date_to": "2026-01-05",
                                              "stage_dir": str(stage_dir), "created_at": time.time() - 60 + created, "kind": "measure",
                                              "args": args})
    return req


def _measure_env(tmp_path, **extra):
    m = tmp_path / "measure"
    m.mkdir(exist_ok=True)
    return m, {"GOLD_MEASURE_ROOT": str(m), "GOLD_RUNNER_MEASURE_NICE": "0", **extra}


def _build_args(m: Path, **over):
    a = {"job": "silver_build", "tables": ["silver_weather"], "version": "m0", "bronze_root": str(m / "syn/bronze"),
         "silver_root": str(m / "syn/_current"), "rebuild_root": str(m / "syn/rebuild"), "report": str(m / "syn/r.json")}
    a.update(over)
    return a


def test_measure_build_uses_only_measure_paths_and_reports_memory(tmp_path, monkeypatch):
    m, env = _measure_env(tmp_path)
    cg = tmp_path / "memory.current"
    cg.write_text("123456789")
    monkeypatch.setattr(R, "CGROUP_MEM", cg)
    calls = []

    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append((cmd, env))
        return Proc(0, "{}", polls=2, stdout=stdout, stderr=stderr)
    put_measure(tmp_path, "measure-1", _build_args(m), m / "syn/reports")
    assert R.process_pending(tmp_path, launcher=run, env=env, sleep=lambda s: None) == [("measure-1", "done")]
    cmd, e = calls[0]
    assert cmd[cmd.index("build") - 1].endswith("silver_rebuild.py") and cmd[cmd.index("--rebuild-root") + 1] == str(m / "syn/rebuild")
    assert (e["SILVER_BRONZE_ROOT"], e["SILVER_ROOT"], e["SILVER_REBUILD_ROOT"]) == (str(m / "syn/bronze"), str(m / "syn/_current"),
                                                                                      str(m / "syn/rebuild"))
    done = json.loads((R.requests_dir(tmp_path) / "measure-1.done").read_text())
    assert done["mem_max_bytes"] == 123456789 and done["mem_sample_sec"] == 2.0


@pytest.mark.parametrize("over", [
    {"rebuild_root": "/data/silver_rebuild"},                 # 운영 재처리 폴더에 쓰려 함
    {"report": "/tmp/x.json"},
    {"bronze_root": "/etc"},                                  # 읽기도 허용된 곳만
    {"job": "rm_rf"},
    {"tables": ["../../etc"]},
])
def test_measure_requests_outside_the_allowed_paths_are_refused(tmp_path, over):
    m, env = _measure_env(tmp_path)

    def no_spark(*a, **k):
        raise AssertionError("거부된 요청은 Spark 를 띄우지 않는다")
    put_measure(tmp_path, "measure-2", _build_args(m, **over), m / "syn/reports")
    assert R.process_pending(tmp_path, launcher=no_spark, env=env) == [("measure-2", "failed")]
    assert "측정 요청 거부" in (R.requests_dir(tmp_path) / "measure-2.failed").read_text()


def test_measure_stage_dir_must_be_under_measure_root(tmp_path):
    m, env = _measure_env(tmp_path)
    put_measure(tmp_path, "measure-3", _build_args(m), tmp_path / "elsewhere")
    assert R.process_pending(tmp_path, launcher=launcher(), env=env) == [("measure-3", "failed")]


def test_measure_may_read_production_roots_read_only(tmp_path):
    m, env = _measure_env(tmp_path)
    put_measure(tmp_path, "measure-4", _build_args(m, bronze_root="/data/bronze", silver_root="/data/silver"), m / "c/w")
    calls = []
    assert R.process_pending(tmp_path, launcher=launcher(seen=calls), env=env) == [("measure-4", "done")]


def test_measure_gold_stage_points_every_gold_root_at_measure_paths(tmp_path):
    m, env = _measure_env(tmp_path, GOLD_SILVER_ROOT="/data/silver", GOLD_BRONZE_ROOT="/data/bronze")
    calls = []

    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append((cmd, env))
        return Proc(0, "{}", stdout=stdout, stderr=stderr)
    args = {"job": "gold_stage", "silver_root": str(m / "s/_current"), "silver_rebuild_root": str(m / "s/rebuild"),
            "bronze_root": str(m / "s/bronze"), "stage_root": str(m / "s/gold_stage/r0")}
    put_measure(tmp_path, "measure-5", args, m / "s/reports")
    assert R.process_pending(tmp_path, launcher=run, env=env) == [("measure-5", "done")]
    cmd, e = calls[0]
    assert cmd[-4:] == ["--from", "2026-01-05", "--to", "2026-01-05"] and any(c.endswith("gold_stage.py") for c in cmd)
    assert "--files-until" not in cmd
    assert (e["GOLD_SILVER_ROOT"], e["GOLD_BRONZE_ROOT"], e["GOLD_SILVER_REBUILD_ROOT"], e["GOLD_STAGE_ROOT"]) == (
        str(m / "s/_current"), str(m / "s/bronze"), str(m / "s/rebuild"), str(m / "s/gold_stage/r0"))


def test_measure_verify_counts_a_failed_comparison_as_a_finished_measurement(tmp_path):
    m, env = _measure_env(tmp_path, SILVER_VERIFY_LOOKBACK_DAYS="1")
    calls = []

    def run(cmd, env, stdout=None, stderr=None, **kw):
        calls.append((cmd, env))
        return Proc(1, "전체: 실패", stdout=stdout, stderr=stderr)
    put_measure(tmp_path, "measure-6", {"job": "silver_verify", "bronze_root": str(m / "c/bronze"), "silver_root": str(m / "c/silver"),
                                        "verify_since": "2026-11-27"}, m / "c/w")
    assert R.process_pending(tmp_path, launcher=run, env=env) == [("measure-6", "done")]
    cmd, e = calls[0]
    assert cmd[-1].endswith("silver_verify.py")
    assert e["SILVER_VERIFY_SINCE"] == "2026-11-27" and "SILVER_VERIFY_LOOKBACK_DAYS" not in e
    assert e["SILVER_REBUILD_ROOT"].startswith(str(m)), "운영 포인터로 떨어지지 않는다"


def test_measure_point_publishes_only_inside_the_measure_folder(tmp_path):
    from streaming import silver_pointer as P
    m, env = _measure_env(tmp_path)
    rb = m / "s/rebuild"
    vd = P.version_dir(rb, "silver_weather", "m0")
    (vd / "event_date=2026-01-05").mkdir(parents=True)
    P.write_json_atomic(vd / P.BUILD_FILE, {"status": "ok", "partition_col": "event_date", "dates": {"2026-01-05": 4}})

    def no_spark(*a, **k):
        raise AssertionError("silver_point 는 Spark 를 띄우지 않는다")
    put_measure(tmp_path, "measure-7", {"job": "silver_point", "tables": ["silver_weather"], "version": "m0", "rebuild_root": str(rb)},
                m / "s/reports")
    assert R.process_pending(tmp_path, launcher=no_spark, env=env) == [("measure-7", "done")]
    assert P.active_dates(rb, "silver_weather") == {"2026-01-05": "m0"}


def test_measure_runs_niced_with_its_own_timeout(tmp_path):
    m, env = _measure_env(tmp_path)
    env.pop("GOLD_RUNNER_MEASURE_NICE")
    calls = []
    put_measure(tmp_path, "measure-8", _build_args(m), m / "syn/reports")
    R.process_pending(tmp_path, launcher=launcher(seen=calls), env=env)
    assert calls[0][0][:3] == ["nice", "-n", "10"]
    killed = []
    put_measure(tmp_path, "measure-9", _build_args(m, version="m1"), m / "syn/reports", created=2.0)
    t = iter(range(0, 10_000, 100))
    R.process_pending(tmp_path, launcher=launcher(polls=10_000), env={**env, "GOLD_RUNNER_MEASURE_TIMEOUT_SEC": "250"},
                      clock=lambda: next(t), sleep=lambda s: None, kill=killed.append)
    assert killed and "250초" in (R.requests_dir(tmp_path) / "measure-9.failed").read_text()
