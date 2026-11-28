"""scripts/recover.py 의 판단 로직을 가짜 docker 로 시험한다 (R6 Docker 무응답, R7 좀비·init, R8 시계, resume 순서, bronze --apply).
실제 Docker Desktop·Mac 은 재현하지 않는다. 실제 동작은 docs/recovery-drills.md 의 훈련 기록으로 남긴다."""
from __future__ import annotations

import subprocess
import time

import pytest

from recovery_support import TWELVE, FakeDocker, container

import hostlib as H
import recover as RC


@pytest.fixture
def cfg():
    c = H.load_config()
    c["diagnose"]["check_timeout_sec"] = 2
    return c


def no_db(sql, params=()):
    if "v_dq_latest" in sql:
        return []
    if "pg_locks" in sql:
        return []
    return [(30,)]


def names(fs):
    return [(f.name, f.ok) for f in fs]


def test_diagnose_with_docker_hung_answers_quickly_with_the_two_restart_commands(tmp_path, monkeypatch, cfg):
    FakeDocker(tmp_path, monkeypatch, mode="hang")
    t0 = time.monotonic()
    fs = RC.diagnose(cfg, query=no_db, expected=TWELVE)
    assert time.monotonic() - t0 < 8, "Docker 가 멈춰도 진단은 시간 제한 안에 끝난다"
    assert fs[0].name == "Docker" and not fs[0].ok
    assert "pkill -x Docker" in fs[0].next_cmd and "open -a Docker" in fs[0].next_cmd and "resume" in fs[0].next_cmd
    assert any(f.name == "나머지 점검" for f in fs), "Docker 가 응답하지 않으면 그 뒤 점검은 건너뛴다"


def test_diagnose_says_all_good_when_everything_is_healthy(tmp_path, monkeypatch, cfg, capsys):
    FakeDocker(tmp_path, monkeypatch)
    monkeypatch.setattr(RC, "check_bronze", lambda *a: RC.Finding("Bronze 체크포인트", True, "ok"))
    monkeypatch.setattr(RC, "check_topics", lambda *a: RC.Finding("토픽 설정", True, "ok"))
    monkeypatch.setattr(RC, "check_runner", lambda *a: RC.Finding("gold-runner 박동", True, "ok"))
    monkeypatch.setattr(RC, "check_host_tools", lambda: [])
    fs = RC.diagnose(cfg, query=no_db, expected=TWELVE)
    assert all(f.ok for f in fs), [f for f in fs if not f.ok]
    assert RC.print_findings(fs) == 0 and "모두 정상" in capsys.readouterr().out


def test_diagnose_points_each_container_problem_at_one_next_command(tmp_path, monkeypatch, cfg):
    cs = [container(s) for s in TWELVE if s not in ("monitor", "api")]
    cs += [container("monitor", state="exited", health="", exit_code=137), container("api", health="unhealthy"),
           ]
    cs[2] = container("spark-bronze", oom=True, restarts=2)
    cs[3] = container("spark-silver", init=False)
    FakeDocker(tmp_path, monkeypatch, containers=cs)
    fs = RC.diagnose(cfg, query=no_db, expected=TWELVE)
    by = {}
    for f in fs:
        by.setdefault(f.name, []).append(f)
    stopped = [f for f in by["컨테이너"] if "멈춤" in f.detail][0]
    assert "monitor" in stopped.detail and stopped.next_cmd == "python scripts/recover.py resume"
    bad = [f for f in by["컨테이너"] if "정상 아님" in f.detail][0]
    assert "api(unhealthy)" in bad.detail and "logs api" in bad.next_cmd
    assert "docker inspect" in by["OOM"][0].next_cmd and "urbanstream-spark-bronze" in by["OOM"][0].next_cmd, \
        "OOM 은 다시 만들기 전에 기록부터 (D-214)"
    init = by["init"][0]
    assert init.info and "spark-silver" in init.detail and "1-A" in init.detail


def test_diagnose_reports_dq_block_old_lock_and_stale_monitor(tmp_path, monkeypatch, cfg):
    FakeDocker(tmp_path, monkeypatch)

    def q(sql, params=()):
        if "v_dq_latest" in sql:
            return [("bus_arrival_future_event", "")]
        if "pg_locks" in sql:
            return [(4242, "uslock:gold_facts_incremental:gold_intraday#1:pid7:r", 3700)]
        return [(1500,)]
    fs = RC.check_db(q, 1800)
    by = {f.name: f for f in fs}
    assert not by["DQ 차단"].ok and "bus_arrival_future_event" in by["DQ 차단"].detail
    assert "pg_terminate_backend(4242)" in by["작업 잠금"].next_cmd and "61분" in by["작업 잠금"].detail
    assert not by["monitor 박동"].ok and "docker start urbanstream-monitor" == by["monitor 박동"].next_cmd


def test_diagnose_survives_a_database_it_cannot_read(cfg):
    def boom(sql, params=()):
        raise ConnectionError("refused")
    fs = RC.check_db(boom, 1800)
    assert len(fs) == 1 and not fs[0].ok and "읽지 못함" in fs[0].detail and "refused" not in fs[0].detail


def test_clock_finds_a_lagging_vm_and_suggests_the_docker_restart(tmp_path, monkeypatch, cfg, capsys):
    FakeDocker(tmp_path, monkeypatch, vm_skew=-1200)
    assert RC.clock(cfg) == 1
    out = capsys.readouterr().out
    assert "1200초 늦다" in out.replace("1199초", "1200초").replace("1201초", "1200초")
    assert "recover.py resume" in out and "open -a Docker" in out
    f = RC.check_clock(*H.vm_clock(2))
    assert not f.ok and f.next_cmd == "python scripts/recover.py clock"


def test_clock_is_fine_within_a_minute(tmp_path, monkeypatch, cfg):
    FakeDocker(tmp_path, monkeypatch, vm_skew=5)
    assert RC.clock(cfg) == 0


def test_resume_starts_everything_but_the_collector_first_and_the_collector_last(tmp_path, monkeypatch, cfg):
    fd = FakeDocker(tmp_path, monkeypatch, containers=[container(s, state="exited", health="") for s in TWELVE])
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    real = subprocess.run

    def runner(cmd, **kw):
        if cmd[:1] == ["bash"]:                   # stack.sh check 는 실제로 돌리지 않는다
            return subprocess.CompletedProcess(cmd, 0, "8/8 정상\n", "")
        return real(cmd, **kw)
    assert RC.resume(cfg, runner=runner, sleep=lambda s: None, expected=TWELVE) == 0
    starts = [c for c in fd.calls() if c["args"][:2] == ["compose", "start"]]
    assert starts[0]["args"] == ["compose", "start"] and "collector" not in starts[0]["profiles"].split(",")
    assert starts[1]["args"] == ["compose", "start", "collector"] and starts[1]["profiles"] == "core,collector", \
        "프로필에는 늘 core 가 들어간다 (D-291)"


def test_resume_does_not_start_the_collector_when_the_rest_is_not_healthy(tmp_path, monkeypatch, cfg, capsys):
    fd = FakeDocker(tmp_path, monkeypatch, containers=[container(s, state="exited", health="") for s in TWELVE], on_start_healthy=False)
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    cfg["resume"]["healthy_wait_sec"] = 0
    assert RC.resume(cfg, sleep=lambda s: None, expected=TWELVE) == 1
    assert not [c for c in fd.calls() if c["args"] == ["compose", "start", "collector"]]
    assert "diagnose" in capsys.readouterr().out


def test_resume_refuses_while_a_measurement_or_drill_holds_the_lock(tmp_path, monkeypatch, cfg):
    fd = FakeDocker(tmp_path, monkeypatch)
    monkeypatch.setattr(RC, "measure_lock_held", lambda: "123 2026-12-10T14:45 M6 1x")
    assert RC.resume(cfg, expected=TWELVE) == 2
    assert fd.calls() == []
    monkeypatch.setattr(RC, "measure_lock_held", lambda: "123 2026-12-16T14:45 drill:docker-restart")
    assert RC.resume(cfg, expected=TWELVE) == 2, "훈련 잠금도 막는다 (훈련 안에서 부를 때만 --in-drill)"


def bronze_runner(fd_calls, status_lines, apply_ok=True):
    real = subprocess.run

    def runner(cmd, **kw):
        if "bronze_recover.py" in " ".join(map(str, cmd)):
            fd_calls.append(cmd)
            if "--apply" in cmd:
                return subprocess.CompletedProcess(cmd, 0 if apply_ok else 1, "bus_arrival      지워진 구간 ...\n"
                                                   "                 적용함. 바꾸기 전 파일: /r/data/_recovery/20261216-150000/bus_arrival\n", "")
            return subprocess.CompletedProcess(cmd, 0, "\n".join(status_lines) + "\n", "")
        return real(cmd, **kw)
    return runner


def test_bronze_preview_changes_nothing(tmp_path, monkeypatch, cfg, capsys):
    fd = FakeDocker(tmp_path, monkeypatch)
    calls = []
    lines = ["bus_arrival      지워진 구간  batch 41: urbanstream.bus.arrival[0] 10→900  (잃은 메시지 890건)"]
    assert RC.bronze(["bus_arrival"], False, runner=bronze_runner(calls, lines)) == 0
    assert fd.calls() == [] and all("--apply" not in c for c in calls)
    assert "recover.py bronze bus_arrival --apply" in capsys.readouterr().out


def test_bronze_apply_stops_checks_fixes_starts_and_verifies(tmp_path, monkeypatch, cfg):
    fd = FakeDocker(tmp_path, monkeypatch)
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    monkeypatch.setattr(RC, "newest_file", lambda d: time.time() + 1)
    calls = []
    lines = ["bus_arrival      지워진 구간  batch 41: t[0] 10→900  (잃은 메시지 890건)"]
    assert RC.bronze(["bus_arrival"], True, ask=lambda q: "yes", runner=bronze_runner(calls, lines), sleep=lambda s: None) == 0
    seq = [c["args"][:2] for c in fd.calls() if c["args"][0] in ("stop", "start")]
    assert seq == [["stop", "urbanstream-spark-bronze"], ["start", "urbanstream-spark-bronze"]]
    assert any("--apply" in c for c in calls)


def test_bronze_apply_needs_yes(tmp_path, monkeypatch, cfg):
    fd = FakeDocker(tmp_path, monkeypatch)
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    calls = []
    lines = ["bus_arrival      지워진 구간  batch 41: t[0] 10→900  (잃은 메시지 890건)"]
    assert RC.bronze(["bus_arrival"], True, ask=lambda q: "no", runner=bronze_runner(calls, lines)) == 0
    assert not [c for c in fd.calls() if c["args"][0] == "stop"] and all("--apply" not in c for c in calls)


def test_bronze_apply_does_not_touch_the_checkpoint_when_stop_hits_a_zombie(tmp_path, monkeypatch, cfg, capsys):
    FakeDocker(tmp_path, monkeypatch, fail={"stop urbanstream-spark-bronze":
                                            "Error response from daemon: cannot stop container: PID 4711 is zombie and can not be killed"})
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    calls = []
    lines = ["bus_arrival      지워진 구간  batch 41: t[0] 10→900  (잃은 메시지 890건)"]
    assert RC.bronze(["bus_arrival"], True, yes=True, runner=bronze_runner(calls, lines)) == 1
    out = capsys.readouterr().out
    assert "docker kill urbanstream-spark-bronze" in out and "1-A" in out
    assert all("--apply" not in c for c in calls)


def test_bronze_failed_verification_prints_how_to_undo_but_does_not_undo(tmp_path, monkeypatch, cfg, capsys):
    FakeDocker(tmp_path, monkeypatch, on_start_healthy=False)
    monkeypatch.setattr(RC, "measure_lock_held", lambda: None)
    calls = []
    lines = ["bus_arrival      지워진 구간  batch 41: t[0] 10→900  (잃은 메시지 890건)"]
    assert RC.bronze(["bus_arrival"], True, yes=True, runner=bronze_runner(calls, lines), sleep=lambda s: None, verify_sec=0) == 1
    out = capsys.readouterr().out
    assert "자동으로 되돌리지 않는다" in out and "cp -p /r/data/_recovery/20261216-150000/bus_arrival/offsets/*" in out


def test_host_tools_never_depend_on_launchd():
    """R11: launchd 가 주기 실행을 하지 않았다(D-290). STEP 3B 호스트 도구는 launchctl 을 부르지 않는다."""
    for name in ("recover.py", "host_watch.py", "drill.py", "hostlib.py"):
        text = (H.ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert "launchctl" not in text and "LaunchAgents" not in text, name


def test_host_tools_compile_on_python_3_11_syntax():
    """Mac 은 Python 3.11 (D-286 류 사고 방지). ruff target py311 이 CI 에서도 보지만 여기서 한 번 더 문법만 본다."""
    import ast
    for name in ("recover.py", "host_watch.py", "drill.py", "hostlib.py"):
        ast.parse((H.ROOT / "scripts" / name).read_text(encoding="utf-8"), feature_version=(3, 11))
