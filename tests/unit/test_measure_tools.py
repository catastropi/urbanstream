"""STEP 2 표본 수집(measure_sample), 환경 기록(measure_env), 하네스(measure_run), 결과 정리(measure_report). Docker·Spark·DB 없이 시험한다."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import measure_env as E  # noqa: E402
import measure_guard as G  # noqa: E402
import measure_report as REP  # noqa: E402
import measure_run as RUN  # noqa: E402
import measure_sample as S  # noqa: E402

CFG = G.load_config(ROOT / "config" / "measure.yaml")


# ------------------------------------------------------------------ measure_sample

def test_parse_docker_stats_and_inspect():
    stats = "\n".join(json.dumps(x) for x in [
        {"Name": "urbanstream-kafka", "CPUPerc": "12.50%", "MemUsage": "973.2MiB / 1.5GiB", "PIDs": "449"},
        {"Name": "urbanstream-api", "CPUPerc": "0.00%", "MemUsage": "82MiB / 256MiB", "PIDs": "--"}])
    s = S.parse_stats(stats)
    assert s["urbanstream-kafka"] == {"cpu_pct": 12.5, "mem_bytes": int(973.2 * 1024 ** 2), "pids": 449}
    assert s["urbanstream-api"]["pids"] == 0
    insp = json.dumps([{"Name": "/urbanstream-kafka", "RestartCount": 2, "Config": {"Labels": {"com.docker.compose.service": "kafka"}},
                        "State": {"Status": "running", "OOMKilled": False, "Health": {"Status": "unhealthy"}},
                        "HostConfig": {"Memory": 1610612736, "NanoCpus": 0}},
                       {"Name": "/urbanstream-collector", "Config": {"Labels": {"com.docker.compose.service": "collector"}},
                        "State": {"Status": "running"}, "HostConfig": {}}])
    i = S.parse_inspect(insp)
    assert i["urbanstream-kafka"]["service"] == "kafka" and i["urbanstream-kafka"]["health"] == "unhealthy"
    assert i["urbanstream-kafka"]["restarts"] == 2 and i["urbanstream-collector"]["health"] == ""


def test_parse_size_units():
    assert S.parse_size("1.5GiB") == int(1.5 * 1024 ** 3) and S.parse_size("12kB") == 12000 and S.parse_size("0B") == 0


def _touch(p: Path, t: float):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("x")
    os.utime(p, (t, t))


def test_checkpoint_batches_measure_offsets_to_commits_and_resume_from_cursor(tmp_path):
    q = tmp_path / "bronze" / "bus_position"
    for n, (a, b) in enumerate([(100, 112), (160, 175), (220, 290)]):
        _touch(q / "offsets" / str(n), a)
        _touch(q / "commits" / str(n), b)
    _touch(q / "offsets" / "3", 280)                              # 진행 중 (아직 commit 없음)
    rows, cur = S.checkpoint_batches(tmp_path, ["bronze", "silver"], {})
    assert [(r["batch"], r["sec"]) for r in rows] == [(0, 12.0), (1, 15.0), (2, 70.0)]
    assert cur == {"bronze/bus_position": 2}
    _touch(q / "commits" / "3", 300)
    rows, cur = S.checkpoint_batches(tmp_path, ["bronze"], cur)
    assert [r["batch"] for r in rows] == [3] and cur["bronze/bus_position"] == 3


def test_runner_queue_and_finished_requests(tmp_path):
    req = tmp_path / "_requests"
    req.mkdir()
    for rid, mark in (("a", "done"), ("b", "running"), ("c", None), ("measure-x", "done")):
        (req / f"{rid}.json").write_text(json.dumps({"id": rid, "key": f"gold_intraday__{rid}", "kind": "gold_stage", "files_until": 1.0}))
        if mark:
            (req / f"{rid}.{mark}").write_text(json.dumps({"elapsed_sec": 42.0, "returncode": 0}))
    (req / ".runner_alive").write_text("1")
    q = S.runner_queue(tmp_path)
    assert (q["pending"], q["running"]) == (1, 1)
    rows, last = S.runner_finished(tmp_path, 0)
    assert [(r["id"], r["elapsed_sec"], r["incremental"]) for r in rows] == [("a", 42.0, True)], "측정 요청은 뺀다"
    assert S.runner_finished(tmp_path, last)[0] == []


def fake_docker(stats=None, inspect=None, info=None, fail=False):
    def run(cmd, capture_output=True, text=True, timeout=None):
        if fail:
            return subprocess.CompletedProcess(cmd, 1, "", "Cannot connect")
        if cmd[:2] == ["docker", "ps"]:
            out = "id1\nid2\n"
        elif cmd[:2] == ["docker", "inspect"]:
            out = json.dumps(inspect or [])
        elif cmd[:2] == ["docker", "stats"]:
            out = "\n".join(json.dumps(x) for x in stats or [])
        elif cmd[:2] == ["docker", "info"]:
            out = json.dumps(info or {"MemTotal": 10 * 1024 ** 3, "NCPU": 4})
        else:
            return subprocess.CompletedProcess(cmd, 1, "", "")
        return subprocess.CompletedProcess(cmd, 0, out, "")
    return run


INSPECT = [{"Name": "/urbanstream-kafka", "Config": {"Labels": {"com.docker.compose.service": "kafka"}},
            "State": {"Status": "running", "Health": {"Status": "healthy"}}, "HostConfig": {}},
           {"Name": "/urbanstream-spark-gold", "Config": {"Labels": {"com.docker.compose.service": "spark-gold"}},
            "State": {"Status": "exited"}, "HostConfig": {}}]
STATS = [{"Name": "urbanstream-kafka", "CPUPerc": "50%", "MemUsage": "1GiB / 1.5GiB", "PIDs": "10"}]


def test_sample_once_appends_one_line_with_batches(tmp_path):
    cfg = json.loads(json.dumps(CFG))
    (tmp_path / "data" / "checkpoints" / "silver" / "silver_weather" / "offsets").mkdir(parents=True)
    _touch(tmp_path / "data/checkpoints/silver/silver_weather/offsets/0", 10)
    _touch(tmp_path / "data/checkpoints/silver/silver_weather/commits/0", 14)
    st = S.sample_once(cfg, tmp_path, fake_docker(STATS, INSPECT))
    p = S.append(cfg, st, tmp_path)
    line = json.loads(p.read_text().splitlines()[0])
    assert line["docker"] is True and G.cpu_sum(line) == 50.0
    assert {c["service"]: c["state"] for c in line["containers"]} == {"kafka": "running", "spark-gold": "exited"}
    assert line["batches"][0]["sec"] == 4.0 and line["vm"]["ncpu"] == 4
    assert S.sample_once(cfg, tmp_path, fake_docker(STATS, INSPECT))["batches"] == [], "같은 배치를 두 번 적지 않는다"


def test_loop_samples_on_wall_clock_ticks_and_survives_a_failing_round(tmp_path, capsys):
    cfg = json.loads(json.dumps(CFG))
    calls = {"n": 0}

    def flaky(cmd, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")      # 첫 회차의 docker 호출이 예외 → 그 회차만 실패
        return fake_docker(STATS, INSPECT)(cmd, **kw)
    slept = []
    t = iter([1000.0, 1290.0, 5000.0])          # 세 번째는 잠들었다 깬 뒤(밀린 회차 여러 개)
    assert S.loop(cfg, 300, tmp_path, flaky, max_iter=4, clock=lambda: next(t), sleep=slept.append) == 0
    assert slept == [200.0, 210.0, 100.0], "다음 300초 배수까지만 잔다. 밀린 회차는 채우지 않는다"
    lines = [json.loads(x) for f in (tmp_path / "data/measure/samples").glob("*.jsonl") for x in f.read_text().splitlines()]
    assert len(lines) == 3 and "표본 실패" in capsys.readouterr().err
    assert not (tmp_path / "data/measure/samples" / S.PID_FILE).exists(), "끝나면 PID 파일을 지운다"


def test_pid_file_allows_one_loop_and_replaces_a_stale_one(tmp_path):
    sdir = tmp_path / "s"
    p = S.take_pid(sdir)
    assert S.read_pid(sdir) == os.getpid()
    p.write_text("999999999\n")                 # 끝난 프로세스가 남긴 파일
    assert S.read_pid(sdir) is None
    S.take_pid(sdir)
    assert S.read_pid(sdir) == os.getpid()
    p.write_text(f"{os.getppid()}\n")           # 살아 있는 다른 프로세스
    with pytest.raises(RuntimeError):
        S.take_pid(sdir)


def test_status_reports_running_late_and_stopped(tmp_path, capsys):
    cfg = json.loads(json.dumps(CFG))
    sdir = tmp_path / "data/measure/samples"
    assert S.status(cfg, tmp_path) == 1
    S.take_pid(sdir)
    (sdir / "2026-11-25.jsonl").write_text(json.dumps({"ts": 1000.0}) + "\n")
    assert S.status(cfg, tmp_path, now=1300.0) == 0
    assert S.status(cfg, tmp_path, now=1000.0 + 3 * 300) == 2
    assert "돌고 있음" in capsys.readouterr().out


def test_sample_without_docker_records_the_gap(tmp_path):
    st = S.collect_state(CFG, tmp_path, fake_docker(fail=True))
    assert st["docker"] is False and st["containers"] == []


# ------------------------------------------------------------------ measure_env

def test_env_record_keeps_only_limit_variables_never_secrets():
    env = {"KAFKA_MEM_LIMIT": "1536m", "AIRFLOW_PARALLELISM": "4", "POSTGRES_PASSWORD": "x", "SEOUL_API_KEY": "k",
           "GOLD_LOADER_PASSWORD": "p", "AIRFLOW_FERNET_KEY": "f", "API_KEYS": "a", "ALERT_WEBHOOK_URL": "u", "MEASURE_MEM_LIMIT": "1g",
           "GOLD_RUNNER_MEASURE_NICE": "10", "SPARK_VERSION": "4.1.3", "MEASURE_SECRET_TOKEN": "nope"}
    assert E.safe_env(env) == {"AIRFLOW_PARALLELISM": "4", "GOLD_RUNNER_MEASURE_NICE": "10", "KAFKA_MEM_LIMIT": "1536m",
                               "MEASURE_MEM_LIMIT": "1g", "SPARK_VERSION": "4.1.3"}


def test_env_collect_writes_json_without_secret_names(tmp_path):
    (tmp_path / ".env").write_text("POSTGRES_PASSWORD=supersecret\nKAFKA_MEM_LIMIT=1536m\n")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "a.yaml").write_text("x: 1")
    env = E.collect(CFG, tmp_path, fake_docker(STATS, INSPECT), note="t")
    p = E.write(CFG, env, tmp_path)
    text = p.read_text()
    assert "supersecret" not in text and "POSTGRES_PASSWORD" not in text
    assert json.loads(text)["dotenv"] == {"KAFKA_MEM_LIMIT": "1536m"} and "a.yaml" in json.loads(text)["code"]["config_sha16"]


# ------------------------------------------------------------------ measure_run

def test_plan_reps_follows_the_design_rule():
    assert RUN.plan_reps(30, CFG, None) == (True, 5), "5분 미만: 첫 회는 예열 + 5회"
    assert RUN.plan_reps(600, CFG, None) == (False, 2), "5~30분: 예열 없이 3회"
    assert RUN.plan_reps(30, CFG, 2) == (False, 1)


def test_parse_sleep_log_finds_sleep_inside_the_run_only():
    log = ("2026-11-27 14:41:00 +0900 Wake  \tDarkWake from Deep Idle\n"
           "2026-11-27 14:50:12 +0900 Sleep  \tEntering Sleep state due to 'Idle Sleep'\n")
    t = lambda h, m: datetime(2026, 11, 27, h, m, tzinfo=G.KST)
    assert RUN.parse_sleep_log(log, t(14, 45), t(14, 55)) is True
    assert RUN.parse_sleep_log(log, t(14, 51), t(14, 55)) is False


def test_runner_path_maps_host_data_to_container_mount():
    assert RUN.runner_path(ROOT / "data/measure/synthetic/s1/bronze") == "/data/measure/synthetic/s1/bronze"
    assert RUN.measure_container_path(ROOT / "data/measure/x") == "/opt/urbanstream/data/measure/x"


class FakeRunner:
    """gold-runner 대신: 요청 파일을 보고 after 초 뒤 done(또는 .cancel 이 있으면 failed)."""

    def __init__(self, req: Path, after=0.05):
        self.req, self.after, self.stop = req, after, False
        (req).mkdir(parents=True, exist_ok=True)
        (req / ".runner_alive").write_text("x")
        self.t = threading.Thread(target=self.loop, daemon=True)
        self.t.start()

    def loop(self):
        while not self.stop:
            for p in self.req.glob("measure-*.json"):
                rid = p.stem
                if any((self.req / f"{rid}.{k}").exists() for k in ("done", "failed")):
                    continue
                if (self.req / f"{rid}.cancel").exists():
                    (self.req / f"{rid}.failed").write_text(json.dumps({"returncode": None, "stderr_tail": "Airflow 가 기다리다 취소해서 실행 중에 껐다"}))
                elif time.time() - p.stat().st_mtime > self.after:
                    (self.req / f"{rid}.done").write_text(json.dumps({"returncode": 0, "elapsed_sec": 1.5, "mem_max_bytes": 5}))
            time.sleep(0.01)


def test_runner_submit_and_wait_until_done(tmp_path):
    fr = FakeRunner(tmp_path / "data/gold_stage/_requests")
    r = RUN.Runner(CFG, root=tmp_path, sampler=lambda: {"ts": time.time(), "containers": [], "vm": None}, sleep=lambda s: time.sleep(0.01))
    assert r.alive()
    (tmp_path / "data/measure").mkdir(parents=True)
    rid = r.submit("silver_build", {"tables": ["silver_weather"]}, RUN.date(2026, 1, 5), RUN.date(2026, 1, 5), tmp_path / "data/measure/w")
    body = json.loads((r.req_dir / f"{rid}.json").read_text())
    assert body["kind"] == "measure" and body["key"] == f"measure__{rid}" and body["stage_dir"] == "/data/measure/w"
    status, res, reason = r.wait(rid, G.Watchdog(CFG), max_sec=30)
    fr.stop = True
    assert (status, res["elapsed_sec"], reason) == ("done", 1.5, None)


def test_runner_wait_cancels_when_the_watchdog_fires(tmp_path):
    fr = FakeRunner(tmp_path / "data/gold_stage/_requests", after=60)
    r = RUN.Runner(CFG, root=tmp_path, sampler=lambda: {"ts": time.time(), "containers": [], "vm": None}, sleep=lambda s: time.sleep(0.01))
    rid = r.submit("gold_stage", {}, RUN.date(2026, 1, 5), RUN.date(2026, 1, 5), tmp_path / "data/measure/w")

    class Fire(G.Watchdog):
        def feed(self, state):
            return "시험"
    status, res, reason = r.wait(rid, Fire(CFG), max_sec=30)
    fr.stop = True
    assert status == "cancelled" and reason == "시험" and (r.req_dir / f"{rid}.cancel").exists()


def test_compact_partition_merges_by_mtime_hour_and_keeps_rows(tmp_path):
    src = tmp_path / "src" / "event_date=2026-11-27"
    src.mkdir(parents=True)
    base = datetime(2026, 11, 27, 10, 0, tzinfo=G.KST).timestamp()
    for i in range(6):
        p = src / f"part-{i}.parquet"
        pq.write_table(pa.table({"a": [i, i + 10]}), p)
        os.utime(p, (base + i * 1200, base + i * 1200))            # 20분 간격 → 10시 3개, 11시 3개
    (src / "_SUCCESS").write_text("")
    r = RUN.compact_partition(src, tmp_path / "dst", meta={"run_id": "c1"})
    assert (r["files_in"], r["files_out"], r["rows"]) == (6, 2, 12)
    out = sorted((tmp_path / "dst").glob("*.parquet"))
    assert [p.name for p in out] == ["part-10-copy.parquet", "part-11-copy.parquet"]
    assert pq.read_schema(out[0]).metadata[b"urbanstream.measure_copy"] == b"true"
    assert sorted(pq.read_table(out[0]).column("a").to_pylist()) == [0, 1, 2, 10, 11, 12]


def test_harness_commands_that_measure_take_the_lock():
    assert {"synth-silver", "synth-stage", "synth-load", "m7", "copy", "selftest"} <= RUN.NEEDS_LOCK
    assert "load-inner" not in RUN.NEEDS_LOCK
    with pytest.raises(SystemExit):
        RUN.main(["load-inner", "--stage", "/tmp/x"])                # 컨테이너 밖에서는 거부


# ------------------------------------------------------------------ measure_report

def test_compare_ab_verdicts():
    assert REP.compare_ab([100, 105, 110], [60, 62, 65], 0.3)["verdict"] == "빨라졌다"
    c = REP.compare_ab([100, 105, 110], [60, 62, 65], 0.3)
    assert c["meets_cut"] and c["ratio"] == pytest.approx(0.59, abs=0.01)
    assert REP.compare_ab([100, 105, 110], [90, 104, 120], 0.3)["verdict"] == "차이 불분명"
    assert REP.compare_ab([100, 101], [80, 85], 0.3)["meets_cut"] is False, "빨라졌어도 30% 미만이면 compaction 조건이 아니다"
    assert REP.compare_ab([], [1], 0.3)["verdict"] == "측정 부족"


def _row(item, sec, *, valid=True, warm=False, scale=None, mem=None, data="합성", started="2026-12-10T10:41:00+09:00", **kw):
    return {"item": item, "elapsed_sec": sec, "valid": valid, "warmup": warm, "scale": scale, "mem_max_bytes": mem, "data": data,
            "started_kst": started, **kw}


def test_active_summary_drops_warmup_and_invalid_runs():
    rows = [_row("m6_synth_silver", 300, warm=True, scale=1), _row("m6_synth_silver", 100, scale=1), _row("m6_synth_silver", 110, scale=1),
            _row("m6_synth_silver", 120, scale=1), _row("m6_synth_silver", 999, valid=False, scale=1, invalid_reason="잠듦")]
    s = REP.active_summary(rows, CFG)[0]
    assert (s["valid_n"], s["invalid_n"], s["median_sec"], s["min_sec"], s["max_sec"]) == (3, 1, 110, 100, 120)
    assert s["notes"] == "잠듦"


def test_m2_decision_uses_memory_and_time_ratio():
    mib = 1024 ** 2
    act = REP.active_summary([_row("m6_synth_load", 10, scale=5, mem=300 * mib), _row("m6_synth_load", 20, scale=10, mem=400 * mib)], CFG)
    assert REP.m2_decision(act, CFG)["verdict"].startswith("조건 안 넘음")
    act = REP.active_summary([_row("m6_synth_load", 10, scale=5, mem=300 * mib), _row("m6_synth_load", 30, scale=10, mem=400 * mib)], CFG)
    assert REP.m2_decision(act, CFG)["verdict"].startswith("조건 넘음")
    act = REP.active_summary([_row("m6_synth_load", 10, scale=5), _row("m6_synth_load", 15, scale=10, mem=600 * mib)], CFG)
    assert REP.m2_decision(act, CFG)["verdict"].startswith("조건 넘음")
    assert REP.m2_decision([], CFG)["verdict"] == "측정 부족"


def test_coverage_marks_days_with_short_gaps_as_full():
    day = datetime(2026, 11, 26, tzinfo=G.KST).timestamp()
    full = [{"ts": day + i * 300} for i in range(288)]
    holey = [{"ts": day + 86400 + i * 300} for i in range(288) if not (100 <= i < 120)]   # 100분 빔
    cov = REP.coverage(full + holey, 300)
    assert cov["2026-11-26"]["full"] and not cov["2026-11-27"]["full"]
    off = [{"ts": s["ts"], "docker": not (100 <= i < 120)} for i, s in enumerate(full)]   # Docker 꺼짐 100분
    assert not REP.coverage(off, 300)["2026-11-26"]["full"]


def test_build_writes_csv_and_only_replaces_marked_sections(tmp_path):
    cfg = json.loads(json.dumps(CFG))
    res = tmp_path / "data/measure/results"
    res.mkdir(parents=True)
    with open(res / "m7_build.jsonl", "w") as f:
        for v, s in (("original", 100), ("copy", 60), ("original", 104), ("copy", 61)):
            f.write(json.dumps(_row("m7_build", s, variant=v, input_date="2026-12-10", data="실제")) + "\n")
    smp = tmp_path / "data/measure/samples"
    smp.mkdir(parents=True)
    t0 = datetime(2026, 11, 26, 12, tzinfo=G.KST).timestamp()
    with open(smp / "2026-11-26.jsonl", "w") as f:
        for i in range(3):
            f.write(json.dumps({"ts": t0 + 300 * i, "docker": True, "vm": {"mem_total": 10 * 1024 ** 3},
                                "containers": [{"service": "kafka", "state": "running", "cpu_pct": 10 + i, "mem_bytes": 1024 ** 3, "pids": 5}],
                                "batches": [{"layer": "bronze", "query": "audit", "batch": i, "start": t0, "end": t0 + 300 * i, "sec": 5.0 + i}],
                                "runner_done": [{"id": f"r{i}", "kind": "gold_stage", "key": "gold_intraday__x", "result": "done",
                                                 "elapsed_sec": 30 + i, "end": t0 + 300 * i, "incremental": True}]}) + "\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/performance.md").write_text(REP.DOC_SKELETON.replace("## 한계", "내가 쓴 문단\n\n## 한계"), encoding="utf-8")
    out = REP.build(cfg, tmp_path)
    doc = (tmp_path / "docs/performance.md").read_text(encoding="utf-8")
    assert "내가 쓴 문단" in doc and "빨라졌다" in doc and "| bronze | audit |" in doc
    assert out["m7"][0]["meets_h5"] is True and out["m4"][0]["runs"] == 3
    assert (tmp_path / "docs/performance/active.csv").exists() and (tmp_path / "docs/performance/m1_streaming.csv").exists()
    REP.build(cfg, tmp_path)
    assert (tmp_path / "docs/performance.md").read_text(encoding="utf-8").count("<!-- measure:m7 -->") == 1, "다시 돌려도 표가 겹치지 않는다"
