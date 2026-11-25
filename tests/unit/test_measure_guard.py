"""STEP 2 측정 안전장치(scripts/measure_guard.py): 금지 시간대, 실행 전 검사, 실행 중 감시, 경로·DB·날짜 가드. Docker 없이 시험한다."""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import measure_guard as G  # noqa: E402

CFG = G.load_config(ROOT / "config" / "measure.yaml")
GIB = G.GIB


def kst(h, m, d=27):
    return datetime(2026, 11, d, h, m, tzinfo=G.KST)


def container(service, state="running", health="healthy", cpu=10.0, mem=0.3 * GIB, restarts=0):
    return {"service": service, "name": f"urbanstream-{service}", "state": state, "health": health, "cpu_pct": cpu,
            "mem_bytes": int(mem), "restarts": restarts}


def good_state(ts=1000.0, **over):
    cs = [container(s, health="" if s == "collector" else "healthy") for s in CFG["gate"]["expected_containers"]]
    st = {"ts": ts, "vm": {"mem_total": int(10.7 * GIB), "ncpu": 4}, "containers": cs, "disk_free": 100 * GIB,
          "runner_queue": {"pending": 0, "running": 0, "alive_age_sec": 3}, "power": {"ac": True, "caffeinate": True},
          "db": {"reprocess_running": 0, "new_gaps": 0}}
    st.update(over)
    return st


# ------------------------------------------------------------------ 설정

def test_config_loads_and_heavy_runs_wait_14_days_after_collection_start():
    assert CFG["collection_start"] == "2026-11-25"
    assert G.heavy_start(CFG) == date(2026, 12, 9)
    assert len(CFG["gate"]["expected_containers"]) == 12


def test_config_rejects_synthetic_dates_that_overlap_real_data(tmp_path):
    bad = (ROOT / "config" / "measure.yaml").read_text(encoding="utf-8").replace('first_date: "2026-01-05"', 'first_date: "2026-11-10"')
    p = tmp_path / "m.yaml"
    p.write_text(bad, encoding="utf-8")
    with pytest.raises(ValueError):
        G.load_config(p)


# ------------------------------------------------------------------ 금지 시간대

@pytest.mark.parametrize("h,m,forbidden", [
    (14, 15, True), (14, 20, True), (14, 39, True), (14, 40, False), (15, 14, False),
    (0, 44, False), (0, 45, True), (3, 0, True), (5, 29, True), (5, 30, True), (5, 40, False),   # 05:30 은 매시 :15~:40 에 걸린다
])
def test_forbidden_windows(h, m, forbidden):
    assert (G.forbidden_reason(kst(h, m), CFG) is not None) is forbidden


def test_next_allowed_and_free_until():
    assert G.next_allowed(kst(14, 20), CFG) == kst(14, 40)
    assert G.next_allowed(kst(1, 0), CFG) == kst(5, 40)
    assert G.free_until(kst(14, 41), CFG) == kst(15, 15)
    assert G.free_until(kst(0, 41), CFG) == kst(0, 45)


def test_window_check_needs_the_whole_run_to_fit_before_the_next_forbidden_window():
    assert G.window_check(kst(14, 41), 25 * 60, CFG)[0]
    ok, why = G.window_check(kst(14, 41), 40 * 60, CFG)
    assert not ok and "한 회 상한" in why
    ok, why = G.window_check(kst(14, 55), 25 * 60, CFG)
    assert not ok and "15:15" in why
    assert not G.window_check(kst(0, 40), 600, CFG)[0], "00:45 새벽 금지 시간대 전에 끝나지 않는다"


# ------------------------------------------------------------------ 실행 전 검사

def _fails(checks):
    return {c.name for c in checks if not c.ok}


def test_gate_passes_on_a_quiet_stack():
    checks = G.gate_checks(good_state(), CFG, now=kst(14, 41), expected_sec=600)
    assert _fails(checks) == set(), [c.__dict__ for c in checks if not c.ok]


@pytest.mark.parametrize("change,name", [
    (lambda s: s["containers"][1].update(health="unhealthy"), "컨테이너"),
    (lambda s: s["containers"].pop(0), "컨테이너"),
    (lambda s: s["containers"][2].update(mem=9 * GIB, mem_bytes=int(9 * GIB)), "메모리 여유"),
    (lambda s: s["runner_queue"].update(running=1), "다른 무거운 작업"),
    (lambda s: s.update(runner_queue=None), "다른 무거운 작업"),
    (lambda s: s["db"].update(reprocess_running=1), "다른 무거운 작업"),
    (lambda s: s.update(disk_free=10 * GIB), "디스크"),
    (lambda s: s["power"].update(ac=False), "전원"),
    (lambda s: s["power"].update(caffeinate=False), "전원"),
    (lambda s: s["db"].update(new_gaps=2), "수집"),
    (lambda s: s["db"].update(new_gaps=None), "수집"),
])
def test_gate_fails_on_each_condition(change, name):
    st = good_state()
    change(st)
    assert name in _fails(G.gate_checks(st, CFG, now=kst(14, 41), expected_sec=600))


def test_gate_fails_on_cpu_time_lock_and_big_output():
    assert "CPU" in _fails(G.gate_checks(good_state(), CFG, now=kst(14, 41), cpu_avg=250.0))
    assert "시간대" in _fails(G.gate_checks(good_state(), CFG, now=kst(14, 20)))
    assert "중복 실행" in _fails(G.gate_checks(good_state(), CFG, now=kst(14, 41), lock_exists=True))
    assert "디스크" in _fails(G.gate_checks(good_state(), CFG, now=kst(14, 41), expected_bytes=40 * GIB))


# ------------------------------------------------------------------ 실행 중 감시

def feed(wd, states):
    return [wd.feed(s) for s in states]


def test_watchdog_stays_quiet_on_a_normal_run():
    wd = G.Watchdog(CFG, targets=["gold-runner"])
    assert feed(wd, [good_state(ts=1000 + 10 * i) for i in range(10)]) == [None] * 10
    assert wd.summary()["abort_reason"] is None and wd.summary()["samples"] == 10


def test_watchdog_aborts_when_another_container_turns_unhealthy_but_not_the_target():
    wd = G.Watchdog(CFG, targets=["gold-runner"])
    s = good_state(ts=1010)
    next(c for c in s["containers"] if c["service"] == "gold-runner")["health"] = "unhealthy"
    assert feed(wd, [good_state(ts=1000), s]) == [None, None]
    s2 = good_state(ts=1020)
    next(c for c in s2["containers"] if c["service"] == "kafka")["health"] = "unhealthy"
    assert "kafka" in wd.feed(s2)


def test_watchdog_aborts_after_60s_of_cpu_above_340():
    wd = G.Watchdog(CFG)
    hot = lambda ts: good_state(ts=ts, containers=[container(s, cpu=30.0) for s in CFG["gate"]["expected_containers"]])  # 360%
    out = feed(wd, [hot(1000 + 10 * i) for i in range(7)])
    assert out[:6] == [None] * 6 and "CPU" in out[6]


def test_watchdog_cpu_spike_shorter_than_60s_is_tolerated():
    wd = G.Watchdog(CFG)
    hot = lambda ts: good_state(ts=ts, containers=[container(s, cpu=30.0) for s in CFG["gate"]["expected_containers"]])
    assert feed(wd, [hot(1000), hot(1010), hot(1020), good_state(ts=1030), hot(1040), hot(1050)]) == [None] * 6


def test_watchdog_aborts_on_low_memory_restarts_and_sleep_gaps():
    low = good_state(ts=1010)
    low["containers"][0]["mem_bytes"] = int(7 * GIB)
    assert "메모리" in feed(G.Watchdog(CFG), [good_state(ts=1000), low])[1]
    rs = good_state(ts=1010)
    next(c for c in rs["containers"] if c["service"] == "spark-bronze")["restarts"] = 1
    assert "재시작" in feed(G.Watchdog(CFG), [good_state(ts=1000), rs])[1]
    assert "잠듦" in feed(G.Watchdog(CFG), [good_state(ts=1000), good_state(ts=1045)])[1]


def test_watchdog_records_target_peak_memory():
    wd = G.Watchdog(CFG, targets=["gold-runner"])
    s = good_state(ts=1010)
    next(c for c in s["containers"] if c["service"] == "gold-runner")["mem_bytes"] = int(1.2 * GIB)
    feed(wd, [good_state(ts=1000), s, good_state(ts=1020)])
    assert wd.summary()["target_peak_mem_bytes"]["gold-runner"] == int(1.2 * GIB)


# ------------------------------------------------------------------ 가드

def test_measure_paths_must_be_under_data_measure_and_never_production():
    assert G.ensure_measure_path("data/measure/synthetic/s1", CFG) == (ROOT / "data/measure/synthetic/s1").resolve()
    for bad in ("data/bronze/x", "data/silver", "data/gold_stage/_requests", "data/measure", "/tmp/x", "data/measure/../bronze/x"):
        with pytest.raises(G.GuardError):
            G.ensure_measure_path(bad, CFG)


def test_synthetic_dates_stay_before_real_data():
    assert G.ensure_synthetic_date(date(2026, 1, 5), CFG)
    for bad in (date(2026, 1, 4), date(2026, 11, 3), date(2026, 11, 27)):
        with pytest.raises(G.GuardError):
            G.ensure_synthetic_date(bad, CFG)


def test_measure_db_guard_refuses_production_and_other_names():
    assert G.ensure_measure_db("urbanstream_measure", CFG) == "urbanstream_measure"
    for bad in ("urbanstream", "airflow", "postgres", "other"):
        with pytest.raises(G.GuardError):
            G.ensure_measure_db(bad, CFG)
    with pytest.raises(G.GuardError):
        G.ensure_measure_db("prod_db", CFG, production="prod_db")


def test_lock_is_exclusive_and_released(tmp_path):
    p = tmp_path / ".lock"
    with G.Lock(p, "a"):
        assert p.exists()
        with pytest.raises(G.GuardError):
            with G.Lock(p, "b"):
                pass
    assert not p.exists()
