"""scripts/host_watch.py 판정 (R6 Docker 무응답, R8 시계, R9 Airflow 폭주, R11 launchd 없이 반복, R12 monitor 정지).
판정 함수(decide)는 순수하다: 관찰 dict + 이전 상태 → 보낼 알림 + 새 상태. 관찰은 가짜 docker 로도 한 번 끝까지 돌려 본다."""
from __future__ import annotations

import json

import pytest

from recovery_support import TWELVE, FakeDocker, container

import hostlib as H
import host_watch as W


@pytest.fixture
def cfg():
    return H.load_config()


def cool(level):
    return {"critical": 1800, "error": 7200, "warning": 21600, "info": 0}[level]


def obs(t, *, docker=True, rows=None, monitor_age=30.0, skew=None, mono=None):
    o = {"ts": t, "mono": t if mono is None else mono, "docker_ok": docker, "docker_detail": "" if docker else "10초 안에 응답 없음"}
    if docker:
        o["containers"] = [container(s) for s in TWELVE] if rows is None else rows
        o["monitor_age"] = monitor_age
        if skew is not None:
            o["clock_skew"] = skew
    return o


def run(cfg, seq):
    st, sent = {}, []
    for o in seq:
        a, st = W.decide(o, st, cfg, TWELVE, cool)
        sent.append(a)
    return sent, st


def test_docker_unresponsive_three_times_in_a_row_is_one_critical(cfg):
    sent, st = run(cfg, [obs(60 * i, docker=False) for i in range(1, 6)])
    flat = [a for s in sent for a in s]
    assert [a["level"] for a in flat] == ["critical"] and sent[2] == flat, "세 번째 실패에서 한 번"
    assert "pkill -x Docker" in flat[0]["body"] and "resume" in flat[0]["body"]
    sent2, _ = run(cfg, [obs(60 * i, docker=False) for i in range(1, 4)] + [obs(240)])
    assert sent2[-1][0]["resolved"] and sent2[-1][0]["title"].startswith("해소: Docker")


def test_two_failures_then_success_sends_nothing(cfg):
    sent, st = run(cfg, [obs(60, docker=False), obs(120, docker=False), obs(180)])
    assert all(s == [] for s in sent) and st["docker_fail"] == 0


def test_container_down_only_after_five_minutes_and_resolves_once(cfg):
    down = [container(s) for s in TWELVE if s != "spark-silver"] + [container("spark-silver", state="exited", health="")]
    seq = [obs(60 * i, rows=down) for i in range(0, 7)] + [obs(480), obs(540)]
    sent, _ = run(cfg, seq)
    when = [i for i, s in enumerate(sent) if s]
    assert when == [5, 7], "5분째에 error, 돌아오면 해소 한 번"
    assert sent[5][0]["key"] == "host:container:spark-silver" and sent[5][0]["level"] == "error"
    assert sent[7][0]["resolved"]


def test_same_problem_is_not_resent_within_cooldown(cfg):
    down = [container(s) for s in TWELVE if s != "api"]           # api 가 아예 없음
    sent, _ = run(cfg, [obs(60 * i, rows=down) for i in range(0, 124)])
    assert sum(len(s) for s in sent) == 1, "2시간(cooldown) 안에는 한 번만"
    sent, _ = run(cfg, [obs(60 * i, rows=down) for i in range(0, 126)])
    assert [i for i, s in enumerate(sent) if s] == [5, 125], "2시간이 지나면 다시 한 번"


def test_wake_grace_holds_container_alerts_but_not_the_clock(cfg):
    unhealthy = [container(s, health="unhealthy" if s == "kafka" else "healthy") for s in TWELVE]
    seq = [obs(0)]
    t = 3600.0                                                      # 벽시계는 1시간, 단조 시계는 60초만 흘렀다 = 잠들었다 깼다
    seq += [obs(t + 60 * i, mono=60 + 60 * i, rows=unhealthy, skew=-1500 if i == 0 else None) for i in range(0, 9)]
    sent, st = run(cfg, seq)
    flat = [a for s in sent for a in s]
    assert [a["key"] for a in flat] == ["host:clock"], "깨어난 직후 10분은 컨테이너 알림을 미루고, 시계는 바로 알린다"
    assert flat[0]["level"] == "error" and "25분" not in flat[0]["title"] and "1500초 늦습니다" in flat[0]["title"]
    more, _ = run(cfg, seq + [obs(t + 60 * i, mono=60 + 60 * i, rows=unhealthy) for i in range(9, 12)])
    assert any(a["key"] == "host:container:kafka" for s in more for a in s), "유예가 끝나면 알린다"


def test_clock_thresholds_warning_and_error(cfg):
    for skew, lvl in ((-30, None), (-90, "warning"), (700, "error")):
        sent, _ = run(cfg, [obs(0, skew=skew)])
        got = [a["level"] for a in sent[0] if a["key"] == "host:clock"]
        assert got == ([] if lvl is None else [lvl])


def test_monitor_heartbeat_stale_is_an_error_and_unknown_db_is_not(cfg):
    sent, _ = run(cfg, [obs(0, monitor_age=900)])
    assert [a["key"] for a in sent[0]] == ["host:monitor"] and "docker start urbanstream-monitor" in sent[0][0]["body"]
    sent, st = run(cfg, [obs(0, monitor_age=900), obs(60, monitor_age=None)])
    assert sent[1] == [] and "host:monitor" in st["active"], "DB 를 못 읽으면 판정을 미룬다(해소로 보지 않는다)"


def test_airflow_runaway_needs_five_minutes(cfg):
    hot = [container(s, pids=659) if s == "airflow-scheduler" else container(s) for s in TWELVE]
    sent, _ = run(cfg, [obs(60 * i, rows=hot) for i in range(0, 6)])
    when = [i for i, s in enumerate(sent) if s]
    assert when == [5] and sent[5][0]["key"] == "host:airflow:airflow-scheduler" and sent[5][0]["level"] == "warning"


def test_while_docker_is_down_other_problems_are_neither_resent_nor_resolved(cfg):
    down = [container(s) for s in TWELVE if s != "api"]
    seq = [obs(60 * i, rows=down) for i in range(0, 6)] + [obs(400 + 60 * i, docker=False) for i in range(5)]
    sent, st = run(cfg, seq)
    keys = [a["key"] for s in sent for a in s]
    assert keys == ["host:container:api", "host:docker"]
    assert set(st["active"]) == {"host:container:api", "host:docker"}


def test_tick_end_to_end_with_fake_docker_writes_state_and_alert_log(tmp_path, monkeypatch, cfg):
    FakeDocker(tmp_path, monkeypatch, mode="down")
    d = tmp_path / "hw"
    sent = []

    def send(level, title, body):
        sent.append((level, title))
        return (len(sent) > 1, "" if len(sent) > 1 else "HTTP 503")
    for i in range(3):
        W.tick(cfg, d, send=send, expected=TWELVE, now=1000 + 60 * i, mono=1000 + 60 * i, monitor_age=lambda: 10.0)
    assert sent == [("critical", "[host_watch] Docker 가 응답하지 않습니다")]
    log = [json.loads(x) for x in (d / W.ALERTS).read_text().splitlines()]
    assert log[0]["sent"] is False and log[0]["why"] == "HTTP 503"
    st = W.read_state(d)
    assert st["failed"] == 1 and st["docker_fail"] == 3
    FakeDocker(tmp_path, monkeypatch)
    W.tick(cfg, d, send=send, expected=TWELVE, now=1200, mono=1200, monitor_age=lambda: 10.0)
    assert sent[-1][1].startswith("[host_watch] 해소: Docker")


def test_print_mode_sends_nothing_and_keeps_state(tmp_path, monkeypatch, cfg):
    FakeDocker(tmp_path, monkeypatch, mode="down")
    d = tmp_path / "hw"
    for i in range(4):
        W.tick(cfg, d, send=lambda *a: pytest.fail("보내면 안 된다"), dry=True, expected=TWELVE, now=60.0 * i, mono=60.0 * i)
    assert not (d / W.STATE).exists()


def test_loop_runs_itself_with_a_pid_file_and_cleans_up(tmp_path, monkeypatch, cfg):
    """R11: launchd 없이 스스로 반복한다. PID 파일로 하나만 돈다."""
    FakeDocker(tmp_path, monkeypatch)
    d = tmp_path / "hw"
    monkeypatch.setattr(W.time, "sleep", lambda s: None)
    cfg["host_watch"]["interval_sec"] = 1
    assert W.loop(cfg, d, max_iter=2, send=lambda *a: (True, ""), expected=TWELVE, monitor_age=lambda: 10.0) == 0
    assert not (d / W.PID).exists() and W.read_state(d)["last_tick"]
    H.take_pid(d / W.PID)
    with pytest.raises(RuntimeError, match="이미 돌고"):
        (d / W.PID).write_text("1\n")                       # PID 1 은 늘 살아 있다
        W.loop(cfg, d, max_iter=1)


def test_status_reports_running_and_active_problems(tmp_path, cfg, capsys):
    d = tmp_path / "hw"
    W.write_state(d, {"last_tick": 1000.0, "active": {"host:monitor": {"level": "error", "title": "감시자(monitor)가 멈췄습니다", "since": 900}}})
    assert W.status(cfg, d, now=1030) == 1
    out = capsys.readouterr().out
    assert "멈춤" in out and "감시자(monitor)가 멈췄습니다" in out and "--start" in out


def test_webhook_url_never_appears_in_a_failure_reason(monkeypatch):
    from urbanstream.notify.transport import SendResult
    url = "https://discord.com/api/webhooks/123/SECRET-TOKEN"
    monkeypatch.setenv("ALERT_WEBHOOK_URL", url)
    monkeypatch.setenv("ALERTS_ENABLED", "true")

    def post(kind, u, text, scrub):
        return SendResult(False, None, scrub(f"URLError: <urlopen error {u}>"))
    ok, why = H.send_webhook("error", "t", "b", post=post)
    assert not ok and "SECRET-TOKEN" not in why
