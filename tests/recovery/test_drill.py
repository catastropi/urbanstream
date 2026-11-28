"""scripts/drill.py 안전장치: 검사 실패 시 시작 안 함, 측정 잠금과 겹치지 않음, 최대 시간·Ctrl-C·Enter 에 되돌림, 하루 공백 상한, 기록 형식."""
from __future__ import annotations

import json

import pytest

from recovery_support import TWELVE, FakeDocker

import drill as D
import hostlib as H
import measure_guard as G


@pytest.fixture
def cfg():
    c = H.load_config()
    c["drills"]["settle_healthy_sec"] = 0
    return c


@pytest.fixture
def doc(tmp_path):
    p = tmp_path / "recovery-drills.md"
    p.write_text("| 날짜 | 종류 |\n|---|---|\n" + D.TABLE_MARK + "\n", encoding="utf-8")
    return p


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def go(tmp_path, cfg, doc, name="monitor-stop", *, gate_ok=True, wait=None, answers=("yes", "14:56", ""), clock=None, **kw):
    ans = iter(answers)
    return D.run_drill(name, cfg, ask=lambda q: next(ans), gate_fn=lambda s: (gate_ok, []), wait_input=wait or (lambda t: True),
                       sleep=lambda s: None, clock_fn=clock or Clock(), expected=TWELVE, drill_dir=tmp_path / "drills", doc=doc,
                       lock_path=tmp_path / "measure" / ".lock", **kw)


def docker_actions(fd):
    return [c["args"][:2] for c in fd.calls() if c["args"][0] in ("stop", "start", "kill")]


def test_failed_gate_starts_nothing(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)
    assert go(tmp_path, cfg, doc, gate_ok=False) == 1
    assert docker_actions(fd) == [] and not (tmp_path / "drills").exists()


def test_drill_does_not_overlap_a_measurement(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)
    lock = tmp_path / "measure" / ".lock"
    lock.parent.mkdir()
    lock.write_text("99 2026-12-10 M6\n")
    with pytest.raises(G.GuardError, match="잠금 파일"):
        go(tmp_path, cfg, doc)
    assert docker_actions(fd) == []


def test_enter_restores_and_the_result_is_recorded(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)
    assert go(tmp_path, cfg, doc) == 0
    assert docker_actions(fd) == [["stop", "urbanstream-monitor"], ["start", "urbanstream-monitor"]]
    assert not (tmp_path / "measure" / ".lock").exists(), "끝나면 잠금을 푼다"
    rec = json.loads((tmp_path / "drills" / "drills.jsonl").read_text().splitlines()[0])
    assert rec["kind"] == "훈련" and rec["scenario"] == "R12" and rec["result"] == "통과" and rec["alert_at"].startswith("14:56")
    rows = [x for x in doc.read_text().splitlines() if x.startswith("| 20")]
    assert len(rows) == 1 and "R12 monitor-stop" in rows[0] and "통과" in rows[0]
    assert doc.read_text().rstrip().endswith(D.TABLE_MARK), "다음 기록도 표 안에 붙도록 표시를 남긴다"


def test_max_time_restores_automatically(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)
    c = Clock()

    def never(t):
        c.t += t
        return False
    assert go(tmp_path, cfg, doc, "spark-bronze-kill", wait=never, clock=c) == 0
    assert docker_actions(fd) == [["kill", "urbanstream-spark-bronze"], ["start", "urbanstream-spark-bronze"]]
    assert c.t >= cfg["drills"]["spark-bronze-kill"]["max_min"] * 60
    assert "최대 시간" in json.loads((tmp_path / "drills" / "drills.jsonl").read_text())["memo"]


def test_ctrl_c_restores(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)

    def interrupt(t):
        raise KeyboardInterrupt
    assert go(tmp_path, cfg, doc, "spark-silver-stop", wait=interrupt) == 0
    assert docker_actions(fd)[-1] == ["start", "urbanstream-spark-silver"]


def test_restore_that_does_not_come_back_healthy_is_recorded_as_such(tmp_path, monkeypatch, cfg, doc):
    FakeDocker(tmp_path, monkeypatch, on_start_healthy=False)
    cfg["drills"]["settle_max_sec"] = 0
    assert go(tmp_path, cfg, doc) == 1
    assert json.loads((tmp_path / "drills" / "drills.jsonl").read_text())["result"] == "되돌림 확인 실패"


def test_collection_cutting_drills_respect_the_daily_gap_budget(tmp_path, monkeypatch, cfg, doc):
    fd = FakeDocker(tmp_path, monkeypatch)
    today = H.now_kst().strftime("%Y-%m-%d")
    (tmp_path / "drills").mkdir()
    (tmp_path / "drills" / "drills.jsonl").write_text(json.dumps({"date": today, "kind": "훈련", "gap_min": 55}) + "\n")
    assert go(tmp_path, cfg, doc, "kafka-stop") == 2
    assert docker_actions(fd) == []


def test_kafka_stop_records_a_collection_gap(tmp_path, monkeypatch, cfg, doc):
    FakeDocker(tmp_path, monkeypatch)
    assert go(tmp_path, cfg, doc, "kafka-stop") == 0
    rec = json.loads((tmp_path / "drills" / "drills.jsonl").read_text())
    assert rec["gap_min"] > 0 and rec["gap"].endswith("분"), "수집을 끊는 훈련은 공백을 따로 센다 (STEP 2 보고서가 뺀다)"


def test_docker_restart_restores_with_resume_inside_the_drill_lock():
    dr = D.DRILLS["docker-restart"]
    assert dr.breaks == [] and dr.manual and "quit app" in dr.manual
    assert dr.restores[0][-2:] == ["resume", "--in-drill"]


def test_recover_rehearsal_never_touches_production_topics():
    cmd = " ".join(D.DRILLS["recover-rehearsal"].breaks[0])
    assert "test_bronze_recover_kafka.py" in cmd and "--profile core" in cmd and "checkpoints" not in cmd


def test_record_natural_incident_goes_to_the_same_table(tmp_path, doc, monkeypatch):
    monkeypatch.setattr(D, "DOC", doc)
    monkeypatch.setattr(D, "DRILL_DIR", tmp_path / "drills")
    assert D.main(["record", "--scenario", "R8", "--date", "2026-11-24", "--start", "05:00", "--end", "09:10",
                   "--result", "실패 기록", "--memo", "bus future_event 로 마트 3개 차단"]) == 0
    row = [x for x in doc.read_text().splitlines() if x.startswith("| 2026-11-24")][0]
    assert "| 자연 |" in row and "R8" in row and "실패 기록" in row


def test_every_drill_has_a_config_entry_and_a_scenario():
    cfg = H.load_config()
    assert set(D.DRILLS) == {k for k in cfg["drills"] if isinstance(cfg["drills"][k], dict)}
    for n, d in D.DRILLS.items():
        assert cfg["drills"][n]["scenario"] == d.scenario
        assert d.restores or n == "recover-rehearsal", n
    assert {n for n in D.DRILLS if cfg["drills"][n]["gap"]} == set(D.GAP_DRILLS)
