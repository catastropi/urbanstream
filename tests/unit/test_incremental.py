"""증분 적재(D-278)의 Airflow 쪽 흐름: 진행 기록, 나눠 따라잡기, 시간 한도, 재연결. Spark·DB 없이 가짜 stage/load/verify 로 본다.
실제 Spark+PG 로 결과가 전체 읽기와 같은지는 tests/spark/test_gold_stage_incremental_pg.py."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from batch import jobs as J
from batch import stage_runner as R

D = date(2026, 11, 23)
MID = J.kst_midnight_epoch(date(2026, 11, 22))       # 증분이 보는 첫 날(전날) 0시


class Clock:
    def __init__(self, t: float):
        self.t = t

    def __call__(self) -> float:
        return self.t


def fakes(calls, *, other_dir=None, until_override=None):
    def stage(d0, d1, sd, files_since=None, files_until=None):
        first = not [c for c in calls if c[0] == "stage"]
        calls.append(("stage", d0, d1, files_since, files_until))
        sd.mkdir(parents=True, exist_ok=True)
        m = {"date_from": d0.isoformat(), "date_to": d1.isoformat(), "created_at": "2999-01-01T00:00:00+00:00",
             "audit_available": True, "row_counts": {"traffic_speed": 1}, "files_since": files_since,
             "files_until": (until_override if first and until_override else files_until),
             "files_read": {"silver_traffic_speed": 1}}
        if other_dir and first:
            other_dir.mkdir(parents=True, exist_ok=True)
            m["_stage_dir"] = str(other_dir)
        return m

    def load(sd, refresh_mart=False, dag_id=None):
        calls.append(("load", sd.name))
        return {"run_id": len(calls)}

    def verify(m, d0, d1):
        return {}
    return stage, load, verify


def test_first_run_starts_at_midnight_of_the_previous_day_and_records_progress(tmp_path):
    calls = []
    stage, load, verify = fakes(calls)
    wall = Clock(MID + 3600)                                   # 전날 01:00 (밀린 것 1시간 - 2분)
    out = J.process_incremental(D, D, dag_id="gold_intraday", run_id="r1", base=tmp_path, stage=stage, load=load,
                                verify=verify, wall=wall)
    st = [c for c in calls if c[0] == "stage"]
    assert len(st) == 1 and st[0][1:3] == (date(2026, 11, 22), D)
    assert st[0][3] == MID - 1 and st[0][4] == MID + 3600 - J.INCR_SETTLE_SEC
    assert J.read_state(tmp_path)["until"] == MID + 3600 - J.INCR_SETTLE_SEC and out["caught_up"]
    assert J.read_state(tmp_path)["until_kst"] == "11-22 00:58:00"
    assert not [p for p in tmp_path.iterdir() if "__" in p.name], "성공한 스테이징 폴더는 지운다"


def test_next_run_continues_from_the_record_with_overlap(tmp_path):
    calls = []
    stage, load, verify = fakes(calls)
    J.write_state(tmp_path, MID + 10_000)
    J.process_incremental(D, D, dag_id="gold_intraday", run_id="r2", base=tmp_path, stage=stage, load=load, verify=verify,
                          wall=Clock(MID + 13_600))
    st = [c for c in calls if c[0] == "stage"]
    assert st[0][3] == MID + 10_000 - J.INCR_OVERLAP_SEC and st[0][4] == MID + 13_600 - J.INCR_SETTLE_SEC


def test_backlog_is_split_into_chunks_and_each_chunk_advances_the_record(tmp_path):
    calls = []
    stage, load, verify = fakes(calls)
    J.write_state(tmp_path, MID)
    out = J.process_incremental(D, D, dag_id="gold_intraday", run_id="r3", base=tmp_path, stage=stage, load=load, verify=verify,
                                wall=Clock(MID + 10 * 3600), max_span_sec=3 * 3600)
    st = [c for c in calls if c[0] == "stage"]
    assert len(st) == 4 and all(b[4] - b[3] <= 3 * 3600 for b in st)
    assert [c[1] for c in calls if c[0] == "load"] == [f"gold_intraday__r3_c{i}__try1" for i in range(4)]
    assert J.read_state(tmp_path)["until"] == MID + 10 * 3600 - J.INCR_SETTLE_SEC and out["caught_up"]


def test_time_budget_leaves_the_rest_for_the_next_run(tmp_path):
    calls = []
    stage, load, verify = fakes(calls)
    J.write_state(tmp_path, MID)
    mono = iter(range(0, 10_000, 1000))
    out = J.process_incremental(D, D, dag_id="g", run_id="r", base=tmp_path, stage=stage, load=load, verify=verify,
                                wall=Clock(MID + 20 * 3600), max_span_sec=3600, budget_sec=1500,
                                mono=lambda: float(next(mono)))
    assert len([c for c in calls if c[0] == "stage"]) == 2 and not out["caught_up"]
    assert J.read_state(tmp_path)["until"] == pytest.approx(MID + 3600 - J.INCR_OVERLAP_SEC + 3600)


def test_failed_load_does_not_advance_the_record(tmp_path):
    calls = []
    stage, _, verify = fakes(calls)
    J.write_state(tmp_path, MID + 100)

    def load(sd, refresh_mart=False, dag_id=None):
        raise RuntimeError("db down")
    with pytest.raises(RuntimeError):
        J.process_incremental(D, D, dag_id="g", run_id="r", base=tmp_path, stage=stage, load=load, verify=verify,
                              wall=Clock(MID + 7200))
    assert J.read_state(tmp_path)["until"] == MID + 100


def test_reattached_request_uses_its_own_folder_and_window(tmp_path):
    calls = []
    other = tmp_path / "g__r_c0__try1"
    stage, load, verify = fakes(calls, other_dir=other, until_override=MID + 500)
    J.write_state(tmp_path, MID + 100)
    J.process_incremental(D, D, dag_id="g", run_id="r", try_number=2, base=tmp_path, stage=stage, load=load, verify=verify,
                          wall=Clock(MID + 600 + J.INCR_SETTLE_SEC), max_span_sec=3600)
    assert ("load", "g__r_c0__try1") in calls
    assert J.read_state(tmp_path)["until"] >= MID + 500


def test_record_never_moves_backwards(tmp_path):
    J.write_state(tmp_path, 2000.0)
    J.write_state(tmp_path, 1000.0)
    assert J.read_state(tmp_path)["until"] == 2000.0


def test_daily_refreshes_marts_once_at_the_end(tmp_path):
    calls, refreshed = [], []
    stage, load, verify = fakes(calls)
    J.write_state(tmp_path, MID)
    J.process_incremental(date(2026, 11, 22), D, dag_id="gold_daily", run_id="r", refresh_mart=True, base=tmp_path, stage=stage,
                          load=load, verify=verify, wall=Clock(MID + 8 * 3600), max_span_sec=3 * 3600,
                          refresh=lambda: refreshed.append(1))
    assert refreshed == [1] and len([c for c in calls if c[0] == "stage"]) == 3


def test_nothing_new_still_refreshes_marts_and_keeps_the_record(tmp_path):
    calls, refreshed = [], []
    stage, load, verify = fakes(calls)
    J.write_state(tmp_path, MID + 1000)
    out = J.process_incremental(D, D, dag_id="gold_daily", run_id="r", refresh_mart=True, base=tmp_path, stage=stage, load=load,
                                verify=verify, wall=Clock(MID + 1000 + J.INCR_SETTLE_SEC - J.INCR_OVERLAP_SEC),
                                refresh=lambda: refreshed.append(1))
    assert not calls and refreshed == [1] and out["chunks"] == []


def test_request_carries_the_file_window_to_the_runner(tmp_path):
    req = R.requests_dir(tmp_path)
    req.mkdir(parents=True)
    R.touch_alive(req)
    seen = {}

    def sleep(_):
        body = json.loads(next(req.glob("*.json")).read_text())
        seen.update(body)
        r = R.Request.load(next(req.glob("*.json")))
        cmd = R.spark_command(r, driver_memory="1g", master="local[1]", spark_submit="spark-submit")
        seen["cmd"] = cmd
        sd = Path(body["stage_dir"])
        sd.mkdir(parents=True, exist_ok=True)
        (sd / "manifest.json").write_text(json.dumps({"date_from": "2026-11-22", "date_to": "2026-11-23"}))
        R.write_json_atomic(req / f"{body['id']}.done", {"returncode": 0})
    J.request_stage(date(2026, 11, 22), D, tmp_path / "g__r_c0__try1", base=tmp_path, sleep=sleep,
                    files_since=100.5, files_until=200.25)
    assert (seen["files_since"], seen["files_until"]) == (100.5, 200.25)
    c = seen["cmd"]
    assert c[c.index("--files-since") + 1] == "100.5" and c[c.index("--files-until") + 1] == "200.25"


def test_full_requests_have_no_file_window():
    r = R.Request("1", "k", "2026-11-22", "2026-11-22", Path("/b/x__y__try1"), 1.0)
    assert "--files-until" not in R.spark_command(r, driver_memory="1g", master="local[1]", spark_submit="s")
    assert R.files_args(None, 5.0) == ["--files-until", "5.0"]


def test_incremental_window_always_includes_the_previous_day():
    assert J.incremental_window(D, D) == (date(2026, 11, 22), D)
    assert J.incremental_window(date(2026, 11, 20), D) == (date(2026, 11, 20), D)
