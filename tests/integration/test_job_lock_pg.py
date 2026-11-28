"""작업 잠금(job_lock)을 실제 PostgreSQL 로 본다 (STEP 3B 4-3). GOLD_TEST_DSN 이 없으면 건너뛴다.

- 두 시도가 동시에 process_incremental: 하나만 스테이징하고, 다른 하나는 기다렸다가 따라잡을 것이 없어 바로 끝난다.
- 잠금을 잡은 프로세스가 kill -9 로 죽으면 서버가 잠금을 풀어 다음 시도가 진행한다.
- D-212 재현: 시도 1(고아)이 잠금을 잡은 채 멈춰 있으면 시도 2는 Spark 를 띄우지 않고 기다리다 실패한다.
"""
from __future__ import annotations

import functools
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import date
from pathlib import Path

import pytest

from batch import jobs as J

DSN = os.environ.get("GOLD_TEST_DSN")
pytestmark = [pytest.mark.pg, pytest.mark.skipif(not DSN, reason="GOLD_TEST_DSN 없음")]
ROOT = Path(__file__).resolve().parents[2]
D = date(2026, 11, 23)


@pytest.fixture(autouse=True)
def _dsn(monkeypatch):
    monkeypatch.setenv("GOLD_DSN", DSN or "")


def _manifest(d0, d1, until):
    return {"date_from": d0.isoformat(), "date_to": d1.isoformat(), "created_at": "2099-01-01T00:00:00+00:00",
            "audit_available": True, "row_counts": {"traffic_speed": 1}, "files_until": until}


def hold_in_subprocess(name: str) -> subprocess.Popen:
    """다른 프로세스가 잠금을 잡고 멈춰 있는 상태(고아 시도)."""
    code = ("import sys,time; sys.path[:0]=[%r,%r]; from batch import jobs as J\n"
            "with J.job_lock(%r):\n    print('held', flush=True); time.sleep(600)\n") % (str(ROOT), str(ROOT / "src"), name)
    p = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True,
                         env={**os.environ, "GOLD_DSN": DSN, "AIRFLOW_CTX_DAG_ID": "gold_intraday", "AIRFLOW_CTX_TRY_NUMBER": "1"})
    assert p.stdout.readline().strip() == "held"
    return p


def test_two_concurrent_tries_do_not_overlap_and_the_second_continues_from_the_first(tmp_path):
    """두 시도가 동시에 시작해도 Spark 스테이징 구간이 시간상 겹치지 않는다. 두 번째는 첫 번째가 남긴 진행 기록에서 이어 간다
    (겹침 INCR_OVERLAP_SEC 만큼만 다시 읽는다. 적재는 DO NOTHING 이라 결과는 같다)."""
    wall = J.kst_midnight_epoch(date(2026, 11, 22)) + 3600          # 따라잡을 구간이 한 조각(3시간 미만)
    spans = []

    def stage(d0, d1, sd, files_since=None, files_until=None):
        t0 = time.monotonic()
        time.sleep(1.5)                                   # 첫 시도가 도는 동안 두 번째가 잠금을 기다리게
        sd.mkdir(parents=True)
        spans.append((threading.current_thread().name, t0, time.monotonic(), files_since, files_until))
        return _manifest(d0, d1, files_until)

    fast = functools.partial(J.job_lock, poll_sec=0.2, wait_sec=30)

    def run(name):
        J.process_incremental(D, D, dag_id="gold_intraday", run_id=name, base=tmp_path, stage=stage,
                              load=lambda *a, **k: {"run_id": 1}, verify=lambda *a: {}, wall=lambda: wall, lock=fast)
    ts = [threading.Thread(target=run, args=(n,), name=n) for n in ("try1", "try2")]
    ts[0].start()
    time.sleep(0.3)
    ts[1].start()
    for t in ts:
        t.join(60)
    assert [s[0] for s in spans] == ["try1", "try2"]
    (_, _, end1, _, until1), (_, start2, _, since2, _) = spans
    assert start2 >= end1, "두 번째 스테이징은 첫 번째가 끝난 뒤에 시작한다 (D-212: 겹치면 메모리 2배)"
    assert since2 == pytest.approx(until1 - J.INCR_OVERLAP_SEC), "두 번째는 처음부터가 아니라 첫 번째의 진행 기록에서 이어 간다"


def test_lock_held_by_a_stuck_orphan_blocks_the_retry_without_starting_spark(tmp_path):
    p = hold_in_subprocess(J.LOCK_INCREMENTAL)
    staged = []
    try:
        with pytest.raises(J.LockBusy, match="이전 시도가 아직 실행 중.*uslock:gold_facts_incremental:gold_intraday#1"):
            J.process_incremental(D, D, dag_id="gold_intraday", run_id="r", try_number=2, base=tmp_path,
                                  stage=lambda *a, **k: staged.append(1), load=lambda *a, **k: {}, verify=lambda *a: {},
                                  lock=functools.partial(J.job_lock, poll_sec=0.2, wait_sec=1.5))
        assert staged == []
    finally:
        p.kill()
        p.wait()


def test_kill_minus_nine_releases_the_lock(tmp_path):
    p = hold_in_subprocess(J.LOCK_WINDOW)
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    t0 = time.monotonic()
    with J.job_lock(J.LOCK_WINDOW, poll_sec=0.2, wait_sec=20):
        pass
    assert time.monotonic() - t0 < 20, "접속이 끊기면 서버가 잠금을 푼다"


def test_different_jobs_do_not_block_each_other():
    p = hold_in_subprocess(J.LOCK_RECONCILE)
    try:
        with J.job_lock(J.LOCK_INCREMENTAL, wait_sec=0.5, poll_sec=0.1):
            pass
    finally:
        p.kill()
        p.wait()
