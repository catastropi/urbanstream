"""Step 8b DQ 작업. DAG(dq_report_daily, silver_reconcile_daily)와 마트 DAG 의 사전 점검이 부른다.

psycopg·Spark 는 함수 안에서 늦게 불러온다 (DAG 파싱이 가볍게, tests/unit/test_batch_jobs.py 가 확인).
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional

from batch.jobs import JobError, ROOT

LOG = logging.getLogger("urbanstream.batch.dq_jobs")
SILVER_LINE = re.compile(r"^(silver_\w+)\s+(OK|FAIL)\b(.*)$")


def _connect(connect: Optional[Callable[[], Any]]):
    if connect is not None:
        return connect()
    from batch.jobs import _default_connect
    return _default_connect()


def run_daily_report(run_date: Optional[date] = None, *, connect: Optional[Callable[[], Any]] = None, rules_path: Optional[Path] = None,
                     send: Optional[Callable[..., Any]] = None, today: Optional[date] = None) -> dict:
    """전날(KST) DQ 를 검사해 ops.dq_result 에 기록하고, 관찰 기간이 아니면 실패를 알린다. 또한 분류되지 않고 오래 남은 pending 알림을 error 로 내보낸다
    (모니터가 꺼져 있어도 DAG 실패 알림이 사라지지 않게 하는 마지막 안전망, ADR-0010 D8-5)."""
    from urbanstream import notify
    from urbanstream.dq import engine, rules
    rs = rules.load(rules_path)
    d = run_date or engine.default_run_date()
    with _connect(connect) as conn:
        rows = engine.run_rules(conn, rs, d, today=today)
        engine.record(conn, rows)
    engine.alert(rows, send or notify.send)
    out = engine.summarize(rows)
    out["run_date"] = d.isoformat()
    out["stale_pending_flushed"] = flush_stale_pending()
    LOG.info("DQ %s: %s", d, {k: v for k, v in out.items() if k != "failed"})
    return out


def flush_stale_pending() -> int:
    """모니터가 분류하지 못한(꺼져 있던) pending 알림을 원래 수준으로 내보낸다. 실패해도 DQ 결과를 막지 않는다."""
    try:
        from urbanstream import notify
        n = notify.get_notifier()
        return n.process_pending(lambda r: notify.Decision(r["level"], r["dedup_key"], "stale_pending_flush"), min_age_sec=n.policy.pending_flush_after_sec)
    except Exception:                                                        # noqa: BLE001
        LOG.exception("오래된 pending 알림 내보내기 실패 (무시)")
        return 0


class DqBlocked(JobError):
    """DQ 규칙이 마트 계산을 막고 있다. 결과는 dq_report_daily(05:00)가 돌 때만 바뀌므로 재시도해도 같은 실패다."""


def fail_fast(msg: str) -> Exception:
    """재시도 없이 태스크를 실패시키는 예외. Airflow 안이면 AirflowFailException, 밖(시험·손 실행)이면 DqBlocked.
    마트 DAG 의 check_upstream 은 '최근 적재 확인'(늦으면 재시도로 풀림)과 'DQ 차단'(재시도 무의미)을 같이 본다.
    그래서 DAG 의 재시도 횟수는 그대로 두고 DQ 차단만 바로 실패시킨다 (STEP 3B, dag-fail-1124 4.4)."""
    try:
        from airflow.sdk.exceptions import AirflowFailException
    except ImportError:
        try:
            from airflow.exceptions import AirflowFailException   # type: ignore[no-redef]
        except ImportError:
            return DqBlocked(msg)
    return AirflowFailException(msg)


def require_dq_ok(dag_id: str, *, connect: Optional[Callable[[], Any]] = None) -> int:
    """마트 DAG 의 사전 점검. 관찰 기간이 끝났고 action=block_mart 인 규칙이 fail 이면 재시도 없이 실패한다(fail_fast) → 알림.
    반환 = 막는 규칙 수(0). 관찰 기간·결과 없음·표 없음은 막지 않는다(= 지금은 사실상 통과)."""
    from urbanstream.dq import engine
    with _connect(connect) as conn:
        fails = engine.blocking_failures(conn)
    if fails:
        raise fail_fast(f"{dag_id}: 데이터 품질 규칙이 마트 계산을 막고 있습니다 — " + ", ".join(f"{r}[{t}]" if t else r for r, t in fails)
                        + " (ops.v_dq_latest 확인. 원인을 고치거나 config/dq_rules.yaml 에서 규칙을 조정. 결과는 dq_report_daily 가 돌 때만"
                        " 바뀌므로 재시도하지 않습니다)")
    return 0


def parse_silver_verify(text: str) -> list[tuple[str, bool, str]]:
    """silver_verify 출력에서 표별 OK/FAIL 을 뽑는다."""
    out = []
    for line in text.splitlines():
        m = SILVER_LINE.match(line.strip())
        if m:
            out.append((m.group(1), m.group(2) == "OK", m.group(3).strip()[:300]))
    return out


VERIFY_TIMEOUT_SEC = 1800


def run_silver_reconcile(*, connect: Optional[Callable[[], Any]] = None, runner: Callable[..., Any] = subprocess.run, timeout_sec: int = VERIFY_TIMEOUT_SEC,
                         send: Optional[Callable[..., Any]] = None, run_date: Optional[date] = None, mode: Optional[str] = None,
                         lock: Optional[Callable[..., Any]] = None, request: Optional[Callable[..., dict]] = None,
                         base: Optional[Path] = None) -> dict:
    """streaming.silver_verify(Bronze 행 = Silver 행 + 읽을 수 없는 행)를 돌려 결과를 DQ 표에 남긴다. 실패하면 error 알림.

    어디서 Spark 를 띄우나 (STEP 3B): GOLD_STAGE_MODE=runner(기본)면 gold-runner 에 kind=silver_verify 로 맡긴다. 그러면 scheduler
    안에 Spark 가 하나도 남지 않고, Spark 는 gold-runner 에서 한 번에 하나만 돈다. inline 이면 예전처럼 이 프로세스의 자식으로 돈다.
    작업 잠금(LOCK_RECONCILE) 안에서 돈다(고아 재시도가 겹쳐 돌지 않게). pool gold_io 도 그대로 쓴다."""
    from batch import jobs as J
    m = mode or J.stage_mode()
    with (lock or J.job_lock)(J.LOCK_RECONCILE):
        if m == "runner":
            rc, stdout, stderr = _verify_in_runner(run_date, timeout_sec=timeout_sec, request=request, base=base)
        else:
            rc, stdout, stderr = _verify_inline(runner, timeout_sec)
    return _record_verify(rc, stdout, stderr, connect=connect, send=send, run_date=run_date)


def _verify_env() -> dict:
    from batch.jobs import stage_env
    env = stage_env(Path("/tmp/silver_reconcile"), silver_root=os.environ.get("GOLD_SILVER_ROOT", "/data/silver"),
                    bronze_root=os.environ.get("GOLD_BRONZE_ROOT", "/data/bronze"))
    env.update({"SILVER_BRONZE_ROOT": env["GOLD_BRONZE_ROOT"], "SILVER_ROOT": env["GOLD_SILVER_ROOT"], "SILVER_CONFIG_DIR": str(ROOT / "config"),
                "SILVER_REBUILD_ROOT": os.environ.get("GOLD_SILVER_REBUILD_ROOT", "/data/silver_rebuild"),
                "SILVER_VERIFY_LOOKBACK_DAYS": lookback_days()})
    return env


def lookback_days() -> str:
    return os.environ.get("SILVER_VERIFY_LOOKBACK_DAYS", "1")


def _verify_inline(runner: Callable[..., Any], timeout_sec: int) -> tuple[Optional[int], str, str]:
    try:
        proc = runner([sys.executable, "-m", "streaming.silver_verify"], cwd=str(ROOT), env=_verify_env(), capture_output=True, text=True,
                      timeout=timeout_sec)
    except subprocess.TimeoutExpired as e:
        from batch.jobs import _tail, _text
        # 11-24 처럼 시간 초과로 끝나면 어디까지 갔는지가 남지 않았다(dag-fail-1124 4.3). 진행 줄 '[경과초] ...' 마지막 부분을 남긴다
        raise JobError(f"silver_verify 가 {timeout_sec}초 안에 끝나지 않았습니다\n{_tail(_text(e.stderr), 40)}") from e
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def _verify_in_runner(run_date: Optional[date], *, timeout_sec: int, request: Optional[Callable[..., dict]],
                      base: Optional[Path]) -> tuple[Optional[int], str, str]:
    """gold-runner 에 맡기고 결과를 기다린다. 대조 실패(종료 코드 1)도 실행기는 '끝남'으로 돌려준다. 표별 줄은 .out 파일에서 읽는다."""
    from batch import jobs as J
    from batch.stage_runner import marker, requests_dir
    from urbanstream.dq import engine
    d = run_date or engine.default_run_date()
    b = Path(base or J.stage_base())
    sd = J.stage_dir_for(b, "silver_reconcile_daily", d.isoformat(), 1)
    res = (request or J.request_runner)("silver_verify", d, d, sd, args={"lookback_days": lookback_days()}, base=b, timeout_sec=timeout_sec)
    rid = res.get("_id")
    out = ""
    if rid:
        try:
            out = marker(requests_dir(b), rid, "out").read_text(encoding="utf-8", errors="replace")
        except OSError:
            out = ""
    out = out or res.get("stdout_tail") or ""
    try:
        J.cleanup_stage(b, Path(res.get("_stage_dir") or sd))
    except J.JobError:
        pass
    return res.get("returncode"), out, res.get("stderr_tail") or ""


def _record_verify(rc: Optional[int], stdout: str, stderr: str, *, connect: Optional[Callable[[], Any]], send: Optional[Callable[..., Any]],
                   run_date: Optional[date]) -> dict:
    from urbanstream import notify
    from urbanstream.dq import checks as C, engine
    from urbanstream.dq.rules import Rule
    d = run_date or engine.default_run_date()
    parsed = parse_silver_verify(stdout or "")
    rule = Rule("silver_reconcile", "silver_reconcile", "error", "alert", None, {})
    if not parsed:
        res = [engine.Row(d, rule, C.Result("unknown", detail={"reason": "no_output", "returncode": rc,
                                                               "stderr_tail": (stderr or "")[-300:]}), False)]
    else:
        res = [engine.Row(d, rule, C.Result("pass" if ok else "fail", t, detail={"line": line}), False) for t, ok, line in parsed]
    with _connect(connect) as conn:
        engine.record(conn, res)
    engine.alert(res, send or notify.send)
    out = engine.summarize(res)
    out.update(returncode=rc, run_date=d.isoformat())
    if rc not in (0, 1):
        raise JobError(f"silver_verify 비정상 종료 (코드 {rc})\n{(stderr or '')[-1500:]}")
    return out
