"""Step 8b DQ 작업. DAG(dq_report_daily, silver_reconcile_daily)와 마트 DAG 의 사전 점검이 부른다.

psycopg·Spark 는 함수 안에서 늦게 불러온다 (DAG 파싱이 가볍게, tests/unit/test_batch_jobs.py 가 확인).
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from datetime import date, datetime, timezone
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


def require_dq_ok(dag_id: str, *, connect: Optional[Callable[[], Any]] = None) -> int:
    """마트 DAG 의 사전 점검. 관찰 기간이 끝났고 action=block_mart 인 규칙이 fail 이면 JobError (→ Airflow 재시도·알림).
    반환 = 막는 규칙 수(0). 관찰 기간·결과 없음·표 없음은 막지 않는다(= 지금은 사실상 통과)."""
    from urbanstream.dq import engine
    with _connect(connect) as conn:
        fails = engine.blocking_failures(conn)
    if fails:
        raise JobError(f"{dag_id}: 데이터 품질 규칙이 마트 계산을 막고 있습니다 — " + ", ".join(f"{r}[{t}]" if t else r for r, t in fails)
                       + " (ops.v_dq_latest 확인. 원인을 고치거나 config/dq_rules.yaml 에서 규칙을 조정)")
    return 0


def parse_silver_verify(text: str) -> list[tuple[str, bool, str]]:
    """silver_verify 출력에서 표별 OK/FAIL 을 뽑는다."""
    out = []
    for line in text.splitlines():
        m = SILVER_LINE.match(line.strip())
        if m:
            out.append((m.group(1), m.group(2) == "OK", m.group(3).strip()[:300]))
    return out


def run_silver_reconcile(*, connect: Optional[Callable[[], Any]] = None, runner: Callable[..., Any] = subprocess.run, timeout_sec: int = 1800,
                         send: Optional[Callable[..., Any]] = None, run_date: Optional[date] = None) -> dict:
    """streaming.silver_verify(Bronze 행 = Silver 행 + 읽을 수 없는 행)를 서브프로세스로 돌려 결과를 DQ 표에 남긴다. 실패하면 error 알림.
    Spark 가 필요하다 → pool gold_io (다른 Spark 작업과 동시에 돌지 않게)."""
    from urbanstream import notify
    from urbanstream.dq import checks as C, engine
    from urbanstream.dq.rules import Rule
    from batch.jobs import stage_env
    d = run_date or engine.default_run_date()
    env = stage_env(Path("/tmp/silver_reconcile"), silver_root=os.environ.get("GOLD_SILVER_ROOT", "/data/silver"),
                    bronze_root=os.environ.get("GOLD_BRONZE_ROOT", "/data/bronze"))
    env.update({"SILVER_BRONZE_ROOT": env["GOLD_BRONZE_ROOT"], "SILVER_ROOT": env["GOLD_SILVER_ROOT"], "SILVER_CONFIG_DIR": str(ROOT / "config"),
                "SILVER_VERIFY_LOOKBACK_DAYS": os.environ.get("SILVER_VERIFY_LOOKBACK_DAYS", "1")})
    try:
        proc = runner([sys.executable, "-m", "streaming.silver_verify"], cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as e:
        raise JobError(f"silver_verify 가 {timeout_sec}초 안에 끝나지 않았습니다") from e
    parsed = parse_silver_verify(proc.stdout or "")
    rule = Rule("silver_reconcile", "silver_reconcile", "error", "alert", None, {})
    if not parsed:
        res = [engine.Row(d, rule, C.Result("unknown", detail={"reason": "no_output", "returncode": proc.returncode,
                                                               "stderr_tail": (proc.stderr or "")[-300:]}), False)]
    else:
        res = [engine.Row(d, rule, C.Result("pass" if ok else "fail", t, detail={"line": line}), False) for t, ok, line in parsed]
    with _connect(connect) as conn:
        engine.record(conn, res)
    engine.alert(res, send or notify.send)
    out = engine.summarize(res)
    out.update(returncode=proc.returncode, run_date=d.isoformat())
    if proc.returncode not in (0, 1):
        raise JobError(f"silver_verify 비정상 종료 (코드 {proc.returncode})\n{(proc.stderr or '')[-1500:]}")
    return out
