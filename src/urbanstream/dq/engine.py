"""DQ 실행: 규칙 → 검사 → ops.dq_result 기록 → (shadow 가 아니면) 알림. 마트 차단 게이트도 여기.

- 한 규칙의 예외는 그 규칙을 unknown(detail.error)으로 기록할 뿐 다른 규칙을 멈추지 않는다.
- shadow(관찰) 기간에는 기록만 한다. 알림·차단 없음.
- fail 만 알린다 (warn=설명 가능한 이탈, unknown=판단 불가 는 요약에만). severity error → 푸시, warning → 아침 요약, info → 알림 없음.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

from ..notify import mask
from . import checks as C
from .rules import Rule, RuleSet

LOG = logging.getLogger("urbanstream.dq")
KST = ZoneInfo("Asia/Seoul")
LEVEL = {"error": "error", "warning": "warning"}

UPSERT = ("INSERT INTO ops.dq_result (run_date, rule_id, kind, target, severity, action, status, shadow, value, n_checked, threshold, detail, checked_at) "
          "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,now()) ON CONFLICT (run_date, rule_id, target) DO UPDATE SET "
          "kind=EXCLUDED.kind, severity=EXCLUDED.severity, action=EXCLUDED.action, status=EXCLUDED.status, shadow=EXCLUDED.shadow, value=EXCLUDED.value, "
          "n_checked=EXCLUDED.n_checked, threshold=EXCLUDED.threshold, detail=EXCLUDED.detail, checked_at=now()")


@dataclass(frozen=True)
class Row:
    run_date: date
    rule: Rule
    result: C.Result
    shadow: bool


def default_run_date(now: Optional[datetime] = None) -> date:
    return ((now or datetime.now(timezone.utc)).astimezone(KST) - timedelta(days=1)).date()


def run_rules(conn, ruleset: RuleSet, run_date: date, *, today: Optional[date] = None) -> list[Row]:
    today = today or datetime.now(timezone.utc).astimezone(KST).date()
    rows: list[Row] = []
    for rule in ruleset.rules:
        shadow = ruleset.is_shadow(rule, today)
        try:
            results = C.CHECKS[rule.kind](conn, rule.params, run_date)
        except Exception as e:                                               # noqa: BLE001
            LOG.exception("DQ 규칙 %s 실패", rule.id)
            results = [C.Result("unknown", detail={"reason": "check_error", "error": mask(f"{type(e).__name__}: {e}")[:300]})]
        for r in results:
            rows.append(Row(run_date, rule, r, shadow))
    return rows


def record(conn, rows: Iterable[Row]) -> int:
    n = 0
    for x in rows:
        r = x.result
        conn.execute(UPSERT, (x.run_date, x.rule.id, x.rule.kind, r.target, x.rule.severity, x.rule.action, r.status, x.shadow, r.value, r.n_checked,
                              r.threshold or None, json.dumps(dict(r.detail), default=str, ensure_ascii=False)))
        n += 1
    return n


def alert(rows: Iterable[Row], send: Callable[..., Any]) -> int:
    """fail 이고 shadow 가 아니고 action 이 alert/block_mart 이고 severity 가 error/warning 인 것만. 같은 규칙·대상은 dedup_key 로 합쳐진다."""
    n = 0
    for x in rows:
        if x.result.status != "fail" or x.shadow or x.rule.action == "record" or x.rule.severity not in LEVEL:
            continue
        tgt = f"[{x.result.target}]" if x.result.target else ""
        body = f"{x.run_date} 검사: 값 {x.result.value if x.result.value is None else round(x.result.value, 4)} (기준 {x.result.threshold}) {json.dumps(dict(x.result.detail), default=str, ensure_ascii=False)[:300]}"
        send(LEVEL[x.rule.severity], f"DQ 실패: {x.rule.id}{tgt}", body, dedup_key=f"dq:{x.rule.id}:{x.result.target}")
        n += 1
    return n


def summarize(rows: list[Row]) -> dict:
    s = {"pass": 0, "warn": 0, "fail": 0, "unknown": 0}
    for x in rows:
        s[x.result.status] = s.get(x.result.status, 0) + 1
    return {"checks": len(rows), **s, "shadow": any(x.shadow for x in rows),
            "failed": [f"{x.rule.id}{'[' + x.result.target + ']' if x.result.target else ''}" for x in rows if x.result.status == "fail"]}


def blocking_failures(conn, *, since_days: int = 2) -> list[tuple[str, str]]:
    """마트 DAG 가 시작 전에 보는 목록: shadow 가 아니고 action=block_mart 이고 가장 최근 결과가 fail 인 (규칙, 대상).
    결과 표가 아직 없거나(010 이전) 비어 있으면 빈 목록 = 막지 않는다 (DQ 가 없다고 파이프라인을 멈추지 않는다)."""
    ok = conn.execute("SELECT to_regclass('ops.dq_result') IS NOT NULL").fetchone()[0]
    if not ok:
        return []
    return [(r[0], r[1]) for r in conn.execute(
        "SELECT rule_id, target FROM ops.v_dq_latest WHERE action='block_mart' AND status='fail' AND NOT shadow "
        "AND run_date >= (now() AT TIME ZONE 'Asia/Seoul')::date - %s ORDER BY 1, 2", (since_days,)).fetchall()]
