"""규칙 파일 읽기와 검사(린터). 규칙 파일이 틀렸으면 DAG 가 첫 실행에서 알려 주는 대신 이 린터와 시험이 먼저 잡는다."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional

SEVERITIES = ("info", "warning", "error")
ACTIONS = ("record", "alert", "block_mart")
# block_mart 는 '미래 시각' 규칙에만 허용한다 (ADR-0010 D8-9): 다른 규칙의 오탐으로 마트가 멈추면 손해가 더 크다.
BLOCK_KINDS = ("future_time",)
_TABLE = re.compile(r"^(dim|fact|mart|ops)\.[a-z_][a-z0-9_]*$")
_COL = re.compile(r"^[a-z_][a-z0-9_]*$")
REQUIRED: dict[str, tuple[str, ...]] = {
    "null_rate": ("table", "date_col", "column", "max_rate", "min_rows"),
    "range": ("table", "date_col", "column", "min", "max", "max_out_rate", "min_rows"),
    "flag_rate": ("table", "date_col", "flag", "max_rate", "min_rows"),
    "future_time": ("table", "date_col", "ts_col", "fetched_col", "margin_sec", "max_count"),
    "hour_coverage": ("table", "date_col", "ts_col", "hours", "min_hours"),
    "volume_drift": ("table", "date_col", "baseline_days", "min_baseline_days", "low", "high"),
    "ref_integrity": ("child_table", "child_col", "parent_table", "parent_col", "date_col", "max_orphan_rate", "min_rows"),
    "mart_reliability": ("mart_table", "flag_col", "source_table", "source_date_col", "mature_after_days"),
    "load_health": ("dag_id", "max_age_hours", "stale_running_hours"),
    "call_fail_rate": ("max_rate", "min_calls"),
    "incident_link_rate": ("min_rows",),
    "table_bloat": ("max_ratio", "min_live_rows"),
}
_TABLE_KEYS = ("table", "child_table", "parent_table", "mart_table", "source_table")
_COL_KEYS = ("date_col", "column", "ts_col", "fetched_col", "child_col", "parent_col", "flag_col", "source_date_col")


class RuleError(ValueError):
    pass


@dataclass(frozen=True)
class Rule:
    id: str
    kind: str
    severity: str
    action: str
    mode: Optional[str]                      # None | 'enforce'
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleSet:
    shadow_until: Optional[date]
    rules: tuple[Rule, ...]

    def is_shadow(self, rule: Rule, today: date) -> bool:
        if rule.mode == "enforce":
            return False
        return self.shadow_until is not None and today <= self.shadow_until


def parse(raw: Mapping[str, Any]) -> RuleSet:
    """검사에 실패하면 RuleError (모든 문제를 한 번에 모아서 알린다)."""
    problems: list[str] = []
    su = raw.get("shadow_until")
    shadow_until = None
    if su is not None:
        try:
            shadow_until = su if isinstance(su, date) else date.fromisoformat(str(su))
        except ValueError:
            problems.append(f"shadow_until 이 날짜가 아닙니다: {su!r}")
    rules, seen = [], set()
    for i, r in enumerate(raw.get("rules") or []):
        rid = str(r.get("id", f"#{i}"))
        if rid in seen:
            problems.append(f"{rid}: id 가 중복입니다")
        seen.add(rid)
        kind = r.get("kind")
        if kind not in REQUIRED:
            problems.append(f"{rid}: 모르는 kind {kind!r} (허용: {', '.join(sorted(REQUIRED))})")
            continue
        sev, act = r.get("severity"), r.get("action")
        if sev not in SEVERITIES:
            problems.append(f"{rid}: severity 는 {SEVERITIES} 중 하나")
        if act not in ACTIONS:
            problems.append(f"{rid}: action 은 {ACTIONS} 중 하나")
        if act == "block_mart" and kind not in BLOCK_KINDS:
            problems.append(f"{rid}: block_mart 는 {BLOCK_KINDS} 규칙에만 쓸 수 있습니다")
        if r.get("mode") not in (None, "enforce"):
            problems.append(f"{rid}: mode 는 비우거나 enforce")
        for k in REQUIRED[kind]:
            if k not in r:
                problems.append(f"{rid}: {kind} 에는 {k} 가 필요합니다")
        for k in _TABLE_KEYS:
            if k in r and not _TABLE.fullmatch(str(r[k])):
                problems.append(f"{rid}: {k}={r[k]!r} 는 스키마.표 형식(dim|fact|mart|ops)이어야 합니다")
        for k in _COL_KEYS:
            if k in r and not _COL.fullmatch(str(r[k])):
                problems.append(f"{rid}: {k}={r[k]!r} 는 열 이름이 아닙니다")
        if kind == "hour_coverage" and not (isinstance(r.get("hours"), list) and len(r["hours"]) == 2 and 0 <= r["hours"][0] <= r["hours"][1] <= 23):
            problems.append(f"{rid}: hours 는 [시작, 끝] (0~23)")
        meta = {"id", "kind", "severity", "action", "mode"}
        rules.append(Rule(rid, str(kind), str(sev), str(act), r.get("mode"), {k: v for k, v in r.items() if k not in meta}))
    if problems:
        raise RuleError("; ".join(problems))
    return RuleSet(shadow_until, tuple(rules))


def load(path: Optional[Path] = None) -> RuleSet:
    import yaml
    p = Path(path) if path else Path(__file__).resolve().parents[3] / "config" / "dq_rules.yaml"
    return parse(yaml.safe_load(p.read_text(encoding="utf-8")) or {})
