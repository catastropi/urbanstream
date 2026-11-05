"""DQ 검사 구현. 각 함수는 (conn, p, run_date) → [Result]. 표·열 이름은 린터(rules.py)가 형식을 확인한 값만 SQL 에 끼운다. 값은 모두 파라미터.

원칙
- 표본 부족·빈 표는 pass 가 아니라 unknown.
- 수집 공백(ops.data_gap)으로 설명되는 부족은 fail 이 아니라 warn (수집기를 못 불렀으니 DQ 문제가 아니다).
- 한 검사가 예외를 던져도 다른 검사는 계속된다 (engine 이 잡는다).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class Result:
    status: str                       # pass / warn / fail / unknown
    target: str = ""
    value: float | None = None
    n_checked: int | None = None
    threshold: str = ""
    detail: Mapping[str, Any] = field(default_factory=dict)


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    a = datetime.combine(d, time(0), tzinfo=KST)
    return a, a + timedelta(days=1)


def gap_hours_on(conn, d: date) -> float:
    """그 KST 날짜에 걸친 수집 공백(host_stall·collector_blackout)의 합(시간). 겹치는 구간은 한 번만 센다."""
    a, b = _day_bounds(d)
    rows = conn.execute("SELECT gap_start, gap_end FROM ops.data_gap WHERE gap_end > %s AND gap_start < %s ORDER BY gap_start", (a, b)).fetchall()
    total, cur_end = 0.0, a
    for s, e in rows:
        s, e = max(s, a, cur_end), min(e, b)
        if e > s:
            total += (e - s).total_seconds()
            cur_end = e
    return total / 3600.0


def _rate_result(n: int, bad: int, p_min_rows: int, max_rate: float, what: str) -> Result:
    if n < p_min_rows:
        return Result("unknown", value=None, n_checked=n, threshold=f"rows>={p_min_rows}", detail={"reason": "insufficient_rows", "rows": n})
    rate = bad / n
    return Result("pass" if rate <= max_rate else "fail", value=rate, n_checked=n, threshold=f"{what}<={max_rate}", detail={"bad": bad})


def null_rate(conn, p, d: date) -> list[Result]:
    n, bad = conn.execute(f"SELECT count(*), count(*) FILTER (WHERE {p['column']} IS NULL) FROM {p['table']} WHERE {p['date_col']} = %s", (d,)).fetchone()
    return [_rate_result(n, bad, int(p["min_rows"]), float(p["max_rate"]), "null_rate")]


def range_check(conn, p, d: date) -> list[Result]:
    n, bad = conn.execute(
        f"SELECT count({p['column']}), count(*) FILTER (WHERE {p['column']} < %s OR {p['column']} > %s) FROM {p['table']} WHERE {p['date_col']} = %s",
        (p["min"], p["max"], d)).fetchone()
    r = _rate_result(n, bad, int(p["min_rows"]), float(p["max_out_rate"]), "out_of_range_rate")
    return [Result(r.status, "", r.value, r.n_checked, f"{p['min']}..{p['max']}, {r.threshold}", r.detail)]


def flag_rate(conn, p, d: date) -> list[Result]:
    n, bad = conn.execute(f"SELECT count(*), count(*) FILTER (WHERE %s = ANY(dq_flags)) FROM {p['table']} WHERE {p['date_col']} = %s", (p["flag"], d)).fetchone()
    return [_rate_result(n, bad, int(p["min_rows"]), float(p["max_rate"]), f"{p['flag']}_rate")]


def future_time(conn, p, d: date) -> list[Result]:
    n, bad = conn.execute(
        f"SELECT count(*), count(*) FILTER (WHERE {p['ts_col']} > {p['fetched_col']} + make_interval(secs => %s)) FROM {p['table']} WHERE {p['date_col']} = %s",
        (float(p["margin_sec"]), d)).fetchone()
    if n == 0:
        return [Result("unknown", n_checked=0, threshold=f"count<={p['max_count']}", detail={"reason": "no_rows"})]
    return [Result("pass" if bad <= int(p["max_count"]) else "fail", value=float(bad), n_checked=n,
                   threshold=f"future(>{p['margin_sec']}s)<={p['max_count']}", detail={"future_rows": bad})]


def hour_coverage(conn, p, d: date) -> list[Result]:
    total = conn.execute(f"SELECT count(*) FROM {p['table']}").fetchone()[0]
    if total == 0:
        return [Result("unknown", n_checked=0, threshold=f"hours>={p['min_hours']}", detail={"reason": "empty_table"})]
    lo, hi = p["hours"]
    hours = {int(r[0]) for r in conn.execute(
        f"SELECT DISTINCT extract(hour FROM {p['ts_col']} AT TIME ZONE 'Asia/Seoul') FROM {p['table']} WHERE {p['date_col']} = %s", (d,)).fetchall()}
    present = len([h for h in hours if lo <= h <= hi])
    need = int(p["min_hours"])
    gap_h = gap_hours_on(conn, d)
    detail = {"present_hours": present, "window": [lo, hi], "gap_hours": round(gap_h, 2)}
    if present >= need:
        return [Result("pass", value=float(present), threshold=f"hours>={need}", detail=detail)]
    if present + math.ceil(gap_h) >= need:                         # 빠진 시간을 수집 공백이 설명한다
        return [Result("warn", value=float(present), threshold=f"hours>={need}", detail={**detail, "explained_by_data_gap": True})]
    return [Result("fail", value=float(present), threshold=f"hours>={need}", detail=detail)]


def volume_drift(conn, p, d: date) -> list[Result]:
    rows = conn.execute(f"SELECT {p['date_col']}, count(*) FROM {p['table']} WHERE {p['date_col']} >= %s AND {p['date_col']} <= %s GROUP BY 1",
                        (d - timedelta(days=int(p["baseline_days"])), d)).fetchall()
    by = {r[0]: r[1] for r in rows}
    today = by.get(d, 0)
    base = sorted(v for k, v in by.items() if k < d and v > 0)
    if len(base) < int(p["min_baseline_days"]):
        return [Result("unknown", n_checked=today, threshold=f"baseline_days>={p['min_baseline_days']}", detail={"reason": "short_baseline", "baseline_days": len(base)})]
    med = base[len(base) // 2] if len(base) % 2 else (base[len(base) // 2 - 1] + base[len(base) // 2]) / 2
    ratio = today / med
    detail = {"today": today, "baseline_median": med, "baseline_days": len(base)}
    thr = f"{p['low']}<=ratio<={p['high']}"
    if p["low"] <= ratio <= p["high"]:
        return [Result("pass", value=ratio, n_checked=today, threshold=thr, detail=detail)]
    if ratio < p["low"] and gap_hours_on(conn, d) >= 1.0:           # 적은 것은 수집 공백이 있었기 때문일 수 있다
        return [Result("warn", value=ratio, n_checked=today, threshold=thr, detail={**detail, "explained_by_data_gap": True})]
    return [Result("fail", value=ratio, n_checked=today, threshold=thr, detail=detail)]


def ref_integrity(conn, p, d: date) -> list[Result]:
    parents = conn.execute(f"SELECT count(*) FROM {p['parent_table']}").fetchone()[0]
    if parents == 0:
        return [Result("unknown", threshold=f"orphan_rate<={p['max_orphan_rate']}", detail={"reason": "empty_parent"})]
    n, bad = conn.execute(
        f"SELECT count(c.{p['child_col']}), count(*) FILTER (WHERE c.{p['child_col']} IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM {p['parent_table']} x WHERE x.{p['parent_col']} = c.{p['child_col']})) FROM {p['child_table']} c WHERE c.{p['date_col']} = %s", (d,)).fetchone()
    return [_rate_result(n, bad, int(p["min_rows"]), float(p["max_orphan_rate"]), "orphan_rate")]


def mart_reliability(conn, p, d: date) -> list[Result]:
    """마트의 provisional 비율이 '지금 기대하는 상태'와 맞는지. 기준선 span 이 mature_after_days 전이면 provisional 이 정상(pass), 지나면 대부분 아니어야 한다."""
    n, prov = conn.execute(f"SELECT count(*), count(*) FILTER (WHERE {p['flag_col']}) FROM {p['mart_table']}").fetchone()
    if n == 0:
        return [Result("unknown", n_checked=0, threshold="rows>0", detail={"reason": "empty_mart"})]
    lo = conn.execute(f"SELECT min({p['source_date_col']}) FROM {p['source_table']}").fetchone()[0]
    span = (d - lo).days + 1 if lo else 0
    expected = "mature" if span >= int(p["mature_after_days"]) else "provisional"
    share = prov / n
    ok = share >= 0.5 if expected == "provisional" else share <= 0.1
    return [Result("pass" if ok else "warn", value=share, n_checked=n, threshold=f"expected_state={expected}",
                   detail={"expected_state": expected, "provisional_rows": prov, "source_span_days": span})]


def load_health(conn, p, d: date) -> list[Result]:
    out = []
    last = conn.execute("SELECT max(finished_at) FROM ops.load_run WHERE job='gold_load' AND status='ok' AND dag_id = %s", (p["dag_id"],)).fetchone()[0]
    if last is None:
        out.append(Result("unknown", "last_ok", detail={"reason": "no_ok_run_recorded", "dag_id": p["dag_id"]}))
    else:
        age_h = (conn.execute("SELECT now()").fetchone()[0] - last).total_seconds() / 3600
        out.append(Result("pass" if age_h <= float(p["max_age_hours"]) else "fail", "last_ok", age_h, threshold=f"age_h<={p['max_age_hours']}", detail={"last_ok_at": last.isoformat()}))
    stale = conn.execute("SELECT count(*) FROM ops.load_run WHERE job='gold_load' AND status='running' AND dag_id = %s AND started_at < now() - make_interval(secs => %s)",
                         (p["dag_id"], float(p["stale_running_hours"]) * 3600)).fetchone()[0]
    out.append(Result("pass" if stale == 0 else "fail", "stale_running", float(stale), threshold="stale_running==0", detail={}))
    return out


def call_fail_rate(conn, p, d: date) -> list[Result]:
    a, b = _day_bounds(d)
    rows = conn.execute("SELECT source_system, count(*), count(*) FILTER (WHERE outcome IN ('failed','permanent_failure')) FROM ops.call_event "
                        "WHERE event='call' AND final AND source_system IS NOT NULL AND recorded_at >= %s AND recorded_at < %s GROUP BY 1 ORDER BY 1", (a, b)).fetchall()
    if not rows:
        return [Result("unknown", threshold=f"fail_rate<={p['max_rate']}", detail={"reason": "no_call_events"})]
    return [dict_to(_rate_result(n, bad, int(p["min_calls"]), float(p["max_rate"]), "fail_rate"), src) for src, n, bad in rows]


def dict_to(r: Result, target: str) -> Result:
    return Result(r.status, target, r.value, r.n_checked, r.threshold, r.detail)


def incident_link_rate(conn, p, d: date) -> list[Result]:
    """돌발이 수집 링크에 연결된 비율. 약 1% 가 정상이다(수집 링크가 4개뿐, D-112) → 값만 기록하고 pass. 표본이 없으면 unknown."""
    n, linked = conn.execute("SELECT count(*), count(*) FILTER (WHERE match_kind <> 'no_link') FROM mart.incident_impact").fetchone()
    if n < int(p["min_rows"]):
        return [Result("unknown", n_checked=n, threshold=f"rows>={p['min_rows']}", detail={"reason": "insufficient_rows"})]
    return [Result("pass", value=linked / n, n_checked=n, threshold="info only", detail={"linked": linked, "expected_state": "low (~1%, 수집 링크 4개)"})]


def table_bloat(conn, p, d: date) -> list[Result]:
    rows = conn.execute(
        "SELECT schema_name, table_name, n_live_est, n_dead_est FROM ops.table_stats WHERE snapshot_date = (SELECT max(snapshot_date) FROM ops.table_stats) "
        "AND n_live_est >= %s ORDER BY 1, 2", (int(p["min_live_rows"]),)).fetchall()
    if not rows:
        return [Result("unknown", threshold=f"dead/live<={p['max_ratio']}", detail={"reason": "no_snapshot_or_small_tables"})]
    return [Result("pass" if (dead or 0) / live <= float(p["max_ratio"]) else "fail", f"{s}.{t}", (dead or 0) / live, live,
                   f"dead/live<={p['max_ratio']}", {"n_live_est": live, "n_dead_est": dead}) for s, t, live, dead in rows]


CHECKS: dict[str, Callable[..., list[Result]]] = {
    "null_rate": null_rate, "range": range_check, "flag_rate": flag_rate, "future_time": future_time, "hour_coverage": hour_coverage,
    "volume_drift": volume_drift, "ref_integrity": ref_integrity, "mart_reliability": mart_reliability, "load_health": load_health,
    "call_fail_rate": call_fail_rate, "incident_link_rate": incident_link_rate, "table_bloat": table_bloat,
}
