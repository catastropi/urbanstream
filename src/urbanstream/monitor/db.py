"""감시 루프가 쓰는 SQL 모음. conn 은 autocommit psycopg 연결. now 를 인자로 받아 시험에서 시계를 고정할 수 있다."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional, Sequence

from .audit import INSERT_SQL
from .gaps import Gap
from .health import StatusRow, Summary
from .layers import LayerRow


def insert_call_events(conn, rows: Sequence[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
        return cur.rowcount if cur.rowcount is not None else 0


def get_state(conn, component: str) -> dict:
    r = conn.execute("SELECT state FROM ops.monitor_state WHERE component=%s", (component,)).fetchone()
    return dict(r[0]) if r else {}


def save_state(conn, component: str, state: dict, now: datetime) -> None:
    conn.execute("INSERT INTO ops.monitor_state (component, heartbeat_at, state) VALUES (%s, %s, %s::jsonb) "
                 "ON CONFLICT (component) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at, state=EXCLUDED.state",
                 (component, now, json.dumps(state, default=str)))


def source_summaries(conn, now: datetime) -> dict[str, Summary]:
    """소스별 요약. 마지막 성공은 2일, 최근 호출 통계는 1시간 범위."""
    ok = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT source_system, max(recorded_at) FILTER (WHERE outcome='ok'), max(recorded_at) "
        "FROM ops.call_event WHERE event='call' AND source_system IS NOT NULL AND recorded_at >= %s GROUP BY 1",
        (now - timedelta(days=2),))}
    stats = {r[0]: r[1:] for r in conn.execute(
        "SELECT source_system, count(*) FILTER (WHERE final), count(*) FILTER (WHERE final AND outcome IN ('failed','permanent_failure')), "
        "percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE final AND latency_ms IS NOT NULL) "
        "FROM ops.call_event WHERE event='call' AND source_system IS NOT NULL AND recorded_at > %s GROUP BY 1",
        (now - timedelta(hours=1),))}
    last = {r[0]: r[1:] for r in conn.execute(
        "SELECT DISTINCT ON (source_system) source_system, outcome, reason, budget_used, budget_limit FROM ops.call_event "
        "WHERE event='call' AND source_system IS NOT NULL AND (final OR outcome='skipped') AND recorded_at >= %s "
        "ORDER BY source_system, recorded_at DESC", (now - timedelta(days=2),))}
    out = {}
    for name in set(ok) | set(stats) | set(last):
        lo, lc = ok.get(name, (None, None))
        st = stats.get(name, (0, 0, None))
        la = last.get(name, (None, None, None, None))
        out[name] = Summary(lo, lc, la[0], la[1], int(st[0]), int(st[1]), st[2], la[2], la[3])
    return out


def upsert_source_status(conn, r: StatusRow, now: datetime) -> None:
    conn.execute(
        "INSERT INTO ops.source_status (source_system, status, reason, last_call_at, last_ok_at, age_sec, expected_gap_sec, calls_1h, fail_1h, "
        "p95_latency_ms, budget_used, budget_limit, checked_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (source_system) DO UPDATE SET status=EXCLUDED.status, reason=EXCLUDED.reason, last_call_at=EXCLUDED.last_call_at, "
        "last_ok_at=EXCLUDED.last_ok_at, age_sec=EXCLUDED.age_sec, expected_gap_sec=EXCLUDED.expected_gap_sec, calls_1h=EXCLUDED.calls_1h, "
        "fail_1h=EXCLUDED.fail_1h, p95_latency_ms=EXCLUDED.p95_latency_ms, budget_used=EXCLUDED.budget_used, "
        "budget_limit=EXCLUDED.budget_limit, checked_at=EXCLUDED.checked_at",
        (r.source_system, r.status, r.reason or None, r.last_call_at, r.last_ok_at, r.age_sec, r.expected_gap_sec, r.calls_1h, r.fail_1h,
         r.p95_latency_ms, r.budget_used, r.budget_limit, now))


def upsert_layer_status(conn, r: LayerRow, now: datetime) -> None:
    conn.execute(
        "INSERT INTO ops.layer_status (layer, table_name, source_system, status, reason, last_write_at, age_sec, checked_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (layer, table_name) DO UPDATE SET source_system=EXCLUDED.source_system, "
        "status=EXCLUDED.status, reason=EXCLUDED.reason, last_write_at=EXCLUDED.last_write_at, age_sec=EXCLUDED.age_sec, checked_at=EXCLUDED.checked_at",
        (r.layer, r.table_name, r.source_system, r.status, r.reason or None, r.last_write_at, r.age_sec, now))


def insert_gap(conn, g: Gap, now: datetime) -> bool:
    cur = conn.execute("INSERT INTO ops.data_gap (kind, gap_start, gap_end, duration_sec, evidence, recoverable, detected_at) "
                       "VALUES (%s,%s,%s,%s,%s,false,%s) ON CONFLICT (kind, gap_start) DO NOTHING",
                       (g.kind, g.start, g.end, g.duration_sec, g.evidence, now))
    return cur.rowcount == 1


def recent_gaps(conn, since: datetime) -> list[Gap]:
    return [Gap(r[0], r[1], r[2], r[3] or "") for r in conn.execute(
        "SELECT kind, gap_start, gap_end, evidence FROM ops.data_gap WHERE gap_end >= %s ORDER BY gap_start", (since,))]


def call_times(conn, since: datetime) -> list[datetime]:
    return [r[0] for r in conn.execute(
        "SELECT recorded_at FROM ops.call_event WHERE event='call' AND recorded_at >= %s ORDER BY recorded_at", (since,))]


def resumes_since(conn, since: datetime) -> list[tuple[datetime, float]]:
    return [(r[0], r[1]) for r in conn.execute(
        "SELECT recorded_at, slept_sec FROM ops.call_event WHERE event='host_resume' AND recorded_at >= %s ORDER BY recorded_at", (since,))]


def last_alive_time(conn) -> Optional[datetime]:
    """수집기가 살아 있다는 마지막 증거 (호출 또는 깨어남 알림). 호스트 시계로 찍힌 값이라 컨테이너 시계보다 앞설 수 있다."""
    r = conn.execute("SELECT max(recorded_at) FROM ops.call_event WHERE event IN ('call','host_resume')").fetchone()
    return r[0] if r else None


def rollup_source_health(conn, since: datetime, now: datetime) -> None:
    conn.execute(
        "INSERT INTO ops.source_health (source_system, bucket_start, n_calls, n_ok, n_failed, n_skipped, n_retry, p95_latency_ms, n_records, computed_at) "
        "SELECT source_system, date_trunc('hour', recorded_at), count(*) FILTER (WHERE final OR outcome='skipped'), "
        "count(*) FILTER (WHERE final AND outcome='ok'), count(*) FILTER (WHERE final AND outcome IN ('failed','permanent_failure')), "
        "count(*) FILTER (WHERE outcome='skipped'), count(*) FILTER (WHERE will_retry), "
        "percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE final AND latency_ms IS NOT NULL), "
        "sum(n_records) FILTER (WHERE final), %s FROM ops.call_event "
        "WHERE event='call' AND source_system IS NOT NULL AND recorded_at >= date_trunc('hour', %s::timestamptz) GROUP BY 1, 2 "
        "ON CONFLICT (source_system, bucket_start) DO UPDATE SET n_calls=EXCLUDED.n_calls, n_ok=EXCLUDED.n_ok, n_failed=EXCLUDED.n_failed, "
        "n_skipped=EXCLUDED.n_skipped, n_retry=EXCLUDED.n_retry, p95_latency_ms=EXCLUDED.p95_latency_ms, n_records=EXCLUDED.n_records, "
        "computed_at=EXCLUDED.computed_at", (now, since))
