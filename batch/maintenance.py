"""Step 7c 창고 관리: 통계 갱신(ANALYZE), 표 크기 기록, 오래된 운영 기록 삭제. DAG warehouse_maintenance 가 부른다.

- ANALYZE 는 소유자 권한 함수(ops.analyze_warehouse)로 한다. VACUUM 은 autovacuum 에 맡기고 n_dead_est 로 지켜본다 (D-110).
- Bronze/Silver 파일 정리는 여기 없다: Airflow 는 그 폴더를 읽기 전용으로만 본다 (D-21, D-24).
- psycopg 는 batch.jobs 를 통해 늦게 불러온다.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Optional

LOG = logging.getLogger("urbanstream.batch.maintenance")
RETENTION_DAYS = 180


def _connect(connect: Optional[Callable[[], Any]]):
    if connect is not None:
        return connect()
    from batch.jobs import _default_connect
    return _default_connect()


def analyze_warehouse(*, connect: Optional[Callable[[], Any]] = None) -> int:
    with _connect(connect) as conn:
        n = conn.execute("SELECT ops.analyze_warehouse()").fetchone()[0]
    LOG.info("ANALYZE %d개 표", n)
    return n


def today_kst() -> date:
    from datetime import datetime, timedelta, timezone
    return datetime.now(timezone(timedelta(hours=9))).date()


def snapshot_table_stats(snapshot_date: Optional[date] = None, *, connect: Optional[Callable[[], Any]] = None) -> int:
    """dim/fact/mart/ops 표의 크기·행 수 추정·죽은 튜플 추정을 snapshot_date 로 기록한다 (같은 날 다시 하면 교체).
    snapshot_date 기본값 = 지금(KST) 날짜. 표 상태는 '실행한 시점'의 값이라 밀린 실행의 논리 날짜를 쓰면 이력이 거짓이 된다."""
    snapshot_date = snapshot_date or today_kst()
    with _connect(connect) as conn, conn.transaction():
        conn.execute("DELETE FROM ops.table_stats WHERE snapshot_date = %s", (snapshot_date,))
        cur = conn.execute("""
            INSERT INTO ops.table_stats (snapshot_date, schema_name, table_name, n_live_est, n_dead_est, total_bytes,
                                         last_autovacuum, last_autoanalyze)
            SELECT %s, n.nspname, c.relname, greatest(c.reltuples, 0)::bigint, s.n_dead_tup,
                   pg_total_relation_size(c.oid), s.last_autovacuum, s.last_autoanalyze
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE n.nspname IN ('dim','fact','mart','ops') AND c.relkind IN ('r','m','p')
            """, (snapshot_date,))
        n = cur.rowcount
    LOG.info("표 통계 %s: %d개", snapshot_date, n)
    return n


def prune_ops(*, retention_days: int = RETENTION_DAYS, connect: Optional[Callable[[], Any]] = None) -> dict:
    """retention_days 보다 오래된 적재 이력·호출 감사·표 통계를 지운다. 실행 중(running) 적재 이력은 남긴다."""
    if retention_days < 30:
        raise ValueError("retention_days 는 30 이상이어야 한다 (상류 검사 72시간·DQ 창보다 짧으면 안 됨)")
    with _connect(connect) as conn, conn.transaction():
        a = conn.execute("DELETE FROM ops.load_run WHERE status <> 'running' AND coalesce(finished_at, started_at) < now() - make_interval(days => %s)",
                         (retention_days,)).rowcount
        b = conn.execute("DELETE FROM ops.call_audit WHERE event_date < (now() AT TIME ZONE 'Asia/Seoul')::date - %s",
                         (retention_days,)).rowcount
        c = conn.execute("DELETE FROM ops.table_stats WHERE snapshot_date < (now() AT TIME ZONE 'Asia/Seoul')::date - %s",
                         (retention_days,)).rowcount
    LOG.info("운영 기록 정리(%d일): load_run %d, call_audit %d, table_stats %d", retention_days, a, b, c)
    return {"load_run": a, "call_audit": b, "table_stats": c, "retention_days": retention_days}
