"""알림 기록 저장소. PgStore = ops.alert_event / ops.alert_state (운영), MemoryStore = DB 가 없을 때·테스트.

두 구현은 같은 동작을 해야 한다(tests/unit/test_notify_core.py 가 MemoryStore 로 규칙을, tests/integration/test_monitor_pg.py 가 PgStore 로 SQL 을 확인한다).
시각은 모두 시간대 있는 datetime 이다.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Optional

STATE_FIELDS = ("level", "title", "status", "first_at", "last_at", "last_sent_at", "count", "suppressed_count", "resolved_at")


def conninfo(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """쓰기 계정 접속 문자열. 명시적인 쓰기 계정(GOLD_DSN 또는 GOLD_WRITER_USER)이 없으면 None → DB 를 쓰지 않는다(호스트 단위 테스트가 DB 를 찾지 않게)."""
    e = os.environ if env is None else env
    if e.get("GOLD_DSN"):
        return e["GOLD_DSN"]
    if not e.get("GOLD_WRITER_USER"):
        return None
    parts = [f"host={e.get('POSTGRES_HOST', '127.0.0.1')}", f"port={e.get('POSTGRES_PORT', '5432')}",
             f"dbname={e.get('POSTGRES_DB', 'urbanstream')}", f"user={e['GOLD_WRITER_USER']}", "connect_timeout=5"]
    if e.get("GOLD_WRITER_PASSWORD"):
        parts.append(f"password={e['GOLD_WRITER_PASSWORD']}")
    return " ".join(parts)


class MemoryStore:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.states: dict[str, dict] = {}
        self._seq = 0

    def get_state(self, key: str) -> Optional[dict]:
        s = self.states.get(key)
        return dict(s) if s else None

    def save_state(self, key: str, **f: Any) -> None:
        cur = self.states.get(key, {})
        cur.update(f)
        self.states[key] = cur

    def add_event(self, **f: Any) -> int:
        self._seq += 1
        e = {"alert_id": self._seq, "original_level": None, "reason": None, "sent_at": None, "meta": {}, "dedup_key": None, "body": ""}
        e.update(f)
        self.events.append(e)
        return self._seq

    def update_event(self, event_id: int, **f: Any) -> None:
        for e in self.events:
            if e["alert_id"] == event_id:
                e.update(f)
                return

    def count_pushed_since(self, ts: datetime) -> int:
        return sum(1 for e in self.events if e["status"] == "sent" and e["sent_at"] and e["sent_at"] >= ts)

    def pending(self, older_than: datetime) -> list[dict]:
        return [dict(e) for e in self.events if e["status"] == "pending" and e["created_at"] <= older_than]

    def digest_pending(self) -> list[dict]:
        return [dict(e) for e in self.events if e["status"] == "digest" or (e["status"] == "suppressed" and e.get("reason") == "rate_limit")]

    def mark_digested(self, ids: Iterable[int]) -> None:
        ids = set(ids)
        for e in self.events:
            if e["alert_id"] in ids:
                e["status"] = "digested"

    def counts_since_last_push(self) -> dict:
        last = max((e["alert_id"] for e in self.events if e["status"] == "sent"), default=None)
        newer = [e for e in self.events if last is None or e["alert_id"] > last]
        return {"failed": sum(1 for e in newer if e["status"] == "failed"),
                "rate_limited": sum(1 for e in newer if e["status"] == "suppressed" and e.get("reason") == "rate_limit")}


class PgStore:
    """psycopg 는 함수 안에서 불러온다(DAG 파싱이 가볍게). 호출마다 짧은 연결 — 알림은 드물다."""

    def __init__(self, dsn: str, connect: Optional[Callable[[], Any]] = None) -> None:
        self.dsn, self._connect = dsn, connect

    def _conn(self):
        if self._connect:
            return self._connect()
        import psycopg
        return psycopg.connect(self.dsn, autocommit=True)

    def get_state(self, key: str) -> Optional[dict]:
        with self._conn() as c:
            r = c.execute(f"SELECT {', '.join(STATE_FIELDS)} FROM ops.alert_state WHERE dedup_key=%s", (key,)).fetchone()
        return dict(zip(STATE_FIELDS, r)) if r else None

    def save_state(self, key: str, **f: Any) -> None:
        """부분 갱신. 먼저 UPDATE 하고 행이 없을 때만 INSERT 한다 (ON CONFLICT 는 INSERT 행의 NOT NULL 을 먼저 검사해서 부분 갱신에 못 쓴다)."""
        cols = [k for k in STATE_FIELDS if k in f]
        with self._conn() as c:
            cur = c.execute(f"UPDATE ops.alert_state SET {', '.join(f'{k}=%s' for k in cols)} WHERE dedup_key=%s", (*[f[k] for k in cols], key))
            if cur.rowcount == 0:
                c.execute(f"INSERT INTO ops.alert_state (dedup_key, {', '.join(cols)}) VALUES (%s, {', '.join(['%s'] * len(cols))}) "
                          f"ON CONFLICT (dedup_key) DO NOTHING", (key, *[f[k] for k in cols]))

    def add_event(self, **f: Any) -> int:
        f = dict(f)
        f["meta"] = json.dumps(f.get("meta") or {}, ensure_ascii=False, default=str)
        cols = ["created_at", "level", "original_level", "title", "body", "dedup_key", "status", "reason", "sent_at", "meta"]
        vals = [f.get(k) for k in cols]
        with self._conn() as c:
            return c.execute(f"INSERT INTO ops.alert_event ({', '.join(cols)}) VALUES ({', '.join(['%s'] * 9)}, %s::jsonb) RETURNING alert_id", vals).fetchone()[0]

    def update_event(self, event_id: int, **f: Any) -> None:
        f = dict(f)
        sets, vals = [], []
        for k, v in f.items():
            if k not in ("level", "original_level", "title", "body", "dedup_key", "status", "reason", "sent_at", "meta"):
                raise ValueError(k)
            sets.append(f"{k}=%s::jsonb" if k == "meta" else f"{k}=%s")
            vals.append(json.dumps(v, ensure_ascii=False, default=str) if k == "meta" else v)
        with self._conn() as c:
            c.execute(f"UPDATE ops.alert_event SET {', '.join(sets)} WHERE alert_id=%s", (*vals, event_id))

    def count_pushed_since(self, ts: datetime) -> int:
        with self._conn() as c:
            return c.execute("SELECT count(*) FROM ops.alert_event WHERE status='sent' AND sent_at >= %s", (ts,)).fetchone()[0]

    def _rows(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._conn() as c:
            cur = c.execute(sql, params)
            names = [d.name for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    _EV = "alert_id, created_at, level, original_level, title, body, dedup_key, status, reason, sent_at, meta"

    def pending(self, older_than: datetime) -> list[dict]:
        return self._rows(f"SELECT {self._EV} FROM ops.alert_event WHERE status='pending' AND created_at <= %s ORDER BY alert_id", (older_than,))

    def digest_pending(self) -> list[dict]:
        return self._rows(f"SELECT {self._EV} FROM ops.alert_event WHERE status='digest' OR (status='suppressed' AND reason='rate_limit') ORDER BY alert_id")

    def mark_digested(self, ids: Iterable[int]) -> None:
        ids = list(ids)
        if ids:
            with self._conn() as c:
                c.execute("UPDATE ops.alert_event SET status='digested' WHERE alert_id = ANY(%s)", (ids,))

    def counts_since_last_push(self) -> dict:
        with self._conn() as c:
            r = c.execute(
                "WITH l AS (SELECT max(alert_id) AS t FROM ops.alert_event WHERE status='sent') "
                "SELECT count(*) FILTER (WHERE a.status='failed'), count(*) FILTER (WHERE a.status='suppressed' AND a.reason='rate_limit') "
                "FROM ops.alert_event a, l WHERE l.t IS NULL OR a.alert_id > l.t").fetchone()
        return {"failed": r[0], "rate_limited": r[1]}
