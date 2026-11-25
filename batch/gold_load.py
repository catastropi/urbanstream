"""Gold 적재 (호스트 Python): data/gold_stage/<표>/ Parquet → PostgreSQL.

표마다 COPY → 임시 표 → INSERT … ON CONFLICT. 사실 표는 DO NOTHING(불변), 기상·감사는 DO UPDATE(나중에 값이 채워질 수 있음).
돌발 수명은 스테이징이 '시작일-1 ~ 종료일'의 관측만으로 계산하므로(gold_stage 머리말) 덮어쓰지 않고 기존 행과 합친다(EPISODE_MERGE).
같은 스테이징을 두 번 적재해도 행이 늘지 않는다. 실행 기록은 ops.load_run. 날짜 범위는 스테이징 manifest 의 것을 기록한다.

mode="replace" (재처리 경로 전용, STEP 1 · ADR-0015): 날짜 범위 d0~d1 의 교체 대상 표를 지우고 스테이징으로 다시 넣는다. 전체가 한 트랜잭션이다.
  중간에 하나라도 실패하면 앞 표의 DELETE 까지 되돌아가 Gold 는 적재 전 그대로다. 평소 DAG(gold_intraday·gold_daily)는 계속 upsert 다.
  가드: manifest 범위 = d0~d1, 교체 대상 표의 스테이징이 0행인데 DB 에 행이 있으면 거부(allow_empty 없이는).
모든 적재는 pg_advisory_xact_lock 으로 서로 겹치지 않는다(Airflow 풀 gold_io 에 더한 2차 방어. 손으로 돌린 적재도 막는다).

사용: python -m batch.gold_load [--stage data/gold_stage] [--refresh-mart]
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@dataclass(frozen=True)
class TableSpec:
    name: str                      # 스테이징 폴더 이름
    target: str                    # 스키마.표
    keys: tuple[str, ...]
    update: bool                   # True 면 충돌 시 갱신
    geom: Optional[str] = None     # 스테이징에 없는 geometry 열을 lon/lat 에서 만든다
    merge: Optional[str] = None    # 충돌 시 SET 절을 직접 준다 (update=True 일 때 'c = EXCLUDED.c' 대신)
    after: Optional[str] = None    # INSERT 뒤 같은 트랜잭션에서 실행할 SQL (_stg 를 쓸 수 있다)


# 돌발 수명 합치기. 스테이징 값은 '이번 구간에서 본 수명'이다.
# - first_seen_at·last_seen_at: 더 이른 값·더 늦은 값. is_long_term: 한 번이라도 참이면 참.
# - absent_since: 스테이징의 마지막 관측이 DB 보다 늦으면 새 값. 같으면 둘 중 이른 값(NULL 은 '아직 증거 없음'이라 무시 —
#   과거 하루를 다시 돌리면 그날 호출만 보므로 NULL 이 나올 수 있다). 더 늦은 관측이 이미 DB 에 있으면 DB 값을 지킨다.
# - expected_clear_at: 마지막 관측이 DB 이상일 때만 새 값(가장 늦게 본 관측의 값).
# - obs_count: 구간 밖 관측은 스테이징이 모르므로, 적재 뒤 fact.incident_observation 에서 다시 센다(EPISODE_RECOUNT).
#   fact.incident_observation 이 SPECS 에서 먼저 적재되므로 이번 구간의 관측도 이미 들어 있다.
EPISODE_MERGE = (
    "first_seen_at = LEAST(t.first_seen_at, EXCLUDED.first_seen_at), "
    "last_seen_at = GREATEST(t.last_seen_at, EXCLUDED.last_seen_at), "
    "absent_since = CASE WHEN EXCLUDED.last_seen_at > t.last_seen_at THEN EXCLUDED.absent_since "
    "WHEN EXCLUDED.last_seen_at = t.last_seen_at THEN LEAST(t.absent_since, EXCLUDED.absent_since) "
    "ELSE t.absent_since END, "
    "expected_clear_at = CASE WHEN EXCLUDED.last_seen_at >= t.last_seen_at THEN EXCLUDED.expected_clear_at "
    "ELSE t.expected_clear_at END, "
    "obs_count = GREATEST(t.obs_count, EXCLUDED.obs_count), "
    "is_long_term = t.is_long_term OR EXCLUDED.is_long_term")
EPISODE_RECOUNT = """
UPDATE fact.incident_episode e
   SET obs_count = GREATEST(e.obs_count, o.n),
       first_seen_at = LEAST(e.first_seen_at, o.first_at),
       last_seen_at = GREATEST(e.last_seen_at, o.last_at)
  FROM (SELECT o.acc_id, o.occurred_at, count(DISTINCT o.ingest_id)::int AS n, min(o.fetched_at) AS first_at,
               max(o.fetched_at) AS last_at
          FROM fact.incident_observation o
          JOIN (SELECT DISTINCT acc_id, occurred_at FROM _stg) s ON s.acc_id = o.acc_id AND s.occurred_at = o.occurred_at
         GROUP BY o.acc_id, o.occurred_at) o
 WHERE e.acc_id = o.acc_id AND e.occurred_at = o.occurred_at
   AND (e.obs_count < o.n OR e.first_seen_at > o.first_at OR e.last_seen_at < o.last_at)
"""

# 사라짐 시각을 DB 에서 다시 판정한다(D-278). 증분 스테이징은 새 관측·새 호출만 보므로, 이전 실행에서 마지막으로 보인 사건이
# 이번 구간의 호출에서 사라진 경우를 Spark 쪽에서는 알 수 없다. 규칙은 gold_stage.stage_episodes 와 같다:
# 마지막 관측 뒤의 **성공·비절단** 돌발 호출 중 첫 시각. 단 아직 관측이 적재되지 않았을 수 있는 호출(Silver 는 Bronze 보다
# 1~2분 늦다)은 쓰지 않는다 → 호출 시각 ≤ 지금까지 적재된 가장 늦은 관측 시각.
EPISODE_ABSENT = """
WITH open AS (SELECT acc_id, occurred_at, last_seen_at FROM fact.incident_episode WHERE absent_since IS NULL),
     bound AS (SELECT max(last_seen_at) AS b, (SELECT min(last_seen_at) FROM open) AS lo FROM fact.incident_episode),
     calls AS (SELECT c.fetched_at FROM ops.call_audit c, bound
                WHERE c.source_system = 'seoul_incident' AND c.ok AND NOT c.truncated
                  AND c.fetched_at > bound.lo AND c.fetched_at <= bound.b),
     found AS (SELECT o.acc_id, o.occurred_at,
                      (SELECT min(k.fetched_at) FROM calls k WHERE k.fetched_at > o.last_seen_at) AS a FROM open o)
UPDATE fact.incident_episode e SET absent_since = f.a
  FROM found f
 WHERE e.acc_id = f.acc_id AND e.occurred_at = f.occurred_at AND f.a IS NOT NULL
"""


SPECS: tuple[TableSpec, ...] = (
    TableSpec("traffic_speed", "fact.traffic_speed", ("dedup_key",), False),
    TableSpec("bus_position", "fact.bus_position", ("dedup_key",), False),
    TableSpec("bus_arrival", "fact.bus_arrival", ("dedup_key",), False),
    TableSpec("incident_observation", "fact.incident_observation", ("dedup_key", "ingest_id"), False, geom="geom"),
    TableSpec("weather_obs", "fact.weather_obs", ("base_time", "nx", "ny"), True),
    TableSpec("incident_episode", "fact.incident_episode", ("acc_id", "occurred_at"), True,
              merge=EPISODE_MERGE, after=EPISODE_RECOUNT),
    TableSpec("call_audit", "ops.call_audit", ("ingest_id",), True),
)

# replace 모드에서 범위를 지우고 다시 넣을 수 있는 표 → 날짜 열. 돌발 수명은 날짜 열이 없어 합치기(EPISODE_MERGE, D-277),
# 호출 감사는 Bronze audit 에서 오고 Silver 와 무관하므로 upsert 로 둔다.
REPLACEABLE: dict[str, str] = {"traffic_speed": "event_date", "bus_position": "event_date", "bus_arrival": "event_date",
                               "incident_observation": "observed_date", "weather_obs": "event_date"}
LOCK_KEY = "urbanstream.gold_load"


class ReplaceGuardError(RuntimeError):
    """범위 교체를 하면 안 되는 상태 (빈 스테이징, 범위 불일치)."""


def _read_rows(path: Path):
    import pyarrow.parquet as pq
    if not path.exists() or not any(path.glob("*.parquet")):
        return [], []
    tbl = pq.read_table(str(path))
    cols = tbl.column_names
    data = tbl.to_pydict()
    rows = []
    for i in range(tbl.num_rows):
        row = []
        for c in cols:
            v = data[c][i]
            if isinstance(v, datetime) and v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)          # 스테이징은 UTC 로 쓴다
            row.append(v)
        rows.append(tuple(row))
    return cols, rows


def _lock(conn) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (LOCK_KEY,))


def load_table(conn, spec: TableSpec, stage: Path, *, conflict: bool = True) -> int:
    """스테이징 → 한 표. 반환값 = 스테이징 행 수 (실제 새로 들어간 수는 아님).
    conflict=False 는 범위를 방금 지운 뒤에 쓴다: 충돌 절이 없어서 키가 범위 밖에 이미 있으면 PK 오류로 크게 실패한다."""
    cols, rows = _read_rows(stage / spec.name)
    if not rows:
        return 0
    col_list = ", ".join(cols)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE _stg ON COMMIT DROP AS SELECT {col_list} FROM {spec.target} WITH NO DATA")
        with cur.copy(f"COPY _stg ({col_list}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)
        insert_cols = col_list + (f", {spec.geom}" if spec.geom else "")
        select = col_list + (", CASE WHEN lon IS NOT NULL AND lat IS NOT NULL THEN ST_SetSRID(ST_MakePoint(lon, lat), 4326) END"
                             if spec.geom else "")
        keys = ", ".join(spec.keys)
        alias = " AS t" if spec.merge else ""
        if not conflict:
            action = ""
        elif spec.update:
            sets = spec.merge or ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in spec.keys)
            action = f" ON CONFLICT ({keys}) DO UPDATE SET {sets}"
        else:
            action = f" ON CONFLICT ({keys}) DO NOTHING"
        cur.execute(f"INSERT INTO {spec.target}{alias} ({insert_cols}) SELECT {select} FROM _stg{action}")
        if spec.after:
            cur.execute(spec.after)
        cur.execute("DROP TABLE _stg")
    return len(rows)


def _staged_rows(stage: Path, name: str) -> int:
    import pyarrow.parquet as pq
    p = stage / name
    if not p.exists() or not any(p.glob("*.parquet")):
        return 0
    return sum(pq.ParquetFile(str(f)).metadata.num_rows for f in p.glob("*.parquet"))


def check_replace(conn, stage: Path, manifest: dict, d0: date, d1: date, tables: set[str], *, allow_empty: bool = False) -> None:
    """범위 교체 전 가드. 트랜잭션 안에서 부른다(잠금을 잡은 뒤 DB 행 수를 본다)."""
    if manifest.get("date_from") != d0.isoformat() or manifest.get("date_to") != d1.isoformat():
        raise ReplaceGuardError(f"manifest 범위({manifest.get('date_from')}~{manifest.get('date_to')})가 교체 범위({d0}~{d1})와 다릅니다")
    unknown = tables - set(REPLACEABLE)
    if unknown:
        raise ReplaceGuardError(f"범위 교체를 할 수 없는 표: {sorted(unknown)}")
    if allow_empty:
        return
    for name in sorted(tables):
        target = next(s.target for s in SPECS if s.name == name)
        if _staged_rows(stage, name) == 0:
            n = conn.execute(f"SELECT count(*) FROM {target} WHERE {REPLACEABLE[name]} BETWEEN %s AND %s", (d0, d1)).fetchone()[0]
            if n:
                raise ReplaceGuardError(f"{name}: 스테이징이 0행인데 DB 에 {n}행이 있습니다 — 지우지 않습니다 (의도한 것이면 allow_empty)")


def load_all(conn, stage: Path, refresh_mart: bool = False, specs: tuple[TableSpec, ...] = SPECS, dag_id: str | None = None,
             *, mode: str = "upsert", replace: Optional[set[str]] = None, d0: Optional[date] = None, d1: Optional[date] = None,
             allow_empty: bool = False) -> dict:
    """dag_id: 적재를 부른 DAG 이름 (ops.load_run.dag_id, D-107). job 은 계속 'gold_load' 다 — 마트 선행조건이 job 으로 거른다.

    mode="upsert"(기본): 지금까지와 같다. 표마다 한 트랜잭션.
    mode="replace": replace(표 이름 집합, 기본 REPLACEABLE 전부)의 d0~d1 을 지우고 다시 넣는다. 나머지 표는 upsert. 전체가 한 트랜잭션."""
    if mode not in ("upsert", "replace"):
        raise ValueError(f"mode 는 upsert 또는 replace: {mode!r}")
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    if mode == "replace":
        if d0 is None or d1 is None:
            raise ReplaceGuardError("replace 모드에는 d0, d1 이 필요합니다")
        replace = set(REPLACEABLE) if replace is None else set(replace)
    else:
        d0 = date.fromisoformat(manifest["date_from"]) if manifest.get("date_from") else date(1970, 1, 1)
        d1 = date.fromisoformat(manifest["date_to"]) if manifest.get("date_to") else date(2999, 12, 31)
    run_id = conn.execute("INSERT INTO ops.load_run (job, date_from, date_to, dag_id) VALUES ('gold_load', %s, %s, %s) RETURNING run_id",
                          (d0, d1, dag_id)).fetchone()[0]
    counts: dict[str, int] = {}
    deleted: dict[str, int] = {}
    absent_updated = 0
    try:
        if mode == "replace":
            with conn.transaction():                        # 전부 한 트랜잭션: 하나라도 실패하면 앞 표의 DELETE 까지 되돌아간다
                _lock(conn)
                check_replace(conn, stage, manifest, d0, d1, replace, allow_empty=allow_empty)
                for s in specs:
                    if s.name in replace:
                        cur = conn.execute(f"DELETE FROM {s.target} WHERE {REPLACEABLE[s.name]} BETWEEN %s AND %s", (d0, d1))
                        deleted[s.name] = cur.rowcount or 0
                        counts[s.name] = load_table(conn, s, stage, conflict=False)
                    else:
                        counts[s.name] = load_table(conn, s, stage)
                absent_updated = conn.execute(EPISODE_ABSENT).rowcount or 0
                conn.execute("UPDATE ops.load_run SET status='ok', finished_at=now(), row_counts=%s::jsonb WHERE run_id=%s",
                             (json.dumps({**counts, "_deleted": deleted, "_mode": "replace"}), run_id))
        else:
            for s in specs:
                with conn.transaction():                    # 표마다 한 트랜잭션: 중간에 죽어도 끝난 표는 남고, 다시 돌려도 안전
                    _lock(conn)
                    counts[s.name] = load_table(conn, s, stage)
            with conn.transaction():                        # 수명·감사가 모두 들어간 뒤 사라짐 판정 (EPISODE_ABSENT)
                _lock(conn)
                absent_updated = conn.execute(EPISODE_ABSENT).rowcount or 0
        if refresh_mart:
            conn.execute("SELECT mart.refresh_all()")
    except Exception as e:                                  # noqa: BLE001
        conn.execute("UPDATE ops.load_run SET status='failed', finished_at=now(), row_counts=%s::jsonb, error=%s WHERE run_id=%s",
                     (json.dumps(counts), f"{type(e).__name__}: {e}"[:500], run_id))
        raise
    if mode == "upsert":
        conn.execute("UPDATE ops.load_run SET status='ok', finished_at=now(), row_counts=%s::jsonb WHERE run_id=%s",
                     (json.dumps(counts), run_id))
    out = {"run_id": run_id, "row_counts": counts, "absent_updated": absent_updated}
    if mode == "replace":
        out["deleted"] = deleted
    return out


def main(argv: Optional[list[str]] = None) -> int:
    import psycopg
    from batch.db import conninfo
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default=str(ROOT / "data/gold_stage"))
    ap.add_argument("--refresh-mart", action="store_true")
    a = ap.parse_args(argv)
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    with psycopg.connect(conninfo(role="writer"), autocommit=True) as conn:
        print(json.dumps(load_all(conn, Path(a.stage), a.refresh_mart), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
