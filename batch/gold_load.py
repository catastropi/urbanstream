"""Gold 적재 (호스트 Python): data/gold_stage/<표>/ Parquet → PostgreSQL.

표마다 COPY → 임시 표 → INSERT … ON CONFLICT. 사실 표는 DO NOTHING(불변), 수명·기상·감사는 DO UPDATE(나중에 값이 채워질 수 있음).
같은 스테이징을 두 번 적재해도 행이 늘지 않는다. 실행 기록은 ops.load_run. 날짜 범위는 스테이징 manifest 의 것을 기록한다.

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


SPECS: tuple[TableSpec, ...] = (
    TableSpec("traffic_speed", "fact.traffic_speed", ("dedup_key",), False),
    TableSpec("bus_position", "fact.bus_position", ("dedup_key",), False),
    TableSpec("bus_arrival", "fact.bus_arrival", ("dedup_key",), False),
    TableSpec("incident_observation", "fact.incident_observation", ("dedup_key", "ingest_id"), False, geom="geom"),
    TableSpec("weather_obs", "fact.weather_obs", ("base_time", "nx", "ny"), True),
    TableSpec("incident_episode", "fact.incident_episode", ("acc_id", "occurred_at"), True),
    TableSpec("call_audit", "ops.call_audit", ("ingest_id",), True),
)


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


def load_table(conn, spec: TableSpec, stage: Path) -> int:
    """스테이징 → 한 표. 반환값 = 스테이징 행 수 (실제 새로 들어간 수는 아님)."""
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
        if spec.update:
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in spec.keys)
            action = f"DO UPDATE SET {sets}"
        else:
            action = "DO NOTHING"
        cur.execute(f"INSERT INTO {spec.target} ({insert_cols}) SELECT {select} FROM _stg ON CONFLICT ({keys}) {action}")
        cur.execute("DROP TABLE _stg")
    return len(rows)


def load_all(conn, stage: Path, refresh_mart: bool = False, specs: tuple[TableSpec, ...] = SPECS) -> dict:
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    d0 = date.fromisoformat(manifest["date_from"]) if manifest.get("date_from") else date(1970, 1, 1)
    d1 = date.fromisoformat(manifest["date_to"]) if manifest.get("date_to") else date(2999, 12, 31)
    run_id = conn.execute("INSERT INTO ops.load_run (job, date_from, date_to) VALUES ('gold_load', %s, %s) RETURNING run_id",
                          (d0, d1)).fetchone()[0]
    counts: dict[str, int] = {}
    try:
        for s in specs:
            with conn.transaction():                        # 표마다 한 트랜잭션: 중간에 죽어도 끝난 표는 남고, 다시 돌려도 안전
                counts[s.name] = load_table(conn, s, stage)
        if refresh_mart:
            conn.execute("SELECT mart.refresh_all()")
    except Exception as e:                                  # noqa: BLE001
        conn.execute("UPDATE ops.load_run SET status='failed', finished_at=now(), row_counts=%s::jsonb, error=%s WHERE run_id=%s",
                     (json.dumps(counts), f"{type(e).__name__}: {e}"[:500], run_id))
        raise
    conn.execute("UPDATE ops.load_run SET status='ok', finished_at=now(), row_counts=%s::jsonb WHERE run_id=%s",
                 (json.dumps(counts), run_id))
    return {"run_id": run_id, "row_counts": counts}


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
