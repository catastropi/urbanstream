"""sql/NNN_*.sql 을 번호 순서로 적용한다. 적용한 파일은 ops.schema_migrations 에 체크섬과 함께 기록한다.

- 이미 적용된 파일의 내용이 바뀌었으면 중단한다(새 번호 파일로 고칠 것).
- 각 파일은 한 트랜잭션. 실패하면 그 파일만 되돌리고 멈춘다.
사용: python scripts/migrate.py [--dir sql] [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_NAME = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def discover(sql_dir: Path) -> list[tuple[str, Path, str]]:
    out = []
    for p in sorted(sql_dir.glob("*.sql")):
        if not _NAME.match(p.name):
            raise SystemExit(f"파일 이름 규칙(NNN_name.sql) 위반: {p.name}")
        out.append((p.name, p, hashlib.sha256(p.read_bytes()).hexdigest()))
    nums = [n[:3] for n, _, _ in out]
    if len(set(nums)) != len(nums):
        raise SystemExit("번호가 겹치는 마이그레이션이 있습니다")
    return out


def migrate(conn, sql_dir: Path, dry_run: bool = False) -> list[str]:
    applied_now: list[str] = []
    with conn.transaction():
        conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        conn.execute("CREATE TABLE IF NOT EXISTS ops.schema_migrations ("
                     "name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())")
    done = dict(conn.execute("SELECT name, checksum FROM ops.schema_migrations").fetchall())
    files = discover(sql_dir)
    for name, _, chk in files:
        if name in done and done[name] != chk:
            raise SystemExit(f"이미 적용된 {name} 의 내용이 바뀌었습니다. 새 번호 파일로 고치세요.")
    for name, path, chk in files:
        if name in done:
            continue
        if dry_run:
            applied_now.append(name)
            continue
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO ops.schema_migrations (name, checksum) VALUES (%s, %s)", (name, chk))
        applied_now.append(name)
    return applied_now


def main() -> int:
    import psycopg
    from batch.db import conninfo
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(ROOT / "sql"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    with psycopg.connect(conninfo(), autocommit=True) as conn:
        done = migrate(conn, Path(a.dir), a.dry_run)
    print(("적용 예정: " if a.dry_run else "적용함: ") + (", ".join(done) if done else "없음 (이미 최신)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
