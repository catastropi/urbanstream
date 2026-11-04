"""Airflow 용 DB 역할을 만든다 (멱등). `scripts/stack.sh up-airflow` 가 Airflow 를 켜기 전에 한 번 부른다.

만드는 것
- 역할 airflow  + 데이터베이스 airflow : Airflow 메타데이터 전용. 업무 데이터(urbanstream DB)와 섞이지 않는다.
- 역할 gold_loader (LOGIN, gold_writer 상속) : DAG 가 Gold 적재에 쓰는 최소 권한 계정. 관리자 비밀번호는 Airflow 에 주지 않는다.

왜 sql/ 마이그레이션이 아닌가: CREATE DATABASE 는 트랜잭션 안에서 못 돌린다(migrate.py 는 파일마다 트랜잭션).
비밀번호는 .env 에서 읽고, 출력하지 않는다. 이미 있으면 비밀번호만 .env 값으로 맞춘다.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "=" in s:
                k, _, v = s.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def main() -> int:
    import psycopg
    from psycopg import sql

    env = {**load_env(ROOT / ".env"), **os.environ}
    need = ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD")
    missing = [k for k in need if not env.get(k)]
    if missing:
        print("누락된 .env 항목: " + ", ".join(missing) + "  → scripts/stack.sh init-env 를 먼저 실행하세요")
        return 2
    conninfo = (f"host={env.get('POSTGRES_HOST', '127.0.0.1')} port={env.get('POSTGRES_PORT', '5433')} "
                f"dbname={env.get('POSTGRES_DB', 'urbanstream')} user={env.get('POSTGRES_USER', 'urbanstream')} "
                f"password={env['POSTGRES_PASSWORD']}")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        def role_exists(name: str) -> bool:
            return conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone() is not None

        if not role_exists("gold_writer"):
            print("gold_writer 역할이 없습니다. 먼저 python scripts/migrate.py 를 실행하세요.")
            return 3
        for role, pw, extra in (("airflow", env["AIRFLOW_DB_PASSWORD"], ""),
                                ("gold_loader", env["GOLD_LOADER_PASSWORD"], " IN ROLE gold_writer")):
            verb = "ALTER" if role_exists(role) else "CREATE"
            tail = "" if verb == "ALTER" else extra
            conn.execute(sql.SQL(f"{verb} ROLE {{}} LOGIN PASSWORD {{}}{tail}").format(sql.Identifier(role), sql.Literal(pw)))
            print(f"역할 {role}: {'비밀번호 갱신' if verb == 'ALTER' else '생성'}")
        if conn.execute("SELECT 1 FROM pg_database WHERE datname='airflow'").fetchone() is None:
            conn.execute(sql.SQL("CREATE DATABASE airflow OWNER airflow"))
            print("데이터베이스 airflow: 생성")
        else:
            print("데이터베이스 airflow: 이미 있음")
        # 메타데이터 DB 는 airflow 역할만 접속한다 (PUBLIC 기본 CONNECT 권한 제거)
        conn.execute("REVOKE CONNECT ON DATABASE airflow FROM PUBLIC")
        conn.execute("GRANT CONNECT ON DATABASE airflow TO airflow")
    return 0


if __name__ == "__main__":
    sys.exit(main())
