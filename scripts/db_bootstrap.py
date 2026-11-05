"""Airflow 용 DB 역할을 만든다 (멱등). `scripts/stack.sh up-airflow` 가 Airflow 를 켜기 전에 한 번 부른다.

만드는 것
- 역할 airflow  + 데이터베이스 airflow : Airflow 메타데이터 전용. 업무 데이터(urbanstream DB)와 섞이지 않는다.
- 역할 gold_loader (LOGIN, gold_writer 상속) : DAG 가 Gold 적재에 쓰는 최소 권한 계정. 관리자 비밀번호는 Airflow 에 주지 않는다.

왜 sql/ 마이그레이션이 아닌가: CREATE DATABASE 는 트랜잭션 안에서 못 돌린다(migrate.py 는 파일마다 트랜잭션).
비밀번호는 .env 에서 읽고, 출력하지 않는다. 이미 있으면 비밀번호만 .env 값으로 맞춘다.

Step 9 (API): 역할 api_service 는 sql/013 이 NOLOGIN 으로 만든다. 여기서는 비밀번호를 지정하고 LOGIN 을 켠다(멱등).
  python scripts/db_bootstrap.py --api   → API 역할만 처리한다(API_DB_PASSWORD 필수). scripts/stack.sh up-api 가 부른다.
  인자 없이 실행(up-airflow·up-monitor)하면 기존 동작 그대로이고, API_DB_PASSWORD 가 .env 에 있을 때만 API 역할도 맞춘다 — 없으면 건너뛴다(기존 흐름을 깨지 않는다).
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


API_ROLE = "api_service"
MIN_API_PASSWORD_LEN = 16


def ensure_api_login(conn, password: str) -> str:
    """api_service 에 LOGIN 과 비밀번호를 준다. 반환: 'ok' | 'no_role'(013 이 아직 적용되지 않음). 비밀번호는 출력하지 않는다."""
    from psycopg import sql
    if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (API_ROLE,)).fetchone() is None:
        return "no_role"
    if len(password) < MIN_API_PASSWORD_LEN:
        raise ValueError(f"API_DB_PASSWORD 가 너무 짧습니다({MIN_API_PASSWORD_LEN}자 이상). scripts/stack.sh init-env 로 다시 만드세요")
    conn.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(API_ROLE), sql.Literal(password)))
    return "ok"


def _connect_info(env: dict[str, str]) -> str:
    return (f"host={env.get('POSTGRES_HOST', '127.0.0.1')} port={env.get('POSTGRES_PORT', '5433')} "
            f"dbname={env.get('POSTGRES_DB', 'urbanstream')} user={env.get('POSTGRES_USER', 'urbanstream')} "
            f"password={env['POSTGRES_PASSWORD']}")


def main_api(env: dict[str, str]) -> int:
    import psycopg
    missing = [k for k in ("POSTGRES_PASSWORD", "API_DB_PASSWORD") if not env.get(k)]
    if missing:
        print("누락된 .env 항목: " + ", ".join(missing) + "  → scripts/stack.sh init-env 를 먼저 실행하세요")
        return 2
    with psycopg.connect(_connect_info(env), autocommit=True) as conn:
        try:
            r = ensure_api_login(conn, env["API_DB_PASSWORD"])
        except ValueError as e:
            print(e)
            return 2
    if r == "no_role":
        print(f"{API_ROLE} 역할이 없습니다. 먼저 python scripts/migrate.py 를 실행하세요(sql/013).")
        return 3
    print(f"역할 {API_ROLE}: 비밀번호 설정·LOGIN 켬")
    return 0


def main() -> int:
    import psycopg
    from psycopg import sql

    if "--api" in sys.argv[1:]:
        return main_api({**load_env(ROOT / ".env"), **os.environ})

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
        if env.get("API_DB_PASSWORD"):
            r = ensure_api_login(conn, env["API_DB_PASSWORD"])
            print(f"역할 {API_ROLE}: " + ("비밀번호 설정·LOGIN 켬" if r == "ok" else "건너뜀 (sql/013 이 아직 적용되지 않음)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
