"""Postgres 통합 테스트 공통 fixture. conftest.py 로 두지 않는 이유: tests/contract 가 `from conftest import ...` 를 쓰기 때문에
conftest 라는 모듈 이름이 둘이 되면 전체 실행이 깨진다. 사용: `from pg_support import pg  # noqa: F401`.
 GOLD_TEST_DSN (예: host=127.0.0.1 port=5432 dbname=ut_test user=postgres) 이 없으면 건너뛴다.
테스트마다 dim/fact/mart/ops 스키마를 지우고 마이그레이션을 새로 적용한다 → 반드시 테스트 전용 DB 를 가리킬 것."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]



@pytest.fixture
def pg():
    dsn = os.environ.get("GOLD_TEST_DSN")
    if not dsn:
        pytest.skip("GOLD_TEST_DSN 없음 — Postgres 통합 테스트 건너뜀")
    psycopg = pytest.importorskip("psycopg")
    from scripts.migrate import migrate
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dim, fact, mart, ops CASCADE")
        migrate(conn, ROOT / "sql")
        yield conn
