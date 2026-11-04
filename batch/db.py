"""Postgres 접속 설정. 비밀번호는 환경변수(.env)에서만 읽고 출력하지 않는다."""
from __future__ import annotations

import os
from typing import Mapping, Optional


def conninfo(env: Optional[Mapping[str, str]] = None, *, role: str = "admin") -> str:
    """libpq 접속 문자열. GOLD_DSN 이 있으면 그대로 쓴다(테스트·CI 용).

    role=admin  → POSTGRES_USER (마이그레이션)
    role=writer → GOLD_WRITER_USER/PASSWORD 가 있으면 그것, 없으면 admin 으로 대체
    """
    e = os.environ if env is None else env
    if e.get("GOLD_DSN"):
        return e["GOLD_DSN"]
    user, pw = e.get("POSTGRES_USER", "urbanstream"), e.get("POSTGRES_PASSWORD", "")
    if role == "writer" and e.get("GOLD_WRITER_USER"):
        user, pw = e["GOLD_WRITER_USER"], e.get("GOLD_WRITER_PASSWORD", "")
    parts = [f"host={e.get('POSTGRES_HOST', '127.0.0.1')}", f"port={e.get('POSTGRES_PORT', '5432')}",
             f"dbname={e.get('POSTGRES_DB', 'urbanstream')}", f"user={user}"]
    if pw:
        parts.append(f"password={pw}")
    return " ".join(parts)
