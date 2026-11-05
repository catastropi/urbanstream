"""읽기 전용 DB 접근. 모든 SQL 은 고정 문자열 + 매개변수다(문자열 보간 금지 — tests/api/test_guards.py 가 검사한다)."""
from __future__ import annotations

import logging
from typing import Any, Optional

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout

from .config import DB_OPTIONS, Settings
from .errors import ApiError

log = logging.getLogger("urbanstream.api")


class Database:
    def __init__(self, settings: Settings):
        if settings.db_dsn:
            conninfo = settings.db_dsn
        else:
            conninfo = make_conninfo(host=settings.db_host, port=settings.db_port, dbname=settings.db_name,
                                     user=settings.db_user, password=settings.db_password)
        options = (DB_OPTIONS + " " + settings.db_extra_options).strip()
        self.pool = ConnectionPool(
            conninfo, min_size=1, max_size=settings.pool_max, timeout=3.0, open=False, name="urbanstream-api",
            check=ConnectionPool.check_connection,
            kwargs={"row_factory": dict_row, "autocommit": True, "options": options, "connect_timeout": 3,
                    "application_name": "urbanstream-api"})

    def open(self) -> None:
        self.pool.open(wait=False)      # DB 가 아직 없어도 API 는 뜬다 (/ready 와 요청이 503 으로 알린다)

    def close(self) -> None:
        self.pool.close()

    def rows(self, sql: str, params: Optional[dict[str, Any]] = None) -> list[dict]:
        try:
            with self.pool.connection() as conn:
                return conn.execute(sql, params or {}).fetchall()
        except (psycopg.errors.DataError, psycopg.errors.InvalidTextRepresentation) as e:     # 예: 문자열에 NUL(0x00)
            raise ApiError(422, "invalid_parameter", "매개변수에 사용할 수 없는 값이 들어 있습니다") from e
        except psycopg.errors.QueryCanceled as e:
            log.warning('{"event":"db_timeout"}')
            raise ApiError(503, "query_timeout", "조회가 제한 시간(5초)을 넘어 중단되었습니다", headers={"Retry-After": "5"}) from e
        except psycopg.errors.ObjectNotInPrerequisiteState as e:
            raise ApiError(503, "data_not_ready", "아직 채워지지 않은 마트를 읽으려 했습니다", headers={"Retry-After": "60"}) from e
        except (PoolTimeout, psycopg.OperationalError, psycopg.InterfaceError) as e:
            log.warning('{"event":"db_unavailable","error":"%s"}', type(e).__name__)
            raise ApiError(503, "database_unavailable", "데이터베이스에 연결할 수 없습니다", headers={"Retry-After": "5"}) from e

    def row(self, sql: str, params: Optional[dict[str, Any]] = None) -> Optional[dict]:
        r = self.rows(sql, params)
        return r[0] if r else None

    def ping(self) -> bool:
        try:
            self.rows("SELECT 1 AS ok")
            return True
        except ApiError:
            return False
