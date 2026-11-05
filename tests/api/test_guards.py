"""구조 가드(DB 없음): SQL 은 고정 문자열, ops 는 뷰만, 쓰기 구문 없음, 모든 뷰가 어딘가에서 서비스됨, 모든 /v1 경로가 인증 아래에 있음."""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.config import load_settings
from api.main import create_app

ROOT = Path(__file__).resolve().parents[2]
API_FILES = sorted((ROOT / "api").rglob("*.py"))
WRITE_WORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|REFRESH|CALL)\b")

# 뷰는 있으나 API 로 노출하지 않는 것 (이유를 적는다). 지금은 없음.
EXCLUDED_VIEWS: dict[str, str] = {}


def _module_consts(tree: ast.Module) -> dict[str, str]:
    out = {}
    for n in tree.body:
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) and isinstance(n.value, ast.Constant) \
                and isinstance(n.value.value, str):
            out[n.targets[0].id] = n.value.value
    return out


def _sql_args(tree: ast.Module):
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("rows", "row") and n.args:
            yield n, n.args[0]


def _resolve(arg: ast.AST, consts: dict[str, str]) -> str:
    """고정 문자열로 풀리지 않으면 AssertionError."""
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.Name) and arg.id in consts and arg.id.lstrip("_").isupper():
        return consts[arg.id]
    if isinstance(arg, ast.IfExp):                 # 두 갈래 모두 고정 문자열이어야 한다. 둘 다 검사 대상으로 합친다
        return _resolve(arg.body, consts) + "\n" + _resolve(arg.orelse, consts)
    if isinstance(arg, ast.JoinedStr):
        parts = []
        for v in arg.values:
            if isinstance(v, ast.Constant):
                parts.append(v.value)
            elif isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name) and v.value.id in consts and v.value.id.lstrip("_").isupper() \
                    and v.conversion == -1 and v.format_spec is None:
                parts.append(consts[v.value.id])
            else:
                raise AssertionError(f"SQL f-string 에 상수가 아닌 값 보간: line {arg.lineno}")
        return "".join(parts)
    raise AssertionError(f"SQL 이 고정 문자열이 아님: line {getattr(arg, 'lineno', '?')}")


def _all_sql() -> list[tuple[str, int, str]]:
    found = []
    for f in API_FILES:
        if f.name == "db.py":                      # 래퍼 자신: 매개변수로 받은 sql 을 그대로 넘긴다 (아래 테스트가 db.py 밖의 직접 접근을 막는다)
            continue
        tree = ast.parse(f.read_text(encoding="utf-8"))
        consts = _module_consts(tree)
        for call, arg in _sql_args(tree):
            found.append((f.name, call.lineno, _resolve(arg, consts)))
    return found


def test_scanner_finds_the_sql():
    sqls = _all_sql()
    assert len(sqls) >= 25, "라우터 SQL 을 못 찾았다 — 검사기가 고장 났을 수 있다"


def test_no_string_building_of_sql():
    _all_sql()          # 보간·연결이 있으면 AssertionError


def test_scanner_rejects_interpolation():
    bad = ast.parse('db.rows(f"SELECT * FROM t WHERE a = {x}")').body[0].value
    with pytest.raises(AssertionError):
        _resolve(bad.args[0], {})
    bad2 = ast.parse('db.rows("SELECT " + col)').body[0].value
    with pytest.raises(AssertionError):
        _resolve(bad2.args[0], {})
    bad3 = ast.parse('db.rows("SELECT %s" % col)').body[0].value
    with pytest.raises(AssertionError):
        _resolve(bad3.args[0], {})


def test_ops_only_through_views():
    for fname, line, sql in _all_sql():
        for m in re.finditer(r"\bops\.(\w+)", sql):
            assert m.group(1).startswith("v_"), f"{fname}:{line} ops 원본 표 접근: ops.{m.group(1)}"


def test_only_select_statements():
    for fname, line, sql in _all_sql():
        s = sql.strip().upper()
        assert s.startswith(("SELECT", "WITH")), f"{fname}:{line}"
        assert not WRITE_WORDS.search(re.sub(r"'[^']*'", "''", sql)), f"{fname}:{line} 쓰기/DDL 키워드"


def test_every_view_in_migrations_is_served_or_excluded():
    created = set()
    for f in (ROOT / "sql").glob("*.sql"):
        created |= set(re.findall(r"CREATE (?:OR REPLACE )?VIEW ((?:mart|ops)\.v_\w+)", f.read_text(encoding="utf-8")))
    used = set()
    for _, _, sql in _all_sql():
        used |= set(re.findall(r"\b((?:mart|ops)\.v_\w+)", sql))
    missing = created - used - set(EXCLUDED_VIEWS)
    assert not missing, f"API 가 서비스하지 않는 뷰(새 뷰를 만들면 엔드포인트 또는 EXCLUDED_VIEWS 사유를 추가): {sorted(missing)}"
    assert not (used - created), f"마이그레이션에 없는 뷰를 읽음: {sorted(used - created)}"


def test_all_v1_routes_sit_under_guard():
    app = create_app(load_settings({"API_KEYS": "k" * 30, "API_DB_PASSWORD": "p" * 16}))
    paths = [p for p in app.openapi()["paths"] if p.startswith("/v1")]
    assert len(paths) >= 20
    with TestClient(app, raise_server_exceptions=False) as c:           # DB 는 필요 없다: 인증이 먼저 막는다
        for p in paths:
            r = c.get(p.replace("{link_id}", "x"))
            assert r.status_code == 401 and r.json()["code"] == "missing_api_key", p


def test_no_secrets_in_source():
    for f in API_FILES:
        txt = f.read_text(encoding="utf-8")
        assert not re.search(r"password\s*=\s*[\"'][^\"']+[\"']", txt, re.I), f.name


def test_only_db_module_touches_the_driver():
    for f in API_FILES:
        if f.name in ("db.py", "__init__.py"):
            continue
        txt = f.read_text(encoding="utf-8")
        assert not re.search(r"\.execute\(|import psycopg|from psycopg", txt.replace("psycopg_pool", "")) or f.name == "config.py", f.name
