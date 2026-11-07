"""구조 가드: 대시보드가 DB 에 직접 붙지 않는다, 시각은 한 곳에서만 바꾼다, 키는 새지 않는다 …
(API 의 tests/api/test_guards.py 와 같은 방식 — 규칙을 어기는 수정이 여기서 걸린다.)"""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PKG = ROOT / "dashboard"
SRC = {p: p.read_text(encoding="utf-8") for p in PKG.rglob("*.py")}
PURE = ("config", "timefmt", "contract", "client", "shape", "freshness")
UI_LIBS = {"streamlit", "pydeck", "altair", "pandas", "numpy"}


def imports(text):
    out = set()
    for n in ast.walk(ast.parse(text)):
        if isinstance(n, ast.Import):
            out |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
            out.add(n.module.split(".")[0])
    return out


def test_pure_modules_do_not_import_ui_libraries():
    for name in PURE:
        bad = imports(SRC[PKG / f"{name}.py"]) & UI_LIBS
        assert not bad, f"{name}.py 가 {bad} 를 import — 순수 모듈이어야 한다(내보내기 스크립트·단위 시험이 가볍게 돌도록)"


def test_dashboard_never_touches_the_database():
    """DB 직접 연결 금지 (사용자 요구 3, ADR-0012 D10-1)."""
    drivers = {"psycopg", "psycopg2", "psycopg_pool", "sqlalchemy", "asyncpg", "pg8000", "pymysql", "sqlite3"}
    for p, t in SRC.items():
        assert not imports(t) & drivers, f"{p.name} 가 DB 드라이버를 import"
        for forbidden in ("POSTGRES_", "API_DB_", "GOLD_LOADER", "GOLD_WRITER", "AIRFLOW_", "ALERT_WEBHOOK", "KAFKA_", "SEOUL_API_KEY", "DATA_GO_KR_KEY"):
            assert forbidden not in t, f"{p.name}: {forbidden} 는 대시보드가 알 필요가 없는 값"
        assert not re.search(r"\bSELECT\b.+\bFROM\b", t), f"{p.name}: SQL 문자열"


def test_http_goes_only_through_client_and_paths_only_live_in_contract():
    for p, t in SRC.items():
        if p.name != "client.py":
            assert "requests" not in imports(t) and "urllib" not in imports(t) and "httpx" not in imports(t), f"{p.name}: 클라이언트를 거치지 않는 HTTP"
        if p.name != "contract.py":
            assert "/v1/" not in t, f"{p.name}: 경로 문자열은 contract.py 에만 둔다"


def test_environment_is_read_only_in_config():
    for p, t in SRC.items():
        if p.name != "config.py":
            assert "os.environ" not in t and "getenv" not in t and "st.secrets" not in t, p.name


def test_timezone_math_lives_only_in_timefmt():
    for p, t in SRC.items():
        if p.name != "timefmt.py":
            assert "astimezone" not in t and "timedelta(hours=9)" not in t and "ZoneInfo" not in t, f"{p.name}: 시각 변환은 timefmt 에서만"
    for p, t in SRC.items():
        if p.parent.name == "views" or p.name in ("shape.py", "ui.py"):
            assert not re.search(r"(fmt_kst|to_kst|parse_utc)\([^)]*(event_date|kst_hour)", t), f"{p.name}: 이미 KST 인 필드를 다시 변환"


def test_views_take_time_from_runtime_and_quantize_windows():
    for p, t in SRC.items():
        if p.parent.name == "views":
            assert "datetime.now" not in t and "utcnow" not in t and "time.time" not in t, f"{p.name}: 현재 시각은 runtime.now()"
            for m in re.finditer(r"\bwindow\(([^\n]*)", t):
                assert m.group(1).startswith("floor_to("), f"{p.name}: window() 는 floor_to(...) 로 내린 시각으로 (캐시 키 안정)"


def test_no_key_echo_no_prints_no_deprecated_widget_args():
    for p, t in SRC.items():
        assert "print(" not in t and "logging" not in t, f"{p.name}: 출력/로그 금지(키 노출 방지)"
        assert "use_container_width" not in t, f"{p.name}: 폐기 예정 인자"
        assert not re.search(r"api_key", t) or p.name in ("config.py", "client.py"), f"{p.name} 가 키를 다룬다"


def test_vehicle_identity_is_not_used_anywhere():
    for p, t in SRC.items():
        assert not re.search(r"\b(plain_no|veh_id)\b", t), f"{p.name}: 차량 식별자는 화면·내보내기에 쓰지 않는다"


def test_every_contract_endpoint_is_used_by_some_view():
    from dashboard import contract as C
    views = "\n".join(t for p, t in SRC.items() if p.parent.name == "views")
    names = {n for n in dir(C) if isinstance(getattr(C, n), C.Endpoint)}
    unused = [n for n in names if f"C.{n}" not in views]
    assert not unused, f"화면이 쓰지 않는 엔드포인트 선언: {unused}"
