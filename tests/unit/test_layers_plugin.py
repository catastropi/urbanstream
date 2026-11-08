"""tests/support/layers_plugin.py + pytest.ini — 계층 마커가 시험 결과를 바꾸지 않는지 지킨다 (Step 11)."""
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("layers_plugin_t", ROOT / "tests" / "support" / "layers_plugin.py")
LP = importlib.util.module_from_spec(spec)
spec.loader.exec_module(LP)


def test_layers_for_directory_names():
    assert LP.layers_for("tests/api/test_x.py") == ("api",)
    assert LP.layers_for("tests/dashboard/test_x.py") == ("dashboard",)
    assert LP.layers_for("tests/recovery/test_x.py") == ("recovery",)
    assert LP.layers_for("tests/e2e/test_x.py") == ("e2e",)


def test_layers_for_data_files_get_extra_marker():
    assert LP.layers_for("tests/unit/test_dq_rules.py") == ("unit", "data")
    assert LP.layers_for("tests/integration/test_dq_pg.py") == ("integration", "data")


def test_layers_for_unknown_locations_are_unmarked():
    assert LP.layers_for("tests/support/pg_support.py") == ()
    assert LP.layers_for("src/app.py") == ()
    assert LP.layers_for("tests/x.py") == ()
    assert LP.layers_for("tests\\api\\test_x.py") == ("api",)


def test_every_test_file_lives_in_a_known_layer_dir():
    """새 폴더를 만들고 마커 등록을 잊으면 CI 의 -m 선택에서 조용히 빠진다."""
    unknown = []
    for p in (ROOT / "tests").rglob("test_*.py"):
        rel = p.relative_to(ROOT).as_posix()
        if not LP.layers_for(rel):
            unknown.append(rel)
    assert unknown == []


def test_data_files_all_exist():
    missing = [f for f in LP.DATA_FILES if not (ROOT / f).exists()]
    assert missing == []


def test_pytest_ini_registers_plugin_and_every_marker():
    ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
    assert "-p layers_plugin" in ini
    for m in (*LP.LAYERS, "data", "pg"):
        assert re.search(rf"^\s+{m}\s*:", ini, re.M), f"마커 {m} 가 pytest.ini 에 없다"
    assert "pythonpath" in ini and "tests/support" in ini


def test_mutation_runners_copy_tests_support():
    """pytest.ini 가 -p layers_plugin 을 요구하므로, 임시 폴더로 복사하는 변이 러너가 tests/support 를 빠뜨리면 전부 오류가 난다."""
    for name in ("mutation_api.py", "mutation_dashboard.py"):
        text = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert "tests/support" in text and "pytest.ini" in text, name
