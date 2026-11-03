"""scripts/stack_check.py 의 순수 함수 (docker 없이 검사 가능한 부분)."""
import importlib.util
import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("stack_check", ROOT / "scripts" / "stack_check.py")
stack_check = importlib.util.module_from_spec(spec)
sys.modules["stack_check"] = stack_check  # dataclass 가 모듈을 찾을 수 있게 먼저 등록
spec.loader.exec_module(stack_check)

ROWS = [
    {"Service": "postgres", "State": "running", "Health": "healthy"},
    {"Service": "kafka", "State": "running", "Health": "healthy"},
]


def test_parse_ps_handles_ndjson_and_json_array():
    nd = "\n".join(json.dumps(r) for r in ROWS)
    assert stack_check.parse_ps(nd) == ROWS
    assert stack_check.parse_ps(json.dumps(ROWS)) == ROWS
    assert stack_check.parse_ps("") == []


def test_evaluate_ps_all_healthy():
    checks = stack_check.evaluate_ps(ROWS)
    assert [c.name for c in checks] == ["container:postgres", "container:kafka"] and all(c.ok for c in checks)


def test_evaluate_ps_flags_unhealthy_exited_and_missing():
    rows = [{"Service": "postgres", "State": "running", "Health": "starting"},
            {"Service": "kafka", "State": "exited", "Health": ""}]
    assert [c.ok for c in stack_check.evaluate_ps(rows)] == [False, False]
    missing = stack_check.evaluate_ps([ROWS[0]])
    assert missing[1].name == "container:kafka" and not missing[1].ok and "stack.sh up" in missing[1].detail


def test_kafka_ui_is_checked_only_when_present():
    with_ui = ROWS + [{"Service": "kafka-ui", "State": "running", "Health": ""}]
    assert [c.name for c in stack_check.evaluate_ps(with_ui)][-1] == "container:kafka-ui"
    assert all(c.ok for c in stack_check.evaluate_ps(with_ui))


def test_port_open_false_for_closed_port():
    assert stack_check.port_open("127.0.0.1", 1, timeout=0.2) is False
