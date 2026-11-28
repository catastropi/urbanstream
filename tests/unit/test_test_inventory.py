"""scripts/test_inventory.py: 테스트 수를 코드에서 세고, 문서의 자동 생성 칸과 대조하고, junit 결과를 계층별로 요약한다 (STEP 5)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import test_inventory as TI  # noqa: E402


def test_count_tests_sees_module_functions_and_test_classes_only():
    src = (
        "def test_a(): pass\n"
        "async def test_b(): pass\n"
        "def helper(): pass\n"
        "class TestX:\n    def test_c(self): pass\n    def other(self): pass\n"
        "class Helper:\n    def test_not_collected(self): pass\n"
    )
    assert TI.count_tests(src) == 3


@pytest.mark.parametrize("rel, src, want", [
    ("tests/unit/test_x.py", "import json", "없음"),
    ("tests/integration/test_x.py", "from pg_support import pg", "PostgreSQL"),
    ("tests/spark/test_x.py", "", "Spark"),
    ("tests/spark/test_x_pg.py", "os.environ['GOLD_TEST_DSN']", "Spark+PostgreSQL"),
    ("tests/recovery/test_x.py", "URBANSTREAM_TEST_KAFKA_HOME", "Kafka"),
    ("tests/spark/test_y.py", "from kafka_broker import broker", "Spark+Kafka"),
])
def test_needs_are_derived_from_the_file(rel, src, want):
    assert TI.needs_of(rel, src) == want


def test_scan_of_the_repo_adds_up():
    s = TI.scan()
    assert s["total_tests"] == sum(f["tests"] for f in s["files"]) == sum(s["by_layer"].values()) == sum(s["by_needs"].values())
    assert s["total_files"] == len(s["files"]) and "other" not in s["by_layer"], "tests/ 아래에 계층이 없는 테스트 파일이 있다"
    assert set(s["mutations"]) == {"api", "dashboard", "ci", "data"} and all(v > 0 for v in s["mutations"].values())


def test_repo_documents_match_the_code():
    """README·docs/testing.md 의 숫자가 코드와 다르면 실패. 고치는 법: python scripts/test_inventory.py readme --write"""
    assert TI.readme(check=True) == 0


def test_replace_block_needs_exactly_one_marker_pair():
    text = "a <!-- tests:readme:start -->old<!-- tests:readme:end --> b"
    assert TI.replace_block(text, "readme", "new") == "a <!-- tests:readme:start -->new<!-- tests:readme:end --> b"
    with pytest.raises(ValueError):
        TI.replace_block("no markers", "readme", "x")
    with pytest.raises(ValueError):
        TI.replace_block(text + text, "readme", "x")


def test_readme_check_fails_on_a_stale_number_and_write_fixes_it(tmp_path):
    (tmp_path / "tests" / "unit").mkdir(parents=True)
    (tmp_path / "tests" / "unit" / "test_a.py").write_text("def test_one(): pass\ndef test_two(): pass\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "README.md").write_text("| x | <!-- tests:readme:start -->테스트 함수 1개<!-- tests:readme:end --> |\n")
    (tmp_path / "docs" / "testing.md").write_text("<!-- tests:testing:start -->\n<!-- tests:testing:end -->\n")
    assert TI.readme(check=True, root=tmp_path) == 1
    assert TI.readme(check=False, root=tmp_path) == 0
    assert "테스트 함수 2개(파일 1개" in (tmp_path / "README.md").read_text()
    assert TI.readme(check=True, root=tmp_path) == 0


JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest">
 <testcase classname="tests.unit.test_a" name="test_1" file="tests/unit/test_a.py"/>
 <testcase classname="tests.unit.test_a.TestX" name="test_2"/>
 <testcase classname="tests.integration.test_b" name="test_3"><skipped message="no pg"/></testcase>
 <testcase classname="tests.integration.test_b" name="test_4"><failure message="boom"/></testcase>
 <testcase classname="tests.api.test_c" name="test_5"><error message="setup"/></testcase>
 <testcase classname="" name="tests.unit.test_skipped_module"><skipped message="no airflow"/></testcase>
</testsuite></testsuites>
"""


def test_summarize_counts_outcomes_per_layer(tmp_path):
    p = tmp_path / "r.xml"
    p.write_text(JUNIT, encoding="utf-8")
    res = TI.summarize([p])
    assert res["by_layer"]["unit"] == {"passed": 2, "skipped": 1}
    assert res["by_layer"]["integration"] == {"skipped": 1, "failed": 1}
    assert res["by_layer"]["api"] == {"error": 1}
    assert res["total"] == {"passed": 2, "skipped": 2, "failed": 1, "error": 1}
    md = TI.summary_markdown(res, "t")
    assert "| 합계 | 2 | 1 | 1 | 2 |" in md


def test_summarize_cli_writes_the_step_summary_and_fails_on_failures(tmp_path, monkeypatch, capsys):
    p = tmp_path / "r.xml"
    p.write_text(JUNIT, encoding="utf-8")
    out = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(out))
    assert TI.main(["summarize", str(p), "--title", "제목"]) == 1
    assert "### 제목" in out.read_text(encoding="utf-8")
    assert TI.main(["summarize", str(tmp_path / "none.xml")]) == 1
