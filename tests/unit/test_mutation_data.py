"""scripts/mutation_data.py 의 정의가 지금 코드와 맞는지 (STEP 5). 실제 뮤테이션 실행은 CI 의 mutation-data job 과 weekly 가 한다."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import mutation_data as MD  # noqa: E402


@pytest.mark.parametrize("i", range(len(MD.MUTATIONS)))
def test_each_mutation_matches_the_code_exactly_once_and_changes_it(i):
    m = MD.MUTATIONS[i]
    assert MD.check_definition(m) is None, m.desc
    assert m.old != m.new


@pytest.mark.parametrize("i", range(len(MD.MUTATIONS)))
def test_each_mutation_points_at_existing_tests_and_k_selects_something(i):
    m = MD.MUTATIONS[i]
    files = [a for a in m.tests if a.startswith("tests/")]
    assert files, m.desc
    names = []
    for f in files:
        p = ROOT / f
        assert p.exists(), f
        names += re.findall(r"^def (test_\w+)", p.read_text(encoding="utf-8"), re.M)
    if "-k" in m.tests:
        k = m.tests[m.tests.index("-k") + 1]
        assert any(k in n for n in names), f"{m.desc}: -k {k} 에 맞는 테스트가 없다"
    assert m.needs in ("", "pg", "spark")
    if any(f.startswith("tests/spark/") for f in files):
        assert m.needs == "spark", m.desc


def test_descriptions_are_unique_and_every_area_is_covered():
    descs = [m.desc for m in MD.MUTATIONS]
    assert len(descs) == len(set(descs))
    assert {m.area for m in MD.MUTATIONS} >= {"재처리", "중복 제거", "적재", "DQ", "증분", "잠금"}


def test_copy_skips_data_and_git(tmp_path, monkeypatch):
    src = tmp_path / "repo"
    for d in ("data/silver", ".git", "batch", "tests/support"):
        (src / d).mkdir(parents=True)
    (src / "data" / "silver" / "big.parquet").write_text("x")
    (src / "batch" / "a.py").write_text("x")
    monkeypatch.setattr(MD, "ROOT", src)
    MD.copy_repo(tmp_path / "copy")
    assert (tmp_path / "copy" / "batch" / "a.py").exists() and (tmp_path / "copy" / "tests" / "support").is_dir()
    assert not (tmp_path / "copy" / ".git").exists() and list((tmp_path / "copy" / "data").iterdir()) == []


def test_list_prints_without_running(capsys):
    assert MD.main(["--list"]) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == len(MD.MUTATIONS)
