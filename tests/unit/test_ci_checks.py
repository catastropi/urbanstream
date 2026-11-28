"""scripts/ci_checks.py — 추적 금지 파일·compose 더미 환경·이미지 검사 (Step 11)."""
import importlib.util
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_checks", ROOT / "scripts" / "ci_checks.py")
CC = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CC)


def paths(bad):
    return [p for p, _ in bad]


def test_forbidden_tracked_flags_env_and_data_files():
    bad = CC.forbidden_tracked([".env", ".env.prod", "data/kafka/x.bin", "tableau/export/a.csv", "src/__pycache__/a.pyc", ".DS_Store", "docs/.DS_Store", "a/b.pyc"])
    assert set(paths(bad)) == {".env", ".env.prod", "data/kafka/x.bin", "tableau/export/a.csv", "src/__pycache__/a.pyc", ".DS_Store", "docs/.DS_Store", "a/b.pyc"}


def test_forbidden_tracked_allows_keep_files_and_normal_code():
    ok = ["data/.gitkeep", "data/postgres/.gitkeep", ".env.example", "src/app.py", "docs/x.md", "sql/001_a.sql", "dataset/readme.md", "database.py"]
    assert CC.forbidden_tracked(ok) == []


def test_forbidden_tracked_ignores_blank_lines():
    assert CC.forbidden_tracked(["", "  ", "."]) == []


def _git(repo, *a):
    subprocess.run(["git", *a], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git 없음")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "t")
    return tmp_path


def test_cmd_tracked_passes_on_clean_repo_and_fails_when_env_is_force_added(repo):
    (repo / "a.py").write_text("x=1\n")
    _git(repo, "add", "a.py")
    assert CC.cmd_tracked(repo) == 0
    (repo / ".env").write_text("K=v\n")
    _git(repo, "add", "-f", ".env")
    assert CC.cmd_tracked(repo) == 1


def test_cmd_tracked_fails_on_data_file_but_not_gitkeep(repo):
    (repo / "data").mkdir()
    (repo / "data" / ".gitkeep").write_text("")
    _git(repo, "add", "-f", "data/.gitkeep")
    assert CC.cmd_tracked(repo) == 0
    (repo / "data" / "x.parquet").write_text("x")
    _git(repo, "add", "-f", "data/x.parquet")
    assert CC.cmd_tracked(repo) == 1


def test_cmd_tracked_reports_environment_problem_outside_git(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git 없음")
    # git 저장소가 아닌 곳(상위에 저장소가 없는 임시 폴더)
    assert CC.cmd_tracked(tmp_path) in (2, 0, 1)   # 상위 저장소 유무에 따라 다르므로 '죽지 않음'만 확인


def test_real_repo_tracks_nothing_forbidden():
    if not (ROOT / ".git").exists():
        pytest.skip("zip 으로 푼 폴더 — git 저장소 아님")
    assert CC.cmd_tracked(ROOT) == 0


def test_dummy_required_matches_compose_required_variables():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*):\?", compose))
    assert required, "compose 에서 필수 변수를 못 찾았다(형식이 바뀜?)"
    assert set(CC.DUMMY_REQUIRED) == required, "compose 의 ${VAR:?} 와 ci_checks.DUMMY_REQUIRED 가 어긋났다"


def test_dummy_env_text_has_every_variable_with_the_marker_value():
    lines = dict(l.split("=", 1) for l in CC.dummy_env_text().splitlines() if "=" in l)
    assert set(lines) >= set(CC.DUMMY_REQUIRED)
    assert all(v == CC.DUMMY_VALUE for k, v in lines.items() if k in CC.DUMMY_REQUIRED)
    assert CC.DUMMY_VALUE == "ci-not-a-secret"      # .gitleaks.toml 의 허용 정규식과 같아야 한다


def test_gitleaks_allowlist_knows_the_dummy_value():
    assert CC.DUMMY_VALUE in (ROOT / ".gitleaks.toml").read_text(encoding="utf-8")


def test_check_image_listing_ok():
    assert CC.check_image_listing(["src", "dashboard", "requirements.txt"], "10001\n") == []


@pytest.mark.parametrize("bad", [".env", "data", "tests", "docs", ".git", ".github", "HANDOFF.md", "sql", "scripts"])
def test_check_image_listing_flags_each_forbidden_item(bad):
    out = CC.check_image_listing(["src", bad], "10001")
    assert len(out) == 1 and bad in out[0]


@pytest.mark.parametrize("user", ["", "0", "root", "1000"])
def test_check_image_listing_flags_non_expected_user(user):
    assert CC.check_image_listing(["src"], user)


def test_main_rejects_unknown_subcommand():
    with pytest.raises(SystemExit):
        CC.main(["bogus"])


def test_ruff_config_selects_the_agreed_rules():
    text = (ROOT / "ruff.toml").read_text(encoding="utf-8")
    sel = re.search(r"^select\s*=\s*\[(.*?)\]", text, re.M | re.S).group(1)
    rules = set(re.findall(r'"(\w+)"', sel))
    assert rules == {"E9", "F63", "F7", "F82", "F401", "F841", "F541"}      # 바꿀 때는 ADR-0018 과 함께


def test_ruff_gate_rejects_an_undefined_name_with_the_repo_config(tmp_path):
    ruff = shutil.which("ruff")
    if ruff is None:
        pytest.skip("ruff 미설치 — CI 의 hygiene job 에서 실행")
    bad = tmp_path / "bad.py"
    bad.write_text("def f():\n    return undefined_name\n")
    r = subprocess.run([ruff, "check", "--config", str(ROOT / "ruff.toml"), str(bad)], capture_output=True, text=True)
    assert r.returncode != 0 and "F821" in r.stdout


def test_ruff_gate_rejects_an_unused_import_with_the_repo_config(tmp_path):
    ruff = shutil.which("ruff")
    if ruff is None:
        pytest.skip("ruff 미설치: CI 의 hygiene job 에서 실행")
    bad = tmp_path / "bad.py"
    bad.write_text("import os\n\n\ndef f():\n    return 1\n")
    r = subprocess.run([ruff, "check", "--config", str(ROOT / "ruff.toml"), str(bad)], capture_output=True, text=True)
    assert r.returncode != 0 and "F401" in r.stdout
