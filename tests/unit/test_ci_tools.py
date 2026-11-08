"""scripts/ci_changes.py · ci_gitleaks.py · pin_gitleaks.py · pin_actions.py (Step 11)."""
import hashlib
import importlib.util
import io
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


CH, GL, PG, PA = load("ci_changes"), load("ci_gitleaks"), load("pin_gitleaks"), load("pin_actions")


# --- ci_changes ----------------------------------------------------------------------------------------------
def all_true(d):
    return d and all(d.values())


def test_classify_unknown_means_run_everything():
    assert all_true(CH.classify(None))


@pytest.mark.parametrize("f", [".github/workflows/ci.yml", "scripts/ci_checks.py", "scripts/pin_actions.py", "ruff.toml",
                               ".gitleaks.toml", "pytest.ini", "tests/support/pg_support.py", "scripts/mutation_ci.py"])
def test_classify_infrastructure_changes_run_everything(f):
    assert all_true(CH.classify(["README.md", f]))


def test_classify_docs_only_runs_nothing_heavy():
    res = CH.classify(["docs/adr/0001.md", "HANDOFF.md"])
    assert set(res) == set(CH.GROUPS) and not any(res.values())


def test_classify_api_change_selects_api_and_docker_not_dags():
    res = CH.classify(["api/main.py"])
    assert res == {"api": True, "dags": False, "docker": True}


def test_classify_dag_change_selects_only_dags():
    assert CH.classify(["dags/x.py"]) == {"api": False, "dags": True, "docker": False}


def test_classify_each_group_triggers_on_its_own_prefix():
    for group, prefixes in CH.GROUPS.items():
        res = CH.classify([prefixes[0] if prefixes[0].endswith(("/", ".py", ".yml", ".txt")) else prefixes[0] + "x"])
        assert res[group], group


def test_changed_files_none_for_empty_or_zero_base():
    assert CH.changed_files("", "HEAD") is None
    assert CH.changed_files("0" * 40, "HEAD") is None
    assert CH.changed_files("abc", "") is None


def test_changed_files_with_real_git(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git 없음")
    def g(*a):
        return subprocess.run(["git", *a], cwd=tmp_path, check=True, capture_output=True, text=True).stdout.strip()
    g("init", "-q"); g("config", "user.email", "t@e.com"); g("config", "user.name", "t")
    (tmp_path / "a").write_text("1"); g("add", "a"); g("commit", "-qm", "1")
    base = g("rev-parse", "HEAD")
    (tmp_path / "b").write_text("2"); g("add", "b"); g("commit", "-qm", "2")
    assert CH.changed_files(base, "HEAD", tmp_path) == ["b"]
    assert CH.changed_files("deadbeef" * 5, "HEAD", tmp_path) is None     # 모르는 커밋 → '모름'


def test_ci_changes_main_writes_github_output(tmp_path, monkeypatch):
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    assert CH.main(["--base", ""]) == 0
    text = out.read_text()
    assert all(f"{g}=true" in text for g in CH.GROUPS)


# --- ci_gitleaks -----------------------------------------------------------------------------------------------
def tarball(with_binary=True):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        name = "gitleaks" if with_binary else "README"
        data = b"#!/bin/sh\necho 'Commands:\n  git   scan'\nexit 0\n"
        ti = tarfile.TarInfo(name); ti.size = len(data); ti.mode = 0o755
        tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def write_pin(path, v="8.30.1", h="a" * 64):
    path.write_text(f"version={v}\nsha256={h}\n")


def test_read_pin_missing_file_is_fail_closed(tmp_path):
    with pytest.raises(GL.PinError):
        GL.read_pin(tmp_path / "none.pin")


@pytest.mark.parametrize("body", ["", "version=UNPINNED\nsha256=UNPINNED\n", "version=8.30.1\nsha256=zz\n", "version=8.30\nsha256=" + "a" * 64 + "\n"])
def test_read_pin_rejects_unpinned_or_malformed(tmp_path, body):
    p = tmp_path / "g.pin"; p.write_text(body)
    with pytest.raises(GL.PinError):
        GL.read_pin(p)


def test_read_pin_ok_and_comments_ignored(tmp_path):
    p = tmp_path / "g.pin"; p.write_text("# c\nversion=8.30.1\nsha256=" + "b" * 64 + "\n")
    assert GL.read_pin(p) == ("8.30.1", "b" * 64)


def test_verify_sha256_accepts_match_rejects_mismatch():
    d = b"abc"
    GL.verify_sha256(d, hashlib.sha256(d).hexdigest())
    with pytest.raises(GL.PinError):
        GL.verify_sha256(d, "0" * 64)


def test_extract_binary_and_missing_member(tmp_path):
    b = GL.extract_binary(tarball(), tmp_path)
    assert b.name == "gitleaks" and b.stat().st_mode & 0o111
    with pytest.raises(GL.PinError):
        GL.extract_binary(tarball(with_binary=False), tmp_path)


NEW_HELP = "Usage:\n\nAvailable Commands:\n  dir         scan\n  git         scan git repos\n  help        Help\n"
OLD_HELP = "Usage:\n\nAvailable Commands:\n  detect      scan\n  protect     x\n"


def test_scan_command_new_and_old_style():
    new = GL.scan_command("/b/gl", "full", None, NEW_HELP)
    assert new[:2] == ["/b/gl", "git"] and "--redact" in new and new[-1] == "."
    old = GL.scan_command("/b/gl", "full", None, OLD_HELP)
    assert old[:2] == ["/b/gl", "detect"] and "--source" in old


def test_scan_command_diff_needs_range_and_passes_log_opts():
    with pytest.raises(GL.PinError):
        GL.scan_command("g", "diff", None, NEW_HELP)
    c = GL.scan_command("g", "diff", "a..b", NEW_HELP)
    assert c[c.index("--log-opts") + 1] == "a..b"


def test_scan_command_always_redacts_and_uses_repo_config():
    for h in (NEW_HELP, OLD_HELP):
        c = GL.scan_command("g", "full", None, h)
        assert "--redact" in c and c[c.index("--config") + 1] == ".gitleaks.toml"


def test_cmd_scan_without_pin_fails_without_downloading(tmp_path, monkeypatch):
    missing = tmp_path / "none.pin"
    real = GL.read_pin
    monkeypatch.setattr(GL, "read_pin", lambda: real(missing))
    called = []
    assert GL.cmd_scan("full", None, dl=lambda u: called.append(u) or b"") == 1
    assert called == []


def test_cmd_scan_rejects_tampered_download(tmp_path, monkeypatch):
    monkeypatch.setattr(GL, "read_pin", lambda: ("8.30.1", "c" * 64))
    ran = []
    monkeypatch.setattr(GL.subprocess, "run", lambda *a, **k: ran.append(a) or None)
    assert GL.cmd_scan("full", None, dl=lambda u: tarball()) == 1
    assert ran == [], "sha 불일치인데 실행했다"


def test_cmd_scan_runs_verified_binary_and_maps_exit_code(monkeypatch):
    data = tarball()
    monkeypatch.setattr(GL, "read_pin", lambda: ("8.30.1", hashlib.sha256(data).hexdigest()))
    urls = []
    class R:
        def __init__(self, rc, out=""): self.returncode, self.stdout = rc, out
    calls = []
    def fake_run(cmd, **k):
        calls.append(cmd)
        return R(0, NEW_HELP) if "--help" in cmd else R(1)
    monkeypatch.setattr(GL.subprocess, "run", fake_run)
    assert GL.cmd_scan("diff", "abc..def", dl=lambda u: urls.append(u) or data) == 1     # 탐지 → 실패
    assert urls == [GL.URL.format(v="8.30.1")] and "--log-opts" in calls[-1]


def test_cmd_scan_first_push_zero_base_scans_everything(monkeypatch):
    data = tarball()
    monkeypatch.setattr(GL, "read_pin", lambda: ("8.30.1", hashlib.sha256(data).hexdigest()))
    class R:
        def __init__(self, rc, out=""): self.returncode, self.stdout = rc, out
    calls = []
    monkeypatch.setattr(GL.subprocess, "run", lambda cmd, **k: calls.append(cmd) or (R(0, NEW_HELP) if "--help" in cmd else R(0)))
    assert GL.cmd_scan("diff", "0" * 40 + "..abc", dl=lambda u: data) == 0
    assert "--log-opts" not in calls[-1]


def test_cmd_scan_network_error_is_environment_problem(monkeypatch):
    monkeypatch.setattr(GL, "read_pin", lambda: ("8.30.1", "d" * 64))
    def boom(u): raise OSError("offline")
    assert GL.cmd_scan("full", None, dl=boom) == 2


def test_committed_pin_file_is_empty_or_valid():
    """사용자가 pin_gitleaks.py 를 돌리기 전에는 없거나 UNPINNED 여야 하고(=CI 가 닫힌 채 실패), 돌린 뒤에는 형식이 맞아야 한다."""
    p = ROOT / ".github" / "gitleaks.pin"
    if p.exists() and "UNPINNED" not in p.read_text():
        GL.read_pin(p)


# --- pin_gitleaks ------------------------------------------------------------------------------------------------
def test_parse_checksums_picks_linux_x64():
    sha = "e" * 64
    text = f"{'1'*64}  gitleaks_8.30.1_darwin_arm64.tar.gz\n{sha}  gitleaks_8.30.1_linux_x64.tar.gz\n"
    assert PG.parse_checksums(text, "8.30.1") == sha


def test_parse_checksums_missing_or_malformed_raises():
    with pytest.raises(ValueError):
        PG.parse_checksums("abc  gitleaks_8.30.1_linux_x64.tar.gz\n", "8.30.1")
    with pytest.raises(ValueError):
        PG.parse_checksums("", "8.30.1")


def test_pin_text_roundtrips_through_read_pin(tmp_path):
    p = tmp_path / "g.pin"; p.write_text(PG.pin_text("8.30.1", "f" * 64))
    assert GL.read_pin(p) == ("8.30.1", "f" * 64)


# --- pin_actions ---------------------------------------------------------------------------------------------------
SHA = "a" * 40
YML = "steps:\n  - uses: actions/checkout@v6\n  - uses: actions/setup-python@v6\n    with:\n      python-version: '3.12'\n  - run: echo uses: x/y@v1\n"


def test_unpinned_lists_tag_refs_only():
    assert PA.unpinned(YML) == ["actions/checkout@v6", "actions/setup-python@v6"]
    assert PA.unpinned(f"  - uses: actions/checkout@{SHA} # v6\n") == []


def test_pin_text_rewrites_tags_with_sha_and_comment():
    new, pinned, failed = PA.pin_text(YML, lambda r, t: SHA)
    assert f"uses: actions/checkout@{SHA} # v6" in new and failed == [] and len(pinned) == 2
    assert PA.unpinned(new) == []
    assert "run: echo uses: x/y@v1" in new       # run 스크립트 안의 문자열은 건드리지 않는다


def test_pin_text_keeps_line_when_resolution_fails():
    new, pinned, failed = PA.pin_text(YML, lambda r, t: None)
    assert new == YML and pinned == [] and len(failed) == 2


def test_pin_text_is_idempotent():
    once, _, _ = PA.pin_text(YML, lambda r, t: SHA)
    twice, pinned, failed = PA.pin_text(once, lambda r, t: "b" * 40)
    assert twice == once and pinned == [] and failed == []


def test_pin_text_rejects_bad_resolver_output():
    new, pinned, failed = PA.pin_text(YML, lambda r, t: "not-a-sha")
    assert new == YML and len(failed) == 2
