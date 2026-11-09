"""GitHub Actions 워크플로 정적 점검 (Step 11). 실제 실행은 GitHub 에서만 가능하므로, 여기서는 '위험한 모양이 없는지'를 지킨다."""
import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / ".github" / "workflows"
FILES = sorted(WF.glob("*.yml"))
SHA_REF = re.compile(r"^[0-9a-f]{40}$")
TAG_REF = re.compile(r"^v\d+(\.\d+){0,2}$")


def load(name):
    return yaml.safe_load((WF / name).read_text(encoding="utf-8"))


def triggers(doc):
    return doc.get("on", doc.get(True))      # YAML 1.1 은 `on` 을 True 로 읽는다


def all_steps(doc):
    for jn, job in doc["jobs"].items():
        for st in job.get("steps", []):
            yield jn, st


def test_expected_workflows_exist():
    assert {f.name for f in FILES} == {"ci.yml", "weekly.yml", "release.yml"}


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_top_level_permissions_are_read_only(f):
    doc = yaml.safe_load(f.read_text(encoding="utf-8"))
    assert doc["permissions"] == {"contents": "read"}


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_every_job_has_timeout(f):
    doc = yaml.safe_load(f.read_text(encoding="utf-8"))
    for name, job in doc["jobs"].items():
        assert isinstance(job.get("timeout-minutes"), int), f"{f.name}:{name} 에 timeout-minutes 가 없다"
        assert job["timeout-minutes"] <= 60


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_actions_are_pinned_not_floating(f):
    doc = yaml.safe_load(f.read_text(encoding="utf-8"))
    for jn, st in all_steps(doc):
        u = st.get("uses")
        if not u:
            continue
        assert not u.startswith("docker://"), "docker:// 액션 금지"
        assert "@" in u, f"{u}: 버전 없음"
        ref = u.split("@", 1)[1]
        assert ref not in ("main", "master", "latest"), f"{u}: 움직이는 참조"
        assert SHA_REF.match(ref) or TAG_REF.match(ref), f"{u}: 40자 SHA 또는 vN 이어야 한다"
        assert u.split("/")[0] in ("actions", "github"), f"{u}: 1st-party 액션만 쓴다(제3자는 DEFERRED 검토 후)"


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_only_github_token_secret_is_used(f):
    text = f.read_text(encoding="utf-8")
    names = set(re.findall(r"secrets\.([A-Za-z0-9_]+)", text))
    assert names <= {"GITHUB_TOKEN"}, names


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_no_dangerous_triggers(f):
    text = f.read_text(encoding="utf-8")
    assert "pull_request_target" not in text
    assert "workflow_run" not in text


@pytest.mark.parametrize("f", FILES, ids=lambda p: p.name)
def test_no_untrusted_expression_inside_run_scripts(f):
    """PR 제목·브랜치명 같은 외부 입력을 run 스크립트에 직접 끼우면 주입이 된다. env 로 받아야 한다."""
    doc = yaml.safe_load(f.read_text(encoding="utf-8"))
    risky = re.compile(r"\$\{\{\s*github\.(head_ref|event\.(pull_request\.(title|body|head\.ref)|issue\.title|comment\.body))")
    for jn, st in all_steps(doc):
        assert not risky.search(st.get("run", "")), f"{f.name}:{jn}"


def test_ci_has_expected_jobs_and_aggregator_needs_all():
    doc = load("ci.yml")
    jobs = doc["jobs"]
    assert set(jobs) == {"changes", "hygiene", "secrets", "test-core", "test-minimal", "test-pg", "mutation-api", "dags", "docker-build", "ci-ok"}
    agg = jobs["ci-ok"]
    assert agg["if"].strip() == "always()"
    assert set(agg["needs"]) == set(jobs) - {"ci-ok"}
    script = "\n".join(s.get("run", "") for s in agg["steps"])
    assert "failure" in script and "cancelled" in script


def test_ci_triggers_and_concurrency():
    doc = load("ci.yml")
    t = triggers(doc)
    assert set(t) == {"pull_request", "push", "workflow_dispatch"}
    assert t["push"]["branches"] == ["main"]
    assert doc["concurrency"]["cancel-in-progress"] is True


def test_ci_pg_job_uses_compose_image_and_role_tests_flag():
    job = load("ci.yml")["jobs"]["test-pg"]
    assert job["services"]["postgres"]["image"] == "imresamu/postgis:17-3.6.1-bookworm"
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "imresamu/postgis:17-3.6.1-bookworm" in compose, "CI 의 PG 이미지가 compose 와 달라졌다"
    assert job["env"]["ALLOW_ROLE_TESTS"] == "1"
    assert "GOLD_TEST_DSN" in job["env"]


def test_ci_secrets_job_uses_pinned_gitleaks_script_only():
    job = load("ci.yml")["jobs"]["secrets"]
    runs = "\n".join(s.get("run", "") for s in job["steps"])
    assert "scripts/ci_gitleaks.py" in runs
    fetch = [s for s in job["steps"] if s.get("with", {}).get("fetch-depth") is not None]
    assert fetch, "diff 범위 검사에는 이력(fetch-depth)이 필요하다"


def test_release_triggers_only_on_version_tags_and_write_scope_is_the_image_job():
    doc = load("release.yml")
    t = triggers(doc)
    assert set(t) == {"push"} and set(t["push"]) == {"tags"} and t["push"]["tags"] == ["v*"]
    assert list(doc["jobs"]) == ["images"]
    perms = doc["jobs"]["images"]["permissions"]
    assert perms == {"contents": "read", "packages": "write"}
    assert doc["jobs"]["images"]["strategy"]["matrix"]["image"] == ["api", "dashboard"]


def test_release_checks_tag_format_and_guards_image_before_push():
    steps = load("release.yml")["jobs"]["images"]["steps"]
    runs = [s.get("run", "") for s in steps]
    idx = {k: next(i for i, r in enumerate(runs) if k in r) for k in ("^v[0-9]+", "image-guard", "docker push")}
    assert idx["^v[0-9]+"] < idx["image-guard"] < idx["docker push"]
    joined = "\n".join(runs)
    assert "--password-stdin" in joined
    assert "org.opencontainers.image.source" in joined
    assert "latest" not in joined, "latest 태그는 쓰지 않는다"


def test_weekly_schedule_and_full_history_scan():
    doc = load("weekly.yml")
    t = triggers(doc)
    assert "schedule" in t and "workflow_dispatch" in t
    assert set(doc["jobs"]) == {"secrets-full", "python-matrix", "spark", "mutation-api-full", "dags"}
    text = (WF / "weekly.yml").read_text(encoding="utf-8")
    assert "--mode full" in text and "fetch-depth: 0" in text


def test_workflow_script_references_exist():
    """워크플로가 부르는 scripts/…·tests/… 경로가 실제로 있어야 한다."""
    for f in FILES:
        for m in re.finditer(r"\b((?:scripts|tests|docker)/[\w./-]+\.(?:py|sh|toml)|docker/\w+/Dockerfile)\b", f.read_text(encoding="utf-8")):
            p = m.group(1)
            if "${" in p:
                continue
            assert (ROOT / p).exists(), f"{f.name} 이 없는 파일을 가리킨다: {p}"


def test_dags_jobs_do_not_install_api_requirements():
    """Airflow constraints 가 fastapi 를 0.136.x 로 고정해 requirements-api(fastapi==0.162.0)와 충돌한다(11-09 실제 CI 실패).
    DAG 파싱 시험에는 requirements.txt + pytest 면 충분하다."""
    for name in ("ci.yml", "weekly.yml"):
        job = load(name)["jobs"]["dags"]
        runs = "\n".join(s.get("run", "") for s in job["steps"])
        assert "apache-airflow==3.3.0" in runs
        assert "requirements-dev.txt" not in runs and "requirements-api.txt" not in runs, name


def test_marker_and_path_combinations_select_something():
    """`pytest -m X <경로>` 에서 그 경로의 마커가 X 가 아니면 0개가 선택돼 exit 5 로 실패한다(11-09 weekly spark 실제 실패).
    마커는 디렉터리 이름으로 붙으므로(layers_plugin) 경로의 폴더가 X 인지 본다."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("lp_wf", ROOT / "tests" / "support" / "layers_plugin.py")
    lp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lp)
    checked = 0
    for f in FILES:
        for m in re.finditer(r"pytest\b[^\n]*?-m\s+\"?([a-z]+)\"?([^\n]*)", f.read_text(encoding="utf-8")):
            marker, rest = m.group(1), m.group(2)
            for path in re.findall(r"\btests/[\w/.-]+", rest):
                checked += 1
                rel = path if path.endswith(".py") else path.rstrip("/") + "/x.py"
                assert marker in lp.layers_for(rel), f"{f.name}: -m {marker} {path} → 선택되는 시험이 0개"
    assert checked >= 0
