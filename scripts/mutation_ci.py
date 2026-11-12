"""CI·도구(Step 11) 변이 시험: 안전장치를 일부러 망가뜨려서 시험이 잡는지 본다 (HANDOFF 검증 습관 ②).

임시 폴더에 복사본을 만들어 한 번에 하나씩 바꾸고, 지정한 테스트가 '실패'해야 통과로 센다. 원본은 건드리지 않는다.
대부분 Postgres·Docker·네트워크가 필요 없다. e2e 변이 1건만 GOLD_TEST_DSN 이 있어야 하고, 없으면 '건너뜀' 으로 세고 실패로 치지 않는다.
실행:  python scripts/mutation_ci.py            python scripts/mutation_ci.py --only 3,7
종료 코드: 못 잡은 변이·원문 불일치가 있으면 1.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COPY = [".github", ".gitleaks.toml", "ruff.toml", "pytest.ini", "docker-compose.yml", ".env.example", ".gitignore", "scripts", "src", "docker", "dashboard", "api",
        "sql", "infra", "docker-compose.cloud.yml", "requirements.txt", "requirements-api.txt", "requirements-dashboard.txt", "requirements-dev.txt", "tests/support", "tests/e2e", "tests/contract/snapshots",
        "tests/unit/test_workflows.py", "tests/unit/test_ci_checks.py", "tests/unit/test_ci_tools.py", "tests/unit/test_layers_plugin.py", "tests/unit/test_cloud_infra.py"]

U = "tests/unit/"
WFT, CCT, TOOLS, LAYT = U + "test_workflows.py", U + "test_ci_checks.py", U + "test_ci_tools.py", U + "test_layers_plugin.py"
E2E = "tests/e2e/test_api_dashboard_pg.py"
CLD = U + "test_cloud_infra.py"
CI, REL, WEEK = ".github/workflows/ci.yml", ".github/workflows/release.yml", ".github/workflows/weekly.yml"

# (설명, 파일, 원래 문자열(정확히 1번 나와야 함), 바꿀 문자열, 잡아야 할 테스트 인자들, PG 필요 여부)
MUTATIONS = [
    ("ci: 기본 권한을 write-all 로", CI, "permissions:\n  contents: read\n", "permissions: write-all\n", [WFT], False),
    ("release: 최상위 권한에 packages: write 추가", REL, "permissions:\n  contents: read\n\njobs:", "permissions:\n  contents: read\n  packages: write\n\njobs:", [WFT], False),
    ("release: 쓰기 권한 제거(푸시 불가를 테스트가 알아야 함)", REL, "      contents: read\n      packages: write\n", "      contents: read\n", [WFT], False),
    ("release: 태그 대신 모든 push 에 반응", REL, "  push:\n    tags: [\"v*\"]", "  push:\n    branches: [\"*\"]", [WFT], False),
    ("release: 이미지 검사 단계 삭제", REL, 'run: python scripts/ci_checks.py image-guard "${IMG}:${TAG#v}"', "run: echo skip", [WFT], False),
    ("release: latest 태그 추가", REL, '-t "${IMG}:${TAG#v}" -t "${IMG}:sha-${SHA::7}" .', '-t "${IMG}:${TAG#v}" -t "${IMG}:latest" .', [WFT], False),
    ("ci: pull_request_target 추가", CI, "on:\n  pull_request:", "on:\n  pull_request_target:\n  pull_request:", [WFT], False),
    ("release: 사용자 비밀 참조(GITHUB_TOKEN 이외)", REL, "GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}", "GH_TOKEN: ${{ secrets.MY_API_KEY }}", [WFT], False),
    ("release: 액션을 main 브랜치로 참조", REL, '      - uses: actions/setup-python@v6\n        with:\n          python-version: "3.12"\n      - name: 태그', '      - uses: actions/setup-python@main\n        with:\n          python-version: "3.12"\n      - name: 태그', [WFT], False),
    ("ci: hygiene job 의 timeout 제거", CI, "  hygiene:\n    runs-on: ubuntu-latest\n    timeout-minutes: 10\n", "  hygiene:\n    runs-on: ubuntu-latest\n", [WFT], False),
    ("ci: ci-ok 집계 조건 if: always() 제거", CI, "    if: always()\n", "", [WFT], False),
    ("ci: 집계가 docker-build 를 기다리지 않음", CI, "needs: [changes, hygiene, secrets, test-core, test-minimal, test-pg, mutation-api, dags, docker-build, terraform]", "needs: [changes, hygiene, secrets, test-core, test-minimal, test-pg, mutation-api, dags, terraform]", [WFT], False),
    ("ci: PG 서비스 이미지를 compose 와 다르게", CI, "  test-pg:\n    runs-on: ubuntu-latest\n    timeout-minutes: 25\n    services:\n      postgres:\n        image: imresamu/postgis:17-3.6.1-bookworm", "  test-pg:\n    runs-on: ubuntu-latest\n    timeout-minutes: 25\n    services:\n      postgres:\n        image: postgres:17", [WFT], False),
    ("ci: 역할 시험 허용 플래그 제거", CI, '      ALLOW_ROLE_TESTS: "1"\n', "", [WFT], False),
    ("weekly: 전체 이력 검사를 diff 로", WEEK, "--mode full", "--mode diff", [WFT], False),
    ("weekly: dags job 이 requirements-dev(= fastapi 충돌)를 다시 설치", WEEK, 'constraints-3.12.txt" -r requirements.txt pytest', 'constraints-3.12.txt" -r requirements-dev.txt', [WFT], False),
    ("weekly: spark job 이 -m spark 로 unit 파일을 걸러 0개 선택", WEEK, "python -m pytest -q tests/spark tests/unit/test_bronze_verify.py", "python -m pytest -q -m spark tests/unit/test_bronze_verify.py", [WFT], False),
    ("컨테이너 래퍼: tests/support 를 PYTHONPATH 에서 뺌(구버전 pytest 는 pythonpath 무시)", "docker/spark-base/urbanstream-pytest.sh", ":/app/tests/support", "", [LAYT], False),
    ("컨테이너 래퍼: addopts 덮어쓰기 제거(구버전 pytest 가 -p layers_plugin 에서 실패)", "docker/spark-base/urbanstream-pytest.sh", " -o addopts=-ra", "", [LAYT], False),
    ("ruff: 정의되지 않은 이름 규칙(F82) 제거", "ruff.toml", '"F82"', '"F63"', [CCT], False),
    ("ci_checks: .env 계열을 허용", "scripts/ci_checks.py", 'if name == ".env" or name.startswith(".env."):', "if False:", [CCT], False),
    ("ci_checks: data/ 실파일을 허용", "scripts/ci_checks.py", 'FORBIDDEN_DIR_PREFIXES = ("data/", "tableau/export/")', 'FORBIDDEN_DIR_PREFIXES = ("tableau/export/",)', [CCT], False),
    ("ci_checks: .gitkeep 이외도 허용(KEEP_NAMES 확대)", "scripts/ci_checks.py", 'KEEP_NAMES = {".gitkeep", ".env.example"}', 'KEEP_NAMES = {".gitkeep", ".env.example", ".env"}', [CCT], False),
    ("ci_checks: 더미 필수 변수 하나 누락", "scripts/ci_checks.py", "DUMMY_VALUE = \"ci-not-a-secret\"", "DUMMY_VALUE = \"ci-not-a-secret\"\nDUMMY_REQUIRED = DUMMY_REQUIRED[:-1]", [CCT], False),
    ("ci_checks: 이미지 검사가 .env 를 못 봄", "scripts/ci_checks.py", 'IMAGE_FORBIDDEN = {".env", ', "IMAGE_FORBIDDEN = {", [CCT], False),
    ("ci_checks: 이미지 사용자 검사를 끔", "scripts/ci_checks.py", "if user.strip() != expected_user:", "if False:", [CCT], False),
    ("ci_changes: CI 자신의 변경이 전부 실행을 켜지 않음", "scripts/ci_changes.py", 'ALWAYS_ALL = (".github/", ', "ALWAYS_ALL = (", [TOOLS], False),
    ("ci_changes: 기준점을 모르면 건너뜀(fail-open 반대)", "scripts/ci_changes.py", "return {g: True for g in GROUPS}\n    files", "return {g: False for g in GROUPS}\n    files", [TOOLS], False),
    ("ci_gitleaks: sha256 검증 제거", "scripts/ci_gitleaks.py", "    if got != expected:", "    if False:", [TOOLS], False),
    ("ci_gitleaks: 핀 형식 검사 제거(UNPINNED 통과)", "scripts/ci_gitleaks.py", 'if not re.fullmatch(r"\\d+\\.\\d+\\.\\d+", v) or not re.fullmatch(r"[0-9a-f]{64}", h):', "if False:", [TOOLS], False),
    ("ci_gitleaks: 비밀 값을 가리지 않음(--redact 제거)", "scripts/ci_gitleaks.py", 'cmd += ["--config", config, "--redact", "--no-banner", "-v"]', 'cmd += ["--config", config, "--no-banner", "-v"]', [TOOLS], False),
    ("ci_gitleaks: 첫 푸시도 diff 범위로 검사", "scripts/ci_gitleaks.py", 'if mode == "diff" and (not rng or set(rng.split("..")[0]) <= {"0"}):', "if False:", [TOOLS], False),
    ("pin_actions: 이미 고정한 줄도 다시 바꿈", "scripts/pin_actions.py", "if not m or SHA.match(m[\"ref\"]):", "if not m:", [TOOLS], False),
    ("pin_actions: 해석 실패해도 줄을 바꿈", "scripts/pin_actions.py", "if not sha or not SHA.match(sha):", "if False:", [TOOLS], False),
    ("pin_gitleaks: 이름 검사 없이 첫 줄 사용", "scripts/pin_gitleaks.py", 'parts[1].lstrip("*") == name and ', "", [TOOLS], False),
    ("layers: data 표지 제거", "tests/support/layers_plugin.py", 'if "/".join(parts) in DATA_FILES:', "if False:", [LAYT], False),
    ("layers: e2e 계층 제거", "tests/support/layers_plugin.py", '"integration", "spark", "e2e", ', '"integration", "spark", ', [LAYT], False),
    ("pytest.ini: 플러그인 등록 제거", "pytest.ini", "addopts = -ra -p layers_plugin", "addopts = -ra", [LAYT], False),
    ("e2e: 대시보드가 호출하는 경로 오타(실서버에서만 404)", "dashboard/contract.py", '"/v1/traffic/current"', '"/v1/traffic/curent"', [E2E], True),
    # --- Step 12 (Azure 최소 증거 배포, ADR-0014) ---
    ("terraform: SSH 출발지를 모든 곳(*)으로", "infra/terraform/main.tf", "source_address_prefix      = var.allowed_ssh_cidr", 'source_address_prefix      = "*"', [CLD], False),
    ("terraform: SSH 대신 모든 포트를 허용", "infra/terraform/main.tf", 'destination_port_range     = "22"', 'destination_port_range     = "*"', [CLD], False),
    ("terraform: VM 비밀번호 로그인을 켬", "infra/terraform/main.tf", "disable_password_authentication = true", "disable_password_authentication = false", [CLD], False),
    ("terraform: SSH CIDR 검증에서 0.0.0.0/0 거부를 뺌", "infra/terraform/variables.tf", '!contains(["0.0.0.0/0", "::/0"], var.allowed_ssh_cidr) && ', "", [CLD], False),
    ("terraform: SSH CIDR 에 기본값(전 세계)을 줌", "infra/terraform/variables.tf", 'variable "allowed_ssh_cidr" {\n', 'variable "allowed_ssh_cidr" {\n  default = "0.0.0.0/0"\n', [CLD], False),
    ("terraform: 기본 VM 크기를 바꿈(크레딧 낭비)", "infra/terraform/variables.tf", 'default     = "Standard_B2s_v2"', 'default     = "Standard_D96s_v5"', [CLD], False),
    ("cloud-init: docker 설치를 뺌", "infra/terraform/cloud-init.yaml.tftpl", "  - docker.io\n", "  - podman\n", [CLD], False),
    ("compose 덮어쓰기: api 포트를 전 인터페이스로 다시 정의", "docker-compose.cloud.yml", "  api:\n    restart: unless-stopped", '  api:\n    restart: unless-stopped\n    ports:\n      - "8010:8000"', [CLD], False),
    ("compose 덮어쓰기: kafka 를 끌어들임", "docker-compose.cloud.yml", "  dashboard:\n    restart: unless-stopped", "  dashboard:\n    restart: unless-stopped\n  kafka:\n    restart: unless-stopped", [CLD], False),
    ("cloud.sh: 터널을 모든 인터페이스에 엶", "scripts/cloud.sh", '-L "127.0.0.1:${LOCAL_API_PORT}:127.0.0.1:8010"', '-L "${LOCAL_API_PORT}:127.0.0.1:8010"', [CLD], False),
    ("cloud.sh: 터널 로컬 포트가 Mac 스택(8010)과 겹침", "scripts/cloud.sh", "LOCAL_API_PORT=18010", "LOCAL_API_PORT=8010", [CLD], False),
    ("cloud.sh: 덤프가 마이그레이션 이력 표 데이터까지 뜸", "scripts/cloud.sh", " --exclude-table-data=ops.schema_migrations'", "'", [CLD], False),
    ("cloud.sh: 전송 목록 검증에서 tfstate 를 뺌", "scripts/cloud.sh", "\\.tfstate|\\.tfvars$|^data/", "\\.tfvars$|^data/", [CLD], False),
    ("cloud.sh: 전송 목록이 .gitignore 를 무시(.env 가 섞임)", "scripts/cloud.sh", "git ls-files -z --cached --others --exclude-standard", "git ls-files -z --cached --others", [CLD], False),
    ("cloud.sh: SSH 호스트 키 확인을 끔", "scripts/cloud.sh", "StrictHostKeyChecking=accept-new", "StrictHostKeyChecking=no", [CLD], False),
    ("cloud.sh: destroy 뒤 삭제 확인을 뺌", "scripts/cloud.sh", 'if [ "$(az group exists --name "$rg")" = "false" ]; then', "if true; then", [CLD], False),
    ("cloud_remote.sh: 복원이 한 트랜잭션이 아님(-1 제거)", "scripts/cloud_remote.sh", "psql -v ON_ERROR_STOP=1 -q -1 -U", "psql -v ON_ERROR_STOP=1 -q -U", [CLD], False),
    ("cloud_remote.sh: 복원 전 비우기가 마이그레이션 이력 표까지 비움", "scripts/cloud_remote.sh", " AND NOT (schemaname = 'ops' AND tablename = 'schema_migrations')", "", [CLD], False),
    ("cloud_remote.sh: postgres 만이 아니라 core 전체(kafka 포함)를 올림", "scripts/cloud_remote.sh", "$COMPOSE up -d --wait postgres\n", "$COMPOSE up -d --wait\n", [CLD], False),
    ("cloud_remote.sh: 완료 메시지에 API 키를 출력", "scripts/cloud_remote.sh", 'echo "완료. Mac 에서: scripts/cloud.sh tunnel"', 'echo "완료. 키: ${KEY}"', [CLD], False),
    (".gitignore: tfstate 규칙을 뺌", ".gitignore", "*.tfstate\n*.tfstate.*\n", "*.tfstate.*\n", [CLD], False),
    ("ci_checks: tracked 검사가 tfstate/tfvars 를 허용", "scripts/ci_checks.py", 'name.endswith((".tfstate", ".tfvars"))', "False", [CLD], False),
    ("ci_terraform: validate 단계를 뺌", "scripts/ci_terraform.py", '        [binary, "validate"],\n', "", [CLD], False),
    ("ci_terraform: sha256 검증 제거", "scripts/ci_terraform.py", "    if got != expected:", "    if False:", [CLD], False),
    ("ci_terraform: 핀 형식 검사 제거(UNPINNED 통과)", "scripts/ci_terraform.py", 'if not re.fullmatch(r"\\d+\\.\\d+\\.\\d+", v) or not re.fullmatch(r"[0-9a-f]{64}", h):', "if False:", [CLD], False),
    ("ci: terraform 잡이 서드파티 액션을 씀", CI, "      - name: terraform fmt -check + init -backend=false + validate\n", "      - uses: hashicorp/setup-terraform@v3\n      - name: terraform fmt -check + init -backend=false + validate\n", [CLD, WFT], False),
]


def run_one(i: int, m, tmp_root: Path) -> str:
    desc, rel, old, new, args, needs_pg = m
    if needs_pg and not os.environ.get("GOLD_TEST_DSN"):
        return "건너뜀(GOLD_TEST_DSN 없음)"
    work = tmp_root / f"m{i}"
    for item in COPY:
        src = ROOT / item
        dst = work / item
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__")) if src.is_dir() else shutil.copy2(src, dst)
    f = work / rel
    text = f.read_text(encoding="utf-8")
    if text.count(old) != 1:
        shutil.rmtree(work, ignore_errors=True)
        return f"오류: 원문이 {text.count(old)}번 나옴 — 변이 정의를 고쳐야 함"
    if old == new:
        shutil.rmtree(work, ignore_errors=True)
        return "오류: 바꿀 문자열이 같음"
    f.write_text(text.replace(old, new), encoding="utf-8")
    r = subprocess.run([sys.executable, "-I", "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *args], cwd=work,
                       capture_output=True, text=True, env=os.environ.copy())
    shutil.rmtree(work, ignore_errors=True)
    return "잡힘" if r.returncode != 0 else "못 잡음"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="쉼표로 구분한 번호(1부터)")
    args = ap.parse_args(argv)
    only = {int(x) for x in args.only.split(",")} if args.only else None
    results = []
    with tempfile.TemporaryDirectory(prefix="mut_ci_") as td:
        for i, m in enumerate(MUTATIONS, 1):
            if only and i not in only:
                continue
            res = run_one(i, m, Path(td))
            results.append(res)
            print(f"{i:2d}. [{res}] {m[0]}  ({m[1]})", flush=True)
    caught = results.count("잡힘")
    skipped = [r for r in results if r.startswith("건너뜀")]
    bad = [r for r in results if r == "못 잡음" or r.startswith("오류")]
    print(f"\n잡힘 {caught} / 건너뜀 {len(skipped)} / 못 잡음·오류 {len(bad)} (전체 {len(results)})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
