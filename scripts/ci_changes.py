#!/usr/bin/env python3
"""CI: 바뀐 파일을 보고 '무거운 job 을 돌릴지' 정한다 (Step 11, ADR-0013 D11-1).

  python scripts/ci_changes.py --base <sha> --head <sha>     결과를 GITHUB_OUTPUT 에 `api=true` 같은 줄로 쓴다(없으면 표준 출력)

원칙: **확신이 없으면 돌린다(fail-open).** base 가 없거나 전부 0 이거나 git 이 실패하면 모든 group 을 true 로 한다.
건너뛰는 것은 '이번에 바뀐 파일이 그 group 과 무관하다고 git 이 확실히 말해 줄 때' 뿐이다.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]

# 이 접두사/파일 중 하나라도 바뀌면 그 group 은 true. 워크플로·CI 스크립트가 바뀌면 전부 true (CI 자신의 변경은 항상 시험한다).
ALWAYS_ALL = (".github/", "scripts/ci_", "scripts/pin_", "ruff.toml", ".gitleaks.toml", "pytest.ini", "tests/support/", "scripts/mutation_ci.py")
GROUPS: dict[str, tuple[str, ...]] = {
    "api": ("api/", "sql/", "tests/api/", "requirements-api.txt", "scripts/mutation_api.py", "scripts/export_openapi.py", "tests/contract/"),
    "dags": ("dags/", "batch/", "src/", "docker/airflow/", "tests/unit/test_dags_parse.py", "config/"),
    "docker": ("docker/api/", "docker/dashboard/", "api/", "dashboard/", "requirements-api.txt", "requirements-dashboard.txt", ".dockerignore"),
    "infra": ("infra/", ".github/terraform.pin"),
}


def classify(changed: Optional[Iterable[str]]) -> dict[str, bool]:
    """changed=None 은 '모름' → 전부 true."""
    if changed is None:
        return {g: True for g in GROUPS}
    files = [c.strip() for c in changed if c.strip()]
    if any(f.startswith(ALWAYS_ALL) or f in ALWAYS_ALL for f in files):
        return {g: True for g in GROUPS}
    return {g: any(f.startswith(p) or f == p for f in files for p in prefixes) for g, prefixes in GROUPS.items()}


def changed_files(base: str, head: str, root: Path = ROOT) -> Optional[list[str]]:
    if not base or set(base) <= {"0"} or not head:
        return None
    r = subprocess.run(["git", "diff", "--name-only", f"{base}...{head}"], cwd=root, capture_output=True, text=True)
    if r.returncode != 0:
        r = subprocess.run(["git", "diff", "--name-only", base, head], cwd=root, capture_output=True, text=True)
        if r.returncode != 0:
            return None
    return r.stdout.splitlines()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="")
    ap.add_argument("--head", default="HEAD")
    args = ap.parse_args(argv)
    res = classify(changed_files(args.base, args.head))
    lines = "".join(f"{k}={'true' if v else 'false'}\n" for k, v in res.items())
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(lines)
    print(lines, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
