#!/usr/bin/env python3
"""CI 보조 검사 (Step 11, ADR-0013). 워크플로 YAML 에 긴 셸을 쓰지 않고 여기에 두어 로컬에서도 같은 명령으로 돌리고 시험한다.

  python scripts/ci_checks.py tracked              git 이 추적 중인 파일에 .env·data/ 실파일·내보내기 CSV·캐시 파일이 없는지 (D-71, D-163 재발 방지)
  python scripts/ci_checks.py compose              docker compose --profile '*' config -q 를 '더미' 환경 파일로 실행 (필수 변수 7개 때문에 .env 없이는 실패하므로)
  python scripts/ci_checks.py image-guard IMAGE    빌드한 이미지 안에 .env·data·tests·docs 가 없고 비루트 사용자인지

종료 코드: 0 통과 / 1 규칙 위반 / 2 실행 환경 문제(git·docker 없음 등).
이 스크립트는 비밀을 읽지도 쓰지도 않는다. 더미 값은 일부러 '비밀이 아님' 이라고 적어 둔 문자열이다.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]

# --- tracked -----------------------------------------------------------------------------------------------
KEEP_NAMES = {".gitkeep", ".env.example"}
FORBIDDEN_DIR_PREFIXES = ("data/", "tableau/export/")
FORBIDDEN_NAMES = {".DS_Store"}
FORBIDDEN_PARTS = {"__pycache__", ".idea"}


def forbidden_tracked(paths: Iterable[str]) -> list[tuple[str, str]]:
    """(경로, 이유) 목록. data/ 아래는 .gitkeep 만 허용한다(.gitignore 의 규칙과 같다)."""
    bad: list[tuple[str, str]] = []
    for raw in paths:
        p = PurePosixPath(raw.strip())
        s = str(p)
        if not s or s == ".":
            continue
        name = p.name
        if name in KEEP_NAMES:
            continue
        if name == ".env" or name.startswith(".env."):
            bad.append((s, "환경 파일(.env 계열)은 추적하면 안 된다"))
        elif any(s.startswith(d) for d in FORBIDDEN_DIR_PREFIXES):
            bad.append((s, "data/ 와 tableau/export/ 의 실파일은 추적하면 안 된다(.gitkeep 만 허용)"))
        elif name in FORBIDDEN_NAMES or FORBIDDEN_PARTS & set(p.parts) or name.endswith(".pyc"):
            bad.append((s, "캐시·편집기 파일은 추적하면 안 된다"))
    return bad


def git_tracked(root: Path = ROOT) -> list[str]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True)
    return [x for x in out.stdout.decode("utf-8").split("\0") if x]


def cmd_tracked(root: Path = ROOT) -> int:
    try:
        files = git_tracked(root)
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"git 으로 파일 목록을 읽지 못했습니다: {e}")
        return 2
    bad = forbidden_tracked(files)
    for path, why in bad:
        print(f"금지: {path} — {why}")
    print(f"추적 파일 {len(files)}개 검사, 위반 {len(bad)}건")
    return 1 if bad else 0


# --- compose -----------------------------------------------------------------------------------------------
# docker-compose.yml 에서 `${NAME:?…}` 로 필수 처리한 변수들. 시험(tests/unit/test_ci_checks.py)이 compose 와 이 목록이 같은지 대조한다.
DUMMY_REQUIRED = ("AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD",
                  "POSTGRES_PASSWORD", "KAFKA_CLUSTER_ID", "AIRFLOW_ADMIN_PASSWORD")
DUMMY_VALUE = "ci-not-a-secret"


def dummy_env_text() -> str:
    return "".join(f"{k}={DUMMY_VALUE}\n" for k in DUMMY_REQUIRED)


def cmd_compose(root: Path = ROOT) -> int:
    with tempfile.TemporaryDirectory() as td:
        envf = Path(td) / "ci.env"
        envf.write_text(dummy_env_text(), encoding="utf-8")
        try:
            r = subprocess.run(["docker", "compose", "--env-file", str(envf), "--profile", "*", "config", "-q"],
                               cwd=root, capture_output=True, text=True)
        except OSError as e:
            print(f"docker 를 실행하지 못했습니다: {e}")
            return 2
    if r.returncode != 0:
        print((r.stderr or r.stdout).strip()[:2000])
        return 1
    print("docker compose config: 통과 (더미 환경 파일, 전체 프로파일)")
    return 0


# --- image-guard -------------------------------------------------------------------------------------------
IMAGE_FORBIDDEN = {".env", "data", "tests", "docs", ".git", ".github", "HANDOFF.md", "sql", "scripts"}


def check_image_listing(listing: Iterable[str], user: str, expected_user: str = "10001") -> list[str]:
    problems = []
    names = {x.strip() for x in listing if x.strip()}
    for n in sorted(names & IMAGE_FORBIDDEN):
        problems.append(f"이미지 /app 안에 들어가면 안 되는 항목: {n}")
    if user.strip() != expected_user:
        problems.append(f"실행 사용자가 {expected_user} 가 아님: {user.strip() or '(root)'}")
    return problems


def cmd_image_guard(image: str) -> int:
    try:
        ls = subprocess.run(["docker", "run", "--rm", "--entrypoint", "ls", image, "-A", "/app"], capture_output=True, text=True)
        usr = subprocess.run(["docker", "image", "inspect", "-f", "{{.Config.User}}", image], capture_output=True, text=True)
    except OSError as e:
        print(f"docker 를 실행하지 못했습니다: {e}")
        return 2
    if ls.returncode != 0 or usr.returncode != 0:
        print((ls.stderr or usr.stderr).strip()[:1000])
        return 2
    problems = check_image_listing(ls.stdout.splitlines(), usr.stdout)
    for p in problems:
        print(p)
    print(f"{image}: /app = {', '.join(ls.stdout.split())} · 사용자 {usr.stdout.strip()}")
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tracked")
    sub.add_parser("compose")
    g = sub.add_parser("image-guard")
    g.add_argument("image")
    args = ap.parse_args(argv)
    if args.cmd == "tracked":
        return cmd_tracked()
    if args.cmd == "compose":
        return cmd_compose()
    return cmd_image_guard(args.image)


if __name__ == "__main__":
    sys.exit(main())
