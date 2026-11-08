#!/usr/bin/env python3
"""워크플로의 `uses: owner/repo@vN` 을 커밋 SHA 로 고정한다 (Step 11, ADR-0013 D11-9). **사용자 Mac 에서 한 번** 실행한다.

  python scripts/pin_actions.py --check     고정 안 된 액션 목록만 본다(바꾸지 않음. 종료 코드 1 = 고정 안 된 것이 있음)
  python scripts/pin_actions.py             태그를 SHA 로 바꾸고 원래 태그를 주석으로 남긴다:  uses: actions/checkout@<40자리> # v6

SHA 는 `git ls-remote https://github.com/<owner>/<repo>` 로 읽는다(로그인·토큰 불필요). 주석 `# v6` 은 Dependabot 이 갱신 PR 을 만들 때 쓴다.
태그는 소유자가 다른 커밋으로 옮길 수 있지만 SHA 는 옮길 수 없다 — 공급망 공격(태그 바꿔치기) 방어.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
USES = re.compile(r"^(?P<pre>\s*-?\s*uses:\s*)(?P<repo>[\w.-]+/[\w./-]+)@(?P<ref>[^\s#]+)(?P<rest>.*)$")
SHA = re.compile(r"^[0-9a-f]{40}$")


def resolve_tag(repo: str, tag: str) -> Optional[str]:
    owner_repo = "/".join(repo.split("/")[:2])
    out = subprocess.run(["git", "ls-remote", f"https://github.com/{owner_repo}", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        return None
    found: dict[str, str] = {}
    for line in out.stdout.splitlines():
        sha, ref = line.split("\t")
        found[ref] = sha
    return found.get(f"refs/tags/{tag}^{{}}") or found.get(f"refs/tags/{tag}")


def pin_text(text: str, resolver: Callable[[str, str], Optional[str]]) -> tuple[str, list[str], list[str]]:
    """(새 텍스트, 고정한 목록, 못 고정한 목록)."""
    pinned, failed, out = [], [], []
    for line in text.splitlines(keepends=True):
        m = USES.match(line.rstrip("\n"))
        if not m or SHA.match(m["ref"]):
            out.append(line)
            continue
        sha = resolver(m["repo"], m["ref"])
        if not sha or not SHA.match(sha):
            failed.append(f"{m['repo']}@{m['ref']}")
            out.append(line)
            continue
        pinned.append(f"{m['repo']}@{m['ref']}")
        out.append(f"{m['pre']}{m['repo']}@{sha} # {m['ref']}\n")
    return "".join(out), pinned, failed


def unpinned(text: str) -> list[str]:
    res = []
    for line in text.splitlines():
        m = USES.match(line)
        if m and not SHA.match(m["ref"]):
            res.append(f"{m['repo']}@{m['ref']}")
    return res


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args(argv)
    files = sorted(WORKFLOWS.glob("*.yml"))
    if a.check:
        todo = [(f.name, u) for f in files for u in unpinned(f.read_text(encoding="utf-8"))]
        for name, u in todo:
            print(f"{name}: {u}")
        print(f"고정 안 된 액션 {len(todo)}개")
        return 1 if todo else 0
    bad = 0
    for f in files:
        new, pinned, failed = pin_text(f.read_text(encoding="utf-8"), resolve_tag)
        if pinned:
            f.write_text(new, encoding="utf-8")
        print(f"{f.name}: 고정 {len(pinned)}개" + (f", 실패 {failed}" if failed else ""))
        bad += len(failed)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
