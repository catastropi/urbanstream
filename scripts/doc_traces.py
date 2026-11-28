#!/usr/bin/env python3
"""작업 환경 흔적 검사 (STEP 5, ADR-0018). 감사 보고서 6-1 의 표현이 파일마다 몇 번 나오는지 세고, 기준보다 늘면 실패한다.

  python scripts/doc_traces.py check          기준(tests/fixtures/doc_traces_baseline.json)보다 늘어난 파일이 있으면 1
  python scripts/doc_traces.py report         파일별 현재 개수 (많은 순)
  python scripts/doc_traces.py update         줄어든 파일의 기준을 낮춘다. 늘리는 것은 --allow-raise 를 줄 때만

지금(STEP 5)은 '더 늘지 않게' 막는 단계다. 기존 흔적은 STEP 6 에서 파일별로 고치고, 그때마다 update 로 기준을 낮춘다.
STEP 6 이 끝나면 기준 파일이 빈 객체가 되어 '0건' 검사와 같아진다.
검사 대상은 git 이 추적하는 텍스트 파일이다(git 이 없으면 저장소를 걷는다). 이 파일과 기준 파일은 패턴 자체를 담고 있어 제외한다.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tests" / "fixtures" / "doc_traces_baseline.json"
SELF = {"scripts/doc_traces.py", "tests/fixtures/doc_traces_baseline.json", "tests/unit/test_doc_traces.py"}
TEXT_SUFFIXES = {".md", ".py", ".sql", ".yml", ".yaml", ".toml", ".txt", ".sh", ".ini", ".cfg", ".json", ".tf", ".example", ".hcl", ""}
SKIP_PARTS = {".git", "data", "__pycache__", ".pytest_cache", ".ruff_cache", ".terraform", "node_modules"}
SKIP_PREFIXES = ("tests/contract/snapshots/", "tests/fixtures/recorded/", "docs/api-catalog/samples/")

# (이름, 정규식). 이름은 보고용이다.
PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("샌드박스", re.compile("샌드박스")),
    ("채팅", re.compile("채팅")),
    ("AI 이름", re.compile(r"\bClaude\b")),
    ("사용자 지시", re.compile(r"사용자\s*지시")),
    ("Blueprint", re.compile(r"\bBlueprint\b")),
    ("대화 회차", re.compile(r"(?<![0-9])[0-9]{1,2}차(?![가-힣])")),
    ("대화 전제", re.compile("이번 대화|새 대화|다음 대화|대화 세션")),
    ("붙여넣기 요청", re.compile("붙여넣어 주|붙여넣으면")),
)


def candidate_files(root: Optional[Path] = None) -> list[str]:
    root = root or ROOT
    r = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, capture_output=True, text=True)
    if r.returncode == 0 and r.stdout.strip():
        files = r.stdout.splitlines()
    else:
        files = [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file() and not (SKIP_PARTS & set(p.relative_to(root).parts))]
    out = []
    for f in files:
        p = Path(f)
        if f in SELF or f.startswith(SKIP_PREFIXES) or SKIP_PARTS & set(p.parts):
            continue
        if p.suffix.lower() in TEXT_SUFFIXES or p.name in ("Dockerfile", ".gitignore", ".env.example"):
            out.append(f)
    return sorted(out)


def count_text(text: str) -> dict[str, int]:
    return {name: len(rx.findall(text)) for name, rx in PATTERNS if rx.search(text)}


def current(root: Optional[Path] = None, files: Optional[Iterable[str]] = None) -> dict[str, int]:
    root = root or ROOT
    out: dict[str, int] = {}
    for f in (files if files is not None else candidate_files(root)):
        p = root / f
        if not p.is_file():
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        n = sum(count_text(text).values())
        if n:
            out[f] = n
    return out


def load_baseline(path: Optional[Path] = None) -> dict[str, int]:
    path = path or BASELINE
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def compare(now: dict[str, int], base: dict[str, int]) -> tuple[list[str], list[str]]:
    """(늘어난 파일 설명, 줄어든 파일 설명)."""
    up = [f"{f}: {base.get(f, 0)} → {n}" for f, n in sorted(now.items()) if n > base.get(f, 0)]
    down = [f"{f}: {b} → {now.get(f, 0)}" for f, b in sorted(base.items()) if now.get(f, 0) < b]
    return up, down


def save(data: dict[str, int], path: Optional[Path] = None) -> None:
    path = path or BASELINE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(sorted(data.items())), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    sub.add_parser("report")
    up_p = sub.add_parser("update")
    up_p.add_argument("--allow-raise", action="store_true", help="늘어난 파일도 기준에 넣는다(사유를 커밋 메시지에)")
    a = ap.parse_args(argv)
    now, base = current(ROOT), load_baseline()
    if a.cmd == "report":
        for f, n in sorted(now.items(), key=lambda kv: (-kv[1], kv[0])):
            p = ROOT / f
            detail = ", ".join(f"{k} {v}" for k, v in count_text(p.read_text(encoding="utf-8")).items())
            print(f"{n:4d}  {f}  ({detail})")
        print(f"합계 {sum(now.values())}건, 파일 {len(now)}개")
        return 0
    up, down = compare(now, base)
    if a.cmd == "check":
        if up:
            print("작업 환경 흔적이 늘었습니다 (scripts/doc_traces.py report 로 위치 확인):\n  " + "\n  ".join(up))
            return 1
        print(f"흔적 {sum(now.values())}건, 기준 {sum(base.values())}건. 늘어난 파일 없음"
              + (f". 줄어든 파일 {len(down)}개는 update 로 기준을 낮출 수 있음" if down else ""))
        return 0
    if up and not a.allow_raise:
        print("늘어난 파일이 있어 기준을 바꾸지 않았습니다(--allow-raise 로만):\n  " + "\n  ".join(up))
        return 1
    new = {f: n for f, n in now.items()} if a.allow_raise else {f: min(n, base[f]) for f, n in now.items() if f in base}
    save(new)
    print(f"기준을 다시 씀: {sum(new.values())}건, 파일 {len(new)}개")
    return 0


if __name__ == "__main__":
    sys.exit(main())
