#!/usr/bin/env python3
"""gitleaks 핀 파일(.github/gitleaks.pin)을 만든다. **사용자 Mac 에서 한 번** 실행한다 (인터넷 필요, 로그인·토큰 불필요).

  python scripts/pin_gitleaks.py                  최신 릴리스의 버전과 linux_x64 tarball 의 sha256 을 기록
  python scripts/pin_gitleaks.py --version 8.30.1  특정 버전

sha256 은 같은 릴리스의 checksums.txt 에서 가져온다. 이것은 '다운로드가 깨지지 않았음'을 보장하지, 릴리스 자체가 안전하다는 보증은 아니다
(릴리스 페이지와 체크섬 파일을 같은 사람이 올리므로). 그래서 값을 커밋해 두고 바뀔 때만 사람이 검토해 갱신한다.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIN = ROOT / ".github" / "gitleaks.pin"


def parse_checksums(text: str, version: str) -> str:
    name = f"gitleaks_{version}_linux_x64.tar.gz"
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].lstrip("*") == name and re.fullmatch(r"[0-9a-f]{64}", parts[0]):
            return parts[0]
    raise ValueError(f"checksums.txt 에서 {name} 을 찾지 못했습니다")


def pin_text(version: str, sha: str) -> str:
    return ("# gitleaks 고정 핀 (scripts/pin_gitleaks.py 가 만든다). 바꿀 때는 diff 를 사람이 검토한다.\n"
            f"version={version}\nsha256={sha}\n")


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "urbanstream-pin"})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
        return r.read()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default=None)
    a = ap.parse_args(argv)
    try:
        version = a.version
        if not version:
            tag = json.loads(_get("https://api.github.com/repos/gitleaks/gitleaks/releases/latest"))["tag_name"]
            version = tag.lstrip("v")
        sums = _get(f"https://github.com/gitleaks/gitleaks/releases/download/v{version}/gitleaks_{version}_checksums.txt").decode()
        sha = parse_checksums(sums, version)
    except Exception as e:  # 네트워크·형식 오류 모두 같은 처리
        print(f"핀을 만들지 못했습니다: {e}")
        return 1
    PIN.parent.mkdir(parents=True, exist_ok=True)
    PIN.write_text(pin_text(version, sha), encoding="utf-8")
    print(f"gitleaks {version} 핀 기록: {PIN.relative_to(ROOT)}  (git add 후 커밋하세요)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
