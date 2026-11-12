#!/usr/bin/env python3
"""Terraform 핀 파일(.github/terraform.pin)을 만든다. **사용자 Mac 에서 한 번** 실행한다 (인터넷 필요, 로그인·토큰 불필요).  Step 12, ADR-0014

  python scripts/pin_terraform.py                   Mac 에 설치된 `terraform version` 의 버전으로 linux_amd64 zip 의 sha256 을 기록
  python scripts/pin_terraform.py --version 1.17.1  특정 버전

sha256 은 같은 릴리스의 SHA256SUMS 에서 가져온다(releases.hashicorp.com). 이것은 '다운로드가 깨지지 않았음'을 보장할 뿐 릴리스가 안전하다는 보증은 아니다.
그래서 값을 커밋해 두고 바뀔 때만 사람이 diff 를 검토한다(pin_gitleaks.py 와 같은 방식, 같은 이유 — CI 에 서드파티 액션을 쓰지 않으려는 것).
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIN = ROOT / ".github" / "terraform.pin"


def parse_sums(text: str, version: str) -> str:
    name = f"terraform_{version}_linux_amd64.zip"
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].lstrip("*") == name and re.fullmatch(r"[0-9a-f]{64}", parts[0]):
            return parts[0]
    raise ValueError(f"SHA256SUMS 에서 {name} 을 찾지 못했습니다")


def pin_text(version: str, sha: str) -> str:
    return ("# terraform 고정 핀 (scripts/pin_terraform.py 가 만든다). 바꿀 때는 diff 를 사람이 검토한다.\n"
            f"version={version}\nsha256={sha}\n")


def local_version() -> str:
    out = subprocess.run(["terraform", "version"], capture_output=True, text=True, check=True).stdout
    m = re.search(r"Terraform v(\d+\.\d+\.\d+)", out)
    if not m:
        raise ValueError("`terraform version` 출력에서 버전을 찾지 못했습니다")
    return m.group(1)


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "urbanstream-pin"})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
        return r.read()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default=None)
    a = ap.parse_args(argv)
    try:
        version = a.version or local_version()
        sums = _get(f"https://releases.hashicorp.com/terraform/{version}/terraform_{version}_SHA256SUMS").decode()
        sha = parse_sums(sums, version)
    except Exception as e:  # 네트워크·형식 오류 모두 같은 처리
        print(f"핀을 만들지 못했습니다: {e}")
        return 1
    PIN.parent.mkdir(parents=True, exist_ok=True)
    PIN.write_text(pin_text(version, sha), encoding="utf-8")
    print(f"terraform {version} 핀 기록: {PIN.relative_to(ROOT)}  (git add 후 커밋하세요)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
