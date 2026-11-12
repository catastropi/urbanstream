#!/usr/bin/env python3
"""CI: Terraform 을 '버전·체크섬 고정'으로 내려받아 형식·문법만 검사한다 (Step 12, ADR-0014 D12-9).

  python scripts/ci_terraform.py check          fmt -check -recursive + init -backend=false + validate   (apply·plan 은 하지 않는다)

- 핀 파일 `.github/terraform.pin` 의 version=…, sha256=…(linux_amd64 zip)이 없거나 형식이 틀리면 **실패**한다(fail-closed).
  핀은 `python scripts/pin_terraform.py` 가 만든다(사용자 Mac 에서 한 번). 서드파티 액션(hashicorp/setup-terraform)을 쓰지 않으려는 방식이다.
- 내려받은 zip 의 sha256 이 핀과 다르면 실행하지 않는다.
- Azure 에 로그인하지 않고 비밀도 쓰지 않는다. `init -backend=false` 는 provider(registry.terraform.io)를 내려받을 뿐이다.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import re
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
PIN = ROOT / ".github" / "terraform.pin"
TF_DIR = ROOT / "infra" / "terraform"
URL = "https://releases.hashicorp.com/terraform/{v}/terraform_{v}_linux_amd64.zip"


class PinError(Exception):
    pass


def read_pin(path: Path | None = None) -> tuple[str, str]:
    path = path or PIN   # 호출 시점의 PIN 을 읽는다(시험이 바꿔 끼울 수 있게)
    if not path.exists():
        raise PinError(f"{path.name} 이 없습니다. 사용자 Mac 에서 `python scripts/pin_terraform.py` 를 실행해 만든 뒤 커밋하세요.")
    kv = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()
    v, h = kv.get("version", ""), kv.get("sha256", "")
    if not re.fullmatch(r"\d+\.\d+\.\d+", v) or not re.fullmatch(r"[0-9a-f]{64}", h):
        raise PinError("terraform 핀이 비어 있거나 형식이 틀립니다(UNPINNED). `python scripts/pin_terraform.py` 를 실행해 채우세요.")
    return v, h


def verify_sha256(data: bytes, expected: str) -> None:
    got = hashlib.sha256(data).hexdigest()
    if got != expected:
        raise PinError(f"terraform 다운로드의 sha256 이 핀과 다릅니다(기대 {expected[:12]}…, 실제 {got[:12]}…). 실행하지 않습니다.")


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as r:  # noqa: S310 — https 고정 URL
        return r.read()


def extract_binary(zip_bytes: bytes, dest: Path) -> Path:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        if "terraform" not in zf.namelist():
            raise PinError("zip 안에 terraform 실행 파일이 없습니다")
        out = dest / "terraform"
        out.write_bytes(zf.read("terraform"))
        out.chmod(0o755)
        return out


def check_commands(binary: str) -> list[list[str]]:
    """실행할 명령 목록(순서대로). apply·plan·destroy 는 절대 들어가지 않는다."""
    return [
        [binary, "fmt", "-check", "-recursive", "-diff"],
        [binary, "init", "-backend=false", "-input=false"],
        [binary, "validate"],
    ]


def cmd_check(*, dl: Callable[[str], bytes] = download, tf_dir: Path = TF_DIR) -> int:
    try:
        version, sha = read_pin()
        data = dl(URL.format(v=version))
        verify_sha256(data, sha)
        with tempfile.TemporaryDirectory() as td:
            binary = extract_binary(data, Path(td))
            for cmd in check_commands(str(binary)):
                print("$", " ".join(["terraform"] + cmd[1:]))
                r = subprocess.run(cmd, cwd=tf_dir)
                if r.returncode != 0:
                    return 1
        print("terraform fmt/validate: 통과")
        return 0
    except PinError as e:
        print(f"terraform 준비 실패: {e}")
        return 1
    except OSError as e:
        print(f"terraform 을 내려받거나 실행하지 못했습니다: {e}")
        return 2


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["check"])
    ap.parse_args(argv)
    return cmd_check()


if __name__ == "__main__":
    sys.exit(main())
