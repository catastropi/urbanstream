#!/usr/bin/env python3
"""CI: gitleaks 를 '버전·체크섬 고정'으로 내려받아 실행한다 (Step 11, ADR-0013 D11-3).

  python scripts/ci_gitleaks.py scan --mode diff --range <base>..<head>   변경분만(PR·푸시)
  python scripts/ci_gitleaks.py scan --mode full                          전체 이력(주 1회·수동)

- 핀 파일 `.github/gitleaks.pin` 에 version=… 과 sha256=…(linux_x64 tarball) 이 있어야 한다. 없거나 'UNPINNED' 이면 **실패**한다(fail-closed:
  스캔을 조용히 건너뛰지 않는다). 핀은 `python scripts/pin_gitleaks.py` 가 만든다(사용자 Mac 에서 한 번).
- 내려받은 파일의 sha256 이 핀과 다르면 실행하지 않는다.
- 결과는 `--redact` 로 출력해 로그에 비밀이 찍히지 않게 한다.
- 이 스크립트는 비밀을 읽거나 보내지 않는다. 네트워크는 GitHub 릴리스 다운로드뿐.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
PIN = ROOT / ".github" / "gitleaks.pin"
URL = "https://github.com/gitleaks/gitleaks/releases/download/v{v}/gitleaks_{v}_linux_x64.tar.gz"


class PinError(Exception):
    pass


def read_pin(path: Path = PIN) -> tuple[str, str]:
    if not path.exists():
        raise PinError(f"{path.name} 이 없습니다. 사용자 Mac 에서 `python scripts/pin_gitleaks.py` 를 실행해 만든 뒤 커밋하세요.")
    kv = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()
    v, h = kv.get("version", ""), kv.get("sha256", "")
    if not re.fullmatch(r"\d+\.\d+\.\d+", v) or not re.fullmatch(r"[0-9a-f]{64}", h):
        raise PinError("gitleaks 핀이 비어 있거나 형식이 틀립니다(UNPINNED). `python scripts/pin_gitleaks.py` 를 실행해 채우세요.")
    return v, h


def verify_sha256(data: bytes, expected: str) -> None:
    got = hashlib.sha256(data).hexdigest()
    if got != expected:
        raise PinError(f"gitleaks 다운로드의 sha256 이 핀과 다릅니다(기대 {expected[:12]}…, 실제 {got[:12]}…). 실행하지 않습니다.")


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as r:  # noqa: S310 — https 고정 URL
        return r.read()


def extract_binary(tar_bytes: bytes, dest: Path) -> Path:
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tf:
        member = next((m for m in tf.getmembers() if m.isfile() and Path(m.name).name == "gitleaks"), None)
        if member is None:
            raise PinError("tarball 안에 gitleaks 실행 파일이 없습니다")
        f = tf.extractfile(member)
        assert f is not None
        out = dest / "gitleaks"
        out.write_bytes(f.read())
        out.chmod(0o755)
        return out


def scan_command(binary: str, mode: str, rng: Optional[str], help_text: str, config: str = ".gitleaks.toml") -> list[str]:
    """v8.19+ 는 `git` 하위 명령, 그 이전은 `detect`. 도움말에 `git` 이 보이면 새 방식."""
    new = re.search(r"^\s+git\s", help_text, re.M) is not None
    cmd = [binary, "git" if new else "detect"]
    if not new:
        cmd += ["--source", "."]
    cmd += ["--config", config, "--redact", "--no-banner", "-v"]
    if mode == "diff":
        if not rng:
            raise PinError("--mode diff 에는 --range 가 필요합니다")
        cmd += ["--log-opts", rng]
    if new:
        cmd += ["."]
    return cmd


def cmd_scan(mode: str, rng: Optional[str], *, dl: Callable[[str], bytes] = download) -> int:
    try:
        version, sha = read_pin()
        data = dl(URL.format(v=version))
        verify_sha256(data, sha)
        with tempfile.TemporaryDirectory() as td:
            binary = extract_binary(data, Path(td))
            helptxt = subprocess.run([str(binary), "--help"], capture_output=True, text=True).stdout
            if mode == "diff" and (not rng or set(rng.split("..")[0]) <= {"0"}):
                mode, rng = "full", None            # 첫 푸시(이전 커밋 없음) → 전체
            r = subprocess.run(scan_command(str(binary), mode, rng, helptxt), cwd=ROOT)
            return 0 if r.returncode == 0 else 1
    except PinError as e:
        print(f"gitleaks 준비 실패: {e}")
        return 1
    except OSError as e:
        print(f"gitleaks 를 내려받거나 실행하지 못했습니다: {e}")
        return 2


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan")
    s.add_argument("--mode", choices=["diff", "full"], required=True)
    s.add_argument("--range", dest="rng", default=None)
    a = ap.parse_args(argv)
    return cmd_scan(a.mode, a.rng)


if __name__ == "__main__":
    sys.exit(main())
