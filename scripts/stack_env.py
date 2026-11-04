"""`.env` 에 Docker Compose 용 항목을 채운다. (scripts/stack.sh init-env 가 호출)

규칙
- `.env.example` 의 `# stack:begin` ~ `# stack:end` 구간에 있는 KEY=VALUE 만 대상으로 한다.
- 이미 값이 있는 항목은 절대 바꾸지 않는다. 키가 없으면 추가하고, 키는 있는데 값이 비어 있으면 채운다.
- POSTGRES_PASSWORD, KAFKA_CLUSTER_ID 는 비어 있을 때 무작위로 만든다.
- 비밀 값은 출력하지 않는다. 어떤 키를 추가/채웠는지 이름만 출력한다.
- 다른 줄(SEOUL_API_KEY 등)은 한 글자도 건드리지 않는다.
"""
from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
import uuid
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
BEGIN, END = "# stack:begin", "# stack:end"


def _gen_password() -> str:
    return secrets.token_hex(16)


def _gen_cluster_id() -> str:
    # Kafka KRaft 클러스터 ID: UUID 16바이트의 base64url (패딩 없음, 22자)
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")


def _gen_fernet_key() -> str:
    # Airflow Fernet 키: 32바이트 무작위의 urlsafe base64 (44자)
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()


def _gen_secret() -> str:
    return secrets.token_urlsafe(32)


GENERATORS: dict[str, Callable[[], str]] = {
    "POSTGRES_PASSWORD": _gen_password,
    "KAFKA_CLUSTER_ID": _gen_cluster_id,
    "AIRFLOW_DB_PASSWORD": _gen_password,
    "GOLD_LOADER_PASSWORD": _gen_password,
    "AIRFLOW_FERNET_KEY": _gen_fernet_key,
    "AIRFLOW_JWT_SECRET": _gen_secret,
    "AIRFLOW_ADMIN_PASSWORD": _gen_password,
}


def stack_defaults(example_text: str) -> list[tuple[str, str]]:
    """`# stack:begin` ~ `# stack:end` 사이의 KEY=VALUE 목록 (순서 유지)."""
    out, inside = [], False
    for line in example_text.splitlines():
        s = line.strip()
        if s == BEGIN:
            inside = True
        elif s == END:
            inside = False
        elif inside and s and not s.startswith("#") and "=" in s:
            key, _, val = s.partition("=")
            out.append((key.strip(), val.strip()))
    return out


def plan_env(example_text: str, env_text: str) -> tuple[str, list[str], list[str]]:
    """(새 .env 내용, 추가한 키, 빈 값을 채운 키)."""
    defaults = stack_defaults(example_text)
    if not defaults:
        raise ValueError(".env.example 에 stack:begin ~ stack:end 구간이 없습니다")
    lines = env_text.split("\n") if env_text else []
    if lines and lines[-1] == "":
        lines.pop()  # 마지막 줄바꿈은 아래에서 다시 붙인다
    index: dict[str, int] = {}
    for i, line in enumerate(lines):
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            index[s.partition("=")[0].strip()] = i

    added, filled = [], []
    for key, default in defaults:
        value = default or (GENERATORS[key]() if key in GENERATORS else "")
        if key not in index:
            lines.append(f"{key}={value}")
            added.append(key)
        else:
            current = lines[index[key]].partition("=")[2].strip().strip('"').strip("'")
            if current == "" and value != "":
                lines[index[key]] = f"{key}={value}"
                filled.append(key)
    return "\n".join(lines) + "\n", added, filled


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=".env 에 Compose 용 항목 채우기")
    ap.add_argument("--env", default=str(ROOT / ".env"))
    ap.add_argument("--example", default=str(ROOT / ".env.example"))
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 무엇을 할지만 출력")
    args = ap.parse_args(argv)

    env_path, ex_path = Path(args.env), Path(args.example)
    if not env_path.exists():
        print(f".env 가 없습니다: {env_path}\n먼저 `cp .env.example .env` 후 API 키를 채우세요.")
        return 2
    if not ex_path.exists():
        print(f".env.example 이 없습니다: {ex_path}")
        return 2

    new_text, added, filled = plan_env(ex_path.read_text(encoding="utf-8"), env_path.read_text(encoding="utf-8"))
    if not added and not filled:
        print("변경 없음: .env 에 Compose 항목이 이미 모두 있습니다.")
        return 0
    if args.dry_run:
        print("(dry-run) 추가할 키:", ", ".join(added) or "-", "/ 빈 값을 채울 키:", ", ".join(filled) or "-")
        return 0

    tmp = env_path.with_suffix(".env.tmp")
    tmp.write_text(new_text, encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(env_path)
    print("추가한 키:", ", ".join(added) or "-")
    print("빈 값을 채운 키:", ", ".join(filled) or "-")
    print("(값은 출력하지 않습니다. .env 는 git 에 올라가지 않습니다.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
