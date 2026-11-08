"""시험 계층 마커 (Step 11, ADR-0013 D11-5). tests/ 아래 디렉터리 이름을 pytest 마커로 붙인다. 시험 코드·결과는 바꾸지 않는다.

  python -m pytest -m unit        python -m pytest -m "integration and data"        python -m pytest -m e2e

규칙
- 디렉터리 이름 = 계층: unit · contract · integration · spark · e2e · recovery · api · dashboard · data
- 'data' 는 디렉터리가 아니라 목록(DATA_FILES)이다. 데이터 품질·마트 값을 보는 기존 시험을 다시 분류할 뿐 새 시험이 아니다.
  (tests/data/ 는 비어 있다 — 같은 내용을 복사해서 두 번 세지 않으려는 결정이다.)
- 로드 방법: pytest.ini 의 `-p layers_plugin` (pythonpath 에 tests/support 가 들어 있다). conftest.py 로 두지 않는 이유는
  tests/contract 가 `from conftest import` 를 쓰기 때문이다(HANDOFF 6장).
- 이 파일이 없거나 import 에 실패하면 pytest 가 시작하지 않는다 → 이 파일을 쓰는 모든 곳(변이 스크립트의 복사 목록, Spark 이미지의 마운트)에
  tests/support 가 함께 들어 있어야 한다. 시험(tests/unit/test_layers_plugin.py)이 그 목록을 대조한다.
"""
from __future__ import annotations

from pathlib import PurePosixPath

import pytest

LAYERS = ("unit", "contract", "integration", "spark", "e2e", "recovery", "api", "dashboard")

DATA_FILES = frozenset({
    "tests/integration/test_dq_pg.py", "tests/integration/test_marts_pg.py", "tests/integration/test_marts_7c_pg.py",
    "tests/integration/test_anomaly_pg.py", "tests/integration/test_mart.py", "tests/unit/test_dq_rules.py",
})


def layers_for(relpath: str) -> tuple[str, ...]:
    """'tests/api/test_x.py' → ('api',). 알 수 없는 위치는 ()."""
    parts = PurePosixPath(relpath.replace("\\", "/")).parts
    if len(parts) < 3 or parts[0] != "tests":
        return ()
    out: list[str] = []
    if parts[1] in LAYERS:
        out.append(parts[1])
    if "/".join(parts) in DATA_FILES:
        out.append("data")
    return tuple(out)


def pytest_collection_modifyitems(config, items):
    root = config.rootpath
    for item in items:
        try:
            rel = PurePosixPath(item.path.resolve().relative_to(root.resolve()).as_posix())
        except (ValueError, AttributeError):
            continue
        for layer in layers_for(str(rel)):
            item.add_marker(getattr(pytest.mark, layer))
