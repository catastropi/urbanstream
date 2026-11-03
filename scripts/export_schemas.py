"""contracts 의 스키마를 tests/contract/snapshots/*.json 으로 저장한다.

언제 쓰나: payload 필드를 의도적으로 바꿨을 때. (스냅샷이 코드와 다르면 test_schema_matches_committed_snapshot 이 실패한다)
실행:  python scripts/export_schemas.py        (프로젝트 루트에서)
주의:  스냅샷을 갱신하는 것은 '계약을 바꾼다'는 뜻이다. 호환되지 않는 변경이면 schema_version 을 올리고
       docs/api-catalog/ 와 docs/adr/ 에 이유를 남긴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urbanstream.contracts import SOURCES, schema_of  # noqa: E402


def main() -> int:
    out_dir = ROOT / "tests" / "contract" / "snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, spec in sorted(SOURCES.items()):
        path = out_dir / f"{spec.schema_version}.json"
        path.write_text(json.dumps(schema_of(name), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"저장: {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
