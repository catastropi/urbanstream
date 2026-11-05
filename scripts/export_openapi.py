"""API 의 OpenAPI 명세를 tests/contract/snapshots/openapi_v1.json 으로 저장한다 (D9-10).

언제 쓰나: 엔드포인트·필드·열거값을 '의도적으로' 바꿨을 때. 스냅샷이 코드와 다르면 tests/api/test_openapi_contract.py 가 실패한다.
실행:  python scripts/export_openapi.py            (프로젝트 루트에서. DB 가 없어도 된다)
       python scripts/export_openapi.py --check    (저장하지 않고 같은지만 확인. 다르면 종료 코드 1)
주의:  스냅샷을 갱신하는 것은 '계약을 바꾼다'는 뜻이다. 필드 삭제·이름 변경·열거값 삭제는 호환되지 않는 변경이므로
       경로를 /v2 로 올리고 docs/adr/ 에 이유를 남긴다. 필드·엔드포인트 추가는 호환된다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SNAPSHOT = ROOT / "tests" / "contract" / "snapshots" / "openapi_v1.json"


def build_spec() -> dict:
    from api.config import load_settings
    from api.main import create_app
    # 명세는 설정(키·DB)과 무관하다. 더미 값으로 앱만 만든다(연결하지 않음).
    app = create_app(load_settings({"API_KEYS": "x" * 32, "API_DB_PASSWORD": "x" * 16}))
    return app.openapi()


def render(spec: dict) -> str:
    return json.dumps(spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OpenAPI 스냅샷 저장/확인")
    ap.add_argument("--check", action="store_true", help="저장하지 않고 현재 코드와 같은지만 본다")
    args = ap.parse_args(argv)
    text = render(build_spec())
    if args.check:
        same = SNAPSHOT.exists() and SNAPSHOT.read_text(encoding="utf-8") == text
        print("OpenAPI 스냅샷이 코드와 같습니다." if same else "OpenAPI 스냅샷이 코드와 다릅니다. 의도한 변경이면: python scripts/export_openapi.py")
        return 0 if same else 1
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT.write_text(text, encoding="utf-8")
    print(f"저장: {SNAPSHOT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
