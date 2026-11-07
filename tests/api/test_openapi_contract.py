"""OpenAPI 계약. 1) 저장된 스냅샷과 같다 2) 스냅샷을 바꿀 때 '호환되지 않는 변경'을 사람이 알아차리게 한다.
Step 10 대시보드(Streamlit/Tableau)가 이 필드 이름·열거값에 기대므로, 지우거나 바꾸면 여기서 먼저 걸린다."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# 기본 환경에 fastapi 가 없으면 오류 대신 건너뛴다(설치: pip install -r requirements-api.txt)
pytest.importorskip("fastapi")

ROOT = Path(__file__).resolve().parents[2]
SNAP = ROOT / "tests" / "contract" / "snapshots" / "openapi_v1.json"


def _current() -> dict:
    sys.path.insert(0, str(ROOT / "scripts"))
    try:
        import export_openapi
        return export_openapi.build_spec()
    finally:
        sys.path.pop(0)


def test_snapshot_matches_code():
    saved = json.loads(SNAP.read_text(encoding="utf-8"))
    assert _current() == saved, "API 명세가 스냅샷과 다릅니다. 의도한 변경이면 python scripts/export_openapi.py 로 갱신하고 호환성을 확인하세요."


def test_check_flag_exit_codes(tmp_path):
    ok = subprocess.run([sys.executable, "-I", str(ROOT / "scripts" / "export_openapi.py"), "--check"], capture_output=True, text=True, cwd=ROOT)
    assert ok.returncode == 0, ok.stdout + ok.stderr


def test_every_operation_has_stable_id_tag_and_problem_responses():
    spec = _current()
    ids = []
    for path, item in spec["paths"].items():
        for method, op in item.items():
            ids.append(op["operationId"])
            assert op["tags"], path
            assert op["summary"], path
            if path.startswith("/v1"):
                assert "422" in op["responses"] or not op.get("parameters"), path


def test_operation_ids_are_unique_function_names():
    spec = _current()
    ids = [op["operationId"] for item in spec["paths"].values() for op in item.values()]
    assert len(ids) == len(set(ids)) and all(i.islower() or "_" in i for i in ids)


def test_enums_the_dashboard_relies_on_are_locked():
    s = _current()["components"]["schemas"]
    assert s["Congestion"]["properties"]["level"]["anyOf"][0]["enum"] == ["severe", "heavy", "slow", "normal"]
    assert s["IncidentImpactItem"]["properties"]["status"]["enum"] == ["listed", "pending", "finalized", "excluded_long_term"]
    assert s["Anomaly"]["properties"]["status"]["enum"] == ["anomaly", "candidate", "degenerate_baseline"]
    assert s["PipelineHealth"]["properties"]["overall"]["enum"] == ["ok", "degraded", "down", "idle", "unknown"]
    assert set(s["Meta"]["properties"]) == {"as_of", "age_sec", "stale", "loaded_at", "total", "notes"}


def test_no_secret_or_internal_names_in_spec():
    text = json.dumps(_current())
    for bad in ("api_service", "gold_writer", "password", "POSTGRES"):
        assert bad not in text, bad
