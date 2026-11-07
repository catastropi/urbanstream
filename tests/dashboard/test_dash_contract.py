"""대시보드가 읽는 엔드포인트·매개변수·필드가 OpenAPI 스냅샷(= 잠긴 계약)에 있는지 대조한다.
API 쪽에서 필드를 지우거나 이름을 바꾸면(비호환, 원래는 /v2) 여기서 먼저 실패한다."""
import json
from pathlib import Path

import pytest

import dashboard_support as D
from dashboard import contract as C
from dashboard.config import TTL_SECONDS

ROOT = Path(__file__).resolve().parents[2]
SNAP = json.loads((ROOT / "tests/contract/snapshots/openapi_v1.json").read_text(encoding="utf-8"))
SCHEMAS = SNAP["components"]["schemas"]


@pytest.mark.parametrize("ep", C.ALL_ENDPOINTS, ids=lambda e: e.name)
def test_endpoint_exists_with_declared_params_and_fields(ep):
    assert ep.path in SNAP["paths"] and "get" in SNAP["paths"][ep.path], f"{ep.path} 가 계약에 없다"
    declared = {p["name"] for p in SNAP["paths"][ep.path]["get"].get("parameters", [])}
    assert ep.params <= declared, f"{ep.name}: 계약에 없는 매개변수 {sorted(ep.params - declared)}"
    assert ep.ttl in TTL_SECONDS
    assert ep.reads, "읽는 필드를 선언해야 한다"
    for schema, fields in ep.reads:
        assert schema in SCHEMAS, f"스키마 {schema} 없음"
        missing = set(fields) - set(SCHEMAS[schema]["properties"])
        assert not missing, f"{ep.name}: {schema} 에 없는 필드 {sorted(missing)}"


def test_paths_and_names_are_unique():
    assert len({e.path for e in C.ALL_ENDPOINTS}) == len(C.ALL_ENDPOINTS) == len({e.name for e in C.ALL_ENDPOINTS})


def test_dashboard_only_reads_get_endpoints_that_need_auth():
    for ep in C.ALL_ENDPOINTS:
        assert ep.path.startswith("/v1/"), "/health·/ready 는 대시보드가 쓰지 않는다"
        assert set(SNAP["paths"][ep.path]) == {"get"}


def test_fake_api_payloads_cover_every_field_the_dashboard_reads():
    """가짜 API 가 실제 계약의 모양을 따르는지: 선언한 필드가 가짜 응답 항목에도 있어야 화면 시험이 의미가 있다."""
    for ep in C.ALL_ENDPOINTS:
        payload = D.payload_for(ep.path, {"link_id": "L", "geometry": "true"})
        data = payload["data"]
        items = data if isinstance(data, list) else (data.get("components", []) if "components" in data else [data])
        assert items, ep.name
        by_schema = dict(ep.reads)
        main = [s for s in by_schema if s not in ("PipelineHealth",)][0] if ep.name != "incidents" else "IncidentImpactItem"
        for field in by_schema[main]:
            assert any(field in it for it in items), f"{ep.name}: 가짜 응답에 {main}.{field} 없음"
