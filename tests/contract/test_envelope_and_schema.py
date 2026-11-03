"""envelope 형태, 직렬화, 스키마 스냅샷(계약 고정), 문서 동기화, 비밀값 검사."""
import json
import re
from datetime import datetime
from pathlib import Path

import pytest
from conftest import MANIFEST, RECORDED, run

from urbanstream.contracts import KST, SOURCES, CallContext, ContractError, Envelope, schema_of

ROOT = Path(__file__).resolve().parents[2]
SNAP = Path(__file__).resolve().parent / "snapshots"

ENVELOPE_FIELDS = ["event_id", "source_system", "schema_version", "event_time", "fetched_at", "ingested_at",
                   "ingest_id", "raw_uri", "dedup_key", "http_status", "latency_ms"]


def _ok_results():
    for rel, meta in MANIFEST.items():
        if meta["expect_kind"] == "ok":
            yield rel, run(rel)


def test_envelope_fields_are_exactly_the_agreed_set():
    assert [f["name"] for f in schema_of("bus_position")["envelope"]] == ENVELOPE_FIELDS


@pytest.mark.parametrize("rel,res", list(_ok_results()), ids=lambda x: x if isinstance(x, str) else "")
def test_every_record_is_json_serializable_and_well_formed(rel, res):
    assert res.records
    for rec in res.records:
        d = rec.to_dict()
        json.dumps(d, ensure_ascii=False)                       # 직렬화 가능
        assert list(d)[:len(ENVELOPE_FIELDS)] == ENVELOPE_FIELDS  # envelope 필드 순서 고정
        assert d["source_system"] == res.source_system and d["schema_version"] == res.schema_version
        assert rec.envelope.event_time.utcoffset() == KST.utcoffset(None)
        assert rec.envelope.ingested_at is None                 # 파서는 ingested_at 을 채우지 않는다
        assert d["http_status"] == 200 and d["latency_ms"] == 50 and d["raw_uri"].startswith("recorded/")


@pytest.mark.parametrize("rel,res", list(_ok_results()), ids=lambda x: x if isinstance(x, str) else "")
def test_dedup_key_unique_within_a_call_and_event_id_deterministic(rel, res):
    keys = [r.envelope.dedup_key for r in res.records]
    assert len(set(keys)) == len(keys)
    ids = {r.envelope.event_id for r in res.records}
    assert len(ids) == len(keys)
    assert {r.envelope.ingest_id for r in res.records} == {res.records[0].envelope.ingest_id}  # 호출 1회 = ingest_id 1개


def test_naive_datetime_is_refused():
    with pytest.raises(ContractError):
        CallContext(fetched_at=datetime(2026, 11, 1, 12, 0, 0))
    with pytest.raises(ContractError):
        Envelope(event_id="x", source_system="s", schema_version="v", event_time=datetime(2026, 1, 1),
                 fetched_at=datetime(2026, 1, 1, tzinfo=KST), ingested_at=None, ingest_id="i", raw_uri=None,
                 dedup_key="k", http_status=200, latency_ms=None)


@pytest.mark.parametrize("source", sorted(SOURCES))
def test_schema_matches_committed_snapshot(source):
    snap = SNAP / f"{SOURCES[source].schema_version}.json"
    assert snap.exists(), f"스냅샷이 없습니다: {snap.name} (python scripts/export_schemas.py 로 만든 뒤 검토해서 커밋)"
    assert json.loads(snap.read_text(encoding="utf-8")) == schema_of(source), (
        "계약이 바뀌었습니다. 의도한 변경이라면 schema_version 을 올리거나(호환 불가) 스냅샷을 갱신하세요"
        " (python scripts/export_schemas.py).")


@pytest.mark.parametrize("source", sorted(SOURCES))
def test_catalog_documents_every_payload_field_and_rules(source):
    doc = (ROOT / "docs" / "api-catalog" / f"{source}.md").read_text(encoding="utf-8")
    spec = SOURCES[source]
    for f in schema_of(source)["payload"]:
        assert f"`{f['name']}`" in doc, f"{source}.md 에 payload 필드 `{f['name']}` 설명이 없습니다"
    assert spec.schema_version in doc
    assert spec.dedup_rule in doc and spec.event_time_rule in doc


def test_recorded_fixtures_contain_no_service_keys():
    pattern = re.compile(r"(service[_]?key|api[_]?key)\s*[=:]\s*(?!\*\*\*KEY\*\*\*)[A-Za-z0-9%+/=]{16,}", re.I)
    for path in list(RECORDED.rglob("*.xml")) + list(RECORDED.rglob("*.json")):
        assert not pattern.search(path.read_text(encoding="utf-8")), f"키처럼 보이는 문자열: {path}"
