"""계약 스키마 → Spark DDL 변환 (순수 Python — pyspark/Java 없이 호스트에서 돈다).

스냅샷(tests/contract/snapshots/*.json)을 기준으로 삼는다: 계약이 바뀌어 스냅샷이 갱신되면 이 변환도 따라가는지(또는 못 따라가면 실패하는지) 본다.
"""
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from urbanstream.contracts import SOURCES
from urbanstream.contracts import spark_schema
from urbanstream.contracts.spark_schema import SPARK_TYPES, ddl_of, required_paths

# 기대 매핑은 구현(SPARK_TYPES)에서 가져오지 않고 여기에 따로 적는다 (같은 값을 양쪽에서 읽으면 틀려도 통과하는 순환 검증이 된다).
# 특히 long → BIGINT: INT(32비트)로 줄이면 큰 값이 조용히 NULL/오류가 된다.
EXPECTED_TYPES = {"string": "STRING", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}

SNAP = Path(__file__).resolve().parents[1] / "contract" / "snapshots"


def snapshot(source: str) -> dict:
    return json.loads((SNAP / f"{SOURCES[source].schema_version}.json").read_text(encoding="utf-8"))


def parse_ddl(ddl: str) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    m = re.fullmatch(r"(.*), payload STRUCT<(.*)>", ddl)
    assert m, ddl
    split = lambda t: [tuple(x.strip().replace(":", " ").split()) for x in t.split(",")]  # noqa: E731
    return split(m.group(1)), split(m.group(2))


@pytest.mark.parametrize("source", sorted(SOURCES))
def test_ddl_has_every_snapshot_field_in_order_with_mapped_types(source):
    snap = snapshot(source)
    env, payload = parse_ddl(ddl_of(source))
    assert env == [(f["name"], EXPECTED_TYPES[f["type"]]) for f in snap["envelope"]]
    assert payload == [(f["name"], EXPECTED_TYPES[f["type"]]) for f in snap["payload"]]


@pytest.mark.parametrize("source", sorted(SOURCES))
def test_required_paths_are_exactly_the_non_nullable_fields(source):
    snap = snapshot(source)
    want = [f["name"] for f in snap["envelope"] if not f["nullable"]] + \
           [f"payload.{f['name']}" for f in snap["payload"] if not f["nullable"]]
    assert required_paths(source) == want
    assert "ingested_at" not in required_paths(source)      # 수집기가 비워 두므로 필수가 아니다 (Bronze 가 채운다)
    assert "event_id" in required_paths(source) and "payload.dedup_key" not in required_paths(source)


def test_type_mapping_table_is_exactly_the_expected_one():
    assert SPARK_TYPES == EXPECTED_TYPES


def test_every_contract_type_used_by_snapshots_has_a_spark_mapping():
    used = {f["type"] for s in SOURCES for part in ("envelope", "payload") for f in snapshot(s)[part]}
    assert used <= set(SPARK_TYPES), f"Spark 매핑이 없는 계약 타입: {used - set(SPARK_TYPES)}"


def test_unknown_contract_type_fails_loudly_instead_of_guessing():
    with pytest.raises(TypeError, match="Spark 타입 매핑이 없는"):
        spark_schema._spark_type("decimal")


def test_module_works_even_when_pyspark_cannot_be_imported():
    """호스트(Mac)에는 pyspark 가 없다. pyspark import 를 일부러 막은 새 프로세스에서도 DDL 을 만들 수 있어야 한다."""
    root = Path(__file__).resolve().parents[2]
    code = (f"import sys; sys.path.insert(0, {str(root / 'src')!r}); sys.modules['pyspark'] = None; "
            "from urbanstream.contracts.spark_schema import ddl_of; "
            "print(ddl_of('seoul_traffic').startswith('event_id STRING'))")
    r = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, cwd=root)
    assert r.returncode == 0 and r.stdout.strip() == "True", r.stderr
