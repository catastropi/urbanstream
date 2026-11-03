"""Spark 테스트 도구 (conftest 와 이름이 겹치지 않도록 일반 모듈로 둔다). pyspark 가 있을 때만 import 한다."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import types as T

from urbanstream.contracts import SOURCES, CallContext

RECORDED = Path(__file__).resolve().parents[1] / "fixtures" / "recorded"
MANIFEST = json.loads((RECORDED / "manifest.json").read_text(encoding="utf-8"))["files"]

# 소스마다 recorded 샘플 1개 (tests/contract/conftest.py 와 같은 것)
SAMPLE_FILES = {
    "seoul_traffic": "seoul_traffic/ok_20261101T180753.xml",
    "seoul_incident": "seoul_incident/ok_20261101T180755.xml",
    "bus_position": "bus_position/ok_20261101T215320.xml",
    "bus_arrival": "bus_arrival/ok_20261101T215321.xml",
    "kma_ncst": "kma_ncst/ok_20261028T150951.json",
}

KAFKA_SCHEMA = T.StructType([
    T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType()),
    T.StructField("timestampType", T.IntegerType()),
])


def parse_sample(source: str):
    """recorded 샘플을 실제 파서로 돌려 Record 목록을 얻는다."""
    rel = SAMPLE_FILES[source]
    meta = MANIFEST[rel]
    ctx = CallContext(fetched_at=datetime.fromisoformat(meta["fetched_at"]), http_status=meta["http_status"],
                      latency_ms=50, raw_uri=f"recorded/{rel}")
    res = SOURCES[meta["source"]].parse((RECORDED / rel).read_bytes(), ctx, **meta.get("kwargs", {}))
    assert res.ok and res.records, f"{source}: 샘플이 정상 파싱되지 않음"
    return res.records


def message_json(record) -> str:
    """수집기가 Kafka 에 넣는 것과 같은 모양의 JSON (collector/runner.py 와 동일: ensure_ascii=False)."""
    return json.dumps(record.to_dict(), ensure_ascii=False)


def kafka_rows(spark, messages, topic="t", ts=None, start_offset=0, partition=0, key="k"):
    """Kafka 소스가 주는 열 구성의 배치 DataFrame. messages 는 str 목록."""
    ts = ts or datetime(2026, 11, 3, 6, 0, 0, tzinfo=timezone.utc)
    rows = [(key.encode(), m.encode("utf-8"), topic, partition, start_offset + i, ts, 0) for i, m in enumerate(messages)]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)
