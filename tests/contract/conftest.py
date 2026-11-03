"""계약 테스트 공통 도구: recorded 샘플 + manifest 로 파서를 실행한다."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from urbanstream.contracts import SOURCES, CallContext

RECORDED = Path(__file__).resolve().parents[1] / "fixtures" / "recorded"
MANIFEST = json.loads((RECORDED / "manifest.json").read_text(encoding="utf-8"))["files"]


def read_fixture(rel: str) -> bytes:
    return (RECORDED / rel).read_bytes()


def run(rel: str, *, body: bytes | str | None = None, http_status: int | None = None):
    """manifest 에 적힌 맥락으로 파서를 실행. body/http_status 로 일부만 바꿔 변형 케이스를 만들 수 있다."""
    meta = MANIFEST[rel]
    ctx = CallContext(
        fetched_at=datetime.fromisoformat(meta["fetched_at"]),
        http_status=meta["http_status"] if http_status is None else http_status,
        latency_ms=50,
        raw_uri=f"recorded/{rel}",
    )
    data = read_fixture(rel) if body is None else body
    return SOURCES[meta["source"]].parse(data, ctx, **meta.get("kwargs", {}))


@pytest.fixture
def traffic_ok():
    return run("seoul_traffic/ok_20261101T180753.xml")


@pytest.fixture
def incident_ok():
    return run("seoul_incident/ok_20261101T180755.xml")


@pytest.fixture
def position_ok():
    return run("bus_position/ok_20261101T215320.xml")


@pytest.fixture
def arrival_ok():
    return run("bus_arrival/ok_20261101T215321.xml")


@pytest.fixture
def weather_ok():
    return run("kma_ncst/ok_20261028T150951.json")
