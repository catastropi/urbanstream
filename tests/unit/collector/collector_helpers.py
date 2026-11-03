"""Collector 테스트 도구: 가짜 호출기/시계/발행기와 조립 함수. (이름이 겹치지 않도록 conftest 대신 일반 모듈로 둔다)"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

from collector.config import CollectorConfig, load_config
from collector.fetch import FetchResult, Redactor
from collector.publisher import MemoryPublisher
from collector.rawstore import RawStore
from collector.resilience import DailyBudget
from collector.runner import Collector
from urbanstream.contracts.common import KST

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "recorded"
KEYS = {"seoul": "SEOULKEY_abc123XYZ", "datago": "DATAGOKEY_xyz789QRS"}
T0 = datetime(2026, 11, 2, 10, 0, 0, tzinfo=KST)  # 월요일 오전 10시 (모든 소스의 활성 시간 안)


def fixture_bytes(rel: str) -> bytes:
    return (FIX / rel).read_bytes()


def ok(rel: str, latency: int = 40) -> FetchResult:
    return FetchResult(200, fixture_bytes(rel), latency)


def body(data: bytes | str, status: int = 200, latency: int = 40) -> FetchResult:
    return FetchResult(status, data if isinstance(data, bytes) else data.encode("utf-8"), latency)


def net_error(text: str = "ConnectionError: boom", kind: str = "network_error") -> FetchResult:
    return FetchResult(None, b"", 5, text, kind)


class Clock:
    def __init__(self, now: datetime = T0):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeFetcher:
    """responses: FetchResult 목록(소진되면 마지막 것을 반복) 또는 router(url, params) -> FetchResult."""

    def __init__(self, responses=None, router=None):
        self.responses, self.router, self.calls = list(responses or []), router, []

    def get(self, url, params):
        self.calls.append((url, params))
        if self.router:
            return self.router(url, params)
        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


def router_for_fixtures(url: str, params) -> FetchResult:
    if "TrafficInfo" in url:
        return ok("seoul_traffic/ok_20261101T180753.xml")
    if "AccInfo" in url:
        return ok("seoul_incident/ok_20261101T180755.xml")
    if "getBusPosByRtid" in url:
        return ok("bus_position/ok_20261101T215320.xml")
    if "getArrInfoByRouteAll" in url:
        return ok("bus_arrival/ok_20261101T215321.xml")
    if "getUltraSrtNcst" in url:
        return ok("kma_ncst/ok_20261028T150951.json")
    raise AssertionError(f"예상 밖의 URL: {url}")


def make_collector(tmp_path: Path, responses=None, router=None, publisher=None, cfg: CollectorConfig | None = None,
                   limits: dict | None = None, clock: Clock | None = None):
    cfg = cfg or load_config(env={})
    clock = clock or Clock()
    publisher = publisher if publisher is not None else MemoryPublisher()
    fetcher = FakeFetcher(responses, router)
    budget = DailyBudget(tmp_path / "budget.json", limits or {s.name: s.daily_budget for s in cfg.sources.values()},
                         clock)
    sleeps: list[float] = []
    redact = Redactor(KEYS.values())
    collector = Collector(cfg, fetcher, publisher, RawStore(tmp_path / "raw", tmp_path), budget, KEYS, redact,
                          clock, sleeps.append, random.Random(1))
    return collector, publisher, fetcher, clock, sleeps


def target(cfg: CollectorConfig, source: str):
    return cfg.sources[source].targets[0]
