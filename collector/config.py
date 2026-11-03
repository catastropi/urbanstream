"""수집 설정 로딩과 검증 (config/sources.yaml, config/topics.yaml).

핵심 안전장치: 호출 계획이 일일 한도를 넘기면 시작 자체를 거부한다 (한도 위반을 운영 중에 발견하지 않는다).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
PLAN_RATIO = 0.8  # 계획된 호출 수가 daily_budget 의 80% 를 넘으면 거부 (나머지는 재시도 여유)
VALID_SOURCES = ("seoul_traffic", "seoul_incident", "bus_position", "bus_arrival", "kma_ncst")
_ENV = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    pass


def expand_env(value: Any, env: Optional[dict] = None) -> Any:
    """문자열 안의 ${NAME:-기본값} 을 환경변수로 치환. 사전/리스트는 재귀."""
    env = os.environ if env is None else env
    if isinstance(value, str):
        def sub(m: re.Match) -> str:
            got = env.get(m.group(1))
            return got if got not in (None, "") else (m.group(2) or "")
        return _ENV.sub(sub, value)
    if isinstance(value, list):
        return [expand_env(v, env) for v in value]
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    return value


# --------------------------------------------------------------------------- 토픽
@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int


@dataclass(frozen=True)
class TopicsConfig:
    topics: dict[str, TopicSpec]
    dlq_topic: str
    audit_topic: str


def load_topics(path: Path | str = ROOT / "config" / "topics.yaml") -> TopicsConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    d = raw.get("defaults", {})
    topics = {}
    for name, spec in (raw.get("topics") or {}).items():
        spec = spec or {}
        topics[name] = TopicSpec(
            name=name,
            partitions=int(spec.get("partitions", d.get("partitions", 1))),
            replication_factor=int(spec.get("replication_factor", d.get("replication_factor", 1))),
            retention_ms=int(spec.get("retention_ms", d.get("retention_ms", 259200000))),
        )
        if topics[name].partitions < 1:
            raise ConfigError(f"{name}: partitions 는 1 이상이어야 합니다")
    for key in ("dlq_topic", "audit_topic"):
        if raw.get(key) not in topics:
            raise ConfigError(f"{key}={raw.get(key)!r} 가 topics 에 정의돼 있지 않습니다")
    return TopicsConfig(topics=topics, dlq_topic=raw["dlq_topic"], audit_topic=raw["audit_topic"])


# --------------------------------------------------------------------------- 소스
def parse_hhmm(s: str) -> int:
    """'04:00' -> 분(240). '24:00' -> 1440."""
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", str(s).strip())
    if not m:
        raise ConfigError(f"시각 형식 오류(HH:MM): {s!r}")
    h, mi = int(m.group(1)), int(m.group(2))
    if (h, mi) > (24, 0) or mi > 59:
        raise ConfigError(f"시각 범위 오류: {s!r}")
    return h * 60 + mi


@dataclass(frozen=True)
class Target:
    """호출 단위: 소스 + 대상(링크/노선/격자). key 는 스케줄·회로차단 식별자."""
    source: str
    params: dict
    key: str


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay_sec: float = 1.0
    max_delay_sec: float = 10.0


@dataclass(frozen=True)
class BreakerConfig:
    failure_threshold: int = 5
    cooldown_sec: int = 300
    permanent_cooldown_sec: int = 900


@dataclass(frozen=True)
class SourceConfig:
    name: str
    enabled: bool
    topic: str
    interval_sec: int
    daily_budget: int
    budget_basis: str
    window_start_min: int
    window_end_min: int
    targets: tuple[Target, ...]
    extra: dict = field(default_factory=dict)

    @property
    def window_seconds(self) -> int:
        return (self.window_end_min - self.window_start_min) * 60

    @property
    def planned_calls_per_day(self) -> int:
        return int(len(self.targets) * (self.window_seconds / self.interval_sec))


@dataclass(frozen=True)
class CollectorConfig:
    sources: dict[str, SourceConfig]
    timeout: tuple[float, float]
    retry: RetryConfig
    breaker: BreakerConfig
    kma_available_after_min: int
    topics: TopicsConfig
    total_timeout_sec: float = 15.0   # 호출 1회(헤더+본문)의 총 제한시간. connect/read 는 바이트 사이 간격만 막는다

    def enabled_sources(self, only: Optional[list[str]] = None) -> list[SourceConfig]:
        out = [s for s in self.sources.values() if s.enabled and (not only or s.name in only)]
        if only:
            unknown = [o for o in only if o not in self.sources]
            if unknown:
                raise ConfigError(f"알 수 없는 소스: {unknown} (가능: {list(self.sources)})")
        return out


def _targets(name: str, raw: dict) -> tuple[Target, ...]:
    if name == "seoul_traffic":
        links = [str(x) for x in raw.get("link_ids", []) if str(x)]
        return tuple(Target(name, {"link_id": x}, f"{name}:{x}") for x in links)
    if name == "seoul_incident":
        return (Target(name, {"page_size": int(raw.get("page_size", 200))}, name),)
    if name in ("bus_position", "bus_arrival"):
        routes = [str(x) for x in raw.get("route_ids", []) if str(x)]
        return tuple(Target(name, {"route_id": x}, f"{name}:{x}") for x in routes)
    if name == "kma_ncst":
        grids = raw.get("grids", [])
        return tuple(Target(name, {"nx": int(g["nx"]), "ny": int(g["ny"])}, f"{name}:{g['nx']},{g['ny']}")
                     for g in grids)
    raise ConfigError(f"알 수 없는 소스: {name}")


def load_config(sources_path: Path | str = ROOT / "config" / "sources.yaml",
                topics_path: Path | str = ROOT / "config" / "topics.yaml",
                env: Optional[dict] = None) -> CollectorConfig:
    raw = expand_env(yaml.safe_load(Path(sources_path).read_text(encoding="utf-8")), env)
    topics = load_topics(topics_path)
    d = raw.get("defaults", {})
    t = d.get("timeout_sec", {})
    r, b = d.get("retry", {}), d.get("breaker", {})
    retry = RetryConfig(int(r.get("max_attempts", 3)), float(r.get("base_delay_sec", 1.0)),
                        float(r.get("max_delay_sec", 10.0)))
    breaker = BreakerConfig(int(b.get("failure_threshold", 5)), int(b.get("cooldown_sec", 300)),
                            int(b.get("permanent_cooldown_sec", 900)))
    if retry.max_attempts < 1:
        raise ConfigError("retry.max_attempts 는 1 이상이어야 합니다")

    sources: dict[str, SourceConfig] = {}
    for name, s in (raw.get("sources") or {}).items():
        if name not in VALID_SOURCES:
            raise ConfigError(f"알 수 없는 소스: {name}")
        interval = int(s["interval_sec"])
        if interval < 10:
            raise ConfigError(f"{name}: interval_sec 는 10 이상이어야 합니다 (초당 호출 제한 보호)")
        start_s, end_s = s.get("active_window", ["00:00", "24:00"])
        w0, w1 = parse_hhmm(start_s), parse_hhmm(end_s)
        if w1 <= w0:
            raise ConfigError(f"{name}: active_window 의 끝이 시작보다 늦어야 합니다")
        if s["topic"] not in topics.topics:
            raise ConfigError(f"{name}: topic {s['topic']!r} 가 topics.yaml 에 없습니다")
        basis = str(s.get("budget_basis", "unknown"))
        if basis not in ("official", "assumed", "unknown"):
            raise ConfigError(f"{name}: budget_basis 는 official/assumed/unknown 중 하나")
        extra = {k: v for k, v in s.items() if k not in
                 ("enabled", "topic", "interval_sec", "daily_budget", "budget_basis", "active_window")}
        cfg = SourceConfig(name=name, enabled=bool(s.get("enabled", True)), topic=s["topic"],
                           interval_sec=interval, daily_budget=int(s["daily_budget"]), budget_basis=basis,
                           window_start_min=w0, window_end_min=w1, targets=_targets(name, s), extra=extra)
        if cfg.enabled and not cfg.targets:
            raise ConfigError(f"{name}: 수집 대상(link_ids/route_ids/grids)이 비어 있습니다")
        if cfg.enabled and cfg.planned_calls_per_day > cfg.daily_budget * PLAN_RATIO:
            raise ConfigError(
                f"{name}: 하루 계획 호출 {cfg.planned_calls_per_day}회가 daily_budget {cfg.daily_budget}의 "
                f"{int(PLAN_RATIO * 100)}%({int(cfg.daily_budget * PLAN_RATIO)})를 넘습니다. "
                "interval_sec 를 늘리거나 active_window/대상 수를 줄이세요.")
        sources[name] = cfg

    total = float(t.get("total", 15))
    if total <= 0:
        raise ConfigError("timeout_sec.total 은 0보다 커야 합니다")
    return CollectorConfig(sources=sources, timeout=(float(t.get("connect", 5)), float(t.get("read", 10))),
                           retry=retry, breaker=breaker,
                           kma_available_after_min=int(d.get("kma_available_after_min", 45)), topics=topics,
                           total_timeout_sec=total)
