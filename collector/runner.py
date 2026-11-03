"""Collector 핵심: 한 대상(Target)에 대한 호출 1사이클과 스케줄러.

한 사이클 = 예산 확인 → (재시도하며) 호출 → 원본 저장 → 계약 파서 → Kafka 발행(+DLQ) → audit.
어떤 실패도 프로세스를 죽이지 않는다. 실패는 audit 과 회로차단으로 드러난다.
"""
from __future__ import annotations

import json
import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional

from urbanstream.contracts import SOURCES, CallContext
from urbanstream.contracts.common import KST

from .config import CollectorConfig, SourceConfig, Target
from .fetch import Fetcher, Redactor
from .publisher import Publisher
from .rawstore import RawStore
from .requests_spec import SEOUL_KEY_SOURCES, build_request, kma_base
from .resilience import CircuitBreaker, DailyBudget, RetryPolicy, classify

log = logging.getLogger("collector")

# Kafka 파티션 키. 같은 대상의 메시지가 같은 파티션에 들어가 순서가 유지된다.
KEY_FUNCS: dict[str, Callable] = {
    "seoul_traffic": lambda p: p.link_id,
    "seoul_incident": lambda p: p.acc_id,
    "bus_position": lambda p: p.veh_id,
    "bus_arrival": lambda p: f"{p.route_id}|{p.station_id}",
    "kma_ncst": lambda p: f"{p.nx}|{p.ny}",
}


def _json(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@dataclass
class CycleReport:
    target_key: str
    outcome: str              # ok | failed | permanent_failure | skipped
    attempts: int = 0
    n_records: int = 0
    n_rejects: int = 0
    published: int = 0
    reason: Optional[str] = None


class Collector:
    def __init__(self, cfg: CollectorConfig, fetcher: Fetcher, publisher: Publisher, rawstore: RawStore,
                 budget: DailyBudget, keys: dict[str, str], redact: Redactor,
                 clock: Callable[[], datetime], sleep: Callable[[float], None],
                 rng: Optional[random.Random] = None):
        self.cfg, self.fetcher, self.publisher, self.rawstore = cfg, fetcher, publisher, rawstore
        self.budget, self.keys, self.redact, self.clock, self.sleep = budget, keys, redact, clock, sleep
        self.retry = RetryPolicy(cfg.retry, rng)
        self.breakers: dict[str, CircuitBreaker] = {}
        self.last_kma_base: dict[str, tuple[str, str]] = {}
        # 소스별 스레드가 함께 쓰는 것들. Kafka 쪽은 '발행 → flush → 전달 실패 수 읽기'가 한 덩어리여야
        # 다른 스레드의 메시지 때문에 실패 수가 섞이지 않는다. 구간은 짧다 (Kafka 가 죽었을 때만 flush 가 길어진다).
        self._pub_lock = threading.RLock()
        self._breaker_lock = threading.Lock()

    # ------------------------------------------------------------------ 보조
    def _breaker(self, key: str) -> CircuitBreaker:
        with self._breaker_lock:
            if key not in self.breakers:
                self.breakers[key] = CircuitBreaker(self.cfg.breaker, self.clock)
            return self.breakers[key]

    def _key_for(self, source: str) -> str:
        return self.keys["seoul"] if source in SEOUL_KEY_SOURCES else self.keys["datago"]

    def should_skip(self, target: Target, now: datetime) -> Optional[str]:
        """호출 전에 건너뛸 이유. 기상청은 이미 받은 발표 시각이면 다시 부르지 않는다 (호출 예산 절약)."""
        if target.source == "kma_ncst":
            base = kma_base(now, self.cfg.kma_available_after_min)
            if self.last_kma_base.get(target.key) == base:
                return "base_already_fetched"
        return None

    def _audit(self, **fields) -> None:
        for k in ("status_message", "error"):
            if fields.get(k):
                fields[k] = self.redact(fields[k])
        msg = {"event": "call", "recorded_at": self.clock().isoformat(timespec="milliseconds"), **fields}
        with self._pub_lock:
            self.publisher.produce(self.cfg.topics.audit_topic, str(fields.get("source_system", "")).encode(),
                                   _json(msg), {}, None)

    # ------------------------------------------------------------------ 사이클
    def run_target(self, target: Target) -> CycleReport:
        src_cfg: SourceConfig = self.cfg.sources[target.source]
        name = target.source
        breaker = self._breaker(target.key)

        if not breaker.allow():
            self._audit(source_system=name, target=target.key, outcome="skipped", reason="circuit_open",
                        breaker=breaker.snapshot())
            return CycleReport(target.key, "skipped", reason="circuit_open")

        base_audit = {"source_system": name, "target": target.key}
        last = None  # 마지막 시도의 정보
        attempts = 0
        budget_stop = False
        for attempt in range(1, self.cfg.retry.max_attempts + 1):
            if not self.budget.try_consume(name):
                budget_stop = True
                break
            attempts = attempt
            now = self.clock()
            spec = build_request(name, target.params, self._key_for(name), now,
                                 kma_available_after_min=self.cfg.kma_available_after_min)
            fr = self.fetcher.get(spec.url, spec.params)
            fetched_at = self.clock()
            ctx = CallContext(fetched_at=fetched_at, http_status=fr.http_status or 0, latency_ms=fr.latency_ms)
            ctx.raw_uri = self.rawstore.write(name, fetched_at, ctx.ingest_id, fr.body) if fr.body else None

            result = None
            if fr.http_status is None:
                kind, code, message = fr.error_kind or "network_error", None, fr.error
            else:
                kwargs = {"route_id": target.params["route_id"]} if name == "bus_position" else {}
                result = SOURCES[name].parse(fr.body, ctx, **kwargs)
                kind, code, message = result.status.kind, result.status.code, result.status.message
            outcome = classify(kind)
            last = dict(ctx=ctx, fr=fr, result=result, kind=kind, code=code, message=message,
                        outcome=outcome, spec=spec)
            will_retry = outcome == "retryable" and attempt < self.cfg.retry.max_attempts
            if will_retry:
                self._audit(**base_audit, attempt=attempt, final=False, will_retry=True,
                            ingest_id=ctx.ingest_id, fetched_at=fetched_at.isoformat(), http_status=fr.http_status,
                            latency_ms=fr.latency_ms, status_kind=kind, status_code=code, status_message=message,
                            error=fr.error, raw_uri=ctx.raw_uri, budget_used=self.budget.used(name),
                            budget_limit=self.budget.limit(name))
                self.sleep(self.retry.delay(attempt))
                continue
            break

        if last is None:  # 첫 시도부터 예산이 없었다
            self._audit(**base_audit, outcome="skipped", reason="budget_exhausted",
                        budget_used=self.budget.used(name), budget_limit=self.budget.limit(name))
            log.warning("[%s] 오늘 호출 예산을 모두 썼습니다 (%d/%d)", target.key, self.budget.used(name),
                        self.budget.limit(name))
            return CycleReport(target.key, "skipped", reason="budget_exhausted")

        return self._finish(target, src_cfg, breaker, last, attempts, budget_stop)

    def _finish(self, target: Target, src_cfg: SourceConfig, breaker: CircuitBreaker, last: dict,
                attempts: int, budget_stop: bool) -> CycleReport:
        name, ctx, fr, result = target.source, last["ctx"], last["fr"], last["result"]
        outcome = last["outcome"]
        n_records = n_rejects = published = delivery_errors = 0
        truncated = False

        if outcome == "ok":
            with self._pub_lock:
                before_err = self.publisher.errors
                for rec in result.records:
                    payload = rec.payload
                    self.publisher.produce(
                        src_cfg.topic, KEY_FUNCS[name](payload).encode("utf-8"), _json(rec.to_dict()),
                        {"schema_version": rec.envelope.schema_version, "source_system": name,
                         "ingest_id": rec.envelope.ingest_id, "event_id": rec.envelope.event_id},
                        int(rec.envelope.event_time.timestamp() * 1000))
                for rj in result.rejects:
                    self.publisher.produce(
                        self.cfg.topics.dlq_topic, name.encode(),
                        _json({"source_system": name, "target": target.key, "ingest_id": ctx.ingest_id,
                               "fetched_at": ctx.fetched_at.isoformat(), "reason": rj.reason, "index": rj.index,
                               "raw_row": rj.raw, "raw_uri": ctx.raw_uri}), {}, None)
                self.publisher.flush(10.0)
                delivery_errors = self.publisher.errors - before_err
            n_records, n_rejects = len(result.records), len(result.rejects)
            published = max(0, n_records - delivery_errors)
            if result.total_count is not None and name == "seoul_incident":
                truncated = result.total_count > int(target.params.get("page_size", 200))
            breaker.record_success()
            if name == "kma_ncst" and result.status.kind == "ok":
                self.last_kma_base[target.key] = last["spec"].base
            report = CycleReport(target.key, "ok", attempts, n_records, n_rejects, published)
        else:
            # 해석 못 한 응답은 원본 위치와 함께 DLQ 로 보내 나중에 조사할 수 있게 한다
            if last["kind"] == "malformed" or (result is not None and not result.status.ok):
                with self._pub_lock:
                    self.publisher.produce(
                        self.cfg.topics.dlq_topic, name.encode(),
                        _json({"source_system": name, "target": target.key, "ingest_id": ctx.ingest_id,
                               "fetched_at": ctx.fetched_at.isoformat(), "reason": f"status:{last['kind']}",
                               "status_code": last["code"], "status_message": self.redact(last["message"]),
                               "raw_uri": ctx.raw_uri}), {}, None)
            breaker.record_failure(permanent=(outcome == "permanent"))
            report = CycleReport(target.key, "permanent_failure" if outcome == "permanent" else "failed",
                                 attempts, reason=last["kind"])

        self._audit(source_system=name, target=target.key, attempt=attempts, final=True, will_retry=False,
                    outcome=report.outcome, ingest_id=ctx.ingest_id, fetched_at=ctx.fetched_at.isoformat(),
                    http_status=fr.http_status, latency_ms=fr.latency_ms, status_kind=last["kind"],
                    status_code=last["code"], status_message=last["message"], error=fr.error,
                    raw_uri=ctx.raw_uri, n_records=n_records, n_rejects=n_rejects, published=published,
                    delivery_errors=delivery_errors, total_count=getattr(result, "total_count", None),
                    truncated=truncated, parse_stats=dict(result.stats) if result is not None else None,
                    budget_stop=budget_stop, budget_used=self.budget.used(name),
                    budget_limit=self.budget.limit(name), breaker=breaker.snapshot())
        log.info("[%s] %s attempts=%d records=%d rejects=%d published=%d kind=%s budget=%d/%d latency=%sms",
                 target.key, report.outcome, attempts, n_records, n_rejects, published, last["kind"],
                 self.budget.used(name), self.budget.limit(name), fr.latency_ms)
        if delivery_errors:
            log.error("[%s] Kafka 전달 실패 %d건: %s (원본은 %s 에 있어 재처리할 수 있습니다)", target.key,
                      delivery_errors, self.publisher.last_error, ctx.raw_uri)
        return report


# ---------------------------------------------------------------------------- 스케줄러
def in_window(src: SourceConfig, now: datetime) -> bool:
    m = now.astimezone(KST).hour * 60 + now.astimezone(KST).minute
    return src.window_start_min <= m < src.window_end_min


def next_window_start(src: SourceConfig, now: datetime) -> datetime:
    t = now.astimezone(KST)
    start = t.replace(hour=src.window_start_min // 60 % 24, minute=src.window_start_min % 60, second=0,
                      microsecond=0)
    return start if t < start else start + timedelta(days=1)


class Scheduler:
    """대상마다 다음 호출 시각을 관리한다. 처음에는 대상마다 stagger 초씩 어긋나게 시작해 한꺼번에 호출하지 않는다."""

    def __init__(self, sources: list[SourceConfig], now: datetime, stagger_sec: float = 3.0,
                 jitter: float = 0.05, rng: Optional[random.Random] = None):
        self.rng, self.jitter = rng or random.Random(), jitter
        self.items: dict[str, tuple[Target, SourceConfig]] = {}
        self.due: dict[str, datetime] = {}
        i = 0
        for src in sources:
            for t in src.targets:
                self.items[t.key] = (t, src)
                self.due[t.key] = now + timedelta(seconds=stagger_sec * i)
                i += 1

    def pop_ready(self, now: datetime) -> list[Target]:
        """지금 호출해야 할 대상(활성 시간 안). 활성 시간 밖이면 다음 시작 시각으로 미룬다."""
        ready = []
        for key, due in sorted(self.due.items(), key=lambda kv: kv[1]):
            if due > now:
                continue
            target, src = self.items[key]
            if in_window(src, now):
                ready.append(target)
            else:
                self.due[key] = next_window_start(src, now)
        return ready

    def reschedule(self, target: Target, now: datetime) -> None:
        _, src = self.items[target.key]
        step = src.interval_sec * (1 + self.rng.uniform(-self.jitter, self.jitter))
        nxt = now + timedelta(seconds=step)
        self.due[target.key] = nxt if in_window(src, nxt) else next_window_start(src, nxt)

    def seconds_until_next(self, now: datetime, exclude=()) -> float:
        """exclude: 지금 호출 중인 대상 키 (이미 due 가 지났지만 끝나야 다음 시각이 정해진다)."""
        dues = [d for k, d in self.due.items() if k not in exclude]
        return max(0.0, min((d - now).total_seconds() for d in dues)) if dues else 3600.0


def run_forever(collector: Collector, scheduler: Scheduler, stop, clock: Callable[[], datetime]) -> None:
    """stop: threading.Event. 신호를 받으면 현재 사이클을 마치고 끝낸다."""
    while not stop.is_set():
        for target in scheduler.pop_ready(clock()):
            if stop.is_set():
                break
            why = collector.should_skip(target, clock())
            if why:
                log.debug("[%s] 건너뜀: %s", target.key, why)
            else:
                try:
                    collector.run_target(target)
                except Exception:  # 한 대상의 예기치 못한 오류가 전체 수집을 멈추지 않게 한다
                    log.exception("[%s] 예기치 못한 오류", target.key)
            scheduler.reschedule(target, clock())
        stop.wait(min(max(scheduler.seconds_until_next(clock()), 0.2), 5.0))
    collector.publisher.flush(10.0)


def run_forever_parallel(collector: Collector, scheduler: Scheduler, stop, clock: Callable[[], datetime]) -> None:
    """소스마다 작업 스레드 1개. 한 소스가 느려도(26초 사례) 다른 소스의 간격이 밀리지 않는다.

    - 같은 소스의 호출은 겹치지 않는다 (소스당 스레드 1개, 호출 중인 대상은 다시 제출하지 않는다).
    - 다음 호출 시각은 호출이 '끝난 뒤' 정한다 (순차 실행 때와 같은 규칙: 끝난 시각 + 간격).
    - 종료 신호를 받으면 새 호출을 내지 않고, 진행 중인 사이클을 마친 뒤 flush 한다.
    """
    lock = threading.Lock()
    wake = threading.Event()          # 호출이 끝나면 메인 루프를 바로 깨워 다음 시각을 다시 계산한다
    in_flight: set[str] = set()
    pools: dict[str, ThreadPoolExecutor] = {}

    def job(target: Target) -> None:
        try:
            why = collector.should_skip(target, clock())
            if why:
                log.debug("[%s] 건너뜀: %s", target.key, why)
            else:
                collector.run_target(target)
        except Exception:  # 한 대상의 예기치 못한 오류가 전체 수집을 멈추지 않게 한다
            log.exception("[%s] 예기치 못한 오류", target.key)
        finally:
            with lock:
                scheduler.reschedule(target, clock())
                in_flight.discard(target.key)
            wake.set()

    try:
        while not stop.is_set():
            with lock:
                ready = [t for t in scheduler.pop_ready(clock()) if t.key not in in_flight]
                in_flight.update(t.key for t in ready)
                wait = scheduler.seconds_until_next(clock(), exclude=in_flight)
            for t in ready:
                pool = pools.setdefault(t.source, ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"src-{t.source}"))
                pool.submit(job, t)
            # 종료 신호(stop)를 빨리 보려고 최대 0.5초마다 깬다. 호출 시각 자체는 wait 으로 정확히 맞춘다.
            wake.wait(min(max(wait, 0.02), 0.5))
            wake.clear()
    finally:
        for pool in pools.values():
            pool.shutdown(wait=True)
        collector.publisher.flush(10.0)
