"""재시도, 회로차단, 일일 호출 예산. (원칙: 외부 API 는 장애가 난다고 전제한다)

- 재시도: 일시적 실패(네트워크, 5xx, 429, 깨진 응답)만 지수 백오프로 다시 시도한다. 인증/인자 오류는 다시 해도 소용없으므로 즉시 멈춘다.
- 회로차단: 연속 실패하면 한동안 호출을 멈춰 API 에도 우리에게도 부담을 주지 않는다.
- 예산: 호출(재시도 포함)마다 하나씩 소비하고, 파일에 저장해서 재시작해도 하루 한도가 초기화되지 않는다.
"""
from __future__ import annotations

import json
import os
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from urbanstream.contracts.common import KST

from .config import BreakerConfig, RetryConfig

PERMANENT_KINDS = frozenset({"auth", "param", "no_service"})
RETRYABLE_KINDS = frozenset({"server_error", "rate_limited", "http_error", "malformed", "unknown",
                             "network_error", "timeout"})


def classify(kind: str) -> str:
    """ApiStatus.kind(또는 network_error/timeout) -> 'ok' | 'retryable' | 'permanent'."""
    if kind in ("ok", "empty"):
        return "ok"
    if kind in PERMANENT_KINDS:
        return "permanent"
    return "retryable"  # 처음 보는 종류도 재시도 대상 (시도 횟수가 제한돼 있다)


class RetryPolicy:
    def __init__(self, cfg: RetryConfig, rng: Optional[random.Random] = None):
        self.cfg = cfg
        self.rng = rng or random.Random()

    def delay(self, attempts_done: int) -> float:
        """방금 attempts_done 번째 시도가 실패했을 때 기다릴 초. 1s, 2s, 4s ... (최대 max) + 최대 25% 지터."""
        base = min(self.cfg.max_delay_sec, self.cfg.base_delay_sec * (2 ** (attempts_done - 1)))
        return base + self.rng.uniform(0, base * 0.25)


class CircuitBreaker:
    """closed -> (연속 실패 threshold 번 또는 영구 실패) -> open -> (cooldown 경과) -> half_open(시험 1회) -> closed/open"""

    def __init__(self, cfg: BreakerConfig, clock: Callable[[], datetime]):
        self.cfg, self.clock = cfg, clock
        self.state = "closed"
        self.failures = 0
        self.opened_until: Optional[datetime] = None

    def allow(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if self.clock() >= self.opened_until:
                self.state = "half_open"
                return True
            return False
        return True  # half_open: 호출자가 순차 실행이라 시험 호출 1회가 곧 다음 호출이다

    def record_success(self) -> None:
        self.state, self.failures, self.opened_until = "closed", 0, None

    def record_failure(self, permanent: bool = False) -> None:
        self.failures += 1
        if permanent or self.state == "half_open" or self.failures >= self.cfg.failure_threshold:
            cooldown = self.cfg.permanent_cooldown_sec if permanent else self.cfg.cooldown_sec
            self.state = "open"
            self.opened_until = self.clock() + timedelta(seconds=cooldown)

    def snapshot(self) -> dict:
        return {"state": self.state, "failures": self.failures,
                "opened_until": self.opened_until.isoformat() if self.opened_until else None}


class DailyBudget:
    """소스별 하루 호출 수. KST 날짜가 바뀌면 0 으로 돌아간다. 상태는 JSON 파일에 즉시 저장한다."""

    def __init__(self, path: Path, limits: dict[str, int], clock: Callable[[], datetime]):
        self.path, self.limits, self.clock = Path(path), dict(limits), clock
        self.date = self._today()
        self.calls: dict[str, int] = {}
        self.recovered_from_corruption = False
        self._lock = threading.RLock()   # 소스별 스레드가 함께 쓴다 (읽기-수정-쓰기와 임시 파일 교체를 보호)
        self._load()

    def _today(self) -> str:
        return self.clock().astimezone(KST).strftime("%Y-%m-%d")

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("date") == self.date:
                self.calls = {k: int(v) for k, v in data.get("calls", {}).items()}
        except (ValueError, OSError, AttributeError):
            self.recovered_from_corruption = True  # 상태를 읽지 못하면 0 부터 센다 (로그로 알린다)

    def _roll(self) -> None:
        today = self._today()
        if today != self.date:
            self.date, self.calls = today, {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"date": self.date, "calls": self.calls}, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)

    def used(self, source: str) -> int:
        with self._lock:
            self._roll()
            return self.calls.get(source, 0)

    def limit(self, source: str) -> int:
        return self.limits[source]

    def try_consume(self, source: str) -> bool:
        with self._lock:
            self._roll()
            if self.calls.get(source, 0) >= self.limits[source]:
                return False
            self.calls[source] = self.calls.get(source, 0) + 1
            self._save()
            return True

    def report(self) -> dict[str, dict]:
        with self._lock:
            self._roll()
            return {s: {"used": self.calls.get(s, 0), "limit": lim,
                        "pct": round(100 * self.calls.get(s, 0) / lim, 1) if lim else None}
                    for s, lim in self.limits.items()}
