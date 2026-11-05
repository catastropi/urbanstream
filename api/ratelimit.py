"""키별 토큰 버킷 (D9-9). 워커가 1개라 프로세스 안 카운터로 충분하다. 인증을 통과한 키만 센다 → 버킷 수는 키 수로 한정된다."""
from __future__ import annotations

import math
import threading
import time
from typing import Callable


class RateLimiter:
    def __init__(self, per_min: int, burst: int, clock: Callable[[], float] = time.monotonic):
        self.rate = per_min / 60.0
        self.burst = float(burst)
        self._clock = clock
        self._state: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, key: str) -> int:
        """허용이면 0, 아니면 다시 시도할 때까지 걸리는 초(올림)."""
        now = self._clock()
        with self._lock:
            tokens, last = self._state.get(key, (self.burst, now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            if tokens >= 1.0:
                self._state[key] = (tokens - 1.0, now)
                return 0
            self._state[key] = (tokens, now)
            return max(1, math.ceil((1.0 - tokens) / self.rate))
