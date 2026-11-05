"""프로세스 안 TTL 캐시 (D9-9). 성공한 결과만 저장한다(예외는 build 밖으로 나가 저장되지 않는다).
같은 키를 동시에 만들면 중복 계산이 생길 수 있다(DB 풀 크기가 상한이라 허용)."""
from __future__ import annotations

import threading
import time
from typing import Any, Callable


class TTLCache:
    def __init__(self, max_entries: int = 128, clock: Callable[[], float] = time.monotonic):
        self.max_entries = max_entries
        self._clock = clock
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_build(self, key: str, ttl: float, build: Callable[[], Any], store_if: Callable[[Any], bool] = lambda v: True) -> Any:
        if ttl <= 0:
            return build()
        now = self._clock()
        with self._lock:
            hit = self._data.get(key)
            if hit and hit[0] > now:
                return hit[1]
        value = build()
        if not store_if(value):          # 예: 행이 아주 많은 응답은 저장하지 않는다(메모리 보호)
            return value
        with self._lock:
            if len(self._data) >= self.max_entries:
                for k in [k for k, (exp, _) in self._data.items() if exp <= now]:
                    del self._data[k]
                while len(self._data) >= self.max_entries:
                    del self._data[next(iter(self._data))]
            self._data[key] = (self._clock() + ttl, value)
        return value

    def __len__(self) -> int:
        return len(self._data)
