"""알림 (Step 7 스텁 → Step 8 에서 웹훅 + ops.alert_event 로 확장).

**호출 모양(`send`)은 Step 7 에서 고정**했다. 로그 형식 `[notify:<level>] title | body | dedup=<key>` 도 그대로다(기존 시험이 본다).
Step 8 은 내부만 바꿨다: 로그를 남긴 뒤 Notifier 로 넘긴다 (error·critical → 웹훅 푸시, warning·info → 아침 요약).

- 어떤 경우에도 예외를 밖으로 던지지 않는다. 알림 실패가 DAG·모니터를 죽이면 안 된다 (잘못된 level 만 ValueError).
- 이 모듈은 psycopg·pyarrow·airflow·pyspark 를 import 하지 않는다 (필요할 때 함수 안에서 지연 import).
- DB 접속 정보(GOLD_DSN / GOLD_WRITER_USER)가 없으면 프로세스 안 메모리 저장소를 쓴다 → 호스트 단위시험은 DB·네트워크 없이 돈다.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from ._mask import mask
from .core import Decision, Notifier
from .policy import load_policy
from .store import MemoryStore, PgStore, conninfo
from .transport import config_from_env

__all__ = ["LEVELS", "mask", "send", "enqueue", "resolve", "sync", "get_notifier", "set_notifier", "Decision", "Notifier"]

_LOG = logging.getLogger("urbanstream.notify")
LEVELS = {"info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR, "critical": logging.CRITICAL}

_NOTIFIER: Optional[Notifier] = None


def get_notifier() -> Notifier:
    global _NOTIFIER
    if _NOTIFIER is None:
        enabled, kind, url = config_from_env()
        dsn = conninfo()
        store: Any = PgStore(dsn) if dsn else MemoryStore()
        _NOTIFIER = Notifier(store, load_policy(), enabled=enabled, kind=kind, url=url)
    return _NOTIFIER


def set_notifier(n: Optional[Notifier]) -> None:
    """시험·모니터가 주입한다. None 이면 다음 호출 때 환경에서 다시 만든다."""
    global _NOTIFIER
    _NOTIFIER = n


def _check(level: str) -> None:
    if level not in LEVELS:
        raise ValueError(f"level 은 {sorted(LEVELS)} 중 하나여야 합니다: {level!r}")


def send(level: str, title: str, body: str = "", *, dedup_key: str | None = None) -> bool:
    """알림 1건. 반환값 True = 기록됨(로그 + 저장소). 웹훅 전달 성공 여부는 ops.alert_event.status 가 말한다."""
    _check(level)
    _LOG.log(LEVELS[level], "[notify:%s] %s | %s | dedup=%s", level, mask(title), mask(body), dedup_key)
    try:
        get_notifier().emit(level, title, body, dedup_key)
    except Exception as e:                                                   # noqa: BLE001 - 알림 실패는 호출자를 죽이지 않는다
        _LOG.warning("알림 저장/전송 실패(무시): %s", mask(f"{type(e).__name__}: {e}"))
    return True


def enqueue(level: str, title: str, body: str = "", *, dedup_key: str | None = None,
            meta: Optional[Mapping[str, Any]] = None) -> bool:
    """DAG 실패 콜백용. pending 으로만 기록하고 분류·전송은 모니터(process_pending)가 한다. 실패하면 False (호출자가 send 로 대체)."""
    _check(level)
    _LOG.log(LEVELS[level], "[notify:%s] %s | %s | dedup=%s", level, mask(title), mask(body), dedup_key)
    try:
        get_notifier().enqueue(level, title, body, dedup_key, meta=meta)
        return True
    except Exception as e:                                                   # noqa: BLE001
        _LOG.warning("pending 기록 실패: %s", mask(f"{type(e).__name__}: {e}"))
        return False


def sync(dedup_key: str, level: str | None, title: str, body: str = "") -> None:
    """상태형 알림. level=None → 해소."""
    if level is not None:
        _check(level)
    try:
        get_notifier().sync(dedup_key, level, title, body)
    except Exception as e:                                                   # noqa: BLE001
        _LOG.warning("알림 sync 실패(무시): %s", mask(f"{type(e).__name__}: {e}"))


def resolve(dedup_key: str, title: str = "", body: str = "") -> bool:
    try:
        return get_notifier().resolve(dedup_key, title, body)
    except Exception as e:                                                   # noqa: BLE001
        _LOG.warning("알림 resolve 실패(무시): %s", mask(f"{type(e).__name__}: {e}"))
        return False
