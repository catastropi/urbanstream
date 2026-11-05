"""python -m urbanstream.monitor — 상시 감시 프로세스 (compose 서비스 `monitor`)."""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from pathlib import Path

from .. import notify
from ..notify.policy import load_policy
from ..notify.store import PgStore, conninfo
from ..notify.transport import config_from_env, post_webhook
from .config import load
from .kafkatail import KafkaTail
from .loop import Monitor, run


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger("urbanstream.monitor")
    dsn = conninfo()
    if not dsn:
        log.error("GOLD_DSN 또는 GOLD_WRITER_USER 가 없습니다 — 감시는 ops 표에 써야 하므로 시작할 수 없습니다.")
        return 2
    cfg, policy = load(), load_policy()
    enabled, kind, url = config_from_env()
    if not (enabled and url):
        log.warning("ALERT_WEBHOOK_URL 이 없거나 ALERTS_ENABLED=false — 알림은 ops.alert_event 에만 기록됩니다(status=dropped).")
    store = PgStore(dsn)
    notifier = notify.Notifier(store, policy, enabled=enabled, kind=kind, url=url)
    notify.set_notifier(notifier)

    def connect():
        import psycopg
        return psycopg.connect(dsn, autocommit=True, connect_timeout=10)

    tail = KafkaTail(os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092"), str(cfg.kafka.get("topic", "platform.audit")),
                     poll_timeout_sec=float(cfg.kafka.get("poll_timeout_sec", 2.0)))
    mon = Monitor(cfg, policy, notifier, connect, tail, alive_path=Path(os.environ.get("MONITOR_ALIVE", "/tmp/monitor.alive")))
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    def emergency(text: str) -> None:
        if enabled and url:
            post_webhook(kind, url, f"[CRITICAL] {text}", scrub=notify.mask)

    log.info("감시 시작: 소스 %s, 주기 %ss", ",".join(cfg.sources), cfg.tick_sec)
    try:
        run(mon, stop, emergency=emergency)
    finally:
        tail.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
