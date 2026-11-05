"""감시 루프 1주기(tick)와 상시 실행(run). 입출력은 모두 주입받는다 → 시험에서는 가짜 Kafka·고정 시계로 한 주기씩 돌려 본다.

tick 순서 (ADR-0010 D8-2~D8-5)
 1. Kafka 감사 토픽 → ops.call_event            (읽기 실패가 3번 이어지면 critical, 회복하면 해소)
 2. host_resume → host_stall 구간, 호출 침묵 → collector_blackout 구간 기록
 3. 소스 상태 계산 → ops.source_status → 상태 알림(sync). 침묵 중이거나 Kafka 를 못 읽는 동안은 소스별 알림을 내지 않는다(폭주 방지)
 4. Bronze/Silver 정체 판정 (원천 소스가 ok 일 때만 알림)
 5. DAG 실패(pending) 분류 → 전송
 6. 아침 요약(07:30 KST, 하루 1번)
 7. 박동 기록
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from ..notify.core import Notifier
from ..notify.policy import Policy
from . import audit, db, gaps as G, health as H, layers as L
from .classify import make_classifier
from .config import MonitorConfig
from .kafkatail import KafkaTail, KafkaUnavailable

LOG = logging.getLogger("urbanstream.monitor")
KST = ZoneInfo("Asia/Seoul")
KAFKA_FAIL_LIMIT = 3


@dataclass
class TickResult:
    ingested: int = 0
    statuses: dict = field(default_factory=dict)
    layers: dict = field(default_factory=dict)
    kafka_down: bool = False
    blackout_sec: Optional[float] = None
    pending_processed: int = 0
    digest_sent: bool = False


class Monitor:
    def __init__(self, cfg: MonitorConfig, policy: Policy, notifier: Notifier, connect: Callable[[], Any], tail: KafkaTail,
                 *, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc), alive_path: Optional[Path] = None) -> None:
        self.cfg, self.policy, self.n, self.connect, self.tail, self.clock = cfg, policy, notifier, connect, tail, clock
        self.alive_path = alive_path
        self.kafka_fail = 0

    # ------------------------------------------------------------------ 1. 수집
    def _ingest(self, conn, st: dict, now: datetime) -> tuple[int, bool]:
        k = self.cfg.kafka
        try:
            msgs = self.tail.read(st.get("offsets") or {}, max_messages=int(k.get("max_messages_per_tick", 20000)),
                                  start_hours_back=float(k.get("start_hours_back", 168)), now=now)
        except KafkaUnavailable as e:
            self.kafka_fail += 1
            LOG.warning("Kafka 읽기 실패 %d회째: %s", self.kafka_fail, e)
            if self.kafka_fail >= KAFKA_FAIL_LIMIT:
                self.n.sync("monitor:kafka", "critical", "감시: Kafka 감사 토픽을 읽지 못합니다", "원인 확인: docker compose ps kafka / logs kafka")
            return 0, self.kafka_fail >= KAFKA_FAIL_LIMIT
        self.kafka_fail = 0
        self.n.sync("monitor:kafka", None, "감시: Kafka 감사 토픽 읽기 회복")
        rows, offs = [], dict(st.get("offsets") or {})
        for part, off, val in msgs:
            offs[str(part)] = max(int(offs.get(str(part), 0)), off + 1)
            r = audit.parse(val, part, off)
            if r is not None:
                rows.append(r)
        db.insert_call_events(conn, rows)
        st["offsets"] = offs                                   # 적재가 끝난 뒤에만 전진 → 중간에 죽으면 다시 읽고, PK 가 중복을 막는다
        return len(rows), False

    # ------------------------------------------------------------------ 2. 공백
    def _gaps(self, conn, st: dict, now: datetime) -> tuple[list[G.Gap], Optional[float]]:
        sp = self.policy.stall
        stalls = G.host_stalls(db.resumes_since(conn, now - timedelta(days=2)), sp.min_slept_sec)
        for g in stalls:
            db.insert_gap(conn, g, now)
        cursor = datetime.fromisoformat(st["blackout_cursor"]) if st.get("blackout_cursor") else now - timedelta(days=2)
        times = db.call_times(conn, cursor)
        known = db.recent_gaps(conn, now - timedelta(days=2))
        for g in G.blackouts(times, [x for x in known if x.kind == "host_stall"], sp.blackout_min_sec, sp.overlap_margin_sec):
            db.insert_gap(conn, g, now)
        if times:
            st["blackout_cursor"] = times[-1].isoformat()
        alive = db.last_alive_time(conn)
        ongoing = G.ongoing_blackout(alive, now, sp.blackout_min_sec)
        return db.recent_gaps(conn, now - timedelta(days=2)), ongoing

    # ------------------------------------------------------------------ 3. 소스
    def _sources(self, conn, now: datetime, known: list[G.Gap], silent: bool) -> dict[str, H.StatusRow]:
        sums = db.source_summaries(conn, now)
        grace = timedelta(seconds=self.policy.stall.post_gap_grace_sec)
        in_grace = any(g.end <= now + grace and now - g.end < grace for g in known)
        out = {}
        for name, sc in self.cfg.sources.items():
            row = H.source_status(sc, sums.get(name, H.Summary()), now, self.cfg, in_grace=in_grace)
            out[name] = row
            db.upsert_source_status(conn, row, now)
            if silent:
                continue
            key = f"source:{name}"
            lvl = H.ALERT_LEVEL.get(row.status)
            if lvl:
                self.n.sync(key, lvl, f"소스 {name} {row.status}", f"{row.reason} (최근 성공 {row.last_ok_at}, 호출 {row.calls_1h}회/실패 {row.fail_1h}회)")
            elif row.status in ("ok", "idle"):
                self.n.sync(key, None, f"소스 {name} 정상")
        return out

    # ------------------------------------------------------------------ 4. 레이어
    def _layers(self, conn, now: datetime, src: dict[str, H.StatusRow], silent: bool) -> dict:
        lc = self.cfg.layers
        margin = float(lc.get("margin_sec", 600))
        out = {}
        for layer, root_key in (("bronze", "bronze_root"), ("silver", "silver_root")):
            root = Path(lc.get(root_key, f"/data/{layer}"))
            for table, source in (lc.get(layer) or {}).items():
                sc, sr = self.cfg.sources.get(source), src.get(source)
                if sc is None or sr is None:
                    continue
                gap = sr.expected_gap_sec
                thr = self.cfg.f("down_factor", 5.0) * gap + margin
                full_day = tuple(sc.window) == (0, 1440)
                row = L.layer_status(layer, table, source, L.newest_mtime(root / table), now,
                                     source_ok=sr.status == "ok", source_in_window=sc.in_window(now), threshold_sec=thr,
                                     window_start=None if full_day else sc.window_start(now))
                out[(layer, table)] = row
                db.upsert_layer_status(conn, row, now)
                if silent:
                    continue
                key = f"layer:{layer}:{table}"
                if row.status == "down":
                    self.n.sync(key, "error", f"{layer} {table} 적재 정체", f"{row.reason} — 소스 {source} 는 정상입니다. Spark 컨테이너 확인: docker compose logs --tail 60 spark-{layer}")
                elif row.status in ("ok", "idle"):
                    self.n.sync(key, None, f"{layer} {table} 적재 정상")
        return out

    # ------------------------------------------------------------------ 한 주기
    def tick(self) -> TickResult:
        now = self.clock()
        res = TickResult()
        with self.connect() as conn:
            st = db.get_state(conn, "monitor")
            res.ingested, res.kafka_down = self._ingest(conn, st, now)
            known, ongoing = self._gaps(conn, st, now)
            res.blackout_sec = ongoing
            if ongoing is not None:
                self.n.sync("collector_blackout", "error", "수집기가 조용합니다", f"{int(ongoing)}초 동안 호출 기록이 없습니다 (잠자기·중단 가능). 수집 공백은 복구되지 않습니다.")
            else:
                self.n.sync("collector_blackout", None, "수집기 호출 재개")
            for g in known:                                    # 장시간 잠자기는 상태가 아니라 사건 — 끝난 뒤 한 번만 error
                if g.kind == "host_stall" and g.duration_sec >= self.policy.stall.long_stall_sec and now - g.end < timedelta(hours=2):
                    self.n.emit("error", f"호스트가 {g.duration_sec // 3600}시간 이상 정지했습니다", f"{g.start} ~ {g.end} 수집 공백(복구 불가)",
                                f"host_stall_long:{g.start.astimezone(KST).date()}")
            silent = res.kafka_down or ongoing is not None
            res.statuses = self._sources(conn, now, known, silent)
            res.layers = self._layers(conn, now, res.statuses, silent)
            db.rollup_source_health(conn, now - timedelta(hours=2), now)
            res.pending_processed = self.n.process_pending(make_classifier(known, margin_sec=self.policy.stall.overlap_margin_sec))
            res.digest_sent = self._maybe_digest(st, now, res)
            db.save_state(conn, "monitor", st, now)
        if self.alive_path:
            try:
                self.alive_path.write_text(str(time.time()))
            except OSError:
                pass
        return res

    # ------------------------------------------------------------------ 6. 아침 요약
    def _maybe_digest(self, st: dict, now: datetime, res: TickResult) -> bool:
        k = now.astimezone(KST)
        h, m = (int(x) for x in self.policy.digest_time_kst.split(":"))
        today = k.date().isoformat()
        if "digest_date" not in st:                  # 처음 켠 날: 요약 시각이 이미 지났으면 오늘은 건너뛴다 (재설치 때 알림 폭탄 방지)
            st["digest_date"] = today if (k.hour, k.minute) >= (h, m) else (k.date() - timedelta(days=1)).isoformat()
            return False
        if (k.hour, k.minute) < (h, m) or st["digest_date"] == today:
            return False
        bad = [f"{n}={r.status}" for n, r in res.statuses.items() if r.status not in ("ok", "idle")]
        extra = (f"소스 이상: {', '.join(bad)}",) if bad else ("소스 전부 정상",)
        self.n.send_digest(extra)
        st["digest_date"] = today
        return True


def run(mon: Monitor, stop, *, emergency: Optional[Callable[[str], None]] = None) -> None:
    """상시 실행. 한 주기가 예외로 실패해도 멈추지 않는다. DB 접근 실패가 이어지면 DB 를 거치지 않고 웹훅으로 직접 알린다."""
    fails, last_emergency = 0, 0.0
    while not stop.is_set():
        t0 = time.monotonic()
        try:
            mon.tick()
            fails = 0
        except Exception as e:                                               # noqa: BLE001
            fails += 1
            LOG.exception("감시 주기 실패 %d회째", fails)
            if fails >= 3 and emergency and time.monotonic() - last_emergency > 3600:
                last_emergency = time.monotonic()
                try:
                    emergency(f"감시 루프가 {fails}회 연속 실패했습니다: {type(e).__name__}")
                except Exception:                                            # noqa: BLE001
                    LOG.exception("긴급 알림 실패")
        stop.wait(max(1.0, mon.cfg.tick_sec - (time.monotonic() - t0)))
