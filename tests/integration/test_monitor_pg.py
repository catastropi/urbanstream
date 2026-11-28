"""Step 8a: Monitor.tick 을 실제 Postgres + 가짜 Kafka + 고정 시계로 돌려 본다 (알림 전송은 가짜)."""
from __future__ import annotations

import dataclasses
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from pg_support import pg  # noqa: F401
from urbanstream.monitor import loop as LP
from urbanstream.monitor.config import load
from urbanstream.monitor.kafkatail import KafkaUnavailable
from urbanstream.notify import Notifier
from urbanstream.notify.policy import load_policy
from urbanstream.notify.store import PgStore
from urbanstream.notify.transport import SendResult

pytestmark = pytest.mark.pg
NOW = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)      # 12:00 KST — 모든 소스 활성 창 안
URL = "https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz0123456789"
SOURCES = ["seoul_traffic", "seoul_incident", "bus_position", "bus_arrival", "kma_ncst"]


class Clock:
    def __init__(self): self.t = NOW
    def __call__(self): return self.t
    def adv(self, sec): self.t += timedelta(seconds=sec)


class FakeTail:
    def __init__(self): self.msgs, self.down, self.reads = [], False, 0
    def add(self, **kw):
        self.msgs.append(kw)
    def read(self, offsets, *, max_messages, start_hours_back, now):
        self.reads += 1
        if self.down:
            raise KafkaUnavailable("down")
        start = int(offsets.get("0", 0))
        return [(0, i, json.dumps(m).encode()) for i, m in enumerate(self.msgs) if i >= start][:max_messages]
    def close(self): pass


def call(clk, src, ago=5, outcome="ok", **kw):
    d = {"event": "call", "recorded_at": (clk.t - timedelta(seconds=ago)).isoformat(), "source_system": src, "target": f"{src}:1",
         "final": True, "outcome": outcome, "attempt": 1, "latency_ms": 200, "n_records": 5}
    d.update(kw)
    return d


class Env:
    def __init__(self, pg, tmp_path):
        self.pg, self.clk, self.tail, self.posts = pg, Clock(), FakeTail(), []
        cfg = load()
        lay = {**cfg.layers, "bronze_root": str(tmp_path / "bronze"), "silver_root": str(tmp_path / "silver")}
        self.cfg = dataclasses.replace(cfg, layers=lay)
        self.root = tmp_path
        policy = load_policy()
        def post(kind, url, text, **kw):
            self.posts.append(text)
            return SendResult(True, 204, "")
        class C:
            def __enter__(s): return pg
            def __exit__(s, *a): return False
        self.notifier = Notifier(PgStore("", connect=lambda: C()), policy, enabled=True, kind="discord", url=URL, clock=self.clk, post=post)
        self.mon = LP.Monitor(self.cfg, policy, self.notifier, lambda: C(), self.tail, clock=self.clk)

    def files(self, fresh=True):
        ts = self.clk.t.timestamp() - (5 if fresh else 99999)
        for layer in ("bronze", "silver"):
            for table in self.cfg.layers[layer]:
                d = self.root / layer / table / "dt=2026-11-05"
                d.mkdir(parents=True, exist_ok=True)
                f = d / "p.parquet"
                f.write_text("x")
                os.utime(f, (ts, ts))

    def healthy_calls(self):
        for s in SOURCES:
            self.tail.add(**call(self.clk, s))

    def tick(self, adv=60):
        self.clk.adv(adv)
        return self.mon.tick()

    def status(self, src):
        return self.pg.execute("SELECT status, reason FROM ops.source_status WHERE source_system=%s", (src,)).fetchone()


@pytest.fixture
def env(pg, tmp_path):
    return Env(pg, tmp_path)


def test_empty_start_is_unknown_and_silent(env):
    r = env.tick(0)
    assert {s.status for s in r.statuses.values()} == {"unknown"} and env.posts == []
    assert env.pg.execute("SELECT count(*) FROM ops.call_event").fetchone()[0] == 0


def test_healthy_ingest_is_idempotent_and_no_alerts(env):
    env.files(); env.healthy_calls()
    r = env.tick(0)
    assert r.ingested == 5 and {s.status for s in r.statuses.values()} == {"ok"} and env.posts == []
    env.tail.msgs.append(call(env.clk, "seoul_traffic"))
    assert env.tick(10).ingested == 1                                         # 이어 읽기 (offset 저장)
    env.pg.execute("UPDATE ops.monitor_state SET state = state - 'offsets'")      # offset 을 잃어도 PK 가 중복을 막는다
    env.tick(10)
    assert env.pg.execute("SELECT count(*) FROM ops.call_event").fetchone()[0] == 6
    assert env.pg.execute("SELECT count(*) FROM ops.source_health WHERE n_calls > 0").fetchone()[0] >= 5
    assert {r[0] for r in env.pg.execute("SELECT status FROM ops.layer_status")} == {"ok"}


def test_source_down_alerts_once_then_resolves(env):
    env.files()
    for s in SOURCES:
        env.tail.add(**call(env.clk, s, ago=5))
    env.tail.add(**call(env.clk, "seoul_traffic", ago=1500))      # 오래된 것은 마지막 성공을 바꾸지 않는다(최신이 5초 전)
    env.tick(0)
    # 이후 traffic 만 40분 침묵(down 기준 5 × 450 = 2250초), 다른 소스는 계속 호출
    for step in range(40):
        env.clk.adv(60)
        for s in SOURCES:
            if s != "seoul_traffic":
                env.tail.add(**call(env.clk, s, ago=1))
        env.files()
        env.mon.tick()
    assert env.status("seoul_traffic")[0] == "down"
    pushed = [p for p in env.posts if "seoul_traffic" in p]
    assert len(pushed) == 1 and "[ERROR]" in pushed[0]
    env.tail.add(**call(env.clk, "seoul_traffic", ago=1))
    env.tick(30)
    assert env.status("seoul_traffic")[0] == "ok" and any("해소" in p for p in env.posts)


def test_host_stall_downgrades_dag_failure_and_groups(env):
    env.files(); env.healthy_calls(); env.tick(0)
    env.tail.add(event="host_resume", recorded_at=env.clk.t.isoformat(), source_system="collector", slept_sec=3000.0)
    for i in range(3):
        env.notifier.enqueue("error", f"dag{i} 실패", "", f"dag_fail:dag{i}",
                             meta={"dag_id": f"dag{i}", "ended_at": (env.clk.t - timedelta(seconds=30)).isoformat()})
    env.healthy_calls()
    env.tick(61)
    assert env.pg.execute("SELECT kind, duration_sec FROM ops.data_gap").fetchall() == [("host_stall", 3000)]
    rows = env.pg.execute("SELECT level, original_level, status FROM ops.alert_event WHERE dedup_key LIKE 'host_stall:%' OR reason LIKE 'grouped%' ORDER BY alert_id").fetchall()
    assert ("warning", "error", "digest") in rows and sum(1 for r in rows if r[2] == "suppressed") == 2
    assert not any("dag0" in p and "[ERROR]" in p for p in env.posts)


def test_dag_failure_without_evidence_stays_error(env):
    env.files(); env.healthy_calls(); env.tick(0)
    env.notifier.enqueue("error", "gold_daily 실패", "", "dag_fail:gold_daily", meta={"dag_id": "gold_daily"})
    env.healthy_calls()
    env.tick(61)
    assert any("gold_daily" in p and "[ERROR]" in p for p in env.posts)


def test_blackout_suppresses_per_source_alerts_and_records_gap_after(env):
    env.files(); env.healthy_calls(); env.tick(0)
    r = env.tick(900)                                          # 15분 침묵, 호스트 알림 없음
    assert r.blackout_sec and r.blackout_sec >= 600
    assert any("조용" in p for p in env.posts) and not any("소스 seoul_" in p for p in env.posts)
    env.tail.add(**call(env.clk, "seoul_traffic", ago=1))
    env.tick(30)
    assert env.pg.execute("SELECT kind FROM ops.data_gap").fetchall() == [("collector_blackout",)]
    assert any("재개" in p for p in env.posts)


def test_resume_event_prevents_false_blackout_right_after_wake(env):
    env.files(); env.healthy_calls(); env.tick(0)
    env.clk.adv(3600)
    env.tail.add(event="host_resume", recorded_at=(env.clk.t - timedelta(seconds=2)).isoformat(), source_system="collector", slept_sec=3590.0)
    r = env.mon.tick()
    assert r.blackout_sec is None
    assert not any("조용" in p for p in env.posts)


def test_kafka_down_three_ticks_then_critical_and_recovery(env):
    env.files(); env.healthy_calls(); env.tick(0)
    env.tail.down = True
    for _ in range(2):
        env.tick(10)
    assert env.posts == []
    r = env.tick(10)
    assert r.kafka_down and any("[CRITICAL]" in p and "Kafka" in p for p in env.posts)
    env.tail.down = False
    env.tick(10)
    assert any("회복" in p for p in env.posts)


def test_layer_stale_alerts_only_when_source_ok(env):
    env.files(fresh=False); env.healthy_calls()
    env.tick(0)
    assert any("적재 정체" in p for p in env.posts)
    n = len(env.posts)
    env.tick(60)
    assert len(env.posts) == n                                  # 쿨다운: 같은 정체를 다시 울리지 않는다
    env.posts.clear()


def test_no_layer_stall_alert_right_after_a_collection_window_opens(env):
    """Mac 04:00 오경보(D-141): 버스 창이 막 열렸고 소스는 ok, 레이어 마지막 파일은 어제 것 → 알리면 안 된다. 창이 열린 지 임계값이 지나도 안 쌓이면 그때는 알린다."""
    env.clk.t = datetime(2026, 11, 4, 19, 1, tzinfo=timezone.utc)       # 04:01 KST, 버스 창(04:00~) 방금 열림
    env.files(fresh=False); env.healthy_calls()
    env.tick(0)
    bus = [p for p in env.posts if "적재 정체" in p and "bus_" in p]
    assert bus == []
    for _ in range(25):                                                 # 1분씩 25분(소스는 계속 정상; 한 번에 건너뛰면 감시 공백으로 보고 유예에 들어간다)
        env.clk.adv(60); env.healthy_calls(); env.tick(0)               # 임계값(1050초) 지남 → 진짜 정체
    assert any("적재 정체" in p and "bus_" in p for p in env.posts)


def test_digest_once_per_day_at_0730_kst(env):
    env.files(); env.healthy_calls()
    env.clk.t = datetime(2026, 11, 4, 22, 20, tzinfo=timezone.utc)      # 07:20 KST
    env.tick(0)
    assert not any("아침 요약" in p for p in env.posts)
    r = env.tick(11 * 60)                                       # 07:31
    assert r.digest_sent and any("아침 요약" in p for p in env.posts)
    n = len(env.posts)
    env.tick(60)
    assert len(env.posts) == n                                  # 같은 날 두 번째는 없다
