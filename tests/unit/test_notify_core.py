"""Notifier 규칙 시험 (저장소·전송·시계 주입, DB·네트워크 없음)."""
from datetime import datetime, timedelta, timezone

import pytest

from urbanstream.notify import Decision, Notifier, mask
from urbanstream.notify.policy import from_mapping
from urbanstream.notify.store import MemoryStore
from urbanstream.notify.transport import SendResult

T0 = datetime(2026, 11, 5, 3, 0, tzinfo=timezone.utc)
URL = "https://discord.com/api/webhooks/123456789/abcdefghijklmnopqrstuvwxyz0123456789ABCDEF"


class Clock:
    def __init__(self):
        self.t = T0

    def __call__(self):
        return self.t

    def adv(self, sec):
        self.t += timedelta(seconds=sec)


class Post:
    def __init__(self, ok=True):
        self.calls, self.ok = [], ok

    def __call__(self, kind, url, text, **kw):
        self.calls.append(text)
        return SendResult(self.ok, 204 if self.ok else 500, "" if self.ok else "boom " + url)


def make(ok=True, url=URL, enabled=True, **pol):
    clk, post, store = Clock(), Post(ok), MemoryStore()
    n = Notifier(store, from_mapping(pol or None), enabled=enabled, kind="discord", url=url, clock=clk, post=post)
    return n, store, clk, post


def statuses(store):
    return [e["status"] for e in store.events]


def test_error_pushes_warning_goes_to_digest():
    n, s, _, p = make()
    n.emit("error", "e1", "b", "k1")
    n.emit("warning", "w1", "b", "k2")
    assert len(p.calls) == 1 and statuses(s) == ["sent", "digest"]


def test_dedup_within_cooldown_counts_but_does_not_resend():
    n, s, clk, p = make()
    n.emit("error", "e", "", "k")
    for _ in range(3):
        clk.adv(60)
        n.emit("error", "e", "", "k")
    assert len(p.calls) == 1 and s.states["k"]["suppressed_count"] == 3
    clk.adv(7200)
    n.emit("error", "e", "", "k")
    assert len(p.calls) == 2


def test_higher_level_passes_cooldown():
    n, _, clk, p = make()
    n.emit("error", "e", "", "k")
    clk.adv(10)
    n.emit("critical", "e", "", "k")
    assert len(p.calls) == 2


def test_open_warning_escalates_to_error_after_24h():
    n, s, clk, p = make()
    n.emit("warning", "w", "", "k")
    clk.adv(86400 + 1)
    n.emit("warning", "w", "", "k")
    assert len(p.calls) == 1 and s.events[-1]["level"] == "error" and s.events[-1]["original_level"] == "warning"


def test_critical_is_never_blocked_by_the_hourly_cap():
    """Mac 11-05(D-145): 복구 직후 '해소' 알림 10건이 상한을 채워, 그 뒤 Kafka 다운 critical 이 7분 넘게 억제됐다."""
    n, s, clk, p = make()
    for i in range(10):
        n.emit("error", f"e{i}", "", f"k{i}")
    assert len(p.calls) == 10
    n.emit("critical", "Kafka 다운", "", "kafka")
    assert len(p.calls) == 11 and s.events[-1]["status"] == "sent"
    n.emit("error", "e-extra", "", "kx")                         # critical 이 아닌 것은 여전히 상한을 받는다
    assert len(p.calls) == 11 and s.events[-1]["reason"] == "rate_limit"


def test_hourly_cap_suppresses_and_digest_carries_it():
    n, s, clk, p = make()
    for i in range(12):
        n.emit("error", f"e{i}", "", f"k{i}")
    assert len(p.calls) == 10
    assert [e["reason"] for e in s.events[10:]] == ["rate_limit", "rate_limit"]
    assert s.states["k11"].get("last_sent_at") is None          # 못 보냈으니 다음 기회에 다시 시도
    clk.adv(3700)
    n.emit("error", "e11", "", "k11")
    assert "억제된 알림 2건" in p.calls[-1]


def test_webhook_failure_is_recorded_not_raised_and_reported_next_time():
    n, s, clk, p = make(ok=False)
    assert n.emit("error", "e", "", "k") == "failed"
    assert URL not in s.events[0]["reason"]                      # 오류문에 URL 이 새지 않는다
    p.ok = True
    clk.adv(9000)
    n.emit("error", "e", "", "k")
    assert "이전 전달 실패 1건" in p.calls[-1]


def test_disabled_or_no_url_is_dropped_but_recorded():
    for kw in ({"enabled": False}, {"url": ""}):
        n, s, _, p = make(**kw)
        n.emit("critical", "c", "", "k")
        assert p.calls == [] and statuses(s) == ["dropped"]


def test_resolve_pushes_only_if_alert_was_pushed():
    n, s, _, p = make()
    n.emit("error", "src down", "", "k")
    assert n.resolve("k", "src down") is True and len(p.calls) == 2
    assert n.resolve("k") is False                               # 이미 해소
    n.emit("warning", "w", "", "kw")
    assert n.resolve("kw") is True and len(p.calls) == 2         # warning 은 푸시된 적 없음 → 해소도 조용히
    assert n.sync("kx", None, "t") == "noop"


def test_resolved_key_can_alert_again_immediately():
    n, _, _, p = make()
    n.emit("error", "e", "", "k")
    n.resolve("k")
    n.emit("error", "e", "", "k")
    assert len(p.calls) == 3


def test_pending_downgrade_group_and_failsafe():
    n, s, clk, p = make()
    for i in range(3):
        n.enqueue("error", f"dag{i} 실패", "", f"fail:dag{i}", meta={"i": i})
    n.enqueue("error", "other", "", "fail:other")
    assert n.process_pending(lambda r: Decision("error", "x")) == 0     # 아직 grace(60s) 전
    clk.adv(61)

    def cls(r):
        if r["dedup_key"] == "fail:other":
            raise RuntimeError("classifier bug")                 # 분류기가 죽어도 원래 수준으로 나간다
        return Decision("warning", "host_stall:2026-11-05", "host_stall")
    assert n.process_pending(cls) == 4
    assert statuses(s).count("suppressed") == 2 and statuses(s).count("digest") == 1 and statuses(s).count("sent") == 1
    assert s.events[0]["level"] == "warning" and s.events[0]["original_level"] == "error"
    assert "같은 종류 2건" in s.events[0]["body"]
    assert s.events[3]["level"] == "error" and s.events[3]["reason"] == "classifier_error"
    assert n.process_pending(cls) == 0                           # 처리된 것은 다시 안 잡힌다


def test_digest_sends_even_when_empty_and_marks_digested():
    n, s, clk, p = make()
    assert n.send_digest() is True and "새 경고 없음" in p.calls[0]
    n.emit("warning", "w", "", "k")
    n.emit("warning", "w2", "", "k2")
    n.send_digest()
    assert "w2" in p.calls[-1] and statuses(s).count("digested") == 2
    n.send_digest()
    assert "새 경고 없음" in p.calls[-1]


def test_digest_failure_keeps_items_for_retry():
    n, s, _, p = make(ok=False)
    n.emit("warning", "w", "", "k")
    assert n.send_digest() is False and "digest" in statuses(s)


def test_mask_hides_webhook_urls_and_configured_url(monkeypatch):
    assert "abcdefghij" not in mask("fail " + URL)
    assert "SLACKTOKEN" not in mask("x https://hooks.slack.com/services/T0/B0/SLACKTOKEN")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://example.test/hook")
    assert "example.test" not in mask("error at https://example.test/hook now")
    assert "secret=***" in mask("secret=abc")


def test_secrets_never_reach_events():
    n, s, _, p = make()
    n.emit("error", "t password=hunter2", f"url {URL}", "k")
    blob = repr(s.events) + repr(p.calls)
    assert "hunter2" not in blob and "abcdefghij" not in blob
