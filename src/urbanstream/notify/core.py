"""알림 핵심 (ADR-0010 D8-3/D8-4/D8-5). 저장소·전송·시계를 주입받아서 DB·네트워크 없이 규칙을 시험할 수 있다.

흐름
- emit        : 즉시 처리. error·critical 은 푸시(웹훅), warning·info 는 아침 요약(digest)에 쌓는다.
- enqueue     : DAG 실패 콜백용. 일단 pending 으로 기록만 한다 → process_pending 이 잠자기 증거와 대조해 분류한 뒤 emit 과 같은 경로로 보낸다.
- sync/resolve: 상태형 알림(소스 down 등). 상태가 바뀔 때만 보내고, 정상으로 돌아오면 해소 알림 1건.
- 규칙: 같은 dedup_key 는 쿨다운 동안 다시 보내지 않고 횟수만 센다 / 시간당 푸시 상한 / 열린 warning 이 길면 error 로 승격 / 의심스러우면 격하하지 않는다.
- 전송 실패는 예외가 아니라 status='failed' 기록이다. 다음 푸시 성공 때 "이전 전달 실패 N건"을 함께 알린다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional
from zoneinfo import ZoneInfo

from ._mask import mask
from .policy import LEVEL_ORDER, Policy
from .transport import SendResult, post_webhook

LOG = logging.getLogger("urbanstream.notify")
KST = ZoneInfo("Asia/Seoul")
LEVEL_LOG = {"info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR, "critical": logging.CRITICAL}
LABEL = {"info": "INFO", "warning": "WARN", "error": "ERROR", "critical": "CRITICAL"}


def _rank(level: str) -> int:
    return LEVEL_ORDER.index(level)


@dataclass(frozen=True)
class Decision:
    """분류 결과. level/dedup_key 를 바꿀 수 있고(격하·묶기), cooldown_sec 로 쿨다운을 덮어쓴다."""
    level: str
    dedup_key: Optional[str]
    reason: Optional[str] = None
    cooldown_sec: Optional[float] = None


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class Notifier:
    def __init__(self, store: Any, policy: Policy, *, enabled: bool = True, kind: str = "discord", url: str = "",
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 post: Callable[..., SendResult] = post_webhook) -> None:
        self.store, self.policy, self.enabled, self.kind, self.url = store, policy, enabled, kind, url
        self.clock, self._post = clock, post

    # ------------------------------------------------------------------ 전송
    def _deliver(self, level: str, title: str, body: str) -> tuple[str, str]:
        """(status, reason). 전송 대상이 없으면 'dropped' — 기록은 남는다."""
        if not self.enabled:
            return "dropped", "alerts_disabled"
        if not self.url:
            return "dropped", "no_webhook_url"
        carry = {}
        try:
            carry = self.store.counts_since_last_push()
        except Exception:                                                    # noqa: BLE001
            pass
        notes = []
        if carry.get("failed"):
            notes.append(f"이전 전달 실패 {carry['failed']}건")
        if carry.get("rate_limited"):
            notes.append(f"시간당 상한으로 억제된 알림 {carry['rate_limited']}건 (아침 요약에 포함)")
        text = f"[{LABEL[level]}] {title}" + (f"\n{body}" if body else "") + (f"\n({'; '.join(notes)})" if notes else "")
        res = self._post(self.kind, self.url, text, scrub=mask)
        if res.ok:
            return "sent", ""
        return "failed", mask(res.error)

    # ------------------------------------------------------------------ 경로 결정
    def _route(self, *, event_id: Optional[int], level: str, original: str, title: str, body: str, key: Optional[str],
               meta: Optional[Mapping[str, Any]] = None, reason: Optional[str] = None,
               cooldown: Optional[float] = None, created_at: Optional[datetime] = None) -> str:
        now = self.clock()
        st = self.store.get_state(key) if key else None
        if st and st["status"] != "open":
            st = None
        if level == "warning" and st and (now - _utc(st["first_at"])).total_seconds() >= self.policy.escalate_warning_after_sec:
            level, reason = "error", "escalated_from_warning"
        push = level in self.policy.push_levels
        cd = cooldown if cooldown is not None else self.policy.cooldown(level)

        # 중복 억제: 열려 있는 같은 키가 쿨다운 안에서 같거나 낮은 수준으로 반복됨 (더 높은 수준으로 올라가면 통과)
        if st and st.get("last_sent_at") and (now - _utc(st["last_sent_at"])).total_seconds() < cd and _rank(level) <= _rank(st["level"]):
            self.store.save_state(key, last_at=now, count=st["count"] + 1, suppressed_count=st["suppressed_count"] + 1)
            if event_id is not None:
                self.store.update_event(event_id, status="suppressed", reason=f"cooldown:{reason or 'dup'}")
            return "suppressed"

        status, why = ("digest", reason or "") if not push else ("sent", "")
        # critical 은 시간당 상한을 받지 않는다: 복구 직후 '해소' 알림이 몰려 상한을 채우면 그 뒤에 생긴 진짜 장애(Kafka 다운)가 막힌다 (Mac 11-05, D-145).
        if push and level != "critical" and self.store.count_pushed_since(now - timedelta(hours=1)) >= self.policy.max_push_per_hour:
            status, why = "suppressed", "rate_limit"
        elif push:
            status, why = self._deliver(level, title, body)
            if why:
                why = why if not reason else f"{reason}; {why}"
            elif reason:
                why = reason
        sent_at = now if status == "sent" else None
        fields = dict(level=level, original_level=original if original != level else None, title=title, body=body,
                      dedup_key=key, status=status, reason=why or None, sent_at=sent_at)
        if meta is not None:
            fields["meta"] = dict(meta)
        if event_id is None:
            event_id = self.store.add_event(created_at=created_at or now, **{k: v for k, v in fields.items()})
        else:
            self.store.update_event(event_id, **fields)

        if key:
            counted = bool(st)
            self.store.save_state(
                key, level=level, title=title, status="open", resolved_at=None,
                first_at=_utc(st["first_at"]) if counted else now, last_at=now,
                # rate_limit 으로 못 보낸 경우에는 last_sent_at 을 갱신하지 않아 다음 기회에 다시 보낸다
                last_sent_at=(now if status != "suppressed" else (st or {}).get("last_sent_at")),
                count=(st["count"] + 1) if counted else 1, suppressed_count=(st["suppressed_count"] if counted else 0))
        return status

    # ------------------------------------------------------------------ 공개 동작
    def emit(self, level: str, title: str, body: str = "", dedup_key: Optional[str] = None, *,
             meta: Optional[Mapping[str, Any]] = None) -> str:
        return self._route(event_id=None, level=level, original=level, title=mask(title), body=mask(body), key=dedup_key, meta=meta)

    def enqueue(self, level: str, title: str, body: str = "", dedup_key: Optional[str] = None, *,
                meta: Optional[Mapping[str, Any]] = None) -> int:
        return self.store.add_event(created_at=self.clock(), level=level, original_level=None, title=mask(title), body=mask(body),
                                    dedup_key=dedup_key, status="pending", reason=None, sent_at=None, meta=dict(meta or {}))

    def sync(self, dedup_key: str, level: Optional[str], title: str, body: str = "", *,
             meta: Optional[Mapping[str, Any]] = None) -> str:
        """상태형 알림. level=None 이면 정상 → 열려 있으면 해소. 아니면 반복 호출돼도 쿨다운이 합친다."""
        if level is None:
            return "resolved" if self.resolve(dedup_key, title) else "noop"
        return self.emit(level, title, body, dedup_key, meta=meta)

    def resolve(self, dedup_key: str, title: str = "", body: str = "") -> bool:
        st = self.store.get_state(dedup_key)
        if not st or st["status"] != "open":
            return False
        now = self.clock()
        self.store.save_state(dedup_key, status="resolved", resolved_at=now, last_at=now)
        t = mask(f"해소: {title or st['title']}")
        if st["level"] in self.policy.push_levels and st.get("last_sent_at") is not None:
            status, why = self._deliver("info", t, mask(body))
            sent_at = now if status == "sent" else None
        else:
            status, why, sent_at = "digest", "resolved", None
        self.store.add_event(created_at=now, level="info", original_level=None, title=t, body=mask(body), dedup_key=dedup_key,
                             status=status, reason=why or None, sent_at=sent_at, meta={"resolved": True})
        return True

    def process_pending(self, classify: Callable[[dict], Decision], *, min_age_sec: Optional[float] = None) -> int:
        """pending 을 분류해 보낸다. 같은 dedup_key 로 분류된 것은 한 번에 묶어 1건만 보내고 나머지는 'grouped' 로 억제한다."""
        now = self.clock()
        age = self.policy.pending_grace_sec if min_age_sec is None else min_age_sec
        rows = self.store.pending(now - timedelta(seconds=age))
        groups: dict[Optional[str], list[tuple[dict, Decision]]] = {}
        order: list[Optional[str]] = []
        for r in rows:
            try:
                d = classify(r)
            except Exception as e:                                           # noqa: BLE001 - 분류가 틀려도 알림은 나간다
                LOG.warning("분류 실패, 원래 수준으로 보냅니다: %s", mask(f"{type(e).__name__}: {e}"))
                d = Decision(r["level"], r["dedup_key"], "classifier_error")
            k = d.dedup_key if d.dedup_key is not None else f"__none__{r['alert_id']}"
            if k not in groups:
                groups[k] = []
                order.append(k)
            groups[k].append((r, d))
        for k in order:
            items = groups[k]
            (head, dec), rest = items[0], items[1:]
            body = head["body"]
            if rest:
                body += f"\n(같은 종류 {len(rest)}건 더: " + ", ".join(x["title"] for x, _ in rest[:8]) + ")"
            self._route(event_id=head["alert_id"], level=dec.level, original=head["level"], title=head["title"], body=mask(body),
                        key=dec.dedup_key, meta=head.get("meta"), reason=dec.reason, cooldown=dec.cooldown_sec)
            for x, _ in rest:
                self.store.update_event(x["alert_id"], status="suppressed", reason=f"grouped:{dec.reason or k}")
        return len(rows)

    def send_digest(self, extra_lines: tuple[str, ...] = ()) -> bool:
        """아침 요약 1건. 쌓인 warning·info(와 상한으로 억제된 푸시)를 묶는다. 할 말이 없어도 '이상 없음'을 보낸다(알림 시스템이 살아 있다는 신호)."""
        rows = self.store.digest_pending()
        lines: list[str] = []
        seen: dict[str, int] = {}
        for r in rows:
            seen[f"{LABEL[r['level']]} {r['title']}"] = seen.get(f"{LABEL[r['level']]} {r['title']}", 0) + 1
        for t, n in list(seen.items())[:30]:
            lines.append(f"- {t}" + (f" ×{n}" if n > 1 else ""))
        if len(seen) > 30:
            lines.append(f"- 외 {len(seen) - 30}종")
        lines.extend(extra_lines)
        title = "아침 요약 " + self.clock().astimezone(KST).strftime("%m-%d")
        body = "\n".join(lines) if lines else "새 경고 없음"
        status, why = self._deliver("info", title, mask(body))
        now = self.clock()
        self.store.add_event(created_at=now, level="info", original_level=None, title=title, body=mask(body), dedup_key="digest",
                             status=status, reason=why or None, sent_at=now if status == "sent" else None, meta={"digest": True})
        if status in ("sent", "dropped"):          # 보냈거나 보낼 곳이 없을 때는 비운다(영원히 쌓이지 않게). 전송 실패면 다음에 다시 시도한다
            self.store.mark_digested(r["alert_id"] for r in rows)
        return status == "sent"
