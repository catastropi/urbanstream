"""meta(as_of·age_sec·stale·loaded_at) → 화면 표시 문구 (ADR-0012 D10-5, D-149). 순수 함수.

원칙
- 경과 시간은 API 의 age_sec 를 그대로 쓰지 않고 `지금 − as_of` 로 화면에서 다시 계산한다. 캐시된 값이 방금 받은 것처럼 보이지 않게.
- 오래됨 여부는 API 의 meta.stale 이 기준이다(엔드포인트마다 기준이 다르다: 운영 5분, 그 밖 3시간). 서버가 판정하지 않으면(null) 우리도 판정하지 않는다.
- 상태는 색이 아니라 **기호+글자**로 구분한다(색만으로 전달하지 않는다).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional

from .timefmt import DASH, age_seconds, fmt_kst, humanize_age, parse_utc

BADGES = {"ok": "● 최신", "stale": "▲ 오래됨", "nodata": "○ 데이터 없음", "unknown": "◇ 기준 없음"}

GOLD_LAG_NOTICE = ("이 화면의 '현재' 값은 실시간이 아닙니다. Gold 가 매시 20분에 적재되므로 최대 약 80분 전 값입니다 "
                   "(적재 시각은 아래 '적재'에 표시).")


@dataclass(frozen=True)
class Freshness:
    level: str          # ok | stale | nodata | unknown
    badge: str
    detail: str

    @property
    def text(self) -> str:
        return f"{self.badge} · {self.detail}" if self.detail else self.badge


def describe(meta: Optional[Mapping], *, now: datetime, fetched_at: Optional[datetime] = None,
             error: Optional[str] = None) -> Freshness:
    meta = meta or {}
    try:
        as_of = parse_utc(meta.get("as_of"))
    except ValueError:
        as_of = None
        level = "unknown"
    else:
        level = None  # type: ignore[assignment]
    if level is None:
        if as_of is None:
            level = "nodata"
        else:
            stale = meta.get("stale")
            level = "unknown" if stale is None else "stale" if stale else "ok"
    parts = []
    if as_of is not None:
        parts.append(f"기준 {fmt_kst(as_of)}")
        parts.append(f"경과 {humanize_age(age_seconds(as_of, now))}")
    try:
        loaded = parse_utc(meta.get("loaded_at"))
    except ValueError:
        loaded = None
    if loaded is not None:
        parts.append(f"적재 {fmt_kst(loaded)}")
    if error:
        when = fmt_kst(fetched_at) if fetched_at else DASH
        parts.append(f"⚠ 갱신 실패 — {when} 에 받은 값을 보여 주는 중 ({error})")
    return Freshness(level, BADGES[level], " · ".join(parts))


NOTE_TEXT = {
    "empty_result": "조건에 맞는 데이터가 없습니다.",
    "baseline_provisional": "기준선(임계값) 표본이 아직 적어 '잠정' 값입니다. 단계 판정이 비어 있을 수 있습니다.",
    "baseline_unavailable": "기준선 표가 아직 만들어지지 않아 기준선 없이 보여 줍니다.",
    "association_not_causation": "연관을 보여 줄 뿐 인과를 뜻하지 않습니다(비가 와서 느려졌다고 단정할 수 없음).",
    "impact_pending_24h": "해제 후 24시간이 지나야 영향이 확정됩니다. 그 전 값은 잠정입니다.",
    "outside_collection_window_or_no_data": "수집 시간(04:00~) 밖이거나 데이터가 없어 비어 있을 수 있습니다.",
    "daily_grain_kst_dates": "일 단위 값입니다. 날짜와 시(hour)는 KST 기준입니다.",
    "provisional_small_sample": "표본이 적은 잠정 값입니다.",
}


def explain_notes(notes) -> list[str]:
    """알려진 코드는 한국어 설명으로, 모르는 코드는 원문 그대로(새 코드가 와도 죽지 않는다)."""
    out = []
    for n in notes or []:
        out.append(NOTE_TEXT.get(n, f"API 안내 코드: {n}"))
    return out
