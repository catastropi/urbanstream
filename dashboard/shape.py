"""API 응답 → 화면용 구조 (순수 함수, streamlit 없음). ADR-0012 D10-6.

원칙 (HANDOFF 7-0)
- null/빈 값은 0·'정상'으로 바꾸지 않는다. 숫자는 fmt_num 으로만 문자열이 되고 None 은 '—' 이다.
- 열거값(혼잡 단계·상태 …)이 늘어도 죽지 않는다: 모르는 값은 원문 그대로 보여 준다.
- 색은 상태(좋음/경고/심각/치명) 팔레트를 쓰되 항상 글자와 기호가 같이 간다(색만으로 전달하지 않는다).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Optional

from .timefmt import DASH, day_type_guess, fmt_kst, to_kst

GRAY = (138, 138, 133, 170)

# 혼잡 단계: 글자·기호·색(상태 팔레트 good/warning/serious/critical)
LEVELS = {
    "severe": ("◆ 매우 혼잡", (208, 59, 59, 230)),
    "heavy": ("▲ 혼잡", (236, 131, 90, 230)),
    "slow": ("■ 서행", (250, 178, 25, 230)),
    "normal": ("● 원활", (12, 163, 12, 230)),
}
LEVEL_ORDER = ("severe", "heavy", "slow", "normal")

STATUS_LABEL = {"ok": "● 정상", "degraded": "▲ 저하", "down": "◆ 중단", "idle": "○ 대기", "unknown": "◇ 알 수 없음"}
PRECIP_LABEL = {"none": "강수 없음", "light": "약한 비", "moderate": "보통 비", "heavy": "강한 비", "snow_mix": "눈·비눈"}
PRECIP_ORDER = ("none", "light", "moderate", "heavy", "snow_mix")
INCIDENT_STATUS = {"listed": "진행 중", "pending": "해제됨(24시간 확정 대기)", "finalized": "영향 확정", "excluded_long_term": "장기(제외)"}
# 돌발 유형 코드. 관측한 4개만 안다(전체 코드표 미확인, D-179). 모르는 코드는 원문 코드를 그대로 보인다.
ACC_TYPE = {"A01": "사고", "A04": "공사·작업", "A10": "행사·통제", "A11": "기타(버스 우회·노면 요철 등)"}
ANOMALY_STATUS = {"anomaly": "이상", "candidate": "후보(표본 부족)", "degenerate_baseline": "기준선 퇴화"}


def fmt_num(x: Any, digits: int = 1, unit: str = "") -> str:
    if x is None or isinstance(x, bool):
        return DASH
    try:
        return f"{float(x):.{digits}f}{unit}"
    except (TypeError, ValueError):
        return DASH


def fmt_int(x: Any) -> str:
    if x is None or isinstance(x, bool):
        return DASH
    try:
        return f"{int(x):,}"
    except (TypeError, ValueError):
        return DASH


def level_text(level: Optional[str]) -> str:
    """None(판정 불가)은 '원활' 이 아니다."""
    if level is None:
        return "◇ 판정 불가(표본 부족)"
    return LEVELS[level][0] if level in LEVELS else f"? {level}"


def level_color(level: Optional[str]) -> tuple:
    return LEVELS[level][1] if level in LEVELS else GRAY


def status_text(status: Optional[str]) -> str:
    return DASH if status is None else STATUS_LABEL.get(status, f"? {status}")


def acc_type_text(code: Optional[str]) -> str:
    """'A01' → '사고 (A01)'. 모르는 코드는 '미확인 코드 (A99)', 없으면 '—'."""
    if not code:
        return DASH
    return f"{ACC_TYPE[code]} ({code})" if code in ACC_TYPE else f"미확인 코드 ({code})"


def label(mapping: Mapping[str, str], key: Optional[str]) -> str:
    return DASH if key is None else mapping.get(key, f"? {key}")


# ---- 혼잡 ---------------------------------------------------------------------------------------------------
def traffic_rows(items: Iterable[Mapping]) -> list[dict]:
    rows = []
    for r in items:
        rows.append({
            "링크": r.get("link_id"),
            "관측(KST)": fmt_kst(r.get("event_time")),
            "속도(km/h)": fmt_num(r.get("speed_kmh")),
            "평소 중앙값(km/h)": fmt_num(r.get("baseline_p50")),
            "평소 대비": fmt_num(None if r.get("ratio_to_baseline") is None else r["ratio_to_baseline"] * 100, 0, "%"),
            "단계": level_text(r.get("level")),
            "기준선 표본": fmt_int(r.get("threshold_n_obs")),
            "잠정": "예" if r.get("threshold_provisional") or r.get("baseline_provisional") else ("아니오" if r.get("threshold_provisional") is False else DASH),
        })
    return rows


def level_counts(items: Iterable[Mapping]) -> dict:
    """단계별 개수. 판정 불가(None)는 따로 센다 — '원활' 에 합치지 않는다."""
    c = {k: 0 for k in LEVEL_ORDER}
    c["unknown_level"] = 0
    for r in items:
        lv = r.get("level")
        if lv in c:
            c[lv] += 1
        else:
            c["unknown_level"] += 1
    return c


def link_paths(links: Iterable[Mapping], level_by_link: Mapping[str, Optional[str]]) -> tuple[list[dict], int]:
    """(지도용 경로 목록, 형상이 없어 건너뛴 수). GeoJSON LineString 은 [경도, 위도] 순서다."""
    out, skipped = [], 0
    for l in links:
        g = l.get("geometry")
        if not isinstance(g, Mapping) or g.get("type") != "LineString" or not g.get("coordinates"):
            skipped += 1
            continue
        lv = level_by_link.get(l.get("link_id"))
        out.append({"link_id": l.get("link_id"), "path": [[float(c[0]), float(c[1])] for c in g["coordinates"]],
                    "color": list(level_color(lv)), "단계": level_text(lv)})
    return out, skipped


def speed_series(points: Iterable[Mapping]) -> list[dict]:
    """시간순 (KST 시각, 속도). 속도 None 은 None 으로 둔다(선이 끊겨 보인다) — 0 으로 바꾸지 않는다."""
    rows = []
    for p in points:
        t = to_kst(p.get("event_time"))
        if t is None:
            continue
        rows.append({"t": t.replace(tzinfo=None), "speed": p.get("speed_kmh"), "kst_hour": t.hour,
                     "day_type": day_type_guess(t.date()), "dq": len(p.get("dq_flags") or [])})
    rows.sort(key=lambda r: r["t"])
    return rows


def attach_baseline(series: list[dict], thresholds: Iterable[Mapping]) -> tuple[list[dict], bool]:
    """각 점에 (요일 구분, KST 시) 기준선 p10/p50/p90 을 붙인다. 기준선이 하나도 없으면 (원본, False)."""
    idx = {}
    for t in thresholds:
        if t.get("speed_p50") is None:
            continue
        idx[(t.get("day_type"), t.get("kst_hour"))] = t
    if not idx:
        return series, False
    out, hit = [], False
    for r in series:
        t = idx.get((r["day_type"], r["kst_hour"]))
        row = dict(r)
        row["p10"], row["p50"], row["p90"] = (t.get("speed_p10"), t.get("speed_p50"), t.get("speed_p90")) if t else (None, None, None)
        hit = hit or t is not None
        out.append(row)
    return out, hit


# ---- 돌발 ---------------------------------------------------------------------------------------------------
def incident_points(items: Iterable[Mapping]) -> tuple[list[dict], int]:
    pts, nocoord = [], 0
    for e in items:
        lat, lon = e.get("lat"), e.get("lon")
        if lat is None or lon is None:
            nocoord += 1
            continue
        pts.append({"lat": float(lat), "lon": float(lon), "acc_id": e.get("acc_id"), "유형": acc_type_text(e.get("acc_type")),
                    "발생(KST)": fmt_kst(e.get("occurred_at")), "해제 예정(KST)": fmt_kst(e.get("expected_clear_at"))})
    return pts, nocoord


def incident_rows(items: Iterable[Mapping]) -> list[dict]:
    rows = []
    for it in items:
        ep, im = it.get("episode") or {}, it.get("impact") or {}
        rows.append({
            "유형": acc_type_text(ep.get("acc_type")), "발생(KST)": fmt_kst(ep.get("occurred_at")),
            "상태": label(INCIDENT_STATUS, it.get("status")),
            "연결 거리(m)": fmt_num(im.get("distance_m"), 0), "지속(분)": fmt_int(im.get("duration_min")),
            "속도 변화(km/h)": fmt_num(im.get("delta")), "표본(전/중)": f"{fmt_int(im.get('n_pre'))}/{fmt_int(im.get('n_in'))}" if im else DASH,
            "충분": "예" if im.get("sufficient") else ("아니오" if im else DASH), "잠정": "예" if im.get("provisional") else ("아니오" if im else DASH),
        })
    return rows


def impact_type_rows(items: Iterable[Mapping]) -> list[dict]:
    return [{"유형": acc_type_text(r.get("acc_type")), "에피소드": fmt_int(r.get("n_episodes")), "링크 연결": fmt_int(r.get("n_linked")),
             "충분한 표본": fmt_int(r.get("n_sufficient")), "속도 변화 중앙값(km/h)": fmt_num(r.get("delta_p50")),
             "사고 중/전 비율 중앙값": fmt_num(r.get("ratio_in_p50"), 2), "잠정": "예" if r.get("provisional") else "아니오"} for r in items]


# ---- 버스 ---------------------------------------------------------------------------------------------------
def bus_points(items: Iterable[Mapping]) -> tuple[list[dict], int]:
    """차량 번호는 화면에 싣지 않는다(필요 없음, 내보내기와 같은 규칙)."""
    pts, nocoord = [], 0
    for b in items:
        if b.get("lat") is None or b.get("lon") is None:
            nocoord += 1
            continue
        pts.append({"lat": float(b["lat"]), "lon": float(b["lon"]), "관측(KST)": fmt_kst(b.get("event_time")),
                    "구간순번": fmt_int(b.get("sect_ord")), "혼잡도": fmt_int(b.get("congestion")),
                    "만차": "예" if b.get("is_full") else ("아니오" if b.get("is_full") is False else DASH)})
    return pts, nocoord


def drift_series(items: Iterable[Mapping]) -> list[dict]:
    """일·시(KST, 이미 KST) 단위 도착 예측 변동. event_date·kst_hour 는 변환하지 않는다."""
    rows = []
    for r in items:
        if r.get("event_date") is None or r.get("kst_hour") is None:
            continue
        rows.append({"일시(KST)": f"{r['event_date']} {int(r['kst_hour']):02d}시", "p50(초)": r.get("drift_p50_sec"),
                     "p90(초)": r.get("drift_p90_sec"), "도착 건수": r.get("n_approaches"), "정류소": r.get("station_id")})
    rows.sort(key=lambda r: r["일시(KST)"])
    return rows


def section_rows(items: Iterable[Mapping]) -> list[dict]:
    return [{"일시(KST)": f"{r.get('event_date')} {int(r['kst_hour']):02d}시" if r.get("kst_hour") is not None else DASH,
             "구간": r.get("section_id"), "순번": fmt_int(r.get("sect_ord")), "통과(초)": fmt_int(r.get("traversal_sec")),
             "평소 중앙값(초)": fmt_int(r.get("base_p50_sec")), "평소 대비": fmt_num(r.get("ratio_to_base"), 2, "배"),
             "잠정": "예" if r.get("base_provisional") else "아니오"} for r in items]


# ---- 날씨 ---------------------------------------------------------------------------------------------------
def weather_effect_rows(items: Iterable[Mapping]) -> list[dict]:
    order = {k: i for i, k in enumerate(PRECIP_ORDER)}
    rows = []
    for r in items:
        rows.append({"강수": label(PRECIP_LABEL, r.get("precip_class")), "_key": r.get("precip_class"),
                     "시간 수": r.get("n_hours"), "링크 수": r.get("n_links"),
                     "p25": r.get("ratio_p25"), "p50": r.get("ratio_p50"), "p75": r.get("ratio_p75"),
                     "표본 부족": bool(r.get("insufficient")), "잠정": bool(r.get("provisional"))})
    rows.sort(key=lambda r: order.get(r["_key"], 99))
    return rows


# ---- 파이프라인 -----------------------------------------------------------------------------------------------
def component_rows(components: Iterable[Mapping], now: datetime) -> list[dict]:
    from .timefmt import age_seconds, humanize_age
    rows = []
    for c in components:
        rows.append({"종류": c.get("component_kind"), "구성요소": c.get("component"), "상태": status_text(c.get("status")),
                     "마지막 정상(KST)": fmt_kst(c.get("last_ok_at")), "경과": humanize_age(age_seconds(c.get("last_ok_at"), now)),
                     "사유": c.get("reason") or DASH})
    return rows


def count_by_status(components: Iterable[Mapping]) -> dict:
    c: dict = {}
    for x in components:
        c[x.get("status")] = c.get(x.get("status"), 0) + 1
    return c


def anomaly_rows(items: Iterable[Mapping]) -> list[dict]:
    return [{"링크": r.get("link_id"), "관측(KST)": fmt_kst(r.get("event_time")), "속도(km/h)": fmt_num(r.get("speed_kmh")),
             "평소 중앙값(km/h)": fmt_num(r.get("baseline_p50")), "robust z": fmt_num(r.get("robust_z"), 2),
             "방향": r.get("direction") or DASH, "상태": label(ANOMALY_STATUS, r.get("status")),
             "기준선 표본": fmt_int(r.get("n_obs")), "탐지(KST)": fmt_kst(r.get("detected_at"))} for r in items]
