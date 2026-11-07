#!/usr/bin/env python3
"""Tableau 용 CSV 추출 (Step 10, D10-8). FastAPI(X-API-Key) 만 읽고, 파일을 tableau/export/ 에 쓴다.

하는 일 / 안 하는 일
- 읽기만 한다: 데이터 입구는 대시보드와 같은 FastAPI 하나다. DB 에 직접 붙지 않는다.
- 집계 값만 낸다: 차량번호·차량 ID·운영 지표(DQ·알림·소스 상태)는 내지 않는다(고정 열 목록 + 시험이 막는다).
- **게시(업로드)는 하지 않는다.** Tableau Public 에 올리면 누구나 볼 수 있다. 라이선스·약관·공개 범위는 사용자가 직접 확인한 뒤
  사용자가 직접 올린다 (manifest.json 의 publish_checklist 참고). 이 스크립트는 네트워크로 어디에도 보내지 않는다(127.0.0.1 API 만 호출).
- 키는 .env 의 DASHBOARD_API_KEY 에서 읽는다. 화면·로그·manifest 에 값을 쓰지 않는다.

사용:
  python scripts/export_tableau.py --preview       # 파일을 쓰지 않고 행 수와 앞부분만 본다
  python scripts/export_tableau.py                 # 최근 7일을 tableau/export/ 에 쓴다
  python scripts/export_tableau.py --days 14       # 최대 28일(API 는 한 번에 7일까지라 7일씩 나눠 읽는다)

종료 코드: 0 성공 / 2 설정 문제(.env·키) / 3 API 오류 / 4 불완전(쪽수 제한에서 멈춤 — 파일은 쓰되 manifest 에 표시)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard import contract as C  # noqa: E402
from dashboard.client import ApiClient, ApiError  # noqa: E402
from dashboard.config import MIN_KEY_LEN, Settings  # noqa: E402
from dashboard.shape import ACC_TYPE  # noqa: E402
from dashboard.timefmt import day_type_guess, floor_to, parse_utc, to_kst, utc_z  # noqa: E402

MAX_DAYS = 28
CHUNK_DAYS = 7           # API 는 한 번에 최대 7일 (HANDOFF 7-10)
MAX_PAGES = 60           # 링크·창당 쪽수 상한(쪽당 1,000행). 넘으면 불완전로 표시한다
DEFAULT_OUT = ROOT / "tableau" / "export"

# 이 열 이름은 어떤 CSV 에도 들어가면 안 된다 (시험이 모든 데이터셋의 열을 검사한다)
FORBIDDEN_COLUMNS = frozenset({"veh_id", "plain_no", "vehicle_id", "plate", "stop_flag", "last_stop_id", "next_stop_id",
                               "dedup_key", "alert_id", "rule_id", "reason"})

COLUMNS: dict[str, list[str]] = {
    "link_speed_hourly": ["snapshot_at", "link_id", "kst_date", "kst_hour", "day_type_guess", "n_points", "n_null",
                          "speed_mean_kmh", "speed_min_kmh", "speed_max_kmh", "n_dq_flagged"],
    "congestion_threshold": ["snapshot_at", "link_id", "day_type", "kst_hour", "speed_p10", "speed_p25", "speed_p50",
                             "speed_p90", "n_obs", "provisional", "sufficient"],
    "incident_impact_by_type": ["snapshot_at", "acc_type", "acc_type_label", "n_episodes", "n_linked", "n_sufficient", "delta_p50",
                                "ratio_in_p50", "provisional"],
    "weather_effect": ["snapshot_at", "precip_class", "n_hours", "n_links", "ratio_p25", "ratio_p50", "ratio_p75",
                       "insufficient", "provisional"],
    "bus_arrival_drift_hourly": ["snapshot_at", "event_date", "kst_hour", "route_id", "station_id", "n_approaches",
                                 "drift_p50_sec", "drift_p90_sec", "provisional"],
}

PUBLISH_CHECKLIST = [
    "Tableau Public 에 올린 통합문서와 데이터는 인터넷의 누구나 볼 수 있고 내려받을 수 있습니다. 비공개 게시는 되지 않습니다.",
    "서울 열린데이터광장·공공데이터포털 등 원천 데이터의 이용 조건(출처 표시, 재배포 허용 여부)을 직접 확인했는지 점검하세요. (이 프로젝트에서는 미확인)",
    "Tableau Public 의 현재 이용약관·데이터 한도·공개 범위를 직접 확인하세요. (이 프로젝트에서는 미확인)",
    "CSV 에는 링크 ID·노선·정류장 ID 같은 집계 단위 식별자가 있습니다. 차량번호·차량 ID 는 없습니다. 올리기 전에 열을 한 번 훑어보세요.",
    "값은 잠정(provisional) 기준선 위의 결과입니다(2026-11-30 전후까지). 'provisional' 열을 필터로 남기고 제목·주석에 '잠정' 을 적으세요.",
    "날씨 효과는 연관이지 인과가 아니며 표본이 적은 구간(insufficient)은 해석하지 마세요.",
    "게시는 사용자가 직접 합니다. 이 스크립트는 어디에도 업로드하지 않습니다.",
]


# --------------------------------------------------------------------------------------------- .env / 설정
def parse_env_text(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


class ExportConfigError(Exception):
    pass


def settings_from_env(env: Mapping[str, str], api_url: Optional[str] = None) -> Settings:
    key = (env.get("DASHBOARD_API_KEY") or "").strip()
    if not key:
        raise ExportConfigError(".env 에 DASHBOARD_API_KEY 가 없습니다. 먼저 실행: scripts/stack.sh init-env")
    if len(key) < MIN_KEY_LEN or any(c.isspace() for c in key) or "," in key:
        raise ExportConfigError(f"DASHBOARD_API_KEY 는 공백·쉼표 없이 {MIN_KEY_LEN}자 이상이어야 합니다")
    url = (api_url or f"http://127.0.0.1:{(env.get('API_PORT') or '8010').strip()}").rstrip("/")
    if not url.startswith(("http://127.0.0.1", "http://localhost")):
        raise ExportConfigError("내보내기는 로컬 API(127.0.0.1) 에서만 읽습니다. 다른 주소는 허용하지 않습니다")
    # 캐시 끔(ttl_scale=0): 내보내기는 항상 최신 값을 읽는다. 호출 상한은 대기로 지킨다(on_budget='wait')
    route = (env.get("SEOUL_BUS_ROUTE_ID") or "").strip() or "100100118"
    return Settings(api_url=url, api_key=key, ttl_scale=0.0, timeout_sec=15.0, budget_per_min=100, bus_route_id=route)


# --------------------------------------------------------------------------------------------- 변환(순수)
def windows(now: datetime, days: int) -> list[tuple[str, str]]:
    """[now-days, now] 를 7일 이하 조각으로 나눈다. 경계는 300초 단위로 내려(캐시·재현성), 조각은 겹치지 않는다."""
    if not 1 <= days <= MAX_DAYS:
        raise ValueError(f"days 는 1~{MAX_DAYS}")
    end = floor_to(now, 300)
    start = end - timedelta(days=days)
    out, cur = [], start
    while cur < end:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), end)
        out.append((utc_z(cur), utc_z(nxt)))
        cur = nxt
    return out


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def hourly_speed_rows(points_by_link: Mapping[str, Iterable[Mapping]], snapshot_at: str) -> list[dict]:
    """링크·KST 날짜·KST 시간 단위 집계. 속도 None 은 평균에서 빼고 n_null 로 센다(0 으로 바꾸지 않는다).
    같은 event_time 이 조각 경계에서 두 번 오면 한 번만 센다."""
    groups: dict[tuple, dict] = {}
    for link_id, points in points_by_link.items():
        seen: set = set()
        for p in points:
            t = parse_utc(p.get("event_time"))
            if t is None or t in seen:
                continue
            seen.add(t)
            k = to_kst(t)
            g = groups.setdefault((link_id, k.date(), k.hour), {"speeds": [], "n_null": 0, "n_dq": 0})
            v = _num(p.get("speed_kmh"))
            if v is None:
                g["n_null"] += 1
            else:
                g["speeds"].append(v)
            if p.get("dq_flags"):
                g["n_dq"] += 1
    rows = []
    for (link_id, d, h), g in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
        sp = g["speeds"]
        rows.append({"snapshot_at": snapshot_at, "link_id": link_id, "kst_date": d.isoformat(), "kst_hour": h,
                     "day_type_guess": day_type_guess(d), "n_points": len(sp), "n_null": g["n_null"],
                     "speed_mean_kmh": round(sum(sp) / len(sp), 2) if sp else None,
                     "speed_min_kmh": min(sp) if sp else None, "speed_max_kmh": max(sp) if sp else None,
                     "n_dq_flagged": g["n_dq"]})
    return rows


def pick(rows: Iterable[Mapping], dataset: str, snapshot_at: str) -> list[dict]:
    """고정 열 목록만 꺼낸다. API 가 열을 더 보내도(차량번호 등) 여기서 걸러진다."""
    cols = COLUMNS[dataset]
    out = []
    for r in rows:
        row = {c: r.get(c) for c in cols}
        if "acc_type_label" in row:
            code = r.get("acc_type")
            row["acc_type_label"] = ACC_TYPE.get(code, "미확인 코드") if code else None
        row["snapshot_at"] = snapshot_at
        out.append(row)
    return out


def assert_columns_safe() -> None:
    for name, cols in COLUMNS.items():
        bad = FORBIDDEN_COLUMNS & set(cols)
        if bad:
            raise AssertionError(f"{name} 에 내보내면 안 되는 열: {sorted(bad)}")
        if cols[0] != "snapshot_at" or len(set(cols)) != len(cols):
            raise AssertionError(f"{name} 열 목록 오류")


# --------------------------------------------------------------------------------------------- 수집
class Collected:
    def __init__(self) -> None:
        self.datasets: dict[str, list[dict]] = {}
        self.as_of: dict[str, Optional[str]] = {}
        self.notes: dict[str, list[str]] = {}
        self.incomplete: list[str] = []
        self.warnings: list[str] = []


def _cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return v


def collect(client: ApiClient, now: datetime, days: int) -> Collected:
    assert_columns_safe()
    snap = utc_z(now.replace(microsecond=0))
    out = Collected()
    wins = windows(now, days)

    links = client.get_all(C.LINKS, {"collected_only": True, "geometry": False, "limit": 500}, max_pages=5)
    link_ids = [str(r["link_id"]) for r in links.rows if r.get("link_id") is not None]
    if links.truncated:
        out.incomplete.append("links")
    if not link_ids:
        out.warnings.append("수집 대상 링크가 없습니다(빈 결과)")

    points: dict[str, list] = defaultdict(list)
    speed_as_of: Optional[str] = None
    for lid in link_ids:
        for frm, to in wins:
            res = client.get_all(C.TRAFFIC_SPEEDS, {"link_id": lid, "from": frm, "to": to, "limit": 1000}, max_pages=MAX_PAGES)
            points[lid].extend(res.rows)
            speed_as_of = speed_as_of or (res.meta or {}).get("as_of")
            if res.truncated:
                out.incomplete.append(f"traffic_speeds:{lid}:{frm}")
    out.datasets["link_speed_hourly"] = hourly_speed_rows(points, snap)
    out.as_of["link_speed_hourly"] = speed_as_of

    thr_rows: list[dict] = []
    thr_meta: Optional[dict] = None
    for lid in link_ids:
        r = client.get(C.TRAFFIC_THRESHOLDS, {"link_id": lid})
        thr_rows.extend({**row, "link_id": row.get("link_id") or lid} for row in r.rows)
        thr_meta = thr_meta or r.meta
    out.datasets["congestion_threshold"] = pick(thr_rows, "congestion_threshold", snap)
    out.as_of["congestion_threshold"] = (thr_meta or {}).get("as_of")
    out.notes["congestion_threshold"] = list((thr_meta or {}).get("notes") or [])

    for ds, ep in (("incident_impact_by_type", C.IMPACT_BY_TYPE), ("weather_effect", C.WEATHER_EFFECT)):
        r = client.get(ep, {})
        out.datasets[ds] = pick(r.rows, ds, snap)
        out.as_of[ds] = (r.meta or {}).get("as_of")
        out.notes[ds] = list((r.meta or {}).get("notes") or [])

    drift: list[dict] = []
    drift_meta: Optional[dict] = None
    for frm, to in wins:
        r = client.get_all(C.BUS_DRIFT, {"route_id": client.bus_route_id, "from": frm, "to": to, "limit": 1000}, max_pages=MAX_PAGES)
        drift.extend(r.rows)
        drift_meta = drift_meta or r.meta
        if r.truncated:
            out.incomplete.append(f"bus_arrival_drift:{frm}")
    # 조각 경계가 같은 날을 가를 수 있어 (날짜,시,노선,정류장) 로 중복 제거(나중 값 우선)
    uniq = {(d.get("event_date"), d.get("kst_hour"), d.get("route_id"), d.get("station_id")): d for d in drift}
    out.datasets["bus_arrival_drift_hourly"] = pick(uniq.values(), "bus_arrival_drift_hourly", snap)
    out.as_of["bus_arrival_drift_hourly"] = (drift_meta or {}).get("as_of")
    out.notes["bus_arrival_drift_hourly"] = list((drift_meta or {}).get("notes") or [])
    return out


# --------------------------------------------------------------------------------------------- 쓰기
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def write_all(col: Collected, out_dir: Path, *, now: datetime, days: int, api_calls: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for name, cols in COLUMNS.items():
        rows = col.datasets.get(name, [])
        path = out_dir / f"{name}.csv"
        tmp = path.with_suffix(".csv.tmp")
        # utf-8-sig: Tableau·Excel 이 한글(사고·공사)을 바로 읽는다
        with tmp.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow([_cell(r.get(c)) for c in cols])
        tmp.replace(path)
        files.append({"file": path.name, "rows": len(rows), "columns": cols, "sha256": _sha256(path),
                      "source_as_of": col.as_of.get(name), "notes": col.notes.get(name, [])})
    manifest = {
        "snapshot_at": utc_z(now.replace(microsecond=0)),
        "window_days": days,
        "time_zone_note": "kst_date·kst_hour·event_date 는 한국 시간(KST). snapshot_at·source_as_of 는 UTC(Z).",
        "complete": not col.incomplete,
        "incomplete_parts": col.incomplete,
        "warnings": col.warnings,
        "api_calls": api_calls,
        "contains_vehicle_identifiers": False,
        "day_type_note": "day_type_guess 는 평일/토/일 근사다(공휴일은 평일로 근사). 정확한 구분은 congestion_threshold 의 day_type.",
        "files": files,
        "publish_checklist": PUBLISH_CHECKLIST,
    }
    mp = out_dir / "manifest.json"
    tmp = mp.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(mp)
    return manifest


def preview_text(col: Collected, n: int = 3) -> str:
    lines = []
    for name, cols in COLUMNS.items():
        rows = col.datasets.get(name, [])
        lines.append(f"[{name}] {len(rows)}행 · 열 {len(cols)}개 · API 기준시각 {col.as_of.get(name) or '—'}")
        for r in rows[:n]:
            lines.append("   " + ", ".join(f"{c}={_cell(r.get(c))}" for c in cols[1:]))
    if col.incomplete:
        lines.append(f"! 불완전: {', '.join(col.incomplete)}")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------------- main
def main(argv: Optional[list[str]] = None, *, client: Optional[ApiClient] = None,
         now: Optional[datetime] = None) -> int:
    ap = argparse.ArgumentParser(description="Tableau 용 CSV 추출(API 만 읽음, 게시 안 함)")
    ap.add_argument("--env", default=str(ROOT / ".env"))
    ap.add_argument("--api-url", default=None)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--preview", action="store_true", help="파일을 쓰지 않고 행 수와 앞부분만 출력")
    args = ap.parse_args(argv)

    if not 1 <= args.days <= MAX_DAYS:
        print(f"--days 는 1~{MAX_DAYS} 이어야 합니다")
        return 2
    if client is None:
        envp = Path(args.env)
        if not envp.exists():
            print(f".env 가 없습니다: {envp}")
            return 2
        try:
            settings = settings_from_env(parse_env_text(envp.read_text(encoding="utf-8")), args.api_url)
        except ExportConfigError as e:
            print(e)
            return 2
        client = ApiClient(settings, on_budget="wait")
    now = now or datetime.now(timezone.utc)

    try:
        col = collect(client, now, args.days)
    except ApiError as e:
        print(f"API 오류: {e.short()}  (API 가 떠 있는지: scripts/stack.sh up-api · 키가 API_KEYS 에 있는지: scripts/stack.sh init-env)")
        return 3
    if args.preview:
        print(preview_text(col))
        print("(미리보기: 파일을 쓰지 않았습니다)")
        return 4 if col.incomplete else 0
    manifest = write_all(col, Path(args.out), now=now, days=args.days, api_calls=len(client.call_log))
    for f in manifest["files"]:
        print(f"{f['file']}: {f['rows']}행")
    print(f"저장 위치: {args.out}  (manifest.json 에 게시 전 점검표가 있습니다)")
    print("이 스크립트는 업로드하지 않습니다. Tableau Public 게시는 약관·공개 범위를 확인한 뒤 직접 하세요.")
    if col.incomplete:
        print(f"! 불완전한 부분이 있습니다: {', '.join(col.incomplete)}")
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
