#!/usr/bin/env python3
"""cadence 가 data/smoke/raw/<api>/ 에 저장해 둔 원본 응답을 필드 단위로 비교한다 (네트워크 호출 없음).

cadence 의 '응답 전체 해시' 는 카운트다운 필드가 있는 소스(버스도착)에서 항상 '변경'으로 나오고,
돌발에서는 건수만 달라도 100% 로 계산된다. 여기서는 실제로 *어느 필드가* 바뀌는지 센다.

사용 (프로젝트 루트):
    python scripts/analyze_smoke_raw.py incident     --since 2026-11-02T13:00
    python scripts/analyze_smoke_raw.py bus_position --since 2026-11-03T14:00
    python scripts/analyze_smoke_raw.py bus_arrival  --since 2026-11-02T13:00

--since/--until 은 KST 기준 'YYYY-MM-DDTHH:MM'. 파일 이름의 저장 시각(KST)으로 거른다.
출력에는 인증키가 없다 (본문에는 키가 들어 있지 않다). 개수와 분포만 출력한다.
"""
from __future__ import annotations

import argparse
import statistics
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "smoke" / "raw"
API_DIRS = {"incident": "seoul_incident", "bus_position": "bus_position", "bus_arrival": "bus_arrival"}
ROW_TAGS = {"incident": "row", "bus_position": "itemList", "bus_arrival": "itemList"}


# ----------------------------------------------------------------------------
# 읽기
# ----------------------------------------------------------------------------
def stamp_of(path: Path) -> datetime:
    """'20261102T132641_KST.xml' -> datetime (KST 로 간주, tz 없음)."""
    return datetime.strptime(path.name.split("_")[0], "%Y%m%dT%H%M%S")


def parse_rows(xml_text: str, row_tag: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    rows = []
    for el in root.iter(row_tag):
        rows.append({child.tag: (child.text or "").strip() for child in el})
    return rows


def load_series(api_dir: Path, row_tag: str, since: datetime | None, until: datetime | None):
    """[(저장 시각, rows)] 를 시간순으로. 같은 초에 겹친 파일은 이름 순서."""
    out = []
    for p in sorted(api_dir.glob("*.xml")):
        try:
            t = stamp_of(p)
        except ValueError:
            continue
        if (since and t < since) or (until and t > until):
            continue
        try:
            rows = parse_rows(p.read_text(encoding="utf-8"), row_tag)
        except ET.ParseError:
            continue
        out.append((t, rows))
    return out


# ----------------------------------------------------------------------------
# 돌발: acc_id 별로 무엇이 바뀌었나
# ----------------------------------------------------------------------------
# contracts/incident.py 의 content_hash 입력 필드와 같아야 한다 (dedup_key 에 들어가는 값).
INCIDENT_HASH_FIELDS = ("exp_clr_date", "exp_clr_time", "acc_type", "acc_dtype", "link_id", "acc_info")


def diff_incident(prev: list[dict], cur: list[dict]) -> dict:
    p = {r.get("acc_id"): r for r in prev if r.get("acc_id")}
    c = {r.get("acc_id"): r for r in cur if r.get("acc_id")}
    added, removed = set(c) - set(p), set(p) - set(c)
    changed_fields: Counter = Counter()
    changed_ids, new_versions = 0, 0
    for acc_id in set(p) & set(c):
        fields = [k for k in set(p[acc_id]) | set(c[acc_id]) if p[acc_id].get(k) != c[acc_id].get(k)]
        if fields:
            changed_ids += 1
            changed_fields.update(fields)
            if any(f in INCIDENT_HASH_FIELDS for f in fields):
                new_versions += 1   # content_hash 가 달라져 dedup_key 가 새로 생긴다
    return {"added": len(added), "removed": len(removed), "changed_ids": changed_ids,
            "new_versions": new_versions, "fields": changed_fields, "same_ids": len(set(p) & set(c))}


def report_incident(series) -> list[str]:
    if len(series) < 2:
        return ["파일이 2개 미만이라 비교할 수 없습니다."]
    tot = Counter(); fields: Counter = Counter()
    n_pairs = n_quiet = 0
    for (_, a), (_, b) in zip(series, series[1:]):
        d = diff_incident(a, b)
        n_pairs += 1
        quiet = not (d["added"] or d["removed"] or d["changed_ids"])
        n_quiet += quiet
        for k in ("added", "removed", "changed_ids", "new_versions", "same_ids"):
            tot[k] += d[k]
        fields.update(d["fields"])
    L = [f"- 비교한 연속 쌍: {n_pairs}개 (저장된 파일은 '변경이 있었던 호출'만이라, 쌍 사이 간격은 일정하지 않다)",
         f"- 합계: 새로 나타남 {tot['added']} / 사라짐 {tot['removed']} / 같은 acc_id 의 내용 변경 {tot['changed_ids']}"
         f" (같은 acc_id 비교 {tot['same_ids']}건 중)",
         f"- **content_hash 가 바뀌어 새 버전이 되는 변경: {tot['new_versions']}건** (해제 예정 시각·유형·링크·설명이 바뀐 경우)",
         "", "| 바뀐 필드 | 횟수 |", "|---|---|"]
    for k, v in fields.most_common():
        mark = " ← hash 입력" if k in INCIDENT_HASH_FIELDS else ""
        L.append(f"| {k}{mark} | {v} |")
    if tot["changed_ids"] == 0:
        L.append("| (같은 acc_id 의 필드 변경 없음) | 0 |")
    return L


# ----------------------------------------------------------------------------
# 버스위치: dataTm
# ----------------------------------------------------------------------------
def parse_data_tm(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S")
    except (ValueError, TypeError):
        return None


def report_bus_position(series) -> list[str]:
    per_veh: dict[str, list[tuple[datetime, datetime]]] = {}   # veh -> [(저장 시각, dataTm)]
    ages = []
    for t, rows in series:
        for r in rows:
            dt = parse_data_tm(r.get("dataTm", ""))
            if dt is None or not r.get("vehId"):
                continue
            per_veh.setdefault(r["vehId"], []).append((t, dt))
            ages.append((t - dt).total_seconds())
    if not per_veh:
        return ["dataTm 을 읽을 수 없습니다."]
    jumps, repeats, obs = [], 0, 0
    for seq in per_veh.values():
        for (t0, d0), (t1, d1) in zip(seq, seq[1:]):
            obs += 1
            if d1 == d0:
                repeats += 1
            else:
                jumps.append((d1 - d0).total_seconds())
    file_gaps = [(b[0] - a[0]).total_seconds() for a, b in zip(series, series[1:])]
    L = [f"- 파일 {len(series)}개, 차량 {len(per_veh)}대, 관측 쌍 {obs}개",
         f"- 호출 간격(저장 시각 기준): 중앙값 {statistics.median(file_gaps):.0f}s" if file_gaps else "",
         f"- 같은 차량의 dataTm 이 **그대로인** 쌍: {repeats}/{obs} ({100 * repeats / max(obs, 1):.0f}%)"]
    if jumps:
        js = sorted(jumps)
        L += [f"- dataTm 이 바뀐 쌍의 증가량(초): 중앙값 {statistics.median(js):.0f} / 최소 {js[0]:.0f} / 최대 {js[-1]:.0f}",
              "  (호출 간격보다 증가량이 훨씬 작으면 차량이 호출 사이에 여러 번 보고한다는 뜻 — 간격을 줄일 이유가 생긴다)"]
    if ages:
        a = sorted(ages)
        L.append(f"- 호출 시점에서 본 데이터 나이(저장 시각 − dataTm, 초): 중앙값 {statistics.median(a):.0f} / "
                 f"p95 {a[int(0.95 * (len(a) - 1))]:.0f}  (저장 시각은 응답 후 기록돼 수 초 늦다)")
    return [x for x in L if x]


# ----------------------------------------------------------------------------
# 버스도착: mkTm
# ----------------------------------------------------------------------------
def parse_mk_tm(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            pass
    return None


def report_bus_arrival(series) -> list[str]:
    mk_per_file = []
    for t, rows in series:
        vals = Counter(r.get("mkTm", "") for r in rows)
        top, cnt = (vals.most_common(1)[0] if vals else ("", 0))
        mk_per_file.append((t, parse_mk_tm(top), len(vals), len(rows), cnt))
    if not mk_per_file:
        return ["파일이 없습니다."]
    changes = sum(1 for a, b in zip(mk_per_file, mk_per_file[1:]) if a[1] != b[1])
    multi = sum(1 for m in mk_per_file if m[2] > 1)
    ages = sorted((t - mk).total_seconds() for t, mk, *_ in mk_per_file if mk)
    L = [f"- 파일 {len(mk_per_file)}개. 응답의 대표 mkTm 이 바뀐 횟수: {changes}/{max(len(mk_per_file) - 1, 0)}",
         f"- 한 응답 안에 mkTm 값이 둘 이상인 응답: {multi}개"]
    if ages:
        L.append(f"- 저장 시각 − mkTm (초): 중앙값 {statistics.median(ages):.0f} / 최소 {ages[0]:.0f} / 최대 {ages[-1]:.0f}"
                 "  (mkTm 은 분 단위라 0~60초 안에 들어오면 '그 분에 만든 값')")
    L.append("- 해석: mkTm 이 호출마다 바뀌면 서버가 매분 새로 계산한다는 뜻, 몇 호출 동안 같으면 그 간격이 갱신 주기다.")
    return L


# ----------------------------------------------------------------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=sorted(API_DIRS))
    ap.add_argument("--since", help="KST 'YYYY-MM-DDTHH:MM' 이후 파일만")
    ap.add_argument("--until", help="KST 'YYYY-MM-DDTHH:MM' 이전 파일만")
    ap.add_argument("--dir", default=str(RAW_DIR), help="원본 저장 폴더 (기본: data/smoke/raw)")
    a = ap.parse_args(argv)
    since = datetime.fromisoformat(a.since) if a.since else None
    until = datetime.fromisoformat(a.until) if a.until else None
    d = Path(a.dir) / API_DIRS[a.what]
    if not d.is_dir():
        print(f"폴더가 없습니다: {d}")
        return 2
    series = load_series(d, ROW_TAGS[a.what], since, until)
    print(f"# 원본 필드 분석: {a.what}  ({d})")
    if series:
        print(f"- 대상 파일 {len(series)}개: {series[0][0]:%Y-%m-%d %H:%M:%S} ~ {series[-1][0]:%H:%M:%S} (KST)")
    else:
        print("- 조건에 맞는 파일이 없습니다. --since/--until 을 확인하세요.")
        return 1
    fn = {"incident": report_incident, "bus_position": report_bus_position, "bus_arrival": report_bus_arrival}[a.what]
    print("\n".join(fn(series)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
