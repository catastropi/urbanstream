"""STEP 2 결과 정리 (설계안 5·9장): 원시 기록 → 요약 CSV(docs/performance/*.csv) → docs/performance.md 의 표.

숫자는 이 스크립트가 CSV 에서 만든다. 손으로 고치지 않는다. docs/performance.md 의 <!-- measure:표이름 --> ~ <!-- /measure:표이름 --> 사이만
다시 쓰고 나머지 글은 그대로 둔다(문서가 없으면 뼈대를 만든다).

입력
  data/measure/results/*.jsonl    능동 측정(M5~M8) 한 회 한 줄 (measure_run.py)
  data/measure/samples/*.jsonl    5분 표본(M1 배치 시간, M2 자원, M4 gold-runner 소요) (measure_sample.py)
  data/bronze·silver 파티션 폴더   M3 날짜별 파일 수·바이트 (stat 만, --storage)
통계 규칙: 예열 제외, 유효 회만, 중앙값·최소~최대·유효 n·무효 n. 5회 이하에서 평균·표준편차를 쓰지 않는다.
전후 비교(M7): B 의 최댓값 < A 의 최솟값일 때만 '빨라졌다', 범위가 겹치면 '차이 불분명'.

사용:  python scripts/measure_report.py [--storage] [--since 2026-11-25] [--no-doc]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from measure_guard import KST, ROOT, _resolve, load_config  # noqa: E402

MIB = 1024 ** 2


# ------------------------------------------------------------------ 통계 (순수)

def pct(vals: list[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize(vals: list[float]) -> dict:
    v = [float(x) for x in vals if x is not None]
    if not v:
        return {"n": 0, "median": None, "min": None, "max": None}
    return {"n": len(v), "median": round(statistics.median(v), 2), "min": round(min(v), 2), "max": round(max(v), 2)}


def compare_ab(a: list[float], b: list[float], min_cut: float) -> dict:
    """A(원본) 대 B(사본). 판정: B 최댓값 < A 최솟값 → '빨라졌다', A 최댓값 < B 최솟값 → '느려졌다', 그 밖 '차이 불분명'."""
    sa, sb = summarize(a), summarize(b)
    if not sa["n"] or not sb["n"]:
        return {"verdict": "측정 부족", "ratio": None, "cut": None, "meets_cut": None, "a": sa, "b": sb}
    ratio = round(sb["median"] / sa["median"], 3) if sa["median"] else None
    if sb["max"] < sa["min"]:
        verdict = "빨라졌다"
    elif sa["max"] < sb["min"]:
        verdict = "느려졌다"
    else:
        verdict = "차이 불분명"
    cut = None if ratio is None else round(1 - ratio, 3)
    return {"verdict": verdict, "ratio": ratio, "cut": cut, "meets_cut": verdict == "빨라졌다" and cut is not None and cut >= min_cut,
            "a": sa, "b": sb}


# ------------------------------------------------------------------ 읽기

def read_jsonl(paths: Iterable[Path]) -> list[dict]:
    out = []
    for p in sorted(paths):
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        continue
    return out


# ------------------------------------------------------------------ 능동 측정

ACTIVE_COLS = ["item", "data", "scale", "variant", "endpoint", "valid_n", "invalid_n", "median_sec", "min_sec", "max_sec",
               "median_mem_mib", "max_mem_mib", "slow_first", "input", "dates", "env_files", "notes"]


def active_summary(rows: list[dict], cfg: dict) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("item") in (None, "watch", "selftest"):
            continue
        groups[(r["item"], r.get("data") or "", r.get("scale") or "", r.get("variant") or "", r.get("endpoint") or "")].append(r)
    out = []
    for (item, data, scale, variant, ep), rs in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        good = [r for r in rs if r.get("valid") and not r.get("warmup")]
        bad = [r for r in rs if not r.get("valid")]
        if item == "m8_api":
            vals = [x / 1000 for r in good for x in r.get("ms") or []]
        else:
            vals = [r.get("elapsed_sec") for r in good]
        s = summarize(vals)
        mem = summarize([(r.get("mem_max_bytes") or 0) / MIB for r in good if r.get("mem_max_bytes")])
        ordered = sorted(good, key=lambda r: r.get("started_kst") or "")
        slow = False
        if len(ordered) >= 3:
            rest = [r.get("elapsed_sec") or 0 for r in ordered[1:]]
            slow = (ordered[0].get("elapsed_sec") or 0) > statistics.median(rest) * float(cfg["repeat"]["slow_first_ratio"])
        inp = rs[-1]
        out.append({"item": item, "data": data, "scale": scale, "variant": variant, "endpoint": ep, "valid_n": s["n"], "invalid_n": len(bad),
                    "median_sec": s["median"], "min_sec": s["min"], "max_sec": s["max"],
                    "median_mem_mib": mem["median"], "max_mem_mib": mem["max"], "slow_first": slow,
                    "input": f"{inp.get('input_date') or ''} 행 {inp.get('input_rows') or ''} 파일 {inp.get('input_files') or ''}".strip(),
                    "dates": ",".join(sorted({(r.get("started_kst") or "")[:10] for r in rs})),
                    "env_files": ",".join(sorted({r.get("env_file") for r in rs if r.get("env_file")})),
                    "notes": "; ".join(sorted({r.get("invalid_reason") for r in bad if r.get("invalid_reason")}))[:300]})
    return out


def m7_verdicts(rows: list[dict], cfg: dict) -> list[dict]:
    out = []
    for item in ("m7_build", "m7_verify"):
        rs = [r for r in rows if r.get("item") == item and r.get("valid")]
        if not rs:
            continue
        for d in sorted({r.get("input_date") for r in rs}):
            a = [r["elapsed_sec"] for r in rs if r.get("input_date") == d and r.get("variant") == "original"]
            b = [r["elapsed_sec"] for r in rs if r.get("input_date") == d and r.get("variant") == "copy"]
            c = compare_ab(a, b, float(cfg["decide"]["h5_min_read_cut"]))
            out.append({"item": item, "input_date": d, "original_n": c["a"]["n"], "original_median": c["a"]["median"],
                        "original_range": f"{c['a']['min']}~{c['a']['max']}", "copy_n": c["b"]["n"], "copy_median": c["b"]["median"],
                        "copy_range": f"{c['b']['min']}~{c['b']['max']}", "ratio": c["ratio"], "verdict": c["verdict"],
                        "meets_h5": c["meets_cut"]})
    return out


def m2_decision(active: list[dict], cfg: dict) -> dict:
    """M-2(배치 COPY) 조건: 10× 로더 최대 메모리 > 기준 또는 5×→10× 적재 시간비 > 기준."""
    load = {str(r["scale"]): r for r in active if r["item"] == "m6_synth_load" and r["valid_n"]}
    d = cfg["decide"]
    r5, r10 = load.get("5"), load.get("10")
    mem10 = r10["max_mem_mib"] if r10 else None
    ratio = round(r10["median_sec"] / r5["median_sec"], 2) if r5 and r10 and r5["median_sec"] else None
    if r10 is None or r5 is None:
        verdict = "측정 부족"
    elif (mem10 or 0) > d["m2_loader_peak_mib"] or (ratio or 0) > d["m2_time_ratio_5_to_10"]:
        verdict = "조건 넘음: 배치 COPY 로 바꾼다"
    else:
        verdict = "조건 안 넘음: 지금 구조로 충분"
    return {"loader_peak_10x_mib": mem10, "time_ratio_5_to_10": ratio, "verdict": verdict}


# ------------------------------------------------------------------ 수동 측정 (표본)

def day_of(ts: float) -> str:
    return datetime.fromtimestamp(ts, KST).date().isoformat()


def coverage(samples: list[dict], interval: float) -> dict[str, dict]:
    """날짜별 표본 수와 표본 사이 공백 합(간격의 2배를 넘는 틈). 30분 미만이면 '온전한 날'(표본 기준).
    Docker 가 응답하지 않던 표본("docker": false)은 스택이 꺼져 있던 것이므로 세지 않는다."""
    by: dict[str, list[float]] = defaultdict(list)
    for s in samples:
        if s.get("docker") is False:
            continue
        by[day_of(s["ts"])].append(s["ts"])
    out = {}
    for d, ts in by.items():
        ts.sort()
        start = datetime.fromisoformat(d).replace(tzinfo=KST).timestamp()
        edges = [start] + ts + [start + 86400]
        gap = sum(b - a - interval for a, b in zip(edges, edges[1:]) if b - a > 2 * interval)
        out[d] = {"samples": len(ts), "gap_min": round(gap / 60, 1), "full": gap < 1800}
    return out


def m1_summary(samples: list[dict], trigger_sec: float, cov: dict, since: Optional[str]) -> list[dict]:
    per: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    seen = set()
    for s in samples:
        for b in s.get("batches") or []:
            k = (b["layer"], b["query"], b["batch"], b["end"])
            if k in seen:
                continue
            seen.add(k)
            d = day_of(b["end"])
            if since and d < since:
                continue
            per[(b["layer"], b["query"])][d].append(float(b["sec"]))
    out = []
    for (layer, q), days in sorted(per.items()):
        daily = {d: statistics.median(v) for d, v in days.items()}
        allv = [x for v in days.values() for x in v]
        full = [d for d in days if cov.get(d, {}).get("full")]
        out.append({"layer": layer, "query": q, "days": len(days), "full_days": len(full), "batches": len(allv),
                    "daily_median_median": round(statistics.median(daily.values()), 2), "batch_p90": round(pct(allv, 0.9), 2),
                    "batch_max": round(max(allv), 2), "over_trigger_pct": round(100 * sum(1 for x in allv if x > trigger_sec) / len(allv), 2)})
    return out


def m2_summary(samples: list[dict], since: Optional[str]) -> list[dict]:
    cpu: dict[str, list[float]] = defaultdict(list)
    mem: dict[str, list[float]] = defaultdict(list)
    pids: dict[str, list[float]] = defaultdict(list)
    unh: dict[str, int] = defaultdict(int)
    vm_free: list[float] = []
    for s in samples:
        if since and day_of(s["ts"]) < since:
            continue
        tot = sum(c.get("mem_bytes") or 0 for c in s.get("containers") or [] if c.get("state") == "running")
        if (s.get("vm") or {}).get("mem_total"):
            vm_free.append((s["vm"]["mem_total"] - tot) / MIB)
        for c in s.get("containers") or []:
            if c.get("state") != "running":
                continue
            n = c.get("service") or c.get("name")
            cpu[n].append(float(c.get("cpu_pct") or 0))
            mem[n].append((c.get("mem_bytes") or 0) / MIB)
            pids[n].append(float(c.get("pids") or 0))
            unh[n] += c.get("health") == "unhealthy"
    out = [{"container": n, "samples": len(cpu[n]), "cpu_median_pct": round(statistics.median(cpu[n]), 1),
            "cpu_p90_pct": round(pct(cpu[n], 0.9), 1), "cpu_max_pct": round(max(cpu[n]), 1),
            "mem_median_mib": round(statistics.median(mem[n]), 0), "mem_max_mib": round(max(mem[n]), 0),
            "pids_max": int(max(pids[n])), "unhealthy_samples": unh[n]} for n in sorted(cpu)]
    if vm_free:
        out.append({"container": "(VM 여유)", "samples": len(vm_free), "cpu_median_pct": None, "cpu_p90_pct": None, "cpu_max_pct": None,
                    "mem_median_mib": round(statistics.median(vm_free), 0), "mem_max_mib": round(min(vm_free), 0),
                    "pids_max": None, "unhealthy_samples": None})
    return out


def dag_of(key: Optional[str]) -> str:
    return (key or "").split("__", 1)[0] or "?"


def m4_summary(samples: list[dict], since: Optional[str]) -> list[dict]:
    by: dict[tuple, list[dict]] = defaultdict(list)
    seen = set()
    for s in samples:
        for r in s.get("runner_done") or []:
            if r["id"] in seen or (since and day_of(r["end"]) < since):
                continue
            seen.add(r["id"])
            by[(r.get("kind") or "gold_stage", dag_of(r.get("key")), bool(r.get("incremental")))].append(r)
    out = []
    for (kind, dag, inc), rs in sorted(by.items()):
        ok = [r["elapsed_sec"] for r in rs if r["result"] == "done" and r.get("elapsed_sec") is not None]
        s = summarize(ok)
        out.append({"kind": kind, "dag": dag, "incremental": inc, "runs": len(rs), "failed": sum(1 for r in rs if r["result"] == "failed"),
                    "median_sec": s["median"], "p90_sec": round(pct(ok, 0.9), 1) if ok else None, "max_sec": s["max"]})
    return out


def m3_storage(root: Path, since: Optional[str]) -> list[dict]:
    """data/{bronze,silver}/<표>/<파티션>=날짜/ 의 파일 수·바이트. stat 만 한다(내용을 읽지 않는다)."""
    out = []
    for layer in ("bronze", "silver"):
        base = root / "data" / layer
        if not base.is_dir():
            continue
        for t in sorted(p for p in base.iterdir() if p.is_dir()):
            for part in sorted(t.glob("*=*")):
                d = part.name.split("=", 1)[1]
                if since and d < since:
                    continue
                n = b = 0
                with os.scandir(part) as it:
                    for e in it:
                        if e.name.endswith(".parquet") and not e.name.startswith((".", "_")):
                            n += 1
                            b += e.stat().st_size
                out.append({"layer": layer, "table": t.name, "date": d, "files": n, "bytes": b,
                            "avg_kib": round(b / n / 1024, 1) if n else None})
    return out


# ------------------------------------------------------------------ 쓰기

def write_csv(path: Path, rows: list[dict], cols: Optional[list[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = cols or (list(rows[0]) if rows else [])
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def md_table(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    if not rows:
        return "_아직 기록 없음_"
    head = "| " + " | ".join(h for _, h in cols) + " |\n|" + "---|" * len(cols) + "\n"
    def cell(v: Any) -> str:
        if v is None or v == "":
            return "-"
        if isinstance(v, bool):
            return "예" if v else "아니오"
        return str(v).replace("|", "/")
    return head + "\n".join("| " + " | ".join(cell(r.get(k)) for k, _ in cols) + " |" for r in rows)


DOC_SKELETON = """# 성능·규모 측정 결과 (STEP 2)

측정 방법과 안전장치는 [runbooks/measure.md](runbooks/measure.md), 설계 근거는 STEP 2 설계안이다.
아래 표는 `scripts/measure_report.py` 가 `docs/performance/*.csv` 에서 만든다. 손으로 고치지 않는다.
모든 값은 운영 중(수집 켜 둠) Mac 한 대에서 잰 것이다. 파일 캐시는 비우지 않았다(warm).

## 측정 환경

환경 JSON 은 `docs/performance/env-*.json`. 표의 '환경' 열이 그 파일 이름이다.

## 실제 데이터: 수동 지표 (M1~M4)

### M1 스트리밍 마이크로배치 소요(초)
<!-- measure:m1 -->
<!-- /measure:m1 -->

### M2 컨테이너 자원
<!-- measure:m2 -->
<!-- /measure:m2 -->

### M3 날짜별 저장량
<!-- measure:m3 -->
<!-- /measure:m3 -->

### M4 정기 배치 소요(gold-runner)
<!-- measure:m4 -->
<!-- /measure:m4 -->

## 능동 측정 (M5~M8)
<!-- measure:active -->
<!-- /measure:active -->

## 작은 파일 비교 (M7)
<!-- measure:m7 -->
<!-- /measure:m7 -->

## 개선 작업 판정 (설계안 2-3)
<!-- measure:decide -->
<!-- /measure:decide -->

## 한계
- 합성 데이터는 모든 행이 정상(`_parse_ok`)이다. 늘린 링크(S 접두)는 `dim_link` 에 없어 링크 조인 마트는 재지 않았다.
- 운영 중 측정이라 같은 조건을 다시 만들 수 없다. 반복·중앙값·번갈아 재기로 흔들림을 줄였다.
"""


def replace_section(doc: str, name: str, body: str) -> str:
    pat = re.compile(rf"(<!-- measure:{name} -->\n)(.*?)(<!-- /measure:{name} -->)", re.S)
    if not pat.search(doc):
        return doc + f"\n<!-- measure:{name} -->\n{body}\n<!-- /measure:{name} -->\n"
    return pat.sub(lambda m: m.group(1) + body + "\n" + m.group(3), doc)


def build(cfg: dict, root: Path = ROOT, *, storage: bool = False, since: Optional[str] = None, write_doc: bool = True) -> dict:
    res = read_jsonl(_resolve(cfg["paths"]["results"], root).glob("*.jsonl")) if _resolve(cfg["paths"]["results"], root).is_dir() else []
    sdir = _resolve(cfg["paths"]["samples"], root)
    samples = [s for s in read_jsonl(sdir.glob("*.jsonl")) if s.get("docker")] if sdir.is_dir() else []
    since = since or cfg["collection_start"]
    out_dir = _resolve(cfg["paths"]["report_dir"], root)
    active = active_summary(res, cfg)
    m7 = m7_verdicts(res, cfg)
    dec = m2_decision(active, cfg)
    cov = coverage(samples, float(cfg["sample"]["interval_sec"]))
    m1 = m1_summary(samples, 60.0, cov, since)
    m2 = m2_summary(samples, since)
    m4 = m4_summary(samples, since)
    m3 = m3_storage(root, since) if storage else None
    write_csv(out_dir / "active.csv", active, ACTIVE_COLS)
    write_csv(out_dir / "m7_compare.csv", m7)
    write_csv(out_dir / "m1_streaming.csv", m1)
    write_csv(out_dir / "m2_resources.csv", m2)
    write_csv(out_dir / "m4_batches.csv", m4)
    write_csv(out_dir / "coverage.csv", [{"date": d, **v} for d, v in sorted(cov.items())])
    if m3 is not None:
        write_csv(out_dir / "m3_storage.csv", m3)
    if write_doc:
        p = root / "docs" / "performance.md"
        doc = p.read_text(encoding="utf-8") if p.exists() else DOC_SKELETON
        full = sum(1 for v in cov.values() if v["full"])
        doc = replace_section(doc, "m1", f"집계 {since} 이후, 표본 기준 온전한 날 {full}/{len(cov)}일. 트리거 60초.\n\n" + md_table(m1, [
            ("layer", "층"), ("query", "쿼리"), ("days", "가동일"), ("full_days", "온전한 날"), ("batches", "배치 수"),
            ("daily_median_median", "날짜별 중앙값의 중앙값"), ("batch_p90", "p90"), ("batch_max", "최대"), ("over_trigger_pct", "60초 넘은 %")]))
        doc = replace_section(doc, "m2", md_table(m2, [("container", "컨테이너"), ("samples", "표본"), ("cpu_median_pct", "CPU 중앙값 %"),
            ("cpu_p90_pct", "p90 %"), ("cpu_max_pct", "최대 %"), ("mem_median_mib", "메모리 중앙값 MiB"), ("mem_max_mib", "최대 MiB (VM 여유는 최소)"),
            ("pids_max", "PIDS 최대"), ("unhealthy_samples", "unhealthy 표본")]))
        if m3 is not None:
            agg: dict[tuple, dict] = {}
            for r in m3:
                a = agg.setdefault((r["layer"], r["table"]), {"layer": r["layer"], "table": r["table"], "days": 0, "files": [], "mib": []})
                a["days"] += 1
                a["files"].append(r["files"])
                a["mib"].append(r["bytes"] / MIB)
            m3s = [{"layer": a["layer"], "table": a["table"], "days": a["days"], "files_median": statistics.median(a["files"]),
                    "files_max": max(a["files"]), "mib_median": round(statistics.median(a["mib"]), 1), "mib_total": round(sum(a["mib"]), 1)}
                   for a in agg.values()]
            doc = replace_section(doc, "m3", md_table(m3s, [("layer", "층"), ("table", "표"), ("days", "날 수"), ("files_median", "하루 파일 중앙값"),
                ("files_max", "하루 파일 최대"), ("mib_median", "하루 MiB 중앙값"), ("mib_total", "합계 MiB")]))
        doc = replace_section(doc, "m4", md_table(m4, [("kind", "종류"), ("dag", "DAG"), ("incremental", "증분"), ("runs", "실행"), ("failed", "실패"),
            ("median_sec", "중앙값 초"), ("p90_sec", "p90"), ("max_sec", "최대")]))
        doc = replace_section(doc, "active", md_table(active, [("item", "항목"), ("data", "데이터"), ("scale", "규모"), ("variant", "변형"),
            ("endpoint", "엔드포인트"), ("valid_n", "유효 n"), ("invalid_n", "무효 n"), ("median_sec", "중앙값 초"), ("min_sec", "최소"),
            ("max_sec", "최대"), ("max_mem_mib", "최대 메모리 MiB"), ("slow_first", "첫 회 느림"), ("input", "입력"), ("dates", "측정일"),
            ("env_files", "환경"), ("notes", "무효 이유")]))
        doc = replace_section(doc, "m7", md_table(m7, [("item", "항목"), ("input_date", "날짜"), ("original_n", "원본 n"),
            ("original_median", "원본 중앙값"), ("original_range", "원본 범위"), ("copy_n", "사본 n"), ("copy_median", "사본 중앙값"),
            ("copy_range", "사본 범위"), ("ratio", "사본/원본"), ("verdict", "판정"), ("meets_h5", "compaction 설계 조건")]))
        h5 = [r for r in m7 if r.get("meets_h5")]
        doc = replace_section(doc, "decide", "\n".join([
            f"- M-2 배치 COPY: 10× 로더 최대 메모리 {dec['loader_peak_10x_mib']} MiB (기준 {cfg['decide']['m2_loader_peak_mib']}), "
            f"5×→10× 적재 시간비 {dec['time_ratio_5_to_10']} (기준 {cfg['decide']['m2_time_ratio_5_to_10']}) → {dec['verdict']}",
            "- H-5 compaction: " + ("측정 부족" if not m7 else ("조건 넘음: compaction 설계안을 따로 올린다" if h5
                                                               else f"조건 안 넘음(읽기 시간 {int(cfg['decide']['h5_min_read_cut'] * 100)}% 이상 줄지 않음)"))]))
        p.write_text(doc, encoding="utf-8")
    return {"active": active, "m7": m7, "decide": dec, "m1": m1, "m2": m2, "m4": m4, "m3": m3, "coverage": cov}


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "measure.yaml"))
    ap.add_argument("--storage", action="store_true", help="M3: 파티션 폴더를 stat 으로 센다 (몇 분 걸릴 수 있다)")
    ap.add_argument("--since", default=None, help="집계 시작일 (기본 collection_start)")
    ap.add_argument("--no-doc", action="store_true", help="CSV 만 쓰고 docs/performance.md 는 건드리지 않는다")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    r = build(cfg, storage=a.storage, since=a.since, write_doc=not a.no_doc)
    print(f"능동 {len(r['active'])}줄, M1 {len(r['m1'])}줄, M2 {len(r['m2'])}줄, M4 {len(r['m4'])}줄, 표본 날짜 {len(r['coverage'])}일 → "
          f"{_resolve(cfg['paths']['report_dir'], ROOT)}")
    print(f"M-2 판정: {r['decide']['verdict']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
