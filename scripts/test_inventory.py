#!/usr/bin/env python3
"""테스트 목록과 실행 결과를 손으로 세지 않고 만든다 (STEP 5, ADR-0018).

  python scripts/test_inventory.py scan                    계층·필요 환경별 테스트 함수 수 (코드만 읽음, 환경과 무관)
  python scripts/test_inventory.py scan --json
  python scripts/test_inventory.py readme --check          README·docs/testing.md 의 자동 생성 칸이 지금 코드와 같은지 (CI hygiene)
  python scripts/test_inventory.py readme --write          자동 생성 칸을 다시 쓴다
  python scripts/test_inventory.py summarize a.xml b.xml   pytest --junitxml 결과를 계층별 passed/failed/skipped 표로 (CI 요약, 기록용)

숫자 두 가지를 나눠서 다룬다.
- 테스트 함수 수(scan): `def test_` 를 AST 로 센다. 설치된 패키지와 상관없이 늘 같은 값이라 CI 에서 문서와 대조할 수 있다.
- 실행 결과(summarize): 파라미터 확장 뒤 개수와 passed/skipped 는 환경(PG, Spark, Kafka 유무)에 따라 다르다.
  그래서 문서와 자동 대조하지 않고, 날짜·환경과 함께 docs/testing.md 의 기록 표에 남긴다.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "support"))
from layers_plugin import LAYERS, layers_for  # noqa: E402

NEEDS_ORDER = ("없음", "PostgreSQL", "Spark", "Spark+PostgreSQL", "Kafka", "Spark+Kafka")
MUTATION_SCRIPTS = (("api", "scripts/mutation_api.py"), ("dashboard", "scripts/mutation_dashboard.py"),
                    ("ci", "scripts/mutation_ci.py"), ("data", "scripts/mutation_data.py"))
BLOCKS = {"README.md": "readme", "docs/testing.md": "testing"}
START, END = "<!-- tests:{name}:start -->", "<!-- tests:{name}:end -->"


# ------------------------------------------------------------------ 코드 읽기

def test_files(root: Path = ROOT) -> list[Path]:
    return sorted(p for p in (root / "tests").rglob("test_*.py") if "__pycache__" not in p.parts)


def count_tests(source: str) -> int:
    """모듈 수준 def test_* 와 class Test* 안의 def test_* 개수. 파라미터 확장 전 값."""
    tree = ast.parse(source)
    n = 0
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
            n += 1
        elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            n += sum(isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)) and b.name.startswith("test_") for b in node.body)
    return n


def needs_of(rel: str, source: str) -> str:
    """파일 단위로 무엇이 있어야 실제로 도는지. 없으면 그 테스트는 skip 된다."""
    spark = rel.startswith("tests/spark/")
    pg = "pg_support" in source or "GOLD_TEST_DSN" in source
    kafka = "kafka_broker" in source or "URBANSTREAM_TEST_KAFKA_HOME" in source
    if spark and kafka:
        return "Spark+Kafka"
    if kafka:
        return "Kafka"
    if spark and pg:
        return "Spark+PostgreSQL"
    if spark:
        return "Spark"
    return "PostgreSQL" if pg else "없음"


def scan(root: Path = ROOT) -> dict:
    files = []
    for p in test_files(root):
        rel = p.relative_to(root).as_posix()
        src = p.read_text(encoding="utf-8")
        files.append({"path": rel, "tests": count_tests(src), "layers": list(layers_for(rel)), "needs": needs_of(rel, src)})
    by_layer: Counter = Counter()
    by_needs: Counter = Counter()
    for f in files:
        by_layer[(f["layers"] or ["other"])[0]] += f["tests"]
        by_needs[f["needs"]] += f["tests"]
    return {
        "total_tests": sum(f["tests"] for f in files), "total_files": len(files),
        "by_layer": {k: by_layer[k] for k in (*LAYERS, "other") if by_layer[k]},
        "data_overlap": sum(f["tests"] for f in files if "data" in f["layers"]),
        "by_needs": {k: by_needs[k] for k in NEEDS_ORDER if by_needs[k]},
        "mutations": mutation_counts(root),
        "files": files,
    }


def mutation_counts(root: Path = ROOT) -> dict[str, int]:
    """각 뮤테이션 스크립트의 MUTATIONS 항목 수. import 하지 않고 AST 로 센다(스크립트가 무거운 의존성을 import 해도 괜찮게)."""
    out = {}
    for name, rel in MUTATION_SCRIPTS:
        p = root / rel
        if not p.exists():
            continue
        for node in ast.parse(p.read_text(encoding="utf-8")).body:
            target = node.targets[0] if isinstance(node, ast.Assign) else node.target if isinstance(node, ast.AnnAssign) else None
            if isinstance(target, ast.Name) and target.id == "MUTATIONS" and isinstance(node.value, (ast.List, ast.Tuple)):
                out[name] = len(node.value.elts)
    return out


# ------------------------------------------------------------------ 문서 칸

def render(name: str, s: dict) -> str:
    m = s["mutations"]
    if name == "readme":
        layers = ", ".join(f"{k} {v}" for k, v in s["by_layer"].items())
        return (f"테스트 함수 {s['total_tests']}개(파일 {s['total_files']}개, 파라미터 확장 전): {layers}. "
                f"뮤테이션 테스트는 데이터 로직 {m.get('data', 0)}건, API {m.get('api', 0)}건, 대시보드 {m.get('dashboard', 0)}건, "
                f"CI 설정 {m.get('ci', 0)}건. 실행 결과와 환경은 [docs/testing.md](docs/testing.md)")
    if name == "testing":
        lines = ["| 계층 | 테스트 함수 |", "|---|---:|"]
        lines += [f"| {k} | {v} |" for k, v in s["by_layer"].items()]
        lines += [f"| 합계 | {s['total_tests']} |", "",
                  f"`data` 표시가 붙은 테스트 {s['data_overlap']}개는 위 계층에 이미 들어 있다(다시 분류한 것이라 합계에 더하지 않는다).", "",
                  "| 필요한 것 (없으면 skip) | 테스트 함수 |", "|---|---:|"]
        lines += [f"| {k} | {v} |" for k, v in s["by_needs"].items()]
        lines += ["", "| 뮤테이션 스크립트 | 항목 |", "|---|---:|"]
        lines += [f"| `scripts/mutation_{k}.py` | {v} |" for k, v in m.items()]
        return "\n".join(lines)
    raise KeyError(name)


def replace_block(text: str, name: str, body: str) -> str:
    s, e = START.format(name=name), END.format(name=name)
    pat = re.compile(re.escape(s) + r".*?" + re.escape(e), re.S)
    if len(pat.findall(text)) != 1:
        raise ValueError(f"{s} … {e} 칸이 정확히 한 번 있어야 합니다")
    sep = "" if name == "readme" else "\n"
    return pat.sub(lambda _: f"{s}{sep}{body}{sep}{e}", text)


def readme(check: bool, root: Path = ROOT) -> int:
    s = scan(root)
    stale = []
    for rel, name in BLOCKS.items():
        p = root / rel
        old = p.read_text(encoding="utf-8")
        new = replace_block(old, name, render(name, s))
        if new != old:
            stale.append(rel)
            if not check:
                p.write_text(new, encoding="utf-8")
    if check and stale:
        print("테스트 숫자가 코드와 다릅니다: " + ", ".join(stale) + "\n  python scripts/test_inventory.py readme --write 로 고치세요")
        return 1
    print(("다시 씀: " + ", ".join(stale)) if stale and not check else "문서의 테스트 숫자가 코드와 같습니다")
    return 0


# ------------------------------------------------------------------ 실행 결과 (junit)

def _case_path(case: ET.Element) -> Optional[str]:
    f = case.get("file")
    if f:
        return f.replace("\\", "/")
    cls = case.get("classname") or case.get("name") or ""      # 모듈째 skip 되면 classname 이 비고 name 에 모듈 경로가 온다
    parts = cls.split(".")
    for i in range(len(parts), 0, -1):          # tests.unit.test_x.TestY → tests/unit/test_x.py
        cand = "/".join(parts[:i]) + ".py"
        if cand.startswith("tests/"):
            return cand
    return None


def summarize(paths: Iterable[Path]) -> dict:
    by: dict[str, Counter] = defaultdict(Counter)
    for path in paths:
        for case in ET.parse(path).getroot().iter("testcase"):
            rel = _case_path(case) or ""
            layer = (layers_for(rel) or ("other",))[0]
            kids = {c.tag for c in case}
            outcome = "failed" if "failure" in kids else "error" if "error" in kids else "skipped" if "skipped" in kids else "passed"
            by[layer][outcome] += 1
    total: Counter = Counter()
    for c in by.values():
        total.update(c)
    return {"by_layer": {k: dict(v) for k, v in sorted(by.items())}, "total": dict(total)}


def summary_markdown(res: dict, title: str) -> str:
    cols = ("passed", "failed", "error", "skipped")
    lines = [f"### {title}", "", "| 계층 | " + " | ".join(cols) + " |", "|---|" + "---:|" * len(cols)]
    for k, v in res["by_layer"].items():
        lines.append(f"| {k} | " + " | ".join(str(v.get(c, 0)) for c in cols) + " |")
    lines.append("| 합계 | " + " | ".join(str(res["total"].get(c, 0)) for c in cols) + " |")
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------ CLI

def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sc = sub.add_parser("scan")
    sc.add_argument("--json", action="store_true")
    rd = sub.add_parser("readme")
    g = rd.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true")
    g.add_argument("--write", action="store_true")
    sm = sub.add_parser("summarize")
    sm.add_argument("junit", nargs="+", type=Path)
    sm.add_argument("--title", default="테스트 결과")
    a = ap.parse_args(argv)

    if a.cmd == "scan":
        s = scan()
        if a.json:
            print(json.dumps(s, ensure_ascii=False, indent=1))
        else:
            print(render("testing", s))
        return 0
    if a.cmd == "readme":
        return readme(check=a.check)
    present = [p for p in a.junit if p.exists()]
    if not present:
        print("junit 파일이 없습니다: " + ", ".join(map(str, a.junit)))
        return 1
    res = summarize(present)
    md = summary_markdown(res, a.title)
    print(md)
    out = os.environ.get("GITHUB_STEP_SUMMARY")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(md + "\n")
    return 1 if res["total"].get("failed") or res["total"].get("error") else 0


if __name__ == "__main__":
    sys.exit(main())
