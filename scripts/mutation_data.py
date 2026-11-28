"""데이터 로직 뮤테이션 테스트: 재처리, 중복 제거, Gold 적재, DQ, 증분 스테이징, 작업 잠금 코드를 일부러 망가뜨려 테스트가 잡는지 본다.

mutation_api.py / mutation_dashboard.py 와 같은 방식이다. 임시 폴더에 저장소 사본을 만들고 한 번에 하나씩 바꾼 뒤,
지정한 테스트가 실패해야 '잡힘'으로 센다. 원본은 건드리지 않는다. 사본에는 data/ 를 넣지 않는다(Mac 의 실데이터를 복사하지 않으려고).

실행
  python scripts/mutation_data.py                     # PG·Spark 가 없으면 그 항목은 건너뛴다
  GOLD_TEST_DSN="host=... dbname=ut_test user=postgres" python scripts/mutation_data.py
  python scripts/mutation_data.py --only 3,7          # 번호 지정
  python scripts/mutation_data.py --needs spark       # Spark 항목만 (weekly)
  python scripts/mutation_data.py --list              # 목록만 출력
종료 코드: 못 잡은 항목이나 정의 오류가 있으면 1.
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "data", "__pycache__", ".pytest_cache", ".ruff_cache", ".terraform", ".venv", "venv", "node_modules"}

POINTER = "tests/unit/test_silver_pointer.py"
INCR = "tests/unit/test_incremental.py"
LOCK_U = "tests/unit/test_job_lock.py"
REPROC_PG = "tests/integration/test_reprocess_pg.py"
DQ_PG = "tests/integration/test_dq_pg.py"
LOCK_PG = "tests/integration/test_job_lock_pg.py"
SILVER_SP = "tests/spark/test_silver.py"
REBUILD_SP = "tests/spark/test_silver_rebuild.py"
STAGE_SP = "tests/spark/test_gold_stage.py"


@dataclass(frozen=True)
class Mutation:
    area: str          # 재처리 / 중복 제거 / 적재 / DQ / 증분 / 잠금
    desc: str
    path: str
    old: str           # 파일 안에서 정확히 한 번 나와야 한다
    new: str
    tests: tuple[str, ...]
    needs: str = ""    # "" (없음) / "pg" / "spark"


M = Mutation
MUTATIONS: tuple[Mutation, ...] = (
    # 재처리: Silver 포인터 (순수 파이썬)
    M("재처리", "시작일이 종료일보다 늦어도 허용", "streaming/silver_pointer.py", "    if d0 > d1:\n", "    if False:\n", (POINTER, "-k", "date_guard")),
    M("재처리", "최근 날짜 막기 해제", "streaming/silver_pointer.py", "if d1 > limit and not allow_recent:", "if False:", (POINTER, "-k", "date_guard")),
    M("재처리", "빌드가 ok 가 아니어도 게시", "streaming/silver_pointer.py",
      "    if b.get(\"status\") != \"ok\":\n        raise PointerError(f\"{table} {version}: 빌드 상태가 ok 가 아니라 게시할 수 없습니다",
      "    if False:\n        raise PointerError(f\"{table} {version}: 빌드 상태가 ok 가 아니라 게시할 수 없습니다", (POINTER, "-k", "only_ok_builds")),
    M("재처리", "빌드에 없는 날짜도 게시", "streaming/silver_pointer.py", "    if missing:\n", "    if False:\n", (POINTER, "-k", "only_ok_builds")),
    M("재처리", "되돌리기가 다른 재처리의 게시를 덮어씀", "streaming/silver_pointer.py", "        if expect_version and not force:", "        if False:", (POINTER, "-k", "rollback_refuses")),
    M("재처리", "버전 이름 검사 해제(경로 탈출)", "streaming/silver_pointer.py", "    if not _VERSION.match(v or \"\") or v in (\".\", \"..\"):", "    if False:", (POINTER, "-k", "names_are_checked")),
    M("재처리", "깨진 포인터 파일을 빈 포인터로 취급", "streaming/silver_pointer.py",
      "        raise PointerError(f\"포인터 파일이 깨졌습니다: {p} ({e})\") from e", "        return empty", (POINTER, "-k", "broken_pointer")),
    # 재처리: 실행 이력과 Gold 범위 교체 (PostgreSQL)
    M("재처리", "허용되지 않는 상태 전이 허용", "batch/reprocess.py", "if new != cur[0] and new not in TRANSITIONS[cur[0]]:", "if False:", (REPROC_PG, "-k", "status_transitions"), "pg"),
    M("재처리", "되돌린 재처리를 다시 이어서 실행", "batch/reprocess.py", "    if row[\"status\"] == \"rolled_back\":", "    if False:", (REPROC_PG, "-k", "status_transitions"), "pg"),
    M("적재", "범위 교체가 범위 밖까지 지움", "batch/gold_load.py",
      "cur = conn.execute(f\"DELETE FROM {s.target} WHERE {REPLACEABLE[s.name]} BETWEEN %s AND %s\", (d0, d1))",
      "cur = conn.execute(f\"DELETE FROM {s.target} WHERE {REPLACEABLE[s.name]} >= %s OR %s IS NOT NULL\", (d0, d1))", (REPROC_PG, "-k", "replace_deletes_only"), "pg"),
    M("적재", "범위 교체가 지우지 않음(정정이 Gold 에 안 들어감)", "batch/gold_load.py",
      "cur = conn.execute(f\"DELETE FROM {s.target} WHERE {REPLACEABLE[s.name]} BETWEEN %s AND %s\", (d0, d1))",
      "cur = conn.execute(f\"DELETE FROM {s.target} WHERE false AND %s IS NULL AND %s IS NULL\", (d0, d1))", (REPROC_PG, "-k", "replace_deletes_only"), "pg"),
    M("적재", "manifest 범위가 달라도 교체", "batch/gold_load.py",
      "    if manifest.get(\"date_from\") != d0.isoformat() or manifest.get(\"date_to\") != d1.isoformat():", "    if False:", (REPROC_PG, "-k", "replace_guards"), "pg"),
    M("적재", "스테이징 0행이어도 DB 범위를 비움", "batch/gold_load.py", "            if n:\n                raise ReplaceGuardError", "            if False:\n                raise ReplaceGuardError", (REPROC_PG, "-k", "replace_guards"), "pg"),
    M("적재", "범위 교체를 트랜잭션 밖에서 실행", "batch/gold_load.py",
      "            with conn.transaction():                        # 전부 한 트랜잭션",
      "            with __import__(\"contextlib\").nullcontext():                        # 전부 한 트랜잭션", (REPROC_PG, "-k", "roll_back"), "pg"),
    M("적재", "평소 적재의 사실 표를 DO UPDATE 로", "batch/gold_load.py", "            action = f\" ON CONFLICT ({keys}) DO NOTHING\"",
      "            action = f\" ON CONFLICT ({keys}) DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in cols if c not in spec.keys) or 'event_time = EXCLUDED.event_time'}\"",
      (REPROC_PG, "-k", "replace_deletes_only"), "pg"),
    # DQ
    M("DQ", "미래 시각 허용 오차를 무시", "src/urbanstream/dq/checks.py", "{p['ts_col']} > {p['fetched_col']} + make_interval(secs => %s)", "{p['ts_col']} > {p['fetched_col']} - make_interval(secs => %s)", (DQ_PG, "-k", "future_time"), "pg"),
    M("DQ", "표본이 적어도 pass/fail 판정", "src/urbanstream/dq/checks.py", "    if n < p_min_rows:\n", "    if False:\n", (DQ_PG, "-k", "null_rate"), "pg"),
    M("DQ", "수집 공백이 설명해도 fail", "src/urbanstream/dq/checks.py", "    if present + math.ceil(gap_h) >= need:", "    if False:", (DQ_PG, "-k", "hour_coverage"), "pg"),
    M("DQ", "기준선 날짜에 당일 포함", "src/urbanstream/dq/checks.py", "base = sorted(v for k, v in by.items() if k < d and v > 0)", "base = sorted(v for k, v in by.items() if k <= d and v > 0)", (DQ_PG, "-k", "volume_drift"), "pg"),
    M("DQ", "shadow 규칙도 마트를 막음", "src/urbanstream/dq/engine.py", "WHERE action='block_mart' AND status='fail' AND NOT shadow ", "WHERE action='block_mart' AND status='fail' ", (DQ_PG, "-k", "gate_blocks_only"), "pg"),
    # 증분 스테이징 (gold_intraday)
    M("증분", "겹침 없이 다음 구간 시작", "batch/jobs.py", "    since = (st[\"until\"] - INCR_OVERLAP_SEC) if st else", "    since = (st[\"until\"]) if st else", (INCR, "-k", "continues_from_the_record")),
    M("증분", "진행 기록이 뒤로 감(같은 구간 반복)", "batch/jobs.py", "    if prev and prev[\"until\"] > until:", "    if False:", (INCR, "-k", "never_moves_backwards")),
    M("증분", "증분 창에서 전날 빠짐", "batch/jobs.py", "    return min(d0, d1 - timedelta(days=1)), d1", "    return d0, d1", (INCR, "-k", "previous_day")),
    # 작업 잠금 (3B-1)
    M("잠금", "잠금을 못 잡아도 진행", "batch/jobs.py", "got = conn.execute(\"SELECT pg_try_advisory_lock(%s, %s)\", (LOCK_NAMESPACE, key)).fetchone()[0]",
      "got = conn.execute(\"SELECT pg_try_advisory_lock(%s, %s)\", (LOCK_NAMESPACE, key)).fetchone()[0] or True", (LOCK_U, LOCK_PG), "pg"),
    # 중복 제거와 Silver 변환 (Spark)
    M("중복 제거", "Silver 중복 제거 키를 ingest_id 까지 넓힘(재전송이 남음)", "streaming/silver.py", ".dropDuplicatesWithinWatermark(list(table.dedup_keys))",
      ".dropDuplicatesWithinWatermark(list(table.dedup_keys) + [\"ingest_id\"])", (SILVER_SP,), "spark"),
    M("중복 제거", "Gold 스테이징의 키별 첫 행 고르기 해제", "batch/gold_stage.py", ".filter(F.col(\"_rn\") == 1)", ".filter(F.col(\"_rn\") >= 1)", (STAGE_SP,), "spark"),
    M("재처리", "재빌드가 줄어든 결과도 그냥 게시", "batch/silver_rebuild.py", "elif after_keys < counts[\"silver_before_keys\"] and not allow_shrink:", "elif False:", (REBUILD_SP,), "spark"),
)


def have(needs: str) -> bool:
    if needs == "pg":
        return bool(os.environ.get("GOLD_TEST_DSN"))
    if needs == "spark":
        return importlib.util.find_spec("pyspark") is not None
    return True


def copy_repo(dst: Path) -> None:
    def ignore(d: str, names: list[str]) -> set[str]:
        return {n for n in names if n in SKIP_DIRS}
    shutil.copytree(ROOT, dst, ignore=ignore, symlinks=True)
    (dst / "data").mkdir(exist_ok=True)


def check_definition(m: Mutation, root: Path = ROOT) -> str | None:
    """원문이 파일에 정확히 한 번 나오는지. 문제가 있으면 설명, 없으면 None."""
    f = root / m.path
    if not f.exists():
        return f"파일이 없음: {m.path}"
    n = f.read_text(encoding="utf-8").count(m.old)
    return None if n == 1 else f"원문이 {n}번 나옴: {m.path}"


def run_one(m: Mutation, base: Path, work: Path) -> str:
    if not have(m.needs):
        return f"건너뜀({m.needs} 없음)"
    err = check_definition(m, base)
    if err:
        return "오류: " + err
    if work.exists():
        shutil.rmtree(work)
    shutil.copytree(base, work, symlinks=True)
    f = work / m.path
    f.write_text(f.read_text(encoding="utf-8").replace(m.old, m.new), encoding="utf-8")
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *m.tests],
                       cwd=work, capture_output=True, text=True, env=os.environ.copy())
    shutil.rmtree(work, ignore_errors=True)
    if r.returncode == 5:          # 고른 테스트가 0개: -k 이름이 바뀐 것
        return "오류: 고른 테스트가 없음(-k 확인)"
    return "잡힘" if r.returncode != 0 else "못 잡음"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--only", help="쉼표로 구분한 번호(1부터)")
    ap.add_argument("--needs", choices=("none", "pg", "spark"), help="이 조건의 항목만")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args(argv)
    only = {int(x) for x in args.only.split(",")} if args.only else None
    picked = [(i, m) for i, m in enumerate(MUTATIONS, 1)
              if (not only or i in only) and (args.needs is None or (m.needs or "none") == args.needs)]
    if args.list:
        for i, m in picked:
            print(f"{i:2d}. [{m.area}] {m.desc}  ({m.path}{', ' + m.needs if m.needs else ''})")
        return 0
    results: list[str] = []
    with tempfile.TemporaryDirectory(prefix="mut_data_") as td:
        base = Path(td) / "base"
        copy_repo(base)
        for i, m in picked:
            res = run_one(m, base, Path(td) / "work")
            results.append(res)
            print(f"{i:2d}. [{res}] [{m.area}] {m.desc}  ({m.path})", flush=True)
    caught = results.count("잡힘")
    bad = [r for r in results if r == "못 잡음" or r.startswith("오류")]
    skipped = sum(r.startswith("건너뜀") for r in results)
    print(f"\n잡힘 {caught} / 못 잡음·오류 {len(bad)} / 건너뜀 {skipped} (전체 {len(results)})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
