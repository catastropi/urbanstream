"""대시보드(Step 10) 변이 시험: 코드를 일부러 망가뜨려서 테스트가 잡는지 본다 (HANDOFF 검증 습관 ②).

임시 폴더에 복사본을 만들어 한 번에 하나씩 바꾸고, 지정한 테스트가 '실패'해야 통과로 센다. 원본은 건드리지 않는다.
Postgres·Docker 가 필요 없다(대시보드는 가짜 API 로 시험한다).
실행:  python scripts/mutation_dashboard.py            (venv 에 requirements-dashboard.txt + requirements-dev.txt 설치 필요)
       python scripts/mutation_dashboard.py --only 3,7  (번호 지정)
종료 코드: 못 잡은 변이·원문 불일치가 있으면 1.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COPY = ["dashboard", "sql/014_holidays_2026.sql", "scripts", "src", "docker", "docker-compose.yml", ".env.example", ".gitignore", "requirements-dashboard.txt",
        "requirements-api.txt", "pytest.ini", "tests/dashboard", "tests/support", "tests/contract/snapshots",
        "sql/014_holidays_2026.sql", "tests/unit/test_export_tableau.py", "tests/unit/test_stack_env.py", "tests/unit/test_compose_policy.py"]

T = "tests/dashboard/"
TIME, CLIENT, FRESH, CONF, ARCH, CONTRACT, BUDGET = (T + "test_dash_timefmt.py", T + "test_dash_client.py", T + "test_dash_freshness_shape.py",
                                                    T + "test_dash_config.py", T + "test_dash_architecture.py", T + "test_dash_contract.py",
                                                    T + "test_dash_views_budget.py")
EXPORT, ENVT, COMPOSE = "tests/unit/test_export_tableau.py", "tests/unit/test_stack_env.py", "tests/unit/test_compose_policy.py"

# (설명, 파일, 원래 문자열(정확히 1번 나와야 함), 바꿀 문자열, 잡아야 할 테스트 인자들)
MUTATIONS = [
    ("오프셋 없는 시각을 받아들임", "dashboard/timefmt.py", "if dt.tzinfo is None or dt.utcoffset() is None:", "if False:", [TIME]),
    ("KST 를 UTC+8 로", "dashboard/timefmt.py", "timedelta(hours=9)", "timedelta(hours=8)", [TIME]),
    ("floor_to 가 내림하지 않음(캐시 키가 매번 달라짐)", "dashboard/timefmt.py", "return datetime.fromtimestamp(ts - ts % seconds, UTC)", "return datetime.fromtimestamp(ts, UTC)", [TIME, BUDGET]),
    ("토/일 뒤바꿈", "dashboard/timefmt.py", '"sat" if wd == 5 else "sun" if wd == 6', '"sat" if wd == 6 else "sun" if wd == 5', [TIME, EXPORT]),
    ("공휴일 판정을 끔", "dashboard/timefmt.py", "if is_known_holiday(kst_day):", "if False:", [TIME]),
    ("모르는 돌발 코드를 '사고'로 번역", "dashboard/shape.py", 'else f"미확인 코드 ({code})"', 'else f"사고 ({code})"', [FRESH]),
    ("API 키 헤더를 비움", "dashboard/client.py", '{"X-API-Key": self._s.api_key,', '{"X-API-Key": "",', [CLIENT]),
    ("호출 예산 확인 끄기", "dashboard/client.py", "if len(self._calls) < self._s.budget_per_min:", "if True:", [CLIENT, BUDGET]),
    ("429 대기 상한 제거(오래 기다려도 재시도)", "dashboard/client.py", "if wait is None or wait > MAX_RETRY_AFTER_SEC:", "if wait is None:", [CLIENT]),
    ("인증 오류도 옛 값으로 덮음", "dashboard/client.py", 'FALLBACK_KINDS = ("server",', 'FALLBACK_KINDS = ("auth", "server",', [CLIENT]),
    ("캐시가 만료되지 않음", "dashboard/client.py", "entry.expires > self._clock()", "True", [CLIENT]),
    ("대시보드가 DB 드라이버를 import", "dashboard/client.py", "import json\n", "import json\nimport psycopg\n", [ARCH]),
    ("API 경로 오타(계약 불일치)", "dashboard/contract.py", '"/v1/traffic/current"', '"/v1/traffic/curent"', [CONTRACT]),
    ("API 가 stale=null 이어도 최신으로 판정", "dashboard/freshness.py", 'level = "unknown" if stale is None else "stale" if stale else "ok"', 'level = "ok" if not stale else "stale"', [FRESH]),
    ("갱신 실패를 화면에 안 보임", "dashboard/freshness.py", "    if error:\n", "    if False:\n", [FRESH, BUDGET]),
    ("판정 불가(None)를 '원활' 로 표시", "dashboard/shape.py", 'return "◇ 판정 불가(표본 부족)"', 'return LEVELS["normal"][0]', [FRESH]),
    ("판정 불가를 '원활' 개수에 합침", "dashboard/shape.py", 'c["unknown_level"] += 1', 'c["normal"] += 1', [FRESH]),
    ("결측 속도를 0 으로 그림", "dashboard/shape.py", '"speed": p.get("speed_kmh"),', '"speed": p.get("speed_kmh") or 0,', [FRESH]),
    ("결측 숫자를 0.0 으로 표시", "dashboard/shape.py", 'if x is None or isinstance(x, bool):\n        return DASH\n    try:\n        return f"{float(x):.{digits}f}{unit}"',
     'if isinstance(x, bool):\n        return DASH\n    try:\n        return f"{float(x or 0):.{digits}f}{unit}"', [FRESH]),
    ("짧은 대시보드 키 허용", "dashboard/config.py", "MIN_KEY_LEN = 24", "MIN_KEY_LEN = 1", [CONF]),
    ("내보내기: 결측 속도를 0 으로 평균에 넣음", "scripts/export_tableau.py", 'if v is None:\n                g["n_null"] += 1', 'if v is None:\n                g["speeds"].append(0.0)\n                g["n_null"] += 1', [EXPORT]),
    ("내보내기: 조각이 7일을 넘음", "scripts/export_tableau.py", "CHUNK_DAYS = 7 ", "CHUNK_DAYS = 8 ", [EXPORT]),
    ("내보내기: 조각 경계 중복을 두 번 셈", "scripts/export_tableau.py", "if t is None or t in seen:", "if t is None:", [EXPORT]),
    ("내보내기: API 가 준 열을 그대로 씀(차량번호 유출 통로)", "scripts/export_tableau.py", "row = {c: r.get(c) for c in cols}", "row = dict(r)", [EXPORT]),
    ("내보내기: 원격 API 주소 허용", "scripts/export_tableau.py", 'if not url.startswith(("http://127.0.0.1", "http://localhost")):', "if False:", [EXPORT]),
    ("버스 화면이 route_id 를 빼고 호출(Mac 실제 API 422)", "dashboard/views/bus.py", '"drift": client.get(C.BUS_DRIFT, {"route_id": r, "from"', '"drift": client.get(C.BUS_DRIFT, {"from"', [BUDGET]),
    ("지도가 버스 route_id 를 빼고 호출", "dashboard/views/map_view.py", '{**BUS_PARAMS, "route_id": client.bus_route_id}', "BUS_PARAMS", [BUDGET]),
    ("내보내기가 버스 route_id 를 빼고 호출", "scripts/export_tableau.py", '{"route_id": client.bus_route_id, "from": frm', '{"from": frm', [EXPORT]),
    ("init-env 가 기존 API 키 목록을 덮어씀", "scripts/stack_env.py", 'lines[vals["API_KEYS"]] = f"API_KEYS={current},{dash}" if current else f"API_KEYS={dash}"',
     'lines[vals["API_KEYS"]] = f"API_KEYS={dash}"', [ENVT]),
    ("대시보드 포트를 모든 인터페이스에 엶", "docker-compose.yml", '"127.0.0.1:${DASHBOARD_PORT:-8501}:8501"', '"${DASHBOARD_PORT:-8501}:8501"', [COMPOSE]),
    ("대시보드에 DB 비밀번호 전달", "docker-compose.yml", "      DASHBOARD_API_URL: http://api:8000\n", "      DASHBOARD_API_URL: http://api:8000\n      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-}\n", [COMPOSE]),
    ("대시보드 키를 compose 에서 필수(:?)로", "docker-compose.yml", "${DASHBOARD_API_KEY:-}", "${DASHBOARD_API_KEY:?required}", [COMPOSE]),
    ("Tableau 추출물을 git 에 올림", ".gitignore", "tableau/export/", "# tableau/export/", ["tests/unit/test_gitignore.py"]),
]


def run_one(i: int, m, tmp_root: Path) -> str:
    desc, rel, old, new, args = m
    work = tmp_root / f"m{i}"
    for item in COPY:
        src = ROOT / item
        dst = work / item
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__")) if src.is_dir() else shutil.copy2(src, dst)
    if "tests/unit/test_gitignore.py" in args:
        (work / "tests/unit").mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "tests/unit/test_gitignore.py", work / "tests/unit/test_gitignore.py")
    f = work / rel
    text = f.read_text(encoding="utf-8")
    if text.count(old) != 1:
        shutil.rmtree(work, ignore_errors=True)
        return f"오류: 원문이 {text.count(old)}번 나옴 — 변이 정의를 고쳐야 함"
    f.write_text(text.replace(old, new), encoding="utf-8")
    r = subprocess.run([sys.executable, "-I", "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *args], cwd=work,
                       capture_output=True, text=True, env=os.environ.copy())
    shutil.rmtree(work, ignore_errors=True)
    return "잡힘" if r.returncode != 0 else "못 잡음"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="쉼표로 구분한 번호(1부터)")
    args = ap.parse_args(argv)
    only = {int(x) for x in args.only.split(",")} if args.only else None
    results = []
    with tempfile.TemporaryDirectory(prefix="mut_dash_") as td:
        for i, m in enumerate(MUTATIONS, 1):
            if only and i not in only:
                continue
            res = run_one(i, m, Path(td))
            results.append(res)
            print(f"{i:2d}. [{res}] {m[0]}  ({m[1]})", flush=True)
    caught, bad = results.count("잡힘"), [r for r in results if r == "못 잡음" or r.startswith("오류")]
    print(f"\n잡힘 {caught} / 못 잡음·오류 {len(bad)} (전체 {len(results)})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
