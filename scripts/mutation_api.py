"""API 변이 시험: 코드를 일부러 망가뜨려서 테스트가 잡는지 본다 (HANDOFF 검증 습관 ②).

임시 폴더에 복사본을 만들어 한 번에 하나씩 바꾸고, 지정한 테스트가 '실패'해야 통과로 센다. 원본은 건드리지 않는다.
실행:  GOLD_TEST_DSN="host=127.0.0.1 port=5432 dbname=ut_test user=postgres password=..." python scripts/mutation_api.py
       python scripts/mutation_api.py --only 3,7        (번호 지정)
종료 코드: 못 잡은 변이가 있으면 1. Postgres 가 필요한 변이는 GOLD_TEST_DSN 이 없으면 건너뛴다(건너뛴 수를 따로 센다).
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
COPY = ["api", "src", "sql", "scripts", "pytest.ini", "tests/api", "tests/support", "tests/contract/snapshots"]

CORE = "tests/api/test_api_core.py"
PG = "tests/api/test_api_pg.py"
GUARD = "tests/api/test_guards.py"
# (설명, 파일, 원래 문자열(정확히 1번 나와야 함), 바꿀 문자열, 잡아야 할 테스트 인자들, Postgres 필요)
MUTATIONS = [
    ("잘못된 API 키도 통과", "api/security.py", "    if hit is None:", "    if False:", [CORE, "-k", "guard_variants"], False),
    ("속도 제한 끄기", "api/security.py", "wait = svc.limiter.check(kid)", "wait = 0", [CORE, "-k", "guard_variants"], False),
    ("토큰이 0 이어도 허용", "api/ratelimit.py", "if tokens >= 1.0:", "if tokens >= 0.0:", [CORE, "-k", "rate_limiter"], False),
    ("7일 경계를 7일 미만으로", "api/pagination.py", "if end - start > timedelta(days=max_days):", "if end - start >= timedelta(days=max_days):", [CORE, "-k", "window"], False),
    ("from==to 를 허용", "api/pagination.py", "if start >= end:", "if start > end:", [CORE, "-k", "window"], False),
    ("오프셋 없는 시각 허용", "api/pagination.py", "if v is not None and (v.tzinfo is None or v.utcoffset() is None):", "if False:", [CORE, "-k", "window"], False),
    ("캐시가 만료되지 않음", "api/cache.py", "if hit and hit[0] > now:", "if hit:", [CORE, "-k", "cache"], False),
    ("캐시 크기 상한 제거", "api/cache.py", "while len(self._data) >= self.max_entries:", "while False:", [CORE, "-k", "cache_is_bounded"], False),
    ("검증 오류가 입력값을 되돌려 줌", "api/errors.py", '"msg": str(e.get("msg", ""))', '"msg": str(e.get("msg", "")) + str(e.get("input", ""))', [CORE, "-k", "echo_input"], False),
    ("DB 오류 문구 노출", "api/db.py", '"데이터베이스에 연결할 수 없습니다", headers={"Retry-After": "5"}) from e', '"데이터베이스에 연결할 수 없습니다 " + str(e), headers={"Retry-After": "5"}) from e', [CORE, "-k", "db_error_mapping"], False),
    ("CORS 전체 허용", "api/main.py", "allow_origins=list(settings.cors_origins)", 'allow_origins=["*"]', [CORE, "-k", "cors"], False),
    ("짧은 API 키 허용", "api/config.py", "MIN_KEY_LEN = 24", "MIN_KEY_LEN = 1", [CORE, "-k", "settings_rejects"], False),
    ("SQL 문자열 보간(주입 통로)", "api/routers/anomalies.py", 'OR link_id = %(l)s) "', 'OR link_id = \'" + link_id + "\') "', [GUARD], False),
    ("ops 원본 표 읽기", "api/routers/pipeline.py", "FROM ops.v_alert_recent ", "FROM ops.alert_event ", [GUARD], False),
    ("쓰기 구문 추가", "api/routers/weather.py", '"SELECT precip_class,', '"DELETE FROM mart.weather_traffic_hourly WHERE false; SELECT precip_class,', [GUARD], False),
    ("모든 경로에서 인증 제거(주간 경로 하나)", "api/routers/bus.py", 'router = APIRouter(prefix="/v1", tags=["bus"], dependencies=[Depends(guard)])', 'router = APIRouter(prefix="/v1", tags=["bus"])', [GUARD], False),
    ("엔드포인트 응답 필드 이름 변경", "api/schemas.py", "    baseline_p50: Optional[float] = None\n    baseline_provisional: Optional[bool] = None\n    ratio_to_baseline", "    baseline_median: Optional[float] = None\n    baseline_provisional: Optional[bool] = None\n    ratio_to_baseline", ["tests/api/test_openapi_contract.py"], False),
    ("페이지가 limit 건일 때도 다음 쪽 있다고 함", "api/pagination.py", "more = len(rows) > limit", "more = len(rows) >= limit", [PG, "-k", "pagination"], True),
    ("커서 비교를 >= 로(중복 행)", "api/routers/traffic.py", "AND (%(c1)s::text IS NULL OR link_id > %(c1)s) \"\n            \"ORDER BY link_id LIMIT", "AND (%(c1)s::text IS NULL OR link_id >= %(c1)s) \"\n            \"ORDER BY link_id LIMIT", [PG, "-k", "pagination"], True),
    ("기간 끝을 포함(<=)", "api/routers/traffic.py", "AND event_time < %(e)s \"", "AND event_time <= %(e)s \"", [PG, "-k", "window_boundaries"], True),
    ("stale 표시를 항상 false", "api/deps.py", "stale = None if (age is None or limit is None) else age > limit", "stale = None if (age is None or limit is None) else False", [PG, "-k", "stale_flag"], True),
    ("기준선 MV 비어 있어도 기준선 쿼리 사용", "api/routers/traffic.py", "rows = svc.db.rows(_CURRENT_WITH_BASE if populated else _CURRENT_NO_BASE,", "rows = svc.db.rows(_CURRENT_WITH_BASE,", [PG, "-k", "before_first_baseline_refresh"], True),
    ("돌발 상태 우선순위 망가뜨림(listed 무시)", "api/routers/incidents.py", 'if r["still_listed"]:', "if False:", [PG, "-k", "incident_statuses"], True),
    ("파이프라인 전체 상태를 가장 좋은 것으로", "api/routers/pipeline.py", "overall = max(live, key=", "overall = min(live, key=", [PG, "-k", "pipeline_and_dq"], True),
    ("NUL 문자 매핑 제거 → 500", "api/db.py", "except (psycopg.errors.DataError, psycopg.errors.InvalidTextRepresentation) as e:", "except psycopg.errors.InvalidTextRepresentation as e:", [PG, "-k", "injection"], True),
    ("읽기 전용 연결 옵션 제거", "api/config.py", "-c default_transaction_read_only=on", "-c default_transaction_read_only=off", [PG, "-k", "connection_options"], True),
    ("current 를 뷰 전체 정렬로 되돌림(LIMIT 1 제거)", "api/routers/traffic.py", "FROM mart.v_traffic_level x WHERE x.link_id = d.link_id LIMIT 1) l\nLEFT JOIN LATERAL (SELECT * FROM mart.v_traffic_latest y WHERE y.link_id = d.link_id AND y.event_time = l.event_time LIMIT 1) b ON true", "FROM mart.v_traffic_level x WHERE x.link_id = d.link_id) l\nLEFT JOIN LATERAL (SELECT * FROM mart.v_traffic_latest y WHERE y.link_id = d.link_id AND y.event_time = l.event_time) b ON true", [PG, "-k", "per_link_index_path"], True),
    ("큰 응답도 캐시에 저장", "api/deps.py", "len(data) <= CACHE_MAX_ROWS", "len(data) <= 10**9", [PG, "-k", "large_responses"], True),
    ("geometry=false 인데 형상을 줌", "api/routers/traffic.py", "CASE WHEN %(geom)s THEN ST_AsGeoJSON(geom)::jsonb END", "ST_AsGeoJSON(geom)::jsonb", [PG, "-k", "geometry_rules"], True),
    ("빈 DB 에서 마트를 ok 로 보고", "api/routers/pipeline.py", "FROM ops.v_mart_freshness ORDER BY mart", "FROM ops.v_mart_freshness WHERE status <> 'unknown' ORDER BY mart", [PG, "-k", "empty_db"], True),
]


def run_one(i: int, m, tmp_root: Path, have_pg: bool) -> str:
    desc, rel, old, new, args, needs_pg = m
    if needs_pg and not have_pg:
        return "건너뜀(GOLD_TEST_DSN 없음)"
    work = tmp_root / f"m{i}"
    for item in COPY:
        src = ROOT / item
        dst = work / item
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__")) if src.is_dir() else shutil.copy2(src, dst)
    (work / "tests" / "__init__.py").unlink(missing_ok=True)
    f = work / rel
    text = f.read_text(encoding="utf-8")
    if text.count(old) != 1:
        return f"오류: 원문이 {text.count(old)}번 나옴 — 변이 정의를 고쳐야 함"
    f.write_text(text.replace(old, new), encoding="utf-8")
    r = subprocess.run([sys.executable, "-I", "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *args], cwd=work, capture_output=True, text=True, env=os.environ.copy())
    shutil.rmtree(work, ignore_errors=True)
    return "잡힘" if r.returncode != 0 else "못 잡음"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="쉼표로 구분한 번호(1부터)")
    args = ap.parse_args(argv)
    only = {int(x) for x in args.only.split(",")} if args.only else None
    have_pg = bool(os.environ.get("GOLD_TEST_DSN"))
    results = []
    with tempfile.TemporaryDirectory(prefix="mut_api_") as td:
        for i, m in enumerate(MUTATIONS, 1):
            if only and i not in only:
                continue
            res = run_one(i, m, Path(td), have_pg)
            results.append(res)
            print(f"{i:2d}. [{res}] {m[0]}  ({m[1]})", flush=True)
    caught, missed = results.count("잡힘"), [r for r in results if r in ("못 잡음",) or r.startswith("오류")]
    print(f"\n잡힘 {caught} / 못 잡음·오류 {len(missed)} / 건너뜀 {sum(r.startswith('건너뜀') for r in results)} (전체 {len(results)})")
    return 1 if missed else 0


if __name__ == "__main__":
    sys.exit(main())
