from __future__ import annotations

from pathlib import Path

import pytest

from pg_support import pg  # noqa: F401
from batch.seed_dim_link import link_row, read_vertices, seed
from scripts.migrate import discover, migrate

pytestmark = pytest.mark.pg
ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests/fixtures/reference/link_vertex_sample.csv"


def test_files_are_numbered_and_unique():       # DB 없이도 도는 검사
    names = [n for n, _, _ in discover(ROOT / "sql")]
    assert names == sorted(names) and names[0].startswith("001_")


def test_migrate_is_idempotent_and_detects_edit(pg, tmp_path):
    assert migrate(pg, ROOT / "sql") == []                       # 두 번째는 할 일 없음
    d = tmp_path / "sql"; d.mkdir()
    for n, p, _ in discover(ROOT / "sql"):
        (d / n).write_text(p.read_text())
    (d / "001_base.sql").write_text("-- changed\n" + (d / "001_base.sql").read_text())
    with pytest.raises(SystemExit, match="바뀌었습니다"):
        migrate(pg, d)


def test_failed_migration_rolls_back_only_that_file(pg, tmp_path):
    d = tmp_path / "sql"; d.mkdir()
    for n, p, _ in discover(ROOT / "sql"):
        (d / n).write_text(p.read_text())
    bad = f"{int(max(n[:3] for n, _, _ in discover(ROOT / 'sql'))) + 1:03d}_bad.sql"      # 다음 번호 (실제 마이그레이션이 늘어도 겹치지 않게)
    (d / bad).write_text("CREATE TABLE ops.t6 (a int); SELECT 1/0;")
    with pytest.raises(Exception):
        migrate(pg, d)
    assert pg.execute("SELECT to_regclass('ops.t6')").fetchone()[0] is None
    assert pg.execute("SELECT count(*) FROM ops.schema_migrations WHERE name=%s", (bad,)).fetchone()[0] == 0


def test_seed_dim_link_geometry_and_upsert(pg):
    v = read_vertices(FIX)
    assert len(v) == 6
    out = seed(pg, v, {"1220003800", "1110014200", "9999999999"}, "fixture")
    assert out == {"loaded": 6, "skipped": 0, "collected_missing": 1}
    n, collected = pg.execute("SELECT count(*), count(*) FILTER (WHERE is_collected) FROM dim.dim_link").fetchone()
    assert (n, collected) == (6, 2)
    # 서울 안(경도 126.7~127.2, 위도 37.4~37.7)이고 길이가 꼭짓점 합과 같은지
    ok = pg.execute("SELECT bool_and(ST_X(ST_StartPoint(geom)) BETWEEN 126.7 AND 127.2 AND ST_Y(ST_StartPoint(geom)) BETWEEN 37.4 AND 37.7) FROM dim.dim_link").fetchone()[0]
    assert ok
    geo_len, stored = pg.execute("SELECT ST_Length(geom::geography), length_m FROM dim.dim_link WHERE link_id='1220003800'").fetchone()
    assert abs(geo_len - stored) / stored < 0.02          # 평면(TM)과 측지 길이는 2% 안
    assert seed(pg, v, set(), "fixture")["loaded"] == 6   # 재실행 → 같은 행 수
    assert pg.execute("SELECT count(*) FROM dim.dim_link").fetchone()[0] == 6
    assert pg.execute("SELECT count(*) FROM dim.dim_link WHERE is_collected").fetchone()[0] == 0


def test_link_row_rejects_single_vertex():
    assert link_row("x", [(1, 200000.0, 500000.0)]) is None


def test_roles_have_least_privilege(pg):
    q = lambda role, obj, priv: pg.execute("SELECT has_table_privilege(%s, %s, %s)", (role, obj, priv)).fetchone()[0]
    assert q("api_reader", "fact.traffic_speed", "SELECT") and not q("api_reader", "fact.traffic_speed", "INSERT")
    assert q("bi_reader", "mart.v_traffic_latest", "SELECT") and not q("bi_reader", "fact.traffic_speed", "SELECT")
    assert q("gold_writer", "fact.traffic_speed", "INSERT") and not q("gold_writer", "mart.mv_link_baseline", "INSERT")
