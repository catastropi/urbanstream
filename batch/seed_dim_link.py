"""dim.dim_link 시드. LINK_VERTEX_2025.xlsx(OA-15060, EPSG:5181 꼭짓점)를 읽어 선(LineString, 4326)으로 넣는다.

- 파일은 git 에 넣지 않는다(공공누리 1유형, data/reference/). CSV(LINK_ID,VER_SEQ,GRS80TM_X,GRS80TM_Y)도 받는다.
- 재실행 안전: link_id 기준 upsert. 수집 대상 표시는 config/sources.yaml 의 link_ids 를 따른다.
사용: python -m batch.seed_dim_link data/reference/LINK_VERTEX_2025.xlsx
"""
from __future__ import annotations

import csv
import math
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


def read_vertices(path: Path) -> "OrderedDict[str, list[tuple[int, float, float]]]":
    rows: Iterable[tuple]
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as f:
            rows = [tuple(r.values()) for r in csv.DictReader(f)]
    else:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        rows = list(wb.worksheets[0].iter_rows(min_row=2, values_only=True))
    out: "OrderedDict[str, list]" = OrderedDict()
    for r in rows:
        if r is None or r[0] is None:      # read_only 모드는 빈 행을 돌려주기도 한다
            continue
        out.setdefault(str(r[0]).strip(), []).append((int(r[1]), float(r[2]), float(r[3])))
    for v in out.values():
        v.sort()
    return out


def link_row(link_id: str, verts: list[tuple[int, float, float]]) -> tuple | None:
    """(link_id, vertex_count, length_m, wkt). 꼭짓점이 2개 미만이면 None."""
    from streaming.geo_tm import tm5181_to_wgs84
    if len(verts) < 2:
        return None
    length = sum(math.hypot(b[1] - a[1], b[2] - a[2]) for a, b in zip(verts, verts[1:]))
    pts = []
    for _, x, y in verts:
        lon, lat = tm5181_to_wgs84(x, y)
        if lon is None:
            return None
        pts.append(f"{lon:.7f} {lat:.7f}")
    return link_id, len(verts), round(length, 1), f"LINESTRING({', '.join(pts)})"


def seed(conn, vertices, collected: set[str], source_note: str) -> dict[str, int]:
    rows, skipped = [], 0
    for lid, v in vertices.items():
        r = link_row(lid, v)
        if r is None:
            skipped += 1
        else:
            rows.append(r + (lid in collected, source_note))
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO dim.dim_link (link_id, vertex_count, length_m, geom, is_collected, source_note) "
                "VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s) "
                "ON CONFLICT (link_id) DO UPDATE SET vertex_count=EXCLUDED.vertex_count, length_m=EXCLUDED.length_m, "
                "geom=EXCLUDED.geom, is_collected=EXCLUDED.is_collected, source_note=EXCLUDED.source_note", rows)
    missing = sorted(collected - {r[0] for r in rows})
    return {"loaded": len(rows), "skipped": skipped, "collected_missing": len(missing)}


def main() -> int:
    import psycopg
    from batch.db import conninfo
    from collector.config import load_config
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data/reference/LINK_VERTEX_2025.xlsx"
    if not path.exists():
        print(f"파일이 없습니다: {path}"); return 2
    try:
        from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    cfg = load_config(ROOT / "config/sources.yaml", env={})
    collected = {t.params["link_id"] for t in cfg.sources["seoul_traffic"].targets}
    with psycopg.connect(conninfo(role="writer"), autocommit=True) as conn:
        print(seed(conn, read_vertices(path), collected, f"LINK_VERTEX_2025 ({path.name})"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
