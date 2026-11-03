"""EPSG:5181 → WGS84 자체 구현 시험 (pyspark 없이 호스트에서 돈다). 기준값은 pyproj 3.8 로 뽑은 것."""
import json
import math
from datetime import datetime
from pathlib import Path

import pytest

from streaming.geo_tm import tm5181_to_wgs84
from urbanstream.contracts import SOURCES, CallContext

# (x, y) → (경도, 위도) — pyproj.Transformer.from_crs("EPSG:5181", "EPSG:4326", always_xy=True) 결과
REFERENCE = [
    ((200000.0, 500000.0), (127.0, 38.0)),                                  # 원점
    ((198000.0, 451500.0), (126.97736303425938, 37.56303076860721)),
    ((205000.0, 445000.0), (127.05654816755917, 37.504454341193764)),
    ((193000.0, 450000.0), (126.92078495936441, 37.54949140466179)),
]


@pytest.mark.parametrize("xy,ll", REFERENCE)
def test_matches_pyproj_reference_values(xy, ll):
    lon, lat = tm5181_to_wgs84(*xy)
    assert math.isclose(lon, ll[0], abs_tol=1e-9) and math.isclose(lat, ll[1], abs_tol=1e-9)   # 1e-9 도 ≈ 0.1 mm


def test_matches_pyproj_everywhere_in_seoul_when_pyproj_is_installed():
    pyproj = pytest.importorskip("pyproj")
    import random
    t = pyproj.Transformer.from_crs("EPSG:5181", "EPSG:4326", always_xy=True)
    rnd = random.Random(7)
    for _ in range(500):
        x, y = rnd.uniform(180000, 230000), rnd.uniform(430000, 470000)
        a, b = tm5181_to_wgs84(x, y), t.transform(x, y)
        assert abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9


@pytest.mark.parametrize("x,y", [(None, 1.0), (1.0, None), (None, None), (float("nan"), 1.0), (1.0, float("inf"))])
def test_missing_or_non_finite_input_gives_none(x, y):
    assert tm5181_to_wgs84(x, y) == (None, None)


def test_recorded_bus_positions_agree_with_their_own_gps_within_10m():
    """수집된 실제 버스위치 샘플: GPS(경도·위도)와 TM 좌표를 같이 주므로, 변환이 맞는지 현장 데이터로 확인한다."""
    rec = Path(__file__).resolve().parents[1] / "fixtures" / "recorded"
    meta = json.loads((rec / "manifest.json").read_text(encoding="utf-8"))["files"]
    rel = "bus_position/ok_20261101T215320.xml"
    m = meta[rel]
    ctx = CallContext(fetched_at=datetime.fromisoformat(m["fetched_at"]), http_status=m["http_status"], latency_ms=1, raw_uri=rel)
    res = SOURCES["bus_position"].parse((rec / rel).read_bytes(), ctx, **m.get("kwargs", {}))
    checked = 0
    for r in res.records:
        p = r.to_dict()["payload"]
        if p.get("tm_x") is None or p.get("lon") is None:
            continue
        lon, lat = tm5181_to_wgs84(p["tm_x"], p["tm_y"])
        d = math.hypot((lon - p["lon"]) * 88000, (lat - p["lat"]) * 111000)   # 서울 위도에서 도 → 미터 근사
        assert d < 10, f"{d:.1f} m"
        checked += 1
    assert checked >= 3
