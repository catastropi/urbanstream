"""EPSG:5181(Korea 2000 / 중부원점) TM 좌표 → WGS84 경도·위도. 표준 라이브러리만 쓴다 (pyproj 를 이미지에 넣지 않으려고, ADR-0007).

Krüger 급수(Karney 2011, n^4 항까지)로 역변환한다. 타원체 GRS80, 원점 위도 38°, 중앙 경선 127°, 축척 1.0, 가산값 E=200000 N=500000.
GRS80 과 WGS84 의 차이(밀리미터 이하)는 무시한다. 검증: tests/unit/test_geo_tm.py (pyproj 와 비교한 기준값 + pyproj 가 있으면 직접 비교).
"""
from __future__ import annotations

import math
from typing import Optional, Tuple

_A = 6378137.0
_F = 1 / 298.257222101
_E = math.sqrt(_F * (2 - _F))
_N = _F / (2 - _F)
_K0, _LAT0, _LON0, _FE, _FN = 1.0, math.radians(38.0), math.radians(127.0), 200000.0, 500000.0

_n = _N
_AA = _A / (1 + _n) * (1 + _n**2 / 4 + _n**4 / 64)
_ALPHA = (
    _n / 2 - 2 * _n**2 / 3 + 5 * _n**3 / 16 + 41 * _n**4 / 180,
    13 * _n**2 / 48 - 3 * _n**3 / 5 + 557 * _n**4 / 1440,
    61 * _n**3 / 240 - 103 * _n**4 / 140,
    49561 * _n**4 / 161280,
)
_BETA = (
    _n / 2 - 2 * _n**2 / 3 + 37 * _n**3 / 96 - _n**4 / 360,
    _n**2 / 48 + _n**3 / 15 - 437 * _n**4 / 1440,
    17 * _n**3 / 480 - 37 * _n**4 / 840,
    4397 * _n**4 / 161280,
)


def _conformal_lat(phi: float) -> float:
    return math.atan(math.sinh(math.asinh(math.tan(phi)) - _E * math.atanh(_E * math.sin(phi))))


def _m0() -> float:
    xi0 = _conformal_lat(_LAT0)
    return _K0 * _AA * (xi0 + sum(a * math.sin(2 * (j + 1) * xi0) for j, a in enumerate(_ALPHA)))


_M0 = _m0()


def tm5181_to_wgs84(x: Optional[float], y: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """(tm_x=동쪽, tm_y=북쪽) → (경도, 위도). 입력이 None/NaN/무한대면 (None, None)."""
    if x is None or y is None:
        return None, None
    x, y = float(x), float(y)
    if not (math.isfinite(x) and math.isfinite(y)):
        return None, None
    xi = (y - _FN + _M0) / (_K0 * _AA)
    eta = (x - _FE) / (_K0 * _AA)
    xi_p = xi - sum(b * math.sin(2 * (j + 1) * xi) * math.cosh(2 * (j + 1) * eta) for j, b in enumerate(_BETA))
    eta_p = eta - sum(b * math.cos(2 * (j + 1) * xi) * math.sinh(2 * (j + 1) * eta) for j, b in enumerate(_BETA))
    chi = math.asin(math.sin(xi_p) / math.cosh(eta_p))
    lon = _LON0 + math.atan2(math.sinh(eta_p), math.cos(xi_p))
    phi = chi
    for _ in range(12):   # 등각 위도 → 측지 위도 (고정점 반복, 보통 3~4회에 수렴)
        nxt = math.atan(math.sinh(math.asinh(math.tan(chi)) + _E * math.atanh(_E * math.sin(phi))))
        if abs(nxt - phi) < 1e-14:
            phi = nxt
            break
        phi = nxt
    return math.degrees(lon), math.degrees(phi)
