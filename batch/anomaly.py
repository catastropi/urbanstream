"""Step 8c 속도 이상 탐지 (DAG anomaly_scan, 매시 35분). ADR-0010 D8-11.

한계(숨기지 않는다): 팩트는 Gold 가 매시 20분에 적재하므로 이상은 발생 후 최대 약 1시간 20분 뒤에 보인다 — 실시간 탐지가 아니다 (D-124).
기준선이 28일 미만(provisional)인 동안은 어떤 이탈도 알림이 아니라 candidate 로만 기록한다 (D-125: 기준선이 아직 믿을 만하지 않다).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

LOG = logging.getLogger("urbanstream.batch.anomaly")
Z_THRESHOLD = 3.5            # [가정] Iglewicz-Hoaglin 의 로버스트 z 관례값. 7일 뒤 candidate 건수를 보고 조정 (D-121)
SIGMA_FLOOR_KMH = 2.0        # [가정] IQR 이 아주 작을 때 z 가 폭발하지 않게 하는 하한 (km/h)
LOOKBACK_HOURS = 3           # 늦게 적재된 관측도 잡도록 3시간을 다시 본다 (PK 가 중복을 막는다)
MIN_OBS = 20                 # batch.marts.MIN_OBS_THRESHOLD 와 같은 값 (임계값 마트의 sufficient 기준과 일치)

SQL = """
WITH obs AS (
    SELECT t.link_id, t.event_time, t.speed_kmh, b.speed_p50, b.speed_p25, b.speed_p75, b.n_obs, b.provisional
    FROM fact.traffic_speed t
    JOIN mart.mv_link_baseline b
      ON b.link_id = t.link_id
     AND b.iso_dow  = extract(isodow FROM t.event_time AT TIME ZONE 'Asia/Seoul')
     AND b.kst_hour = extract(hour   FROM t.event_time AT TIME ZONE 'Asia/Seoul')
    WHERE t.event_time >= %(since)s AND t.event_time <= %(until)s AND t.speed_kmh IS NOT NULL
      AND b.speed_p50 IS NOT NULL AND b.speed_p25 IS NOT NULL AND b.speed_p75 IS NOT NULL
), z AS (
    SELECT *, (speed_p75 - speed_p25) AS iqr,
           CASE WHEN speed_p75 - speed_p25 > 0
                THEN (speed_kmh - speed_p50) / greatest((speed_p75 - speed_p25) / 1.349, %(floor)s) END AS rz
    FROM obs
)
INSERT INTO mart.anomaly_event (link_id, event_time, speed_kmh, baseline_p50, baseline_iqr, robust_z, direction, status, n_obs, provisional, detected_at)
SELECT link_id, event_time, speed_kmh, speed_p50, iqr, rz,
       CASE WHEN speed_kmh < speed_p50 THEN 'slow' WHEN speed_kmh > speed_p50 THEN 'fast' END,
       CASE WHEN iqr = 0 THEN 'degenerate_baseline'
            WHEN n_obs >= %(min_obs)s AND NOT provisional THEN 'anomaly'
            ELSE 'candidate' END,
       n_obs, provisional, %(now)s
FROM z
WHERE (iqr = 0 AND speed_kmh <> speed_p50) OR abs(rz) >= %(thr)s
ON CONFLICT (link_id, event_time) DO NOTHING
RETURNING link_id, event_time, speed_kmh, baseline_p50, robust_z, status
"""

COUNT_SQL = """
SELECT count(*) FILTER (WHERE t.speed_kmh IS NOT NULL),
       count(*) FILTER (WHERE t.speed_kmh IS NOT NULL AND EXISTS (
           SELECT 1 FROM mart.mv_link_baseline b WHERE b.link_id = t.link_id
              AND b.iso_dow = extract(isodow FROM t.event_time AT TIME ZONE 'Asia/Seoul')
              AND b.kst_hour = extract(hour FROM t.event_time AT TIME ZONE 'Asia/Seoul')
              AND b.speed_p50 IS NOT NULL AND b.speed_p25 IS NOT NULL AND b.speed_p75 IS NOT NULL))
FROM fact.traffic_speed t WHERE t.event_time >= %s AND t.event_time <= %s
"""


def _connect(connect: Optional[Callable[[], Any]]):
    if connect is not None:
        return connect()
    from batch.jobs import _default_connect
    return _default_connect()


def scan(until: Optional[datetime] = None, *, lookback_hours: float = LOOKBACK_HOURS, threshold: float = Z_THRESHOLD, floor: float = SIGMA_FLOOR_KMH,
         min_obs: int = MIN_OBS, connect: Optional[Callable[[], Any]] = None, send: Optional[Callable[..., Any]] = None,
         now: Optional[datetime] = None) -> dict:
    """최근 lookback_hours 의 속도를 기준선과 비교해 이상을 mart.anomaly_event 에 기록한다. 새로 발견된 anomaly 만 알린다(warning → 아침 요약).
    같은 구간을 다시 돌려도 행이 늘지 않고 다시 알리지 않는다."""
    from batch.marts import _require_baseline
    until = until or datetime.now(timezone.utc)
    now = now or datetime.now(timezone.utc)
    since = until - timedelta(hours=lookback_hours)
    with _connect(connect) as conn:
        _require_baseline(conn)
        scanned, with_base = conn.execute(COUNT_SQL, (since, until)).fetchone()
        rows = conn.execute(SQL, {"since": since, "until": until, "floor": floor, "min_obs": min_obs, "thr": threshold, "now": now}).fetchall()
    by_status = {"anomaly": 0, "candidate": 0, "degenerate_baseline": 0}
    for r in rows:
        by_status[r[5]] += 1
    out = {"scanned": scanned, "no_baseline": scanned - with_base, "new": by_status, "window": [since.isoformat(), until.isoformat()]}
    anomalies = sorted((r for r in rows if r[5] == "anomaly"), key=lambda r: -abs(r[4]))
    if anomalies:
        if send is None:
            from urbanstream import notify
            send = notify.send
        top = "; ".join(f"{r[0]} {r[2]:.0f}km/h (평소 {r[3]:.0f}, z={r[4]:+.1f})" for r in anomalies[:5])
        send("warning", f"속도 이상 {len(anomalies)}건 (링크 {len({r[0] for r in anomalies})}개)", top, dedup_key="anomaly:traffic_speed")
    LOG.info("이상 탐지: %s", out)
    return out
