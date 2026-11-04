"""Step 7b 마트 계산 (Postgres SQL). Airflow DAG(baseline_build, bus_delay)가 부르는 얇은 함수 모음.

- 계산은 Spark 가 아니라 SQL 이다: 데이터가 작다(링크 4개·노선 1개, ADR-0009 §2).
- 마트 표는 파생물이라 **날짜 범위를 지우고 다시 넣는다**(한 트랜잭션). 같은 구간을 다시 돌려도 결과가 같다.
- 선행 조건: 최근 gold_load 가 ok 여야 한다(require_recent_load). 덜 쌓인 데이터로 마트를 만들지 않는다.
- psycopg 는 함수 안에서 불러온다 → DAG 파싱이 가볍다 (batch.jobs 와 같은 규칙).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Optional

from batch.jobs import JobError

LOG = logging.getLogger("urbanstream.batch.marts")

MIN_OBS_THRESHOLD = 20          # 임계값 칸당 최소 관측 수 (미만이면 분위수를 비운다)
DRIFT_GAP_SEC = 20 * 60         # 같은 버스·정류소 예측이 이 시간 넘게 끊기면 다른 접근(왕복 구분)
DRIFT_MIN_PRED = 3              # 접근 1회의 최소 예측 수 (미만이면 sufficient=false)
SECTION_MAX_GAP_SEC = 30 * 60   # 구간 k → k+1 첫 관측 간격 상한 (넘으면 중간에 놓친 것으로 보고 버린다)

# KST 날짜 → 요일유형. 공휴일 데이터가 없어(D-74) 지금은 holiday 가 나오지 않는다.
_DAY_TYPE = ("CASE WHEN d.is_holiday THEN 'holiday' WHEN d.iso_dow = 6 THEN 'sat' "
             "WHEN d.iso_dow = 7 THEN 'sun' ELSE 'weekday' END")


def _connect(connect: Optional[Callable[[], Any]]):
    if connect is not None:
        return connect()
    from batch.jobs import _default_connect
    return _default_connect()


def require_recent_load(d0: date, d1: date, *, max_age_hours: float = 36.0,
                        connect: Optional[Callable[[], Any]] = None) -> int:
    """d0~d1 를 덮는 ok 적재(gold_load)가 최근 max_age_hours 안에 있어야 한다. 없으면 JobError → Airflow 가 재시도한다."""
    with _connect(connect) as conn:
        row = conn.execute(
            "SELECT run_id FROM ops.load_run WHERE job='gold_load' AND status='ok' AND date_from <= %s AND date_to >= %s "
            "AND finished_at >= now() - make_interval(secs => %s) ORDER BY finished_at DESC LIMIT 1",
            (d0, d1, max_age_hours * 3600)).fetchone()
    if row is None:
        raise JobError(f"{d0}~{d1} 를 덮는 성공한 적재(gold_load)가 최근 {max_age_hours:g}시간 안에 없습니다 — gold_daily 를 먼저 확인하세요")
    return row[0]


def _require_baseline(conn) -> None:
    """mv_link_baseline 이 한 번도 채워지지 않았으면 기준선 비율을 만들 수 없다 → 알아보기 쉬운 오류로 멈춘다 (gold_daily 가 채운다)."""
    row = conn.execute("SELECT ispopulated FROM pg_matviews WHERE schemaname='mart' AND matviewname='mv_link_baseline'").fetchone()
    if row is None or not row[0]:
        raise JobError("기준선(mart.mv_link_baseline)이 아직 한 번도 채워지지 않았습니다 — gold_daily 가 먼저 성공해야 합니다")


def build_congestion_threshold(version_date: date, *, min_obs: int = MIN_OBS_THRESHOLD,
                               connect: Optional[Callable[[], Any]] = None) -> dict:
    """링크 × 요일유형 × KST 시각의 속도 분위수(p10/p25/p50/p90)를 version_date 판으로 만든다 (같은 날 다시 돌리면 교체)."""
    with _connect(connect) as conn, conn.transaction():
        conn.execute("DELETE FROM mart.congestion_threshold WHERE version_date = %s", (version_date,))
        cur = conn.execute(f"""
            INSERT INTO mart.congestion_threshold
                (version_date, link_id, day_type, kst_hour, n_obs, n_days, span_days,
                 speed_p10, speed_p25, speed_p50, speed_p90, sufficient, provisional)
            SELECT %(v)s, k.link_id, k.day_type, k.kst_hour, count(*), count(DISTINCT k.kst_date),
                   max(k.kst_date) - min(k.kst_date) + 1,
                   CASE WHEN count(*) >= %(m)s THEN percentile_cont(0.10) WITHIN GROUP (ORDER BY k.speed_kmh) END,
                   CASE WHEN count(*) >= %(m)s THEN percentile_cont(0.25) WITHIN GROUP (ORDER BY k.speed_kmh) END,
                   CASE WHEN count(*) >= %(m)s THEN percentile_cont(0.50) WITHIN GROUP (ORDER BY k.speed_kmh) END,
                   CASE WHEN count(*) >= %(m)s THEN percentile_cont(0.90) WITHIN GROUP (ORDER BY k.speed_kmh) END,
                   count(*) >= %(m)s,
                   (max(k.kst_date) - min(k.kst_date) + 1) < 28
            FROM (
                SELECT t.link_id, t.speed_kmh,
                       (t.event_time AT TIME ZONE 'Asia/Seoul')::date AS kst_date,
                       extract(hour FROM t.event_time AT TIME ZONE 'Asia/Seoul')::smallint AS kst_hour,
                       {_DAY_TYPE} AS day_type
                FROM fact.traffic_speed t
                JOIN dim.dim_date d ON d.date_key = (t.event_time AT TIME ZONE 'Asia/Seoul')::date
                WHERE t.speed_kmh IS NOT NULL
            ) k
            GROUP BY k.link_id, k.day_type, k.kst_hour
            """, {"v": version_date, "m": min_obs})
        n = cur.rowcount
        suff = conn.execute("SELECT count(*) FILTER (WHERE sufficient), count(*) FILTER (WHERE provisional) "
                            "FROM mart.congestion_threshold WHERE version_date = %s", (version_date,)).fetchone()
    LOG.info("혼잡 임계값 %s: %d칸 (충분 %d, 잠정 %d)", version_date, n, suff[0], suff[1])
    return {"version_date": version_date.isoformat(), "cells": n, "sufficient": suff[0], "provisional": suff[1]}


def build_bus_drift(d0: date, d1: date, *, gap_sec: int = DRIFT_GAP_SEC, min_pred: int = DRIFT_MIN_PRED,
                    connect: Optional[Callable[[], Any]] = None) -> dict:
    """버스 도착 예측 드리프트. 접근 1회 = 같은 (노선, 정류소, 차량)의 예측이 gap_sec 이내로 이어지는 묶음.
    d0~d1(KST) 에 **시작한** 접근만 지우고 다시 넣는다(하루 전부터 읽어 자정을 넘는 접근도 한 덩어리로 본다)."""
    if d0 > d1:
        raise ValueError("d0 > d1")
    with _connect(connect) as conn, conn.transaction():
        conn.execute("DELETE FROM mart.bus_arrival_drift WHERE event_date BETWEEN %s AND %s", (d0, d1))
        cur = conn.execute("""
            WITH src AS (
                SELECT route_id, station_id, veh_id, event_time, arrive_in_sec,
                       event_time + make_interval(secs => arrive_in_sec) AS eta
                FROM fact.bus_arrival
                WHERE event_date BETWEEN %(d0)s - 1 AND %(d1)s
                  AND arrive_in_sec IS NOT NULL AND arrive_in_sec >= 0
                  AND coalesce(arrive_code, 0) <> 2          -- 2 = 출발대기 행(노선 시작점): 예측이 의미 없다
            ), flagged AS (
                SELECT *, CASE WHEN lag(event_time) OVER w IS NULL
                                 OR event_time - lag(event_time) OVER w > make_interval(secs => %(gap)s)
                               THEN 1 ELSE 0 END AS new_grp
                FROM src WINDOW w AS (PARTITION BY route_id, station_id, veh_id ORDER BY event_time)
            ), grp AS (
                SELECT *, sum(new_grp) OVER (PARTITION BY route_id, station_id, veh_id ORDER BY event_time) AS g
                FROM flagged
            ), agg AS (
                SELECT route_id, station_id, veh_id, g,
                       min(event_time) AS first_t, max(event_time) AS last_t, count(*) AS n,
                       (array_agg(arrive_in_sec ORDER BY event_time))[1]      AS first_in,
                       (array_agg(arrive_in_sec ORDER BY event_time DESC))[1] AS last_in,
                       (array_agg(eta ORDER BY event_time))[1]                AS first_eta,
                       (array_agg(eta ORDER BY event_time DESC))[1]           AS last_eta,
                       max(eta) - min(eta)                                    AS eta_range
                FROM grp GROUP BY route_id, station_id, veh_id, g
            )
            INSERT INTO mart.bus_arrival_drift
                (route_id, station_id, veh_id, first_event_time, last_event_time, event_date, kst_hour, n_pred,
                 first_arrive_in_sec, last_arrive_in_sec, first_eta, last_eta, drift_sec, eta_range_sec, sufficient)
            SELECT route_id, station_id, veh_id, first_t, last_t,
                   (first_t AT TIME ZONE 'Asia/Seoul')::date,
                   extract(hour FROM first_t AT TIME ZONE 'Asia/Seoul')::smallint,
                   n, first_in, last_in, first_eta, last_eta,
                   round(extract(epoch FROM last_eta - first_eta))::int,
                   round(extract(epoch FROM eta_range))::int,
                   n >= %(minp)s
            FROM agg
            WHERE (first_t AT TIME ZONE 'Asia/Seoul')::date BETWEEN %(d0)s AND %(d1)s
            """, {"d0": d0, "d1": d1, "gap": gap_sec, "minp": min_pred})
        n = cur.rowcount
        suff = conn.execute("SELECT count(*) FILTER (WHERE sufficient) FROM mart.bus_arrival_drift "
                            "WHERE event_date BETWEEN %s AND %s", (d0, d1)).fetchone()[0]
    LOG.info("버스 도착 드리프트 %s~%s: 접근 %d건 (충분 %d)", d0, d1, n, suff)
    return {"window": [d0.isoformat(), d1.isoformat()], "approaches": n, "sufficient": suff}


def build_bus_section_time(d0: date, d1: date, *, max_gap_sec: int = SECTION_MAX_GAP_SEC,
                           connect: Optional[Callable[[], Any]] = None) -> dict:
    """구간 통과 시간: 구간 k 첫 관측 → 구간 k+1 첫 관측 (sect_ord 가 정확히 1 증가, 간격이 max_gap_sec 이내일 때만)."""
    if d0 > d1:
        raise ValueError("d0 > d1")
    with _connect(connect) as conn, conn.transaction():
        conn.execute("DELETE FROM mart.bus_section_time WHERE event_date BETWEEN %s AND %s", (d0, d1))
        cur = conn.execute(f"""
            WITH pos AS (
                SELECT route_id, veh_id, section_id, sect_ord, event_time,
                       lag(section_id) OVER w AS prev_section, lag(event_time) OVER w AS prev_time
                FROM fact.bus_position
                WHERE event_date BETWEEN %(d0)s - 1 AND %(d1)s
                  AND route_id IS NOT NULL AND section_id IS NOT NULL AND sect_ord IS NOT NULL
                WINDOW w AS (PARTITION BY route_id, veh_id ORDER BY event_time)
            ), entries AS (                  -- 차량이 구간에 들어온 첫 관측 (구간이 바뀌었거나 오래 안 보이다 나타난 곳)
                SELECT route_id, veh_id, section_id, sect_ord, event_time AS entered_at
                FROM pos
                WHERE prev_section IS DISTINCT FROM section_id
                   OR event_time - prev_time > make_interval(secs => %(gap)s)
            ), paired AS (
                SELECT *, lead(sect_ord)   OVER w AS next_ord,
                          lead(entered_at) OVER w AS next_entered_at
                FROM entries WINDOW w AS (PARTITION BY route_id, veh_id ORDER BY entered_at)
            )
            INSERT INTO mart.bus_section_time
                (route_id, veh_id, section_id, sect_ord, entered_at, next_entered_at, traversal_sec,
                 event_date, kst_hour, day_type)
            SELECT p.route_id, p.veh_id, p.section_id, p.sect_ord, p.entered_at, p.next_entered_at,
                   round(extract(epoch FROM p.next_entered_at - p.entered_at))::int,
                   (p.entered_at AT TIME ZONE 'Asia/Seoul')::date,
                   extract(hour FROM p.entered_at AT TIME ZONE 'Asia/Seoul')::smallint,
                   {_DAY_TYPE}
            FROM paired p
            JOIN dim.dim_date d ON d.date_key = (p.entered_at AT TIME ZONE 'Asia/Seoul')::date
            WHERE p.next_ord = p.sect_ord + 1
              AND p.next_entered_at - p.entered_at <= make_interval(secs => %(gap)s)
              AND (p.entered_at AT TIME ZONE 'Asia/Seoul')::date BETWEEN %(d0)s AND %(d1)s
            """, {"d0": d0, "d1": d1, "gap": max_gap_sec})
        n = cur.rowcount
    LOG.info("버스 구간 통과 시간 %s~%s: %d건", d0, d1, n)
    return {"window": [d0.isoformat(), d1.isoformat()], "traversals": n}


# ====================================================================== Step 7c

INCIDENT_FINALIZE_AFTER_HOURS = 24      # absent_since 가 이만큼 지나야 확정 (다시 나타나는 돌발을 거른다)
INCIDENT_MATCH_RADIUS_M = 300.0         # 돌발 → 수집 링크 거리 상한 (link_id 가 안 맞을 때)
INCIDENT_PRE_MINUTES = 60               # 돌발 직전 비교 구간
INCIDENT_MIN_OBS = 3                    # 구간 안/직전 속도 관측 최소 수
WEATHER_MIN_SPEED_OBS = 3               # 시간당 속도 관측 최소 수
RAIN_LIGHT_MAX_MM = 1.0                 # rn1 < 1 → light
RAIN_HEAVY_MIN_MM = 10.0                # rn1 >= 10 → heavy
SNOW_MIX_PTY = (2, 3, 6, 7)             # 비/눈, 눈, 빗방울/눈날림, 눈날림 (기상청 초단기실황 PTY 코드, [추정] 코드표 기준)
RETENTION_DAYS = 180


def build_incident_impact(*, as_of=None, finalize_after_hours: float = INCIDENT_FINALIZE_AFTER_HOURS,
                          radius_m: float = INCIDENT_MATCH_RADIUS_M, pre_minutes: int = INCIDENT_PRE_MINUTES,
                          min_obs: int = INCIDENT_MIN_OBS, connect: Optional[Callable[[], Any]] = None) -> dict:
    """해제된 지 finalize_after_hours 이상 된 돌발 에피소드를 한 번 확정한다. 이미 확정된 에피소드는 건드리지 않는다.
    장기(is_long_term) 에피소드는 영향으로 읽을 수 없어 제외하고 개수만 센다. as_of 는 테스트용(기본 now())."""
    with _connect(connect) as conn, conn.transaction():
        _require_baseline(conn)
        p = {"asof": as_of, "fin": finalize_after_hours * 3600, "rad": radius_m, "pre": pre_minutes * 60, "minn": min_obs}
        cur = conn.execute("""
            WITH asof AS (SELECT coalesce(%(asof)s::timestamptz, now()) AS t),
            eps AS (
                SELECT e.acc_id, e.occurred_at, e.first_seen_at, e.absent_since,
                       o.acc_type, o.link_id AS obs_link, o.lon, o.lat
                FROM fact.incident_episode e
                CROSS JOIN asof
                LEFT JOIN LATERAL (
                    SELECT acc_type, link_id, lon, lat FROM fact.incident_observation x
                    WHERE x.acc_id = e.acc_id ORDER BY x.fetched_at DESC LIMIT 1) o ON true
                WHERE e.absent_since IS NOT NULL AND NOT e.is_long_term
                  AND e.absent_since <= asof.t - make_interval(secs => %(fin)s)
                  AND NOT EXISTS (SELECT 1 FROM mart.incident_impact m
                                  WHERE m.acc_id = e.acc_id AND m.occurred_at = e.occurred_at)
            ), matched AS (
                SELECT eps.*, m.link_id, m.kind, m.dist FROM eps
                LEFT JOIN LATERAL (
                    SELECT c.link_id,
                           CASE WHEN c.link_id = eps.obs_link THEN 'link_id' ELSE 'distance' END AS kind,
                           CASE WHEN eps.lon IS NOT NULL AND eps.lat IS NOT NULL THEN
                                ST_Distance(c.geom::geography, ST_SetSRID(ST_MakePoint(eps.lon, eps.lat), 4326)::geography) END AS dist
                    FROM dim.dim_link c
                    WHERE c.is_collected
                      AND (c.link_id = eps.obs_link
                           OR (eps.lon IS NOT NULL AND eps.lat IS NOT NULL
                               AND ST_DWithin(c.geom::geography, ST_SetSRID(ST_MakePoint(eps.lon, eps.lat), 4326)::geography, %(rad)s)))
                    ORDER BY (c.link_id = eps.obs_link) DESC, dist NULLS LAST, c.link_id LIMIT 1) m ON true
            ), ratios AS (                   -- 속도 관측마다 같은 요일·시각 기준선 대비 비율
                SELECT mt.acc_id, mt.occurred_at,
                       (t.event_time >= mt.first_seen_at) AS in_window,
                       t.speed_kmh / nullif(b.speed_p50, 0) AS ratio, b.provisional
                FROM matched mt
                JOIN fact.traffic_speed t ON t.link_id = mt.link_id AND t.speed_kmh IS NOT NULL
                 AND t.event_time >= mt.first_seen_at - make_interval(secs => %(pre)s) AND t.event_time < mt.absent_since
                JOIN mart.mv_link_baseline b ON b.link_id = t.link_id
                 AND b.iso_dow  = extract(isodow FROM t.event_time AT TIME ZONE 'Asia/Seoul')
                 AND b.kst_hour = extract(hour   FROM t.event_time AT TIME ZONE 'Asia/Seoul')
                WHERE mt.link_id IS NOT NULL
            ), agg AS (
                SELECT acc_id, occurred_at,
                       count(*) FILTER (WHERE in_window)  AS n_in,
                       count(*) FILTER (WHERE NOT in_window) AS n_pre,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) FILTER (WHERE in_window)     AS r_in,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) FILTER (WHERE NOT in_window) AS r_pre,
                       bool_or(provisional) AS prov
                FROM ratios WHERE ratio IS NOT NULL GROUP BY 1, 2
            )
            INSERT INTO mart.incident_impact
                (acc_id, occurred_at, first_seen_at, absent_since, duration_min, acc_type, link_id, match_kind, distance_m,
                 n_in, n_pre, ratio_in, ratio_pre, delta, sufficient, provisional)
            SELECT mt.acc_id, mt.occurred_at, mt.first_seen_at, mt.absent_since,
                   round(extract(epoch FROM mt.absent_since - mt.first_seen_at) / 60)::int,
                   mt.acc_type, mt.link_id, coalesce(mt.kind, 'no_link'), mt.dist,
                   coalesce(a.n_in, 0), coalesce(a.n_pre, 0),
                   CASE WHEN coalesce(a.n_in, 0)  >= %(minn)s THEN a.r_in  END,
                   CASE WHEN coalesce(a.n_pre, 0) >= %(minn)s THEN a.r_pre END,
                   CASE WHEN coalesce(a.n_in, 0) >= %(minn)s AND coalesce(a.n_pre, 0) >= %(minn)s THEN a.r_in - a.r_pre END,
                   coalesce(a.n_in, 0) >= %(minn)s,
                   coalesce(a.prov, true)
            FROM matched mt LEFT JOIN agg a USING (acc_id, occurred_at)
            """, p)
        new = cur.rowcount
        s = conn.execute("""
            SELECT count(*) FILTER (WHERE match_kind <> 'no_link'), count(*) FILTER (WHERE match_kind = 'no_link'),
                   count(*) FILTER (WHERE sufficient) FROM mart.incident_impact""").fetchone()
        sk = conn.execute("""
            SELECT count(*) FILTER (WHERE is_long_term),
                   count(*) FILTER (WHERE NOT is_long_term AND absent_since IS NULL),
                   count(*) FILTER (WHERE NOT is_long_term AND absent_since IS NOT NULL
                                    AND absent_since > coalesce(%(asof)s::timestamptz, now()) - make_interval(secs => %(fin)s))
            FROM fact.incident_episode""", p).fetchone()
    LOG.info("돌발 영향 확정: 신규 %d (누적 연결 %d / 비연결 %d / 충분 %d), 장기 제외 %d, 아직 목록에 있음 %d, 확정 대기 %d",
             new, s[0], s[1], s[2], sk[0], sk[1], sk[2])
    return {"new": new, "linked_total": s[0], "no_link_total": s[1], "sufficient_total": s[2],
            "long_term_skipped": sk[0], "still_listed": sk[1], "waiting": sk[2]}


def build_weather_traffic(d0: date, d1: date, *, min_speed_obs: int = WEATHER_MIN_SPEED_OBS,
                          light_max: float = RAIN_LIGHT_MAX_MM, heavy_min: float = RAIN_HEAVY_MIN_MM,
                          connect: Optional[Callable[[], Any]] = None) -> dict:
    """링크 × KST 시각의 속도 비율(기준선 대비)에 같은 시각 날씨를 붙인다. d0~d1(KST 날짜) 를 지우고 다시 넣는다."""
    if d0 > d1:
        raise ValueError("d0 > d1")
    with _connect(connect) as conn, conn.transaction():
        _require_baseline(conn)
        conn.execute("DELETE FROM mart.weather_traffic_hourly WHERE kst_date BETWEEN %s AND %s", (d0, d1))
        cur = conn.execute("""
            WITH spd AS (
                SELECT t.link_id,
                       (t.event_time AT TIME ZONE 'Asia/Seoul')::date AS kst_date,
                       extract(hour FROM t.event_time AT TIME ZONE 'Asia/Seoul')::smallint AS kst_hour,
                       count(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY t.speed_kmh) AS p50
                FROM fact.traffic_speed t
                WHERE t.speed_kmh IS NOT NULL AND t.event_date BETWEEN %(d0)s - 1 AND %(d1)s + 1
                  AND (t.event_time AT TIME ZONE 'Asia/Seoul')::date BETWEEN %(d0)s AND %(d1)s
                GROUP BY 1, 2, 3
            ), wx AS (                         -- 격자가 여러 개여도 (nx, ny) 가 가장 작은 하나만: 현재 격자 1개
                SELECT DISTINCT ON (kd, kh) kd, kh, rn1, pty, t1h FROM (
                    SELECT (w.base_time AT TIME ZONE 'Asia/Seoul')::date AS kd,
                           extract(hour FROM w.base_time AT TIME ZONE 'Asia/Seoul')::smallint AS kh,
                           w.nx, w.ny, w.rn1, w.pty, w.t1h, w.base_time
                    FROM fact.weather_obs w
                    WHERE (w.base_time AT TIME ZONE 'Asia/Seoul')::date BETWEEN %(d0)s AND %(d1)s) z
                ORDER BY kd, kh, nx, ny, base_time DESC
            )
            INSERT INTO mart.weather_traffic_hourly
                (link_id, kst_date, kst_hour, n_speed, speed_p50, baseline_p50, ratio, rn1, pty, t1h, precip_class, sufficient, provisional)
            SELECT s.link_id, s.kst_date, s.kst_hour, s.n, s.p50, b.speed_p50,
                   s.p50 / nullif(b.speed_p50, 0), x.rn1, x.pty, x.t1h, pc.cls,
                   coalesce(s.n >= %(minn)s AND b.speed_p50 > 0 AND pc.cls IS NOT NULL, false),   -- 기준선이 없으면 NULL → false
                   coalesce(b.provisional, true)
            FROM spd s
            LEFT JOIN dim.dim_date d ON d.date_key = s.kst_date
            LEFT JOIN mart.mv_link_baseline b ON b.link_id = s.link_id AND b.iso_dow = d.iso_dow AND b.kst_hour = s.kst_hour
            LEFT JOIN wx x ON x.kd = s.kst_date AND x.kh = s.kst_hour
            CROSS JOIN LATERAL (SELECT CASE
                    WHEN x.pty = ANY(%(snow)s) THEN 'snow_mix'
                    WHEN x.rn1 IS NULL THEN NULL
                    WHEN x.rn1 >= %(heavy)s THEN 'heavy'
                    WHEN x.rn1 >= %(light)s THEN 'moderate'
                    WHEN x.rn1 > 0 OR coalesce(x.pty, 0) > 0 THEN 'light'
                    ELSE 'none' END AS cls) pc
            """, {"d0": d0, "d1": d1, "minn": min_speed_obs, "light": light_max, "heavy": heavy_min,
                  "snow": [float(v) for v in SNOW_MIX_PTY]})
        n = cur.rowcount
        s = conn.execute("SELECT count(*) FILTER (WHERE sufficient), count(*) FILTER (WHERE precip_class IS NULL) "
                         "FROM mart.weather_traffic_hourly WHERE kst_date BETWEEN %s AND %s", (d0, d1)).fetchone()
    LOG.info("날씨×교통 %s~%s: %d행 (충분 %d, 날씨 없음 %d)", d0, d1, n, s[0], s[1])
    return {"window": [d0.isoformat(), d1.isoformat()], "rows": n, "sufficient": s[0], "no_weather": s[1]}
