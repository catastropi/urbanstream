-- 006: Step 7b 마트. 문제 A(혼잡 단계 임계값)와 문제 C(버스 도착 예측 드리프트·구간 통과 시간).
-- 마트 표는 파생물이라 날짜 범위를 지우고 다시 넣어도 안전하다(ADR-0009 §2, 팩트는 해당 없음).
-- 새 표·뷰는 이 파일 안에서 읽기 역할에 GRANT 한다 (DEFAULT PRIVILEGES 를 쓰지 않는다).

-- ------------------------------------------------------------------ 문제 A: 혼잡 단계 임계값
-- 링크 × 요일유형 × KST 시각의 속도 분위수. 임계값은 분포에서 나온다(임의 숫자 아님, D-78).
-- 표본이 min_obs 미만인 칸은 분위수를 비우고 sufficient=false. 28일 미만이면 provisional.
-- 공휴일 데이터가 없어서(D-74) day_type='holiday' 는 아직 만들어지지 않는다 (공휴일은 평일로 계산됨).
CREATE TABLE IF NOT EXISTS mart.congestion_threshold (
    version_date date        NOT NULL,
    link_id      text        NOT NULL,
    day_type     text        NOT NULL CHECK (day_type IN ('weekday','sat','sun','holiday')),
    kst_hour     smallint    NOT NULL CHECK (kst_hour BETWEEN 0 AND 23),
    n_obs        integer     NOT NULL,
    n_days       integer     NOT NULL,
    span_days    integer     NOT NULL,
    speed_p10    double precision,
    speed_p25    double precision,
    speed_p50    double precision,
    speed_p90    double precision,
    sufficient   boolean     NOT NULL,
    provisional  boolean     NOT NULL,
    computed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (version_date, link_id, day_type, kst_hour)
);

CREATE OR REPLACE VIEW mart.v_congestion_threshold_latest AS
SELECT * FROM mart.congestion_threshold
WHERE version_date = (SELECT max(version_date) FROM mart.congestion_threshold);

-- 링크별 최근 관측의 혼잡 단계: p10 이하 severe, p25 이하 heavy, p50 이하 slow, 그 위 normal.
-- 표본이 모자라면 level 은 NULL('unknown' 이 아니라 계산하지 않음).
CREATE OR REPLACE VIEW mart.v_traffic_level AS
SELECT l.link_id, l.event_time, l.speed_kmh, th.day_type, th.kst_hour,
       CASE WHEN th.sufficient AND l.speed_kmh IS NOT NULL THEN
            CASE WHEN l.speed_kmh <= th.speed_p10 THEN 'severe'
                 WHEN l.speed_kmh <= th.speed_p25 THEN 'heavy'
                 WHEN l.speed_kmh <= th.speed_p50 THEN 'slow'
                 ELSE 'normal' END
       END AS level,
       th.n_obs AS threshold_n_obs, th.provisional AS threshold_provisional, th.version_date AS threshold_version
FROM (SELECT DISTINCT ON (t.link_id) t.link_id, t.event_time, t.speed_kmh
      FROM fact.traffic_speed t ORDER BY t.link_id, t.event_time DESC) l
LEFT JOIN dim.dim_date d ON d.date_key = (l.event_time AT TIME ZONE 'Asia/Seoul')::date
LEFT JOIN mart.v_congestion_threshold_latest th
       ON th.link_id = l.link_id
      AND th.kst_hour = extract(hour FROM l.event_time AT TIME ZONE 'Asia/Seoul')
      AND th.day_type = CASE WHEN d.is_holiday THEN 'holiday' WHEN d.iso_dow = 6 THEN 'sat'
                             WHEN d.iso_dow = 7 THEN 'sun' ELSE 'weekday' END;

-- ------------------------------------------------------------------ 문제 C-i: 도착 예측 드리프트
-- 같은 버스·정류소의 예측 도착 시각(event_time + arrive_in_sec)이 접근하는 동안 얼마나 밀렸나.
-- drift_sec = 마지막 예측 − 처음 예측. 양수 = 처음 예측보다 늦게 도착하는 쪽으로 밀림.
-- 시간표 대비 지연이 아니다. 마지막 예측이 실제 도착에 가깝다는 가정의 대리 지표(D-57/D-75 전까지).
CREATE TABLE IF NOT EXISTS mart.bus_arrival_drift (
    route_id            text        NOT NULL,
    station_id          text        NOT NULL,
    veh_id              text        NOT NULL,
    first_event_time    timestamptz NOT NULL,
    last_event_time     timestamptz NOT NULL,
    event_date          date        NOT NULL,          -- first_event_time 의 KST 날짜
    kst_hour            smallint    NOT NULL,
    n_pred              integer     NOT NULL,
    first_arrive_in_sec integer     NOT NULL,
    last_arrive_in_sec  integer     NOT NULL,
    first_eta           timestamptz NOT NULL,
    last_eta            timestamptz NOT NULL,
    drift_sec           integer     NOT NULL,
    eta_range_sec       integer     NOT NULL,
    sufficient          boolean     NOT NULL,
    computed_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, station_id, veh_id, first_event_time)
);
CREATE INDEX IF NOT EXISTS bus_arrival_drift_date_ix ON mart.bus_arrival_drift (event_date);

CREATE OR REPLACE VIEW mart.v_bus_drift_daily AS
SELECT route_id, station_id, event_date, kst_hour,
       count(*)                                                  AS n_approaches,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY drift_sec)    AS drift_p50_sec,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY drift_sec)    AS drift_p90_sec,
       count(*) < 10                                             AS provisional
FROM mart.bus_arrival_drift
WHERE sufficient
GROUP BY route_id, station_id, event_date, kst_hour;

-- ------------------------------------------------------------------ 문제 C-ii: 구간 통과 시간
-- 같은 차량이 구간 k 에 처음 관측된 시각 → 구간 k+1 에 처음 관측된 시각 (sect_ord 가 정확히 1 증가할 때만).
-- 관측 주기(약 90초) 때문에 ±90초 오차가 있다. sect_ord 가 구간 경계에서 바뀐다는 가정은 데이터로 확인 전(D-103).
CREATE TABLE IF NOT EXISTS mart.bus_section_time (
    route_id        text        NOT NULL,
    veh_id          text        NOT NULL,
    section_id      text        NOT NULL,
    sect_ord        integer     NOT NULL,
    entered_at      timestamptz NOT NULL,
    next_entered_at timestamptz NOT NULL,
    traversal_sec   integer     NOT NULL,
    event_date      date        NOT NULL,              -- entered_at 의 KST 날짜
    kst_hour        smallint    NOT NULL,
    day_type        text        NOT NULL CHECK (day_type IN ('weekday','sat','sun','holiday')),
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, veh_id, section_id, entered_at)
);
CREATE INDEX IF NOT EXISTS bus_section_time_date_ix ON mart.bus_section_time (event_date);

-- 평소(구간 × 요일유형 × 시각 중앙값) 대비 통과 시간 비. 평소 표본이 10건 미만이면 ratio 는 NULL.
CREATE OR REPLACE VIEW mart.v_bus_section_slowness AS
WITH base AS (
    SELECT route_id, section_id, day_type, kst_hour,
           count(*)                                              AS base_n_obs,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY traversal_sec) AS base_p50_sec,
           (max(event_date) - min(event_date) + 1) < 28          AS base_provisional
    FROM mart.bus_section_time
    GROUP BY 1, 2, 3, 4
)
SELECT s.route_id, s.veh_id, s.section_id, s.sect_ord, s.entered_at, s.traversal_sec, s.event_date, s.kst_hour, s.day_type,
       b.base_p50_sec, b.base_n_obs, b.base_provisional,
       CASE WHEN b.base_n_obs >= 10 AND b.base_p50_sec > 0 THEN s.traversal_sec / b.base_p50_sec END AS ratio_to_base
FROM mart.bus_section_time s
JOIN base b USING (route_id, section_id, day_type, kst_hour);

-- ------------------------------------------------------------------ 권한
GRANT SELECT, INSERT, UPDATE, DELETE ON mart.congestion_threshold, mart.bus_arrival_drift, mart.bus_section_time TO gold_writer;
GRANT SELECT ON mart.congestion_threshold, mart.bus_arrival_drift, mart.bus_section_time,
                mart.v_congestion_threshold_latest, mart.v_traffic_level, mart.v_bus_drift_daily, mart.v_bus_section_slowness
      TO api_reader, bi_reader;
