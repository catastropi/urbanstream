-- 004: Mart. 문제 A 기준선 = 링크 × ISO 요일 × KST 시각의 분포. 4주 미만이면 provisional.
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_link_baseline AS
SELECT
    t.link_id,
    extract(isodow FROM t.event_time AT TIME ZONE 'Asia/Seoul')::smallint AS iso_dow,
    extract(hour   FROM t.event_time AT TIME ZONE 'Asia/Seoul')::smallint AS kst_hour,
    count(*)                                                         AS n_obs,
    count(DISTINCT t.event_date)                                     AS n_days,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY t.speed_kmh)        AS speed_p50,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY t.speed_kmh)        AS speed_p25,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY t.speed_kmh)        AS speed_p75,
    (max(t.event_date) - min(t.event_date) + 1) < 28                 AS provisional
FROM fact.traffic_speed t
WHERE t.speed_kmh IS NOT NULL
GROUP BY 1, 2, 3
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS mv_link_baseline_pk ON mart.mv_link_baseline (link_id, iso_dow, kst_hour);

-- 링크별 최근 값과 기준선 대비 (BI/API 용 읽기 뷰)
CREATE OR REPLACE VIEW mart.v_traffic_latest AS
SELECT DISTINCT ON (t.link_id)
    t.link_id, t.event_time, t.speed_kmh, t.travel_time_sec,
    b.speed_p50 AS baseline_p50, b.provisional AS baseline_provisional,
    CASE WHEN b.speed_p50 > 0 THEN t.speed_kmh / b.speed_p50 END AS ratio_to_baseline
FROM fact.traffic_speed t
LEFT JOIN mart.mv_link_baseline b
  ON b.link_id = t.link_id
 AND b.iso_dow  = extract(isodow FROM t.event_time AT TIME ZONE 'Asia/Seoul')
 AND b.kst_hour = extract(hour   FROM t.event_time AT TIME ZONE 'Asia/Seoul')
ORDER BY t.link_id, t.event_time DESC;

CREATE OR REPLACE VIEW mart.v_incident_episode AS
SELECT e.*, (e.absent_since IS NULL) AS still_listed,
       o.acc_type, o.link_id, o.lon, o.lat
FROM fact.incident_episode e
LEFT JOIN LATERAL (
    SELECT acc_type, link_id, lon, lat FROM fact.incident_observation x
    WHERE x.acc_id = e.acc_id ORDER BY x.fetched_at DESC LIMIT 1
) o ON true;

CREATE OR REPLACE VIEW mart.v_weather_hourly AS
SELECT w.*, d.iso_dow, d.is_weekend
FROM fact.weather_obs w
JOIN dim.dim_date d ON d.date_key = (w.base_time AT TIME ZONE 'Asia/Seoul')::date;

-- 새로고침은 소유자 권한으로만 (gold_writer 가 부른다)
CREATE OR REPLACE FUNCTION mart.refresh_all() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, mart, fact AS $$
BEGIN
    -- 첫 갱신(WITH NO DATA)은 CONCURRENTLY 가 불가능하다
    IF NOT (SELECT ispopulated FROM pg_matviews WHERE schemaname='mart' AND matviewname='mv_link_baseline') THEN
        REFRESH MATERIALIZED VIEW mart.mv_link_baseline;
    ELSE
        REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_link_baseline;
    END IF;
END $$;
