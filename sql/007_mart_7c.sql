-- 007: Step 7c. 문제 B(돌발 영향 확정), 문제 D(날씨 × 교통), 창고 관리(표 통계).
-- 마트 표는 파생물이다. 새 표·뷰·함수는 이 파일 안에서 GRANT 한다 (DEFAULT PRIVILEGES 를 쓰지 않는다).

-- ------------------------------------------------------------------ 문제 B: 돌발 영향 (확정본)
-- 돌발 에피소드 1건당 1행. 돌발이 보이던 구간(first_seen_at ~ absent_since)의 속도를 '같은 요일·시각 기준선 중앙값'으로 나눈 비율.
-- absent_since 는 해제 시각이 아니라 "목록에서 처음 안 보인 시각"이다 → 지속 시간(duration_min)은 근사 (ADR-0008 ⑧).
-- 한 번 확정(finalized_at)되면 다시 계산하지 않는다 → 이 표가 있으면 에피소드 원본을 지울 수 있다 (D-95).
-- match_kind: link_id(수집 링크와 id 일치) / distance(수집 링크에서 match_radius_m 이내) / no_link(연결되는 수집 링크 없음).
-- no_link 행도 남긴다 → "몇 건 중 몇 건 연결됨"을 계산할 수 있다.
CREATE TABLE IF NOT EXISTS mart.incident_impact (
    acc_id            text        NOT NULL,
    occurred_at       timestamptz NOT NULL,
    first_seen_at     timestamptz NOT NULL,
    absent_since      timestamptz NOT NULL,
    duration_min      integer     NOT NULL,
    acc_type          text,
    link_id           text,
    match_kind        text        NOT NULL CHECK (match_kind IN ('link_id','distance','no_link')),
    distance_m        double precision,
    n_in              integer     NOT NULL DEFAULT 0,     -- 돌발 구간 안의 속도 관측 수 (기준선이 있는 것만)
    n_pre             integer     NOT NULL DEFAULT 0,     -- 돌발 직전 pre_minutes 분 안의 관측 수
    ratio_in          double precision,                   -- 비율의 중앙값 (1 = 평소와 같음, 작을수록 느림)
    ratio_pre         double precision,
    delta             double precision,                   -- ratio_in - ratio_pre (직전이 부족하면 NULL)
    sufficient        boolean     NOT NULL,
    provisional       boolean     NOT NULL,               -- 쓴 기준선 중 4주 미만이 있음
    finalized_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (acc_id, occurred_at)
);
CREATE INDEX IF NOT EXISTS incident_impact_type_ix ON mart.incident_impact (acc_type);

CREATE OR REPLACE VIEW mart.v_incident_impact_by_type AS
SELECT coalesce(acc_type, 'unknown')                                    AS acc_type,
       count(*)                                                         AS n_episodes,
       count(*) FILTER (WHERE match_kind <> 'no_link')                  AS n_linked,
       count(*) FILTER (WHERE sufficient)                               AS n_sufficient,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio_in) FILTER (WHERE sufficient) AS ratio_in_p50,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY delta)    FILTER (WHERE sufficient AND delta IS NOT NULL) AS delta_p50,
       count(*) FILTER (WHERE sufficient) < 10                          AS provisional
FROM mart.incident_impact
GROUP BY 1;

-- ------------------------------------------------------------------ 문제 D: 날씨 × 교통
-- 링크 × KST 날짜 × KST 시각. 속도 비율 = 그 시간 속도 중앙값 / 같은 요일·시각 기준선 중앙값 (출퇴근 효과를 걸러 낸다).
-- 날씨는 격자 1개의 같은 KST 시각 관측. 인과가 아니라 연관이다.
-- precip_class: none / light / moderate / heavy / snow_mix, 강수량(rn1) 값이 없으면 NULL(알 수 없음).
CREATE TABLE IF NOT EXISTS mart.weather_traffic_hourly (
    link_id       text        NOT NULL,
    kst_date      date        NOT NULL,
    kst_hour      smallint    NOT NULL CHECK (kst_hour BETWEEN 0 AND 23),
    n_speed       integer     NOT NULL,
    speed_p50     double precision,
    baseline_p50  double precision,
    ratio         double precision,
    rn1           double precision,
    pty           double precision,
    t1h           double precision,
    precip_class  text CHECK (precip_class IN ('none','light','moderate','heavy','snow_mix')),
    sufficient    boolean     NOT NULL,                  -- 속도 3개 이상 + 기준선 있음 + 날씨 있음
    provisional   boolean     NOT NULL,                  -- 기준선이 4주 미만
    computed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (link_id, kst_date, kst_hour)
);
CREATE INDEX IF NOT EXISTS weather_traffic_date_ix ON mart.weather_traffic_hourly (kst_date);

CREATE OR REPLACE VIEW mart.v_weather_effect AS
SELECT precip_class,
       count(*)                                               AS n_hours,
       count(DISTINCT link_id)                                AS n_links,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY ratio)    AS ratio_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY ratio)    AS ratio_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY ratio)    AS ratio_p75,
       count(*) < 20                                          AS insufficient,
       bool_or(provisional)                                   AS provisional
FROM mart.weather_traffic_hourly
WHERE sufficient AND precip_class IS NOT NULL
GROUP BY precip_class;
COMMENT ON VIEW mart.v_weather_effect IS '강수 등급별 속도 비율(기준선 대비). 연관이지 인과가 아님. insufficient=true 이면 n_hours<20 이라 읽지 말 것.';

-- ------------------------------------------------------------------ 창고 관리
-- 표 크기·행 수·죽은 튜플 추정치를 주 1회 기록한다 (D-70 파티션 판단 근거). 값은 추정치(pg_class/pg_stat)다.
CREATE TABLE IF NOT EXISTS ops.table_stats (
    snapshot_date   date   NOT NULL,
    schema_name     text   NOT NULL,
    table_name      text   NOT NULL,
    n_live_est      bigint,
    n_dead_est      bigint,
    total_bytes     bigint NOT NULL,
    last_autovacuum timestamptz,
    last_autoanalyze timestamptz,
    PRIMARY KEY (snapshot_date, schema_name, table_name)
);

-- gold_writer 는 표 소유자가 아니라 ANALYZE 를 직접 못 한다 → 소유자 권한 함수로 (mart.refresh_all 과 같은 방식).
-- VACUUM 은 함수 안에서 못 하므로 autovacuum 에 맡기고, 밀리는지는 table_stats.n_dead_est 로 본다 (D-110).
CREATE OR REPLACE FUNCTION ops.analyze_warehouse() RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE r record; n integer := 0;
BEGIN
    FOR r IN SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('dim','fact','mart','ops') AND c.relkind IN ('r','m','p') ORDER BY 1, 2
    LOOP
        EXECUTE format('ANALYZE %I.%I', r.nspname, r.relname);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;
REVOKE ALL ON FUNCTION ops.analyze_warehouse() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.analyze_warehouse() TO gold_writer;

-- ------------------------------------------------------------------ 권한
GRANT SELECT, INSERT, UPDATE, DELETE ON mart.incident_impact, mart.weather_traffic_hourly, ops.table_stats TO gold_writer;
GRANT SELECT ON mart.incident_impact, mart.weather_traffic_hourly, mart.v_incident_impact_by_type, mart.v_weather_effect
      TO api_reader, bi_reader;
