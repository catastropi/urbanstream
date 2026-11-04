-- 002: 차원 표
CREATE TABLE IF NOT EXISTS dim.dim_link (
    link_id      text PRIMARY KEY,
    vertex_count integer          NOT NULL CHECK (vertex_count >= 2),
    length_m     double precision NOT NULL CHECK (length_m >= 0),
    geom         geometry(LineString, 4326) NOT NULL,
    is_collected boolean          NOT NULL DEFAULT false,   -- config/sources.yaml 의 수집 대상 여부
    source_note  text
);
CREATE INDEX IF NOT EXISTS dim_link_geom_gix ON dim.dim_link USING gist (geom);
COMMENT ON COLUMN dim.dim_link.length_m IS '꼭짓점(EPSG:5181) 사이 거리 합. 평면 근사이며 공식 링크 길이와 다를 수 있다.';

CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_key     date PRIMARY KEY,
    iso_dow      smallint NOT NULL CHECK (iso_dow BETWEEN 1 AND 7),
    is_weekend   boolean  NOT NULL,
    is_holiday   boolean  NOT NULL DEFAULT false,   -- 공휴일 데이터는 아직 없음 (DEFERRED): 전부 false
    holiday_name text
);
INSERT INTO dim.dim_date (date_key, iso_dow, is_weekend)
SELECT d::date, extract(isodow FROM d)::smallint, extract(isodow FROM d) >= 6
FROM generate_series('2026-01-01'::date, '2028-12-31'::date, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim.dim_weather_category (
    category text PRIMARY KEY,
    name_ko  text NOT NULL,
    unit     text NOT NULL
);
INSERT INTO dim.dim_weather_category VALUES
 ('T1H','기온','℃'),('RN1','1시간 강수량','mm'),('UUU','동서바람성분','m/s'),('VVV','남북바람성분','m/s'),
 ('REH','습도','%'),('PTY','강수형태','code'),('VEC','풍향','deg'),('WSD','풍속','m/s')
ON CONFLICT (category) DO NOTHING;
