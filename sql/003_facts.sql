-- 003: 사실 표. PK = Silver 자연키(dedup_key) → 재적재가 중복을 만들지 않는다.
CREATE TABLE IF NOT EXISTS fact.traffic_speed (
    dedup_key       text PRIMARY KEY,
    link_id         text        NOT NULL,
    event_time      timestamptz NOT NULL,
    fetched_at      timestamptz NOT NULL,
    speed_kmh       double precision,
    travel_time_sec integer,
    dq_flags        text[]      NOT NULL DEFAULT '{}',
    event_date      date        NOT NULL
);
CREATE INDEX IF NOT EXISTS traffic_speed_link_time_ix ON fact.traffic_speed (link_id, event_time);
CREATE INDEX IF NOT EXISTS traffic_speed_date_ix ON fact.traffic_speed (event_date);

CREATE TABLE IF NOT EXISTS fact.bus_position (
    dedup_key    text PRIMARY KEY,
    route_id     text,
    veh_id       text        NOT NULL,
    plain_no     text,
    event_time   timestamptz NOT NULL,
    fetched_at   timestamptz NOT NULL,
    lon          double precision,
    lat          double precision,
    section_id   text,
    sect_ord     integer,
    last_stop_id text,
    next_stop_id text,
    sect_dist    double precision,
    full_sect_dist double precision,
    rt_dist      double precision,
    congestion   integer,
    stop_flag    integer,
    is_running   boolean,
    is_last      boolean,
    is_full      boolean,
    dq_flags     text[]      NOT NULL DEFAULT '{}',
    event_date   date        NOT NULL
);
CREATE INDEX IF NOT EXISTS bus_position_veh_time_ix ON fact.bus_position (veh_id, event_time);
CREATE INDEX IF NOT EXISTS bus_position_date_ix ON fact.bus_position (event_date);

CREATE TABLE IF NOT EXISTS fact.bus_arrival (
    dedup_key      text PRIMARY KEY,
    route_id       text        NOT NULL,
    station_id     text        NOT NULL,
    station_order  integer     NOT NULL,
    slot           smallint    NOT NULL,
    veh_id         text        NOT NULL,
    plain_no       text,
    event_time     timestamptz NOT NULL,
    fetched_at     timestamptz NOT NULL,
    arrival_msg    text,
    stops_away     integer,
    arrive_in_sec  integer,
    pred_exps_sec  integer,
    pred_kals_sec  integer,
    pred_neus_sec  integer,
    arrive_code    integer,
    is_last_bus    boolean,
    dq_flags       text[]      NOT NULL DEFAULT '{}',
    event_date     date        NOT NULL
);
CREATE INDEX IF NOT EXISTS bus_arrival_station_time_ix ON fact.bus_arrival (route_id, station_id, event_time);
CREATE INDEX IF NOT EXISTS bus_arrival_date_ix ON fact.bus_arrival (event_date);

-- 돌발 = 관측 로그 (호출마다 같은 돌발이 다시 나오는 것이 정상, ADR-0007)
CREATE TABLE IF NOT EXISTS fact.incident_observation (
    dedup_key             text        NOT NULL,   -- 내용 해시 기반: 같은 돌발이 호출마다 같은 값으로 나온다
    ingest_id             text        NOT NULL,   -- 호출 1회의 ID → (dedup_key, ingest_id) 가 한 관측

    acc_id                text        NOT NULL,
    link_id               text,
    fetched_at            timestamptz NOT NULL,
    occurred_at           timestamptz,
    expected_clear_at     timestamptz,
    acc_type              text,
    acc_detail_type       text,
    lon                   double precision,
    lat                   double precision,
    geom                  geometry(Point, 4326),
    description           text,
    road_code             text,
    planned_duration_days double precision,
    is_long_term          boolean     NOT NULL DEFAULT false,
    dq_flags              text[]      NOT NULL DEFAULT '{}',
    observed_date         date        NOT NULL,
    PRIMARY KEY (dedup_key, ingest_id)
);
CREATE INDEX IF NOT EXISTS incident_obs_acc_ix ON fact.incident_observation (acc_id, fetched_at);
CREATE INDEX IF NOT EXISTS incident_obs_link_ix ON fact.incident_observation (link_id);
CREATE INDEX IF NOT EXISTS incident_obs_geom_gix ON fact.incident_observation USING gist (geom);

-- 기상: 시각×격자 한 줄(8개 항목을 열로). 항목이 비면 NULL
CREATE TABLE IF NOT EXISTS fact.weather_obs (
    base_time  timestamptz NOT NULL,
    nx         integer     NOT NULL,
    ny         integer     NOT NULL,
    t1h double precision, rn1 double precision, uuu double precision, vvv double precision,
    reh double precision, pty double precision, vec double precision, wsd double precision,
    value_unparsed_count integer NOT NULL DEFAULT 0,
    event_date date        NOT NULL,
    PRIMARY KEY (base_time, nx, ny)
);
CREATE INDEX IF NOT EXISTS weather_obs_date_ix ON fact.weather_obs (event_date);

-- 돌발 수명: 성공·비절단 호출 감사 기준 (ADR-0008 ⑧). absent_since 는 '해제'가 아니라 '목록에서 안 보이기 시작한 첫 성공 호출'
CREATE TABLE IF NOT EXISTS fact.incident_episode (
    acc_id            text        NOT NULL,
    occurred_at       timestamptz NOT NULL,
    first_seen_at     timestamptz NOT NULL,
    last_seen_at      timestamptz NOT NULL,
    absent_since      timestamptz,
    expected_clear_at timestamptz,
    obs_count         integer     NOT NULL CHECK (obs_count > 0),
    is_long_term      boolean     NOT NULL DEFAULT false,
    PRIMARY KEY (acc_id, occurred_at)
);

-- 호출 감사 (수집기 audit 로그에서). Step 8 DQ·수집 공백 판단용
CREATE TABLE IF NOT EXISTS ops.call_audit (
    ingest_id    text PRIMARY KEY,
    source_system text       NOT NULL,
    target       text,
    fetched_at   timestamptz NOT NULL,
    status_kind  text,
    ok           boolean     NOT NULL,
    http_status  integer,
    latency_ms   integer,
    truncated    boolean     NOT NULL DEFAULT false,
    record_count integer,
    event_date   date        NOT NULL
);
CREATE INDEX IF NOT EXISTS call_audit_src_time_ix ON ops.call_audit (source_system, fetched_at);
