-- 009: Step 8a. 알림·실시간 감시용 운영 표와 읽기 뷰. 결정 근거: docs/adr/0010-step8-dq-monitoring.md
-- 새 표·뷰·함수는 이 파일 안에서 GRANT 한다 (DEFAULT PRIVILEGES 를 쓰지 않는다 — 005 의 GRANT ... ON ALL TABLES 는 그때 있던 표에만 적용됐다).
-- api_reader / bi_reader 에는 ops 스키마 USAGE 와 아래 **뷰** SELECT 만 준다. 원본 표(alert_event, call_event ...)는 주지 않는다 (D-102).

-- ------------------------------------------------------------------ 적재 이력에 DAG 이름 (D-107)
-- job 은 계속 'gold_load' 다: marts.require_recent_load 와 테스트가 job='gold_load' 로 거른다. 새 열은 덧붙임이라 기존 코드가 그대로 동작한다.
ALTER TABLE ops.load_run ADD COLUMN IF NOT EXISTS dag_id text;
COMMENT ON COLUMN ops.load_run.dag_id IS '적재를 부른 Airflow DAG (gold_intraday / gold_daily / backfill ...). 009 이전 행은 NULL.';

-- ------------------------------------------------------------------ 수집기 감사 이벤트 (Kafka platform.audit 의 거울)
-- 기존 ops.call_audit 는 '최종 결과 행'만 시간 단위로 적재한다(재시도·skipped 는 버림, D-77). 이 표는 모든 이벤트를 몇 분 안에 담는다.
-- PK = (파티션, offset) 이라 같은 메시지를 다시 읽어도 행이 늘지 않는다. recorded_at 은 수집기(호스트) 시계다.
CREATE TABLE IF NOT EXISTS ops.call_event (
    kafka_partition smallint    NOT NULL,
    kafka_offset    bigint      NOT NULL,
    recorded_at     timestamptz NOT NULL,
    event           text        NOT NULL,            -- 'call' | 'host_resume' | 그 밖(무시하지 않고 보존)
    source_system   text,
    target          text,
    final           boolean,
    outcome         text,                            -- ok / failed / permanent_failure / skipped ...
    reason          text,                            -- skipped 이유 (circuit_open / budget_exhausted)
    attempt         integer,
    will_retry      boolean,
    status_kind     text,
    http_status     integer,
    latency_ms      integer,
    n_records       integer,
    n_rejects       integer,
    delivery_errors integer,
    budget_used     integer,
    budget_limit    integer,
    breaker_state   text,
    slept_sec       double precision,                -- host_resume 전용: 잠들어 있던 시간
    ingest_id       text,
    PRIMARY KEY (kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS call_event_time_ix ON ops.call_event (recorded_at);
CREATE INDEX IF NOT EXISTS call_event_src_time_ix ON ops.call_event (source_system, recorded_at);

-- ------------------------------------------------------------------ 현재 상태 (감시 루프가 매 주기 덮어쓴다)
CREATE TABLE IF NOT EXISTS ops.source_status (
    source_system     text PRIMARY KEY,
    status            text        NOT NULL CHECK (status IN ('ok','degraded','down','idle','unknown')),
    reason            text,
    last_call_at      timestamptz,
    last_ok_at        timestamptz,
    age_sec           double precision,
    expected_gap_sec  double precision,
    calls_1h          integer,
    fail_1h           integer,
    p95_latency_ms    integer,
    budget_used       integer,
    budget_limit      integer,
    checked_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.layer_status (
    layer         text        NOT NULL CHECK (layer IN ('bronze','silver')),
    table_name    text        NOT NULL,
    source_system text,
    status        text        NOT NULL CHECK (status IN ('ok','degraded','down','idle','unknown')),
    reason        text,
    last_write_at timestamptz,
    age_sec       double precision,
    checked_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name)
);

-- 시간 단위 롤업 (call_event 에서 계산). 30일 지난 call_event 를 지워도 추이는 남는다.
CREATE TABLE IF NOT EXISTS ops.source_health (
    source_system  text        NOT NULL,
    bucket_start   timestamptz NOT NULL,
    n_calls        integer     NOT NULL,
    n_ok           integer     NOT NULL,
    n_failed       integer     NOT NULL,
    n_skipped      integer     NOT NULL,
    n_retry        integer     NOT NULL,
    p95_latency_ms integer,
    n_records      bigint,
    computed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_system, bucket_start)
);

-- 수집 공백. host_stall = 수집기가 '잠에서 깼다'고 직접 알린 구간, collector_blackout = 모든 소스가 동시에 조용했던 구간(원인 미상).
-- 수집기가 못 부른 구간은 API 가 실시간 값만 줘서 복구할 수 없다(recoverable=false, D-09).
CREATE TABLE IF NOT EXISTS ops.data_gap (
    gap_id       bigserial PRIMARY KEY,
    kind         text        NOT NULL CHECK (kind IN ('host_stall','collector_blackout')),
    gap_start    timestamptz NOT NULL,
    gap_end      timestamptz NOT NULL,
    duration_sec integer     NOT NULL,
    evidence     text,
    recoverable  boolean     NOT NULL DEFAULT false,
    detected_at  timestamptz NOT NULL DEFAULT now(),
    CHECK (gap_end >= gap_start),
    UNIQUE (kind, gap_start)
);
CREATE INDEX IF NOT EXISTS data_gap_time_ix ON ops.data_gap (gap_start);

-- 감시 루프의 박동과 커서(Kafka offset, 마지막 요약 날짜 ...). 컴포넌트당 1행.
CREATE TABLE IF NOT EXISTS ops.monitor_state (
    component    text PRIMARY KEY,
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    state        jsonb       NOT NULL DEFAULT '{}'::jsonb
);

-- ------------------------------------------------------------------ 알림
-- status: pending(분류 대기) / sent / digest(아침 요약 대기) / digested(요약에 포함됨) / suppressed / failed / dropped
CREATE TABLE IF NOT EXISTS ops.alert_event (
    alert_id       bigserial PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    level          text        NOT NULL CHECK (level IN ('info','warning','error','critical')),
    original_level text,                              -- 격하·승격 전의 수준
    title          text        NOT NULL,
    body           text        NOT NULL DEFAULT '',
    dedup_key      text,
    status         text        NOT NULL CHECK (status IN ('pending','sent','digest','digested','suppressed','failed','dropped')),
    reason         text,                              -- 억제·격하·실패 이유
    sent_at        timestamptz,
    meta           jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS alert_event_time_ix ON ops.alert_event (created_at);
CREATE INDEX IF NOT EXISTS alert_event_status_ix ON ops.alert_event (status, created_at);

CREATE TABLE IF NOT EXISTS ops.alert_state (
    dedup_key        text PRIMARY KEY,
    level            text        NOT NULL,
    title            text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('open','resolved')),
    first_at         timestamptz NOT NULL,
    last_at          timestamptz NOT NULL,
    last_sent_at     timestamptz,
    count            integer     NOT NULL DEFAULT 1,
    suppressed_count integer     NOT NULL DEFAULT 0,
    resolved_at      timestamptz
);

-- ------------------------------------------------------------------ 읽기 뷰 (Step 9 API / Step 10 대시보드가 그대로 읽는다)
-- 상태 값은 ok / degraded / down / idle / unknown 다섯 가지로 고정한다.
CREATE OR REPLACE VIEW ops.v_source_status AS
SELECT source_system, status, reason, last_call_at, last_ok_at, age_sec, expected_gap_sec,
       calls_1h, fail_1h, p95_latency_ms, budget_used, budget_limit, checked_at
FROM ops.source_status;

CREATE OR REPLACE VIEW ops.v_layer_status AS
SELECT layer, table_name, source_system, status, reason, last_write_at, age_sec, checked_at FROM ops.layer_status;

CREATE OR REPLACE VIEW ops.v_load_recent AS
SELECT run_id, job, dag_id, date_from, date_to, started_at, finished_at, status
FROM ops.load_run
ORDER BY run_id DESC
LIMIT 100;

CREATE OR REPLACE VIEW ops.v_alert_recent AS
SELECT alert_id, created_at, level, title, status, dedup_key
FROM ops.alert_event
WHERE created_at >= now() - interval '7 days'
ORDER BY alert_id DESC;

CREATE OR REPLACE VIEW ops.v_data_gap_recent AS
SELECT gap_id, kind, gap_start, gap_end, duration_sec, recoverable
FROM ops.data_gap
WHERE gap_start >= now() - interval '30 days'
ORDER BY gap_start DESC;

-- 마트 신선도. 마트 DAG 가 하루 한 번(임계값은 주 1회) 다시 쓰는 computed_at 으로 본다. incident_impact 는 새 에피소드가 없으면 갱신되지 않아 제외한다.
CREATE OR REPLACE VIEW ops.v_mart_freshness AS
WITH m AS (
    SELECT 'congestion_threshold'::text AS mart, max(computed_at) AS last_computed_at, 192.0 AS max_age_hours FROM mart.congestion_threshold
    UNION ALL SELECT 'bus_arrival_drift',    max(computed_at), 36.0 FROM mart.bus_arrival_drift
    UNION ALL SELECT 'bus_section_time',     max(computed_at), 36.0 FROM mart.bus_section_time
    UNION ALL SELECT 'weather_traffic_hourly', max(computed_at), 36.0 FROM mart.weather_traffic_hourly
)
SELECT mart, last_computed_at,
       extract(epoch FROM (now() - last_computed_at)) AS age_sec,
       max_age_hours,
       CASE WHEN last_computed_at IS NULL THEN 'unknown'
            WHEN now() - last_computed_at <= make_interval(secs => max_age_hours * 3600) THEN 'ok'
            WHEN now() - last_computed_at <= make_interval(secs => max_age_hours * 7200) THEN 'degraded'
            ELSE 'down' END AS status
FROM m;

-- 한 줄에 하나씩 보는 파이프라인 건강. component_kind: source / layer / monitor / load / mart
CREATE OR REPLACE VIEW ops.v_pipeline_health AS
SELECT 'source'::text AS component_kind, source_system AS component, status, reason, last_ok_at, age_sec, checked_at
FROM ops.source_status
UNION ALL
SELECT 'layer', layer || ':' || table_name, status, reason, last_write_at, age_sec, checked_at
FROM ops.layer_status
UNION ALL
SELECT 'monitor', component,
       CASE WHEN now() - heartbeat_at <= interval '3 minutes' THEN 'ok'
            WHEN now() - heartbeat_at <= interval '15 minutes' THEN 'degraded' ELSE 'down' END,
       NULL, heartbeat_at, extract(epoch FROM (now() - heartbeat_at)), heartbeat_at
FROM ops.monitor_state
WHERE component = 'monitor'
UNION ALL
SELECT 'load', 'gold_load',
       CASE WHEN last_ok IS NULL THEN 'unknown'
            WHEN now() - last_ok <= interval '3 hours' THEN 'ok'
            WHEN now() - last_ok <= interval '36 hours' THEN 'degraded' ELSE 'down' END,
       NULL, last_ok, extract(epoch FROM (now() - last_ok)), now()
FROM (SELECT max(finished_at) FILTER (WHERE status = 'ok') AS last_ok FROM ops.load_run WHERE job = 'gold_load') l
UNION ALL
SELECT 'mart', mart, status, NULL, last_computed_at, age_sec, now() FROM ops.v_mart_freshness;

-- ------------------------------------------------------------------ 권한
GRANT SELECT, INSERT, UPDATE, DELETE ON ops.call_event, ops.source_status, ops.layer_status, ops.source_health,
      ops.data_gap, ops.monitor_state, ops.alert_event, ops.alert_state TO gold_writer;
GRANT USAGE, SELECT ON SEQUENCE ops.data_gap_gap_id_seq, ops.alert_event_alert_id_seq TO gold_writer;
-- gold_writer 는 뷰의 원천(mart 표)을 읽어 v_pipeline_health 를 직접 조회할 수 있어야 감시 루프가 같은 뷰로 점검할 수 있다
GRANT SELECT ON ops.v_source_status, ops.v_layer_status, ops.v_load_recent, ops.v_alert_recent, ops.v_data_gap_recent,
      ops.v_mart_freshness, ops.v_pipeline_health TO gold_writer;

GRANT USAGE ON SCHEMA ops TO api_reader, bi_reader;
GRANT SELECT ON ops.v_source_status, ops.v_layer_status, ops.v_load_recent, ops.v_alert_recent, ops.v_data_gap_recent,
      ops.v_mart_freshness, ops.v_pipeline_health TO api_reader, bi_reader;
