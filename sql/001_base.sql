-- 001: 확장, 스키마, 적재 이력 표
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

-- 적재 1회의 기록 (Step 7 Airflow 가 run 단위로 재실행 여부를 판단할 때도 쓴다)
CREATE TABLE IF NOT EXISTS ops.load_run (
    run_id      bigserial PRIMARY KEY,
    job         text        NOT NULL,
    date_from   date        NOT NULL,
    date_to     date        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status      text        NOT NULL DEFAULT 'running' CHECK (status IN ('running','ok','failed')),
    row_counts  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    error       text
);
COMMENT ON TABLE ops.load_run IS 'Gold 적재 실행 이력. 같은 구간을 다시 돌려도 fact 는 PK 로 멱등.';
