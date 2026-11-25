-- 015: STEP 1. 재처리 실행 이력. Bronze→Silver 재생성과 Gold 범위 교체를 한 행으로 추적한다. 결정 근거: docs/adr/0015-reprocess.md
-- 001~014 는 수정하지 않는다. 되돌리기: DROP VIEW ops.v_reprocess_recent; DROP TABLE ops.reprocess_run;
-- 새 표·뷰는 이 파일 안에서 GRANT 한다 (009 와 같은 관례. FK 는 두지 않는다).

CREATE TABLE IF NOT EXISTS ops.reprocess_run (
    reprocess_id      bigserial   PRIMARY KEY,
    request_key       text        NOT NULL UNIQUE,      -- '<dag_id>__<airflow run_id>__<날짜>'. Airflow 재시도는 같은 행을 이어 쓴다
    scope             text        NOT NULL CHECK (scope IN ('silver', 'silver_gold', 'gold')),
    tables            text[]      NOT NULL,             -- Silver 표 이름
    date_from         date        NOT NULL,             -- KST, 양 끝 포함
    date_to           date        NOT NULL,
    reason            text        NOT NULL CHECK (length(btrim(reason)) > 0),   -- 왜 다시 만드는지
    status            text        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'silver_built', 'silver_published', 'gold_loaded', 'ok', 'failed', 'rolled_back')),
    bronze_read_mode  text        NOT NULL DEFAULT 'listing' CHECK (bronze_read_mode IN ('listing', 'sink_log')),
    allow_shrink      boolean     NOT NULL DEFAULT false,
    code_version      text,                             -- git commit (짧은 해시, 알 수 있을 때만)
    config_sha256     text,                             -- config/silver.yaml 해시 앞 16자
    silver_version    text,                             -- data/silver_rebuild/<표>/<silver_version>/
    previous_version  jsonb,                            -- 게시 전 포인터 {표: {날짜: 버전 | null}}. 되돌리기에 쓴다
    bronze_max_ingested_at timestamptz,                 -- 재처리가 읽은 Bronze 의 마지막 적재 시각 (늦게 온 행 감지용)
    load_run_id       bigint,                           -- ops.load_run.run_id
    row_counts        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    dag_id            text,
    airflow_run_id    text,
    try_number        integer,
    started_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    error             text,
    CHECK (date_from <= date_to),
    CHECK (cardinality(tables) > 0),
    -- 끝나지 않은 재처리끼리 날짜가 겹치지 못하게 한다 (daterange 의 && 는 btree_gist 없이 GiST 로 된다)
    CONSTRAINT reprocess_run_no_overlap EXCLUDE USING gist (daterange(date_from, date_to, '[]') WITH &&)
        WHERE (status IN ('running', 'silver_built', 'silver_published', 'gold_loaded'))
);
COMMENT ON TABLE ops.reprocess_run IS 'Bronze→Silver 재생성과 Gold 범위 교체 실행 이력. 한 행 = 재처리 요청 하나(하루).';
COMMENT ON COLUMN ops.reprocess_run.status IS 'running → silver_built → silver_published → gold_loaded → ok. 게시 전 실패는 failed, 되돌리면 rolled_back.';

CREATE INDEX IF NOT EXISTS reprocess_run_started_idx ON ops.reprocess_run (started_at DESC);

CREATE OR REPLACE VIEW ops.v_reprocess_recent AS
SELECT reprocess_id, scope, tables, date_from, date_to, reason, status, silver_version, load_run_id,
       row_counts, started_at, finished_at,
       EXTRACT(EPOCH FROM (coalesce(finished_at, now()) - started_at))::int AS elapsed_sec
FROM ops.reprocess_run
ORDER BY reprocess_id DESC
LIMIT 100;

GRANT SELECT, INSERT, UPDATE ON ops.reprocess_run TO gold_writer;
GRANT USAGE, SELECT ON SEQUENCE ops.reprocess_run_reprocess_id_seq TO gold_writer;
GRANT SELECT ON ops.v_reprocess_recent TO gold_writer, api_reader, bi_reader;
