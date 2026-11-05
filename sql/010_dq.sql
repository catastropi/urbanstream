-- 010: Step 8b. 데이터 품질(DQ) 결과 표. 결정 근거: docs/adr/0010-step8-dq-monitoring.md (D8-9, D8-10)
-- 새 표·뷰는 이 파일 안에서 GRANT 한다. api_reader / bi_reader 는 뷰만 읽는다(원본 표 금지, 009 와 같은 규칙).

-- 규칙 1개 × 대상 1개 × 날짜 1일 = 1행. 같은 날 다시 돌리면 교체한다(멱등).
--   status : pass / warn(설명 가능한 이탈: 예 수집 공백으로 설명됨) / fail / unknown(판단할 자료 부족 — 빈 표를 pass 로 치지 않는다) / skipped
--   shadow : true 면 7일 관찰 기간 — 기록만 하고 알림·마트 차단에 쓰지 않는다
CREATE TABLE IF NOT EXISTS ops.dq_result (
    result_id  bigserial PRIMARY KEY,
    run_date   date        NOT NULL,                 -- 검사한 KST 날짜
    checked_at timestamptz NOT NULL DEFAULT now(),
    rule_id    text        NOT NULL,
    kind       text        NOT NULL,
    target     text        NOT NULL DEFAULT '',      -- 예: 소스 이름·표 이름. 대상이 하나면 빈 문자열
    severity   text        NOT NULL CHECK (severity IN ('info','warning','error')),
    action     text        NOT NULL CHECK (action IN ('record','alert','block_mart')),
    status     text        NOT NULL CHECK (status IN ('pass','warn','fail','unknown','skipped')),
    shadow     boolean     NOT NULL,
    value      double precision,
    n_checked  bigint,
    threshold  text,
    detail     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_date, rule_id, target)
);
CREATE INDEX IF NOT EXISTS dq_result_rule_ix ON ops.dq_result (rule_id, run_date DESC);
CREATE INDEX IF NOT EXISTS dq_result_gate_ix ON ops.dq_result (action, status) WHERE action = 'block_mart';

-- 규칙·대상별 가장 최근 결과
CREATE OR REPLACE VIEW ops.v_dq_latest AS
SELECT DISTINCT ON (rule_id, target)
       rule_id, kind, target, severity, action, status, shadow, run_date, checked_at, value, n_checked, threshold, detail
FROM ops.dq_result
ORDER BY rule_id, target, run_date DESC, checked_at DESC;

-- 날짜별 요약 (API 의 "오늘 데이터 품질" 카드, 대시보드용)
CREATE OR REPLACE VIEW ops.v_dq_summary AS
SELECT run_date,
       count(*)                                  AS n_checks,
       count(*) FILTER (WHERE status = 'pass')   AS n_pass,
       count(*) FILTER (WHERE status = 'warn')   AS n_warn,
       count(*) FILTER (WHERE status = 'fail')   AS n_fail,
       count(*) FILTER (WHERE status = 'unknown') AS n_unknown,
       count(*) FILTER (WHERE status = 'fail' AND NOT shadow AND severity = 'error') AS n_fail_error,
       max(checked_at)                           AS checked_at
FROM ops.dq_result
GROUP BY run_date;

GRANT SELECT, INSERT, UPDATE, DELETE ON ops.dq_result TO gold_writer;
GRANT USAGE, SELECT ON SEQUENCE ops.dq_result_result_id_seq TO gold_writer;
GRANT SELECT ON ops.v_dq_latest, ops.v_dq_summary TO gold_writer, api_reader, bi_reader;
