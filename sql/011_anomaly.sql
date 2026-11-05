-- 011: Step 8c. 속도 이상 탐지 결과. 결정 근거: docs/adr/0010-step8-dq-monitoring.md (D8-11)
-- 방법: 링크 × 요일 × KST 시각 기준선(mart.mv_link_baseline)의 중앙값·사분위 범위(IQR)로 만든 로버스트 z.  z = (속도 - p50) / max(IQR/1.349, 하한)
-- status:  anomaly = 기준선이 충분(n_obs>=20)하고 28일 이상일 때의 이탈 → 알림 후보
--          candidate = 기준선이 잠정(provisional)이거나 표본이 적을 때의 이탈 → 기록만 (알림 없음)
--          degenerate_baseline = IQR 이 0(기준선 칸의 속도가 모두 같음)이라 z 를 계산할 수 없는데 속도가 중앙값과 다름 → 기록만
-- 기준선 칸이 없는 관측은 행을 만들지 않는다(건수만 DAG 결과에 센다).
CREATE TABLE IF NOT EXISTS mart.anomaly_event (
    link_id      text        NOT NULL,
    event_time   timestamptz NOT NULL,
    speed_kmh    double precision NOT NULL,
    baseline_p50 double precision NOT NULL,
    baseline_iqr double precision NOT NULL,
    robust_z     double precision,                 -- degenerate_baseline 이면 NULL
    direction    text        CHECK (direction IN ('slow','fast')),
    status       text        NOT NULL CHECK (status IN ('anomaly','candidate','degenerate_baseline')),
    n_obs        integer     NOT NULL,
    provisional  boolean     NOT NULL,
    detected_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (link_id, event_time)
);
CREATE INDEX IF NOT EXISTS anomaly_event_time_ix ON mart.anomaly_event (event_time DESC);
CREATE INDEX IF NOT EXISTS anomaly_event_status_ix ON mart.anomaly_event (status, event_time DESC);

-- 최근 7일 (대시보드·API 용)
CREATE OR REPLACE VIEW mart.v_anomaly_recent AS
SELECT link_id, event_time, speed_kmh, baseline_p50, baseline_iqr, robust_z, direction, status, n_obs, provisional, detected_at
FROM mart.anomaly_event
WHERE event_time >= now() - interval '7 days';

GRANT SELECT, INSERT, UPDATE, DELETE ON mart.anomaly_event TO gold_writer;
GRANT SELECT ON mart.anomaly_event, mart.v_anomaly_recent TO api_reader, bi_reader;
