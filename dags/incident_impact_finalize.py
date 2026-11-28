"""incident_impact_finalize — 매일 03:00 KST (bus_delay 02:00 다음). 해제된 지 24시간 지난 돌발 에피소드의 영향(문제 B)을 한 번 확정한다.

확정된 에피소드는 다시 계산하지 않는다 → mart.incident_impact 가 있으면 에피소드 원본을 지울 수 있다 (D-95).
선행: 어제~오늘을 덮는 성공한 적재가 최근 36시간 안에 있어야 한다. 기준선(mv_link_baseline)은 gold_daily 가 갱신한다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import dq_jobs, jobs, marts

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 3, "retry_delay": pendulum.duration(minutes=20)}


@dag(
    dag_id="incident_impact_finalize",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["mart", "incident", "step7c"],
)
def incident_impact_finalize():
    @task(on_failure_callback=jobs.on_failure)
    def check_upstream(**ctx) -> int:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        d0, d1 = jobs.window_daily(anchor)
        run_id = marts.require_recent_load(d0, d1)
        dq_jobs.require_dq_ok("incident_impact_finalize")          # block_mart 규칙이 fail 이면 재시도 없이 바로 실패한다(DQ 결과는 05:00 에만 바뀐다). 적재 확인은 재시도한다
        return run_id

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def finalize(_upstream: int) -> dict:
        return marts.build_incident_impact()

    finalize(check_upstream())


incident_impact_finalize()
