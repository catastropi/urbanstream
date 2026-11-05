"""dq_report_daily — 매일 05:00 KST. 전날(KST)의 데이터 품질을 검사해 ops.dq_result 에 기록한다 (규칙: config/dq_rules.yaml).

관찰(shadow) 기간(기본 7일)에는 기록만 하고 알림·마트 차단을 하지 않는다. 끝난 뒤에는 실패한 규칙을 알린다.
모니터가 분류하지 못한 오래된 DAG 실패 알림(pending)을 내보내는 안전망도 겸한다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from batch import dq_jobs, jobs

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 2, "retry_delay": pendulum.duration(minutes=15)}


@dag(
    dag_id="dq_report_daily",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=1),
    tags=["dq", "step8b"],
)
def dq_report_daily():
    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def report(**ctx) -> dict:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        return dq_jobs.run_daily_report(jobs.kst_date(anchor) - timedelta(days=1))

    report()


dq_report_daily()
