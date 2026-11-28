"""silver_reconcile_daily — 매일 04:30 KST. Bronze 행 수 = Silver 행 수 + 읽을 수 없는 행 인지 대조한다 (streaming/silver_verify, D-08).

Spark 가 필요하므로 pool gold_io (한 번에 하나). 결과는 ops.dq_result(rule_id=silver_reconcile)에 남고, 실패하면 error 알림.
STEP 3B: Spark 는 scheduler 가 아니라 gold-runner 에서 돈다(kind=silver_verify, GOLD_STAGE_MODE=inline 이면 예전처럼 여기서).
작업 잠금 silver_reconcile 을 잡고 돈다. 잠금 대기 최대 10분 + 검증 30분 = 40분이라 task 시간 한도는 45분이다.
Silver 가 Bronze 를 따라가는 중이므로 비교 기준은 'Silver 가 처리한 마지막 시각 - 15분' 까지다 (silver_verify 설명 참고).
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import dq_jobs, jobs

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 1, "retry_delay": pendulum.duration(minutes=20)}


@dag(
    dag_id="silver_reconcile_daily",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=1),
    tags=["dq", "step8b"],
)
def silver_reconcile_daily():
    @task(pool="gold_io", on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=45))
    def reconcile() -> dict:
        return dq_jobs.run_silver_reconcile()

    reconcile()


silver_reconcile_daily()
