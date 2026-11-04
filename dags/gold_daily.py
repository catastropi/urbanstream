"""gold_daily — 매일 01:00 KST. 어제~오늘 윈도우를 다시 처리하고 마트를 갱신한다.

자정 전후 늦게 도착한 데이터를 어제 날짜에 반영하려고 윈도우를 이틀로 잡는다.
적재 뒤에는 오래된 스테이징 폴더를 지우고, 비정상 종료로 'running' 에 남은 load_run 을 failed 로 돌린다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import jobs

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 2, "retry_delay": pendulum.duration(minutes=10)}


@dag(
    dag_id="gold_daily",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["gold", "mart", "step7a"],
)
def gold_daily():
    @task(pool="gold_io", on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=90))
    def run(**ctx) -> dict:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"),
                                  getattr(ctx.get("dag_run"), "run_after", None))
        d0, d1 = jobs.window_daily(anchor)
        ti = ctx["ti"]
        return jobs.process_window(d0, d1, dag_id=ti.dag_id, run_id=ti.run_id, try_number=ti.try_number, refresh_mart=True)

    @task(on_failure_callback=jobs.on_failure)
    def housekeeping(_result: dict) -> dict:
        return {"pruned_stages": jobs.prune_old_stages(jobs.stage_base()),
                "stale_runs_marked": jobs.mark_stale_runs(older_than_minutes=120)}

    housekeeping(run())


gold_daily()
