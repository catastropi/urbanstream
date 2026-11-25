"""gold_daily — 매일 01:00 KST. 남은 새 파일을 마저 적재하고(증분, D-278) 마트를 갱신한다.

예전에는 어제~오늘 이틀을 통째로 다시 읽었다(11-23 01:00: 계산만 9분, 잠자기까지 겹쳐 취소). 이제 gold_intraday 와 같은 진행
기록을 이어 쓰므로, 자정 전후 늦게 도착한 어제 날짜 파일도 새 파일로 잡힌다.
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
        return jobs.process_incremental(d0, d1, dag_id=ti.dag_id, run_id=ti.run_id, try_number=ti.try_number, refresh_mart=True)

    @task(on_failure_callback=jobs.on_failure)
    def housekeeping(_result: dict) -> dict:
        return {"pruned_stages": jobs.prune_old_stages(jobs.stage_base()),
                "stale_runs_marked": jobs.mark_stale_runs(older_than_minutes=120)}

    housekeeping(run())


gold_daily()
