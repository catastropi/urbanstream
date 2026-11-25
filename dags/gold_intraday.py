"""gold_intraday — 매시 20분, 지난 실행 뒤 새로 생긴 Silver 파일만 Gold 로 적재한다 (증분, D-278).

예전에는 '오늘(KST)' 하루를 매번 다시 읽어 밤이 되면 30분을 넘었다. 이제 진행 기록(data/gold_stage/_state/gold_facts.json)
이후의 파일만 읽고, 실행이 몇 번 빠지면(Mac 절전) 다음 실행이 3시간씩 나눠 따라잡는다. 볼 날짜 폴더는 어제·오늘이다.
팩트는 DO NOTHING, 날씨·감사는 DO UPDATE, 수명은 합치기라 겹쳐 읽어도 결과가 같다(멱등).
무거운 import(pyspark, psycopg)는 batch.jobs 안에서 지연 import 된다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import jobs

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 2, "retry_delay": pendulum.duration(minutes=5)}


@dag(
    dag_id="gold_intraday",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(minutes=50),
    tags=["gold", "step7a"],
)
def gold_intraday():
    @task(pool="gold_io", on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=45))
    def run(**ctx) -> dict:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"),
                                  getattr(ctx.get("dag_run"), "run_after", None))
        d0, d1 = jobs.window_intraday(anchor)
        ti = ctx["ti"]
        return jobs.process_incremental(d0, d1, dag_id=ti.dag_id, run_id=ti.run_id, try_number=ti.try_number, refresh_mart=False)

    run()


gold_intraday()
