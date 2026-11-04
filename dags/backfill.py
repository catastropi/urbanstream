"""backfill — 수동 실행. 날짜 범위(최대 31일)를 하루씩 Silver→Gold 로 다시 처리한다.

하루 단위 mapped 태스크이고 풀(gold_io, 슬롯 1)이라 동시에 하나씩만 돈다.
한 날이 실패해도 그 날 스테이징 폴더가 남고, 재시도하면 그 날부터 다시 한다. 마트는 마지막에 한 번만 갱신한다.
이 DAG 는 Silver 에 이미 있는 데이터를 다시 쌓는다. Bronze 에서 Silver 를 다시 만드는 backfill_from_raw 는 ADR-0009 에서 보류.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

from batch import jobs

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}


@dag(
    dag_id="backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    params={
        "date_from": Param("2026-11-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$", description="시작 날짜 (KST, YYYY-MM-DD)"),
        "date_to": Param("2026-11-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$", description="끝 날짜 (KST, 포함). 최대 31일"),
    },
    tags=["gold", "manual", "step7a"],
)
def backfill():
    @task
    def plan(**ctx) -> list[str]:
        p = ctx["params"]
        days = jobs.days_between(jobs.parse_day(p["date_from"]), jobs.parse_day(p["date_to"]))
        return [d.isoformat() for d in days]

    @task(pool="gold_io", on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=60), map_index_template="{{ day }}")
    def one_day(day: str, **ctx) -> dict:
        from airflow.sdk import get_current_context
        get_current_context()["day"] = day
        d = jobs.parse_day(day)
        ti = ctx["ti"]
        return jobs.process_window(d, d, dag_id=f"{ti.dag_id}_{day}", run_id=ti.run_id, try_number=ti.try_number, refresh_mart=False)

    @task(trigger_rule="all_done_min_one_success", on_failure_callback=jobs.on_failure)
    def refresh(_results) -> str:
        jobs.refresh_marts()
        return "mart.refresh_all done"

    refresh(one_day.expand(day=plan()))


backfill()
