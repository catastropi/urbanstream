"""anomaly_scan — 매시 35분 KST. 최근 3시간의 도로 속도를 요일×시각 기준선과 비교해 이상을 mart.anomaly_event 에 기록한다 (로버스트 z).

gold_intraday 가 매시 20분에 적재한 뒤에 돈다. 그래서 이상은 발생 후 최대 약 1시간 20분 뒤에 보인다 (실시간이 아님, D-124).
기준선이 28일 미만이면 이탈은 candidate 로만 기록하고 알리지 않는다. 같은 구간을 다시 돌려도 안전하다(PK).
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import anomaly, jobs, marts

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 1, "retry_delay": pendulum.duration(minutes=10)}


@dag(
    dag_id="anomaly_scan",
    schedule="35 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(minutes=30),
    tags=["anomaly", "step8c"],
)
def anomaly_scan():
    @task(on_failure_callback=jobs.on_failure)
    def check_upstream(**ctx) -> int:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        today = jobs.kst_date(anchor)
        # 최근 3시간 안에 오늘을 덮는 성공한 적재가 있어야 한다 (없으면 낡은 데이터로 '이상 없음'을 말하게 된다)
        return marts.require_recent_load(today, today, max_age_hours=3)

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=10))
    def scan(_upstream: int, **ctx) -> dict:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        return anomaly.scan(until=anchor)

    scan(check_upstream())


anomaly_scan()
