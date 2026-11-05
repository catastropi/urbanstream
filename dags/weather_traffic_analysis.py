"""weather_traffic_analysis — 매일 03:30 KST. 링크 × KST 시각의 속도 비율(기준선 대비)에 같은 시각 날씨를 붙인다 (문제 D).

어제~오늘 창을 지우고 다시 넣는다(멱등). 연관이지 인과가 아니다. 강수가 거의 없는 기간에는 등급별 표본이 모자라 insufficient 가 정상이다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import dq_jobs, jobs, marts

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 3, "retry_delay": pendulum.duration(minutes=20)}


def _window(ctx) -> tuple:
    anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
    return jobs.window_daily(anchor)


@dag(
    dag_id="weather_traffic_analysis",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["mart", "weather", "step7c"],
)
def weather_traffic_analysis():
    @task(on_failure_callback=jobs.on_failure)
    def check_upstream(**ctx) -> int:
        d0, d1 = _window(ctx)
        run_id = marts.require_recent_load(d0, d1)
        dq_jobs.require_dq_ok("weather_traffic_analysis")          # 관찰 기간이 끝난 block_mart 규칙(시각 이상)이 fail 이면 여기서 멈춘다 — 지금은 사실상 통과
        return run_id

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def build(_upstream: int, **ctx) -> dict:
        d0, d1 = _window(ctx)
        return marts.build_weather_traffic(d0, d1)

    build(check_upstream())


weather_traffic_analysis()
