"""bus_delay — 매일 02:00 KST (gold_daily 01:00 다음). 버스 도착 예측 드리프트와 구간 통과 시간을 어제~오늘 창으로 다시 만든다.

선행 조건: 그 창을 덮는 성공한 적재가 최근 36시간 안에 있어야 한다(없으면 실패 → 재시도, 덜 쌓인 데이터로 만들지 않는다).
마트 표는 파생물이라 창을 지우고 다시 넣는다(멱등). 시간표 대비 지연이 아니다 — 정의는 sql/006_mart_7b.sql 주석과 ADR-0009 §5.
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
    dag_id="bus_delay",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=3),
    tags=["mart", "bus", "step7b"],
)
def bus_delay():
    @task(on_failure_callback=jobs.on_failure)
    def check_upstream(**ctx) -> int:
        d0, d1 = _window(ctx)
        run_id = marts.require_recent_load(d0, d1)
        dq_jobs.require_dq_ok("bus_delay")          # 관찰 기간이 끝난 block_mart 규칙(시각 이상)이 fail 이면 여기서 멈춘다 — 지금은 사실상 통과
        return run_id

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def arrival_drift(_upstream: int, **ctx) -> dict:
        d0, d1 = _window(ctx)
        return marts.build_bus_drift(d0, d1)

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def section_time(_upstream: int, **ctx) -> dict:
        d0, d1 = _window(ctx)
        return marts.build_bus_section_time(d0, d1)

    up = check_upstream()
    arrival_drift(up)
    section_time(up)


bus_delay()
