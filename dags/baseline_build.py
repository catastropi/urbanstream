"""baseline_build — 매주 일요일 02:30 KST. 혼잡 단계 임계값(링크 × 요일유형 × KST 시각의 속도 분위수)을 새 판으로 만든다.

기준선 materialized view(mart.mv_link_baseline)의 갱신은 gold_daily 가 이미 한다 — 여기서 중복하지 않는다.
임계값은 분포에서 나온다(임의 숫자 아님). 표본이 모자란 칸은 분위수를 비우고, 28일 미만이면 provisional 이다.
같은 날 다시 돌리면 그 날의 판을 교체한다(멱등).
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import jobs, marts

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 3, "retry_delay": pendulum.duration(minutes=20)}


@dag(
    dag_id="baseline_build",
    schedule="30 2 * * 0",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["mart", "baseline", "step7b"],
)
def baseline_build():
    @task(on_failure_callback=jobs.on_failure)
    def check_upstream(**ctx) -> int:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        today = jobs.kst_date(anchor)
        # 일요일 02:30 에는 01:00 gold_daily(어제~오늘)가 끝나 있어야 한다. 오늘을 덮는 성공한 적재가 최근 72시간 안에 있으면 통과.
        return marts.require_recent_load(today, today, max_age_hours=72)

    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=30))
    def thresholds(_upstream: int, **ctx) -> dict:
        anchor = jobs.pick_anchor(ctx.get("data_interval_end"), ctx.get("logical_date"), getattr(ctx.get("dag_run"), "run_after", None))
        return marts.build_congestion_threshold(jobs.kst_date(anchor))

    thresholds(check_upstream())


baseline_build()
