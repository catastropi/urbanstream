"""warehouse_maintenance — 매주 일요일 04:00 KST. 통계 갱신(ANALYZE) → 표 크기 기록 → 오래된 운영 기록 삭제(180일).

DB 만 다룬다. Bronze/Silver 파일 정리는 하지 않는다 (Airflow 는 그 폴더를 읽기 전용으로만 본다, D-21/D-24).
세 단계는 순서가 있다: ANALYZE 뒤에 크기를 기록해야 행 수 추정이 새 값이다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from batch import jobs, maintenance

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 2, "retry_delay": pendulum.duration(minutes=30)}


@dag(
    dag_id="warehouse_maintenance",
    schedule="0 4 * * 0",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    dagrun_timeout=pendulum.duration(hours=1),
    tags=["maintenance", "step7c"],
)
def warehouse_maintenance():
    @task(on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=20))
    def analyze() -> int:
        return maintenance.analyze_warehouse()

    @task(on_failure_callback=jobs.on_failure)
    def snapshot(_n: int) -> int:
        return maintenance.snapshot_table_stats()           # 실행 시점의 KST 날짜 (밀린 실행의 논리 날짜가 아님)

    @task(on_failure_callback=jobs.on_failure)
    def prune(_n: int) -> dict:
        return maintenance.prune_ops()

    prune(snapshot(analyze()))


warehouse_maintenance()
