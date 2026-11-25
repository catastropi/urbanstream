"""reprocess — 수동 실행. 지난 날짜의 Silver 를 Bronze 에서 다시 만들고(날짜별 포인터로 교체), Gold fact 를 그 날짜 범위만 지우고 다시 넣는다.

결정 근거: docs/adr/0015-reprocess.md, 사용법: docs/runbooks/reprocess.md
- 하루 단위 mapped 태스크, 풀 gold_io(슬롯 1)라 다른 Spark 작업과 겹치지 않는다. Spark 는 gold-runner 에서만 돈다.
- 날짜는 KST 그제 이전만 받는다(오늘·어제는 스트리밍이 아직 행을 붙인다). 최대 31일.
- 평소의 gold_intraday·gold_daily 는 그대로(팩트 DO NOTHING)다. 범위 교체는 이 DAG 에서만 한다.
- 실행 이력은 ops.reprocess_run (ops.v_reprocess_recent). 되돌리기는 런북 참고.
- one_day 는 저절로 재시도하지 않는다(retries=0). 빌드가 한도에 걸려 실패했을 때 같은 무거운 계산을 곧바로 한 번 더 하지 않게 한다.
  원인을 본 뒤 그 태스크를 Clear 하면 이어서 한다(게시까지 끝난 단계는 다시 하지 않는다).
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

from batch import jobs, reprocess

DEFAULT_ARGS = {"owner": "urbanstream", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}


@dag(
    dag_id="reprocess",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=jobs.on_failure,
    params={
        "date_from": Param("2026-11-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$", description="시작 날짜 (KST, YYYY-MM-DD)"),
        "date_to": Param("2026-11-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$", description="끝 날짜 (KST, 포함). 최대 31일, 그제까지"),
        "tables": Param("all", type="string", description="all 또는 쉼표로 구분한 Silver 표 (예: silver_traffic_speed,silver_weather)"),
        "reason": Param("", type="string", minLength=1, description="왜 다시 만드는지 (ops.reprocess_run.reason 에 남는다)"),
        "scope": Param("silver_gold", type="string", enum=list(reprocess.SCOPES),
                       description="silver: Silver 만 / silver_gold: Silver 와 Gold / gold: 지금 Silver 로 Gold 범위만 교체"),
        "allow_shrink": Param(False, type="boolean", description="재처리 결과 키가 현재 Silver 보다 적어도 게시 (규칙이 행을 빼는 것이 의도일 때)"),
        "bronze_read_mode": Param("listing", type="string", enum=list(reprocess.READ_MODES),
                                  description="listing: 파티션 폴더 직접 나열(sink 로그 밖 파일도 읽음, 기본) / sink_log: 스트리밍과 같은 시야"),
    },
    tags=["silver", "gold", "manual", "step1"],
)
def reprocess_dag():
    @task(on_failure_callback=jobs.on_failure)
    def plan(**ctx) -> list[str]:
        from streaming.silver_pointer import check_dates
        p = ctx["params"]
        reprocess.check_args(p["scope"], p["reason"], p["bronze_read_mode"])
        reprocess.parse_tables(p["tables"])
        days = check_dates(jobs.parse_day(p["date_from"]), jobs.parse_day(p["date_to"]))
        with jobs._default_connect() as conn:
            stale = reprocess.mark_stale(conn)
        if stale["needs_attention"]:
            raise jobs.JobError(f"게시 뒤 멈춘 재처리가 있습니다 {stale['needs_attention']} — 런북의 '중간에 멈춘 재처리' 절차로 먼저 정리하세요")
        return [d.isoformat() for d in days]

    @task(pool="gold_io", retries=0, on_failure_callback=jobs.on_failure, execution_timeout=pendulum.duration(minutes=120),
          map_index_template="{{ day }}")
    def one_day(day: str, **ctx) -> dict:
        from airflow.sdk import get_current_context
        get_current_context()["day"] = day
        p, ti = ctx["params"], ctx["ti"]
        return reprocess.process_reprocess_day(
            jobs.parse_day(day), tables=reprocess.parse_tables(p["tables"]), reason=p["reason"], scope=p["scope"],
            allow_shrink=bool(p["allow_shrink"]), bronze_read_mode=p["bronze_read_mode"],
            dag_id=ti.dag_id, run_id=ti.run_id, try_number=ti.try_number)

    @task(trigger_rule="all_done_min_one_success", on_failure_callback=jobs.on_failure)
    def refresh(_results, **ctx) -> str:
        if ctx["params"]["scope"] == "silver":
            return "scope=silver: Gold 를 건드리지 않았으므로 마트 갱신 없음"
        jobs.refresh_marts()
        return "mart.refresh_all done"

    refresh(one_day.expand(day=plan()))


reprocess_dag()
