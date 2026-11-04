"""dags/ 가 Airflow 에서 파싱되고 운영 정책을 지키는지 본다. Airflow 가 없는 환경에서는 건너뛴다."""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "dags"
EXPECTED = {"gold_intraday", "gold_daily", "backfill", "bus_delay", "baseline_build",
            "incident_impact_finalize", "weather_traffic_analysis", "warehouse_maintenance"}


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_expected_dags_exist(bag):
    assert EXPECTED <= set(bag.dags)


def test_policies(bag):
    for dag_id in EXPECTED:
        d = bag.dags[dag_id]
        assert d.catchup is False, dag_id                    # Mac 절전 뒤 밀린 실행이 한꺼번에 몰리지 않게
        assert d.max_active_runs == 1, dag_id                # 같은 윈도우를 동시에 쓰지 않게
        assert d.on_failure_callback is not None, dag_id
        assert str(d.timezone) in ("Asia/Seoul", "Timezone('Asia/Seoul')"), dag_id


def test_heavy_tasks_use_the_single_slot_pool(bag):
    for dag_id, task_id in (("gold_intraday", "run"), ("gold_daily", "run"), ("backfill", "one_day")):
        assert bag.dags[dag_id].get_task(task_id).pool == "gold_io", (dag_id, task_id)


def test_schedules(bag):
    assert bag.dags["gold_intraday"].schedule == "20 * * * *"
    assert bag.dags["gold_daily"].schedule == "0 1 * * *"
    assert bag.dags["backfill"].schedule is None
    assert bag.dags["bus_delay"].schedule == "0 2 * * *"         # gold_daily(01:00) 다음
    assert bag.dags["baseline_build"].schedule == "30 2 * * 0"   # 일요일
    assert bag.dags["incident_impact_finalize"].schedule == "0 3 * * *"
    assert bag.dags["weather_traffic_analysis"].schedule == "30 3 * * *"
    assert bag.dags["warehouse_maintenance"].schedule == "0 4 * * 0"


def test_backfill_params_have_date_pattern(bag):
    params = bag.dags["backfill"].params
    assert {"date_from", "date_to"} <= set(params)


def test_start_date_is_not_in_the_future_of_first_manual_run(bag):
    # start_date 가 첫 수동 실행 날짜보다 늦으면 Airflow 가 태스크를 만들지 않고 실행이 '성공'으로 끝난다 (11-04 샌드박스에서 재현).
    import pendulum
    for dag_id in EXPECTED:
        start = bag.dags[dag_id].start_date
        assert start <= pendulum.datetime(2026, 11, 4, tz="Asia/Seoul"), (dag_id, start)


def test_7b_dags_check_upstream_before_building(bag):
    for dag_id, builders in (("bus_delay", {"arrival_drift", "section_time"}), ("baseline_build", {"thresholds"})):
        d = bag.dags[dag_id]
        assert {t.task_id for t in d.tasks} == {"check_upstream"} | builders
        for b in builders:
            assert "check_upstream" in d.get_task(b).upstream_task_ids, (dag_id, b)


def test_7c_dags_wiring_and_no_spark_pool(bag):
    for dag_id, builders in (("incident_impact_finalize", {"finalize"}), ("weather_traffic_analysis", {"build"})):
        d = bag.dags[dag_id]
        assert {t.task_id for t in d.tasks} == {"check_upstream"} | builders
        for b in builders:
            assert "check_upstream" in d.get_task(b).upstream_task_ids, (dag_id, b)
        assert all(t.pool != "gold_io" for t in d.tasks), dag_id      # SQL 만 쓰므로 Spark 풀을 잡지 않는다
    m = bag.dags["warehouse_maintenance"]
    assert m.get_task("snapshot").upstream_task_ids == {"analyze"} and m.get_task("prune").upstream_task_ids == {"snapshot"}
