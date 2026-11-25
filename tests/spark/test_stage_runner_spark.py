"""gold-runner 의 실제 경로: Airflow 쪽 request_stage → 실행기 → 실제 spark-submit(gold_stage.py) → manifest.

가짜 runner 를 쓰는 단위 시험(tests/unit/test_stage_runner.py)이 확인하지 못하는 두 가지를 본다.
  - 실행기가 만드는 spark-submit 명령이 실제로 돈다 (gold_stage.py 를 스크립트로 실행해도 batch.* 를 import 한다).
  - 만든 Parquet 폴더가 다른 사용자도 지울 수 있는 권한(777)이다. 운영에서는 spark 사용자(185)가 만들고 Airflow(50000)가 지운다.
spark-submit 은 pyspark 패키지에 들어 있는 것을 쓴다 (컨테이너에서는 /opt/spark/bin/spark-submit).
"""
from __future__ import annotations

import os
import shutil
import stat
import sys
import threading
import time
from pathlib import Path

import pytest

from batch import jobs as J
from batch import stage_runner as R
from test_gold_e2e_pg import D, _audit, _write_silver

ROOT = Path(__file__).resolve().parents[2]


def _spark_submit() -> str:
    import pyspark
    for p in (Path(pyspark.__file__).parent / "bin" / "spark-submit", Path("/opt/spark/bin/spark-submit")):
        if p.exists():
            return str(p)
    found = shutil.which("spark-submit")
    if not found:
        pytest.skip("spark-submit 없음")
    return found


def test_request_runs_real_spark_and_leaves_folders_deletable_by_others(spark, tmp_path):
    silver, bronze, base = tmp_path / "silver", tmp_path / "bronze", tmp_path / "stage"
    _write_silver(spark, silver)
    spark.createDataFrame([_audit("c0", 0), _audit("t1", 5, "seoul_traffic")], "_raw_value string").write.parquet(str(bronze / "audit"))
    env = dict(os.environ, GOLD_SILVER_ROOT=str(silver), GOLD_BRONZE_ROOT=str(bronze), SPARK_SUBMIT=_spark_submit(),
               PYTHONPATH=os.pathsep.join([str(ROOT), str(ROOT / "src")]), GOLD_SPARK_MASTER="local[1]")
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    req = R.requests_dir(base)
    req.mkdir(parents=True)
    R.touch_alive(req)
    stop, seen = threading.Event(), []

    def loop():
        while not stop.is_set():
            seen.extend(R.process_pending(base, env=env, timeout_sec=600))
            time.sleep(0.2)
    t = threading.Thread(target=loop, daemon=True)
    old = os.umask(0)                  # 실행기 main() 과 같은 조건
    t.start()
    try:
        sd = base / "gold_intraday__r1__try1"
        m = J.request_stage(D, D, sd, base=base, poll_sec=0.5, timeout_sec=600)
    finally:
        stop.set()
        t.join(30)
        os.umask(old)
    assert [k for _, k in seen] == ["done"]
    assert m["row_counts"]["traffic_speed"] == 2 and m["row_counts"]["call_audit"] == 2
    modes = {stat.S_IMODE(p.stat().st_mode) for p in sd.rglob("*") if p.is_dir()}
    assert modes == {0o777}, f"Hadoop 기본 umask(022)가 적용되면 Airflow 가 폴더를 지우지 못한다: {sorted(map(oct, modes))}"
