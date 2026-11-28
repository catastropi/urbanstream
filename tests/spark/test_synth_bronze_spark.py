"""STEP 2 합성 Bronze 를 Spark 로 읽는다 (설계안 3-2): 스키마가 streaming.bronze.to_bronze_typed 와 같고, Silver 변환을 거쳐 손상 행이 0 이며,
gold-runner 의 측정 작업(kind=measure: silver_build → silver_point → gold_stage)이 합성 폴더 안에서만 끝까지 돈다."""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import synth_bronze as SB  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

from batch import stage_runner as R  # noqa: E402
from streaming.silver import bronze_schema, transform  # noqa: E402
from streaming.silver_config import SilverConfig, load_silver_tables  # noqa: E402

PROFILE = json.loads((ROOT / "tests/fixtures/measure/profile_small.json").read_text(encoding="utf-8"))
D = date(2026, 1, 5)


@pytest.fixture(scope="module")
def synth(tmp_path_factory):
    out = tmp_path_factory.mktemp("measure") / "synthetic" / "s1"
    m = SB.generate(PROFILE, scale=2, d=D, seed=3, run_id="s1", out_dir=out)
    return out, m


def _strip(t):
    """nullable 을 빼고 이름·타입만 비교한다 (Spark 는 파일을 읽을 때 모든 열을 nullable 로 본다)."""
    from pyspark.sql import types as T
    if isinstance(t, T.StructType):
        return [(f.name, _strip(f.dataType)) for f in t.fields]
    return t.simpleString()


def test_synthetic_files_have_exactly_the_bronze_schema(spark, synth):
    out, _ = synth
    for src in SB.SOURCES:
        df = spark.read.parquet(str(out / "bronze" / src))
        want = _strip(bronze_schema(spark, src))
        got = _strip(df.schema)
        assert got == want, src


def test_silver_transform_of_synthetic_bronze_drops_nothing(spark, synth):
    out, m = synth
    cfg = SilverConfig.from_env({})
    for t in load_silver_tables(cfg.config_dir):
        b = spark.read.schema(bronze_schema(spark, t.source)).parquet(str(out / "bronze" / t.source))
        assert b.filter(~F.col("_parse_ok")).count() == 0, t.source
        s = transform(b, t, cfg)
        assert s.count() == m["tables"][t.source]["rows"], f"{t.name}: 변환에서 행이 사라지면 안 된다"
        if t.source == "seoul_incident":
            assert s.filter(F.col("lon").isNull() & F.col("tm_x").isNotNull()).count() == 0, "TM 좌표 → 위경도"


def test_measure_jobs_run_end_to_end_inside_the_measure_folder(synth, tmp_path):
    out, m = synth
    mroot = out.parent.parent
    sd = out
    (sd / "_current").mkdir(exist_ok=True)
    base = tmp_path / "gold_stage"
    submit = shutil.which("spark-submit")
    if submit is None:
        pytest.skip("spark-submit 없음")
    env = {**os.environ, "GOLD_MEASURE_ROOT": str(mroot), "SPARK_SUBMIT": submit, "GOLD_RUNNER_MEASURE_NICE": "0",
           "PYTHONPATH": f"{ROOT}:{ROOT / 'src'}", "GOLD_SILVER_ROOT": "/nonexistent/silver", "GOLD_BRONZE_ROOT": "/nonexistent/bronze"}
    tables = ["silver_weather", "silver_traffic_speed", "silver_incident_observation"]

    def go(rid, args):
        req = R.requests_dir(base)
        req.mkdir(parents=True, exist_ok=True)
        R.write_json_atomic(req / f"{rid}.json", {"id": rid, "key": f"measure__{rid}", "date_from": D.isoformat(), "date_to": D.isoformat(),
                                                  "stage_dir": str(sd / "reports"), "created_at": time.time(), "kind": "measure", "args": args})
        assert R.process_pending(base, env=env) == [(rid, "done")], (req / f"{rid}.failed").read_text() if (req / f"{rid}.failed").exists() else ""
        return json.loads((req / f"{rid}.done").read_text())
    go("measure-b", {"job": "silver_build", "tables": tables, "version": "m0", "bronze_root": str(sd / "bronze"),
                     "silver_root": str(sd / "_current"), "rebuild_root": str(sd / "silver_rebuild"), "report": str(sd / "reports/b.json")})
    rep = json.loads((sd / "reports/b.json").read_text())
    assert rep["ok"] and all(v["status"] == "ok" and v["counts"]["missing"] == 0 for v in rep["tables"].values())
    go("measure-p", {"job": "silver_point", "tables": tables, "version": "m0", "rebuild_root": str(sd / "silver_rebuild")})
    go("measure-g", {"job": "gold_stage", "silver_root": str(sd / "_current"), "silver_rebuild_root": str(sd / "silver_rebuild"),
                     "bronze_root": str(sd / "bronze"), "stage_root": str(sd / "gold_stage/r0")})
    man = json.loads((sd / "gold_stage/r0/manifest.json").read_text())
    rows = man["row_counts"]
    assert rows["traffic_speed"] == m["tables"]["seoul_traffic"]["rows"]
    assert rows["weather_obs"] > 0 and rows["incident_episode"] == PROFILE["sources"]["seoul_incident"]["incidents"] * 2
    assert rows["call_audit"] == m["tables"]["audit"]["rows"] and man["audit_available"]
    assert not Path("/nonexistent").exists(), "운영 기본 경로로 떨어지지 않는다"
