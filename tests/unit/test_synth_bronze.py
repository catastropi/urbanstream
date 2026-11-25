"""STEP 2 합성 Bronze 생성기(scripts/synth_bronze.py): 시드 재현, 경로·날짜 가드, 합성 표시 네 곳, 규모, 프로파일. JVM 없이 시험한다.
Spark 로 읽어 Bronze 스키마·Silver 변환을 확인하는 시험은 tests/spark/test_synth_bronze.py."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import synth_bronze as SB  # noqa: E402
from measure_guard import GuardError  # noqa: E402

PROFILE = json.loads((ROOT / "tests/fixtures/measure/profile_small.json").read_text(encoding="utf-8"))
D = date(2026, 1, 5)


def gen(tmp_path, name="r1", **kw):
    args = {"scale": 1, "d": D, "seed": 7, "run_id": name, "out_dir": tmp_path / name, "layout": "minute"}
    args.update(kw)
    return SB.generate(PROFILE, **args)


def test_same_seed_same_files_and_different_seed_differs(tmp_path):
    a, b = gen(tmp_path, "r1"), gen(tmp_path / "x", "r1")
    assert a["content_sha256"] == b["content_sha256"] and a["total"] == b["total"]
    c = gen(tmp_path / "y", "r1", seed=8)
    assert c["content_sha256"] != a["content_sha256"]


def test_four_synthetic_marks(tmp_path):
    m = gen(tmp_path)
    assert m["synthetic"] is True and m["seed"] == 7 and m["scale"] == 1 and m["profile_date"] == "2026-11-04"
    assert json.loads((tmp_path / "r1" / "manifest.json").read_text())["synthetic"] is True
    f = next((tmp_path / "r1" / "bronze" / "bus_position").rglob("*.parquet"))
    md = pq.read_schema(f).metadata
    assert md[b"urbanstream.synthetic"] == b"true" and md[b"urbanstream.run_id"] == b"r1"
    rows = pq.read_table(f).to_pylist()
    assert all(r["ingest_id"].startswith("syn-") and r["raw_uri"] == "synthetic://r1" for r in rows)
    raw = json.loads(rows[0]["_raw_value"])
    assert raw["ingest_id"] == rows[0]["ingest_id"] and raw["payload"]["veh_id"] == rows[0]["payload"]["veh_id"]


def test_columns_match_the_bronze_contract_and_every_row_parses(tmp_path):
    gen(tmp_path)
    for src in SB.SOURCES:
        files = list((tmp_path / "r1" / "bronze" / src).rglob("*.parquet"))
        assert files, src
        t = pq.read_table(files[0])
        assert t.schema.remove_metadata() == SB.bronze_arrow_schema(src).remove_metadata()
        assert all(t.column("_parse_ok").to_pylist())
    audit = pq.read_table(next((tmp_path / "r1" / "bronze" / "audit").rglob("*.parquet")))
    msg = json.loads(audit.column("_raw_value")[0].as_py())
    assert msg["event"] == "call" and msg["final"] is True and msg["ingest_id"].startswith("syn-")


def test_dedup_keys_follow_the_contract_rules(tmp_path):
    gen(tmp_path)
    t = pq.read_table(list((tmp_path / "r1" / "bronze" / "seoul_traffic").rglob("*.parquet"))[0]).to_pylist()[0]
    assert t["dedup_key"] == f"{t['payload']['link_id']}|{SB.traffic.minute_key(t['event_time'])}"
    w = [r for f in (tmp_path / "r1" / "bronze" / "kma_ncst").rglob("*.parquet") for r in pq.read_table(f).to_pylist()]
    assert {r["payload"]["category"] for r in w} == set(SB.weather.CATEGORIES)
    assert all(r["event_time"].minute == 0 for r in w), "기상 event_time 은 정시 발표 시각"
    keys = [r["dedup_key"] for r in w]
    assert len(set(keys)) < len(keys), "같은 발표를 여러 호출이 다시 받는다 (Silver 중복 제거 대상)"


def test_scale_multiplies_entities_with_s_prefix(tmp_path):
    one, five = gen(tmp_path, "a"), gen(tmp_path, "b", scale=5)
    for src in ("seoul_traffic", "bus_position", "bus_arrival", "kma_ncst"):
        assert five["tables"][src]["rows"] == pytest.approx(5 * one["tables"][src]["rows"], rel=0.02), src
    links = {r["payload"]["link_id"] for f in (tmp_path / "b" / "bronze" / "seoul_traffic").rglob("*.parquet")
             for r in pq.read_table(f).to_pylist()}
    assert len(links) == 10 and {x for x in links if x.startswith("S")} and set(PROFILE["sources"]["seoul_traffic"]["entities"]) <= links


def test_hour_layout_writes_at_most_one_file_per_table_hour(tmp_path):
    m = gen(tmp_path, layout="hour")
    for src, x in m["tables"].items():
        assert x["files"] <= 25 * len(x["partitions"]), src
    assert m["total"]["rows"] == gen(tmp_path / "z")["total"]["rows"]


def test_profile_round_trip_on_generated_bronze(tmp_path):
    gen(tmp_path)
    prof = SB.build_profile(tmp_path / "r1" / "bronze", D)
    t = prof["sources"]["seoul_traffic"]
    assert t["calls"] == sum(PROFILE["sources"]["seoul_traffic"]["calls_by_hour"])
    assert t["entities"] == sorted(PROFILE["sources"]["seoul_traffic"]["entities"])
    assert prof["sources"]["bus_arrival"]["records_per_call"]["median"] == 40
    assert prof["sources"]["kma_ncst"]["grids"] == [[60, 127]]
    sp = prof["sources"]["seoul_traffic"]["fields"]["speed_kmh"]
    assert sp["t"] == "num" and 5 <= sp["q"][0] <= sp["q"][-1] <= 60
    assert prof["sources"]["bus_arrival"]["fields"]["arrival_msg"]["t"] == "cat"


def test_cli_refuses_production_output_and_real_dates(tmp_path, monkeypatch):
    p = tmp_path / "p.json"
    p.write_text(json.dumps(PROFILE), encoding="utf-8")
    with pytest.raises(GuardError):
        SB.main(["generate", "--profile", str(p), "--run-id", "x", "--out-root", "data/bronze"])
    with pytest.raises(GuardError):
        SB.main(["generate", "--profile", str(p), "--run-id", "x", "--date", "2026-11-27"])
    with pytest.raises(SystemExit):
        SB.main(["generate", "--profile", str(p), "--run-id", "../x"])
