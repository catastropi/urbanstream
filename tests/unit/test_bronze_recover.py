"""scripts/bronze_recover.py 의 체크포인트 파일 처리 (Kafka·Spark 없이). 실제 Kafka 재현은 tests/spark/test_bronze_recover_kafka.py."""
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _mod():
    spec = importlib.util.spec_from_file_location("bronze_recover_unit", ROOT / "scripts" / "bronze_recover.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bronze_recover_unit"] = mod
    spec.loader.exec_module(mod)
    return mod


META = '{"batchWatermarkMs":0,"batchTimestampMs":1,"conf":{}}'


def ckpt(tmp_path, offsets: dict[int, dict], commits: list[int]) -> Path:
    c = tmp_path / "bus_arrival"
    (c / "offsets").mkdir(parents=True)
    (c / "commits").mkdir()
    for b, off in offsets.items():
        (c / "offsets" / str(b)).write_text(f"v1\n{META}\n{json.dumps(off)}", encoding="utf-8")
        (c / "offsets" / f".{b}.crc").write_bytes(b"crc")
    for b in commits:
        (c / "commits" / str(b)).write_text('v1\n{"nextBatchWatermarkMs":0}', encoding="utf-8")
    return c


def wm(low):
    return lambda topic, part: (low[(topic, part)], low[(topic, part)] + 100)


def test_only_the_last_committed_and_later_batches_are_touched(tmp_path):
    m = _mod()
    c = ckpt(tmp_path, {5: {}, 6: {}, 7: {}}, [5, 6])
    assert m.batches_to_fix(c) == [6, 7]


def test_plan_raises_only_offsets_that_kafka_already_deleted(tmp_path):
    m = _mod()
    c = ckpt(tmp_path, {9: {"t": {"0": 10, "1": 500}}}, [9])
    plans = m.plan_table("bus_arrival", c, wm({("t", 0): 300, ("t", 1): 300}))
    assert len(plans) == 1 and plans[0].changes == {"t": {"0": (10, 300)}} and plans[0].lost == 290


def test_nothing_to_do_when_offsets_are_still_in_kafka(tmp_path):
    m = _mod()
    c = ckpt(tmp_path, {9: {"t": {"0": 400}}}, [9])
    assert m.plan_table("bus_arrival", c, wm({("t", 0): 300})) == []


def test_apply_keeps_batch_number_and_metadata_and_backs_up(tmp_path):
    m = _mod()
    c = ckpt(tmp_path, {9: {"t": {"0": 10, "1": 500}}}, [9])
    plan = m.plan_table("bus_arrival", c, wm({("t", 0): 300, ("t", 1): 300}))[0]
    m.apply_plan(plan, tmp_path / "bak")
    lines = (c / "offsets" / "9").read_text(encoding="utf-8").splitlines()
    assert lines[0] == "v1" and lines[1] == META
    assert json.loads(lines[2]) == {"t": {"0": 300, "1": 500}}
    assert not (c / "offsets" / ".9.crc").exists(), "내용이 바뀐 파일의 옛 체크섬은 지워야 Spark 가 읽는다"
    assert "\"0\": 10" in (tmp_path / "bak" / "bus_arrival" / "offsets" / "9").read_text(encoding="utf-8")


def test_spark_metadata_and_commits_are_never_touched(tmp_path):
    m = _mod()
    c = ckpt(tmp_path, {9: {"t": {"0": 10}}}, [9])
    before = (c / "commits" / "9").read_bytes()
    for p in m.plan_table("bus_arrival", c, wm({("t", 0): 300})):
        m.apply_plan(p, tmp_path / "bak")
    assert (c / "commits" / "9").read_bytes() == before
    assert "_spark_metadata" not in (ROOT / "scripts" / "bronze_recover.py").read_text(encoding="utf-8").split('"""', 2)[2]
