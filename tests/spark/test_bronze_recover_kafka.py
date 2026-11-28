"""실제 Kafka 로 D-255 를 재현하고 scripts/bronze_recover.py 로 복구한다.

재현: Bronze 쿼리가 일부를 처리한 뒤 멈춘 사이 Kafka 보존 기간이 지나 체크포인트의 offset 이 지워진다 → 재시작이 실패한다.
복구: bronze_recover 가 체크포인트의 시작 위치만 '남아 있는 가장 이른 offset' 으로 올린다. batch 번호와 _spark_metadata 는 그대로다.
확인: (1) 재시작이 성공한다 (2) 복구 전에 쓴 Bronze 행이 계속 읽힌다 (3) 새 행이 이어서 쓰인다 (4) 같은 offset 이 두 번 쓰이지 않는다.
대조: 체크포인트만 지우는 옛 방법은 새 batch 가 sink 기록에 막혀 쓰이지 않는다(조용한 유실)는 것도 같은 Kafka 로 보인다.

실행 조건: Spark Kafka 커넥터와 실제 Kafka. URBANSTREAM_TEST_KAFKA_BOOTSTRAP(예: kafka:19092) 이 있으면 그 브로커를,
없고 URBANSTREAM_TEST_KAFKA_HOME 이 있으면 시험 전용 브로커를 띄워 쓴다 (conftest.kafka_bootstrap, STEP 3B).
토픽을 새로 만들고 지운다. 운영 Kafka 의 토픽은 건드리지 않는다(이름이 test.recover. 로 시작하는 토픽만 쓴다).
"""
from __future__ import annotations

import importlib.util
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

BOOTSTRAP = ""                                 # topic fixture 가 conftest.kafka_bootstrap 값으로 채운다

ROOT = Path(__file__).resolve().parents[2]


def _recover_mod():
    spec = importlib.util.spec_from_file_location("bronze_recover_mod", ROOT / "scripts" / "bronze_recover.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bronze_recover_mod"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def topic(kafka_bootstrap):
    global BOOTSTRAP
    BOOTSTRAP = kafka_bootstrap
    ck = pytest.importorskip("confluent_kafka")
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    name = f"test.recover.{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1,
                                  config={"segment.ms": "1000", "retention.ms": "604800000"})])[name].result(30)
    yield name, admin, ck
    admin.delete_topics([name])[name].result(30)


def produce(ck, topic: str, n: int, start: int) -> None:
    p = ck.Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(start, start + n):
        p.produce(topic, key=str(i % 2).encode(), value=f'{{"n": {i}}}'.encode(), partition=i % 2)
    p.flush(30)


def expire_everything(admin, topic: str, ck, wm) -> None:
    """보존 기간을 1초로 줄여 지금까지의 메시지를 지우게 한다. 활성 세그먼트는 지워지지 않으므로 새 메시지로 세그먼트를 넘긴다."""
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource, ResourceType
    res = ConfigResource(ResourceType.TOPIC, topic, incremental_configs=[
        ConfigEntry("retention.ms", "1000", incremental_operation=AlterConfigOpType.SET)])
    list(admin.incremental_alter_configs([res]).values())[0].result(30)
    deadline = time.time() + 120
    k = 10_000
    while time.time() < deadline:
        time.sleep(2)
        produce(ck, topic, 2, k)
        k += 2
        if all(wm(topic, p)[0] > 0 for p in (0, 1)):
            break
    else:
        pytest.fail("Kafka 가 120초 안에 오래된 세그먼트를 지우지 않았다 (log.retention.check.interval.ms 를 확인)")
    res = ConfigResource(ResourceType.TOPIC, topic, incremental_configs=[
        ConfigEntry("retention.ms", "604800000", incremental_operation=AlterConfigOpType.SET)])
    list(admin.incremental_alter_configs([res]).values())[0].result(30)


def run_bronze(spark, table, cfg):
    from streaming.bronze import read_kafka, to_bronze, write_bronze
    from pyspark.errors import StreamingQueryException
    q = write_bronze(to_bronze(read_kafka(spark, table, cfg), table), table, cfg, trigger={"availableNow": True})
    try:
        q.awaitTermination(180)
    except StreamingQueryException:
        pass                                   # 실패는 q.exception() 으로 확인한다
    return q


def offsets(spark, path: str) -> list[tuple[int, int]]:
    return sorted((r[0], r[1]) for r in spark.read.parquet(path).select("_kafka_partition", "_kafka_offset").collect())


def test_offset_out_of_range_is_recovered_without_hiding_old_bronze_files(spark, tmp_path, topic):
    from streaming.bronze import BronzeConfig, BronzeTable
    name, admin, ck = topic
    rec = _recover_mod()
    wm = rec.kafka_watermarks(BOOTSTRAP)
    table = BronzeTable("recover_t", name, None)
    cfg = replace(BronzeConfig(), bootstrap=BOOTSTRAP, bronze_root=str(tmp_path / "bronze"),
                  checkpoint_root=str(tmp_path / "ckpt"), max_offsets_per_trigger=None)
    out = f"{cfg.bronze_root}/{table.name}"

    produce(ck, name, 40, 0)
    q = run_bronze(spark, table, cfg)
    assert q.exception() is None
    before = offsets(spark, out)
    assert len(before) == 40

    produce(ck, name, 30, 100)                 # 멈춘 사이 쌓였다가
    expire_everything(admin, name, ck, wm)     # 보존 기간이 지나 지워진 메시지

    q = run_bronze(spark, table, cfg)          # 재시작: failOnDataLoss=true 라 실패해야 한다 (유실을 숨기지 않는다)
    assert q.exception() is not None

    ckpt = Path(cfg.checkpoint_root) / table.name
    plans = rec.plan_table(table.name, ckpt, wm)
    assert plans and plans[0].lost >= 30
    for p in plans:
        rec.apply_plan(p, tmp_path / "bak")
    assert not rec.plan_table(table.name, ckpt, wm), "복구 뒤에는 고칠 것이 없어야 한다"

    produce(ck, name, 20, 200)
    q = run_bronze(spark, table, cfg)
    assert q.exception() is None

    after = offsets(spark, out)
    assert set(before) <= set(after), "복구 전에 쓴 Bronze 행이 sink 기록에서 빠지면 안 된다 (D-258 의 원인)"
    assert len(after) == len(set(after)), "같은 offset 이 두 번 쓰이면 안 된다"
    new = [o for o in after if o not in set(before)]
    assert len(new) >= 20, "복구 뒤 새 메시지가 이어서 쓰여야 한다"
    assert (tmp_path / "bak" / "recover_t" / "offsets").is_dir(), "바꾸기 전 체크포인트 파일을 남겨야 한다"


def test_moving_only_the_checkpoint_silently_drops_new_batches(spark, tmp_path, topic):
    """옛 복구 방법의 함정(D-255): 체크포인트만 지우면 새 batch 번호가 sink 기록 이하라서 쓰이지 않는다."""
    import shutil

    from streaming.bronze import BronzeConfig, BronzeTable
    name, _admin, ck = topic
    table = BronzeTable("trap_t", name, None)
    cfg = replace(BronzeConfig(), bootstrap=BOOTSTRAP, bronze_root=str(tmp_path / "bronze"),
                  checkpoint_root=str(tmp_path / "ckpt"), max_offsets_per_trigger=5)   # batch 를 여러 개 만든다
    out = f"{cfg.bronze_root}/{table.name}"
    produce(ck, name, 40, 0)
    assert run_bronze(spark, table, cfg).exception() is None
    n1 = len(offsets(spark, out))

    shutil.rmtree(Path(cfg.checkpoint_root) / table.name)
    produce(ck, name, 10, 100)
    cfg2 = replace(cfg, starting_offsets="latest", max_offsets_per_trigger=None)
    q = run_bronze(spark, table, cfg2)
    produce(ck, name, 10, 200)
    q = run_bronze(spark, table, cfg2)
    assert q.exception() is None
    assert len(offsets(spark, out)) == n1, "새 batch 가 '이미 쓴 batch' 로 처리돼 하나도 보이지 않는다"


def test_recover_cli_status_preview_apply_on_a_real_checkpoint(spark, tmp_path, topic, capsys):
    """R1 (STEP 3B): 명령 단위. status 가 '지워진 구간' 을 찾고(rc 1), 미리보기는 아무것도 바꾸지 않고,
    --apply 가 고친 뒤 status 가 정상(rc 0)이 되고, 재시작한 Bronze 가 이어서 쓴다. recover.py bronze 가 부르는 것과 같은 명령이다."""
    from streaming.bronze import BronzeConfig, BronzeTable
    name, admin, ck = topic
    rec = _recover_mod()
    table = BronzeTable("bus_position", name, None)            # 표 이름은 config 의 실제 이름이어야 CLI 가 받는다
    cfg = replace(BronzeConfig(), bootstrap=BOOTSTRAP, bronze_root=str(tmp_path / "bronze"),
                  checkpoint_root=str(tmp_path / "ckpt"), max_offsets_per_trigger=None)
    out = f"{cfg.bronze_root}/{table.name}"
    produce(ck, name, 20, 0)
    assert run_bronze(spark, table, cfg).exception() is None
    before = offsets(spark, out)
    produce(ck, name, 10, 100)
    expire_everything(admin, name, ck, rec.kafka_watermarks(BOOTSTRAP))
    args = ["--checkpoint-root", cfg.checkpoint_root, "--bootstrap", BOOTSTRAP, "--backup-root", str(tmp_path / "bak")]

    assert rec.main(["status", "bus_position", *args]) == 1
    assert "지워진 구간" in capsys.readouterr().out
    ckpt = Path(cfg.checkpoint_root) / "bus_position" / "offsets"
    snap = {p.name: p.read_bytes() for p in ckpt.iterdir()}
    assert rec.main(["reset", "bus_position", *args]) == 0
    assert "미리보기였다" in capsys.readouterr().out
    assert {p.name: p.read_bytes() for p in ckpt.iterdir()} == snap, "미리보기는 체크포인트를 바꾸면 안 된다"

    assert rec.main(["reset", "bus_position", "--apply", *args]) == 0
    assert "적용함" in capsys.readouterr().out
    assert rec.main(["status", "bus_position", *args]) == 0
    produce(ck, name, 10, 200)
    assert run_bronze(spark, table, cfg).exception() is None
    after = offsets(spark, out)
    assert set(before) <= set(after) and len(after) == len(set(after))
    assert len(after) - len(before) >= 10
