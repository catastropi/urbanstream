"""실제 Kafka 로 '도중에 죽고 다시 켜짐' 을 재현한다 (STEP 3B, 설계안 7-3 R2·R4).

R2  Bronze 프로세스를 배치 도중 kill -9 → 같은 체크포인트로 다시 켜면 빠짐·중복 없이 이어 쓴다.
    (운영의 spark-bronze 컨테이너가 OOM 이나 Docker 재시작으로 죽는 경우. 정상 종료 SIGTERM 보다 나쁜 경우를 시험한다)
R4  Bronze 가 도는 중에 Kafka 브로커를 kill -9 → 다시 켠다. 쿼리가 버티든 실패하든, 체크포인트로 다시 켜면
    브로커가 죽기 전·후의 메시지가 모두 한 번씩만 쓰인다. (운영의 kafka 컨테이너가 죽었다 restart 되는 경우)

실행 조건: conftest.kafka_bootstrap 과 같다. R4 는 브로커를 직접 죽이므로 URBANSTREAM_TEST_KAFKA_HOME(시험 전용 브로커)이 있어야 한다.
Spark Kafka 커넥터가 필요하다 (컨테이너 밖이면 URBANSTREAM_TEST_SPARK_PACKAGES).
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
ck = pytest.importorskip("confluent_kafka")

ROOT = Path(__file__).resolve().parents[2]
TABLE_NAME = "crash_t"


def make_topic(bootstrap: str, partitions: int = 2) -> str:
    from confluent_kafka.admin import AdminClient, NewTopic
    name = f"test.recover.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})       # 결과를 받을 때까지 변수로 잡아 둔다 (먼저 사라지면 _DESTROY)
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[name].result(30)
    return name


def produce(bootstrap: str, topic: str, n: int, start: int) -> None:
    p = ck.Producer({"bootstrap.servers": bootstrap})
    for i in range(start, start + n):
        p.produce(topic, key=str(i % 2).encode(), value=f'{{"n": {i}}}'.encode(), partition=i % 2)
    assert p.flush(60) == 0, "보낸 메시지가 모두 전달되지 않았다"


def numbers(spark, path: str) -> list[int]:
    """Bronze 에 커밋된 행의 n 값 (_spark_metadata 를 따라 읽으므로 커밋 안 된 파일은 보이지 않는다)."""
    from pyspark.sql import functions as F
    df = spark.read.parquet(path)
    return sorted(r[0] for r in df.select(F.get_json_object(F.col("_raw_value"), "$.n").cast("int")).collect())


def bronze_cfg(tmp_path: Path, bootstrap: str, **kw):
    from streaming.bronze import BronzeConfig
    return replace(BronzeConfig(), bootstrap=bootstrap, bronze_root=str(tmp_path / "bronze"),
                   checkpoint_root=str(tmp_path / "ckpt"), **kw)


def run_available_now(spark, topic: str, cfg):
    from pyspark.errors import StreamingQueryException

    from streaming.bronze import BronzeTable, read_kafka, to_bronze, write_bronze
    t = BronzeTable(TABLE_NAME, topic, None)
    q = write_bronze(to_bronze(read_kafka(spark, t, cfg), t), t, cfg, trigger={"availableNow": True})
    try:
        q.awaitTermination(300)
    except StreamingQueryException:
        pass
    return q


CHILD = textwrap.dedent("""
    import os, sys, time
    sys.path.insert(0, sys.argv[1])
    from dataclasses import replace
    from pyspark.sql import SparkSession
    from streaming.bronze import BronzeConfig, BronzeTable, read_kafka, to_bronze, write_bronze
    b = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
    if os.environ.get("URBANSTREAM_TEST_SPARK_PACKAGES"):
        b = b.config("spark.jars.packages", os.environ["URBANSTREAM_TEST_SPARK_PACKAGES"])
    spark = b.getOrCreate()
    bootstrap, topic, root = sys.argv[2], sys.argv[3], sys.argv[4]
    cfg = replace(BronzeConfig(), bootstrap=bootstrap, bronze_root=root + "/bronze", checkpoint_root=root + "/ckpt",
                  max_offsets_per_trigger=7)
    t = BronzeTable("crash_t", topic, None)
    q = write_bronze(to_bronze(read_kafka(spark, t, cfg), t), t, cfg, trigger={"processingTime": "1 second"})
    print("started", flush=True)
    q.awaitTermination()
""")


def committed_batches(root: Path) -> int:
    meta = root / "bronze" / TABLE_NAME / "_spark_metadata"
    return len([p for p in meta.iterdir() if p.name.isdigit()]) if meta.is_dir() else 0


def test_bronze_killed_mid_batch_resumes_without_loss_or_duplicates(spark, tmp_path, kafka_bootstrap):
    """R2. 자식 프로세스의 Bronze 가 배치 몇 개를 커밋한 뒤, 진행 중인 배치 도중 SIGKILL. 다시 켜서 끝까지 읽는다."""
    topic = make_topic(kafka_bootstrap)
    produce(kafka_bootstrap, topic, 200, 0)
    script = tmp_path / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    log = open(tmp_path / "child.log", "wb")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))     # pytest 의 pythonpath 설정을 자식에게도
    p = subprocess.Popen([sys.executable, str(script), str(ROOT), kafka_bootstrap, topic, str(tmp_path)],
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    try:
        deadline = time.time() + 240
        while time.time() < deadline and committed_batches(tmp_path) < 3:
            assert p.poll() is None, f"자식 Spark 가 먼저 끝났다: {(tmp_path / 'child.log').read_text()[-2000:]}"
            time.sleep(0.2)
        assert committed_batches(tmp_path) >= 3, "240초 안에 배치 3개가 커밋되지 않았다"
    finally:
        try:
            os.killpg(p.pid, signal.SIGKILL)                 # 정상 종료 없이 (OOM·전원 차단과 같다)
        except ProcessLookupError:
            pass
        p.wait(60)
        log.close()
    done_before = committed_batches(tmp_path)
    assert done_before * 7 < 200, "죽이기 전에 다 읽어 버리면 '도중에 죽음' 을 시험하지 못한다"

    produce(kafka_bootstrap, topic, 50, 1000)                # 죽어 있는 동안 쌓인 메시지
    q = run_available_now(spark, topic, bronze_cfg(tmp_path, kafka_bootstrap, max_offsets_per_trigger=None))
    assert q.exception() is None
    got = numbers(spark, str(tmp_path / "bronze" / TABLE_NAME))
    want = list(range(200)) + list(range(1000, 1050))
    assert len(got) == len(set(got)), "같은 메시지가 두 번 쓰였다"
    assert got == want, f"빠진 메시지: {sorted(set(want) - set(got))[:10]}"


def test_bronze_survives_broker_kill_and_restart(spark, tmp_path):
    """R4. 시험 전용 브로커를 하나 더 띄워 Bronze 가 도는 중에 kill -9 → 20초 뒤 다시 켠다."""
    if not os.environ.get("URBANSTREAM_TEST_KAFKA_HOME"):
        pytest.skip("브로커를 죽였다 켜는 시험이라 URBANSTREAM_TEST_KAFKA_HOME(시험 전용 브로커)이 필요하다")
    from kafka_broker import Broker
    from pyspark.errors import StreamingQueryException

    from streaming.bronze import BronzeTable, read_kafka, to_bronze, write_bronze
    broker = Broker(tmp_path / "kafka").start()
    try:
        bs = broker.bootstrap
        topic = make_topic(bs)
        cfg = bronze_cfg(tmp_path, bs, max_offsets_per_trigger=None)
        t = BronzeTable(TABLE_NAME, topic, None)
        out = str(tmp_path / "bronze" / TABLE_NAME)
        produce(bs, topic, 60, 0)
        q = write_bronze(to_bronze(read_kafka(spark, t, cfg), t), t, cfg, trigger={"processingTime": "1 second"})
        deadline = time.time() + 120
        while time.time() < deadline and committed_batches(tmp_path) < 1:
            time.sleep(0.5)
        assert committed_batches(tmp_path) >= 1

        broker.kill9()
        time.sleep(20)                                       # 브로커가 없는 동안 쿼리는 재시도하거나 실패한다
        broker.start()
        produce(bs, topic, 40, 500)

        deadline = time.time() + 180                         # 버틴 쿼리라면 스스로 이어 읽는다
        while time.time() < deadline and q.isActive:
            if (tmp_path / "bronze" / TABLE_NAME).is_dir() and len(numbers(spark, out)) >= 100:
                break
            time.sleep(2)
        survived = q.isActive
        q.stop()
        try:
            q.awaitTermination(30)
        except StreamingQueryException:
            pass
        q2 = run_available_now(spark, topic, cfg)            # 실패했다면 운영처럼 다시 켠다 (컨테이너 restart 정책)
        assert q2.exception() is None, q2.exception()
        got = numbers(spark, out)
        assert len(got) == len(set(got)), "같은 메시지가 두 번 쓰였다"
        assert got == list(range(60)) + list(range(500, 540)), f"빠짐 (쿼리가 버텼나: {survived})"
    finally:
        broker.stop()
