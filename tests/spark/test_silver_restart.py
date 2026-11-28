"""R3 (STEP 3B, 설계안 7-3): Silver 쿼리가 도는 도중 멈췄다가(docker stop 과 같은 정상 종료) 다시 켜져도
Bronze 와 Silver 가 맞는다. 빠짐·중복·늘어남 없이 이어서 처리하고, 멈춘 사이 들어온 Bronze 도 처리한다.

Kafka 는 쓰지 않는다 (test_silver_e2e 와 같은 방식으로 Bronze 파일 sink 를 만든다). 운영 훈련 spark-silver-stop(drill.py)의 시험판이다.
"""
from __future__ import annotations

import time
from dataclasses import replace

import pytest

pytest.importorskip("pyspark")

from streaming import silver as S  # noqa: E402
from streaming import silver_verify as V  # noqa: E402
from streaming.verify_bronze import read_bronze  # noqa: E402

from test_silver import at, bronze, cfg_for, table_of  # noqa: E402
from test_silver_e2e import feed_bronze_sink, pos_rows  # noqa: E402

SRC = "bus_position"


def test_silver_stopped_mid_stream_resumes_and_matches_bronze(spark, tmp_path):
    for i in range(4):                                         # Bronze 커밋 4번 = 파일 묶음 4개
        feed_bronze_sink(spark, tmp_path, SRC, bronze(spark, SRC, pos_rows(30 * i, 30, f"r{i}-")), i)
    t = table_of(SRC)
    cfg = replace(cfg_for(tmp_path), max_files_per_trigger=1)  # 한 번에 파일 하나씩 → 여러 배치에 걸쳐 처리된다
    q = S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"processingTime": "1 second"})
    deadline = time.time() + 120
    while time.time() < deadline and sum(p["numInputRows"] for p in q.recentProgress) == 0:
        time.sleep(0.2)
    q.stop()                                                   # 처리 도중에 멈춘다
    assert q.exception() is None
    part = read_bronze(spark, f"{cfg.silver_root}/{t.name}").count()
    assert 0 < part < 120, f"멈추기 전에 일부만 처리돼야 시험이 된다 (처리됨 {part})"

    again = [{"dedup_key": "v3", "event_time": at(60 * 3), "fetched_at": at(60 * 200), "ingest_id": "again"}]   # 이미 본 키
    feed_bronze_sink(spark, tmp_path, SRC, bronze(spark, SRC, pos_rows(120, 20, "late-") + again), 9)   # 멈춘 사이 들어온 Bronze
    S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"availableNow": True}).awaitTermination(300)
    silver = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    assert silver.count() == 140, "dedup 상태가 체크포인트에서 돌아와야 이미 본 키(v3)가 다시 나가지 않는다"
    r = V.compare(read_bronze(spark, f"{cfg.bronze_root}/{SRC}"), silver, t)
    assert r["ok"] and r["missing"] == 0 and r["extra"] == 0 and r["dup_rows"] == 0, r
