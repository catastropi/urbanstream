"""Bronze → Silver 재처리 (STEP 1, batch/silver_rebuild.py · streaming/silver_read.py). 설계안 5-2 의 S1~S11.

운영과 같은 폴더 모양을 쓴다: Bronze 는 파일 sink(_spark_metadata 포함), Silver 는 스트리밍 쿼리(availableNow)가 만든다.
날짜는 2026-11-03(KST) 이고 재처리 날짜 가드(그제 이전)를 통과한다.
"""
from __future__ import annotations

import copy
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from batch import silver_rebuild as SR  # noqa: E402
from streaming import silver as S  # noqa: E402
from streaming import silver_pointer as P  # noqa: E402
from streaming import silver_verify as V  # noqa: E402
from streaming.bronze import to_bronze_typed  # noqa: E402
from streaming.silver_read import read_silver  # noqa: E402
from streaming.verify_bronze import read_bronze  # noqa: E402

from spark_helpers import kafka_rows, parse_sample  # noqa: E402
from test_silver import CONFIG_DIR, at, cfg_for, table_of  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
D = date(2026, 11, 3)


# ------------------------------------------------------------------ 도구

def bronze_df(spark, source, rows, *, start_offset=0, raw=None):
    """test_silver.bronze 와 같되 Kafka offset 시작값을 줄 수 있다 (두 번째 배치의 offset 이 첫 배치와 겹치지 않게)."""
    base = parse_sample(source)[0].to_dict()
    msgs = []
    for i, ov in enumerate(rows):
        d = copy.deepcopy(base)
        ov = dict(ov)
        d["payload"].update(ov.pop("payload", {}))
        d.update(ov)
        d.setdefault("ingest_id", f"call-{start_offset + i}")
        msgs.append(json.dumps(d, ensure_ascii=False))
    msgs += list(raw or [])
    return to_bronze_typed(kafka_rows(spark, msgs, topic="t-" + source, start_offset=start_offset), source)


def feed(spark, tmp_path, source, df, *, tag="a"):
    """df 를 Bronze 파일 sink 로 append 한다 (운영 Bronze 와 같은 _spark_metadata). tag 가 다르면 체크포인트가 다르다."""
    stage = tmp_path / f"stage_{tag}" / source
    df.write.mode("append").partitionBy("event_date").parquet(str(stage))
    q = (spark.readStream.schema(df.schema).parquet(str(stage))
         .writeStream.format("parquet").outputMode("append").partitionBy("event_date")
         .option("path", str(tmp_path / "bronze" / source)).option("checkpointLocation", str(tmp_path / f"bronze_ckpt_{tag}" / source))
         .trigger(availableNow=True).start())
    q.awaitTermination()


def stream_silver(spark, cfg, source):
    t = table_of(source)
    S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"availableNow": True}).awaitTermination()
    return t


def pos(start, n, ingest="a", **extra):
    return [{"dedup_key": f"v{start + i}", "event_time": at(60 * (start + i)), "fetched_at": at(60 * (start + i) + 5),
             "ingest_id": f"{ingest}{start + i}", **extra} for i in range(n)]


def traffic(speeds):
    return [{"dedup_key": f"k{i}", "event_time": at(300 * i), "fetched_at": at(300 * i + 3), "ingest_id": f"c{i}",
             "payload": {"speed_kmh": float(s)}} for i, s in enumerate(speeds)]


def rows_of(df):
    """비교용: silver_processed_at(실행마다 다름)을 뺀 행 집합."""
    return sorted(json.dumps({k: str(v) for k, v in r.asDict().items() if k != "silver_processed_at"}, sort_keys=True)
                  for r in df.collect())


def setup(tmp_path):
    cfg = cfg_for(tmp_path, rebuild_root=str(tmp_path / "rebuild"))
    return cfg, tmp_path / "rebuild"


def rebuild(spark, cfg, rb, tables, version, days=(D,), **kw):
    return SR.build(spark, cfg, list(tables), list(days), rebuild_root=str(rb), version=version, **kw)


def rs(spark, cfg, rb, t):
    return read_silver(spark, cfg.silver_root, str(rb), t.name, t.partition_col)


# ------------------------------------------------------------------ S8: 전제 확인

def test_reading_partition_dirs_ignores_parent_sink_log(spark, tmp_path):
    """Spark 4.1 에서 하위 파티션 폴더를 직접 읽으면 부모 _spark_metadata 를 쓰지 않는다: 로그 밖 파일도 보이고, 로그에서 지운 파일로 오류가 나지 않는다.
    (루트를 읽으면 로그만 본다 — D-21.) listing 방식 Bronze 읽기와 read_silver 의 버전 폴더 읽기가 이 동작에 기댄다."""
    src = "bus_position"
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(0, 6)))
    root = tmp_path / "bronze" / src
    part = root / f"event_date={D}"
    assert (root / "_spark_metadata").is_dir()
    stray = bronze_df(spark, src, pos(100, 1, "x"), start_offset=100)
    stray.drop("event_date").coalesce(1).write.parquet(str(tmp_path / "stray"))
    shutil.copy(next((tmp_path / "stray").glob("*.parquet")), part / "part-stray-not-in-log.parquet")
    assert read_bronze(spark, str(root)).count() == 6                                          # 루트: 로그만
    direct = spark.read.option("basePath", str(root)).parquet(str(part))
    assert direct.count() == 7 and "event_date" in direct.columns                               # 하위 폴더: 나열한 파일 전부
    # 로그에 있는 파일을 지워도 하위 폴더 읽기는 오류가 나지 않는다
    logged = [p for p in part.glob("part-*.parquet") if "stray" not in p.name]
    logged[0].unlink()
    assert spark.read.option("basePath", str(root)).parquet(str(part)).count() < 7


# ------------------------------------------------------------------ S1 · S2: 스트리밍과 동등, 두 번 해도 같음

def test_rebuild_equals_streaming_silver_and_two_rebuilds_are_identical(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    src = "bus_position"
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(0, 30), raw=["not json"]))
    # 두 번째 Bronze 배치: 새 행 + 워터마크 안에서 다시 온 키(v29). 스트리밍은 첫 행만 남긴다
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(30, 5, "b") + [{"dedup_key": "v29", "event_time": at(60 * 29),
                                                                       "fetched_at": at(60 * 29 + 65), "ingest_id": "again"}],
                                         start_offset=100))
    t = stream_silver(spark, cfg, src)
    streamed = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    r1 = rebuild(spark, cfg, rb, [t.name], "v1")
    r2 = rebuild(spark, cfg, rb, [t.name], "v2")
    assert r1["ok"] and r2["ok"], (r1, r2)
    b1 = r1["tables"][t.name]
    assert b1["counts"]["silver_after"] == 35 and b1["counts"]["silver_before_keys"] == 35
    assert (b1["counts"]["added"], b1["counts"]["removed"], b1["counts"]["changed"]) == (0, 0, 0)
    assert b1["counts"]["bronze_rows"] == 37 and b1["counts"]["bronze_parse_ok"] == 36
    assert b1["dates"] == {D.isoformat(): 35}
    v1 = spark.read.parquet(str(P.version_dir(rb, t.name, "v1")))
    v2 = spark.read.parquet(str(P.version_dir(rb, t.name, "v2")))
    assert rows_of(v1) == rows_of(streamed)                                   # S1: 규칙을 바꾸지 않으면 스트리밍과 같은 행
    assert rows_of(v1) == rows_of(v2)                                         # S2: 두 번 해도 같은 행
    assert sorted(v1.columns) == sorted(streamed.columns)
    assert P.active_dates(rb, t.name) == {}                                   # build 만으로는 게시되지 않는다


# ------------------------------------------------------------------ S3 · S4: 중간 실패 주입

def _two_tables(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    feed(spark, tmp_path, "seoul_traffic", bronze_df(spark, "seoul_traffic", traffic([30, 20, 10])))
    feed(spark, tmp_path, "bus_position", bronze_df(spark, "bus_position", pos(0, 8)))
    ts = [stream_silver(spark, cfg, s) for s in ("seoul_traffic", "bus_position")]
    return cfg, rb, ts


def test_failure_in_the_middle_keeps_existing_silver(spark, tmp_path, monkeypatch):
    cfg, rb, (tt, tp) = _two_tables(spark, tmp_path)
    before = {t.name: rows_of(rs(spark, cfg, rb, t)) for t in (tt, tp)}
    monkeypatch.setenv(SR.FAIL_ENV, tt.name)                                  # 첫 표를 쓴 직후 예외
    with pytest.raises(RuntimeError, match="실패 주입"):
        rebuild(spark, cfg, rb, [tt.name, tp.name], "r0001t1")
    assert not (rb / P.ACTIVE_DIR).exists() or not any((rb / P.ACTIVE_DIR).glob("*.json"))
    assert not P.version_dir(rb, tp.name, "r0001t1").exists()                 # 두 번째 표는 시작도 안 함
    with pytest.raises(P.PointerError):
        SR.publish_all(str(rb), [tt.name, tp.name], [D], "r0001t1")           # 모든 표의 빌드가 ok 여야 게시
    assert P.active_dates(rb, tt.name) == {}                                  # 첫 표만 게시되는 일도 없다
    assert {t.name: rows_of(rs(spark, cfg, rb, t)) for t in (tt, tp)} == before


def test_killed_rebuild_process_leaves_no_publishable_version(spark, tmp_path):
    cfg, rb, (tt, tp) = _two_tables(spark, tmp_path)
    before = rows_of(rs(spark, cfg, rb, tp))
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "src")]), "SILVER_BRONZE_ROOT": cfg.bronze_root,
           "SILVER_ROOT": cfg.silver_root, "SILVER_CONFIG_DIR": CONFIG_DIR, "PYSPARK_SUBMIT_ARGS": "--master local[1] pyspark-shell"}
    p = subprocess.Popen([sys.executable, "-m", "batch.silver_rebuild", "build", "--tables", f"{tt.name},{tp.name}", "--from", str(D),
                          "--to", str(D), "--version", "r0002t1", "--rebuild-root", str(rb)], cwd=str(ROOT), env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    target = P.version_dir(rb, tt.name, "r0002t1")
    deadline = time.time() + 180
    while not target.exists() and p.poll() is None and time.time() < deadline:
        time.sleep(0.02)
    assert p.poll() is None, "프로세스가 쓰기 전에 끝났다"
    os.killpg(p.pid, signal.SIGKILL)                                         # 쓰는 도중 종료 (JVM 포함)
    p.wait(30)
    assert not (target / P.BUILD_FILE).exists()
    with pytest.raises(P.PointerError, match="빌드 기록이 없습니다"):
        SR.publish_all(str(rb), [tt.name, tp.name], [D], "r0002t1")
    assert P.active_dates(rb, tp.name) == {} and rows_of(rs(spark, cfg, rb, tp)) == before


# ------------------------------------------------------------------ S6: 정정 반영 (결정 (a) 근거) + 축소 가드

def test_rule_change_removes_rows_and_changes_values_only_through_the_pointer(spark, tmp_path, monkeypatch):
    cfg, rb = setup(tmp_path)
    src = "seoul_traffic"
    feed(spark, tmp_path, src, bronze_df(spark, src, traffic([30, 0, 25, 0, 40])))
    t = stream_silver(spark, cfg, src)
    orig = S.transform_traffic
    # 바뀐 규칙: 속도 0 행은 버리고, 남는 행에 새 표시를 단다
    monkeypatch.setattr(S, "transform_traffic", lambda b: orig(b).filter(F.col("speed_kmh") != 0)
                        .withColumn("dq_flags", F.array_union(F.col("dq_flags"), F.array(F.lit("rule_v2")))))
    blocked = rebuild(spark, cfg, rb, [t.name], "r0003t1")
    b = blocked["tables"][t.name]
    assert not blocked["ok"] and b["status"] == "shrink_blocked" and b["counts"]["removed"] == 2 and b["counts"]["changed"] == 3
    with pytest.raises(P.PointerError, match="shrink_blocked"):
        SR.publish_all(str(rb), [t.name], [D], "r0003t1")
    ok = rebuild(spark, cfg, rb, [t.name], "r0004t1", allow_shrink=True)
    assert ok["ok"]
    SR.publish_all(str(rb), [t.name], [D], "r0004t1", reprocess_id=4)
    now = rs(spark, cfg, rb, t).collect()
    assert sorted(r["speed_kmh"] for r in now) == [25.0, 30.0, 40.0]           # 빠지는 정정이 반영됨
    assert all("rule_v2" in r["dq_flags"] for r in now)                       # 값이 바뀌는 정정이 반영됨
    # 비교: 두 루트를 합치는 방식(ADR-0009 원안)이면 옛 행이 남는다
    streamed = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
    union = streamed.unionByName(spark.read.parquet(str(P.version_dir(rb, t.name, "r0004t1"))))
    assert union.select("dedup_key").distinct().count() == 5 and union.filter("speed_kmh = 0").count() == 2


# ------------------------------------------------------------------ S7: D-258 (sink 로그 밖 Bronze)

def test_listing_sees_bronze_files_outside_the_sink_log_and_sink_log_mode_is_stopped_by_shrink_guard(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    src = "bus_position"
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(0, 20)), tag="a")
    t = stream_silver(spark, cfg, src)                                        # Silver 에 20행
    # D-255 복구 때처럼 Bronze 의 sink 로그를 옆으로 옮기고 새 체크포인트로 이어 쓴다 → 예전 파일은 새 로그에 없다
    shutil.move(str(tmp_path / "bronze" / src / "_spark_metadata"), str(tmp_path / "moved_meta"))
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(20, 5, "b"), start_offset=100), tag="b")
    assert read_bronze(spark, f"{cfg.bronze_root}/{src}").count() == 5        # 루트(sink 로그)로는 5행만 보인다
    sl = rebuild(spark, cfg, rb, [t.name], "r0005t1", mode="sink_log")
    assert not sl["ok"] and sl["tables"][t.name]["status"] == "shrink_blocked" and sl["tables"][t.name]["counts"]["removed"] == 20
    li = rebuild(spark, cfg, rb, [t.name], "r0006t1", mode="listing")
    c = li["tables"][t.name]["counts"]
    assert li["ok"] and c["silver_after"] == 25 and c["added"] == 5 and c["removed"] == 0
    assert c["files_listed"] >= 2 and c["files_with_rows"] == c["files_listed"]


# ------------------------------------------------------------------ S10: 돌발 (Bronze 발생 날짜 ≠ Silver 관측 날짜)

def test_incident_goes_to_its_observed_date_version(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    src = "seoul_incident"
    obs = [{"dedup_key": "acc-1", "ingest_id": f"call{i}", "event_time": at(0, day=1), "fetched_at": at(120 * i + 5),
            "payload": {"occurred_at": at(0, day=1), "expected_clear_at": at(30 * 86400), "tm_x": 198000.0, "tm_y": 451500.0}}
           for i in range(4)]
    feed(spark, tmp_path, src, bronze_df(spark, src, obs))
    assert (tmp_path / "bronze" / src / "event_date=2026-11-01").is_dir()     # Bronze 는 발생 날짜
    t = stream_silver(spark, cfg, src)
    r = rebuild(spark, cfg, rb, [t.name], "r0007t1")
    b = r["tables"][t.name]
    assert r["ok"] and b["partition_col"] == "observed_date" and b["dates"] == {D.isoformat(): 4}
    assert (P.version_dir(rb, t.name, "r0007t1") / f"observed_date={D}").is_dir()
    r0 = rebuild(spark, cfg, rb, [t.name], "r0008t1", days=(date(2026, 11, 1),))
    assert r0["ok"] and r0["tables"][t.name]["dates"] == {"2026-11-01": 0}    # 11-01 에 관측된 행은 없다


# ------------------------------------------------------------------ S11 · S9: 게시 뒤 대조, 늦은 행 경고

def test_after_publish_silver_verify_passes_and_late_streaming_rows_are_reported(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    src = "bus_position"
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(0, 12, ingested_at=at(3600)), raw=["{}"]))
    t = stream_silver(spark, cfg, src)
    assert rebuild(spark, cfg, rb, [t.name], "r0009t1")["ok"]
    SR.publish_all(str(rb), [t.name], [D], "r0009t1", reprocess_id=9)
    silver = rs(spark, cfg, rb, t)
    r = V.compare(read_bronze(spark, f"{cfg.bronze_root}/{src}"), silver, t)
    assert r["ok"] and r["missing"] == 0 and r["extra"] == 0 and r["bronze_total"] == 13, r    # S11
    dates = P.active_dates(rb, t.name)
    assert V.late_rows(read_bronze(spark, f"{cfg.silver_root}/{t.name}"), str(rb), t, dates) == {}
    # 게시 뒤 같은 날짜에 스트리밍이 행을 더 쓴다 (Mac 이 깬 뒤 밀린 Kafka 처럼)
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(40, 3, "late", ingested_at=at(7200)), start_offset=100))
    stream_silver(spark, cfg, src)
    assert V.late_rows(read_bronze(spark, f"{cfg.silver_root}/{t.name}"), str(rb), t, dates) == {D.isoformat(): 3}   # S9
    assert rs(spark, cfg, rb, t).count() == 12                                # 가려져 있다 → 다시 재처리하면 반영
    assert rebuild(spark, cfg, rb, [t.name], "r0010t1")["tables"][t.name]["counts"]["added"] == 3


# ------------------------------------------------------------------ S5: 스트리밍 쿼리와 공존

def test_rebuild_and_publish_while_streaming_query_runs(spark, tmp_path):
    cfg, rb = setup(tmp_path)
    src = "bus_position"
    feed(spark, tmp_path, src, bronze_df(spark, src, pos(0, 10)))
    t = table_of(src)
    q = S.write_silver(S.build(spark, t, cfg), t, cfg, trigger={"processingTime": "1 second"})
    try:
        q.processAllAvailable()
        assert rebuild(spark, cfg, rb, [t.name], "r0011t1")["ok"]
        SR.publish_all(str(rb), [t.name], [D], "r0011t1")
        assert q.isActive and q.exception() is None
        feed(spark, tmp_path, src, bronze_df(spark, src, pos(10, 4, "b"), start_offset=100))
        q.processAllAvailable()
        assert q.isActive and q.exception() is None
    finally:
        q.stop()
    silver_root = Path(cfg.silver_root) / t.name
    assert read_bronze(spark, str(silver_root)).count() == 14                 # 스트리밍은 계속 진행
    # 스트리밍 폴더의 Parquet 파일 = sink 로그에 있는 파일 (재처리가 스트리밍 폴더에 아무것도 쓰지 않았다)
    on_disk = {p.name for p in silver_root.rglob("*.parquet")}
    logged = set()
    for f in (silver_root / "_spark_metadata").iterdir():
        if f.name.startswith(".") or not f.is_file():
            continue
        for line in f.read_text().splitlines()[1:]:
            logged.add(Path(json.loads(line)["path"]).name)
    assert on_disk == logged
    assert not any("silver_rebuild" in str(p) or "rebuild" in p.name for p in Path(cfg.checkpoint_root).rglob("*"))
