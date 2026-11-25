"""증분 스테이징(D-278): 파일 수정 시각으로 고른 새 파일만 읽어 여러 번 적재한 결과가, 한 번에 전체를 읽은 결과와 같다.

시나리오는 test_gold_stage_window_pg 와 같은 사건·호출을 쓰고, 호출마다 Parquet 파일을 따로 만들어 수정 시각을 호출 시각 +1분
(Silver)·+30초(audit)로 맞춘다. 스트리밍이 1분마다 파일을 남기는 모양이다. 덧붙여:
- 늦게 온 행: D2 날짜 관측이 D3 에 쓰인 파일로 들어온다(어제 폴더도 봐야 잡힌다).
- 깨진 파일: 실패한 배치가 남긴 것처럼 Parquet 가 아닌 파일이 폴더에 있다(건너뛰어야 한다).
- 사라짐이 다음 구간에서만 보이는 사건: Spark 는 그 구간에 관측이 없어 모르고, 적재 뒤 DB 판정(EPISODE_ABSENT)이 채운다.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pg_support import pg  # noqa: F401
from batch import gold_load as L
from batch import gold_stage as G
from batch import jobs as J
from batch.gold_config import GoldConfig
from test_gold_stage_window_pg import CALLS, D1, D2, D3, KST, OBS_SCHEMA, OCC, SEEN, TRUNC, k, u

LATE = ("ACC1", "c4b", k(D2, 23, 59), k(D3, 0, 30))         # D2 23:59 관측인데 파일은 D3 00:30 에 쓰였다


def _new_files(root: Path, before: set) -> list[Path]:
    return [p for p in root.rglob("*.parquet") if p not in before]


def _put(spark, rows, schema, root: Path, part: str, mtime: datetime):
    before = set(root.rglob("*.parquet")) if root.exists() else set()
    spark.createDataFrame(rows, schema).coalesce(1).write.mode("append").partitionBy(part).parquet(str(root))
    for f in _new_files(root, before):
        os.utime(f, (mtime.timestamp(), mtime.timestamp()))


def _obs_row(acc, c, t, off):
    return (f"{acc}-h", t, t, off, [], c, acc, OCC[acc], t + timedelta(hours=3), "A04", None, "L1", "공사", "R1",
            2.0, acc == "ACC1", 126.97, 37.56, t.astimezone(KST).date())


def _audit_row(c, t):
    raw = json.dumps({"event": "call", "source_system": "seoul_incident", "target": "seoul_incident", "final": True,
                      "outcome": "ok", "ingest_id": c, "fetched_at": t.isoformat(), "http_status": 200, "latency_ms": 40,
                      "status_kind": "ok", "n_records": 1, "truncated": c in TRUNC})
    return (raw, (t + timedelta(seconds=1)).astimezone(KST).date())


def _write_streaming_like(spark, root: Path):
    silver, audit = root / "silver" / "silver_incident_observation", root / "bronze" / "audit"
    off = 0
    for c, t in CALLS.items():
        rows = []
        for acc, calls in SEEN.items():
            if c in calls:
                off += 1
                rows.append(_obs_row(acc, c, t, off))
        if rows:
            _put(spark, rows, OBS_SCHEMA, silver, "observed_date", t + timedelta(minutes=1))
        _put(spark, [_audit_row(c, t)], "_raw_value string, event_date date", audit, "event_date", t + timedelta(seconds=30))
    acc, c, t, written = LATE
    _put(spark, [_obs_row(acc, c, t, 999)], OBS_SCHEMA, silver, "observed_date", written)
    _put(spark, [_audit_row(c, t)], "_raw_value string, event_date date", audit, "event_date", written)
    bad = silver / f"observed_date={D3.isoformat()}" / "part-99999-broken.c000.snappy.parquet"
    bad.write_bytes(b"not a parquet file")
    os.utime(bad, (k(D3, 9, 3).timestamp(),) * 2)


def _episodes(pg):
    rows = pg.execute("select acc_id, first_seen_at, last_seen_at, absent_since, expected_clear_at, obs_count, is_long_term "
                      "from fact.incident_episode order by acc_id").fetchall()
    return {r[0]: tuple(u(x) if isinstance(x, datetime) else x for x in r[1:]) for r in rows}


def _expected(spark, root: Path):
    obs = spark.read.option("ignoreCorruptFiles", "true").parquet(str(root / "silver" / "silver_incident_observation"))
    audit = G.parse_audit(spark.read.parquet(str(root / "bronze" / "audit")))
    out = {}
    for r in G.stage_episodes(obs, audit, None).collect():
        out[r["acc_id"]] = (u(r["first_seen_at"]), u(r["last_seen_at"]), u(r["absent_since"]), u(r["expected_clear_at"]),
                            r["obs_count"], r["is_long_term"])
    return out, obs.count()


def test_incremental_chunks_load_the_same_as_reading_everything(spark, tmp_path, pg):
    _write_streaming_like(spark, tmp_path)
    # 구간 경계: D1 하루 / D2 00:00~12:00 직전(c4 파일은 12:01 이라 다음 구간) / 나머지(늦은 파일·깨진 파일 포함)
    bounds = [(J.kst_midnight_epoch(D1) - 1, k(D2, 0, 0).timestamp(), D1),
              (k(D2, 0, 0).timestamp() - J.INCR_OVERLAP_SEC, k(D2, 12, 0, 30).timestamp(), D2),
              (k(D2, 12, 0, 30).timestamp() - J.INCR_OVERLAP_SEC, k(D3, 23, 0).timestamp(), D3)]
    for i, (since, until, d) in enumerate(bounds):
        w0, w1 = J.incremental_window(d, d)
        cfg = GoldConfig(silver_root=str(tmp_path / "silver"), bronze_root=str(tmp_path / "bronze"),
                         stage_root=str(tmp_path / f"s{i}"), date_from=w0, date_to=w1, files_since=since, files_until=until)
        m = G.run(spark, cfg)
        assert m["audit_available"] is True and m["files_until"] == until
        L.load_all(pg, tmp_path / f"s{i}")
        if i == 1:
            # c3 에서 ACC2 가 사라졌다: 이 구간에는 ACC2 관측이 없어 Spark 는 모르지만 DB 판정이 채운다
            assert _episodes(pg)["ACC2"][2] == CALLS["c3"]
    want, n_obs = _expected(spark, tmp_path)
    assert _episodes(pg) == want
    assert want["ACC1"][1] == LATE[2]                               # 늦게 쓰인 D2 관측이 마지막 관측
    assert pg.execute("select count(*) from fact.incident_observation").fetchone()[0] == n_obs
    assert pg.execute("select count(*) from ops.call_audit").fetchone()[0] == len(CALLS) + 1


def test_files_are_chosen_by_modification_time(tmp_path):
    root = tmp_path / "t"
    for d, names in ((D1, ["a", "b"]), (D2, ["c"])):
        p = root / f"event_date={d.isoformat()}"
        p.mkdir(parents=True)
        for n in names:
            (p / f"part-{n}.parquet").write_bytes(b"x")
        (p / "_SUCCESS").write_bytes(b"")
        (p / ".part-a.parquet.crc").write_bytes(b"")
    t = k(D2, 12, 0).timestamp()
    os.utime(root / f"event_date={D1.isoformat()}" / "part-a.parquet", (t - 100, t - 100))
    os.utime(root / f"event_date={D1.isoformat()}" / "part-b.parquet", (t + 100, t + 100))
    os.utime(root / f"event_date={D2.isoformat()}" / "part-c.parquet", (t - 10, t - 10))
    got = [Path(f).name for f in G.list_new_files(str(root), "event_date", [D1, D2], t - 50, t)]
    assert got == ["part-c.parquet"]                                 # a 는 since 이하, b 는 until 초과, _·. 파일은 제외
    assert len(G.list_new_files(str(root), "event_date", [D1, D2, D3], None, t + 200)) == 3


def test_incremental_reads_pointer_days_from_the_published_version(tmp_path, monkeypatch):
    """STEP 1 포인터가 있는 날짜는 스트리밍 루트가 아니라 게시된 버전 폴더에서 새 파일을 고른다 (한 날짜는 한쪽에서만)."""
    from streaming.silver_pointer import version_dir
    seen = []
    monkeypatch.setattr(G, "read_new_files", lambda spark, root, part, days, cfg: seen.append((root, list(days))) or None)
    cfg = GoldConfig(silver_root=str(tmp_path / "silver"), bronze_root=str(tmp_path / "bronze"), rebuild_root=str(tmp_path / "rb"),
                     stage_root=str(tmp_path / "s"), date_from=D1, date_to=D3, files_since=1.0, files_until=2.0)
    assert G.read_new_silver(None, cfg, "silver_traffic_speed", "event_date", [D1, D2, D3], {D2.isoformat(): "r0007t1"}) is None
    assert seen == [(f"{tmp_path}/silver/silver_traffic_speed", [D1, D3]),
                    (str(version_dir(tmp_path / "rb", "silver_traffic_speed", "r0007t1")), [D2])]
