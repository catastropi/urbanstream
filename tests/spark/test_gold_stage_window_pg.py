"""Gold 스테이징의 읽기 범위 제한(D-274)과 돌발 수명 합치기.

1. 날짜 범위를 주면 범위 밖 파티션(Silver 돌발 관측, Bronze audit)을 읽지 않는다.
   확인 방법: DataFrame 을 만든 뒤(파일 목록은 이미 잡힘) 범위 밖 파일을 지운다. 그 파일을 읽으려 하면 Spark 가 실패한다.
2. 하루씩 스테이징·적재한 결과가, 전체 이력으로 한 번에 계산한 수명과 같다(자정을 넘는 사라짐, 여러 날 이어지는 사건 포함).
   과거 하루를 다시 돌려도 값이 뒤로 가지 않는다.
"""
from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pg_support import pg  # noqa: F401
from batch import gold_load as L
from batch import gold_stage as G
from batch.gold_config import GoldConfig

KST = timezone(timedelta(hours=9))
D1, D2, D3 = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)
OLD = date(2026, 10, 25)


def k(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=KST).astimezone(timezone.utc)


# 돌발 호출 (수집 시각, KST). c6 은 잘린 호출이라 사라짐 증거가 아니다.
CALLS = {"c1": k(D1, 10, 0), "c2": k(D1, 23, 58), "c3": k(D2, 0, 2), "c4": k(D2, 12, 0),
         "c5": k(D3, 9, 0), "c6": k(D3, 9, 2), "c7": k(D3, 9, 4)}
TRUNC = {"c6"}
OCC = {"ACC1": k(date(2026, 10, 20), 8, 0), "ACC2": k(D1, 9, 0), "ACC3": k(D3, 8, 0)}
# 사건별로 보인 호출
SEEN = {"ACC1": ["c1", "c2", "c3", "c4"],      # 여러 날 → D3 c5 에서 사라짐
        "ACC2": ["c1", "c2"],                  # D1 23:58 이 마지막 → 자정 넘어 D2 c3 에서 사라짐
        "ACC3": ["c5"]}                        # c6(잘림) 건너뛰고 c7 에서 사라짐

OBS_SCHEMA = ("dedup_key string, event_time timestamp, fetched_at timestamp, _kafka_offset long, dq_flags array<string>, "
              "ingest_id string, acc_id string, occurred_at timestamp, expected_clear_at timestamp, acc_type string, "
              "acc_detail_type string, link_id string, description string, road_code string, planned_duration_days double, "
              "is_long_term boolean, lon double, lat double, observed_date date")


def _obs_rows(extra_old: bool = False):
    rows, off = [], 0
    for acc, calls in SEEN.items():
        for c in calls:
            off += 1
            t = CALLS[c]
            clear = t + timedelta(hours=3)                   # 호출마다 다른 값: 마지막 관측의 값이 남아야 한다
            rows.append((f"{acc}-h", t, t, off, [], c, acc, OCC[acc], clear, "A04", None, "L1", "공사", "R1",
                         2.0, acc == "ACC1", 126.97, 37.56, t.astimezone(KST).date()))
    if extra_old:
        t = k(OLD, 10, 0)
        rows.append(("OLD-h", t, t, 999, [], "c0", "ACC0", t, None, "A04", None, "L1", "x", "R1", 0.1, False, 126.9, 37.5, OLD))
    return rows


def _audit_rows(extra_old: bool = False):
    calls = dict(CALLS)
    if extra_old:
        calls["c0"] = k(OLD, 10, 0)
    out = []
    for c, t in calls.items():
        raw = json.dumps({"event": "call", "source_system": "seoul_incident", "target": "seoul_incident", "final": True,
                          "outcome": "ok", "ingest_id": c, "fetched_at": t.isoformat(), "http_status": 200, "latency_ms": 40,
                          "status_kind": "ok", "n_records": 1, "truncated": c in TRUNC})
        out.append((raw, (t + timedelta(seconds=1)).astimezone(KST).date()))      # Bronze 파티션 = Kafka 시각(조금 뒤)의 KST 날짜
    return out


def _write(spark, root: Path, extra_old: bool = False):
    spark.createDataFrame(_obs_rows(extra_old), OBS_SCHEMA).write.partitionBy("observed_date").parquet(
        str(root / "silver" / "silver_incident_observation"))
    spark.createDataFrame(_audit_rows(extra_old), "_raw_value string, event_date date").write.partitionBy("event_date").parquet(
        str(root / "bronze" / "audit"))


def _cfg(root: Path, d0=None, d1=None, stage="stage") -> GoldConfig:
    return GoldConfig(silver_root=str(root / "silver"), bronze_root=str(root / "bronze"), stage_root=str(root / stage),
                      date_from=d0, date_to=d1)


def u(x):
    return x.astimezone(timezone.utc) if x is not None else None


def test_date_range_does_not_read_old_partitions(spark, tmp_path, monkeypatch):
    _write(spark, tmp_path, extra_old=True)
    real = G.read_bronze

    def read_then_remove_old(sp, path):
        df = real(sp, path)                                   # 파일 목록이 여기서 잡힌다
        for part in ("observed_date", "event_date"):
            old = Path(path) / f"{part}={OLD.isoformat()}"
            if old.exists():
                shutil.rmtree(old)                            # 읽으려 하면 FileNotFound 로 실패한다
        return df
    monkeypatch.setattr(G, "read_bronze", read_then_remove_old)
    m = G.run(spark, _cfg(tmp_path, D3, D3))
    assert m["episode_window"] == [str(D2), str(D3)]
    assert m["row_counts"]["incident_observation"] == 1 and m["row_counts"]["call_audit"] == 3   # D3 관측은 ACC3@c5 하나, 호출 c5~c7
    assert set(m["elapsed_by_table"]) >= {"incident_episode", "call_audit"}


def test_full_history_still_reads_everything_without_a_range(spark, tmp_path):
    _write(spark, tmp_path, extra_old=True)
    m = G.run(spark, _cfg(tmp_path))
    assert m["row_counts"]["incident_episode"] == 4 and m["episode_window"] == [None, None]


def _episodes(pg):
    rows = pg.execute("select acc_id, first_seen_at, last_seen_at, absent_since, expected_clear_at, obs_count, is_long_term "
                      "from fact.incident_episode order by acc_id").fetchall()
    return {r[0]: tuple(u(x) if isinstance(x, datetime) else x for x in r[1:]) for r in rows}


def _expected(spark, root: Path):
    full = _cfg(root)
    obs = G.read_bronze(spark, str(root / "silver" / "silver_incident_observation"))
    audit = G.parse_audit(G.read_bronze(spark, str(root / "bronze" / "audit")))
    out = {}
    for r in G.stage_episodes(obs, audit, full).collect():
        out[r["acc_id"]] = (u(r["first_seen_at"]), u(r["last_seen_at"]), u(r["absent_since"]), u(r["expected_clear_at"]),
                            r["obs_count"], r["is_long_term"])
    return out


def test_day_by_day_windows_merge_to_the_full_history_result(spark, tmp_path, pg):
    _write(spark, tmp_path)
    for i, d in enumerate((D1, D2, D3)):
        G.run(spark, _cfg(tmp_path, d, d, stage=f"stage{i}"))
        L.load_all(pg, tmp_path / f"stage{i}")
        if d == D1:
            # D1 만 본 시점: ACC2 는 아직 사라짐 증거가 없다(c3 는 D2)
            assert _episodes(pg)["ACC2"][2] is None
    want = _expected(spark, tmp_path)
    assert want["ACC2"][2] == CALLS["c3"] and want["ACC1"][2] == CALLS["c5"] and want["ACC3"][2] == CALLS["c7"]
    assert _episodes(pg) == want

    # 과거 하루(D1)를 다시 돌려도 더 늦은 관측·사라짐 값이 뒤로 가지 않는다
    G.run(spark, _cfg(tmp_path, D1, D1, stage="again"))
    L.load_all(pg, tmp_path / "again")
    assert _episodes(pg) == want


def test_intraday_twice_a_day_then_daily_window(spark, tmp_path, pg):
    """gold_intraday(오늘) 를 여러 번, gold_daily(어제~오늘) 를 한 번 — 순서와 상관없이 같은 결과."""
    _write(spark, tmp_path)
    for i, (d0, d1) in enumerate([(D2, D2), (D2, D2), (D1, D2), (D3, D3), (D2, D3)]):
        G.run(spark, _cfg(tmp_path, d0, d1, stage=f"s{i}"))
        L.load_all(pg, tmp_path / f"s{i}")
    assert _episodes(pg) == _expected(spark, tmp_path)


@pytest.mark.parametrize("bad", [0, -1])
def test_lookback_must_cover_the_previous_day(bad):
    with pytest.raises(ValueError):
        GoldConfig(episode_lookback_days=bad)
