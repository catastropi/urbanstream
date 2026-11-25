"""Gold 스테이징 (Spark 배치): Silver + Bronze audit → data/gold_stage/<표>/ Parquet + manifest.json.

Silver 는 streaming/silver_read.read_silver 로 읽는다: 재처리로 게시된 날짜는 data/silver_rebuild 의 버전, 나머지는 스트리밍 루트.
잡을 시작할 때 포인터를 한 번 읽어 스냅샷으로 쓰고, 그 지문(silver_pointer_sha)과 내용(silver_pointer)을 manifest 에 남긴다.

DB 에 쓰지 않는다. 적재는 batch/gold_load.py (호스트 Python). 스트리밍이 쓰는 폴더(Silver/Bronze)는 읽기만 한다.
같은 날짜 범위로 다시 돌려도 결과가 같다(스테이징 폴더를 통째로 덮어씀).

읽기 범위 (D-95, D-274)
  날짜 범위를 주면 Silver·Bronze 를 모두 파티션 날짜로 잘라 읽는다. 스트리밍이 1분마다 작은 파일을 남기므로, 전체 이력을 읽으면
  읽는 파일 수가 수집한 날 수만큼 매일 늘어난다(예전에는 돌발 수명과 audit 이 그랬다).
  - 돌발 수명(episode)은 '시작일-1 ~ 종료일'에 관측된 사건만, 그 구간의 관측으로 계산한다. 구간 밖의 이력은 적재 때
    DB 의 기존 행·fact.incident_observation 과 합친다(batch/gold_load.py 의 EPISODE_MERGE). 전날을 넣는 이유: 자정 직전에
    마지막으로 보이고 자정 직후 호출에서 사라진 사건의 absent_since 를 오늘 실행에서도 채우기 위해서다.
  - audit 은 Bronze 파티션(event_date = Kafka 메시지 시각의 KST 날짜, 수집 시각 이후)으로 '시작일-1 ~ 종료일+1'만 읽고
    한 번만 풀어 둔다(persist). 수명과 call_audit 이 같이 쓴다.
  날짜 범위가 없으면(--from/--to 생략) 예전처럼 전체 이력이다.

증분 (D-278, --files-since/--files-until)
  매시 실행이 그날 하루를 통째로 다시 읽으면 시간이 그날 쌓인 양에 비례해 밤에는 30분을 넘는다(11-22 Mac 실측).
  증분이면 날짜 범위의 파티션 폴더에서 **수정 시각이 (since, until] 인 Parquet 파일만** 골라 읽는다. 대상은 사팩트 4개 표,
  돌발 관측, Bronze audit 이다. 사팩트는 DO NOTHING 이고, 수명은 적재 때 합치며(gold_load.EPISODE_MERGE), 사라짐 시각은
  적재 뒤 DB 에서 다시 판정한다(gold_load.EPISODE_ABSENT) — 그래서 일부 구간만 읽어도 결과가 같다.
  날씨는 '시작일 ~ 종료일' 전체를 그대로 읽는다(작고, 항목 8개를 한 행으로 합치는 DO UPDATE 라 일부만 읽으면 값이 빈다).
  파일 목록은 sink 로그(_spark_metadata)를 거치지 않는다. 그래서 실패한 배치가 남긴 파일도 보일 수 있는데, 같은 행이 다시
  커밋되므로 중복은 dedup_key 로 정리되고, 깨진 파일은 건너뛴다(ignoreCorruptFiles). until 은 호출 쪽이 '지금 - 2분'으로 준다
  (쓰는 중인 파일을 피한다).

사용: python -m batch.gold_stage --from 2026-11-03 --to 2026-11-04
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from batch.gold_config import GoldConfig
from streaming.silver_pointer import snapshot, snapshot_sha, version_dir
from streaming.silver_read import read_silver
from streaming.verify_bronze import read_bronze

KST = "Asia/Seoul"
WEATHER_CATS = ["T1H", "RN1", "UUU", "VVV", "REH", "PTY", "VEC", "WSD"]

AUDIT_SCHEMA = T.StructType([
    T.StructField("event", T.StringType()), T.StructField("source_system", T.StringType()),
    T.StructField("target", T.StringType()), T.StructField("final", T.BooleanType()),
    T.StructField("outcome", T.StringType()), T.StructField("ingest_id", T.StringType()),
    T.StructField("fetched_at", T.StringType()), T.StructField("http_status", T.IntegerType()),
    T.StructField("latency_ms", T.IntegerType()), T.StructField("status_kind", T.StringType()),
    T.StructField("n_records", T.IntegerType()), T.StructField("truncated", T.BooleanType()),
])


def _in_range(df: DataFrame, col: str, cfg: GoldConfig) -> DataFrame:
    if cfg.date_from:
        df = df.filter(F.col(col) >= F.lit(cfg.date_from))
    if cfg.date_to:
        df = df.filter(F.col(col) <= F.lit(cfg.date_to))
    return df


def _lookback_cfg(cfg: GoldConfig) -> GoldConfig:
    """수명 계산용 구간: 시작일을 하루(cfg.episode_lookback_days) 앞당긴다. 범위가 없으면 그대로(전체 이력)."""
    if not cfg.date_from:
        return cfg
    return dataclasses.replace(cfg, date_from=cfg.date_from - timedelta(days=cfg.episode_lookback_days))


def audit_partitions(raw: DataFrame, cfg: GoldConfig) -> DataFrame:
    """Bronze audit 원문을 파티션 날짜로 자른다: 시작일-lookback ~ 종료일+1. Kafka 시각은 수집 시각보다 늦어 자정 근처 호출이
    다음 날 파티션에 들어갈 수 있어서 하루를 더 본다. 파티션 열이 없는 입력(시험용 평평한 폴더)은 그대로 둔다."""
    if "event_date" not in raw.columns:
        return raw
    lb = _lookback_cfg(cfg)
    if lb.date_from:
        raw = raw.filter(F.col("event_date") >= F.lit(lb.date_from))
    if lb.date_to:
        raw = raw.filter(F.col("event_date") <= F.lit(lb.date_to + timedelta(days=1)))
    return raw


def list_new_files(table_root: str, part_col: str, days: list, since: Optional[float], until: float) -> list[str]:
    """table_root/<part_col>=<날짜>/ 아래 Parquet 파일 중 수정 시각이 (since, until] 인 것. 숨김·_ 로 시작하는 파일은 뺀다."""
    out: list[str] = []
    for d in days:
        p = Path(table_root) / f"{part_col}={d.isoformat()}"
        if not p.is_dir():
            continue
        with os.scandir(p) as it:
            for e in it:
                if not e.name.endswith(".parquet") or e.name.startswith((".", "_")) or not e.is_file():
                    continue
                m = e.stat().st_mtime
                if (since is None or m > since) and m <= until:
                    out.append(e.path)
    return sorted(out)


def read_new_files(spark: SparkSession, table_root: str, part_col: str, days: list, cfg: GoldConfig) -> Optional[DataFrame]:
    files = list_new_files(table_root, part_col, days, cfg.files_since, cfg.files_until)
    if not files:
        return None
    return spark.read.option("basePath", table_root).option("ignoreCorruptFiles", "true").parquet(*files)


def read_new_silver(spark: SparkSession, cfg: GoldConfig, table: str, part_col: str, days: list,
                    pointer: Optional[dict]) -> Optional[DataFrame]:
    """증분 + 재처리 포인터: 포인터가 있는 날짜는 게시된 버전 폴더에서, 나머지는 스트리밍 루트에서 새 파일만 읽는다.
    한 날짜는 한쪽에서만 온다(read_silver 와 같은 규칙)."""
    pointer = pointer or {}
    groups: dict[str, list] = {f"{cfg.silver_root}/{table}": [d for d in days if d.isoformat() not in pointer]}
    for d in days:
        v = pointer.get(d.isoformat())
        if v:
            groups.setdefault(str(version_dir(Path(cfg.rebuild_root), table, v)), []).append(d)
    dfs = []
    for root, ds in groups.items():
        df = read_new_files(spark, root, part_col, ds, cfg) if ds else None
        if df is not None:
            dfs.append(df)
    if not dfs:
        return None
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df.select(*out.columns))
    return out


def _days(d0: date, d1: date) -> list:
    return [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]


def _first_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    """Silver 에 같은 키가 둘 이상 있으면(워터마크 뒤 재통과, ADR-0006 7-B) 가장 먼저 본 한 행만 남긴다. 결정적."""
    w = Window.partitionBy(*keys).orderBy(F.col("fetched_at"), F.col("_kafka_offset"))
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _flags(c: str = "dq_flags") -> Column:
    return F.coalesce(F.col(c), F.array().cast("array<string>")).alias("dq_flags")


# ------------------------------------------------------------------ 표별 변환 (입력 = Silver DataFrame)

def stage_traffic(s: DataFrame, cfg: GoldConfig) -> DataFrame:
    s = _first_per_key(_in_range(s, "event_date", cfg), ["dedup_key"])
    return s.select("dedup_key", "link_id", "event_time", "fetched_at", "speed_kmh",
                    F.col("travel_time_sec").cast("int"), _flags(), "event_date")


def stage_position(s: DataFrame, cfg: GoldConfig) -> DataFrame:
    s = _first_per_key(_in_range(s, "event_date", cfg), ["dedup_key"])
    return s.select("dedup_key", "route_id", "veh_id", "plain_no", "event_time", "fetched_at", "lon", "lat",
                    "section_id", F.col("sect_ord").cast("int"), "last_stop_id", "next_stop_id",
                    "sect_dist", "full_sect_dist", "rt_dist", F.col("congestion").cast("int"),
                    F.col("stop_flag").cast("int"), "is_running", "is_last", "is_full", _flags(), "event_date")


def stage_arrival(s: DataFrame, cfg: GoldConfig) -> DataFrame:
    s = _first_per_key(_in_range(s, "event_date", cfg), ["dedup_key"])
    ints = ["station_order", "stops_away", "arrive_in_sec", "pred_exps_sec", "pred_kals_sec", "pred_neus_sec", "arrive_code"]
    return s.select("dedup_key", "route_id", "station_id", F.col("station_order").cast("int"),
                    F.col("slot").cast("short"), "veh_id", "plain_no", "event_time", "fetched_at", "arrival_msg",
                    *[F.col(c).cast("int") for c in ints[1:]], "is_last_bus", _flags(), "event_date")


def stage_incident_obs(s: DataFrame, cfg: GoldConfig) -> DataFrame:
    s = _first_per_key(_in_range(s, "observed_date", cfg), ["dedup_key", "ingest_id"])
    return s.select("dedup_key", "ingest_id", "acc_id", "link_id", "fetched_at", "occurred_at", "expected_clear_at",
                    "acc_type", "acc_detail_type", "lon", "lat", "description", "road_code", "planned_duration_days",
                    F.coalesce(F.col("is_long_term"), F.lit(False)).alias("is_long_term"), _flags(),
                    "observed_date")


def stage_weather(s: DataFrame, cfg: GoldConfig) -> DataFrame:
    """긴 형태(항목 1행) → 시각×격자 1행(항목 8열). 같은 (시각, 격자, 항목)이 둘이면 가장 먼저 본 값."""
    s = _first_per_key(_in_range(s, "event_date", cfg), ["event_time", "nx", "ny", "category"])
    unparsed = F.sum(F.when(F.array_contains(F.coalesce(F.col("dq_flags"), F.array().cast("array<string>")),
                                             "value_unparsed"), 1).otherwise(0)).cast("int")
    cols = [F.max(F.when(F.col("category") == c, F.col("value"))).alias(c.lower()) for c in WEATHER_CATS]
    return (s.groupBy(F.col("event_time").alias("base_time"), F.col("nx").cast("int").alias("nx"),
                      F.col("ny").cast("int").alias("ny"), "event_date")
            .agg(*cols, unparsed.alias("value_unparsed_count")))


def parse_audit(raw: DataFrame) -> DataFrame:
    """Bronze audit(원문 JSON) → 최종 결과 행(final=true, ingest_id 있음)만. 건너뜀·재시도 중간 기록은 제외."""
    j = F.from_json(F.col("_raw_value"), AUDIT_SCHEMA)
    a = raw.select(j.alias("a")).select("a.*").filter(
        (F.col("event") == "call") & (F.col("final") == True) & F.col("ingest_id").isNotNull())  # noqa: E712
    return a.select(
        "ingest_id", "source_system", "target", F.to_timestamp("fetched_at").alias("fetched_at"),
        F.col("status_kind"), (F.col("outcome") == "ok").alias("ok"), "http_status", "latency_ms",
        F.coalesce(F.col("truncated"), F.lit(False)).alias("truncated"), F.col("n_records").alias("record_count"),
    ).filter(F.col("fetched_at").isNotNull()).dropDuplicates(["ingest_id"])


def stage_audit(a: DataFrame, cfg: GoldConfig) -> DataFrame:
    a = a.withColumn("event_date", F.to_date(F.from_utc_timestamp("fetched_at", KST)))
    return _in_range(a, "event_date", cfg)


def stage_episodes(obs: DataFrame, audit: Optional[DataFrame], cfg: Optional[GoldConfig] = None) -> DataFrame:
    """돌발 수명. absent_since = 마지막 관측 뒤의 **성공·비절단** 돌발 호출 중 첫 시각 (벽시계 아님, ADR-0008 ⑧).
    audit 이 없으면 모든 행이 absent_since NULL(= 아직 목록에 있음)이 되므로 호출 쪽에서 audit 없음을 경고한다.

    cfg 에 날짜 범위가 있으면 '시작일-1 ~ 종료일'의 관측·호출만 쓴다. 이 결과는 그 구간에서 본 값이므로 적재 때 기존 행과 합친다
    (gold_load.EPISODE_MERGE). 호출은 수집 시각의 KST 날짜가 종료일 이하인 것만 쓴다: 그보다 뒤의 호출은 관측을 읽지 않았으므로
    사라짐의 증거로 쓸 수 없다."""
    o = obs
    if cfg is not None and (cfg.date_from or cfg.date_to):
        lb = _lookback_cfg(cfg)
        o = _in_range(o, "observed_date", lb)
        if audit is not None:
            audit = _in_range(audit.withColumn("_d", F.to_date(F.from_utc_timestamp("fetched_at", KST))), "_d", lb).drop("_d")
    o = o.filter(F.col("occurred_at").isNotNull())
    ep = o.groupBy("acc_id", "occurred_at").agg(
        F.min("fetched_at").alias("first_seen_at"), F.max("fetched_at").alias("last_seen_at"),
        F.countDistinct("ingest_id").cast("int").alias("obs_count"),
        F.max_by("expected_clear_at", "fetched_at").alias("expected_clear_at"),
        F.max("is_long_term").alias("is_long_term"))
    if audit is None:
        return ep.withColumn("absent_since", F.lit(None).cast("timestamp")).select(*_EP_COLS)
    good = (audit.filter((F.col("source_system") == "seoul_incident") & F.col("ok") & ~F.col("truncated"))
            .select(F.col("fetched_at").alias("_call_at")))
    j = ep.join(good, F.col("_call_at") > F.col("last_seen_at"), "left")
    return (j.groupBy(*[c for c in ep.columns]).agg(F.min("_call_at").alias("absent_since")).select(*_EP_COLS))


_EP_COLS = ["acc_id", "occurred_at", "first_seen_at", "last_seen_at", "absent_since", "expected_clear_at",
            "obs_count", "is_long_term"]


# ------------------------------------------------------------------ 실행

SILVER_TABLES = {"silver_traffic_speed": "event_date", "silver_bus_position": "event_date", "silver_bus_arrival": "event_date",
                 "silver_incident_observation": "observed_date", "silver_weather": "event_date"}

def _write(df: DataFrame, cfg: GoldConfig, name: str) -> int:
    path = f"{cfg.stage_root}/{name}"
    df.write.mode("overwrite").parquet(path)
    return df.sparkSession.read.parquet(path).count()


def run(spark: SparkSession, cfg: GoldConfig) -> dict:
    t0 = time.monotonic()
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")   # 호스트 pyarrow 가 UTC 로 읽도록
    rebuild = Path(cfg.rebuild_root) if cfg.rebuild_root else None
    snap = snapshot(rebuild, SILVER_TABLES)
    audit_root = Path(cfg.bronze_root) / "audit"
    if cfg.incremental:
        days = _days(cfg.date_from, cfg.date_to)
        silver = {n: read_new_silver(spark, cfg, n, part, days, snap[n]) for n, part in SILVER_TABLES.items() if n != "silver_weather"}
        silver["silver_weather"] = read_silver(spark, cfg.silver_root, cfg.rebuild_root, "silver_weather", "event_date",
                                               snap["silver_weather"])
        # audit 파티션 = Kafka 시각의 날짜라 종료일 다음 날 폴더도 본다(자정 직전 호출)
        audit_raw = read_new_files(spark, str(audit_root), "event_date", _days(cfg.date_from, cfg.date_to + timedelta(days=1)), cfg)
        audit_available = audit_root.is_dir()                 # 새 호출이 없을 수도 있다(그래도 audit 은 있는 것)
    else:
        silver = {n: read_silver(spark, cfg.silver_root, cfg.rebuild_root, n, part, snap[n]) for n, part in SILVER_TABLES.items()}
        audit_raw = read_bronze(spark, str(audit_root))
        audit_raw = audit_partitions(audit_raw, cfg) if audit_raw is not None else None
        audit_available = audit_raw is not None
    # 한 번만 풀어 둔다: 수명과 call_audit 이 같이 쓴다. 범위가 있으면 하루 이틀치라 메모리에 들어간다.
    audit = parse_audit(audit_raw).persist() if audit_raw is not None else None
    counts: dict[str, int] = {}
    elapsed: dict[str, float] = {}
    files_read = ({n: (len(df.inputFiles()) if df is not None else 0) for n, df in silver.items() if n != "silver_weather"}
                  if cfg.incremental else None)
    if cfg.incremental:
        files_read["audit"] = len(audit_raw.inputFiles()) if audit_raw is not None else 0
        print(f"[0s] 증분 파일 수: {files_read}", file=sys.stderr, flush=True)
    plan = [("traffic_speed", "silver_traffic_speed", stage_traffic), ("bus_position", "silver_bus_position", stage_position),
            ("bus_arrival", "silver_bus_arrival", stage_arrival),
            ("incident_observation", "silver_incident_observation", stage_incident_obs),
            ("weather_obs", "silver_weather", stage_weather)]
    def timed(name: str, make) -> None:
        t = time.monotonic()
        counts[name] = make()
        elapsed[name] = round(time.monotonic() - t, 1)
        # 진행 상황: 실행기 로그(stderr 끝부분)에서 어느 표가 오래 걸리는지 보인다
        print(f"[{round(time.monotonic() - t0)}s] {name}: {counts[name]}행, {elapsed[name]}초", file=sys.stderr, flush=True)

    for out, src, fn in plan:
        timed(out, lambda fn=fn, src=src, out=out: _write(fn(silver[src], cfg), cfg, out) if silver[src] is not None else 0)
    inc = silver["silver_incident_observation"]
    # 증분이면 입력이 이미 새 파일로 좁혀져 있다 → 날짜로 더 자르지 않는다(cfg=None)
    ep_cfg = None if cfg.incremental else cfg
    timed("incident_episode", lambda: _write(stage_episodes(inc, audit, ep_cfg), cfg, "incident_episode") if inc is not None else 0)
    timed("call_audit", lambda: _write(stage_audit(audit, cfg), cfg, "call_audit") if audit is not None else 0)
    if audit is not None:
        audit.unpersist()
    manifest = {"created_at": datetime.now(timezone.utc).isoformat(), "date_from": str(cfg.date_from) if cfg.date_from else None,
                "date_to": str(cfg.date_to) if cfg.date_to else None, "row_counts": counts,
                "audit_available": audit_available, "silver_pointer_sha": snapshot_sha(snap),
                "silver_pointer": {n: d for n, d in snap.items() if d},
                "files_since": cfg.files_since, "files_until": cfg.files_until,
                "files_read": files_read,
                "episode_window": [str(_lookback_cfg(cfg).date_from) if cfg.date_from else None,
                                   str(cfg.date_to) if cfg.date_to else None],
                "elapsed_sec": round(time.monotonic() - t0, 1), "elapsed_by_table": elapsed}
    Path(cfg.stage_root).mkdir(parents=True, exist_ok=True)
    (Path(cfg.stage_root) / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", type=date.fromisoformat)
    ap.add_argument("--to", dest="d1", type=date.fromisoformat)
    ap.add_argument("--files-since", type=float, default=None, help="증분: 파일 수정 시각 하한(초과, epoch 초)")
    ap.add_argument("--files-until", type=float, default=None, help="증분: 파일 수정 시각 상한(이하, epoch 초)")
    a = ap.parse_args(argv)
    cfg = GoldConfig.from_env(a.d0, a.d1, files_since=a.files_since, files_until=a.files_until)
    spark = (SparkSession.builder.appName("urbanstream-gold-stage").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    m = run(spark, cfg)
    print(json.dumps(m, ensure_ascii=False, indent=2))
    if not m["audit_available"]:
        print("경고: Bronze audit 이 없어 돌발 absent_since 를 계산하지 못했습니다 (전부 NULL)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
