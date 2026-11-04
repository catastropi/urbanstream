"""Gold 스테이징 (Spark 배치): Silver + Bronze audit → data/gold_stage/<표>/ Parquet + manifest.json.

DB 에 쓰지 않는다. 적재는 batch/gold_load.py (호스트 Python). 스트리밍이 쓰는 폴더(Silver/Bronze)는 읽기만 한다.
같은 날짜 범위로 다시 돌려도 결과가 같다(스테이징 폴더를 통째로 덮어씀). 돌발 수명(episode)은 구간과 무관하게 전체 이력으로 계산한다.

사용: python -m batch.gold_stage --from 2026-11-03 --to 2026-11-04
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from batch.gold_config import GoldConfig
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


def stage_episodes(obs: DataFrame, audit: Optional[DataFrame]) -> DataFrame:
    """돌발 수명. absent_since = 마지막 관측 뒤의 **성공·비절단** 돌발 호출 중 첫 시각 (벽시계 아님, ADR-0008 ⑧).
    audit 이 없으면 모든 행이 absent_since NULL(= 아직 목록에 있음)이 되므로 호출 쪽에서 audit 없음을 경고한다."""
    o = obs.filter(F.col("occurred_at").isNotNull())
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

def _write(df: DataFrame, cfg: GoldConfig, name: str) -> int:
    path = f"{cfg.stage_root}/{name}"
    df.write.mode("overwrite").parquet(path)
    return df.sparkSession.read.parquet(path).count()


def run(spark: SparkSession, cfg: GoldConfig) -> dict:
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")   # 호스트 pyarrow 가 UTC 로 읽도록
    silver = {n: read_bronze(spark, f"{cfg.silver_root}/{n}") for n in
              ("silver_traffic_speed", "silver_bus_position", "silver_bus_arrival", "silver_incident_observation",
               "silver_weather")}
    audit_raw = read_bronze(spark, f"{cfg.bronze_root}/audit")
    audit = parse_audit(audit_raw) if audit_raw is not None else None
    counts: dict[str, int] = {}
    plan = [("traffic_speed", "silver_traffic_speed", stage_traffic), ("bus_position", "silver_bus_position", stage_position),
            ("bus_arrival", "silver_bus_arrival", stage_arrival),
            ("incident_observation", "silver_incident_observation", stage_incident_obs),
            ("weather_obs", "silver_weather", stage_weather)]
    for out, src, fn in plan:
        counts[out] = _write(fn(silver[src], cfg), cfg, out) if silver[src] is not None else 0
    inc = silver["silver_incident_observation"]
    counts["incident_episode"] = _write(stage_episodes(inc, audit), cfg, "incident_episode") if inc is not None else 0
    counts["call_audit"] = _write(stage_audit(audit, cfg), cfg, "call_audit") if audit is not None else 0
    manifest = {"created_at": datetime.now(timezone.utc).isoformat(), "date_from": str(cfg.date_from) if cfg.date_from else None,
                "date_to": str(cfg.date_to) if cfg.date_to else None, "row_counts": counts,
                "audit_available": audit is not None}
    Path(cfg.stage_root).mkdir(parents=True, exist_ok=True)
    (Path(cfg.stage_root) / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", type=date.fromisoformat)
    ap.add_argument("--to", dest="d1", type=date.fromisoformat)
    a = ap.parse_args(argv)
    cfg = GoldConfig.from_env(a.d0, a.d1)
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
