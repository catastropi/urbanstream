"""Bronze 프로파일링 (Step 5-0) — Silver 규칙(워터마크, 중복, 0값/결측, 돌발 처리)을 가정이 아니라 데이터로 정하기 위한 읽기 전용 측정.

Bronze 를 읽기만 한다 (아무것도 쓰지 않는다. 결과 JSON 만 `PROFILE_OUT_DIR`(기본 /data/profile)이 있으면 거기에 저장).
결과는 ADR-0007(0값/결측 정책)과 Silver 워터마크 값의 근거가 된다. 숫자의 의미와 한계는 각 함수 설명에 적었다.

실행: scripts/silver_profile.sh   (docs/runbooks/step5-silver.md)

주의 (측정의 한계)
  - Bronze 의 `ingested_at` 은 Bronze 가 처리한 시각이다. 처음 시작할 때 Kafka 에 쌓여 있던 구간(백필)과 재시작 직후 구간은 몇 시간 늦게 찍혀서
    `ingested_at - fetched_at` 꼬리를 부풀린다 → 파이프라인 지연은 분포(구간별 건수)로 보고, 워터마크의 근거는 `lateness`(아래)로 삼는다.
  - 표본이 작으면(몇 시간) 드문 일은 안 보인다. 숫자와 함께 표본 크기(rows, 기간)를 항상 같이 본다.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from streaming.bronze_config import BronzeConfig, load_tables
from streaming.verify_bronze import read_bronze

KST = "Asia/Seoul"
PCTS = [0.5, 0.95, 0.99]


# ---------------------------------------------------------------- 공통 도구

def _secs(a, b):
    """두 timestamp 열의 차이(초, a - b)."""
    return (F.unix_micros(a) - F.unix_micros(b)) / 1e6


def _num(v):
    if v is None:
        return None
    return round(float(v), 3)


def stats(df: DataFrame, col) -> dict:
    """숫자 열의 n / min / p50 / p95 / p99 / max (NULL 제외). 퍼센타일은 근사값."""
    c = F.col(col) if isinstance(col, str) else col
    r = df.select(c.alias("v")).where(F.col("v").isNotNull()).agg(
        F.count("*").alias("n"), F.min("v").alias("mn"), F.max("v").alias("mx"),
        F.percentile_approx("v", PCTS, 10000).alias("p")).collect()[0]
    p = r["p"] or [None, None, None]
    return {"n": r["n"], "min": _num(r["mn"]), "p50": _num(p[0]), "p95": _num(p[1]), "p99": _num(p[2]), "max": _num(r["mx"])}


def _counts(df: DataFrame, col, limit: int = 10) -> dict:
    rows = df.groupBy(col).count().orderBy(F.desc("count")).limit(limit).collect()
    return {str(r[0]): r["count"] for r in rows}


# ---------------------------------------------------------------- 모든 소스 공통 측정

def coverage(df: DataFrame) -> dict:
    """행 수, 파싱 실패, 수집 기간(KST). 수집 공백은 hourly 에서 본다."""
    kst = lambda c: F.date_format(F.from_utc_timestamp(c, KST), "yyyy-MM-dd HH:mm:ss")
    r = df.agg(F.count("*").alias("rows"), F.sum(F.when(~F.col("_parse_ok"), 1).otherwise(0)).alias("bad"),
               kst(F.min("fetched_at")).alias("lo"), kst(F.max("fetched_at")).alias("hi")).collect()[0]
    out = {"rows": r["rows"], "parse_failed": int(r["bad"] or 0), "fetched_first_kst": r["lo"], "fetched_last_kst": r["hi"]}
    if out["parse_failed"]:
        out["parse_failed_by_reason"] = _counts(df.where(~F.col("_parse_ok")), "_parse_error")
    return out


def hourly(df: DataFrame) -> dict:
    """KST 시간대별 수집 행 수 (fetched_at 기준 — 실제로 호출한 시각). 비어 있는 시간대 = 수집 공백."""
    h = F.date_format(F.from_utc_timestamp("fetched_at", KST), "yyyy-MM-dd HH")
    rows = df.groupBy(h.alias("h")).count().orderBy("h").collect()
    return {r["h"]: r["count"] for r in rows}


def lateness(df: DataFrame) -> dict:
    """워터마크의 근거: 각 행이 '그 행보다 먼저 호출된 행들 중 가장 늦은 event_time' 보다 얼마나 뒤처졌나(초, 0 이면 순서대로).

    도착 순서는 fetched_at(실제 호출 시각)으로 본다 — Bronze 의 처리 시각(백필/재시작)에 영향을 받지 않는다. 같은 호출의 행끼리는 같은 순서(동률)로 본다.
    워터마크 지연은 이 값의 꼬리(p99, max)보다 커야 한다. event_time 이 발생 시각인 돌발에는 의미가 없다(쓰지 않는다).
    """
    w = Window.orderBy("ft").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    x = (df.select(F.unix_micros("event_time").alias("et"), F.unix_micros("fetched_at").alias("ft"))
         .withColumn("run_max", F.max("et").over(w))
         .withColumn("late", (F.col("run_max") - F.col("et")) / 1e6))
    out = stats(x, "late")
    out["rows_late_gt_0"] = x.where(F.col("late") > 0).count()
    return out


def source_age(df: DataFrame) -> dict:
    """fetched_at - event_time (초): 사건이 일어난 뒤 우리가 받기까지 걸린 시간. 버스위치는 GPS 나이, 도로소통은 0~59초(분 절삭)."""
    return stats(df, _secs(F.col("fetched_at"), F.col("event_time")))


def pipeline_delay(df: DataFrame) -> dict:
    """ingested_at - fetched_at (초) 구간별 건수. 꼬리(>10분)는 Bronze 백필/재시작 구간이 대부분이다 (모듈 설명 참고)."""
    d = _secs(F.col("ingested_at"), F.col("fetched_at"))
    r = df.agg(F.count("*").alias("n"),
               F.sum(F.when(d <= 120, 1).otherwise(0)).alias("le_2m"),
               F.sum(F.when((d > 120) & (d <= 600), 1).otherwise(0)).alias("2m_10m"),
               F.sum(F.when(d > 600, 1).otherwise(0)).alias("gt_10m")).collect()[0]
    return {"rows": r["n"], "le_2min": int(r["le_2m"] or 0), "2_to_10min": int(r["2m_10m"] or 0), "gt_10min": int(r["gt_10m"] or 0),
            "stats_sec": stats(df, d)}


def duplicates(df: DataFrame) -> dict:
    """dedup_key 중복. same_call = 한 호출 안의 중복(파서/계약 문제 신호), cross_call = 같은 사건을 여러 호출에서 본 것(Silver dedup 이 없애는 양)."""
    r = df.agg(F.count("*").alias("rows"), F.countDistinct("dedup_key").alias("keys"),
               F.countDistinct("dedup_key", "ingest_id").alias("key_call")).collect()[0]
    return {"rows": r["rows"], "distinct_keys": r["keys"], "same_call_dups": r["rows"] - r["key_call"],
            "cross_call_dups": r["key_call"] - r["keys"]}


def time_anomalies(df: DataFrame) -> dict:
    """시간 이상 건수. future_event: event_time 이 fetched_at 보다 60초 넘게 미래 / event_after_ingest: event_time > ingested_at /
    fetched_after_ingest: fetched_at > ingested_at + 1초 (시계 어긋남) / event_older_than_1d: 사건이 하루 넘게 전 (돌발·기상에서는 정상일 수 있다)."""
    r = df.agg(
        F.sum(F.when(_secs(F.col("event_time"), F.col("fetched_at")) > 60, 1).otherwise(0)).alias("future"),
        F.sum(F.when(_secs(F.col("event_time"), F.col("ingested_at")) > 0, 1).otherwise(0)).alias("after_ing"),
        F.sum(F.when(_secs(F.col("fetched_at"), F.col("ingested_at")) > 1, 1).otherwise(0)).alias("fetch_after"),
        F.sum(F.when(_secs(F.col("fetched_at"), F.col("event_time")) > 86400, 1).otherwise(0)).alias("old")).collect()[0]
    return {"future_event": int(r["future"] or 0), "event_after_ingest": int(r["after_ing"] or 0),
            "fetched_after_ingest": int(r["fetch_after"] or 0), "event_older_than_1d": int(r["old"] or 0)}


def per_call(df: DataFrame) -> dict:
    """호출 1회당 행 수 분포."""
    return stats(df.groupBy("ingest_id").count(), "count")


# ---------------------------------------------------------------- 소스별 측정

def traffic(df: DataFrame) -> dict:
    """도로소통: speed_kmh = 0 의 비율 등. 0 이 정체인지 결측인지는 이 숫자만으로 단정하지 않는다 (ADR-0007 에서 근거와 함께 정한다)."""
    r = df.agg(F.count("*").alias("n"),
               F.sum(F.when(F.col("payload.speed_kmh") == 0, 1).otherwise(0)).alias("zero"),
               F.sum(F.when(F.col("payload.speed_kmh").isNull(), 1).otherwise(0)).alias("null"),
               F.sum(F.when(F.col("payload.travel_time_sec") == 0, 1).otherwise(0)).alias("tt_zero"),
               F.countDistinct("payload.link_id").alias("links")).collect()[0]
    return {"rows": r["n"], "links": r["links"], "speed_zero": int(r["zero"] or 0), "speed_null": int(r["null"] or 0),
            "travel_time_zero": int(r["tt_zero"] or 0), "speed_kmh": stats(df, "payload.speed_kmh")}


def position(df: DataFrame) -> dict:
    """버스위치: 같은 차량의 보고 시각(dataTm=event_time)이 호출 사이에 거꾸로 가거나(regress) 제자리(same)인 쌍, 좌표 범위."""
    x = df.select("payload.veh_id", "event_time", "fetched_at", "payload.lon", "payload.lat")
    w = Window.partitionBy("veh_id").orderBy(F.unix_micros("fetched_at"))
    x = x.withColumn("prev", F.lag("event_time").over(w))
    d = _secs(F.col("event_time"), F.col("prev"))
    r = x.where(F.col("prev").isNotNull()).agg(
        F.count("*").alias("pairs"), F.sum(F.when(d < 0, 1).otherwise(0)).alias("regress"),
        F.sum(F.when(d == 0, 1).otherwise(0)).alias("same"), F.min(d).alias("min_delta")).collect()[0]
    c = x.agg(F.min("lon").alias("lon0"), F.max("lon").alias("lon1"), F.min("lat").alias("lat0"), F.max("lat").alias("lat1"),
              F.countDistinct("veh_id").alias("vehicles")).collect()[0]
    return {"vehicles": c["vehicles"], "consecutive_pairs": r["pairs"], "regress_pairs": int(r["regress"] or 0),
            "same_event_time_pairs": int(r["same"] or 0), "min_delta_sec": _num(r["min_delta"]),
            "delta_sec": stats(x.where(F.col("prev").isNotNull()), d),
            "lon_range": [_num(c["lon0"]), _num(c["lon1"])], "lat_range": [_num(c["lat0"]), _num(c["lat1"])]}


def arrival(df: DataFrame) -> dict:
    """버스도착: 호출당 행 수, 한 호출의 mkTm 이 하나인지, arrive_in_sec = 0 의 모습(arrival_msg 와 함께), arrive_code 분포."""
    per = df.groupBy("ingest_id").agg(F.countDistinct("event_time").alias("mk"))
    zero = df.where(F.col("payload.arrive_in_sec") == 0)
    return {"calls": per.count(), "max_distinct_mkTm_in_a_call": per.agg(F.max("mk")).collect()[0][0],
            "arrive_in_sec": stats(df, "payload.arrive_in_sec"),
            "arrive_in_sec_zero": zero.count(), "arrive_in_sec_null": df.where(F.col("payload.arrive_in_sec").isNull()).count(),
            "arrival_msg_when_zero_top": _counts(zero, "payload.arrival_msg", 5),
            "arrive_code": _counts(df, "payload.arrive_code"), "speed": stats(df, "payload.speed")}


def incident(df: DataFrame) -> dict:
    """돌발: 사건(acc_id)별 관측 횟수와 내용 해시 변화, 유형, 지속 기간(해제 예정 - 발생) 구간, 발생 후 경과(관측 시점 - 발생)."""
    per = df.groupBy("payload.acc_id").agg(
        F.count("*").alias("obs"), F.countDistinct("payload.content_hash").alias("hashes"),
        F.first("payload.acc_type").alias("acc_type"), F.min("payload.occurred_at").alias("occ"),
        F.max("payload.expected_clear_at").alias("clr"), F.min("fetched_at").alias("first_seen"))
    dur = _secs(F.col("clr"), F.col("occ")) / 86400
    bucket = (F.when(F.col("clr").isNull(), "해제예정없음").when(dur <= 1, "<=1일").when(dur <= 7, "1~7일")
              .when(dur <= 30, "7~30일").otherwise(">30일"))
    age = _secs(F.col("first_seen"), F.col("occ")) / 86400
    age_bucket = F.when(age <= 1, "<=1일").when(age <= 7, "1~7일").when(age <= 30, "7~30일").otherwise(">30일")
    n = per.count()
    return {"observations": df.count(), "distinct_incidents": n,
            "incidents_with_changed_content": per.where(F.col("hashes") > 1).count(),
            "max_content_versions_of_one_incident": per.agg(F.max("hashes")).collect()[0][0],
            "observations_per_incident": stats(per, "obs"), "rows_per_call": per_call(df),
            "by_acc_type": _counts(per, "acc_type"), "planned_duration": _counts(per.withColumn("b", bucket), "b"),
            "age_at_first_seen": _counts(per.withColumn("b", age_bucket), "b")}


def weather(df: DataFrame) -> dict:
    """기상: 카테고리별 건수·값 범위, 숫자로 못 읽은 값(value 가 NULL 인데 원문이 있는 것)의 예, RN1 원문 값의 모양."""
    unreadable = df.where(F.col("payload.value").isNull() & F.col("payload.value_raw").isNotNull())
    rn1 = df.where(F.col("payload.category") == "RN1")
    cats = {r["c"]: {"n": r["n"], "min": _num(r["mn"]), "max": _num(r["mx"])} for r in
            df.groupBy(F.col("payload.category").alias("c")).agg(F.count("*").alias("n"), F.min("payload.value").alias("mn"),
                                                                   F.max("payload.value").alias("mx")).collect()}
    return {"rows": df.count(), "distinct_base_times": df.select("event_time").distinct().count(), "categories": cats,
            "unreadable_value_rows": unreadable.count(),
            "unreadable_examples": [f"{r['c']}={r['v']}" for r in
                                    unreadable.select(F.col("payload.category").alias("c"), F.col("payload.value_raw").alias("v")).distinct().limit(5).collect()],
            "RN1_raw_values": _counts(rn1, "payload.value_raw", 10)}


SECTIONS = {  # 소스 → 소스별 측정 함수
    "seoul_traffic": traffic, "bus_position": position, "bus_arrival": arrival, "seoul_incident": incident, "kma_ncst": weather,
}
NO_LATENESS = {"seoul_incident"}   # event_time 이 발생 시각이라 도착 순서와 무관 (lateness 가 의미 없다)


# ---------------------------------------------------------------- 조립

def profile_table(df: DataFrame, source: str) -> dict:
    """정상 파싱된 행(`_parse_ok`)만으로 측정한다. 섹션마다 오류를 격리해서 한 섹션의 실패가 나머지를 가리지 않는다."""
    ok = df.where(F.col("_parse_ok"))
    plan = {"coverage": lambda: coverage(df), "hourly": lambda: hourly(ok), "source_age_sec": lambda: source_age(ok),
            "pipeline_delay": lambda: pipeline_delay(ok), "duplicates": lambda: duplicates(ok),
            "time_anomalies": lambda: time_anomalies(ok), "rows_per_call": lambda: per_call(ok),
            "source_specific": lambda: SECTIONS[source](ok)}
    if source not in NO_LATENESS:
        plan["lateness_sec"] = lambda: lateness(ok)
    out: dict = {}
    for name, fn in plan.items():
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001
            out[name] = {"error": f"{type(e).__name__}: {(str(e).strip().splitlines() or [''])[0][:200]}"}
    return out


def profile_all(spark: SparkSession, cfg: BronzeConfig) -> dict:
    report: dict = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "bronze_root": cfg.bronze_root, "tables": {}}
    for t in load_tables(cfg.config_dir):
        if t.source_system is None:        # dlq, audit: 계약 스키마가 없다 (Step 8 에서)
            continue
        df = read_bronze(spark, f"{cfg.bronze_root}/{t.name}")
        report["tables"][t.name] = {"rows": 0} if df is None else profile_table(df, t.source_system)
    return report


def format_report(obj, indent: int = 0) -> str:
    """사람이 읽는 텍스트. 사전은 들여쓰기, 짧은 값은 한 줄."""
    pad = "  " * indent
    lines = []
    for k, v in obj.items():
        if isinstance(v, dict) and any(isinstance(x, (dict, list)) for x in v.values()):
            lines.append(f"{pad}{k}:")
            lines.append(format_report(v, indent + 1))
        elif isinstance(v, dict):
            lines.append(f"{pad}{k}: " + ", ".join(f"{a}={b}" for a, b in v.items()))
        else:
            lines.append(f"{pad}{k}: {v}")
    return "\n".join(lines)


def save_json(report: dict, out_dir: Optional[str]) -> Optional[str]:
    if not out_dir or not Path(out_dir).is_dir():
        return None
    path = Path(out_dir) / f"bronze_profile_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(path)


def main() -> int:
    cfg = BronzeConfig.from_env()
    spark = (SparkSession.builder.appName("urbanstream-bronze-profile")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    report = profile_all(spark, cfg)
    print("\n=== Bronze 프로파일 (읽기 전용) ===")
    for name, sec in report["tables"].items():
        print(f"\n##### {name}")
        print(format_report(sec))
    saved = save_json(report, os.environ.get("PROFILE_OUT_DIR", "/data/profile"))
    print("\n저장:", saved or "(저장 폴더 없음 — 화면 출력만)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
