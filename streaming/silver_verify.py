"""Silver 검증 잡 (Step 5, D-08) — Bronze 에서 Silver 로 오면서 행이 사라지거나 생기지 않았는지 대조한다.

행 수 등식(표마다):   Bronze 전체 = 읽을 수 없는 행(`_parse_error`, 사유별) + 정상 행
                     정상 행의 서로 다른 키 = Silver 의 서로 다른 키        (키 = dedup_key, 돌발은 dedup_key + ingest_id)
  - 유실(missing): Bronze 정상 행에는 있는데 Silver 에 없는 키 → 0 이어야 함
  - 초과(extra)  : Silver 에는 있는데 Bronze 정상 행에 없는 키 → 0 이어야 함
  - 돌발은 Silver 가 관측 로그라서 같은 (dedup_key, ingest_id) 가 두 번 있으면 실패. 다른 표의 재통과(워터마크가 지난 뒤 같은 키가 다시 들어옴)는
    정보로만 보여 준다 (ADR-0006 7-B: 워터마크 dedup 의 알려진 한계).
Silver 는 Bronze 를 뒤따라가므로, Silver 가 처리한 마지막 `fetched_at` 에서 15분을 뺀 시각까지만 비교한다 (그 뒤는 아직 처리 중일 수 있음).
실패하면 종료 코드 1. 쓰기 잡이 도는 중에도 실행할 수 있다.
비교 구간: 기본은 전체 이력. SILVER_VERIFY_LOOKBACK_DAYS(최근 N일) 또는 SILVER_VERIFY_SINCE(YYYY-MM-DD, KST)를 주면 그 이후만 읽는다
(파티션 날짜로 먼저 걸러 읽는 파일 수가 줄어든다). Airflow 의 silver_reconcile_daily 는 기본 1일로 돌린다 (D-258).

실행: scripts/silver_verify.sh   (docs/runbooks/step5-silver.md)
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from streaming.silver_config import SilverConfig, SilverTable, load_silver_tables, verify_since
from streaming.verify_bronze import read_bronze

MARGIN_SEC = 900
_T0 = time.monotonic()


def _step(msg: str) -> None:
    """진행 로그. stderr 로만 낸다(stdout 은 dq_jobs 가 표별 OK/FAIL 줄을 읽는 곳). Spark 로그 수준이 ERROR 라 이게 없으면 오래 걸릴 때 멈춘 것과 구별이 안 된다."""
    print(f"[{time.monotonic() - _T0:7.1f}s] {msg}", file=sys.stderr, flush=True)


def bronze_partition_col(table: SilverTable) -> Optional[str]:
    """Bronze 를 파티션으로 먼저 거를 때 쓸 열. Bronze 의 event_date 는 '사건 시각' 날짜라서, 돌발은 발생한 지 오래된 장기 돌발이
    최근 관측에 계속 섞여 있다(2026-11-19 Mac 실측: Silver 초과 574 가 이것이었다). 돌발은 파티션으로 거르지 않고 fetched_at 으로만 자른다."""
    return None if table.source == "seoul_incident" else "event_date"


def windowed(df: Optional[DataFrame], partition_col: Optional[str], since: Optional[datetime]) -> Optional[DataFrame]:
    """since 이후 행만 남긴다. 파티션 날짜(KST, 하루 여유)로 먼저 걸러서 읽는 파일 수를 줄이고, fetched_at 으로 정확히 자른다.
    partition_col 이 None 이면 파티션으로는 거르지 않는다."""
    if df is None or since is None:
        return df
    if partition_col is None:
        return df.filter(F.col("fetched_at") >= F.lit(since))
    first_day = (since + timedelta(hours=9)).date() - timedelta(days=1)
    return df.filter(F.col(partition_col) >= F.lit(first_day)).filter(F.col("fetched_at") >= F.lit(since))


def cached(df: Optional[DataFrame], since: Optional[datetime]) -> Optional[DataFrame]:
    """구간을 준 경우에만 메모리(모자라면 디스크)에 올려 둔다. 같은 작은 파일들을 액션마다 다시 여는 것이 시간을 가장 많이 먹는다(D-258).
    구간이 없으면(전체 이력) 크기를 알 수 없으니 올리지 않는다."""
    return df.persist(StorageLevel.MEMORY_AND_DISK) if df is not None and since is not None else df


def compare(bronze: DataFrame, silver: Optional[DataFrame], table: SilverTable, margin_sec: int = MARGIN_SEC,
            since: Optional[datetime] = None, prewindowed: bool = False) -> dict:
    """prewindowed=True 면 bronze/silver 가 이미 since 로 잘려 있다고 보고 다시 자르지 않는다(main 이 같은 구간을 시간 검사에도 쓰려고 미리 자른다)."""
    keys = list(table.dedup_keys)
    if not prewindowed:
        bronze = windowed(bronze, bronze_partition_col(table), since)
        silver = windowed(silver, table.partition_col, since)
    ok_rows = bronze.filter(F.col("_parse_ok"))
    reasons = {r["_parse_error"]: r["n"] for r in
               bronze.filter(~F.col("_parse_ok")).groupBy("_parse_error").agg(F.count("*").alias("n")).collect()}
    out = {"table": table.name, "bronze_total": bronze.count(), "parse_failed": reasons, "bronze_ok": ok_rows.count(),
           "silver_rows": 0, "missing": 0, "extra": 0, "dup_rows": 0, "cutoff": None, "note": "", "since": since}
    out["bronze_total_check"] = out["bronze_total"] == out["bronze_ok"] + sum(reasons.values())

    if silver is None or silver.limit(1).count() == 0:
        out["ok"] = out["bronze_ok"] == 0
        out["note"] = "Silver 비어 있음" + (" (Bronze 에 정상 행이 있으므로 쓰기 잡이 안 돌았거나 첫 배치 전)" if not out["ok"] else "")
        return out

    last = silver.agg(F.max("fetched_at")).first()[0]
    cutoff = last - timedelta(seconds=margin_sec)
    out["cutoff"] = cutoff
    b = ok_rows.filter(F.col("fetched_at") <= F.lit(cutoff))
    s = silver.filter(F.col("fetched_at") <= F.lit(cutoff))
    b_keys, s_keys = b.select(*keys).distinct(), s.select(*keys).distinct()
    out["silver_rows"] = s.count()
    out["missing"] = b_keys.join(s_keys, keys, "left_anti").count()
    out["extra"] = s_keys.join(b_keys, keys, "left_anti").count()
    out["dup_rows"] = out["silver_rows"] - s_keys.count()
    dup_is_failure = table.source == "seoul_incident"
    out["ok"] = out["bronze_total_check"] and out["missing"] == 0 and out["extra"] == 0 and not (dup_is_failure and out["dup_rows"] > 0)
    return out


def time_checks(silver: DataFrame, source: str) -> dict:
    """Silver 출력의 시간 검사 (HANDOFF 7-5 완료 기준, D-45). 둘 다 0 이어야 한다.
    future_event: event_time 이 fetched_at 보다 60초 넘게 뒤 (미래 시각). reversal: 버스위치에서 같은 차량의 GPS 시각이 수집 순서대로 거꾸로 감."""
    future = silver.filter(F.col("event_time") > F.col("fetched_at") + F.expr("INTERVAL 60 SECONDS")).count()
    reversal = 0
    if source == "bus_position":
        w = Window.partitionBy("veh_id").orderBy(F.unix_micros("fetched_at"), F.unix_micros("event_time"))
        reversal = (silver.select("veh_id", "event_time", "fetched_at")
                    .withColumn("prev", F.lag("event_time").over(w))
                    .filter(F.col("prev").isNotNull() & (F.col("event_time") < F.col("prev"))).count())
    return {"future_event": future, "reversal": reversal}


def incident_sample(silver: DataFrame, n: int = 6) -> list[dict]:
    """돌발 변환 좌표를 설명문과 나란히 본다 (D-46, 사람이 눈으로 확인하는 용도 — 통과/실패에 쓰지 않는다). 기간이 긴 것부터."""
    rows = (silver.groupBy("acc_id").agg(F.first("description").alias("description"), F.first("lon").alias("lon"), F.first("lat").alias("lat"),
                                         F.max("planned_duration_days").alias("days"), F.first("is_long_term").alias("long"))
            .orderBy(F.col("days").desc_nulls_last()).limit(n).collect())
    return [r.asDict() for r in rows]


def _since_note(r: dict) -> str:
    return f" | 구간 fetched_at ≥ {r['since']:%Y-%m-%d %H:%M:%S}Z" if r.get("since") else " | 구간 전체 이력"


def format_line(r: dict) -> str:
    if r["cutoff"] is None:
        return f"{r['table']:<28} {'OK  ' if r['ok'] else 'FAIL'} Bronze {r['bronze_total']:>8,}행 (정상 {r['bronze_ok']:,}) · {r['note']}" + _since_note(r)
    pf = ", ".join(f"{k}={v:,}" for k, v in sorted(r["parse_failed"].items())) or "0"
    kind = "중복(실패)" if r["table"] == "silver_incident_observation" else "재통과(정보)"
    return (f"{r['table']:<28} {'OK  ' if r['ok'] else 'FAIL'} Bronze {r['bronze_total']:>8,}행 = 정상 {r['bronze_ok']:,} + 읽기불가 {pf}"
            f" | Silver {r['silver_rows']:>8,}행  유실 {r['missing']}  초과 {r['extra']}  {kind} {r['dup_rows']}"
            f" | 비교 기준 fetched_at ≤ {r['cutoff']:%Y-%m-%d %H:%M:%S}Z" + _since_note(r))


def main() -> int:
    cfg = SilverConfig.from_env()
    since = verify_since()
    spark = (SparkSession.builder.appName("urbanstream-silver-verify")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    lines = []
    for t in load_silver_tables(cfg.config_dir):
        cache_used: list = []
        try:
            b = read_bronze(spark, f"{cfg.bronze_root}/{t.source}")
            if b is None:
                lines.append({"ok": True, "text": f"{t.name:<28} OK   Bronze 비어 있음"})
                continue
            silver = read_bronze(spark, f"{cfg.silver_root}/{t.name}")
            # 한 번 자르고 한 번만 읽는다. 시간 검사·돌발 표본도 같은 구간을 쓴다 (자르지 않으면 구간을 주어도 Silver 전체 이력을 읽는다. D-258).
            b_w = cached(windowed(b, bronze_partition_col(t), since), since)
            silver_w = cached(windowed(silver, t.partition_col, since), since)
            cache_used = [x for x in (b_w, silver_w) if x is not None and since is not None]  # 표마다 끝나면 반환
            _step(f"{t.name}: 행 수 대조 시작")
            r = compare(b_w, silver_w, t, since=since, prewindowed=True)
            _step(f"{t.name}: 행 수 대조 끝")
            text, ok = format_line(r), r["ok"]
            if silver is not None and r["cutoff"] is not None:
                tc = time_checks(silver_w, t.source)
                _step(f"{t.name}: 시간 검사 끝")
                ok = ok and tc["future_event"] == 0 and tc["reversal"] == 0
                text += f" | 미래 시각 {tc['future_event']} · 시간 역전 {tc['reversal']}" + ("" if ok else "  ← FAIL")
            lines.append({"ok": ok, "text": text})
            if silver is not None and t.source == "seoul_incident":
                for x in incident_sample(silver_w):
                    lines.append({"ok": True, "text": f"    [좌표 확인용] {x['acc_id']}  lon {x['lon']:.5f} lat {x['lat']:.5f}  {x['days'] if x['days'] is None else round(x['days'], 1)}일{' (장기)' if x['long'] else ''}  {(x['description'] or '')[:40]}"
                                  if x['lon'] is not None else f"    [좌표 확인용] {x['acc_id']}  좌표 없음  {(x['description'] or '')[:40]}"})
        except Exception as e:  # 한 표의 오류가 다른 표의 결과를 가리지 않게 한다
            first = (str(e).strip().splitlines() or [type(e).__name__])[0][:200]
            lines.append({"ok": False, "text": f"{t.name:<28} FAIL 검증 중 오류: {type(e).__name__}: {first}"})
        finally:
            for df in cache_used:
                df.unpersist()
    print("\n=== Silver 검증 결과 ===")
    for x in lines:
        print(x["text"])
    ok = all(x["ok"] for x in lines)
    print("\n전체:", "통과" if ok else "실패 (위 FAIL 줄 확인)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
