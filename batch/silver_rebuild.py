"""Bronze → Silver 재처리 (STEP 1, docs/adr/0015-reprocess.md). 지난 날짜의 Silver 를 고친 규칙으로 다시 만들고, 날짜별 포인터로 바꿔 끼운다.

하위 명령
  build     Bronze(날짜 범위) → 스트리밍과 같은 transform → 결정적 중복 제거 → data/silver_rebuild/<표>/<버전>/   (Spark)
            같은 잡 안에서 대조한다: Bronze 정상 키 = 새 Silver 키, 현재 Silver 와 비교(추가·삭제·값 변경), 축소 가드.
            결과는 표마다 <버전>/_build.json. status=ok 가 아니면 게시할 수 없다.
  publish   _active/<표>.json 의 날짜를 새 버전으로 바꾼다 (파일 하나 이름 바꾸기, Spark 없음). 바꾸기 전 값을 돌려준다.
  rollback  게시 전 값으로 되돌린다 (--reprocess-id 의 게시 기록을 포인터 이력에서 찾는다). 버전 폴더는 남아 있다.
  prune     포인터가 가리키지 않는 72시간 지난 옛 버전 폴더를 지운다 (표마다 최근 2개는 남김).
  status    포인터가 가리키는 날짜·버전을 보여 준다.

스트리밍 폴더(data/silver, _spark_metadata, 체크포인트)는 읽기만 한다. spark-silver 를 켜 둔 채 돌릴 수 있다.
DB 에는 쓰지 않는다 (Spark 컨테이너에 비밀을 주지 않음, ADR-0008 ⑤). 실행 이력(ops.reprocess_run)은 Airflow 쪽 batch/reprocess.py 가 기록한다.

사용 (gold-runner 컨테이너 안, 또는 Java·pyspark 가 있는 곳)
  python -m batch.silver_rebuild build --tables silver_traffic_speed --from 2026-11-10 --to 2026-11-10 --version m1 --report /tmp/r.json
  python -m batch.silver_rebuild publish --tables silver_traffic_speed --from 2026-11-10 --to 2026-11-10 --version m1
  python -m batch.silver_rebuild rollback --tables silver_traffic_speed --reprocess-id 17
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:                                   # spark-submit 으로 파일을 직접 실행할 때
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from streaming import silver_pointer as P  # noqa: E402

READ_MODES = ("listing", "sink_log")
DEFAULT_REBUILD_ROOT = "/data/silver_rebuild"
FAIL_ENV = "SILVER_REBUILD_FAIL_AFTER"          # 시험용 실패 주입: 이 표를 쓴 직후 예외 (운영에서는 쓰지 않는다)


class GuardError(RuntimeError):
    """대조·축소 가드에 걸려 게시하면 안 되는 결과."""


def config_sha(config_dir: str) -> str:
    return hashlib.sha256((Path(config_dir) / "silver.yaml").read_bytes()).hexdigest()[:16]


# ------------------------------------------------------------------ 입력 (Bronze)

def bronze_dirs(bronze_root: str, source: str, days: list[date], *, incident: bool) -> list[str]:
    """읽을 Bronze 파티션 폴더. 돌발은 Bronze 파티션(발생 날짜)과 Silver 파티션(관측 날짜)이 달라서 마지막 날 이전의 모든 폴더를 본다."""
    root = Path(bronze_root) / source
    if not root.is_dir():
        return []
    if incident:
        last = max(days).isoformat()
        return sorted(str(p) for p in root.glob("event_date=*") if p.is_dir() and p.name.split("=", 1)[1] <= last)
    return [str(root / f"event_date={d.isoformat()}") for d in days if (root / f"event_date={d.isoformat()}").is_dir()]


def read_input(spark, cfg, table, days: list[date], mode: str):
    """(Bronze DataFrame | None, 파일 통계). listing = 파티션 폴더 직접 나열(sink 로그 밖 파일도 봄, D-258), sink_log = 루트 읽기(스트리밍과 같은 시야)."""
    from pyspark.sql import functions as F
    from streaming.silver import bronze_schema
    from streaming.verify_bronze import read_bronze
    incident = table.source == "seoul_incident"
    stats = {"bronze_read_mode": mode, "files_listed": None}
    if mode == "listing":
        dirs = bronze_dirs(cfg.bronze_root, table.source, days, incident=incident)
        if not dirs:
            return None, stats
        stats["files_listed"] = sum(1 for d in dirs for _ in Path(d).glob("*.parquet"))
        df = (spark.read.schema(bronze_schema(spark, table.source)).option("basePath", f"{cfg.bronze_root}/{table.source}")
              .option("ignoreCorruptFiles", "true").parquet(*dirs))
    elif mode == "sink_log":
        df = read_bronze(spark, f"{cfg.bronze_root}/{table.source}")
        if df is None:
            return None, stats
        df = df.filter(F.col("event_date") <= F.lit(max(days))) if incident else df.filter(F.col("event_date").isin(days))
    else:
        raise ValueError(f"bronze_read_mode 는 {READ_MODES} 중 하나: {mode!r}")
    return df, stats


def in_days(df, table, days: list[date]):
    """Silver 파티션 날짜로 자른다. 돌발은 관측 날짜(fetched_at 의 KST 날짜) = Silver 의 observed_date."""
    from pyspark.sql import functions as F
    from streaming.bronze import _kst_date
    col = _kst_date(F.col("fetched_at")) if table.source == "seoul_incident" and "observed_date" not in df.columns else F.col(table.partition_col)
    return df.filter(col.isin(days))


# ------------------------------------------------------------------ 변환 · 중복 제거

def batch_dedup(df, table):
    """스트리밍의 dropDuplicatesWithinWatermark 대신 결정적 버전: 키마다 (fetched_at, Kafka partition, offset) 가 가장 앞선 한 행.
    같은 키가 두 행이면 Kafka 메시지가 같은 것(실패한 배치가 남긴 파일)이라 내용도 같다."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    w = Window.partitionBy(*table.dedup_keys).orderBy(F.col("fetched_at"), F.col("_kafka_partition"), F.col("_kafka_offset"))
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def value_hash(df, keys):
    """키와 silver_processed_at(실행마다 다른 처리 시각)을 뺀 나머지 열의 지문."""
    from pyspark.sql import functions as F
    cols = sorted(c for c in df.columns if c != "silver_processed_at")
    return df.select(*keys, F.sha2(F.to_json(F.struct(*cols)), 256).alias("_h"))


def compare_with_current(new, current, table) -> dict:
    """현재 Silver(같은 날짜) 대비 추가·삭제·값 변경 키 수. current 는 키당 한 행으로 줄인 뒤 비교한다."""
    from pyspark.sql import functions as F
    keys = list(table.dedup_keys)
    if current is None:
        n = new.select(*keys).distinct().count()
        return {"silver_before": 0, "silver_before_keys": 0, "added": n, "removed": 0, "changed": 0}
    cur1 = batch_dedup(current, table)
    nh, ch = value_hash(new, keys), value_hash(cur1, keys)
    j = nh.alias("n").join(ch.alias("c"), keys, "inner")
    return {"silver_before": current.count(), "silver_before_keys": cur1.count(),
            "added": nh.join(ch, keys, "left_anti").count(), "removed": ch.join(nh, keys, "left_anti").count(),
            "changed": j.filter(F.col("n._h") != F.col("c._h")).count()}


# ------------------------------------------------------------------ build

def build_table(spark, cfg, table, days: list[date], *, rebuild_root: str, version: str, mode: str = "listing",
                allow_shrink: bool = False) -> dict:
    """표 하나를 버전 폴더에 쓰고 대조한 뒤 _build.json 을 남긴다. 반환 = _build.json 내용."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from streaming.silver import transform
    from streaming.silver_read import read_silver

    root = Path(rebuild_root)
    out_dir = P.version_dir(root, table.name, version)
    if version in P.active_dates(root, table.name).values():
        raise P.PointerError(f"{table.name}: {version} 는 이미 게시된 버전이라 다시 쓸 수 없습니다")
    bronze, stats = read_input(spark, cfg, table, days, mode)
    keys = list(table.dedup_keys)
    counts: dict = {"bronze_rows": 0, "bronze_parse_ok": 0, "silver_after": 0, "missing": 0, "extra": 0, **stats}
    per_day = {d.isoformat(): 0 for d in days}
    max_ing = None
    if bronze is not None:
        bronze = in_days(bronze, table, days)
        # Bronze 는 작은 파일이 많아 읽기 한 번이 비싸다. 파일 이름을 열로 붙여 한 번만 읽고 캐시한다(캐시 뒤에는 input_file_name 이 빈다).
        # 나열한 파일 수와 files_with_rows 가 다르면 그 차이는 빈 파일이거나 건너뛴 깨진 파일이다
        cached = bronze.withColumn("_src_file", F.input_file_name()).persist(StorageLevel.MEMORY_AND_DISK)
        counts["files_with_rows"] = cached.select("_src_file").distinct().count() if mode == "listing" else None
        bronze = cached.drop("_src_file")
        new = batch_dedup(in_days(transform(bronze, table, cfg), table, days), table)
        (new.repartition(table.partition_col).write.mode("overwrite").partitionBy(table.partition_col).parquet(str(out_dir)))
        counts["bronze_rows"] = bronze.count()
        counts["bronze_parse_ok"] = bronze.filter(F.col("_parse_ok")).count()
        mi = bronze.filter(F.col("_parse_ok")).agg(F.max("ingested_at")).first()[0]     # Silver 로 가는 행의 마지막 적재 시각
        max_ing = mi.replace(tzinfo=timezone.utc).isoformat() if mi is not None and mi.tzinfo is None else (mi.isoformat() if mi else None)
        written = _read_written(spark, out_dir)
        ok_keys = in_days(transform(bronze, table, cfg), table, days).select(*keys).distinct()
        if written is not None:
            written = written.persist(StorageLevel.MEMORY_AND_DISK)
            counts["silver_after"] = written.count()
            for r in written.groupBy(table.partition_col).count().collect():
                per_day[r[0].isoformat()] = r["count"]
            new_keys = written.select(*keys).distinct()
            counts["missing"] = ok_keys.join(new_keys, keys, "left_anti").count()
            counts["extra"] = new_keys.join(ok_keys, keys, "left_anti").count()
        else:
            counts["missing"] = ok_keys.count()
        cached.unpersist()
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        written = None
    current = read_silver(spark, cfg.silver_root, str(root), table.name, table.partition_col)
    if current is not None:                                  # 현재 Silver 도 작은 파일이 많다. 대조에서 여러 번 세므로 한 번만 읽는다
        current = in_days(current, table, days).persist(StorageLevel.MEMORY_AND_DISK)
    if written is None:
        written = spark.createDataFrame([], current.schema) if current is not None else None
    cmp = compare_with_current(written, current, table) if written is not None else \
        {"silver_before": 0, "silver_before_keys": 0, "added": 0, "removed": 0, "changed": 0}
    counts.update(cmp)
    after_keys = counts["silver_after"]                     # 결정적 dedup 이라 행 수 = 키 수
    status, why = "ok", ""
    if counts["missing"] or counts["extra"]:
        status, why = "reconcile_failed", f"Bronze 정상 키와 다릅니다 (유실 {counts['missing']}, 초과 {counts['extra']})"
    elif after_keys < counts["silver_before_keys"] and not allow_shrink:
        status, why = "shrink_blocked", (f"재처리 결과 키 {after_keys} < 현재 Silver 키 {counts['silver_before_keys']} "
                                         f"(삭제 {counts['removed']}). 의도한 것이면 allow_shrink 와 사유를 주세요")
    body = {"status": status, "reason": why, "table": table.name, "version": version, "partition_col": table.partition_col,
            "dates": per_day, "counts": counts, "bronze_max_ingested_at": max_ing, "allow_shrink": allow_shrink,
            "config_sha256": config_sha(cfg.config_dir), "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    P.write_json_atomic(out_dir / P.BUILD_FILE, body)
    if written is not None:
        written.unpersist()
    if current is not None:
        current.unpersist()
    return body


def _read_written(spark, path: Path):
    from streaming.verify_bronze import read_bronze
    return read_bronze(spark, str(path))


def build(spark, cfg, table_names: list[str], days: list[date], *, rebuild_root: str, version: str, mode: str = "listing",
          allow_shrink: bool = False) -> dict:
    """여러 표를 차례로 만든다. 하나라도 status 가 ok 가 아니면 GuardError (앞 표의 버전 폴더는 남지만 게시되지 않는다)."""
    from streaming.silver_config import load_silver_tables
    P.check_version(version)
    by_name = {t.name: t for t in load_silver_tables(cfg.config_dir)}
    unknown = [n for n in table_names if n not in by_name]
    if unknown:
        raise ValueError(f"모르는 Silver 표: {unknown} (있는 표: {sorted(by_name)})")
    fail_after = os.environ.get(FAIL_ENV)
    report = {"version": version, "date_from": min(days).isoformat(), "date_to": max(days).isoformat(), "tables": {}}
    for n in table_names:
        report["tables"][n] = build_table(spark, cfg, by_name[n], days, rebuild_root=rebuild_root, version=version, mode=mode,
                                          allow_shrink=allow_shrink)
        if fail_after == n:
            raise RuntimeError(f"시험용 실패 주입 ({FAIL_ENV}={n})")
    bad = {n: b["reason"] for n, b in report["tables"].items() if b["status"] != "ok"}
    report["ok"] = not bad
    report["bronze_max_ingested_at"] = max((b["bronze_max_ingested_at"] for b in report["tables"].values() if b["bronze_max_ingested_at"]),
                                           default=None)
    if bad:
        report["error"] = "; ".join(f"{n}: {w}" for n, w in bad.items())
    return report


# ------------------------------------------------------------------ publish · rollback (Spark 없음)

def publish_all(rebuild_root: str, table_names: list[str], days: list[date], version: str, *, reprocess_id: Optional[int] = None) -> dict:
    """모든 표의 빌드가 ok 인지 먼저 확인한 뒤 표마다 포인터를 바꾼다. 반환 = {표: {날짜: 바꾸기 전 버전 | None}}."""
    root = Path(rebuild_root)
    for n in table_names:
        b = P.load_build(root, n, version)
        if b.get("status") != "ok":
            raise P.PointerError(f"{n} {version}: 빌드 상태 {b.get('status')} — 게시하지 않습니다 ({b.get('reason')})")
    return {n: P.publish(root, n, version, days, reprocess_id=reprocess_id) for n in table_names}


def rollback_all(rebuild_root: str, previous: dict, *, expect_version: Optional[str] = None, reprocess_id: Optional[int] = None,
                 force: bool = False) -> dict:
    root = Path(rebuild_root)
    return {n: P.rollback(root, n, prev, expect_version=expect_version, reprocess_id=reprocess_id, force=force)
            for n, prev in previous.items()}


# ------------------------------------------------------------------ CLI

def _spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("urbanstream-silver-rebuild").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    return s


def _all_tables(config_dir: str) -> list[str]:
    from streaming.silver_config import load_silver_tables
    return [t.name for t in load_silver_tables(config_dir)]


def main(argv: Optional[list[str]] = None) -> int:
    from streaming.silver_config import SilverConfig
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["build", "publish", "rollback", "prune", "status"])
    ap.add_argument("--tables", default="all", help="쉼표로 구분한 Silver 표 이름, 또는 all")
    ap.add_argument("--from", dest="d0", type=date.fromisoformat)
    ap.add_argument("--to", dest="d1", type=date.fromisoformat)
    ap.add_argument("--version")
    ap.add_argument("--reprocess-id", type=int)
    ap.add_argument("--bronze-read", choices=READ_MODES, default="listing")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--allow-recent", action="store_true", help="오늘·어제도 허용 (수동 실행 전용)")
    ap.add_argument("--force", action="store_true", help="rollback: 그 뒤 다른 버전이 게시된 날짜도 되돌림")
    ap.add_argument("--dry-run", action="store_true", help="prune: 지울 목록만")
    ap.add_argument("--report", help="build/publish 결과 JSON 을 쓸 파일")
    ap.add_argument("--rebuild-root", default=os.environ.get("SILVER_REBUILD_ROOT", DEFAULT_REBUILD_ROOT))
    a = ap.parse_args(argv)
    cfg = SilverConfig.from_env()
    names = _all_tables(cfg.config_dir) if a.tables == "all" else [t.strip() for t in a.tables.split(",") if t.strip()]
    out: dict
    rc = 0
    if a.command in ("build", "publish"):
        if not (a.d0 and a.d1 and a.version):
            ap.error("--from, --to, --version 이 필요합니다")
        days = P.check_dates(a.d0, a.d1, allow_recent=a.allow_recent)
        if a.command == "build":
            try:
                out = build(_spark(), cfg, names, days, rebuild_root=a.rebuild_root, version=a.version, mode=a.bronze_read,
                            allow_shrink=a.allow_shrink)
            except Exception as e:                                           # noqa: BLE001
                out = {"ok": False, "version": a.version, "error": f"{type(e).__name__}: {e}"[:1000]}
                rc = 1
            if not out.get("ok"):
                rc = rc or 2
        else:
            out = {"version": a.version, "previous": publish_all(a.rebuild_root, names, days, a.version, reprocess_id=a.reprocess_id)}
    elif a.command == "rollback":
        if a.reprocess_id is None:
            ap.error("--reprocess-id 가 필요합니다")
        prev, ver = {}, None
        for n in names:
            h = P.find_publish(Path(a.rebuild_root), n, a.reprocess_id)
            if h:
                prev[n], ver = h["previous"], h["version"]
        if not prev:
            print(f"재처리 {a.reprocess_id} 의 게시 기록이 없습니다", file=sys.stderr)
            return 1
        out = {"version": ver, "replaced": rollback_all(a.rebuild_root, prev, expect_version=ver, reprocess_id=a.reprocess_id, force=a.force),
               "restored": prev}
    elif a.command == "prune":
        out = {n: P.prune(Path(a.rebuild_root), n, dry_run=a.dry_run) for n in names}
    else:
        out = {n: P.active_dates(Path(a.rebuild_root), n) for n in names}
    text = json.dumps(out, ensure_ascii=False, indent=1, default=str)
    if a.report:
        P.write_json_atomic(Path(a.report), out)
    print(text)
    return rc


if __name__ == "__main__":
    sys.exit(main())
