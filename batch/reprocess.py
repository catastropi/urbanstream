"""Bronze→Silver 재처리와 Gold 범위 교체를 하루 단위로 묶는다 (STEP 1, docs/adr/0015-reprocess.md). DAG `reprocess` 가 부른다.

하루 D 의 순서 (상태는 ops.reprocess_run.status)
  1. 행 생성 running                                   같은 request_key 의 재시도는 같은 행을 이어 쓴다
  2. silver_rebuild (gold-runner, Spark)               Bronze(D) → data/silver_rebuild/<표>/<버전>/          → silver_built
  3. silver_publish (gold-runner, 파일 하나 바꾸기)      _active/<표>.json 의 D 를 새 버전으로                  → silver_published
  4. gold_stage (gold-runner, Spark)                   read_silver 로 읽으므로 새 버전을 본다
  5. load_all(mode=replace, D, D)                      교체 대상 표의 D 를 지우고 다시 넣는다(한 트랜잭션)    → gold_loaded
  6. verify_counts(exact) → 범위 마트 재계산                                                                  → ok
실패
  - 2~3 에서 실패: 포인터가 그대로라 Silver·Gold 모두 재처리 전 상태. status=failed. 재시도하면 새 버전 폴더를 만든다.
  - 3 이후 실패: Silver 는 새 버전, Gold 는 옛 값. status 는 silver_published(또는 gold_loaded)로 남고 error 에 사유가 남는다.
    같은 DAG 실행을 재시도하면 4 부터 이어 간다(4·5 는 멱등). 원하면 `batch.silver_rebuild rollback` 으로 Silver 를 되돌린다.
scope: silver(1~3), silver_gold(1~6, 기본), gold(4~6: 지금 Silver 로 Gold 범위만 교체).

이 모듈은 import 만으로 pyspark·psycopg 를 불러오지 않는다 (DAG 파싱이 가벼워야 한다, batch.jobs 와 같은 규칙).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from batch import jobs
from batch.jobs import JobError

LOG = logging.getLogger("urbanstream.batch.reprocess")

SILVER_TO_GOLD: dict[str, str] = {
    "silver_traffic_speed": "traffic_speed", "silver_bus_position": "bus_position", "silver_bus_arrival": "bus_arrival",
    "silver_incident_observation": "incident_observation", "silver_weather": "weather_obs",
}
ALL_TABLES = tuple(SILVER_TO_GOLD)
SCOPES = ("silver", "silver_gold", "gold")
READ_MODES = ("listing", "sink_log")
ACTIVE = ("running", "silver_built", "silver_published", "gold_loaded")
# 허용하는 상태 전이. failed → running 은 재시도. 되돌리기(rolled_back)는 게시 이후에만 의미가 있다.
TRANSITIONS: dict[str, set[str]] = {
    "running": {"silver_built", "gold_loaded", "failed"},
    "silver_built": {"silver_published", "failed"},
    "silver_published": {"gold_loaded", "ok", "rolled_back"},
    "gold_loaded": {"ok", "rolled_back"},
    "ok": {"rolled_back"},
    "failed": {"running"},
    "rolled_back": set(),
}


# ------------------------------------------------------------------ 인자 확인

def parse_tables(value: Any) -> list[str]:
    """'all', 쉼표 문자열, 목록 → Silver 표 이름 목록 (ALL_TABLES 순서)."""
    if value is None or (isinstance(value, str) and value.strip().lower() in ("", "all")):
        return list(ALL_TABLES)
    items = [x.strip() for x in (value.split(",") if isinstance(value, str) else value) if str(x).strip()]
    bad = [x for x in items if x not in SILVER_TO_GOLD]
    if bad:
        raise ValueError(f"모르는 Silver 표: {bad} (있는 표: {list(ALL_TABLES)})")
    return [t for t in ALL_TABLES if t in items]


def check_args(scope: str, reason: str, mode: str) -> None:
    if scope not in SCOPES:
        raise ValueError(f"scope 는 {SCOPES} 중 하나: {scope!r}")
    if not (reason or "").strip():
        raise ValueError("reason(왜 다시 만드는지)을 적어 주세요")
    if mode not in READ_MODES:
        raise ValueError(f"bronze_read_mode 는 {READ_MODES} 중 하나: {mode!r}")


def request_key(dag_id: str, run_id: str, day: date) -> str:
    return f"{dag_id}__{run_id}__{day.isoformat()}"


# ------------------------------------------------------------------ ops.reprocess_run

_COLS = ("reprocess_id", "status", "silver_version", "previous_version", "try_number", "scope", "tables", "load_run_id")


def _row(conn, where: str, params) -> Optional[dict]:
    r = conn.execute(f"SELECT {', '.join(_COLS)} FROM ops.reprocess_run WHERE {where}", params).fetchone()
    return dict(zip(_COLS, r)) if r else None


def start_run(conn, key: str, *, scope: str, tables: list[str], d0: date, d1: date, reason: str, mode: str = "listing",
              allow_shrink: bool = False, dag_id: Optional[str] = None, run_id: Optional[str] = None, try_number: int = 1,
              code_version: Optional[str] = None, config_sha: Optional[str] = None) -> dict:
    """같은 key 가 없으면 running 으로 만든다. failed 면 running 으로 되살린다. 끝나지 않은 상태면 그대로 이어 간다.
    겹치는 날짜의 다른 재처리가 끝나지 않았으면 EXCLUDE 제약이 막는다 → JobError."""
    import psycopg
    row = _row(conn, "request_key = %s", (key,))
    if row is None:
        try:
            conn.execute(
                "INSERT INTO ops.reprocess_run (request_key, scope, tables, date_from, date_to, reason, bronze_read_mode, allow_shrink, "
                "code_version, config_sha256, dag_id, airflow_run_id, try_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (key, scope, tables, d0, d1, reason, mode, allow_shrink, code_version, config_sha, dag_id, run_id, try_number))
        except psycopg.errors.ExclusionViolation as e:
            raise JobError(f"{d0}~{d1} 와 겹치는 끝나지 않은 재처리가 있습니다 (ops.v_reprocess_recent 확인)") from e
        return _row(conn, "request_key = %s", (key,))
    if row["status"] == "rolled_back":
        raise JobError(f"재처리 {row['reprocess_id']} 는 되돌린 기록입니다. 새 DAG 실행으로 다시 하세요")
    if row["status"] == "failed":
        try:
            set_status(conn, row["reprocess_id"], "running", try_number=try_number, error=None)
        except psycopg.errors.ExclusionViolation as e:
            raise JobError(f"{d0}~{d1} 와 겹치는 끝나지 않은 재처리가 있습니다") from e
        row = _row(conn, "request_key = %s", (key,))
    else:
        conn.execute("UPDATE ops.reprocess_run SET try_number=%s, updated_at=now() WHERE reprocess_id=%s", (try_number, row["reprocess_id"]))
        row["try_number"] = try_number
    return row


def set_status(conn, rid: int, new: str, **fields) -> None:
    """상태를 바꾼다. 허용되지 않은 전이는 JobError. fields 는 함께 바꿀 열 (row_counts 는 합친다)."""
    with conn.transaction():
        cur = conn.execute("SELECT status FROM ops.reprocess_run WHERE reprocess_id=%s FOR UPDATE", (rid,)).fetchone()
        if cur is None:
            raise JobError(f"재처리 {rid} 가 없습니다")
        if new != cur[0] and new not in TRANSITIONS[cur[0]]:
            raise JobError(f"재처리 {rid}: {cur[0]} → {new} 는 허용되지 않는 전이입니다")
        sets, params = ["status=%s", "updated_at=now()"], [new]
        if new in ("ok", "failed", "rolled_back"):
            sets.append("finished_at=now()")
        elif new == "running":
            sets.append("finished_at=NULL")
        for k, v in fields.items():
            if k == "row_counts":
                sets.append("row_counts = row_counts || %s::jsonb")
                params.append(json.dumps(v, default=str))
            elif k == "previous_version":
                sets.append("previous_version=%s::jsonb")
                params.append(json.dumps(v))
            else:
                sets.append(f"{k}=%s")
                params.append(v)
        params.append(rid)
        conn.execute(f"UPDATE ops.reprocess_run SET {', '.join(sets)} WHERE reprocess_id=%s", params)


def record_error(conn, rid: int, err: str) -> None:
    conn.execute("UPDATE ops.reprocess_run SET error=%s, updated_at=now() WHERE reprocess_id=%s", (err[:2000], rid))


def mark_stale(conn, *, older_than_minutes: int = 180) -> dict:
    """프로세스가 사라져 running·silver_built 에 멈춘 행을 failed 로 바꾼다(겹치는 새 재처리를 막지 않게).
    silver_published·gold_loaded 는 Silver 가 이미 바뀐 상태라 사람이 확인해야 하므로 바꾸지 않고 목록만 돌려준다."""
    cur = conn.execute(
        "UPDATE ops.reprocess_run SET status='failed', finished_at=now(), updated_at=now(), "
        "error=coalesce(error || ' / ', '') || 'stale: 프로세스가 끝나기 전에 사라짐' "
        "WHERE status IN ('running', 'silver_built') AND updated_at < now() - make_interval(mins => %s)", (older_than_minutes,))
    stuck = [r[0] for r in conn.execute(
        "SELECT reprocess_id FROM ops.reprocess_run WHERE status IN ('silver_published', 'gold_loaded') "
        "AND updated_at < now() - make_interval(mins => %s) ORDER BY reprocess_id", (older_than_minutes,)).fetchall()]
    return {"failed": cur.rowcount or 0, "needs_attention": stuck}


# ------------------------------------------------------------------ 단계 실행 (runner 또는 inline)

def rebuild_root(env: Optional[Mapping[str, str]] = None) -> str:
    e = os.environ if env is None else env
    return e.get("GOLD_SILVER_REBUILD_ROOT") or jobs.DEFAULT_REBUILD


def _read_report(path: Path) -> Optional[dict]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def run_rebuild(d0: date, d1: date, stage_dir: Path, *, tables: list[str], version: str, mode: str = "listing",
                allow_shrink: bool = False, timeout_sec: int = 3600, runner: Callable[..., Any] = subprocess.run) -> dict:
    """Silver 버전 만들기. 반환 = silver_rebuild 의 보고서. 가드에 걸리면 보고서의 사유를 담아 JobError."""
    from batch.stage_runner import REBUILD_REPORT
    report_path = Path(stage_dir) / REBUILD_REPORT
    args = {"tables": tables, "version": version, "bronze_read_mode": mode, "allow_shrink": allow_shrink}
    try:
        if jobs.stage_mode() == "runner":
            jobs.request_runner("silver_rebuild", d0, d1, Path(stage_dir), args=args, timeout_sec=timeout_sec)
        else:
            Path(stage_dir).mkdir(parents=True, exist_ok=True)
            env = jobs.stage_env(Path(stage_dir), silver_root=os.environ.get("GOLD_SILVER_ROOT", jobs.DEFAULT_SILVER),
                                 bronze_root=os.environ.get("GOLD_BRONZE_ROOT", jobs.DEFAULT_BRONZE),
                                 driver_memory=os.environ.get("GOLD_DRIVER_MEMORY", "1g"),
                                 master=os.environ.get("GOLD_SPARK_MASTER", "local[2]"))
            env.update({"SILVER_BRONZE_ROOT": env["GOLD_BRONZE_ROOT"], "SILVER_ROOT": env["GOLD_SILVER_ROOT"],
                        "SILVER_CONFIG_DIR": os.environ.get("SILVER_CONFIG_DIR", str(jobs.ROOT / "config"))})
            for k in ("SILVER_REBUILD_FAIL_AFTER",):                  # 시험용 실패 주입만 넘긴다
                if k in os.environ:
                    env[k] = os.environ[k]
            cmd = [sys.executable, "-m", "batch.silver_rebuild", "build", "--tables", ",".join(tables), "--from", d0.isoformat(),
                   "--to", d1.isoformat(), "--version", version, "--bronze-read", mode, "--rebuild-root", rebuild_root(),
                   "--report", str(report_path)] + (["--allow-shrink"] if allow_shrink else [])
            proc = runner(cmd, cwd=str(jobs.ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
            if proc.returncode != 0:
                raise JobError(f"silver_rebuild 실패 (종료 코드 {proc.returncode})\n{jobs._tail(proc.stderr, 40)}")
    except JobError as e:
        rep = _read_report(report_path)
        if rep and rep.get("error"):
            raise JobError(f"silver_rebuild 가 게시할 수 없는 결과를 냈습니다: {rep['error']}") from e
        raise
    rep = _read_report(report_path)
    if not rep or not rep.get("ok"):
        raise JobError(f"silver_rebuild 보고서가 없거나 실패입니다: {(rep or {}).get('error')}")
    return rep


def run_publish(d0: date, d1: date, stage_dir: Path, *, tables: list[str], version: str, reprocess_id: Optional[int]) -> dict:
    """포인터 바꾸기. 반환 = {표: {날짜: 바꾸기 전 버전 | None}}."""
    from batch.stage_runner import PUBLISH_REPORT
    if jobs.stage_mode() == "runner":
        jobs.request_runner("silver_publish", d0, d1, Path(stage_dir),
                            args={"tables": tables, "version": version, "reprocess_id": reprocess_id}, timeout_sec=300)
        rep = _read_report(Path(stage_dir) / PUBLISH_REPORT)
        if not rep:
            raise JobError("silver_publish 보고서가 없습니다")
        return rep["previous"]
    from batch.silver_rebuild import publish_all
    from streaming.silver_pointer import check_dates
    days = check_dates(d0, d1, allow_recent=True)                 # 날짜 가드는 build 단계에서 이미 통과했다
    return publish_all(rebuild_root(), tables, days, version, reprocess_id=reprocess_id)


def rebuild_marts(d0: date, d1: date, *, connect: Optional[Callable[[], Any]] = None) -> dict:
    """범위 교체 뒤 날짜 범위 마트를 다시 계산한다. 기준선이 아직 없으면 기상×교통은 건너뛴다(마트 DAG 와 같은 조건).
    이상 탐지(mart.anomaly_event)와 돌발 영향(incident_impact)은 다시 계산하지 않는다: 이미 보낸 알림·확정 기록이라서다(ADR-0015)."""
    from batch import marts
    out: dict[str, Any] = {}
    out["bus_arrival_drift"] = marts.build_bus_drift(d0, d1, connect=connect)
    out["bus_section_time"] = marts.build_bus_section_time(d0, d1, connect=connect)
    try:
        out["weather_traffic_hourly"] = marts.build_weather_traffic(d0, d1, connect=connect)
    except JobError as e:
        out["weather_traffic_hourly"] = f"건너뜀: {e}"
    return out


def check_pointer(manifest: Mapping[str, Any], day: date, tables: Iterable[str], version: str) -> None:
    """스테이징이 방금 게시한 버전을 읽었는지 확인한다 (게시 전 스냅샷으로 만든 manifest 를 적재하지 않게)."""
    ptr = manifest.get("silver_pointer") or {}
    bad = [t for t in tables if (ptr.get(t) or {}).get(day.isoformat()) != version]
    if bad:
        raise JobError(f"스테이징이 새 Silver 버전 {version} 을 읽지 않았습니다: {bad}")


# ------------------------------------------------------------------ 하루 처리

def process_reprocess_day(day: date, *, tables: list[str], reason: str, scope: str = "silver_gold", allow_shrink: bool = False,
                          bronze_read_mode: str = "listing", dag_id: str, run_id: str, try_number: int = 1, allow_recent: bool = False,
                          base: Optional[Path] = None, connect: Optional[Callable[[], Any]] = None,
                          build: Callable[..., dict] = run_rebuild, publish: Callable[..., dict] = run_publish,
                          stage: Callable[..., dict] = jobs.run_stage, load: Callable[..., dict] = jobs.run_load,
                          verify: Callable[..., Any] = jobs.verify_counts, marts: Callable[..., dict] = rebuild_marts) -> dict:
    from streaming.silver_pointer import check_dates, version_name
    check_args(scope, reason, bronze_read_mode)
    tables = parse_tables(tables)
    check_dates(day, day, allow_recent=allow_recent)
    connect = connect or jobs._default_connect
    base = Path(base or jobs.stage_base())
    key = request_key(dag_id, run_id, day)
    with connect() as conn:
        row = start_run(conn, key, scope=scope, tables=tables, d0=day, d1=day, reason=reason, mode=bronze_read_mode,
                        allow_shrink=allow_shrink, dag_id=dag_id, run_id=run_id, try_number=try_number,
                        code_version=os.environ.get("URBANSTREAM_CODE_VERSION") or None)
        rid, status = row["reprocess_id"], row["status"]
        if status == "ok":
            LOG.info("재처리 %s (%s) 는 이미 끝났습니다", rid, day)
            return {"reprocess_id": rid, "status": "ok", "skipped": True}
        version = row["silver_version"]
        dirs = []
        try:
            if scope != "gold" and status == "running":
                version = version_name(rid, try_number)
                sd = jobs.stage_dir_for(base, f"{dag_id}_rebuild_{day}", run_id, try_number)
                dirs.append(sd)
                rep = build(day, day, sd, tables=tables, version=version, mode=bronze_read_mode, allow_shrink=allow_shrink)
                set_status(conn, rid, "silver_built", silver_version=version, config_sha256=_first(rep, "config_sha256"),
                           bronze_max_ingested_at=rep.get("bronze_max_ingested_at"),
                           row_counts={t: b["counts"] for t, b in rep["tables"].items()})
                status = "silver_built"
            if status == "silver_built":
                sd = jobs.stage_dir_for(base, f"{dag_id}_publish_{day}", run_id, try_number)
                dirs.append(sd)
                prev = publish(day, day, sd, tables=tables, version=version, reprocess_id=rid)
                set_status(conn, rid, "silver_published", previous_version=prev)
                status = "silver_published"
                LOG.info("재처리 %s: Silver %s 게시 (%s, 이전 %s)", rid, version, day, prev)
            if scope == "silver":
                set_status(conn, rid, "ok")
                _cleanup(base, dirs)
                return {"reprocess_id": rid, "status": "ok", "silver_version": version}
            if status in ("running", "silver_published"):            # scope=gold 는 running 에서 바로 온다
                sd = jobs.stage_dir_for(base, f"{dag_id}_stage_{day}", run_id, try_number)
                dirs.append(sd)
                started = datetime.now(timezone.utc)
                manifest = stage(day, day, sd)
                for w in jobs.check_manifest(manifest, day, day, started_at=started):
                    LOG.warning(w)
                if scope == "silver_gold":
                    check_pointer(manifest, day, tables, version)
                replace = {SILVER_TO_GOLD[t] for t in tables}
                loaded = load(sd, dag_id=dag_id, mode="replace", replace=replace, d0=day, d1=day)
                counts = verify(manifest, day, day, exact=replace)
                set_status(conn, rid, "gold_loaded", load_run_id=loaded.get("run_id"),
                           row_counts={"_gold": {k: list(v) for k, v in counts.items()}, "_gold_deleted": loaded.get("deleted", {})})
                status = "gold_loaded"
            if status == "gold_loaded":
                m = marts(day, day)
                set_status(conn, rid, "ok", row_counts={"_marts": m})
                status = "ok"
        except Exception as e:                                                   # noqa: BLE001
            msg = f"{type(e).__name__}: {e}"
            cur = _row(conn, "reprocess_id=%s", (rid,))["status"]
            if cur in ("running", "silver_built"):
                set_status(conn, rid, "failed", error=msg[:2000])
            else:
                record_error(conn, rid, msg)
            raise
        _cleanup(base, dirs)
    return {"reprocess_id": rid, "status": status, "silver_version": version}


def _first(rep: dict, key: str):
    for b in (rep.get("tables") or {}).values():
        if b.get(key):
            return b[key]
    return None


def _cleanup(base: Path, dirs: list[Path]) -> None:
    for d in dirs:
        try:
            jobs.cleanup_stage(base, d)
        except (JobError, OSError) as e:                                        # 정리 실패는 결과를 바꾸지 않는다 (72시간 뒤 prune)
            LOG.warning("스테이징 정리 실패 %s: %s", d, e)


# ------------------------------------------------------------------ 되돌린 뒤 기록 (CLI)

def main(argv: Optional[list[str]] = None) -> int:
    """DB 쪽 기록 도구. 파일 쪽 되돌리기는 gold-runner 안에서 `python3 -m batch.silver_rebuild rollback --reprocess-id N`."""
    ap = argparse.ArgumentParser(description=main.__doc__)
    ap.add_argument("command", choices=["show", "mark-rolled-back", "mark-stale"])
    ap.add_argument("--id", type=int)
    a = ap.parse_args(argv)
    with jobs._default_connect() as conn:
        if a.command == "mark-stale":
            print(json.dumps(mark_stale(conn), ensure_ascii=False))
            return 0
        if a.id is None:
            ap.error("--id 가 필요합니다")
        r = conn.execute("SELECT reprocess_id, status, scope, tables, date_from, date_to, silver_version, previous_version, load_run_id, error "
                         "FROM ops.reprocess_run WHERE reprocess_id=%s", (a.id,)).fetchone()
        if r is None:
            print(f"재처리 {a.id} 가 없습니다", file=sys.stderr)
            return 1
        if a.command == "mark-rolled-back":
            set_status(conn, a.id, "rolled_back")
        cols = ("reprocess_id", "status", "scope", "tables", "date_from", "date_to", "silver_version", "previous_version", "load_run_id", "error")
        print(json.dumps(dict(zip(cols, r)), ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
