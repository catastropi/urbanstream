"""Airflow 가 부르는 얇은 함수 모음 (Step 7, ADR-0009).

DAG 파일에는 로직을 두지 않는다. 날짜 계산, 스테이징 폴더 관리, Spark 서브프로세스 실행, 적재, 검증은 전부 여기 있고
Airflow 없이 단위 테스트할 수 있다. **이 모듈은 import 만으로 pyspark·psycopg 를 불러오지 않는다**(DAG 파싱이 가벼워야 한다).

핵심 약속
- 윈도우는 '날짜'로 정한다(실행 시각의 간격이 아니라). 잠자기로 실행이 빠져도 다음 실행이 따라잡는다. Step 6 의 멱등 인터페이스 덕분이다.
- 실행마다 스테이징 폴더가 다르다(겹치는 실행이 서로의 파일을 덮어쓰지 않는다). 정리는 base 폴더 안에서만 한다.
- Spark 서브프로세스에는 DB·Airflow 비밀을 넘기지 않는다(환경을 화이트리스트로 만든다, ADR-0008 ⑤).
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

LOG = logging.getLogger("urbanstream.batch.jobs")
KST = ZoneInfo("Asia/Seoul")
ROOT = Path(os.environ.get("URBANSTREAM_ROOT") or Path(__file__).resolve().parents[1])
DEFAULT_STAGE_BASE = "/data/gold_stage"
DEFAULT_SILVER = "/data/silver"
DEFAULT_BRONZE = "/data/bronze"
MAX_BACKFILL_DAYS = 31


class JobError(RuntimeError):
    """Gold 배치 작업 실패 (Airflow 태스크를 실패로 만든다)."""


# ------------------------------------------------------------------ 날짜 윈도우 (KST)

def pick_anchor(*candidates: Optional[datetime]) -> datetime:
    """DAG 실행의 기준 시각. Airflow 3 는 수동 실행에서 logical_date 가 비어 있을 수 있어 순서대로 첫 값을 쓴다."""
    for c in candidates:
        if c is not None:
            return c
    return datetime.now(timezone.utc)


def kst_date(anchor: datetime) -> date:
    """시각 → KST 날짜. 시간대 없는 값은 UTC 로 본다."""
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    return anchor.astimezone(KST).date()


def window_intraday(anchor: datetime) -> tuple[date, date]:
    """오늘(KST) 하루. 신선도용 — 팩트는 DO NOTHING 이라 부분 하루를 여러 번 돌려도 안전하다."""
    d = kst_date(anchor)
    return d, d


def window_daily(anchor: datetime) -> tuple[date, date]:
    """어제~오늘(KST). 자정 직후 늦게 도착한 어제 데이터(버스 GPS 최대 약 200초, 워터마크 10분)를 닫는다."""
    d = kst_date(anchor)
    return d - timedelta(days=1), d


def days_between(d0: date, d1: date, *, max_days: int = MAX_BACKFILL_DAYS) -> list[date]:
    """양 끝 포함 날짜 목록. 순서가 뒤집혔거나 너무 길면 거부한다(넓은 범위는 로더가 메모리를 많이 쓴다)."""
    if d0 > d1:
        raise ValueError(f"시작일({d0})이 종료일({d1})보다 늦습니다")
    n = (d1 - d0).days + 1
    if n > max_days:
        raise ValueError(f"한 번에 {max_days}일까지만 허용합니다 (요청 {n}일). 나눠서 실행하세요")
    return [d0 + timedelta(days=i) for i in range(n)]


def parse_day(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as e:
        raise ValueError(f"날짜는 YYYY-MM-DD 형식이어야 합니다: {value!r}") from e


# ------------------------------------------------------------------ 스테이징 폴더

def stage_base(env: Optional[Mapping[str, str]] = None) -> Path:
    e = os.environ if env is None else env
    return Path(e.get("GOLD_STAGE_BASE") or e.get("GOLD_STAGE_ROOT") or DEFAULT_STAGE_BASE)


_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def stage_dir_for(base: Path, dag_id: str, run_id: str, try_number: int = 1) -> Path:
    """실행·시도마다 다른 폴더. 이름은 정리 때 알아볼 수 있게 '__' 로 구분한다."""
    name = _SAFE.sub("_", f"{dag_id}__{run_id}__try{int(try_number)}")[:140]
    return Path(base) / name


def _inside(base: Path, path: Path) -> bool:
    b, p = Path(base).resolve(), Path(path).resolve()
    return p != b and b in p.parents


def cleanup_stage(base: Path, stage_dir: Path) -> bool:
    """스테이징 폴더 삭제. base 밖이거나 base 자신이면 거부한다(잘못된 경로가 들어와도 다른 폴더를 지우지 않게)."""
    if not _inside(base, stage_dir):
        raise JobError(f"스테이징 폴더가 base 안이 아닙니다: {stage_dir} (base={base})")
    if "__" not in Path(stage_dir).name:
        raise JobError(f"스테이징 폴더 이름 규칙(dag__run__try)이 아닙니다: {stage_dir}")
    if not Path(stage_dir).exists():
        return False
    shutil.rmtree(stage_dir)
    return True


def prune_old_stages(base: Path, *, keep_hours: float = 72.0, now: Optional[float] = None) -> list[str]:
    """실패해서 남겨 둔 스테이징 폴더를 오래된 것부터 지운다. base 바로 아래의 'dag__run__try' 이름만 대상이다."""
    base = Path(base)
    if not base.exists():
        return []
    cutoff = (time.time() if now is None else now) - keep_hours * 3600
    removed = []
    for child in sorted(base.iterdir()):
        if child.is_dir() and not child.is_symlink() and "__" in child.name and child.stat().st_mtime < cutoff:
            shutil.rmtree(child)
            removed.append(child.name)
    return removed


# ------------------------------------------------------------------ Spark 스테이징 (서브프로세스)

_ENV_PASS = ("PATH", "HOME", "LANG", "LC_ALL", "JAVA_HOME", "TMPDIR", "TZ")


def stage_env(stage_dir: Path, *, silver_root: str, bronze_root: str, driver_memory: str = "1g",
              master: str = "local[2]", source_env: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    """Spark 서브프로세스의 환경. **화이트리스트** — POSTGRES_*, GOLD_WRITER_*, AIRFLOW__* 같은 비밀은 넘기지 않는다."""
    src = os.environ if source_env is None else source_env
    env = {k: src[k] for k in _ENV_PASS if k in src}
    env.update({
        "GOLD_SILVER_ROOT": str(silver_root), "GOLD_BRONZE_ROOT": str(bronze_root), "GOLD_STAGE_ROOT": str(stage_dir),
        "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "src")]),
        "PYTHONDONTWRITEBYTECODE": "1",
        # pyspark 가 JVM 을 띄울 때 쓰는 인자 (드라이버 힙·스레드). 컨테이너 한도는 이 값의 1.4배 이상이어야 한다(정책 테스트).
        "PYSPARK_SUBMIT_ARGS": f"--master {master} --driver-memory {driver_memory} pyspark-shell",
    })
    return env


def _tail(text: str, n: int) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-n:])


def run_stage(d0: date, d1: date, stage_dir: Path, *, silver_root: Optional[str] = None, bronze_root: Optional[str] = None,
              driver_memory: Optional[str] = None, master: Optional[str] = None, timeout_sec: int = 1800,
              runner: Callable[..., Any] = subprocess.run, python: Optional[str] = None) -> dict:
    """Silver·Bronze audit → stage_dir (Parquet + manifest.json). 비밀 없는 환경에서 `python -m batch.gold_stage` 를 실행한다."""
    if d0 > d1:
        raise ValueError("d0 > d1")
    Path(stage_dir).mkdir(parents=True, exist_ok=True)
    env = stage_env(
        stage_dir, silver_root=silver_root or os.environ.get("GOLD_SILVER_ROOT", DEFAULT_SILVER),
        bronze_root=bronze_root or os.environ.get("GOLD_BRONZE_ROOT", DEFAULT_BRONZE),
        driver_memory=driver_memory or os.environ.get("GOLD_DRIVER_MEMORY", "1g"),
        master=master or os.environ.get("GOLD_SPARK_MASTER", "local[2]"))
    cmd = [python or sys.executable, "-m", "batch.gold_stage", "--from", d0.isoformat(), "--to", d1.isoformat()]
    started = time.monotonic()
    LOG.info("gold_stage 시작: %s ~ %s → %s", d0, d1, stage_dir)
    try:
        proc = runner(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as e:
        raise JobError(f"gold_stage 가 {timeout_sec}초 안에 끝나지 않았습니다") from e
    if proc.returncode != 0:
        raise JobError(f"gold_stage 실패 (종료 코드 {proc.returncode})\n{_tail(proc.stderr, 60)}")
    if proc.stderr:
        LOG.info("gold_stage stderr 끝부분:\n%s", _tail(proc.stderr, 15))
    LOG.info("gold_stage 완료 (%.1f초)", time.monotonic() - started)
    return read_manifest(stage_dir)


def read_manifest(stage_dir: Path) -> dict:
    p = Path(stage_dir) / "manifest.json"
    if not p.exists():
        raise JobError(f"manifest.json 이 없습니다: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def check_manifest(manifest: Mapping[str, Any], d0: date, d1: date, *, started_at: Optional[datetime] = None,
                   require_audit: bool = True) -> list[str]:
    """스테이징 결과가 이번 실행의 것인지, 쓸 수 있는 상태인지 확인한다. 경고 목록을 돌려주고, 치명적이면 JobError.

    - audit 이 없으면 돌발 absent_since 가 전부 NULL(= '아직 목록에 있음')이 된다 → 적재하지 않는다 (Step 6 한계 6).
    - 날짜 범위가 다르면 옛 manifest 가 남아 있는 것이다.
    """
    warnings: list[str] = []
    if manifest.get("date_from") != d0.isoformat() or manifest.get("date_to") != d1.isoformat():
        raise JobError(f"manifest 날짜 범위({manifest.get('date_from')}~{manifest.get('date_to')})가 요청({d0}~{d1})과 다릅니다")
    if started_at is not None:
        created = datetime.fromisoformat(str(manifest["created_at"]))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created < started_at - timedelta(seconds=2):
            raise JobError(f"manifest 가 이번 실행 이전에 만들어졌습니다 ({created.isoformat()})")
    if require_audit and not manifest.get("audit_available", False):
        raise JobError("Bronze audit 이 없어 돌발 absent_since 를 계산하지 못했습니다 — 적재하지 않습니다")
    counts = manifest.get("row_counts") or {}
    if not counts:
        raise JobError("manifest 에 row_counts 가 없습니다")
    if all(v == 0 for v in counts.values()):
        warnings.append("모든 표의 스테이징 행이 0입니다 (Silver 가 아직 없거나 날짜에 데이터가 없음)")
    return warnings


# ------------------------------------------------------------------ 적재 · 검증

def _default_connect():
    import psycopg  # 지연 import: DAG 파싱 때 불러오지 않는다
    from batch.db import conninfo
    return psycopg.connect(conninfo(role="writer"), autocommit=True)


def run_load(stage_dir: Path, *, refresh_mart: bool = False, connect: Optional[Callable[[], Any]] = None) -> dict:
    from batch.gold_load import load_all
    with (connect or _default_connect)() as conn:
        return load_all(conn, Path(stage_dir), refresh_mart=refresh_mart)


def refresh_marts(*, connect: Optional[Callable[[], Any]] = None) -> None:
    """mart.refresh_all() 만 따로 실행 (백필의 마지막 단계)."""
    with (connect or _default_connect)() as conn:
        conn.execute("SELECT mart.refresh_all()")


# 표 → 날짜 열. 에피소드는 날짜 범위가 없다(전체 이력).
DATE_COLUMNS: dict[str, tuple[str, str]] = {
    "traffic_speed": ("fact.traffic_speed", "event_date"),
    "bus_position": ("fact.bus_position", "event_date"),
    "bus_arrival": ("fact.bus_arrival", "event_date"),
    "incident_observation": ("fact.incident_observation", "observed_date"),
    "weather_obs": ("fact.weather_obs", "event_date"),
    "call_audit": ("ops.call_audit", "event_date"),
}


def verify_counts(manifest: Mapping[str, Any], d0: date, d1: date, *, connect: Optional[Callable[[], Any]] = None) -> dict[str, tuple[int, int]]:
    """적재 뒤 확인: 날짜 범위의 DB 행 수 ≥ 스테이징 행 수 (팩트는 DO NOTHING 이라 DB 가 더 많은 것은 정상일 수 있다).
    DB 가 더 적으면 일부가 들어가지 못한 것이다 → JobError. 반환 = {표: (스테이징, DB)}."""
    counts = manifest.get("row_counts") or {}
    result: dict[str, tuple[int, int]] = {}
    bad: list[str] = []
    with (connect or _default_connect)() as conn:
        for name, (table, col) in DATE_COLUMNS.items():
            staged = int(counts.get(name, 0))
            db = int(conn.execute(f"SELECT count(*) FROM {table} WHERE {col} BETWEEN %s AND %s", (d0, d1)).fetchone()[0])
            result[name] = (staged, db)
            if db < staged:
                bad.append(f"{name}: 스테이징 {staged} > DB {db}")
    if bad:
        raise JobError("적재 후 행 수가 맞지 않습니다: " + "; ".join(bad))
    return result


def mark_stale_runs(*, older_than_minutes: int = 120, connect: Optional[Callable[[], Any]] = None) -> int:
    """SIGKILL·OOM 으로 'running' 에 멈춘 적재 기록을 failed 로 바꾼다(정상 실행은 2~3분이면 끝난다)."""
    with (connect or _default_connect)() as conn:
        cur = conn.execute(
            "UPDATE ops.load_run SET status='failed', finished_at=now(), error='stale: 프로세스가 끝나기 전에 사라짐' "
            "WHERE status='running' AND started_at < now() - make_interval(mins => %s)", (older_than_minutes,))
        return cur.rowcount or 0


# ------------------------------------------------------------------ 한 번에 (백필용) · 알림

def process_window(d0: date, d1: date, *, dag_id: str, run_id: str, try_number: int = 1, refresh_mart: bool = False,
                   base: Optional[Path] = None, keep_on_failure: bool = True,
                   stage: Callable[..., dict] = run_stage, load: Callable[..., dict] = run_load,
                   verify: Callable[..., Any] = verify_counts) -> dict:
    """스테이징 → manifest 검사 → 적재 → 행 수 검증 → 폴더 정리. 백필의 하루 단위 태스크가 쓴다."""
    base = Path(base or stage_base())
    sd = stage_dir_for(base, dag_id, run_id, try_number)
    started = datetime.now(timezone.utc)
    manifest = stage(d0, d1, sd)
    warnings = check_manifest(manifest, d0, d1, started_at=started)
    for w in warnings:
        LOG.warning(w)
    loaded = load(sd, refresh_mart=refresh_mart)
    counts = verify(manifest, d0, d1)
    cleanup_stage(base, sd)
    return {"window": [d0.isoformat(), d1.isoformat()], "run_id": loaded.get("run_id"), "counts": counts, "warnings": warnings}


def on_failure(context: Mapping[str, Any]) -> None:
    """Airflow on_failure_callback. 알림 함수(urbanstream.notify.send)로 넘긴다 — 내용은 Step 8 에서 확장."""
    from urbanstream import notify
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(ti, "dag_id", None) or (context.get("dag") and getattr(context["dag"], "dag_id", "?")) or "?"
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id") or getattr(ti, "run_id", "?")
    exc = context.get("exception")
    body = f"run_id={run_id} try={getattr(ti, 'try_number', '?')} {type(exc).__name__ if exc else ''}: {exc if exc else ''}"
    notify.send("error", f"Airflow 실패: {dag_id}.{task_id}", body, dedup_key=f"airflow:{dag_id}.{task_id}")
