"""Silver 재처리 버전과 날짜별 포인터 (STEP 1, docs/adr/0015-reprocess.md). pyspark 를 가져오지 않는다.

폴더 모양
  <rebuild_root>/<표>/<버전>/<파티션열>=D/part-*.parquet   재처리 결과 (일반 배치 쓰기, _spark_metadata 없음)
  <rebuild_root>/<표>/<버전>/_build.json                   빌드 결과. status=ok 인 버전만 게시할 수 있다
  <rebuild_root>/_active/<표>.json                         날짜별 포인터 {"dates": {"2026-11-10": "r0017t1"}, "history": [...]}

읽는 쪽(streaming/silver_read.py)은 날짜 D 에 포인터가 있으면 그 버전만, 없으면 스트리밍 루트(data/silver)를 본다.
게시는 임시 파일에 쓴 뒤 os.replace 한 번이다. 그 전에 무엇이 실패해도 읽는 쪽은 예전 포인터를 본다.
스트리밍 폴더(data/silver, _spark_metadata, 체크포인트)는 이 모듈이 절대 건드리지 않는다.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ACTIVE_DIR = "_active"
BUILD_FILE = "_build.json"
HISTORY_KEEP = 200
PRUNE_KEEP_HOURS = 72.0
PRUNE_KEEP_RECENT = 2
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_TABLE = re.compile(r"^silver_[a-z_]+$")


class PointerError(RuntimeError):
    """포인터나 버전 폴더가 예상과 다르다. 조용히 스트리밍 루트로 돌아가지 않고 멈춘다."""


# ------------------------------------------------------------------ 이름 · 경로

def check_version(v: str) -> str:
    if not _VERSION.match(v or "") or v in (".", ".."):
        raise PointerError(f"버전 이름이 규칙에 맞지 않습니다: {v!r}")
    return v


def check_table(t: str) -> str:
    if not _TABLE.match(t or ""):
        raise PointerError(f"Silver 표 이름이 아닙니다: {t!r}")
    return t


def version_name(reprocess_id: Optional[int], try_number: int = 1) -> str:
    """ops.reprocess_run 행이 있으면 r0017t1, 없으면(수동 실행) m20261122T080000."""
    if reprocess_id is None:
        return datetime.now(timezone.utc).strftime("m%Y%m%dT%H%M%S")
    return f"r{int(reprocess_id):04d}t{int(try_number)}"


def version_dir(root: Path, table: str, version: str) -> Path:
    return Path(root) / check_table(table) / check_version(version)


def pointer_path(root: Path, table: str) -> Path:
    return Path(root) / ACTIVE_DIR / f"{check_table(table)}.json"


# ------------------------------------------------------------------ 날짜 가드

def kst_today(now: Optional[datetime] = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(KST).date()


def check_dates(d0: date, d1: date, *, allow_recent: bool = False, now: Optional[datetime] = None, max_days: int = 31) -> list[date]:
    """재처리할 수 있는 날짜 목록. 기본은 KST 그제 이전만(오늘·어제는 스트리밍이 아직 행을 붙인다)."""
    if d0 > d1:
        raise PointerError(f"시작일({d0})이 종료일({d1})보다 늦습니다")
    n = (d1 - d0).days + 1
    if n > max_days:
        raise PointerError(f"한 번에 {max_days}일까지만 허용합니다 (요청 {n}일)")
    limit = kst_today(now) - timedelta(days=2)
    if d1 > limit and not allow_recent:
        raise PointerError(f"{d1} 는 너무 최근입니다. 재처리는 KST 그제({limit})까지만 받습니다 (수동 실행에서만 --allow-recent)")
    return [d0 + timedelta(days=i) for i in range(n)]


# ------------------------------------------------------------------ 파일 쓰기

def write_json_atomic(path: Path, data) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@contextmanager
def _lock(root: Path):
    """포인터 읽기-수정-쓰기를 한 번에 하나만 (게시·되돌리기가 겹치지 않게). 읽기만 하는 쪽은 잠그지 않는다."""
    p = Path(root) / ACTIVE_DIR
    p.mkdir(parents=True, exist_ok=True)
    with open(p / ".lock", "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


# ------------------------------------------------------------------ 포인터 읽기

def load_pointer(root: Optional[Path], table: str) -> dict:
    """포인터 전체. 파일이 없으면 빈 포인터(= 모든 날짜가 스트리밍 루트). 깨졌으면 PointerError."""
    empty = {"table": table, "dates": {}, "history": []}
    if root is None:
        return empty
    p = pointer_path(root, table)
    if not p.exists():
        return empty
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        dates = d["dates"]
        if not isinstance(dates, dict):
            raise TypeError("dates 가 객체가 아님")
        for k, v in dates.items():
            date.fromisoformat(k)
            check_version(v)
    except (ValueError, KeyError, TypeError, PointerError) as e:
        raise PointerError(f"포인터 파일이 깨졌습니다: {p} ({e})") from e
    d.setdefault("history", [])
    return d


def active_dates(root: Optional[Path], table: str) -> dict[str, str]:
    """{날짜 문자열: 버전}. 읽는 잡은 시작할 때 이것을 한 번 읽어 스냅샷으로 쓴다."""
    return dict(load_pointer(root, table)["dates"])


def snapshot(root: Optional[Path], tables: Iterable[str]) -> dict[str, dict[str, str]]:
    return {t: active_dates(root, t) for t in tables}


def snapshot_sha(snap: Mapping[str, Mapping[str, str]]) -> str:
    """Gold manifest 에 남길 지문. 포인터가 하나도 없으면 고정값 'none'."""
    if not any(snap.values()):
        return "none"
    return hashlib.sha256(json.dumps(snap, sort_keys=True).encode()).hexdigest()[:16]


def load_build(root: Path, table: str, version: str) -> dict:
    p = version_dir(root, table, version) / BUILD_FILE
    if not p.exists():
        raise PointerError(f"빌드 기록이 없습니다(미완성 버전): {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def version_inputs(root: Path, table: str, dates: Mapping[str, str]) -> dict[str, list[str]]:
    """포인터 스냅샷 → {버전: [읽을 파티션 폴더 경로]}. 행이 0 인 날짜는 폴더가 없으므로 뺀다.
    빌드 기록이 없거나 ok 가 아니거나, 행이 있다던 폴더가 없으면 PointerError (조용히 스트리밍 루트로 돌아가지 않는다)."""
    by_version: dict[str, list[str]] = {}
    for d, v in sorted(dates.items()):
        b = load_build(root, table, v)
        if b.get("status") != "ok":
            raise PointerError(f"{table} {v}: 빌드 상태가 ok 가 아닙니다 ({b.get('status')})")
        n = (b.get("dates") or {}).get(d)
        if n is None:
            raise PointerError(f"{table} {v}: 빌드에 {d} 가 없습니다")
        part = version_dir(root, table, v) / f"{b['partition_col']}={d}"
        if n > 0 and not part.is_dir():
            raise PointerError(f"{table} {v}: {d} 는 {n}행이라는데 폴더가 없습니다: {part}")
        by_version.setdefault(v, [])
        if n > 0:
            by_version[v].append(str(part))
    return by_version


# ------------------------------------------------------------------ 게시 · 되돌리기

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def publish(root: Path, table: str, version: str, days: Iterable[date], *, reprocess_id: Optional[int] = None,
            note: str = "") -> dict[str, Optional[str]]:
    """days 의 포인터를 version 으로 바꾼다. 반환 = 바꾸기 전 값 {날짜: 버전 | None}. 같은 값으로 다시 게시하면 아무것도 바뀌지 않는다(멱등)."""
    b = load_build(root, table, version)
    if b.get("status") != "ok":
        raise PointerError(f"{table} {version}: 빌드 상태가 ok 가 아니라 게시할 수 없습니다 ({b.get('status')})")
    keys = [d.isoformat() for d in days]
    missing = [k for k in keys if k not in (b.get("dates") or {})]
    if missing:
        raise PointerError(f"{table} {version}: 빌드에 없는 날짜를 게시하려 합니다: {missing}")
    with _lock(root):
        ptr = load_pointer(root, table)
        prev = {k: ptr["dates"].get(k) for k in keys}
        if all(prev[k] == version for k in keys):
            return {k: _prev_before(ptr, k, version) for k in keys}      # 재시도: 이미 게시됨
        for k in keys:
            ptr["dates"][k] = version
        ptr["history"] = (ptr["history"] + [{"at": _now_iso(), "action": "publish", "version": version,
                                             "reprocess_id": reprocess_id, "previous": prev, "note": note}])[-HISTORY_KEEP:]
        ptr["table"] = table
        write_json_atomic(pointer_path(root, table), ptr)
    return prev


def _prev_before(ptr: dict, day: str, version: str) -> Optional[str]:
    """이미 게시된 경우 이력에서 그 게시 직전 값을 찾는다 (재시도가 previous 를 잃지 않게)."""
    for h in reversed(ptr.get("history", [])):
        if h.get("action") == "publish" and h.get("version") == version and day in (h.get("previous") or {}):
            return h["previous"][day]
    return None


def rollback(root: Path, table: str, previous: Mapping[str, Optional[str]], *, expect_version: Optional[str] = None,
             reprocess_id: Optional[int] = None, force: bool = False) -> dict[str, Optional[str]]:
    """previous({날짜: 버전 | None})로 되돌린다. None 이면 그 날짜를 포인터에서 빼서 스트리밍 루트로 돌아간다.
    expect_version 을 주면, 지금 포인터가 그 버전이 아닌 날짜(그 뒤 다른 재처리가 게시함)는 force 없이는 거부한다."""
    with _lock(root):
        ptr = load_pointer(root, table)
        cur = {k: ptr["dates"].get(k) for k in previous}
        if expect_version and not force:
            other = {k: v for k, v in cur.items() if v != expect_version}
            if other:
                raise PointerError(f"{table}: 지금 포인터가 {expect_version} 가 아닌 날짜가 있습니다 {other} (다른 재처리가 그 뒤에 게시함). --force 로만 진행")
        for k, v in previous.items():
            if v is None:
                ptr["dates"].pop(k, None)
            else:
                load_build(root, table, check_version(v))
                ptr["dates"][k] = v
        ptr["history"] = (ptr["history"] + [{"at": _now_iso(), "action": "rollback", "version": expect_version,
                                             "reprocess_id": reprocess_id, "previous": cur}])[-HISTORY_KEEP:]
        write_json_atomic(pointer_path(root, table), ptr)
    return cur


def find_publish(root: Path, table: str, reprocess_id: int) -> Optional[dict]:
    """포인터 이력에서 그 재처리의 마지막 게시 기록 (DB 없이 되돌릴 때 쓴다)."""
    for h in reversed(load_pointer(root, table).get("history", [])):
        if h.get("action") == "publish" and h.get("reprocess_id") == reprocess_id:
            return h
    return None


# ------------------------------------------------------------------ 옛 버전 정리

def prune(root: Path, table: str, *, keep_hours: float = PRUNE_KEEP_HOURS, keep_recent: int = PRUNE_KEEP_RECENT,
          now: Optional[float] = None, dry_run: bool = False) -> list[str]:
    """포인터가 가리키지 않고, keep_hours 보다 오래됐고, 최근 keep_recent 개 밖인 버전 폴더만 지운다.
    읽는 중인 잡이 옛 스냅샷으로 그 폴더를 보고 있을 수 있어서 게시 직후에는 지우지 않는다."""
    tdir = Path(root) / check_table(table)
    if not tdir.is_dir():
        return []
    used = set(active_dates(root, table).values())
    vers = sorted((p for p in tdir.iterdir() if p.is_dir() and not p.is_symlink() and _VERSION.match(p.name)),
                  key=lambda p: p.stat().st_mtime, reverse=True)
    cutoff = (time.time() if now is None else now) - keep_hours * 3600
    removed = []
    for i, p in enumerate(vers):
        if p.name in used or i < keep_recent or p.stat().st_mtime >= cutoff:
            continue
        if not dry_run:
            shutil.rmtree(p)
        removed.append(p.name)
    return removed
