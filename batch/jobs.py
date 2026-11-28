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
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

LOG = logging.getLogger("urbanstream.batch.jobs")
KST = ZoneInfo("Asia/Seoul")
ROOT = Path(os.environ.get("URBANSTREAM_ROOT") or Path(__file__).resolve().parents[1])
DEFAULT_STAGE_BASE = "/data/gold_stage"
DEFAULT_SILVER = "/data/silver"
DEFAULT_BRONZE = "/data/bronze"
DEFAULT_REBUILD = "/data/silver_rebuild"
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


# ------------------------------------------------------------------ 작업 잠금 (STEP 3B, D-212 재발 방지)

LOCK_WAIT_SEC = 600          # 잠금을 못 잡으면 이만큼 기다린다(그 뒤 실패 + 알림)
LOCK_POLL_SEC = 30
LOCK_LABEL_PREFIX = "uslock:"
LOCK_NAMESPACE = 3217        # pg_try_advisory_lock(int, int) 의 첫 번째 값. 다른 advisory lock(적재의 xact lock)과 섞이지 않게


class LockBusy(JobError):
    """같은 종류의 작업이 이미 돌고 있다(고아로 판정된 이전 시도 등)."""


def lock_key(name: str) -> int:
    """작업 이름 → 32비트 정수(두 번째 키). 파이썬 hash() 는 실행마다 달라서 쓰지 않는다."""
    import zlib
    return zlib.crc32(name.encode("utf-8")) & 0x7FFFFFFF


def lock_label(name: str, env: Optional[Mapping[str, str]] = None) -> str:
    """pg_stat_activity.application_name 에 남길 이름(63바이트에서 잘림). 'uslock:<작업>:<dag>#<시도>:pid<N>:<run_id>'.
    누가 잠금을 잡았는지 diagnose 가 보여 준다. 잘리면 뒤쪽 run_id 부터 잘린다."""
    e = os.environ if env is None else env
    dag, run, tr = e.get("AIRFLOW_CTX_DAG_ID") or "manual", e.get("AIRFLOW_CTX_DAG_RUN_ID") or "", e.get("AIRFLOW_CTX_TRY_NUMBER") or "-"
    return f"{LOCK_LABEL_PREFIX}{name}:{dag}#{tr}:pid{os.getpid()}:{run}"[:63]


def _lock_connect(label: str):
    import psycopg
    from batch.db import conninfo
    return psycopg.connect(conninfo(role="writer"), autocommit=True, application_name=label, connect_timeout=10)


@contextmanager
def job_lock(name: str, *, connect: Optional[Callable[[str], Any]] = None, wait_sec: float = LOCK_WAIT_SEC,
             poll_sec: float = LOCK_POLL_SEC, sleep: Callable[[float], None] = time.sleep,
             clock: Callable[[], float] = time.monotonic) -> Iterator[None]:
    """같은 이름의 작업이 동시에 두 개 돌지 않게 한다. PostgreSQL **세션** advisory lock 이다.

    - 작업이 끝나면 풀고, 프로세스가 죽어 접속이 끊기면 서버가 저절로 푼다(파일 잠금처럼 남은 찌꺼기를 지울 일이 없다).
    - 못 잡으면 poll_sec 마다 다시 시도하고 wait_sec 가 지나면 LockBusy. 잡고 있는 쪽의 application_name·시작 시각을 메시지에 넣는다.
    - 잠금 전용 접속을 따로 연다. 작업의 적재 접속이 끊겨도 잠금은 작업이 끝날 때까지 유지된다.
    - connect(label) 는 시험용. 기본은 gold_loader(writer)로 붙는다.
    """
    conn = (connect or _lock_connect)(lock_label(name))
    key = lock_key(name)
    try:
        t0 = clock()
        while True:
            got = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (LOCK_NAMESPACE, key)).fetchone()[0]
            if got:
                break
            if clock() - t0 >= wait_sec:
                holder = lock_holder(conn, name)
                raise LockBusy(f"이전 시도가 아직 실행 중입니다 ({name}: {holder or '누가 잡았는지 모름'}). "
                               f"{int(wait_sec)}초 기다렸지만 풀리지 않았습니다. 멈춘 시도라면 "
                               f"`python scripts/recover.py diagnose` 가 잠금을 푸는 질의를 보여 줍니다")
            LOG.info("작업 잠금 %s 대기 중 (%s)", name, lock_holder(conn, name) or "?")
            sleep(poll_sec)
        LOG.info("작업 잠금을 잡았습니다: %s", name)
        try:
            yield
        finally:
            try:
                conn.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_NAMESPACE, key))
            except Exception:                                                # noqa: BLE001 (접속이 끊겼으면 서버가 이미 풀었다)
                LOG.warning("작업 잠금 해제 실패(접속이 끊겼으면 이미 풀렸다): %s", name)
    finally:
        try:
            conn.close()
        except Exception:                                                    # noqa: BLE001
            pass


def lock_holder(conn: Any, name: str) -> Optional[str]:
    """잠금을 잡은 접속의 'application_name (pid N, 시작 HH:MM)'. 못 읽으면 None."""
    try:
        r = conn.execute(
            "SELECT a.application_name, a.pid, to_char(a.backend_start AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') "
            "FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE l.locktype = 'advisory' AND l.granted AND l.classid = %s AND l.objid = %s AND l.objsubid = 2 LIMIT 1",
            (LOCK_NAMESPACE, lock_key(name))).fetchone()
    except Exception:                                                        # noqa: BLE001
        return None
    return f"{r[0]} (pid {r[1]}, 접속 {r[2]})" if r else None


@contextmanager
def no_lock(name: str, **_: Any) -> Iterator[None]:
    """잠금 없이 (시험, 또는 DB 없이 손으로 돌릴 때)."""
    yield


LockFn = Callable[..., Any]
LOCK_INCREMENTAL = "gold_facts_incremental"
LOCK_WINDOW = "gold_window"
LOCK_INLINE = "gold_stage_inline"
LOCK_RECONCILE = "silver_reconcile"


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
    rebuild = src.get("GOLD_SILVER_REBUILD_ROOT", DEFAULT_REBUILD)
    env.update({
        "GOLD_SILVER_ROOT": str(silver_root), "GOLD_BRONZE_ROOT": str(bronze_root), "GOLD_STAGE_ROOT": str(stage_dir),
        "GOLD_SILVER_REBUILD_ROOT": rebuild, "SILVER_REBUILD_ROOT": rebuild,
        "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "src")]),
        "PYTHONDONTWRITEBYTECODE": "1",
        # pyspark 가 JVM 을 띄울 때 쓰는 인자 (드라이버 힙·스레드). 컨테이너 한도는 이 값의 1.4배 이상이어야 한다(정책 테스트).
        "PYSPARK_SUBMIT_ARGS": f"--master {master} --driver-memory {driver_memory} pyspark-shell",
    })
    return env


def _tail(text: str, n: int) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-n:])


def stage_mode(env: Optional[Mapping[str, str]] = None) -> str:
    """'runner'(gold-runner 컨테이너에 요청) 또는 'inline'(이 프로세스에서 Spark 실행). 값이 없으면 inline.

    기본값을 inline 으로 둔 이유: Airflow 는 코드 폴더를 직접 마운트하므로 파일을 바꾸는 순간 새 코드가 쓰인다. 컨테이너를
    다시 만들기 전(= GOLD_STAGE_MODE 가 환경에 들어가기 전)에는 예전과 똑같이 동작해야 실행 중인 스택이 깨지지 않는다.
    """
    e = os.environ if env is None else env
    mode = (e.get("GOLD_STAGE_MODE") or "inline").strip().lower()
    if mode not in ("inline", "runner"):
        raise JobError(f"GOLD_STAGE_MODE 는 inline 또는 runner 여야 합니다: {mode!r}")
    return mode


def run_stage(d0: date, d1: date, stage_dir: Path, *, files_since: Optional[float] = None,
              files_until: Optional[float] = None, **kw) -> dict:
    """스테이징 한 번. 방식은 GOLD_STAGE_MODE 로 고른다(stage_mode). files_until 이 있으면 증분(D-278)."""
    if stage_mode() == "runner":
        return request_stage(d0, d1, stage_dir, timeout_sec=kw.get("timeout_sec", 1800),
                             files_since=files_since, files_until=files_until)
    return run_stage_inline(d0, d1, stage_dir, files_since=files_since, files_until=files_until, **kw)


RUNNER_STALE_SEC = 180


def runner_alive(base: Path, *, stale_sec: float = RUNNER_STALE_SEC, now: Optional[float] = None) -> bool:
    from batch.stage_runner import ALIVE, requests_dir
    p = requests_dir(base) / ALIVE
    return p.exists() and ((time.time() if now is None else now) - p.stat().st_mtime) < stale_sec


def request_key(stage_dir: Path) -> str:
    """같은 DAG 실행의 같은 구간이면 같은 key. 폴더 이름 'dag__run__tryN' 에서 시도 번호만 뺀다 → 고아 재시도의 중복 요청이 묶인다."""
    return re.sub(r"__try\d+$", "", Path(stage_dir).name)


def find_in_flight(req_dir: Path, key: str) -> Optional[dict]:
    """같은 key 로 아직 끝나지 않았고 취소되지도 않은 요청(가장 새 것). 없으면 None.
    Airflow 가 태스크를 죽인 뒤(execution_timeout, 고아 판정) 재시도가 같은 계산을 처음부터 다시 요청하지 않게 한다."""
    from batch.stage_runner import TERMINAL, marker
    found = []
    for p in Path(req_dir).glob("*.json"):
        rid = p.stem
        if any(marker(req_dir, rid, k).exists() for k in (*TERMINAL, "cancel")):
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            continue
        if d.get("key") == key:
            found.append(d)
    return max(found, key=lambda d: float(d.get("created_at") or 0)) if found else None


def request_runner(kind: str, d0: date, d1: date, stage_dir: Path, *, args: Optional[dict] = None, base: Optional[Path] = None,
                   timeout_sec: int = 1800, poll_sec: float = 5.0, sleep: Callable[[float], None] = time.sleep,
                   clock: Callable[[], float] = time.monotonic, stale_sec: float = RUNNER_STALE_SEC,
                   files_since: Optional[float] = None, files_until: Optional[float] = None,
                   stale_grace_sec: float = 30.0) -> dict:
    """gold-runner 에 요청 파일을 남기고 결과를 기다린다. 이 프로세스는 Spark 를 띄우지 않는다(메모리를 거의 쓰지 않는다).
    반환 = 실행기의 done 표시 내용 + '_stage_dir'(실제로 쓰인 폴더). kind 가 gold_stage 가 아니면 key 앞에 kind 를 붙여
    Gold 요청과 supersede 되지 않게 한다.

    - gold_stage 는 같은 실행의 이전 시도가 남긴 요청이 아직 줄에 있거나 도는 중이면 새로 만들지 않고 그 결과를 기다린다(재연결).
      silver_rebuild·silver_publish·silver_verify 는 재연결하지 않는다(결과가 자기 요청 파일·폴더에 있다).
    - timeout_sec 는 줄 서기와 실행에 각각 적용한다: 실행이 시작되면(.running) 그때부터 다시 timeout_sec 를 센다.
    - 기다리는 동안 실행기 박동이 stale_sec 넘게 멈추면 실행기가 죽은 것이므로 바로 실패한다(실행기는 Spark 실행 중에도 박동한다).
    - 대기 한도는 clock(기본 time.monotonic)으로 센다: Mac 이 잠든 시간은 들어가지 않는다(11-23 01:00 gold_daily 는 계산 538초인데
      잠자기 1시간이 벽시계에 더해져 취소됐다). 요청 시각(created_at)과 박동 비교는 벽시계다(실행기와 같은 시계).
    - 재연결한 경우 증분 구간도 그 요청의 것이다. 호출 쪽은 반환된 manifest 의 files_until 을 쓴다.
    """
    from batch.stage_runner import marker, requests_dir, write_json_atomic
    if d0 > d1:
        raise ValueError("d0 > d1")
    base = Path(base or stage_base())
    if not _inside(base, Path(stage_dir)):
        raise JobError(f"스테이징 폴더가 base 안이 아닙니다: {stage_dir} (base={base})")
    req_dir = requests_dir(base)
    req_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(req_dir, 0o777)                     # 실행기(spark 사용자)가 같은 폴더에 표시 파일을 쓴다
    except PermissionError:
        pass
    if not runner_alive(base, stale_sec=stale_sec):
        raise JobError("gold-runner 가 응답하지 않습니다(박동 파일이 없거나 오래됨). `scripts/stack.sh up-airflow` 로 띄우거나, "
                       "임시로 GOLD_STAGE_MODE=inline 으로 되돌리세요")
    started = clock()
    wall = time.time()
    key = request_key(stage_dir) if kind == "gold_stage" else f"{kind}:{request_key(stage_dir)}"
    prev = find_in_flight(req_dir, key) if kind == "gold_stage" else None
    if prev and prev.get("date_from") == d0.isoformat() and prev.get("date_to") == d1.isoformat() \
            and _inside(base, Path(prev.get("stage_dir", ""))):
        rid, stage_dir = prev["id"], Path(prev["stage_dir"])
        LOG.info("이전 시도의 요청이 아직 남아 있어 그 결과를 기다립니다: %s", rid)
    else:
        rid = f"{int(wall * 1000)}__{Path(stage_dir).name}"
        body = {"id": rid, "key": key, "date_from": d0.isoformat(), "date_to": d1.isoformat(), "stage_dir": str(stage_dir),
                "created_at": wall}
        if kind != "gold_stage":                               # gold_stage 요청은 3A 때와 같은 모양 (kind 없는 요청 = gold_stage)
            body.update(kind=kind, args=args or {})
        if files_until is not None:
            body.update(files_since=files_since, files_until=files_until)
        write_json_atomic(req_dir / f"{rid}.json", body)
        LOG.info("gold-runner 에 요청: %s %s (%s ~ %s)", kind, rid, d0, d1)
    deadline = started + timeout_sec
    run_started: Optional[float] = None
    stale_seen: Optional[float] = None
    while True:
        for end in ("done", "failed", "superseded"):
            m = marker(req_dir, rid, end)
            if m.exists():
                res = json.loads(m.read_text(encoding="utf-8") or "{}")
                if end == "done":
                    LOG.info("%s 완료 (%.1f초, 실행기)", kind, res.get("elapsed_sec") or 0)
                    res["_stage_dir"] = str(stage_dir)
                    res["_id"] = rid
                    return res
                if end == "superseded":
                    raise JobError(f"같은 구간의 더 새 요청이 있어 이 요청은 건너뛰었습니다: {res.get('by')}")
                raise JobError(f"{kind} 실패 (종료 코드 {res.get('returncode')})\n{res.get('stderr_tail', '')}")
        now = clock()
        if run_started is None and marker(req_dir, rid, "running").exists():
            run_started = now
            deadline = max(deadline, now + timeout_sec)
            LOG.info("실행기가 %s 를 시작했습니다 (줄 선 시간 %.0f초)", kind, now - started)
        if now > deadline:
            marker(req_dir, rid, "cancel").write_text("timeout", encoding="utf-8")
            phase = "실행" if run_started is not None else "줄 서기"
            raise JobError(f"{kind} 가 {phase} {timeout_sec}초 안에 끝나지 않았습니다 (실행기 요청 {rid}). 실행기에 취소를 남겼습니다")
        if runner_alive(base, stale_sec=stale_sec):
            stale_seen = None
        elif stale_seen is None:
            stale_seen = now                     # 잠자기에서 깬 직후에는 실행기도 막 깨어나 박동이 잠깐 오래돼 보인다
        elif now - stale_seen > stale_grace_sec:
            raise JobError(f"기다리는 동안 gold-runner 박동이 {int(stale_sec)}초 넘게 멈췄습니다(실행기가 죽었거나 멈춤). "
                           f"요청 {rid} 은 실행기가 다시 뜨면 이어서 처리합니다")
        sleep(poll_sec)


def request_stage(d0: date, d1: date, stage_dir: Path, **kw) -> dict:
    """gold_stage 를 gold-runner 에 맡기고 manifest 를 돌려준다. 재연결했으면 '_stage_dir' 이 실제 폴더다."""
    res = request_runner("gold_stage", d0, d1, stage_dir, **kw)
    sd = Path(res.get("_stage_dir") or stage_dir)
    manifest = read_manifest(sd)
    manifest["_stage_dir"] = str(sd)
    return manifest


def run_stage_inline(d0: date, d1: date, stage_dir: Path, *, silver_root: Optional[str] = None, bronze_root: Optional[str] = None,
              driver_memory: Optional[str] = None, master: Optional[str] = None, timeout_sec: int = 1800,
              runner: Callable[..., Any] = subprocess.run, python: Optional[str] = None,
              files_since: Optional[float] = None, files_until: Optional[float] = None,
              lock: Optional[LockFn] = None) -> dict:
    """Silver·Bronze audit → stage_dir (Parquet + manifest.json). 비밀 없는 환경에서 `python -m batch.gold_stage` 를 이 프로세스의 자식으로 실행한다.

    GOLD_STAGE_MODE=inline 일 때 쓴다(예전 방식, CI·호스트 실행용). Airflow 컨테이너에서는 Spark 메모리가 scheduler 와 겹친다(D-211).
    """
    if d0 > d1:
        raise ValueError("d0 > d1")
    Path(stage_dir).mkdir(parents=True, exist_ok=True)
    env = stage_env(
        stage_dir, silver_root=silver_root or os.environ.get("GOLD_SILVER_ROOT", DEFAULT_SILVER),
        bronze_root=bronze_root or os.environ.get("GOLD_BRONZE_ROOT", DEFAULT_BRONZE),
        driver_memory=driver_memory or os.environ.get("GOLD_DRIVER_MEMORY", "1g"),
        master=master or os.environ.get("GOLD_SPARK_MASTER", "local[2]"))
    from batch.stage_runner import files_args
    cmd = [python or sys.executable, "-m", "batch.gold_stage", "--from", d0.isoformat(), "--to", d1.isoformat(),
           *files_args(files_since, files_until)]
    started = time.monotonic()
    LOG.info("gold_stage 시작: %s ~ %s → %s", d0, d1, stage_dir)
    try:
        with (lock or job_lock)(LOCK_INLINE):         # 되돌리기 모드에서도 Spark 가 이 컨테이너에 두 개 뜨지 않게 (STEP 3B)
            proc = runner(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as e:
        raise JobError(f"gold_stage 가 {timeout_sec}초 안에 끝나지 않았습니다\n{_tail(_text(e.stderr), 40)}") from e
    if proc.returncode != 0:
        raise JobError(f"gold_stage 실패 (종료 코드 {proc.returncode})\n{_tail(proc.stderr, 60)}")
    if proc.stderr:
        LOG.info("gold_stage stderr 끝부분:\n%s", _tail(proc.stderr, 15))
    LOG.info("gold_stage 완료 (%.1f초)", time.monotonic() - started)
    return read_manifest(stage_dir)


def _text(v: Any) -> str:
    """TimeoutExpired.stderr 는 bytes 이거나 None 일 수 있다."""
    if v is None:
        return ""
    return v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)


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


def run_load(stage_dir: Path, *, refresh_mart: bool = False, connect: Optional[Callable[[], Any]] = None,
             dag_id: Optional[str] = None, **replace_kw) -> dict:
    """replace_kw: 재처리 경로의 범위 교체 인자 (mode="replace", replace=..., d0=..., d1=...). 평소에는 비어 있다."""
    from batch.gold_load import load_all
    extra = {"dag_id": dag_id} if dag_id is not None else {}            # 모르면 넘기지 않는다 → 009 이전 호출 모양과 같다
    with (connect or _default_connect)() as conn:
        return load_all(conn, Path(stage_dir), refresh_mart=refresh_mart, **extra, **replace_kw)


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


def verify_counts(manifest: Mapping[str, Any], d0: date, d1: date, *, connect: Optional[Callable[[], Any]] = None,
                  exact: Iterable[str] = ()) -> dict[str, tuple[int, int]]:
    """적재 뒤 확인: 날짜 범위의 DB 행 수 ≥ 스테이징 행 수 (팩트는 DO NOTHING 이라 DB 가 더 많은 것은 정상일 수 있다).
    DB 가 더 적으면 일부가 들어가지 못한 것이다 → JobError. 반환 = {표: (스테이징, DB)}.
    exact 에 든 표(재처리로 범위를 교체한 표)는 같아야 한다(등호)."""
    counts = manifest.get("row_counts") or {}
    exact = set(exact)
    result: dict[str, tuple[int, int]] = {}
    bad: list[str] = []
    with (connect or _default_connect)() as conn:
        for name, (table, col) in DATE_COLUMNS.items():
            staged = int(counts.get(name, 0))
            db = int(conn.execute(f"SELECT count(*) FROM {table} WHERE {col} BETWEEN %s AND %s", (d0, d1)).fetchone()[0])
            result[name] = (staged, db)
            if db < staged:
                bad.append(f"{name}: 스테이징 {staged} > DB {db}")
            elif name in exact and db != staged:
                bad.append(f"{name}: 범위 교체 뒤 스테이징 {staged} ≠ DB {db}")
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
                   verify: Callable[..., Any] = verify_counts, lock: Optional[LockFn] = None) -> dict:
    """스테이징 → manifest 검사 → 적재 → 행 수 검증 → 폴더 정리. 백필의 하루 단위 태스크가 쓴다.
    작업 잠금(LOCK_WINDOW) 안에서 돈다: 고아로 판정된 이전 시도가 아직 돌면 기다리고, 10분 넘게 안 풀리면 실패한다."""
    with (lock or job_lock)(LOCK_WINDOW):
        return _process_window(d0, d1, dag_id=dag_id, run_id=run_id, try_number=try_number, refresh_mart=refresh_mart,
                               base=base, stage=stage, load=load, verify=verify)


def _process_window(d0: date, d1: date, *, dag_id: str, run_id: str, try_number: int, refresh_mart: bool,
                    base: Optional[Path], stage: Callable[..., dict], load: Callable[..., dict],
                    verify: Callable[..., Any]) -> dict:
    base = Path(base or stage_base())
    sd = stage_dir_for(base, dag_id, run_id, try_number)
    started = datetime.now(timezone.utc)
    manifest = stage(d0, d1, sd)
    other = manifest.pop("_stage_dir", None)               # 재시도가 이전 시도의 요청에 다시 붙은 경우 그 폴더 (request_stage)
    if other and Path(other) != sd:
        if not _inside(base, Path(other)):
            raise JobError(f"스테이징 폴더가 base 안이 아닙니다: {other} (base={base})")
        LOG.info("이전 시도의 스테이징 폴더를 씁니다: %s", other)
        sd = Path(other)
        started = None                                     # 그 요청은 이번 시도 전에 시작됐을 수 있다 (날짜 범위 검사는 그대로)
    warnings = check_manifest(manifest, d0, d1, started_at=started)
    for w in warnings:
        LOG.warning(w)
    loaded = load(sd, refresh_mart=refresh_mart, dag_id=dag_id)
    counts = verify(manifest, d0, d1)
    cleanup_stage(base, sd)
    return {"window": [d0.isoformat(), d1.isoformat()], "run_id": loaded.get("run_id"), "counts": counts, "warnings": warnings}


# ------------------------------------------------------------------ 증분 (D-278)

STATE_DIRNAME = "_state"
STATE_KEY = "gold_facts"            # gold_intraday·gold_daily 가 같이 쓴다: '수정 시각이 until 이하인 Silver 파일은 다 적재했다'
INCR_OVERLAP_SEC = 300              # 다음 구간을 이만큼 겹쳐 시작한다(컨테이너·호스트 시계 차이). 겹친 행은 DO NOTHING
INCR_SETTLE_SEC = 120               # until = 지금 - 이 값: 아직 쓰는 중인 파일을 피한다
INCR_MAX_SPAN_SEC = 3 * 3600        # 한 번에 읽는 구간의 최대 길이. 밀려 있으면 여러 번에 나눠 따라잡는다
INCR_BUDGET_SEC = 30 * 60           # 태스크 하나가 따라잡기에 쓰는 시간 한도 (gold_intraday execution_timeout 45분 안)


def state_path(base: Path, key: str = STATE_KEY) -> Path:
    return Path(base) / STATE_DIRNAME / f"{key}.json"


def read_state(base: Path, key: str = STATE_KEY) -> Optional[dict]:
    p = state_path(base, key)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        return d if isinstance(d.get("until"), (int, float)) else None
    except (OSError, ValueError):
        return None


def write_state(base: Path, until: float, *, key: str = STATE_KEY, **info: Any) -> None:
    from batch.stage_runner import write_json_atomic
    p = state_path(base, key)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(p.parent, 0o777)
    except PermissionError:
        pass
    prev = read_state(base, key)
    if prev and prev["until"] > until:                   # 뒤로 가지 않는다 (재연결로 예전 구간 결과를 받은 경우 등)
        return
    write_json_atomic(p, {"until": until, "updated_at": time.time(), **info})


def kst_midnight_epoch(d: date) -> float:
    return datetime(d.year, d.month, d.day, tzinfo=KST).timestamp()


def incremental_window(d0: date, d1: date) -> tuple[date, date]:
    """증분이 볼 파티션 날짜: 적어도 전날부터. 자정 뒤에 쓰인 어제 날짜 파일(늦게 온 행)도 놓치지 않는다."""
    return min(d0, d1 - timedelta(days=1)), d1


def process_incremental(d0: date, d1: date, *, dag_id: str, run_id: str, try_number: int = 1, refresh_mart: bool = False,
                        base: Optional[Path] = None, stage: Callable[..., dict] = run_stage, load: Callable[..., dict] = run_load,
                        verify: Callable[..., Any] = verify_counts, wall: Callable[[], float] = time.time,
                        mono: Callable[[], float] = time.monotonic, max_span_sec: Optional[float] = None,
                        budget_sec: Optional[float] = None, refresh: Optional[Callable[[], None]] = None,
                        lock: Optional[LockFn] = None) -> dict:
    """새로 생긴 Silver 파일만 스테이징·적재한다. 진행 기록(_state/gold_facts.json 의 until)이 없으면 시작일 0시부터.

    밀려 있으면(잠자기, 실패 뒤) 최대 max_span_sec 씩 나눠 여러 번 돌린다. 한 번 끝날 때마다 진행 기록을 앞으로 옮기므로
    태스크가 중간에 죽어도 다음 실행이 이어서 한다. budget_sec 를 넘으면 남은 것은 다음 실행에 맡긴다.

    gold_intraday·gold_daily 가 같은 작업 잠금(LOCK_INCREMENTAL)을 쓴다. 진행 기록을 같이 쓰기 때문이다. 잠금을 기다린 뒤에는
    진행 기록을 다시 읽으므로, 앞 시도가 이미 따라잡았으면 할 일이 없어 바로 끝난다.
    """
    with (lock or job_lock)(LOCK_INCREMENTAL):
        return _process_incremental(d0, d1, dag_id=dag_id, run_id=run_id, try_number=try_number, refresh_mart=refresh_mart,
                                    base=base, stage=stage, load=load, verify=verify, wall=wall, mono=mono,
                                    max_span_sec=max_span_sec, budget_sec=budget_sec, refresh=refresh)


def _process_incremental(d0: date, d1: date, *, dag_id: str, run_id: str, try_number: int, refresh_mart: bool,
                         base: Optional[Path], stage: Callable[..., dict], load: Callable[..., dict],
                         verify: Callable[..., Any], wall: Callable[[], float], mono: Callable[[], float],
                         max_span_sec: Optional[float], budget_sec: Optional[float],
                         refresh: Optional[Callable[[], None]]) -> dict:
    base = Path(base or stage_base())
    w0, w1 = incremental_window(d0, d1)
    max_span = float(max_span_sec or os.environ.get("GOLD_INCR_MAX_SPAN_SEC") or INCR_MAX_SPAN_SEC)
    budget = float(budget_sec or os.environ.get("GOLD_INCR_BUDGET_SEC") or INCR_BUDGET_SEC)
    st = read_state(base)
    since = (st["until"] - INCR_OVERLAP_SEC) if st else kst_midnight_epoch(w0) - 1
    since = max(since, kst_midnight_epoch(w0) - 1)          # 볼 폴더보다 앞은 의미가 없다
    t0 = mono()
    chunks: list[dict] = []
    target = wall() - INCR_SETTLE_SEC
    while True:
        target = wall() - INCR_SETTLE_SEC
        if since >= target:
            break
        until = min(target, since + max_span)
        i = len(chunks)
        sd = stage_dir_for(base, dag_id, f"{run_id}_c{i}", try_number)
        started = datetime.now(timezone.utc)
        manifest = stage(w0, w1, sd, files_since=since, files_until=until)
        other = manifest.pop("_stage_dir", None)
        if other and Path(other) != sd:
            if not _inside(base, Path(other)):
                raise JobError(f"스테이징 폴더가 base 안이 아닙니다: {other} (base={base})")
            sd, started = Path(other), None
        warnings = [w for w in check_manifest(manifest, w0, w1, started_at=started) if "행이 0" not in w]
        for w in warnings:
            LOG.warning(w)
        loaded = load(sd, refresh_mart=False, dag_id=dag_id)
        counts = verify(manifest, w0, w1)
        cleanup_stage(base, sd)
        got_until = float(manifest.get("files_until") or until)       # 재연결했으면 그 요청의 구간
        write_state(base, got_until, until_kst=_fmt_ts(got_until), dag_id=dag_id, run_id=run_id)
        LOG.info("증분 %d: 파일 시각 %s ~ %s, 읽은 파일 %s", i, _fmt_ts(since), _fmt_ts(got_until), manifest.get("files_read"))
        chunks.append({"since": since, "until": got_until, "run_id": loaded.get("run_id"), "counts": counts,
                       "files_read": manifest.get("files_read")})
        if chunks[:-1] and got_until <= chunks[-2]["until"]:     # 앞으로 가지 못했다(이상 상황): 무한 반복하지 않는다
            LOG.warning("증분 기록이 앞으로 가지 않아 멈춥니다 (%s)", _fmt_ts(got_until))
            break
        since = got_until - INCR_OVERLAP_SEC
        if mono() - t0 > budget:
            LOG.warning("따라잡기 시간 한도(%d초)에 닿아 나머지는 다음 실행에 맡깁니다 (기록 %s)", budget, _fmt_ts(got_until))
            break
        if got_until >= target - 1:
            break
    if refresh_mart:
        (refresh or refresh_marts)()
    caught_up = not chunks or chunks[-1]["until"] >= target - 1
    if chunks and caught_up:
        LOG.info("따라잡음: 기록 %s", _fmt_ts(chunks[-1]["until"]))
    elif chunks:
        LOG.info("남은 구간은 다음 실행: 기록 %s, 목표 %s", _fmt_ts(chunks[-1]["until"]), _fmt_ts(target))
    return {"window": [w0.isoformat(), w1.isoformat()], "chunks": chunks, "caught_up": caught_up}


def _fmt_ts(t: Optional[float]) -> str:
    return datetime.fromtimestamp(t, KST).strftime("%m-%d %H:%M:%S") if t else "-"


def on_failure(context: Mapping[str, Any]) -> None:
    """Airflow on_failure_callback. 실패를 'pending' 으로 기록만 한다 — 보낼지·격하할지는 모니터가 잠자기 증거와 대조해 정한다 (ADR-0010 D8-5).

    pending 기록이 실패하면(DB 불가 등) 기존 notify.send 로 대체한다 → 실패 알림이 사라지는 일은 없다.
    알림이 이 콜백을 깨뜨리면 안 되므로 어떤 예외도 밖으로 내보내지 않는다.
    """
    from urbanstream import notify
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(ti, "dag_id", None) or (context.get("dag") and getattr(context["dag"], "dag_id", "?")) or "?"
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id") or getattr(ti, "run_id", "?")
    exc = context.get("exception")
    body = f"run_id={run_id} try={getattr(ti, 'try_number', '?')} {type(exc).__name__ if exc else ''}: {exc if exc else ''}"
    title, key = f"Airflow 실패: {dag_id}.{task_id}", f"airflow:{dag_id}.{task_id}"
    meta = {"dag_id": str(dag_id), "task_id": str(task_id), "run_id": str(run_id), "try_number": getattr(ti, "try_number", None)}
    for name, attr in (("started_at", "start_date"), ("ended_at", "end_date")):
        v = getattr(ti, attr, None)
        if hasattr(v, "isoformat"):
            meta[name] = v.isoformat()
    try:
        if not notify.enqueue("error", title, body, dedup_key=key, meta=meta):
            notify.send("error", title, body, dedup_key=key)
    except Exception:                                                        # noqa: BLE001
        LOG.exception("실패 알림 기록 중 오류 (무시)")
