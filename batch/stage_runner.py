"""Gold 스테이징 실행기. `gold-runner` 컨테이너(Spark 이미지)에서 상주하며 Airflow 가 남긴 요청을 하나씩 처리한다.

왜 따로 두나 (D-211, D-212, D-257)
  예전에는 Airflow scheduler 컨테이너 안에서 Spark JVM 을 서브프로세스로 띄웠다. 작업 하나가 약 1.6 GiB 를 썼고, 절전으로
  heartbeat 가 끊겨 같은 작업이 '고아'로 판정되면 두 번째 시도가 겹쳐 scheduler 가 메모리 한도를 넘었다.
  이제 Spark 는 이 컨테이너에서만 돈다. 실행기는 한 번에 요청 하나만 처리하므로 Spark 가 동시에 두 개 뜨지 않는다.

요청 규약 (모두 <stage_base>/_requests/ 아래 파일)
  <id>.json        요청. Airflow 가 임시 이름으로 쓴 뒤 이름을 바꿔 원자적으로 만든다.
                   {"id", "key", "date_from", "date_to", "stage_dir", "created_at", "kind", "args"[, "files_since", "files_until"]}
                   kind (STEP 1): gold_stage(기본값. kind 가 없는 예전 요청도 이것) / silver_rebuild(Spark) / silver_publish(Spark 없이 포인터 파일만).
                   args 는 kind 별 인자. silver_rebuild·silver_publish 의 결과는 stage_dir 의 rebuild_report.json·publish_report.json.
                   files_since/files_until 이 있으면 증분 gold_stage 다(gold_stage 머리말, D-278).
                   kind=measure (STEP 2, docs/runbooks/measure.md): 성능 측정 전용. args.job 이 MEASURE_JOBS 중 하나이고, 쓰는 경로는 모두
                   GOLD_MEASURE_ROOT(/data/measure) 아래, 읽는 경로는 그 아래이거나 운영 읽기 전용 루트(/data/bronze·silver·silver_rebuild)여야 한다.
                   stage_dir 도 /data/measure 아래다. 운영 포인터를 게시하지 않는다(silver_point 는 측정 폴더 안의 포인터만 바꾼다).
                   nice GOLD_RUNNER_MEASURE_NICE(기본 10), 시간 한도 GOLD_RUNNER_MEASURE_TIMEOUT_SEC(기본 1200초).
                   실행 중 컨테이너 cgroup 메모리(memory.current)를 poll 마다 읽어 최댓값을 .done 의 mem_max_bytes 에 남긴다.
  <id>.running     실행 중 (실행기가 만든다)
  <id>.done        성공. {"returncode": 0, "elapsed_sec", "stdout_tail"}
  <id>.failed      실패. {"returncode", "elapsed_sec", "stderr_tail"}
  <id>.superseded  같은 key(= 같은 DAG 실행의 같은 구간)의 더 새 요청이 있어 건너뜀. 고아 재시도로 생긴 중복 요청이 여기로 간다.
  <id>.cancel      Airflow 가 기다리다 포기했다는 표시. 아직 시작하지 않았으면 건너뛰고, 실행 중이면 Spark 를 끈다.
  <id>.out/.err    Spark 의 stdout·stderr (파이프 대신 파일: 출력이 많아도 막히지 않는다). 요청 파일과 같이 정리한다.
  .runner_alive    실행기 박동. 기다리는 동안과 Spark 실행 중에도 poll 마다 갱신한다(D-274). Airflow 와 healthcheck 가
                   이 파일의 수정 시각을 본다. 예전에는 요청 사이에만 갱신해서, Spark 가 3분을 넘으면 그동안 들어온 요청이
                   '응답 없음'으로 즉시 실패했다.

Spark 는 새 프로세스 그룹으로 띄운다. 시간 초과·취소 때 그룹 전체(spark-submit JVM 과 그 자식 python)를 끈다.
오래 줄을 선 요청(만든 지 GOLD_RUNNER_MAX_QUEUE_SEC, 기본 1시간 넘은 것)은 실행하지 않고 실패로 표시한다: 기다리던 Airflow 태스크는
이미 끝났고, 잠자기 뒤에 밀린 요청을 차례로 돌며 시간을 쓰지 않게 한다.

DB 접속 정보는 이 컨테이너에 없다. Spark 는 Parquet 만 만들고 적재는 Airflow 쪽 로더가 한다(ADR-0008 ⑤).

실행:  python3 -m batch.stage_runner            (컨테이너 기본 명령)
       python3 -m batch.stage_runner --once     (대기 중인 요청을 한 번 처리하고 끝낸다. 시험·수동 실행용)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

LOG = logging.getLogger("urbanstream.batch.stage_runner")
REQ_DIRNAME = "_requests"
ALIVE = ".runner_alive"
TERMINAL = ("done", "failed", "superseded")
LOGS = ("out", "err")
DEFAULT_TIMEOUT_SEC = 1800
# 재처리 빌드는 하루치 전체를 다시 만든다. Airflow 쪽 대기(reprocess.run_rebuild 3600초)와 같은 한도를 쓴다
DEFAULT_REBUILD_TIMEOUT_SEC = 3600
# 재처리는 급하지 않다. 낮은 CPU 우선순위로 돌려 스트리밍·Kafka·healthcheck 가 먼저 CPU 를 얻게 한다 (0 = 끔)
DEFAULT_REBUILD_NICE = 10
DEFAULT_MAX_QUEUE_SEC = 3600
DEFAULT_POLL_SEC = 5.0
KILL_GRACE_SEC = 15.0
KEEP_HOURS = 72.0
KINDS = ("gold_stage", "silver_rebuild", "silver_publish", "measure")
# STEP 2 측정 (kind=measure). 값 = (Spark 로 돌리는가, 성공으로 보는 종료 코드)
MEASURE_JOBS: dict[str, tuple[bool, tuple[int, ...]]] = {
    "silver_build": (True, (0,)),      # batch/silver_rebuild.py build: 입력 Bronze → 측정 폴더의 Silver 버전
    "silver_point": (False, (0,)),     # 측정 폴더 안의 포인터를 그 버전으로 (Spark 없음)
    "gold_stage": (True, (0,)),        # batch/gold_stage.py: 측정 Silver → 측정 스테이징
    "silver_verify": (True, (0, 1)),   # streaming/silver_verify.py: 읽기 시간만 잰다. 대조 실패(1)도 측정으로는 성공
}
DEFAULT_MEASURE_ROOT = "/data/measure"
DEFAULT_MEASURE_TIMEOUT_SEC = 1200
DEFAULT_MEASURE_NICE = 10
MEASURE_READ_ROOTS = ("/data/bronze", "/data/silver", "/data/silver_rebuild")
MEASURE_OUT_ARGS = ("rebuild_root", "stage_root", "report")
MEASURE_IN_ARGS = ("bronze_root", "silver_root", "silver_rebuild_root")
CGROUP_MEM = Path("/sys/fs/cgroup/memory.current")
REBUILD_REPORT = "rebuild_report.json"
PUBLISH_REPORT = "publish_report.json"


def requests_dir(stage_base: Path) -> Path:
    return Path(stage_base) / REQ_DIRNAME


def marker(req_dir: Path, req_id: str, kind: str) -> Path:
    return Path(req_dir) / f"{req_id}.{kind}"


def write_json_atomic(path: Path, data: Any) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


@dataclass(frozen=True)
class Request:
    id: str
    key: str
    date_from: str
    date_to: str
    stage_dir: Path
    created_at: float
    kind: str = "gold_stage"
    args: Optional[dict] = None
    files_since: Optional[float] = None
    files_until: Optional[float] = None

    @classmethod
    def load(cls, path: Path) -> "Request":
        d = json.loads(path.read_text(encoding="utf-8"))
        kind = d.get("kind") or "gold_stage"
        if kind not in KINDS:
            raise ValueError(f"모르는 kind: {kind!r}")
        fs, fu = d.get("files_since"), d.get("files_until")
        return cls(d["id"], d["key"], d["date_from"], d["date_to"], Path(d["stage_dir"]), float(d["created_at"]), kind, d.get("args") or {},
                   float(fs) if fs is not None else None, float(fu) if fu is not None else None)


def _inside(base: Path, path: Path) -> bool:
    b, p = Path(base).resolve(), Path(path).resolve()
    return p != b and b in p.parents


def _under(base: str | Path, path: str | Path) -> bool:
    b, p = Path(base).resolve(), Path(path).resolve()
    return p == b or b in p.parents


class MeasureArgError(ValueError):
    """kind=measure 요청의 인자가 허용 범위 밖이다."""


def check_measure(r: "Request", measure_root: str) -> dict:
    """kind=measure 요청 검사. 반환 = 정리한 args. 쓰는 경로는 measure_root 아래, 읽는 경로는 그 아래이거나 운영 읽기 전용 루트."""
    a = dict(r.args or {})
    job = a.get("job")
    if job not in MEASURE_JOBS:
        raise MeasureArgError(f"모르는 측정 작업: {job!r} (허용: {sorted(MEASURE_JOBS)})")
    if not _inside(Path(measure_root), r.stage_dir):
        raise MeasureArgError(f"측정 요청의 stage_dir 는 {measure_root} 아래여야 한다: {r.stage_dir}")
    for k in MEASURE_OUT_ARGS:
        if a.get(k) is not None and not _inside(Path(measure_root), Path(a[k])):
            raise MeasureArgError(f"{k} 는 {measure_root} 아래여야 한다: {a[k]}")
    for k in MEASURE_IN_ARGS:
        v = a.get(k)
        if v is not None and not (_under(measure_root, v) or any(_under(x, v) for x in MEASURE_READ_ROOTS)):
            raise MeasureArgError(f"{k} 는 {measure_root} 또는 {MEASURE_READ_ROOTS} 아래여야 한다: {v}")
    need = {"silver_build": ("tables", "version", "bronze_root", "silver_root", "rebuild_root"),
            "silver_point": ("tables", "version", "rebuild_root"),
            "gold_stage": ("silver_root", "bronze_root", "stage_root"),
            "silver_verify": ("bronze_root", "silver_root")}[job]
    miss = [k for k in need if not a.get(k)]
    if miss:
        raise MeasureArgError(f"{job}: 인자 없음 {miss}")
    if job in ("silver_build", "silver_point"):
        from streaming.silver_pointer import check_table, check_version
        a["tables"] = [check_table(t) for t in a["tables"]]
        check_version(str(a["version"]))
    return a


def measure_env(a: dict, e: dict, r: "Request") -> dict:
    """측정 작업의 경로 환경변수. 운영 기본값을 쓰지 않도록 모두 명시한다."""
    out = dict(e)
    empty = str(Path(r.stage_dir) / "_empty")                       # '없음'을 뜻하는 빈 폴더 (운영 루트로 떨어지지 않게)
    if a["job"] in ("silver_build", "silver_verify"):
        out["SILVER_BRONZE_ROOT"] = a["bronze_root"]
        out["SILVER_ROOT"] = a["silver_root"]
        out["SILVER_REBUILD_ROOT"] = a.get("rebuild_root") or a.get("silver_rebuild_root") or empty
        if a.get("verify_since"):
            out["SILVER_VERIFY_SINCE"] = a["verify_since"]
            out.pop("SILVER_VERIFY_LOOKBACK_DAYS", None)
    if a["job"] == "gold_stage":
        out["GOLD_SILVER_ROOT"] = a["silver_root"]
        out["GOLD_BRONZE_ROOT"] = a["bronze_root"]
        out["GOLD_SILVER_REBUILD_ROOT"] = a.get("silver_rebuild_root") or empty
        out["GOLD_STAGE_ROOT"] = a["stage_root"]
    return out


def pending(req_dir: Path) -> list[Request]:
    """끝나지 않은 요청을 만든 순서대로. 실행 중 표시(.running)가 남은 것도 포함한다: 실행기가 죽었다 다시 뜬 경우라 다시 실행한다."""
    if not req_dir.is_dir():
        return []
    out = []
    for p in req_dir.glob("*.json"):
        rid = p.stem
        if any(marker(req_dir, rid, k).exists() for k in TERMINAL):
            continue
        try:
            out.append(Request.load(p))
        except (ValueError, KeyError, OSError) as e:
            LOG.warning("요청 파일을 읽지 못해 실패로 표시합니다: %s (%s)", p.name, e)
            write_json_atomic(marker(req_dir, rid, "failed"), {"returncode": None, "stderr_tail": f"요청 파일 오류: {e}"})
    return sorted(out, key=lambda r: (r.created_at, r.id))


def supersede(req_dir: Path, reqs: list[Request]) -> list[Request]:
    """같은 key 의 요청이 여럿이면 가장 새 것만 남기고 나머지는 superseded 로 표시한다."""
    latest: dict[str, Request] = {}
    for r in reqs:
        latest[r.key] = r
    keep = []
    for r in reqs:
        if latest[r.key] is r:
            keep.append(r)
        else:
            write_json_atomic(marker(req_dir, r.id, "superseded"), {"by": latest[r.key].id})
            LOG.info("같은 구간의 새 요청이 있어 건너뜁니다: %s → %s", r.id, latest[r.key].id)
    return keep


def spark_command(r: Request, *, driver_memory: str, master: str, spark_submit: str, rebuild_root: str = "/data/silver_rebuild") -> list[str]:
    # umask 000: Hadoop 은 프로세스 umask 와 따로 022 를 적용한다. 그러면 spark 사용자가 만든 Parquet 폴더를 Airflow(uid 50000)가
    # 지우지 못해 오래된 스테이징 정리(prune_old_stages)가 실패한다.
    head = [spark_submit, "--master", master, "--driver-memory", driver_memory, "--conf", "spark.hadoop.fs.permissions.umask-mode=000"]
    here = Path(__file__).resolve().parent
    if r.kind == "silver_rebuild":
        a = r.args or {}
        cmd = head + [str(here / "silver_rebuild.py"), "build", "--tables", ",".join(a["tables"]), "--from", r.date_from, "--to", r.date_to,
                      "--version", str(a["version"]), "--bronze-read", a.get("bronze_read_mode", "listing"),
                      "--rebuild-root", rebuild_root, "--report", str(r.stage_dir / REBUILD_REPORT)]
        return cmd + (["--allow-shrink"] if a.get("allow_shrink") else [])
    if r.kind == "measure":
        a = r.args or {}
        job = a.get("job")
        if job == "silver_build":
            return head + [str(here / "silver_rebuild.py"), "build", "--tables", ",".join(a["tables"]), "--from", r.date_from,
                           "--to", r.date_to, "--version", str(a["version"]), "--bronze-read", a.get("bronze_read_mode", "listing"),
                           "--rebuild-root", a["rebuild_root"], "--report", a.get("report") or str(r.stage_dir / REBUILD_REPORT),
                           "--allow-shrink"]
        if job == "gold_stage":
            return head + [str(here / "gold_stage.py"), "--from", r.date_from, "--to", r.date_to]
        if job == "silver_verify":
            return head + [str(here.parent / "streaming" / "silver_verify.py")]
        raise ValueError(f"측정 작업 {job} 는 Spark 명령이 없습니다")
    if r.kind != "gold_stage":
        raise ValueError(f"{r.kind} 는 Spark 명령이 없습니다")
    return head + [str(here / "gold_stage.py"), "--from", r.date_from, "--to", r.date_to, *files_args(r.files_since, r.files_until)]


def files_args(since: Optional[float], until: Optional[float]) -> list[str]:
    """증분 인자. until 이 없으면 증분이 아니다."""
    if until is None:
        return []
    return (["--files-since", repr(float(since))] if since is not None else []) + ["--files-until", repr(float(until))]


def run_publish(r: Request, rebuild_root: str) -> dict:
    """silver_publish: Spark 없이 이 프로세스에서 포인터 파일만 바꾼다. 반환 = 게시 전 값."""
    from datetime import date as _date
    from batch.silver_rebuild import publish_all
    from streaming.silver_pointer import check_dates
    a = r.args or {}
    days = check_dates(_date.fromisoformat(r.date_from), _date.fromisoformat(r.date_to), allow_recent=bool(a.get("allow_recent")))
    prev = publish_all(rebuild_root, list(a["tables"]), days, str(a["version"]), reprocess_id=a.get("reprocess_id"))
    out = {"version": a["version"], "previous": prev}
    write_json_atomic(r.stage_dir / PUBLISH_REPORT, out)
    return out


def run_measure_point(r: Request, a: dict) -> dict:
    """silver_point: 측정 폴더 안의 포인터를 게시한다(운영 포인터와 무관). 날짜 검사는 측정 날짜라 최근 제한을 두지 않는다."""
    from datetime import date as _date, timedelta as _td
    from batch.silver_rebuild import publish_all
    d0, d1 = _date.fromisoformat(r.date_from), _date.fromisoformat(r.date_to)
    days = [d0 + _td(days=i) for i in range((d1 - d0).days + 1)]
    prev = publish_all(a["rebuild_root"], list(a["tables"]), days, str(a["version"]))
    return {"version": a["version"], "previous": prev}


def cgroup_mem(path: Optional[Path] = None) -> Optional[int]:
    """이 컨테이너의 지금 메모리 사용(cgroup v2). 컨테이너 밖이거나 파일이 없으면 None."""
    try:
        return int((path or CGROUP_MEM).read_text().strip())
    except (OSError, ValueError):
        return None


def _tail(text: str, n: int = 60) -> str:
    return "\n".join((text or "").splitlines()[-n:])


def _read_tail(path: Path, n: int) -> str:
    try:
        return _tail(path.read_text(encoding="utf-8", errors="replace"), n)
    except OSError:
        return ""


def kill_group(proc: Any, *, grace_sec: float = KILL_GRACE_SEC) -> None:
    """프로세스 그룹 전체를 끈다: SIGTERM, grace_sec 안에 안 끝나면 SIGKILL."""
    for sig, wait in ((signal.SIGTERM, grace_sec), (signal.SIGKILL, 5.0)):
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def _env_float(e: dict, name: str, default: float) -> float:
    try:
        return float(e.get(name) or default)
    except ValueError:
        return default


def run_one(r: Request, req_dir: Path, stage_base: Path, *, launcher: Callable[..., Any] = subprocess.Popen,
            env: Optional[dict] = None, timeout_sec: Optional[float] = None, poll_sec: Optional[float] = None,
            clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
            kill: Callable[[Any], None] = kill_group, now: Optional[float] = None) -> str:
    """요청 하나를 실행하고 결과 종류('done'|'failed'|'skipped')를 돌려준다.

    Spark 를 기다리는 동안 poll_sec 마다 박동을 갱신하고, .cancel 이 생기거나 timeout_sec 를 넘으면 프로세스 그룹을 끈다."""
    e = dict(os.environ if env is None else env)
    if timeout_sec is None:
        timeout_sec = (_env_float(e, "GOLD_RUNNER_REBUILD_TIMEOUT_SEC", DEFAULT_REBUILD_TIMEOUT_SEC) if r.kind == "silver_rebuild"
                       else _env_float(e, "GOLD_RUNNER_MEASURE_TIMEOUT_SEC", DEFAULT_MEASURE_TIMEOUT_SEC) if r.kind == "measure"
                       else _env_float(e, "GOLD_RUNNER_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
    if poll_sec is None:      # 측정은 메모리 표본을 촘촘히 뜬다 (설계안 5-1: 2초 간격 최댓값)
        poll_sec = (_env_float(e, "GOLD_RUNNER_MEASURE_POLL_SEC", 2.0) if r.kind == "measure"
                    else _env_float(e, "GOLD_RUNNER_POLL_SEC", DEFAULT_POLL_SEC))
    max_queue = _env_float(e, "GOLD_RUNNER_MAX_QUEUE_SEC", DEFAULT_MAX_QUEUE_SEC)
    if marker(req_dir, r.id, "cancel").exists():
        write_json_atomic(marker(req_dir, r.id, "failed"), {"returncode": None, "stderr_tail": "Airflow 가 기다리다 취소했다"})
        return "skipped"
    measure_args: Optional[dict] = None
    if r.kind == "measure":
        try:
            measure_args = check_measure(r, e.get("GOLD_MEASURE_ROOT", DEFAULT_MEASURE_ROOT))
        except Exception as ex:                                          # noqa: BLE001 (어떤 검사 오류든 실행하지 않는다)
            write_json_atomic(marker(req_dir, r.id, "failed"), {"returncode": None, "stderr_tail": f"측정 요청 거부: {ex}"})
            return "failed"
    elif not _inside(stage_base, r.stage_dir):
        write_json_atomic(marker(req_dir, r.id, "failed"),
                          {"returncode": None, "stderr_tail": f"스테이징 폴더가 {stage_base} 밖이다: {r.stage_dir}"})
        return "failed"
    waited = (time.time() if now is None else now) - r.created_at
    if waited > max_queue and not marker(req_dir, r.id, "running").exists():
        write_json_atomic(marker(req_dir, r.id, "failed"),
                          {"returncode": None, "stderr_tail": f"줄을 선 지 {int(waited)}초가 지나 실행하지 않았다 (한도 {int(max_queue)}초)"})
        return "skipped"
    r.stage_dir.mkdir(parents=True, exist_ok=True)
    marker(req_dir, r.id, "running").write_text(str(os.getpid()), encoding="utf-8")
    e["GOLD_STAGE_ROOT"] = str(r.stage_dir)
    rebuild_root = e.get("GOLD_SILVER_REBUILD_ROOT", "/data/silver_rebuild")
    LOG.info("%s 시작 %s: %s ~ %s → %s", r.kind, r.id, r.date_from, r.date_to, r.stage_dir)
    out_p, err_p = marker(req_dir, r.id, "out"), marker(req_dir, r.id, "err")
    t0 = clock()
    stopped: Optional[str] = None
    rc: Optional[int] = None
    ok_rc: tuple[int, ...] = (0,)
    mem_max: Optional[int] = None
    if measure_args is not None:
        ok_rc = MEASURE_JOBS[measure_args["job"]][1]
        e = measure_env(measure_args, e, r)
    if measure_args is not None and not MEASURE_JOBS[measure_args["job"]][0]:   # Spark 없는 측정 작업 (silver_point)
        try:
            out_p.write_text(json.dumps(run_measure_point(r, measure_args), ensure_ascii=False), encoding="utf-8")
            rc = 0
        except Exception as ex:                                          # noqa: BLE001
            err_p.write_text(f"{type(ex).__name__}: {ex}", encoding="utf-8")
            rc = 1
    elif r.kind == "silver_publish":                                       # Spark 없이 포인터 파일만: 바로 끝난다
        try:
            out_p.write_text(json.dumps(run_publish(r, rebuild_root), ensure_ascii=False), encoding="utf-8")
            rc = 0
        except Exception as ex:                                          # noqa: BLE001
            err_p.write_text(f"{type(ex).__name__}: {ex}", encoding="utf-8")
            rc = 1
    else:
        # silver_rebuild 는 streaming.silver_config 를 쓴다: 경로를 SILVER_* 로 맞춘다 (gold_stage 는 GOLD_* 를 쓴다)
        e.setdefault("SILVER_BRONZE_ROOT", e.get("GOLD_BRONZE_ROOT", "/data/bronze"))
        e.setdefault("SILVER_ROOT", e.get("GOLD_SILVER_ROOT", "/data/silver"))
        e.setdefault("SILVER_REBUILD_ROOT", rebuild_root)
        try:
            cmd = spark_command(r, driver_memory=e.get("GOLD_DRIVER_MEMORY", "1g"), master=e.get("GOLD_SPARK_MASTER", "local[2]"),
                                spark_submit=e.get("SPARK_SUBMIT", "/opt/spark/bin/spark-submit"), rebuild_root=rebuild_root)
        except (KeyError, ValueError) as ex:
            cmd = None
            stopped = f"요청 인자 오류: {type(ex).__name__}: {ex}"
        if cmd is not None and r.kind in ("silver_rebuild", "measure"):
            nice = int(_env_float(e, "GOLD_RUNNER_REBUILD_NICE", DEFAULT_REBUILD_NICE) if r.kind == "silver_rebuild"
                       else _env_float(e, "GOLD_RUNNER_MEASURE_NICE", DEFAULT_MEASURE_NICE))
            if nice > 0:
                cmd = ["nice", "-n", str(nice), *cmd]
        if cmd is not None:
            with open(out_p, "w", encoding="utf-8") as fo, open(err_p, "w", encoding="utf-8") as fe:
                proc = launcher(cmd, env=e, stdout=fo, stderr=fe, start_new_session=True)
            while True:
                rc = proc.poll()
                if r.kind == "measure":
                    m = cgroup_mem()
                    if m is not None:
                        mem_max = m if mem_max is None else max(mem_max, m)
                if rc is not None:
                    break
                touch_alive(req_dir)
                if marker(req_dir, r.id, "cancel").exists():
                    stopped = "Airflow 가 기다리다 취소해서 실행 중에 껐다"
                elif clock() - t0 > timeout_sec:
                    stopped = f"{int(timeout_sec)}초 안에 끝나지 않았다"
                if stopped:
                    LOG.warning("%s 중단 %s: %s", r.kind, r.id, stopped)
                    kill(proc)
                    rc = None
                    break
                sleep(poll_sec)
    elapsed = round(clock() - t0, 1)
    kind = "done" if rc is not None and rc in ok_rc else "failed"
    body: dict[str, Any] = {"returncode": rc, "elapsed_sec": elapsed}
    if r.kind == "measure":
        body["mem_max_bytes"] = mem_max
        body["mem_sample_sec"] = poll_sec
    if kind == "done":
        body["stdout_tail"] = _read_tail(out_p, 15)
    else:
        body["stderr_tail"] = "\n".join(x for x in (stopped, _read_tail(err_p, 60)) if x)
    write_json_atomic(marker(req_dir, r.id, kind), body)
    marker(req_dir, r.id, "running").unlink(missing_ok=True)
    for line in [x for x in _read_tail(err_p, 200).splitlines() if x.startswith("[")][-12:]:
        LOG.info("  %s", line)                     # gold_stage 의 표별 진행 줄 '[경과초] 표: 행, 초' (docker logs 에서 보이게)
    LOG.info("%s %s %s (%.1f초)", r.kind, "완료" if kind == "done" else "실패", r.id, elapsed)
    return kind


def prune(req_dir: Path, *, keep_hours: float = KEEP_HOURS, now: Optional[float] = None) -> int:
    """끝난 지 오래된 요청 파일을 지운다. 진행 중이거나 끝나지 않은 요청은 건드리지 않는다."""
    if not req_dir.is_dir():
        return 0
    cutoff = (time.time() if now is None else now) - keep_hours * 3600
    removed = 0
    for p in req_dir.glob("*.json"):
        rid = p.stem
        ends = [marker(req_dir, rid, k) for k in TERMINAL if marker(req_dir, rid, k).exists()]
        if ends and all(m.stat().st_mtime < cutoff for m in ends):
            for f in [p, *ends, marker(req_dir, rid, "cancel"), *(marker(req_dir, rid, k) for k in LOGS)]:
                f.unlink(missing_ok=True)
            removed += 1
    return removed


def touch_alive(req_dir: Path) -> None:
    (req_dir / ALIVE).write_text(str(time.time()), encoding="utf-8")


def process_pending(stage_base: Path, **kw) -> list[tuple[str, str]]:
    req_dir = requests_dir(stage_base)
    req_dir.mkdir(parents=True, exist_ok=True)
    done = []
    for r in supersede(req_dir, pending(req_dir)):
        done.append((r.id, run_one(r, req_dir, stage_base, **kw)))
        touch_alive(req_dir)
    return done


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage-base", default=os.environ.get("GOLD_STAGE_BASE") or "/data/gold_stage")
    ap.add_argument("--poll-sec", type=float, default=float(os.environ.get("GOLD_RUNNER_POLL_SEC", "5")))
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Airflow(uid 50000)와 이 컨테이너(spark 사용자)가 같은 폴더를 쓰고 지운다. 만든 파일·폴더를 서로 지울 수 있게 한다.
    os.umask(0)
    base = Path(a.stage_base)
    req_dir = requests_dir(base)
    req_dir.mkdir(parents=True, exist_ok=True)
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("flag", True))
    LOG.info("실행기 시작: 요청 폴더 %s, %s초마다 확인", req_dir, a.poll_sec)
    last_prune = 0.0
    while True:
        touch_alive(req_dir)
        process_pending(base)
        if time.time() - last_prune > 3600:
            n = prune(req_dir)
            if n:
                LOG.info("오래된 요청 %d건을 지웠습니다", n)
            last_prune = time.time()
        if a.once or stop["flag"]:
            return 0
        time.sleep(a.poll_sec)


if __name__ == "__main__":
    sys.exit(main())
