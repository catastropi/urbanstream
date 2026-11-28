"""호스트(Mac)에서 도는 STEP 3B 도구(recover.py, host_watch.py, drill.py)가 같이 쓰는 작은 함수들.

- Docker 는 늘 시간 제한을 두고 부른다. Docker Desktop 이 멈추면 `docker` 명령이 끝없이 기다리기 때문이다(11-24~25).
- 비밀 값(.env 의 비밀번호, 웹훅 URL)은 읽기만 하고 출력하지 않는다. 오류 문장도 notify 의 마스킹을 거친다.
- 판정에 쓰는 값은 모두 dict 로 돌려준다. 판정 함수는 순수하게 두고 시험은 가짜 `docker` 로 한다(tests/recovery).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9), "KST")
PREFIX = "urbanstream-"
CONFIG = ROOT / "config" / "recovery.yaml"
for _p in (str(ROOT), str(ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

Runner = Callable[..., subprocess.CompletedProcess]


def load_config(path: Path | str = CONFIG) -> dict:
    import yaml
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if cfg.get("version") != 1:
        raise ValueError("config/recovery.yaml version 은 1 이어야 합니다")
    for k in ("init_services", "resume", "host_watch", "diagnose", "drills"):
        if k not in cfg:
            raise ValueError(f"config/recovery.yaml 에 {k} 절이 없습니다")
    return cfg


def now_kst() -> datetime:
    return datetime.now(KST)


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, KST).strftime("%m-%d %H:%M:%S")


# ------------------------------------------------------------------ 명령 실행

def run(cmd: list[str], *, timeout: float = 10, runner: Runner = subprocess.run, env: Optional[dict] = None,
        cwd: Optional[Path] = None) -> tuple[Optional[int], str, str]:
    """(종료 코드, stdout, stderr). 시간 초과나 실행 불가는 종료 코드 None."""
    try:
        p = runner(cmd, capture_output=True, text=True, timeout=timeout, env=env, cwd=str(cwd or ROOT))
    except subprocess.TimeoutExpired:
        return None, "", f"{timeout:.0f}초 안에 응답 없음"
    except OSError as e:
        return None, "", f"{type(e).__name__}: {e}"
    return p.returncode, p.stdout or "", p.stderr or ""


def docker_version(timeout: float = 10, runner: Runner = subprocess.run) -> tuple[bool, str]:
    """Docker 서버가 응답하는가. (True, 서버 버전) / (False, 이유)."""
    rc, out, err = run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=timeout, runner=runner)
    if rc == 0 and out.strip():
        return True, out.strip()
    return False, (err.strip().splitlines() or ["응답 없음"])[-1][:200]


def parse_inspect(text: str) -> list[dict]:
    """`docker inspect` JSON → 컨테이너 상태 목록. measure_sample.parse_inspect 와 같은 값에 Init·OOM·종료 시각을 더한다."""
    rows = []
    for d in json.loads(text or "[]"):
        st = d.get("State") or {}
        hc = d.get("HostConfig") or {}
        name = (d.get("Name") or "").lstrip("/")
        rows.append({"name": name, "service": ((d.get("Config") or {}).get("Labels") or {}).get("com.docker.compose.service") or name,
                     "state": st.get("Status"), "health": (st.get("Health") or {}).get("Status") or "",
                     "restarts": int(d.get("RestartCount") or 0), "oom": bool(st.get("OOMKilled")),
                     "exit_code": st.get("ExitCode"), "finished_at": st.get("FinishedAt") or "",
                     "init": bool(hc.get("Init")), "mem_limit": int(hc.get("Memory") or 0)})
    return sorted(rows, key=lambda r: r["name"])


def containers(timeout: float = 10, runner: Runner = subprocess.run, with_stats: bool = False) -> Optional[list[dict]]:
    """urbanstream-* 컨테이너(멈춘 것 포함). Docker 가 응답하지 않으면 None. with_stats 면 mem_bytes·pids 를 더한다."""
    rc, ids, _ = run(["docker", "ps", "-a", "-q", "--filter", f"name={PREFIX}"], timeout=timeout, runner=runner)
    if rc != 0:
        return None
    ids_l = ids.split()
    if not ids_l:
        return []
    rc, text, _ = run(["docker", "inspect", *ids_l], timeout=timeout, runner=runner)
    if rc != 0:
        return None
    rows = parse_inspect(text)
    if with_stats:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from measure_sample import parse_stats
        rc, st, _ = run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids_l], timeout=max(timeout, 30), runner=runner)
        stats = parse_stats(st) if rc == 0 else {}
        for r in rows:
            s = stats.get(r["name"], {})
            r["mem_bytes"] = s.get("mem_bytes", 0) if r["state"] == "running" else 0
            r["pids"] = s.get("pids", 0) if r["state"] == "running" else 0
    return rows


def by_service(rows: list[dict]) -> dict[str, dict]:
    return {r["service"]: r for r in rows}


def healthy(r: Optional[dict]) -> bool:
    """running 이고 healthcheck 가 없거나(빈 값) healthy."""
    return r is not None and r.get("state") == "running" and (r.get("health") or "") in ("", "healthy")


def expected_services() -> list[str]:
    """늘 켜져 있어야 하는 12개 (config/measure.yaml gate.expected_containers 와 같은 목록)."""
    import yaml
    m = yaml.safe_load((ROOT / "config" / "measure.yaml").read_text(encoding="utf-8"))
    return list(m["gate"]["expected_containers"])


def vm_clock(timeout: float = 10, runner: Runner = subprocess.run, container: str = "urbanstream-collector") -> tuple[Optional[float], str]:
    """Docker VM 시계(초). 모든 컨테이너가 같은 VM 커널 시계를 쓰므로 하나만 재면 된다. 호스트 시각과 함께 왕복 시간의 가운데로 맞춘다."""
    t0 = time.time()
    rc, out, err = run(["docker", "exec", container, "date", "+%s"], timeout=timeout, runner=runner)
    t1 = time.time()
    if rc != 0:
        return None, (err.strip().splitlines() or ["실패"])[-1][:200]
    try:
        vm = float(out.strip())
    except ValueError:
        return None, f"date 출력을 읽지 못함: {out.strip()[:50]}"
    return vm - (t0 + t1) / 2, f"왕복 {t1 - t0:.1f}초"     # 양수 = VM 이 빠름, 음수 = VM 이 늦음


# ------------------------------------------------------------------ PostgreSQL (읽기 전용, .env 의 관리자 접속)

def dotenv(path: Optional[Path] = None) -> dict[str, str]:
    p = path or ROOT / ".env"
    env: dict[str, str] = {}
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def pg_query(sql: str, params: tuple = (), *, env: Optional[dict] = None, timeout: int = 5) -> list[tuple]:
    """읽기 전용 질의. 실패하면 예외(호출 쪽이 '모름'으로 처리한다). 호스트에서 Docker 가 연 포트(기본 5433)로 붙는다."""
    import psycopg
    e = {**dotenv(), **{k: v for k, v in os.environ.items() if k.startswith("POSTGRES_")}, **(env or {})}
    if e.get("GOLD_DSN"):
        info = e["GOLD_DSN"]
    else:
        info = (f"host={e.get('POSTGRES_HOST', '127.0.0.1')} port={e.get('POSTGRES_PORT', '5433')} "
                f"dbname={e.get('POSTGRES_DB', 'urbanstream')} user={e.get('POSTGRES_USER', 'urbanstream')} "
                f"password={e.get('POSTGRES_PASSWORD', '')}")
    with psycopg.connect(info, autocommit=True, connect_timeout=timeout, application_name="urbanstream-host-tools") as c:
        c.execute("SET default_transaction_read_only = on")
        c.execute(f"SET statement_timeout = '{int(timeout)}s'")
        return list(c.execute(sql, params).fetchall())


def monitor_heartbeat_age(env: Optional[dict] = None) -> Optional[float]:
    """ops.monitor_state 의 monitor 박동 나이(초). monitor 루프가 1분마다 쓴다(ops.v_pipeline_health 와 같은 값). 못 읽으면 None."""
    try:
        rows = pg_query("SELECT extract(epoch FROM now() - heartbeat_at) FROM ops.monitor_state WHERE component = 'monitor'", env=env)
    except Exception:                                                        # noqa: BLE001
        return None
    return float(rows[0][0]) if rows else None


# ------------------------------------------------------------------ 웹훅 (monitor 를 거치지 않는 직접 전송)

def webhook_config(env: Optional[dict] = None) -> tuple[bool, str, str]:
    from urbanstream.notify.transport import config_from_env
    return config_from_env({**dotenv(), **os.environ, **(env or {})})


def send_webhook(level: str, title: str, body: str, *, env: Optional[dict] = None,
                 post: Optional[Callable[..., Any]] = None) -> tuple[bool, str]:
    """웹훅 1건. (보냈는가, 이유). URL 은 결과·오류 문장에 넣지 않는다."""
    from urbanstream.notify._mask import mask
    from urbanstream.notify.transport import post_webhook
    enabled, kind, url = webhook_config(env)
    if not enabled:
        return False, "ALERTS_ENABLED=false"
    if not url:
        return False, "ALERT_WEBHOOK_URL 이 .env 에 없음"
    text = mask(f"[{level.upper()}] {title}\n{body}".strip())
    res = (post or post_webhook)(kind, url, text, scrub=lambda s: mask(s.replace(url, "<url>")))
    return bool(res.ok), "" if res.ok else (res.error or f"HTTP {res.status}")


# ------------------------------------------------------------------ 상주 실행 (measure_sample.py 와 같은 방식, launchd 없음)

def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid(path: Path) -> Optional[int]:
    try:
        pid = int(Path(path).read_text(encoding="utf-8").split()[0])
    except (OSError, ValueError, IndexError):
        return None
    return pid if pid_alive(pid) else None


def take_pid(path: Path) -> Path:
    """PID 파일을 만든다. 살아 있는 다른 실행이 있으면 RuntimeError. 끝난 프로세스가 남긴 파일은 지우고 다시 만든다."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            other = read_pid(path)
            if other is not None and other != os.getpid():
                raise RuntimeError(f"이미 돌고 있다 (PID {other})") from None
            path.unlink(missing_ok=True)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()}\n")
        return path
    raise RuntimeError(f"PID 파일을 만들 수 없다: {path}")


def start_background(cmd: list[str], pid_path: Path, log_path: Path, *, caffeinate: bool = True) -> Optional[int]:
    """cmd 를 터미널과 분리해 띄운다(macOS 면 caffeinate -i 로 감싼다). PID 파일이 생기면 그 PID, 10초 안에 안 생기면 None."""
    import shutil
    if caffeinate and shutil.which("caffeinate"):
        cmd = ["caffeinate", "-i", *cmd]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log:
        subprocess.Popen(cmd, cwd=str(ROOT), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(50):
        time.sleep(0.2)
        pid = read_pid(pid_path)
        if pid is not None:
            return pid
    return None


def stop_background(pid_path: Path, wait_sec: float = 15) -> tuple[bool, Optional[int]]:
    """(멈췄거나 원래 안 돌았음, PID)."""
    import signal
    pid = read_pid(pid_path)
    if pid is None:
        Path(pid_path).unlink(missing_ok=True)
        return True, None
    os.kill(pid, signal.SIGTERM)
    t0 = time.time()
    while time.time() - t0 < wait_sec:
        if not pid_alive(pid):
            return True, pid
        time.sleep(0.2)
    return False, pid


def measure_lock_path() -> Path:
    """STEP 2 측정·STEP 3B 훈련이 같이 쓰는 잠금 파일 (config/measure.yaml paths.lock)."""
    import yaml
    m = yaml.safe_load((ROOT / "config" / "measure.yaml").read_text(encoding="utf-8"))
    p = Path(m["paths"]["lock"])
    return p if p.is_absolute() else ROOT / p
