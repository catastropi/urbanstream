"""STEP 2 표본 수집 (M1 스트리밍 배치 시간, M2 자원 사용). 읽기만 한다: docker stats·inspect, 체크포인트 파일 시각, 디스크 여유.

5분마다 한 번 실행하면 data/measure/samples/<KST 날짜>.jsonl 에 한 줄을 붙인다.
기본은 스스로 반복하는 백그라운드 실행(--start)이다. launchd 예시(scripts/launchd/)는 11-25 Mac 에서 주기 실행이 되지 않아
(launchd 로그 'domain in on-demand-only mode', 원인 미확정) 쓰지 않는다. 이 기록은 지나가면 다시 만들 수 없다:
  - Spark 는 체크포인트의 offsets/N·commits/N 을 최근 100개만 남긴다 → 그때그때 새 배치의 시작·끝 시각을 떠 둔다.
  - docker stats 는 그 순간의 값이다.

한 줄의 모양
  {"ts": epoch, "kst": "...", "docker": true, "vm": {"mem_total", "ncpu"}, "disk_free": bytes,
   "containers": [{"service", "state", "health", "restarts", "oom", "cpu_pct", "mem_bytes", "mem_limit", "pids"}],
   "runner_queue": {"pending", "running", "alive_age_sec"},
   "batches": [{"layer", "query", "batch", "start", "end", "sec"}],     # 지난 표본 뒤에 새로 끝난 마이크로배치
   "runner_done": [{"id", "kind", "key", "result", "elapsed_sec", "returncode", "end"}]}   # M4: 지난 표본 뒤에 끝난 gold-runner 요청
  gold-runner 요청 파일은 72시간 뒤 지워지므로(prune) 그 전에 소요 시간을 옮겨 둔다.

사용
  python scripts/measure_sample.py --start         백그라운드로 5분마다 (caffeinate -i 아래, 터미널을 닫아도 계속)
  python scripts/measure_sample.py --status        돌고 있는지, 마지막 표본 시각
  python scripts/measure_sample.py --stop          멈춘다
  python scripts/measure_sample.py --loop 300      앞에서 5분마다 (Ctrl+C 로 끝. --start 가 이것을 띄운다)
  python scripts/measure_sample.py                 한 번 떠서 붙이고 끝낸다
  python scripts/measure_sample.py --print         붙이지 않고 화면에만
반복 실행은 data/measure/samples/sampler.pid 로 하나만 돈다. Mac 이 자는 동안은 표본이 없고, 그 공백은 표본 시각 사이의 간격으로 남는다
(깨어나면 밀린 회차를 채우지 않고 바로 다음 표본을 뜬다). Mac 을 다시 켜면 저절로 시작하지 않으니 --start 를 다시 한다.
Docker 가 꺼져 있으면 {"docker": false} 한 줄만 남긴다(공백이 기록에 보이게).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from measure_guard import KST, ROOT, _resolve, cpu_sum, load_config  # noqa: E402

Runner = Callable[..., subprocess.CompletedProcess]
_UNITS = {"b": 1, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3, "tb": 1000 ** 4,
          "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4}
PREFIX = "urbanstream-"
STATE_FILE = ".batch_cursor.json"


def _run(cmd: list[str], run: Runner = subprocess.run, timeout: float = 30) -> Optional[str]:
    try:
        p = run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return p.stdout if p.returncode == 0 else None


def parse_size(s: str) -> int:
    """'512MiB', '1.5GiB', '0B', '12.3kB' → 바이트."""
    m = re.match(r"^\s*([0-9.]+)\s*([A-Za-z]*)\s*$", s or "")
    if not m:
        return 0
    return int(float(m.group(1)) * _UNITS.get(m.group(2).lower() or "b", 1))


def parse_stats(text: str) -> dict[str, dict]:
    """`docker stats --no-stream --format '{{json .}}'` → {컨테이너 이름: {cpu_pct, mem_bytes, pids}}."""
    out = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        used = (d.get("MemUsage") or "0B / 0B").split("/")[0]
        try:
            pids = int(d.get("PIDs") or 0)
        except ValueError:
            pids = 0
        out[d.get("Name", "")] = {"cpu_pct": float((d.get("CPUPerc") or "0%").rstrip("%") or 0),
                                  "mem_bytes": parse_size(used), "pids": pids}
    return out


def parse_inspect(text: str) -> dict[str, dict]:
    """`docker inspect <id...>` JSON 배열 → {컨테이너 이름: {service, state, health, restarts, oom, mem_limit}}."""
    out = {}
    for d in json.loads(text or "[]"):
        st = d.get("State") or {}
        name = (d.get("Name") or "").lstrip("/")
        out[name] = {"service": ((d.get("Config") or {}).get("Labels") or {}).get("com.docker.compose.service") or name,
                     "state": st.get("Status"), "health": (st.get("Health") or {}).get("Status") or "",
                     "restarts": int(d.get("RestartCount") or 0), "oom": bool(st.get("OOMKilled")),
                     "mem_limit": int((d.get("HostConfig") or {}).get("Memory") or 0),
                     "nano_cpus": int((d.get("HostConfig") or {}).get("NanoCpus") or 0)}
    return out


def docker_containers(run: Runner = subprocess.run) -> Optional[list[dict]]:
    ids = _run(["docker", "ps", "-a", "-q", "--filter", f"name={PREFIX}"], run)
    if ids is None:
        return None
    ids_l = ids.split()
    if not ids_l:
        return []
    insp = parse_inspect(_run(["docker", "inspect", *ids_l], run) or "[]")
    stats = parse_stats(_run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids_l], run, timeout=60) or "")
    rows = []
    for name, i in sorted(insp.items()):
        s = stats.get(name, {})
        rows.append({**i, "name": name, "cpu_pct": s.get("cpu_pct", 0.0) if i["state"] == "running" else 0.0,
                     "mem_bytes": s.get("mem_bytes", 0) if i["state"] == "running" else 0, "pids": s.get("pids", 0)})
    return rows


def docker_vm(run: Runner = subprocess.run) -> Optional[dict]:
    t = _run(["docker", "info", "--format", "{{json .}}"], run)
    if t is None:
        return None
    d = json.loads(t)
    return {"mem_total": int(d.get("MemTotal") or 0), "ncpu": int(d.get("NCPU") or 0)}


def runner_queue(stage_base: Path, now: Optional[float] = None) -> Optional[dict]:
    """gold-runner 요청 폴더: 끝나지 않은 요청 수, 실행 중 수, 박동 나이."""
    req = Path(stage_base) / "_requests"
    if not req.is_dir():
        return None
    now = time.time() if now is None else now
    pending = running = 0
    for p in req.glob("*.json"):
        rid = p.stem
        if any((req / f"{rid}.{k}").exists() for k in ("done", "failed", "superseded")):
            continue
        if (req / f"{rid}.running").exists():
            running += 1
        else:
            pending += 1
    alive = req / ".runner_alive"
    age = round(now - alive.stat().st_mtime, 1) if alive.exists() else None
    return {"pending": pending, "running": running, "alive_age_sec": age}


def _numbered(d: Path) -> dict[int, float]:
    out = {}
    if d.is_dir():
        with os.scandir(d) as it:
            for e in it:
                if e.name.isdigit():
                    out[int(e.name)] = e.stat().st_mtime
    return out


def checkpoint_batches(ckpt_root: Path, layers: list[str], cursor: dict) -> tuple[list[dict], dict]:
    """새로 끝난 마이크로배치. 시작 = offsets/N 의 수정 시각(배치 계획을 적은 때), 끝 = commits/N(커밋한 때).
    cursor = {"<layer>/<query>": 마지막으로 기록한 batch}. 처음 보는 쿼리는 지금 남아 있는 것(최대 100개)을 모두 적는다."""
    rows, new_cursor = [], dict(cursor)
    for layer in layers:
        base = Path(ckpt_root) / layer
        if not base.is_dir():
            continue
        for q in sorted(p for p in base.iterdir() if p.is_dir()):
            key = f"{layer}/{q.name}"
            offs, coms = _numbered(q / "offsets"), _numbered(q / "commits")
            last = cursor.get(key, -1)
            for n in sorted(coms):
                if n <= last or n not in offs:
                    continue
                rows.append({"layer": layer, "query": q.name, "batch": n, "start": round(offs[n], 3), "end": round(coms[n], 3),
                             "sec": round(coms[n] - offs[n], 3)})
            if coms:
                new_cursor[key] = max(last, max(coms))
    return rows, new_cursor


def runner_finished(stage_base: Path, since: float) -> tuple[list[dict], float]:
    """since(수정 시각) 뒤에 끝난 요청(.done/.failed). 측정 요청(measure-*)은 뺀다(results 에 따로 있다)."""
    req = Path(stage_base) / "_requests"
    rows, last = [], since
    if not req.is_dir():
        return rows, last
    for k in ("done", "failed"):
        for p in req.glob(f"*.{k}"):
            m = p.stat().st_mtime
            if m <= since or p.stem.startswith("measure-"):
                continue
            try:
                body = json.loads(p.read_text(encoding="utf-8"))
                reqj = json.loads((req / f"{p.stem}.json").read_text(encoding="utf-8")) if (req / f"{p.stem}.json").exists() else {}
            except (OSError, ValueError):
                continue
            rows.append({"id": p.stem, "kind": reqj.get("kind") or "gold_stage", "key": reqj.get("key"), "result": k,
                         "elapsed_sec": body.get("elapsed_sec"), "returncode": body.get("returncode"), "end": round(m, 3),
                         "incremental": reqj.get("files_until") is not None})
            last = max(last, m)
    return sorted(rows, key=lambda r: r["end"]), last


def power_state(run: Runner = subprocess.run) -> dict:
    """macOS 전원·caffeinate. 다른 OS 면 None."""
    if sys.platform != "darwin":
        return {"ac": None, "caffeinate": None}
    batt = _run(["pmset", "-g", "batt"], run) or ""
    caf = _run(["pgrep", "-x", "caffeinate"], run)
    return {"ac": ("AC Power" in batt) if batt else None, "caffeinate": bool(caf and caf.strip())}


def db_status(root: Path, lookback_min: int) -> dict:
    """진행 중 재처리 수, 최근 N분 새 수집 공백 수. .env 의 관리자 접속으로 읽기만 한다. 실패하면 None."""
    out: dict = {"reprocess_running": None, "new_gaps": None}
    try:
        import psycopg
        env = _dotenv(root / ".env")
        env = {**env, **{k: v for k, v in os.environ.items() if k.startswith("POSTGRES_")}}
        info = (f"host={env.get('POSTGRES_HOST', '127.0.0.1')} port={env.get('POSTGRES_PORT', '5433')} "
                f"dbname={env.get('POSTGRES_DB', 'urbanstream')} user={env.get('POSTGRES_USER', 'urbanstream')} "
                f"password={env.get('POSTGRES_PASSWORD', '')} connect_timeout=5")
        with psycopg.connect(info, autocommit=True) as c:
            c.execute("SET default_transaction_read_only = on")
            out["reprocess_running"] = c.execute("SELECT count(*) FROM ops.reprocess_run WHERE status = 'running'").fetchone()[0]
            out["new_gaps"] = c.execute("SELECT count(*) FROM ops.data_gap WHERE detected_at > now() - make_interval(mins => %s)"
                                        " OR gap_end > now() - make_interval(mins => %s)", (lookback_min, lookback_min)).fetchone()[0]
    except Exception as e:                                          # noqa: BLE001  (접속 실패 = 모름 = 게이트 FAIL)
        out["error"] = type(e).__name__
    return out


def _dotenv(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def collect_state(cfg: dict, root: Path = ROOT, run: Runner = subprocess.run, *, with_queue: bool = True) -> dict:
    """감시용 빠른 표본 (체크포인트·DB 는 보지 않는다)."""
    now = time.time()
    st: dict = {"ts": round(now, 3), "kst": datetime.fromtimestamp(now, KST).isoformat(timespec="seconds")}
    rows = docker_containers(run)
    st["docker"] = rows is not None
    st["containers"] = rows or []
    st["vm"] = docker_vm(run) if rows is not None else None
    data_dir = _resolve("data", root)
    st["disk_free"] = shutil.disk_usage(data_dir if data_dir.exists() else root).free
    if with_queue:
        st["runner_queue"] = runner_queue(_resolve("data/gold_stage", root), now)
    return st


def collect_gate_state(cfg: dict, root: Path = ROOT, run: Runner = subprocess.run,
                       sleep: Callable[[float], None] = time.sleep) -> tuple[dict, float]:
    """실행 전 검사용: 1분 동안 cpu_samples 번 떠서 CPU 합을 평균한다. 마지막 표본에 전원·DB 상태를 붙인다."""
    n = max(1, int(cfg["gate"]["cpu_samples"]))
    cpus, st = [], {}
    for i in range(n):
        st = collect_state(cfg, root, run)
        cpus.append(cpu_sum(st))
        if i < n - 1:
            sleep(60 / n)
    st["power"] = power_state(run)
    st["db"] = db_status(root, int(cfg["gate"]["data_gap_lookback_min"]))
    return st, round(sum(cpus) / len(cpus), 1)


def sample_once(cfg: dict, root: Path = ROOT, run: Runner = subprocess.run) -> dict:
    st = collect_state(cfg, root, run)
    sdir = _resolve(cfg["paths"]["samples"], root)
    sdir.mkdir(parents=True, exist_ok=True)
    cur_p = sdir / STATE_FILE
    cursor = json.loads(cur_p.read_text(encoding="utf-8")) if cur_p.exists() else {}
    rows, cursor = checkpoint_batches(_resolve(cfg["sample"]["checkpoint_root"], root), list(cfg["sample"]["layers"]), cursor)
    st["batches"] = rows
    done, last = runner_finished(_resolve("data/gold_stage", root), float(cursor.get("_runner_done_mtime", time.time() - 3600)))
    st["runner_done"] = done
    cursor["_runner_done_mtime"] = last
    tmp = cur_p.with_name(cur_p.name + ".tmp")
    tmp.write_text(json.dumps(cursor), encoding="utf-8")
    os.replace(tmp, cur_p)
    return st


def append(cfg: dict, st: dict, root: Path = ROOT) -> Path:
    sdir = _resolve(cfg["paths"]["samples"], root)
    sdir.mkdir(parents=True, exist_ok=True)
    p = sdir / f"{datetime.fromtimestamp(st['ts'], KST):%Y-%m-%d}.jsonl"
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(st, ensure_ascii=False, separators=(",", ":")) + "\n")
    return p


PID_FILE = "sampler.pid"
LOG_FILE = "sampler.log"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid(sdir: Path) -> Optional[int]:
    """살아 있는 반복 실행의 PID. 파일이 없거나 그 프로세스가 끝났으면 None."""
    try:
        pid = int((sdir / PID_FILE).read_text(encoding="utf-8").split()[0])
    except (OSError, ValueError, IndexError):
        return None
    return pid if _pid_alive(pid) else None


def take_pid(sdir: Path) -> Path:
    """PID 파일을 만든다. 살아 있는 다른 반복 실행이 있으면 RuntimeError. 끝난 프로세스가 남긴 파일은 지우고 다시 만든다."""
    sdir.mkdir(parents=True, exist_ok=True)
    p = sdir / PID_FILE
    for _ in range(2):
        try:
            fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            other = read_pid(sdir)
            if other is not None and other != os.getpid():
                raise RuntimeError(f"표본 수집기가 이미 돌고 있다 (PID {other})")
            p.unlink(missing_ok=True)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()}\n")
        return p
    raise RuntimeError(f"PID 파일을 만들 수 없다: {p}")


def next_tick(now: float, interval: float) -> float:
    """다음 표본 시각: 간격의 배수(벽시계)에 맞춘다. 잠들었다 깨어 여러 회차가 지났어도 밀린 것은 건너뛰고 바로 다음 배수."""
    return (now // interval + 1) * interval


def loop(cfg: dict, interval: float, root: Path = ROOT, run: Runner = subprocess.run, max_iter: Optional[int] = None,
         clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep) -> int:
    """interval 초마다 sample_once + append. 한 회차가 실패해도(예: Docker 응답 없음) 다음 회차는 돈다. SIGTERM·Ctrl+C 로 끝난다."""
    import signal
    sdir = _resolve(cfg["paths"]["samples"], root)
    pid_p = take_pid(sdir)
    stop = {"flag": False}

    def _term(signum, frame):  # noqa: ARG001
        stop["flag"] = True
        raise KeyboardInterrupt
    old = signal.signal(signal.SIGTERM, _term)
    n = 0
    print(f"[{datetime.now(KST):%m-%d %H:%M:%S}] 표본 수집 시작: {interval:.0f}초 간격, PID {os.getpid()}", flush=True)
    try:
        while max_iter is None or n < max_iter:
            try:
                st = sample_once(cfg, root, run)
                out = append(cfg, st, root)
                print(f"[{datetime.now(KST):%m-%d %H:%M:%S}] {out.name}: 컨테이너 {len(st.get('containers') or [])}개, "
                      f"새 배치 {len(st.get('batches') or [])}개", flush=True)
            except Exception as e:                                              # noqa: BLE001
                print(f"[{datetime.now(KST):%m-%d %H:%M:%S}] 표본 실패: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
            n += 1
            if max_iter is not None and n >= max_iter:
                break
            now = clock()
            sleep(max(0.0, next_tick(now, interval) - now))
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, old)
        if read_pid(sdir) == os.getpid() or not (sdir / PID_FILE).exists():
            pid_p.unlink(missing_ok=True)
        print(f"[{datetime.now(KST):%m-%d %H:%M:%S}] 표본 수집 끝 ({n}회)", flush=True)
    return 0


def start(cfg: dict, interval: float, config_path: str, root: Path = ROOT, caffeinate: bool = True) -> int:
    """--loop 를 새 세션(터미널과 분리)으로 띄운다. macOS 면 caffeinate -i 로 감싸 가만히 둬서 자는 것을 막는다(덮개 닫힘은 못 막는다)."""
    sdir = _resolve(cfg["paths"]["samples"], root)
    sdir.mkdir(parents=True, exist_ok=True)
    pid = read_pid(sdir)
    if pid is not None:
        print(f"이미 돌고 있다 (PID {pid}). 멈추려면: python scripts/measure_sample.py --stop")
        return 1
    cmd = [sys.executable, str(Path(__file__).resolve()), "--config", config_path, "--loop", str(int(interval))]
    if caffeinate and shutil.which("caffeinate"):
        cmd = ["caffeinate", "-i", *cmd]
    log = open(sdir / LOG_FILE, "a", encoding="utf-8")
    subprocess.Popen(cmd, cwd=str(root), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    for _ in range(50):
        time.sleep(0.2)
        pid = read_pid(sdir)
        if pid is not None:
            print(f"시작했다 (PID {pid}). 기록: {sdir / LOG_FILE}")
            return 0
    print(f"시작하지 못했다. {sdir / LOG_FILE} 끝부분을 본다")
    return 1


def stop(cfg: dict, root: Path = ROOT, wait_sec: float = 15) -> int:
    import signal
    sdir = _resolve(cfg["paths"]["samples"], root)
    pid = read_pid(sdir)
    if pid is None:
        (sdir / PID_FILE).unlink(missing_ok=True)
        print("돌고 있지 않다")
        return 0
    os.kill(pid, signal.SIGTERM)
    t0 = time.time()
    while time.time() - t0 < wait_sec:
        if not _pid_alive(pid):
            print(f"멈췄다 (PID {pid})")
            return 0
        time.sleep(0.2)
    print(f"{wait_sec:.0f}초 안에 끝나지 않았다 (PID {pid}). 확인: ps -p {pid}")
    return 1


def status(cfg: dict, root: Path = ROOT, now: Optional[float] = None) -> int:
    """0 = 돌고 있고 마지막 표본이 간격의 2배 안, 1 = 안 돈다, 2 = 돌지만 표본이 늦다(Mac 이 잤거나 회차가 실패)."""
    sdir = _resolve(cfg["paths"]["samples"], root)
    interval = float(cfg["sample"]["interval_sec"])
    pid = read_pid(sdir)
    files = sorted(sdir.glob("????-??-??.jsonl"))
    last = None
    if files:
        lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
        if lines:
            last = json.loads(lines[-1]).get("ts")
    now = time.time() if now is None else now
    age = None if last is None else now - float(last)
    when = "없음" if last is None else f"{datetime.fromtimestamp(float(last), KST):%m-%d %H:%M:%S} ({age / 60:.0f}분 전)"
    print(f"표본 수집기: {'돌고 있음 (PID ' + str(pid) + ')' if pid else '멈춤'} · 마지막 표본: {when}")
    if pid is None:
        return 1
    return 0 if age is not None and age <= 2 * interval else 2


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "measure.yaml"))
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--print", action="store_true", help="파일에 붙이지 않고 출력만")
    g.add_argument("--loop", type=float, metavar="SEC", help="SEC 초마다 반복 (앞에서. Ctrl+C 로 끝)")
    g.add_argument("--start", action="store_true", help="백그라운드로 반복 시작 (간격은 설정의 sample.interval_sec)")
    g.add_argument("--stop", action="store_true", help="백그라운드 반복을 멈춘다")
    g.add_argument("--status", action="store_true", help="돌고 있는지와 마지막 표본 시각")
    ap.add_argument("--no-caffeinate", action="store_true", help="--start 때 caffeinate 로 감싸지 않는다")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    if a.loop:
        if a.loop < 10:
            ap.error("--loop 는 10초 이상")
        return loop(cfg, a.loop)
    if a.start:
        return start(cfg, float(cfg["sample"]["interval_sec"]), a.config, caffeinate=not a.no_caffeinate)
    if a.stop:
        return stop(cfg)
    if a.status:
        return status(cfg)
    st = sample_once(cfg)
    if a.print:
        print(json.dumps(st, ensure_ascii=False, indent=1))
    else:
        print(f"{append(cfg, st)}: 컨테이너 {len(st['containers'])}개, 새 배치 {len(st['batches'])}개, CPU 합 {cpu_sum(st):.0f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
