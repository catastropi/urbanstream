"""STEP 2 환경 기록 (설계안 4장). 측정 한 번마다 이 JSON 을 남기고, 결과 표의 '환경' 열은 이 파일 이름을 가리킨다. 읽기만 한다.

비밀 값은 읽지 않는다. .env 는 아래 ENV_ALLOW 패턴에 맞는 이름(한도·버전 변수)만 이름과 값을 적고, 나머지 변수는 이름도 적지 않는다.
컨테이너 환경변수도 같은 규칙으로 고른다(docker inspect 의 Config.Env 에는 비밀번호가 들어 있다).

사용
  python scripts/measure_env.py                         data/measure/env/env-<KST 시각>.json
  python scripts/measure_env.py --note "M5 11-27"       메모를 같이 적는다
  python scripts/measure_env.py --input-table bronze/bus_position --input-date 2026-11-27   입력 데이터 규모를 같이 센다
  python scripts/measure_env.py --copy-to-docs          docs/performance/ 에도 복사한다 (결과와 같이 커밋할 때)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from measure_guard import KST, ROOT, _resolve, load_config  # noqa: E402
import measure_sample as S  # noqa: E402

# 이름이 이 패턴에 맞는 변수만 기록한다. 비밀 이름 패턴(DENY)에 걸리면 맞아도 버린다.
ENV_ALLOW = re.compile(r"^(.*_MEM_LIMIT|.*_CPUS|AIRFLOW_PARALLELISM|AIRFLOW_VERSION|SPARK_VERSION|PYSPARK_VERSION|.*_IMAGE|.*_IMAGE_TAG|"
                       r"GOLD_STAGE_MODE|GOLD_DRIVER_MEMORY|GOLD_SPARK_MASTER|GOLD_RUNNER_[A-Z_]+|MEASURE_[A-Z_]+|"
                       r"BRONZE_TRIGGER_SEC|BRONZE_MAX_OFFSETS_PER_TRIGGER|COMPOSE_PROFILES|TZ)$")
ENV_DENY = re.compile(r"(KEY|PASSWORD|SECRET|TOKEN|WEBHOOK|FERNET|DSN|PASS|CRED)", re.I)
IMAGES = ("urbanstream/",)


def safe_env(items: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in sorted(items.items()) if ENV_ALLOW.match(k) and not ENV_DENY.search(k)}


def _out(cmd: list[str], run: Callable = subprocess.run) -> Optional[str]:
    return S._run(cmd, run)


def machine(run: Callable = subprocess.run) -> dict:
    if sys.platform != "darwin":
        return {"platform": sys.platform}
    def sysctl(k: str) -> Optional[str]:
        v = _out(["sysctl", "-n", k], run)
        return v.strip() if v else None
    mem = sysctl("hw.memsize")
    return {"platform": "darwin", "model": sysctl("hw.model"), "chip": sysctl("machdep.cpu.brand_string"),
            "cores": sysctl("hw.ncpu"), "mem_bytes": int(mem) if mem and mem.isdigit() else None,
            "macos": (_out(["sw_vers", "-productVersion"], run) or "").strip() or None, **S.power_state(run)}


def docker_desktop_settings(home: Path = Path.home()) -> dict:
    """Docker Desktop 설정 파일에서 자원·폴더 공유 방식만 고른다(파일 이름은 버전마다 다르다). 없으면 빈 dict."""
    want = ("cpus", "memoryMiB", "swapMiB", "diskSizeMiB", "useVirtualizationFramework", "useVirtualizationFrameworkVirtioFS",
            "useGrpcfuse", "filesharingDirectories", "useContainerdSnapshotter", "kernelForUDP")
    for p in (home / "Library/Group Containers/group.com.docker/settings-store.json",
              home / "Library/Group Containers/group.com.docker/settings.json"):
        if p.exists():
            try:
                d = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            pick = {k: d[k] for k in d if k in want or k.lower() in {w.lower() for w in want}}
            vfs = pick.get("useVirtualizationFrameworkVirtioFS")
            grpc = pick.get("useGrpcfuse")
            pick["file_sharing"] = "VirtioFS" if vfs else ("gRPC FUSE" if grpc else ("osxfs(legacy)" if vfs is False and grpc is False else None))
            pick["settings_file"] = p.name
            return pick
    return {}


def docker_info(run: Callable = subprocess.run) -> dict:
    t = _out(["docker", "info", "--format", "{{json .}}"], run)
    if t is None:
        return {"running": False}
    d = json.loads(t)
    v = _out(["docker", "version", "--format", "{{json .}}"], run)
    ver = json.loads(v) if v else {}
    return {"running": True, "server_version": (ver.get("Server") or {}).get("Version"),
            "client_version": (ver.get("Client") or {}).get("Version"),
            "desktop": ((ver.get("Server") or {}).get("Platform") or {}).get("Name"),
            "ncpu": d.get("NCPU"), "mem_total": d.get("MemTotal"), "os": d.get("OperatingSystem"),
            "kernel": d.get("KernelVersion"), "cgroup": d.get("CgroupVersion"), "storage_driver": d.get("Driver"),
            "settings": docker_desktop_settings()}


def images(run: Callable = subprocess.run) -> list[str]:
    t = _out(["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"], run) or ""
    return sorted(x for x in t.splitlines() if x.startswith(IMAGES))


def git_state(root: Path, run: Callable = subprocess.run) -> dict:
    head = _out(["git", "-C", str(root), "rev-parse", "HEAD"], run)
    st = _out(["git", "-C", str(root), "status", "--porcelain"], run)
    changed = [ln[3:] for ln in (st or "").splitlines() if ln.strip()]
    return {"commit": head.strip() if head else None, "dirty": bool(changed), "changed_files": changed[:50]}


def config_hashes(root: Path) -> dict[str, str]:
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()[:16] for p in sorted((root / "config").glob("*.yaml"))}


def container_settings(run: Callable = subprocess.run) -> list[dict]:
    ids = (_out(["docker", "ps", "-q", "--filter", f"name={S.PREFIX}"], run) or "").split()
    if not ids:
        return []
    out = []
    for d in json.loads(_out(["docker", "inspect", *ids], run) or "[]"):
        env = dict(x.split("=", 1) for x in (d.get("Config") or {}).get("Env") or [] if "=" in x)
        hc = d.get("HostConfig") or {}
        out.append({"service": ((d.get("Config") or {}).get("Labels") or {}).get("com.docker.compose.service"),
                    "image": (d.get("Config") or {}).get("Image"), "mem_limit": hc.get("Memory"),
                    "nano_cpus": hc.get("NanoCpus"), "env": safe_env(env)})
    return sorted(out, key=lambda x: x["service"] or "")


def input_scale(root: Path, table: str, day: str) -> dict:
    """data/<table>/event_date=<day>/ (또는 observed_date) 의 Parquet 파일 수·바이트. 행 수는 Parquet 꼬리말에서 센다(pyarrow 있으면)."""
    base = root / "data" / table
    for part in ("event_date", "observed_date"):
        d = base / f"{part}={day}"
        if d.is_dir():
            files = [p for p in d.glob("*.parquet") if not p.name.startswith((".", "_"))]
            rows = None
            try:
                import pyarrow.parquet as pq
                rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files)
            except Exception:                                      # noqa: BLE001
                pass
            return {"table": table, "date": day, "files": len(files), "bytes": sum(f.stat().st_size for f in files), "rows": rows}
    return {"table": table, "date": day, "files": 0, "bytes": 0, "rows": 0}


def collect(cfg: dict, root: Path = ROOT, run: Callable = subprocess.run, *, note: str = "",
            inputs: Optional[list[tuple[str, str]]] = None) -> dict:
    now = datetime.now(KST)
    st = S.collect_state(cfg, root, run)
    return {"recorded_at": now.isoformat(timespec="seconds"), "note": note,
            "machine": machine(run), "docker": docker_info(run), "images": images(run),
            "code": {**git_state(root, run), "config_sha16": config_hashes(root)},
            "dotenv": safe_env(S._dotenv(root / ".env")), "containers": container_settings(run),
            "state": {"containers": [{k: c.get(k) for k in ("service", "state", "health", "restarts", "cpu_pct", "mem_bytes", "pids")}
                                     for c in st["containers"]],
                      "vm": st.get("vm"), "disk_free": st.get("disk_free"), "runner_queue": st.get("runner_queue")},
            "inputs": [input_scale(root, t, d) for t, d in (inputs or [])]}


def write(cfg: dict, env: dict, root: Path = ROOT, copy_to_docs: bool = False) -> Path:
    d = _resolve(cfg["paths"]["env"], root)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"env-{datetime.fromisoformat(env['recorded_at']):%Y%m%dT%H%M%S}.json"
    p.write_text(json.dumps(env, ensure_ascii=False, indent=1), encoding="utf-8")
    if copy_to_docs:
        dd = _resolve(cfg["paths"]["report_dir"], root)
        dd.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dd / p.name)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "measure.yaml"))
    ap.add_argument("--note", default="")
    ap.add_argument("--input-table", action="append", default=[], help="예: bronze/bus_position (여러 번)")
    ap.add_argument("--input-date", default=None)
    ap.add_argument("--copy-to-docs", action="store_true")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    inputs = [(t, a.input_date) for t in a.input_table] if a.input_date else []
    env = collect(cfg, note=a.note, inputs=inputs)
    p = write(cfg, env, copy_to_docs=a.copy_to_docs)
    print(f"환경 기록: {p}  (커밋 {str(env['code']['commit'])[:7]}{' dirty' if env['code']['dirty'] else ''}, "
          f"Docker CPU {env['docker'].get('ncpu')}, 메모리 {round((env['docker'].get('mem_total') or 0) / 1024 ** 3, 1)} GiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
