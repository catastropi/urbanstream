"""STEP 2 측정 하네스 (설계안 5·6장): 실행 전 검사 → 실행 → 10초 감시(자동 중단) → 기록. 항목별 하위 명령.

모든 능동 측정은 data/measure/results/<항목>.jsonl 에 한 회 한 줄로 남는다(무효 회도 지우지 않고 이유와 함께).
요약은 scripts/measure_report.py 가 만든다. Spark 가 필요한 측정은 gold-runner 요청 큐(kind=measure)로, 합성 Gold 적재는
measure 컨테이너(`docker compose --profile core --profile measure run --rm`)로 돈다. 운영 컨테이너는 건드리지 않는다(중단은 측정 요청의 .cancel
또는 측정 컨테이너 kill 뿐). 이 스크립트가 운영 폴더에 쓰는 것은 gold-runner 요청 파일(data/gold_stage/_requests/measure-*) 하나뿐이다.

하위 명령 (프로젝트 루트, 호스트 Python. 순서와 예시는 docs/runbooks/measure.md)
  M6 합성 (scripts/synth_bronze.py generate 로 만든 <run_id>)
    synth-silver  --run-id s1 --date 2026-01-05            합성 Bronze → Silver 빌드 시간·메모리
    synth-stage   --run-id s1 --date 2026-01-05            합성 Silver → Gold 스테이징 (silver-point 를 먼저 한 번)
    synth-point   --run-id s1 --date 2026-01-05            합성 Silver 버전을 측정 폴더 안의 포인터로 게시 (Spark 없음, 측정 아님)
    db-create / db-drop                                     측정 DB(urbanstream_measure) 만들기(마이그레이션 001~)·지우기
    synth-load    --run-id s1                               합성 스테이징 → 측정 DB 적재 시간·최대 메모리 (measure 컨테이너)
  M7 작은 파일
    copy          --date 2026-11-27 --run-id c1             실제 하루치를 표·시간당 1개로 합친 사본 (원본은 읽기만)
    m7            --date 2026-11-27 --run-id c1 --what build|verify   원본·사본을 A,B,A,B 로 번갈아 잰다
  M5 재처리 (Airflow reprocess DAG 로 실행하고, 그동안 감시만 한다)
    watch         --label m5-2026-11-27 --max-sec 1500      감시를 켜 두고, 걸리면 실행 중인 gold-runner 요청을 취소한다
  M8 API
    api           --n 20                                    엔드포인트 3개 응답 시간
  A5 안전장치 시험
    selftest                                               작은 합성으로 요청 → 강제 중단(.cancel) → 실패 기록까지 확인
공통 옵션: --repeat N (기본은 설계안 5-1 규칙), --no-gate (시험용. 결과는 무효로 표시), --expected-sec
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT, ROOT / "src", HERE):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from measure_guard import (GIB, KST, GuardError, Lock, Watchdog, _resolve, ensure_measure_db, ensure_measure_path,  # noqa: E402
                           gate_checks, load_config, print_checks)

NON_SPARK_TABLES = ["silver_traffic_speed", "silver_bus_position", "silver_bus_arrival", "silver_weather"]   # M7: 돌발은 파티션 규칙이 달라 뺀다
ALL_TABLES = NON_SPARK_TABLES + ["silver_incident_observation"]
SILVER_SOURCE = {"silver_traffic_speed": "seoul_traffic", "silver_bus_position": "bus_position", "silver_bus_arrival": "bus_arrival",
                 "silver_weather": "kma_ncst", "silver_incident_observation": "seoul_incident"}
DEFAULT_EXPECTED = {"synth_silver": 600, "synth_stage": 600, "synth_load": 300, "m7_build": 400, "m7_verify": 400, "selftest": 120,
                    "copy": 300, "api": 60, "watch": 600}


# ------------------------------------------------------------------ 경로 변환 (호스트 ↔ 컨테이너)

def runner_path(host: Path, root: Path = ROOT) -> str:
    """호스트 data/... → gold-runner 의 /data/... (compose 마운트와 같다)."""
    rel = Path(host).resolve().relative_to((Path(root) / "data").resolve())
    return "/data/" + rel.as_posix()


def measure_container_path(host: Path) -> str:
    rel = Path(host).resolve().relative_to(ROOT.resolve())
    return "/opt/urbanstream/" + rel.as_posix()


# ------------------------------------------------------------------ 기록

def now_kst() -> datetime:
    return datetime.now(KST)


def append_result(cfg: dict, item: str, row: dict, root: Path = ROOT) -> Path:
    d = _resolve(cfg["paths"]["results"], root)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{item}.jsonl"
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")
    return p


def record_env(cfg: dict, note: str, inputs: Optional[list[tuple[str, str]]] = None) -> str:
    import measure_env
    env = measure_env.collect(cfg, note=note, inputs=inputs or [])
    return measure_env.write(cfg, env).name


def slept_between(t0: datetime, t1: datetime, run: Callable = subprocess.run) -> Optional[bool]:
    """macOS pmset 로그에 t0~t1 사이 Sleep 항목이 있는지. 다른 OS 면 None."""
    if sys.platform != "darwin":
        return None
    try:
        out = run(["pmset", "-g", "log"], capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    return parse_sleep_log(out, t0, t1)


def parse_sleep_log(text: str, t0: datetime, t1: datetime) -> bool:
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 4 or parts[3] != "Sleep":
            continue
        try:
            t = datetime.strptime(" ".join(parts[:3]), "%Y-%m-%d %H:%M:%S %z")
        except ValueError:
            continue
        if t0 <= t <= t1:
            return True
    return False


# ------------------------------------------------------------------ 반복 규칙 (설계안 5-1)

def plan_reps(first_sec: float, cfg: dict, repeat: Optional[int]) -> tuple[bool, int]:
    """첫 회 소요로 (첫 회를 예열로 볼지, 첫 회 뒤에 더 돌 횟수)."""
    r = cfg["repeat"]
    if repeat is not None:
        return False, max(0, repeat - 1)
    if first_sec < r["short_sec"]:
        return True, int(r["short_n"])
    return False, int(r["long_n"]) - 1


# ------------------------------------------------------------------ gold-runner 요청

class Runner:
    """gold-runner 요청 큐에 측정 요청을 넣고 10초 감시하며 기다린다."""

    def __init__(self, cfg: dict, root: Path = ROOT, sampler: Optional[Callable[[], dict]] = None,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time):
        self.cfg, self.root = cfg, root
        self.req_dir = root / "data" / "gold_stage" / "_requests"
        self.sampler = sampler
        self.sleep, self.clock = sleep, clock

    def alive(self) -> bool:
        p = self.req_dir / ".runner_alive"
        return p.exists() and self.clock() - p.stat().st_mtime < 180

    def submit(self, job: str, args: dict, d0: date, d1: date, stage_dir: Path) -> str:
        rid = f"measure-{now_kst():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        body = {"id": rid, "key": f"measure__{rid}", "date_from": d0.isoformat(), "date_to": d1.isoformat(),
                "stage_dir": runner_path(stage_dir, self.root), "created_at": self.clock(), "kind": "measure", "args": {"job": job, **args}}
        self.req_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.req_dir / f".{rid}.json.tmp"
        tmp.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.req_dir / f"{rid}.json")
        return rid

    def result(self, rid: str) -> Optional[tuple[str, dict]]:
        for k in ("done", "failed", "superseded"):
            p = self.req_dir / f"{rid}.{k}"
            if p.exists():
                try:
                    return k, json.loads(p.read_text(encoding="utf-8"))
                except ValueError:
                    return None                                  # 쓰는 중
        return None

    def cancel(self, rid: str) -> None:
        (self.req_dir / f"{rid}.cancel").write_text("measure watchdog", encoding="utf-8")

    def wait(self, rid: str, wd: Watchdog, max_sec: float) -> tuple[str, dict, Optional[str]]:
        """끝날 때까지 감시. 반환 = (done|failed|cancelled, 결과, 중단 이유)."""
        t0 = self.clock()
        reason = None
        interval = float(self.cfg["watch"]["interval_sec"])
        while True:
            r = self.result(rid)
            if r is not None:
                return (r[0] if reason is None else "cancelled"), r[1], reason
            if reason is None:
                if self.sampler is not None:
                    reason = wd.feed(self.sampler())
                if reason is None and self.clock() - t0 > max_sec:
                    reason = f"하네스 대기 한도 {int(max_sec)}초를 넘었다"
                if reason is not None:
                    self.cancel(rid)
                    t_cancel = self.clock()
            elif self.clock() - t_cancel > 90:
                return "cancelled", {}, reason + " (실행기가 90초 안에 끝을 기록하지 않았다)"
            self.sleep(interval)


# ------------------------------------------------------------------ 한 회 실행 공통

def gate(cfg: dict, expected_sec: float, expected_bytes: float = 0, *, skip: bool = False) -> tuple[bool, list]:
    if skip:
        return True, []
    import measure_sample as S
    state, cpu_avg = S.collect_gate_state(cfg, ROOT)
    checks = gate_checks(state, cfg, now=now_kst(), expected_sec=expected_sec, expected_bytes=expected_bytes,
                         cpu_avg=cpu_avg, lock_exists=False)
    print_checks(checks)
    return all(c.ok for c in checks), [c.__dict__ for c in checks]


def sampler(cfg: dict) -> Callable[[], dict]:
    import measure_sample as S
    return lambda: S.collect_state(cfg, ROOT, with_queue=False)


def run_reps(cfg: dict, item: str, once: Callable[[int], dict], *, repeat: Optional[int], expected_sec: float,
             expected_bytes: float = 0, no_gate: bool = False, meta: Optional[dict] = None, env_file: Optional[str] = None) -> list[dict]:
    """설계안 5-1 규칙으로 반복한다. once(rep) = 한 회 실행 결과 dict(elapsed_sec, status, ...). 매 회 앞에 검사를 한다.
    연속 두 번 중단되면 멈춘다(6-6)."""
    rows: list[dict] = []
    planned: Optional[int] = None
    warm = False
    aborted_in_row = 0
    rep = 0
    while planned is None or rep <= planned:
        ok, checks = gate(cfg, expected_sec, expected_bytes, skip=no_gate)
        if not ok:
            print(f"[{item}] 검사 통과 못 함: {rep}회째에서 멈춘다. 다음 창에서 같은 명령을 다시 실행한다")
            break
        t0 = now_kst()
        res = once(rep)
        t1 = now_kst()
        slept = slept_between(t0, t1)
        invalid = []
        if no_gate:
            invalid.append("검사 없이 실행(--no-gate)")
        if res.get("status") != "done":
            invalid.append(res.get("abort_reason") or f"실행 결과 {res.get('status')}")
        if slept:
            invalid.append("측정 중 Mac 잠듦 (pmset)")
        if planned is None:
            warm, more = plan_reps(float(res.get("elapsed_sec") or (t1 - t0).total_seconds()), cfg, repeat)
            planned = more
        row = {"item": item, "rep": rep, "warmup": warm and rep == 0, "valid": not invalid, "invalid_reason": "; ".join(invalid) or None,
               "started_kst": t0.isoformat(timespec="seconds"), "ended_kst": t1.isoformat(timespec="seconds"),
               "wall_sec": round((t1 - t0).total_seconds(), 1), "env_file": env_file, "gate": checks, **(meta or {}), **res}
        append_result(cfg, item, row)
        rows.append(row)
        print(f"[{item}] {rep}회 {'(예열) ' if row['warmup'] else ''}{res.get('status')} {res.get('elapsed_sec')}초"
              + (f"  무효: {row['invalid_reason']}" if invalid else ""))
        aborted_in_row = aborted_in_row + 1 if res.get("status") == "cancelled" else 0
        if aborted_in_row >= 2:
            print(f"[{item}] 두 번 연속 중단: 이 항목을 멈춘다. 규모를 낮추거나 한계로 적는다 (설계안 6-6)")
            break
        if res.get("status") == "cancelled":
            settle(cfg)
        rep += 1
    return rows


def settle(cfg: dict, sleep: Callable[[float], None] = time.sleep) -> None:
    """중단 뒤 모든 컨테이너가 settle_healthy_sec 동안 이어서 정상일 때까지 기다린다."""
    import measure_sample as S
    from measure_guard import by_service, container_ok
    need = float(cfg["watch"]["settle_healthy_sec"])
    good_since = None
    print(f"중단 뒤 대기: 모두 정상이 {int(need)}초 이어질 때까지")
    while True:
        st = S.collect_state(cfg, ROOT, with_queue=False)
        svc = by_service(st)
        ok = all(container_ok(svc.get(s)) for s in cfg["gate"]["expected_containers"])
        good_since = (good_since or st["ts"]) if ok else None
        if good_since and st["ts"] - good_since >= need:
            return
        sleep(15)


def runner_once(cfg: dict, job: str, args: dict, d0: date, d1: date, stage_dir: Path, *, max_sec: float) -> dict:
    r = Runner(cfg, sampler=sampler(cfg))
    if not r.alive():
        return {"status": "failed", "abort_reason": "gold-runner 박동이 3분 넘게 없다 (scripts/stack.sh up-airflow)"}
    stage_dir.mkdir(parents=True, exist_ok=True)
    wd = Watchdog(cfg, targets=["gold-runner"])
    rid = r.submit(job, args, d0, d1, stage_dir)
    status, body, reason = r.wait(rid, wd, max_sec)
    return {"status": status, "request_id": rid, "elapsed_sec": body.get("elapsed_sec"), "returncode": body.get("returncode"),
            "mem_max_bytes": body.get("mem_max_bytes"), "abort_reason": reason, "watch": wd.summary(),
            "stdout_tail": body.get("stdout_tail"), "stderr_tail": (body.get("stderr_tail") or "")[-2000:] or None}


# ------------------------------------------------------------------ M6 합성

def synth_dir(cfg: dict, run_id: str) -> Path:
    return ensure_measure_path(Path(cfg["paths"]["synthetic"]) / run_id, cfg)


def synth_manifest(cfg: dict, run_id: str) -> dict:
    p = synth_dir(cfg, run_id) / "manifest.json"
    if not p.exists():
        raise SystemExit(f"합성 manifest 가 없습니다: {p} (scripts/synth_bronze.py generate 먼저)")
    m = json.loads(p.read_text(encoding="utf-8"))
    if m.get("synthetic") is not True:
        raise SystemExit("manifest 에 synthetic=true 가 없습니다")
    return m


def synth_meta(m: dict) -> dict:
    return {"data": "합성", "run_id": m["run_id"], "scale": m["scale"], "input_date": m["date"], "input_rows": m["total"]["rows"],
            "input_files": m["total"]["files"], "input_bytes": m["total"]["bytes"], "manifest_sha256": m["content_sha256"]}


def cmd_synth_silver(cfg: dict, a: argparse.Namespace) -> int:
    m = synth_manifest(cfg, a.run_id)
    sd = synth_dir(cfg, a.run_id)
    d = date.fromisoformat(a.date or m["date"])
    tables = a.tables.split(",") if a.tables else ALL_TABLES
    (sd / "_current").mkdir(exist_ok=True)
    env = record_env(cfg, f"M6 synth-silver {a.run_id}")

    def once(rep: int) -> dict:
        version = f"m{rep}"
        rb = sd / "silver_rebuild"
        for t in tables:                                         # 같은 버전 이름을 다시 쓰지 않게 이전 회 폴더를 지운다
            shutil.rmtree(rb / t / version, ignore_errors=True)
        return runner_once(cfg, "silver_build", {"tables": tables, "version": version, "bronze_root": runner_path(sd / "bronze"),
                                                 "silver_root": runner_path(sd / "_current"), "rebuild_root": runner_path(rb),
                                                 "report": runner_path(sd / "reports" / f"silver_build_{version}.json")},
                           d, d, sd / "reports", max_sec=float(cfg["runner"]["measure_timeout_sec"]) + 120)
    run_reps(cfg, "m6_synth_silver", once, repeat=a.repeat, expected_sec=a.expected_sec or DEFAULT_EXPECTED["synth_silver"],
             no_gate=a.no_gate, meta={**synth_meta(m), "tables": tables}, env_file=env)
    return 0


def cmd_synth_point(cfg: dict, a: argparse.Namespace) -> int:
    m = synth_manifest(cfg, a.run_id)
    sd = synth_dir(cfg, a.run_id)
    d = date.fromisoformat(a.date or m["date"])
    tables = a.tables.split(",") if a.tables else ALL_TABLES
    res = runner_once(cfg, "silver_point", {"tables": tables, "version": a.version, "rebuild_root": runner_path(sd / "silver_rebuild")},
                      d, d, sd / "reports", max_sec=300)
    print(json.dumps({k: res.get(k) for k in ("status", "request_id", "stdout_tail", "stderr_tail")}, ensure_ascii=False, indent=1))
    return 0 if res["status"] == "done" else 1


def cmd_synth_stage(cfg: dict, a: argparse.Namespace) -> int:
    m = synth_manifest(cfg, a.run_id)
    sd = synth_dir(cfg, a.run_id)
    d = date.fromisoformat(a.date or m["date"])
    (sd / "_current").mkdir(exist_ok=True)
    env = record_env(cfg, f"M6 synth-stage {a.run_id}")

    def once(rep: int) -> dict:
        st = sd / "gold_stage" / f"r{rep}"
        shutil.rmtree(st, ignore_errors=True)
        res = runner_once(cfg, "gold_stage", {"silver_root": runner_path(sd / "_current"), "silver_rebuild_root": runner_path(sd / "silver_rebuild"),
                                              "bronze_root": runner_path(sd / "bronze"), "stage_root": runner_path(st)},
                          d, d, sd / "reports", max_sec=float(cfg["runner"]["measure_timeout_sec"]) + 120)
        mf = st / "manifest.json"
        if mf.exists():
            j = json.loads(mf.read_text(encoding="utf-8"))
            res.update(stage_rows=j.get("row_counts"), stage_elapsed_by_table=j.get("elapsed_by_table"))
        return res
    run_reps(cfg, "m6_synth_stage", once, repeat=a.repeat, expected_sec=a.expected_sec or DEFAULT_EXPECTED["synth_stage"],
             no_gate=a.no_gate, meta=synth_meta(m), env_file=env)
    return 0


def _admin_conn(dbname: str):
    import psycopg
    import measure_sample as S
    env = {**S._dotenv(ROOT / ".env"), **{k: v for k, v in os.environ.items() if k.startswith("POSTGRES_")}}
    info = (f"host={env.get('POSTGRES_HOST', '127.0.0.1')} port={env.get('POSTGRES_PORT', '5433')} dbname={dbname} "
            f"user={env.get('POSTGRES_USER', 'urbanstream')} password={env.get('POSTGRES_PASSWORD', '')}")
    return psycopg.connect(info, autocommit=True)


def cmd_db_create(cfg: dict, a: argparse.Namespace) -> int:
    from psycopg import sql
    from migrate import migrate                                 # scripts/migrate.py
    db = ensure_measure_db(cfg["synthetic"]["measure_db"], cfg)
    with _admin_conn(os.environ.get("POSTGRES_DB") or "urbanstream") as c:
        if c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db,)).fetchone() is None:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
            print(f"데이터베이스 {db}: 생성")
        c.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO gold_loader").format(sql.Identifier(db)))
    with _admin_conn(db) as c:
        c.execute("SET statement_timeout = '15min'")
        done = migrate(c, ROOT / "sql")
        print("마이그레이션: " + (", ".join(done) if done else "이미 최신"))
    return 0


def cmd_db_drop(cfg: dict, a: argparse.Namespace) -> int:
    from psycopg import sql
    db = ensure_measure_db(cfg["synthetic"]["measure_db"], cfg)
    with _admin_conn(os.environ.get("POSTGRES_DB") or "urbanstream") as c:
        c.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db)))
    print(f"데이터베이스 {db}: 지움")
    return 0


FACT_TABLES = ["fact.traffic_speed", "fact.bus_position", "fact.bus_arrival", "fact.incident_observation", "fact.weather_obs",
               "fact.incident_episode", "ops.call_audit", "ops.load_run"]


def reset_measure_facts(cfg: dict) -> None:
    db = ensure_measure_db(cfg["synthetic"]["measure_db"], cfg)
    with _admin_conn(db) as c:
        if c.execute("SELECT current_database()").fetchone()[0] != db:
            raise GuardError("측정 DB 가 아닌 곳에 붙었다")
        c.execute("TRUNCATE " + ", ".join(FACT_TABLES))


def cmd_synth_load(cfg: dict, a: argparse.Namespace) -> int:
    m = synth_manifest(cfg, a.run_id)
    sd = synth_dir(cfg, a.run_id)
    stage = sd / "gold_stage" / a.stage
    if not (stage / "manifest.json").exists():
        raise SystemExit(f"스테이징이 없습니다: {stage} (synth-stage 먼저)")
    env = record_env(cfg, f"M6 synth-load {a.run_id}")

    def once(rep: int) -> dict:
        reset_measure_facts(cfg)                                 # 매 회 빈 표에 넣는다(회마다 같은 조건)
        return container_once(cfg, ["scripts/measure_run.py", "load-inner", "--stage", measure_container_path(stage)],
                              max_sec=float(cfg["runner"]["measure_timeout_sec"]))
    run_reps(cfg, "m6_synth_load", once, repeat=a.repeat, expected_sec=a.expected_sec or DEFAULT_EXPECTED["synth_load"],
             no_gate=a.no_gate, meta={**synth_meta(m), "stage": a.stage}, env_file=env)
    return 0


# measure 는 postgres(core 프로필)에 depends_on 이라 core 도 켜야 Compose 가 정의를 받아들인다. --no-deps 라 postgres 를 새로 띄우지는 않는다.
MEASURE_COMPOSE = ["docker", "compose", "--profile", "core", "--profile", "measure"]


def container_once(cfg: dict, args: list[str], *, max_sec: float, run: Callable = subprocess.Popen) -> dict:
    """measure 컨테이너를 run --rm 으로 띄우고 10초 감시. 걸리면 그 컨테이너만 kill. 마지막 줄의 JSON 이 결과."""
    name = f"urbanstream-measure-{uuid.uuid4().hex[:8]}"
    cmd = [*MEASURE_COMPOSE, "run", "--rm", "--no-deps", "-T", "--name", name, "measure", *args]
    wd = Watchdog(cfg, targets=["measure", name])
    smp = sampler(cfg)
    t0 = time.monotonic()
    import tempfile
    fo, fe = tempfile.TemporaryFile("w+", encoding="utf-8"), tempfile.TemporaryFile("w+", encoding="utf-8")   # 파이프가 차서 막히지 않게
    proc = run(cmd, cwd=str(ROOT), stdout=fo, stderr=fe, text=True)
    reason = None
    while proc.poll() is None:
        reason = reason or wd.feed(smp())
        if reason is None and time.monotonic() - t0 > max_sec:
            reason = f"{int(max_sec)}초를 넘었다"
        if reason:
            subprocess.run(["docker", "kill", name], capture_output=True)
            break
        time.sleep(float(cfg["watch"]["interval_sec"]))
    proc.wait(timeout=120)
    fo.seek(0)
    fe.seek(0)
    out, err = fo.read(), fe.read()
    res: dict[str, Any] = {}
    for line in reversed((out or "").splitlines()):
        if line.startswith("{"):
            try:
                res = json.loads(line)
                break
            except ValueError:
                continue
    status = "cancelled" if reason else ("done" if proc.returncode == 0 and res else "failed")
    return {"status": status, "abort_reason": reason, "returncode": proc.returncode, "watch": wd.summary(),
            "elapsed_sec": res.get("elapsed_sec"), "mem_max_bytes": res.get("memory_peak_bytes"),
            "mem_source": res.get("memory_peak_source"), "load": res.get("load"),
            "stage_rows": res.get("stage_rows"), "stderr_tail": (err or "")[-2000:] or None}


def cmd_load_inner(cfg: dict, a: argparse.Namespace) -> int:
    """measure 컨테이너 안에서만. 측정 DB 에 적재하고 시간·cgroup 최대 메모리를 JSON 한 줄로."""
    if os.environ.get("MEASURE_IN_CONTAINER") != "1":
        raise SystemExit("load-inner 는 measure 컨테이너 안에서만 실행한다 (synth-load 가 부른다)")
    import psycopg
    from batch.db import conninfo
    from batch import gold_load
    ensure_measure_db(os.environ.get("POSTGRES_DB", ""), cfg, production="urbanstream")
    stage = Path(a.stage).resolve()
    if "/data/measure/" not in stage.as_posix():
        raise GuardError(f"측정 스테이징이 아니다: {stage}")
    cg = Path("/sys/fs/cgroup")
    t0 = time.monotonic()
    with psycopg.connect(conninfo(role="writer"), autocommit=True) as conn:
        if conn.execute("SELECT current_database()").fetchone()[0] != os.environ["POSTGRES_DB"]:
            raise GuardError("측정 DB 가 아닌 곳에 붙었다")
        conn.execute("SET statement_timeout = '15min'")
        out = gold_load.load_all(conn, stage, dag_id="measure")
    el = round(time.monotonic() - t0, 2)
    peak = None
    for f in ("memory.peak", "memory.max_usage_in_bytes"):
        try:
            peak = int((cg / f).read_text().strip())
            break
        except (OSError, ValueError):
            continue
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024        # Linux: KiB. cgroup 값이 없을 때의 대체(프로세스 하나만)
    print(json.dumps({"elapsed_sec": el, "memory_peak_bytes": peak if peak is not None else rss,
                      "memory_peak_source": "cgroup memory.peak" if peak is not None else "ru_maxrss", "load": out,
                      "stage_rows": {s.name: gold_load._staged_rows(stage, s.name) for s in gold_load.SPECS}}, ensure_ascii=False, default=str))
    return 0


# ------------------------------------------------------------------ M7 작은 파일

def compact_partition(src_dir: Path, dst_dir: Path, *, meta: dict) -> dict:
    """한 파티션 폴더의 Parquet 을 수정 시각(KST)의 시간마다 하나로 합친다. 원본은 읽기만."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    files = sorted(f for f in src_dir.glob("*.parquet") if not f.name.startswith((".", "_")))
    groups: dict[str, list[Path]] = {}
    for f in files:
        h = datetime.fromtimestamp(f.stat().st_mtime, KST).strftime("%H")
        groups.setdefault(h, []).append(f)
    dst_dir.mkdir(parents=True, exist_ok=True)
    n_out = rows = skipped = 0
    for h, fs in sorted(groups.items()):
        tables = []
        for f in fs:
            try:
                tables.append(pq.read_table(str(f)))
            except Exception:                                      # noqa: BLE001 (깨진 파일은 Spark 도 건너뛴다)
                skipped += 1
        tables = [t for t in tables if t.num_rows]
        if not tables:
            continue
        t = pa.concat_tables(tables, promote_options="permissive")
        md = dict(t.schema.metadata or {})
        md.update({b"urbanstream.measure_copy": b"true", **{k.encode(): str(v).encode() for k, v in meta.items()}})
        pq.write_table(t.replace_schema_metadata(md), str(dst_dir / f"part-{h}-copy.parquet"), compression="snappy")
        n_out += 1
        rows += t.num_rows
    return {"files_in": len(files), "files_out": n_out, "rows": rows, "skipped": skipped}


def cmd_copy(cfg: dict, a: argparse.Namespace) -> int:
    d = date.fromisoformat(a.date)
    out = ensure_measure_path(Path(cfg["paths"]["copy"]) / a.run_id, cfg)
    if out.exists():
        raise SystemExit(f"이미 있습니다: {out}")
    tables = a.tables.split(",") if a.tables else NON_SPARK_TABLES
    summary: dict[str, Any] = {"run_id": a.run_id, "date": d.isoformat(), "created_at": now_kst().isoformat(timespec="seconds"),
                               "data": "실제 사본", "tables": {}}
    for t in tables:
        src = SILVER_SOURCE[t]
        for layer, name in (("bronze", src), ("silver", t)):
            s = ROOT / "data" / layer / name / f"event_date={d.isoformat()}"
            if not s.is_dir():
                print(f"건너뜀 (없음): {s}")
                continue
            r = compact_partition(s, out / layer / name / f"event_date={d.isoformat()}", meta={"run_id": a.run_id, "source": f"data/{layer}/{name}"})
            summary["tables"][f"{layer}/{name}"] = r
            print(f"{layer}/{name}: 파일 {r['files_in']} → {r['files_out']}, {r['rows']}행")
    (out / "manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


def cmd_m7(cfg: dict, a: argparse.Namespace) -> int:
    d = date.fromisoformat(a.date)
    cp = ensure_measure_path(Path(cfg["paths"]["copy"]) / a.run_id, cfg)
    man = cp / "manifest.json"
    if not man.exists():
        raise SystemExit(f"사본이 없습니다: {man} (copy 먼저)")
    mj = json.loads(man.read_text(encoding="utf-8"))
    tables = a.tables.split(",") if a.tables else NON_SPARK_TABLES
    env = record_env(cfg, f"M7 {a.what} {a.date}", inputs=[(f"bronze/{SILVER_SOURCE[t]}", a.date) for t in tables])
    (cp / "_empty").mkdir(exist_ok=True)
    work = cp / "work"
    item = f"m7_{a.what}"
    pairs = a.pairs or (int(cfg["repeat"]["long_n"]) if a.what == "build" else int(cfg["repeat"]["short_n"]))
    order = [v for _ in range(pairs) for v in ("original", "copy")]          # A, B, A, B (설계안 5-2)
    for i, variant in enumerate(order):
        ok, checks = gate(cfg, a.expected_sec or DEFAULT_EXPECTED[item], skip=a.no_gate)
        if not ok:
            print(f"[{item}] 검사 통과 못 함: {i}번째에서 멈춘다")
            break
        bronze = "/data/bronze" if variant == "original" else runner_path(cp / "bronze")
        silver = "/data/silver" if variant == "original" else runner_path(cp / "silver")
        version = f"m7{i}{variant[0]}"
        if a.what == "build":
            rb = work / "rebuild"
            for t in tables:
                shutil.rmtree(rb / t / version, ignore_errors=True)
            args = {"tables": tables, "version": version, "bronze_root": bronze, "silver_root": silver, "rebuild_root": runner_path(rb),
                    "report": runner_path(work / f"build_{version}.json")}
            job = "silver_build"
        else:
            args = {"bronze_root": bronze, "silver_root": silver, "silver_rebuild_root": runner_path(cp / "_empty"),
                    "verify_since": d.isoformat()}
            job = "silver_verify"
        t0 = now_kst()
        res = runner_once(cfg, job, args, d, d, work, max_sec=float(cfg["runner"]["measure_timeout_sec"]) + 120)
        t1 = now_kst()
        slept = slept_between(t0, t1)
        invalid = [x for x in ("검사 없이 실행(--no-gate)" if a.no_gate else None,
                               None if res["status"] == "done" else (res.get("abort_reason") or res["status"]),
                               "측정 중 Mac 잠듦 (pmset)" if slept else None) if x]
        row = {"item": item, "rep": i, "variant": variant, "warmup": False, "valid": not invalid, "invalid_reason": "; ".join(invalid) or None,
               "data": "실제" if variant == "original" else "실제 사본", "input_date": a.date, "tables": tables,
               "copy_files": {k: v["files_out"] for k, v in mj["tables"].items()}, "original_files": {k: v["files_in"] for k, v in mj["tables"].items()},
               "started_kst": t0.isoformat(timespec="seconds"), "ended_kst": t1.isoformat(timespec="seconds"),
               "env_file": env, "gate": checks, **res}
        append_result(cfg, item, row)
        print(f"[{item}] {i} {variant}: {res['status']} {res.get('elapsed_sec')}초")
        if a.what == "build":
            for t in tables:
                shutil.rmtree(work / "rebuild" / t / version, ignore_errors=True)
    return 0


# ------------------------------------------------------------------ M5 감시만

def cmd_watch(cfg: dict, a: argparse.Namespace) -> int:
    """Airflow 로 시작한 재처리 등을 감시한다. 걸리면 실행 중인 gold-runner 요청에 .cancel 을 만든다(운영 컨테이너는 건드리지 않음)."""
    import measure_sample as S
    env = record_env(cfg, f"watch {a.label}")
    wd = Watchdog(cfg, targets=["gold-runner"])
    req = ROOT / "data" / "gold_stage" / "_requests"
    t0, start = time.monotonic(), now_kst()
    seen_busy = False
    idle_since = None
    cancelled: list[str] = []
    while time.monotonic() - t0 < a.max_sec:
        st = S.collect_state(cfg, ROOT)
        q = st.get("runner_queue") or {}
        busy = bool(q.get("running") or q.get("pending"))
        seen_busy = seen_busy or busy
        reason = wd.feed(st)
        if reason and not cancelled:
            for p in req.glob("*.running"):
                (req / f"{p.stem}.cancel").write_text("measure watch", encoding="utf-8")
                cancelled.append(p.stem)
            print(f"중단: {reason} → 취소 {cancelled or '(실행 중 요청 없음)'}")
        if seen_busy and not busy:
            idle_since = idle_since or time.monotonic()
            if time.monotonic() - idle_since > 60:
                break
        else:
            idle_since = None
        time.sleep(float(cfg["watch"]["interval_sec"]))
    row = {"item": "watch", "label": a.label, "started_kst": start.isoformat(timespec="seconds"),
           "ended_kst": now_kst().isoformat(timespec="seconds"), "env_file": env, "cancelled": cancelled,
           "valid": wd.reason is None, "invalid_reason": wd.reason, **wd.summary()}
    append_result(cfg, "watch", row)
    print(json.dumps({k: row[k] for k in ("label", "valid", "invalid_reason", "max_cpu_pct", "min_free_mem_bytes", "target_peak_mem_bytes")},
                     ensure_ascii=False, default=str))
    return 0 if wd.reason is None else 1


# ------------------------------------------------------------------ M8 API

API_ENDPOINTS = ["/traffic/current", "/incidents/active", "/pipeline/health"]


def cmd_api(cfg: dict, a: argparse.Namespace) -> int:
    import urllib.request
    import measure_sample as S
    env_v = S._dotenv(ROOT / ".env")
    key = env_v.get("DASHBOARD_API_KEY") or (env_v.get("API_KEYS") or "").split(",")[0]
    port = env_v.get("API_PORT") or "8010"
    if not key:
        raise SystemExit(".env 에 DASHBOARD_API_KEY 가 없습니다")
    env = record_env(cfg, "M8 api")
    ok, checks = gate(cfg, DEFAULT_EXPECTED["api"], skip=a.no_gate)
    if not ok:
        return 1
    for ep in API_ENDPOINTS:
        times, codes = [], []
        for i in range(a.n + 1):
            req = urllib.request.Request(f"http://127.0.0.1:{port}{ep}", headers={"X-API-Key": key})
            t = time.perf_counter()
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    r.read()
                    codes.append(r.status)
            except Exception as e:                                 # noqa: BLE001
                codes.append(getattr(e, "code", None) or type(e).__name__)
            times.append(round((time.perf_counter() - t) * 1000, 2))
            time.sleep(a.gap)
        row = {"item": "m8_api", "endpoint": ep, "data": "실제", "n": a.n, "first_ms": times[0], "ms": times[1:], "codes": codes,
               "median_ms": statistics.median(times[1:]), "min_ms": min(times[1:]), "max_ms": max(times[1:]),
               "valid": all(c == 200 for c in codes) and not a.no_gate, "invalid_reason": None if all(c == 200 for c in codes) else f"응답 {set(codes)}",
               "started_kst": now_kst().isoformat(timespec="seconds"), "env_file": env, "gate": checks}
        append_result(cfg, "m8_api", row)
        print(f"{ep}: 첫 호출 {times[0]} ms, 이후 중앙값 {row['median_ms']} ms ({row['min_ms']}~{row['max_ms']})")
    return 0


# ------------------------------------------------------------------ A5 안전장치 시험

def cmd_selftest(cfg: dict, a: argparse.Namespace) -> int:
    """작은 합성(도로 소통만)으로 Silver 빌드를 요청하고, cancel_after 초 뒤 강제로 중단해 실행기가 실패로 기록하는지 본다."""
    import synth_bronze as SB
    run_id = f"selftest-{now_kst():%m%d%H%M}"
    sd = synth_dir(cfg, run_id)
    prof = json.loads(Path(a.profile).read_text(encoding="utf-8"))
    d = date.fromisoformat(cfg["synthetic"]["first_date"])
    SB.generate(prof, scale=1, d=d, seed=1, run_id=run_id, out_dir=sd, sources=["seoul_traffic"])
    (sd / "_current").mkdir(exist_ok=True)
    ok, checks = gate(cfg, DEFAULT_EXPECTED["selftest"], skip=a.no_gate)
    if not ok:
        return 1
    r = Runner(cfg, sampler=sampler(cfg))
    if not r.alive():
        print("FAIL gold-runner 박동 없음")
        return 1
    rid = r.submit("silver_build", {"tables": ["silver_traffic_speed"], "version": "t0", "bronze_root": runner_path(sd / "bronze"),
                                    "silver_root": runner_path(sd / "_current"), "rebuild_root": runner_path(sd / "silver_rebuild")},
                   d, d, sd / "reports")
    t0 = time.monotonic()
    forced = {"n": 0}

    class Forced(Watchdog):
        def feed(self, state: dict) -> Optional[str]:
            super().feed(state)
            forced["n"] += 1
            return "시험용 강제 중단" if time.monotonic() - t0 >= a.cancel_after else None
    status, body, reason = r.wait(rid, Forced(cfg, targets=["gold-runner"]), max_sec=600)
    passed = status == "cancelled" and "취소" in (body.get("stderr_tail") or "")
    print(f"{'PASS' if passed else 'FAIL'} 강제 중단: 상태 {status}, 실행기 기록 {(body.get('stderr_tail') or '').splitlines()[:1]}, 감시 표본 {forced['n']}개")
    append_result(cfg, "selftest", {"item": "selftest", "run_id": run_id, "passed": passed, "status": status, "runner": body,
                                    "reason": reason, "started_kst": now_kst().isoformat(timespec="seconds"), "gate": checks})
    shutil.rmtree(sd, ignore_errors=True)
    return 0 if passed else 1


# ------------------------------------------------------------------ CLI

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "measure.yaml"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--repeat", type=int, default=None)
        p.add_argument("--no-gate", action="store_true")
        p.add_argument("--expected-sec", type=float, default=None)
    for name in ("synth-silver", "synth-stage", "synth-point", "synth-load"):
        p = sub.add_parser(name)
        p.add_argument("--run-id", required=True)
        p.add_argument("--date", default=None)
        p.add_argument("--tables", default=None)
        common(p)
        if name == "synth-point":
            p.add_argument("--version", default="m0")
        if name == "synth-load":
            p.add_argument("--stage", default="r0")
    sub.add_parser("db-create")
    sub.add_parser("db-drop")
    p = sub.add_parser("load-inner")
    p.add_argument("--stage", required=True)
    p = sub.add_parser("copy")
    p.add_argument("--date", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--tables", default=None)
    p = sub.add_parser("m7")
    p.add_argument("--date", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--what", choices=["build", "verify"], required=True)
    p.add_argument("--tables", default=None)
    p.add_argument("--pairs", type=int, default=None)
    common(p)
    p = sub.add_parser("watch")
    p.add_argument("--label", required=True)
    p.add_argument("--max-sec", type=float, default=1500)
    p = sub.add_parser("api")
    p.add_argument("--n", type=int, default=20)
    p.add_argument("--gap", type=float, default=0.5)
    p.add_argument("--no-gate", action="store_true")
    p = sub.add_parser("selftest")
    p.add_argument("--profile", required=True)
    p.add_argument("--cancel-after", type=float, default=20)
    p.add_argument("--no-gate", action="store_true")
    return ap


COMMANDS = {"synth-silver": cmd_synth_silver, "synth-stage": cmd_synth_stage, "synth-point": cmd_synth_point, "synth-load": cmd_synth_load,
            "db-create": cmd_db_create, "db-drop": cmd_db_drop, "load-inner": cmd_load_inner, "copy": cmd_copy, "m7": cmd_m7,
            "watch": cmd_watch, "api": cmd_api, "selftest": cmd_selftest}
NEEDS_LOCK = {"synth-silver", "synth-stage", "synth-load", "copy", "m7", "selftest", "api"}


def main(argv: Optional[list[str]] = None) -> int:
    a = build_parser().parse_args(argv)
    cfg = load_config(a.config)
    fn = COMMANDS[a.cmd]
    if a.cmd in NEEDS_LOCK and os.environ.get("MEASURE_IN_CONTAINER") != "1":
        with Lock(_resolve(cfg["paths"]["lock"], ROOT), note=a.cmd):
            return fn(cfg, a)
    return fn(cfg, a)


if __name__ == "__main__":
    sys.exit(main())
