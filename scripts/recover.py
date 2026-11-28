"""복구 명령 (STEP 3B, 설계안 5장). Mac 의 프로젝트 폴더에서 conda `urbanstream` 환경으로 실행한다. 증상별 안내는 docs/runbooks/recovery.md.

  python scripts/recover.py diagnose            읽기만 한다. 문제마다 '지금 상태 한 줄 + 다음에 칠 명령 한 줄'. 문제가 없으면 '모두 정상'
  python scripts/recover.py clock               Mac 시계와 Docker 시계 비교, 어긋나 있으면 다음 할 일
  python scripts/recover.py resume              멈춘 스택을 순서대로 시작 (수집기 뺀 나머지 → 모두 healthy 대기 → 수집기 → stack.sh check)
  python scripts/recover.py bronze [표...]      Bronze 체크포인트가 Kafka 범위를 벗어났는지와 고칠 내용 (미리보기, D-255)
  python scripts/recover.py bronze 표 --apply   spark-bronze 멈춤 → 확인 → 체크포인트 고침(백업) → 시작 → 3분 안에 healthy·새 파일 확인

지키는 것
  - 비밀 값(.env, 웹훅 URL)을 출력하지 않는다. data/ 에서 지우는 명령이 없다. stack.sh reset·down 을 부르지 않는다.
  - 바꾸는 명령(resume, bronze --apply)은 STEP 2 측정·훈련 잠금(data/measure/.lock)이 있으면 시작하지 않는다.
  - Docker Desktop 자체를 죽이지 않는다(결정 6). 응답이 없으면 사람이 칠 두 줄을 보여 준다.
  - bronze --apply 는 '지워진 구간을 잃었다'를 확정하는 조치라 잃을 건수를 보여 주고 yes 를 받는다(결정 4). 확인이 실패해도 자동으로
    되돌리지 않는다(되돌리면 다시 OffsetOutOfRange). 되돌리는 명령을 출력한다.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hostlib as H  # noqa: E402

DOCKER_RESTART = "pkill -x Docker; pkill -f com.docker.backend   (그다음)   open -a Docker"
INSPECT = "docker inspect -f '{{.Name}} 재시작횟수={{.RestartCount}} OOM={{.State.OOMKilled}} 종료={{.State.FinishedAt}}' "


@dataclass
class Finding:
    name: str
    ok: bool
    detail: str
    next_cmd: str = ""
    info: bool = False          # 문제는 아니지만 알아둘 것 (예: init 이 아직 안 들어간 컨테이너)


# ------------------------------------------------------------------ 점검 (diagnose)

def check_docker(timeout: float, runner) -> Finding:
    ok, why = H.docker_version(timeout, runner=runner)
    return Finding("Docker", ok, f"서버 {why}" if ok else f"응답 없음 ({why})",
                   "" if ok else f"Docker Desktop 다시 띄우기: {DOCKER_RESTART}  → 뜨면 python scripts/recover.py resume")


def check_containers(rows: list[dict], expected: list[str]) -> list[Finding]:
    svc = H.by_service(rows)
    out: list[Finding] = []
    missing = [s for s in expected if s not in svc]
    stopped = [s for s in expected if s in svc and svc[s]["state"] != "running"]
    unhealthy = [s for s in expected if s in svc and svc[s]["state"] == "running" and svc[s]["health"] not in ("", "healthy")]
    ooms = [r for r in rows if r["oom"]]
    if not (missing or stopped or unhealthy):
        out.append(Finding("컨테이너", True, f"{len(expected)}/{len(expected)} 정상"))
    if stopped:
        out.append(Finding("컨테이너", False, "멈춤: " + ", ".join(f"{s}({svc[s]['state']}, 종료 코드 {svc[s].get('exit_code')})" for s in stopped),
                           "python scripts/recover.py resume"))
    if missing:
        out.append(Finding("컨테이너", False, "없음(지워졌거나 아직 안 만듦): " + ", ".join(missing),
                           "mac-on-off.md 2-B (scripts/stack.sh up-*) 로 다시 만든다"))
    if unhealthy:
        out.append(Finding("컨테이너", False, "정상 아님: " + ", ".join(f"{s}({svc[s]['health']})" for s in unhealthy),
                           f"scripts/stack.sh logs {unhealthy[0]}   (켠 지 몇 분 안 됐으면 잠시 기다린다)"))
    for r in ooms:
        out.append(Finding("OOM", False, f"{r['service']} 가 메모리 한도로 죽은 적이 있다 (재시작 {r['restarts']}회)",
                           f"다시 만들기 전에 기록: {INSPECT}{r['name']}"))
    many = [r for r in rows if r["restarts"] >= 3 and not r["oom"]]
    if many:
        out.append(Finding("재시작", True, "재시작이 잦은 컨테이너: " + ", ".join(f"{r['service']} {r['restarts']}회" for r in many), info=True))
    return out


def check_init(rows: list[dict], init_services: list[str]) -> Optional[Finding]:
    svc = H.by_service(rows)
    no = [s for s in init_services if s in svc and not svc[s]["init"]]
    if not no:
        return None
    return Finding("init", True, "init 이 아직 안 들어간 컨테이너: " + ", ".join(no)
                   + ". 다음에 다시 만들 때(stack.sh up-*) 들어간다. 그전에 docker stop 이 'is zombie' 로 실패하면 mac-on-off.md 1-A", info=True)


def check_clock(skew: Optional[float], detail: str) -> Finding:
    if skew is None:
        return Finding("시계", True, f"잴 수 없음 ({detail})", info=True)
    a = abs(skew)
    side = "늦음" if skew < 0 else "빠름"
    if a < 60:
        return Finding("시계", True, f"Docker 시계 차이 {skew:+.0f}초 ({detail})")
    return Finding("시계", False, f"Docker 시계가 Mac 보다 {a:.0f}초 {side}", "python scripts/recover.py clock")


def check_bronze(runner, timeout: float) -> Finding:
    rc, out, err = H.run([sys.executable, str(H.ROOT / "scripts" / "bronze_recover.py"), "status"], timeout=max(timeout, 60), runner=runner)
    lines = [x for x in out.splitlines() if "지워진 구간" in x]
    if rc == 0:
        return Finding("Bronze 체크포인트", True, "모든 표의 offset 이 Kafka 에 남아 있다")
    if rc == 1 and lines:
        tables = sorted({x.split()[0] for x in lines})
        return Finding("Bronze 체크포인트", False, f"Kafka 에서 지워진 구간을 가리킴: {', '.join(tables)} (D-255)",
                       f"python scripts/recover.py bronze {' '.join(tables)}   (미리보기. 고칠 때 --apply)")
    return Finding("Bronze 체크포인트", True, f"확인 못 함 ({(err or out).strip().splitlines()[-1:] or ['?']})", info=True)


def check_topics(runner, timeout: float) -> Finding:
    rc, out, err = H.run([sys.executable, str(H.ROOT / "scripts" / "create_topics.py"), "--check"], timeout=max(timeout, 30), runner=runner)
    if rc == 0:
        return Finding("토픽 설정", True, "보존 기간 등 파일과 같음")
    if rc is None:
        return Finding("토픽 설정", True, f"확인 못 함 ({err})", info=True)
    return Finding("토픽 설정", False, "파일(config)과 다른 토픽 설정이 있다", "python scripts/create_topics.py --apply-config")


def check_db(q: Callable[..., list], lock_old_sec: float) -> list[Finding]:
    """DQ 차단, 오래 잡힌 작업 잠금, monitor 박동. DB 를 못 읽으면 한 줄로 알린다."""
    out: list[Finding] = []
    try:
        blocks = q("SELECT rule_id, coalesce(target, '') FROM ops.v_dq_latest WHERE action = 'block_mart' AND status = 'fail' AND NOT shadow "
                   "AND run_date >= (now() AT TIME ZONE 'Asia/Seoul')::date - 2 ORDER BY 1, 2")
        locks = q("SELECT a.pid, a.application_name, extract(epoch FROM now() - a.backend_start)::int FROM pg_locks l "
                  "JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND l.classid = %s "
                  "AND now() - a.backend_start > make_interval(secs => %s) ORDER BY 3 DESC", (3217, lock_old_sec))
        beat = q("SELECT extract(epoch FROM now() - heartbeat_at)::int FROM ops.monitor_state WHERE component = 'monitor'")
    except Exception as e:                                                   # noqa: BLE001
        return [Finding("DB", False, f"읽지 못함 ({type(e).__name__})", "scripts/stack.sh logs postgres   (.env 의 POSTGRES_PORT 확인)")]
    if blocks:
        out.append(Finding("DQ 차단", False, "마트를 막는 규칙: " + ", ".join(f"{r}[{t}]" if t else r for r, t in blocks),
                           "docs/runbooks/recovery.md 'DQ 차단' 절 (규칙을 잠깐 alert 로 → dq_report_daily 재실행 → 되돌림)"))
    else:
        out.append(Finding("DQ 차단", True, "없음"))
    for pid, app, age in locks:
        out.append(Finding("작업 잠금", False, f"{app} 가 {age // 60}분째 잠금을 잡고 있다 (pid {pid})",
                           f"멈춘 시도가 맞으면 Postgres 에서: SELECT pg_terminate_backend({pid});"))
    if not beat:
        out.append(Finding("monitor 박동", False, "기록 없음", "scripts/stack.sh up-monitor"))
    elif beat[0][0] > 600:
        out.append(Finding("monitor 박동", False, f"{beat[0][0] // 60}분째 없음", "docker start urbanstream-monitor"))
    else:
        out.append(Finding("monitor 박동", True, f"{beat[0][0]}초 전"))
    return out


def check_airflow(rows: list[dict], hw: dict) -> list[Finding]:
    out = []
    for r in rows:
        if r["service"] not in hw["airflow_services"] or r["state"] != "running":
            continue
        lim, mem, pids = r.get("mem_limit") or 0, r.get("mem_bytes") or 0, r.get("pids") or 0
        if (lim and mem >= hw["airflow_mem_ratio"] * lim) or pids >= hw["airflow_pids"]:
            out.append(Finding("Airflow", False, f"{r['service']} 메모리 {mem / 2**30:.2f}/{lim / 2**30:.2f} GiB, PIDS {pids}",
                               "docker compose stop airflow-scheduler   (D-280. 안 멈추면 docker kill urbanstream-airflow-scheduler)"))
    return out


def check_runner(stale_sec: float, now: Optional[float] = None) -> Finding:
    p = H.ROOT / "data" / "gold_stage" / "_requests" / ".runner_alive"
    if not p.exists():
        return Finding("gold-runner 박동", False, "박동 파일 없음", "scripts/stack.sh up-airflow")
    age = (time.time() if now is None else now) - p.stat().st_mtime
    if age > stale_sec:
        return Finding("gold-runner 박동", False, f"{int(age)}초째 없음", "docker restart urbanstream-gold-runner")
    return Finding("gold-runner 박동", True, f"{int(age)}초 전")


def check_host_tools() -> list[Finding]:
    out = []
    if H.read_pid(H.ROOT / "data" / "host_watch" / "host_watch.pid") is None:
        out.append(Finding("host_watch", False, "돌고 있지 않다 (Docker 가 멈춰도 알림이 오게 하는 감시자)", "python scripts/host_watch.py --start"))
    else:
        out.append(Finding("host_watch", True, "돌고 있음"))
    if H.read_pid(H.ROOT / "data" / "measure" / "samples" / "sampler.pid") is None:
        out.append(Finding("측정 표본", False, "measure_sample 이 돌고 있지 않다 (STEP 2 표본은 지나가면 다시 못 만든다)",
                           "python scripts/measure_sample.py --start"))
    return out


def diagnose(cfg: dict, *, runner=subprocess.run, query: Optional[Callable[..., list]] = None,
             expected: Optional[list[str]] = None) -> list[Finding]:
    t = float(cfg["diagnose"]["check_timeout_sec"])
    out = [check_docker(t, runner)]
    if not out[0].ok:
        out.append(Finding("나머지 점검", True, "Docker 가 응답하지 않아 건너뜀", info=True))
        out += check_host_tools()
        return out
    rows = H.containers(t, runner=runner, with_stats=True) or []
    out += check_containers(rows, expected or H.expected_services())
    f = check_init(rows, cfg["init_services"])
    if f:
        out.append(f)
    if any(r["service"] == "collector" and r["state"] == "running" for r in rows):
        out.append(check_clock(*H.vm_clock(t, runner=runner)))
    out.append(check_bronze(runner, t))
    out.append(check_topics(runner, t))
    out += check_db(query or (lambda sql, params=(): H.pg_query(sql, params)), float(cfg["diagnose"]["lock_old_sec"]))
    out += check_airflow(rows, cfg["host_watch"])
    out.append(check_runner(float(cfg["diagnose"]["runner_stale_sec"])))
    out += check_host_tools()
    return out


def print_findings(fs: list[Finding]) -> int:
    bad = [f for f in fs if not f.ok]
    for f in fs:
        mark = "확인" if f.info else ("정상" if f.ok else "문제")
        print(f"{mark}  {f.name:<14} {f.detail}")
        if f.next_cmd and not f.ok:
            print(f"      → 다음: {f.next_cmd}")
    print()
    print("모두 정상" if not bad else f"문제 {len(bad)}건. 위 '→ 다음' 줄을 위에서부터 하나씩 한다")
    return 0 if not bad else 1


# ------------------------------------------------------------------ clock

def clock(cfg: dict, runner=subprocess.run) -> int:
    t = float(cfg["diagnose"]["check_timeout_sec"])
    ok, why = H.docker_version(t, runner=runner)
    if not ok:
        print(f"Docker 가 응답하지 않는다 ({why}). 먼저: {DOCKER_RESTART}")
        return 1
    skew, detail = H.vm_clock(t, runner=runner)
    sched, sdetail = H.vm_clock(t, runner=runner, container="urbanstream-airflow-scheduler")
    print(f"Mac 시계         {H.now_kst():%Y-%m-%d %H:%M:%S} KST")
    for name, v, d in (("Docker(수집기)", skew, detail), ("Docker(scheduler)", sched, sdetail)):
        print(f"{name:<16} " + ("잴 수 없음 (" + d + ")" if v is None else f"Mac 보다 {v:+.0f}초 ({d})"))
    print("컨테이너는 모두 같은 Docker VM 시계를 쓴다. 두 값이 다르면 측정 오차(왕복 시간)다.")
    if skew is None:
        return 1
    if abs(skew) < 60:
        print("정상 (60초 안)")
        return 0
    print(f"\nDocker 시계가 {abs(skew):.0f}초 {'늦다' if skew < 0 else '빠르다'}. 수집 시각(fetched_at)이 이 시계를 따른다.")
    print("1) 지금 확인된 방법은 Docker Desktop 재시작이다 (수집 공백이 생긴다):")
    print("   docker stop $(docker ps -q --filter name=urbanstream-)")
    print(f"   {DOCKER_RESTART}")
    print("   python scripts/recover.py resume")
    print("2) 버스 표가 '미래 시각'으로 마트를 막았는지: python scripts/recover.py diagnose 의 'DQ 차단' 줄")
    print("   (VM 시계만 바로 맞추는 명령은 Docker Desktop 에서 되는지 아직 확인하지 않았다. 자연 발생 때 시험해 보고 기록한다)")
    return 1


# ------------------------------------------------------------------ resume

def wait_healthy(services: list[str], wait_sec: float, *, runner=subprocess.run, sleep=time.sleep,
                 clock_fn=time.monotonic, poll_sec: float = 10) -> tuple[bool, list[str]]:
    t0 = clock_fn()
    bad = list(services)
    while True:
        rows = H.containers(10, runner=runner) or []
        svc = H.by_service(rows)
        bad = [s for s in services if not H.healthy(svc.get(s))]
        if not bad:
            return True, []
        if clock_fn() - t0 >= wait_sec:
            return False, bad
        print(f"  기다리는 중: {', '.join(bad)}", flush=True)
        sleep(poll_sec)


def measure_lock_held() -> Optional[str]:
    p = H.measure_lock_path()
    if p.exists():
        try:
            return p.read_text(encoding="utf-8").strip()
        except OSError:
            return "?"
    return None


def compose(profiles: str, args: list[str], *, runner=subprocess.run, timeout: float = 300) -> tuple[Optional[int], str, str]:
    import os
    return H.run(["docker", "compose", *args], timeout=timeout, runner=runner, env={**os.environ, "COMPOSE_PROFILES": profiles})


def resume(cfg: dict, *, runner=subprocess.run, sleep=time.sleep, clock_fn=time.monotonic, expected: Optional[list[str]] = None,
           in_drill: bool = False) -> int:
    held = measure_lock_held()
    if held and not (in_drill and "drill:" in held):          # 훈련(docker-restart)이 잡은 잠금 안에서 부를 때만 통과
        print(f"측정·훈련 잠금이 있어 시작하지 않는다: {held}\n측정·훈련이 끝났는지 보고, 비정상 종료였으면 잠금 파일({H.measure_lock_path()})을 지운다.")
        return 2
    ok, why = H.docker_version(10, runner=runner)
    if not ok:
        print(f"Docker 가 응답하지 않는다 ({why}). 먼저 Docker Desktop 을 띄운다: {DOCKER_RESTART}")
        return 1
    rc_cfg = cfg["resume"]
    exp = expected or H.expected_services()
    first = [s for s in exp if s != "collector"]
    print(f"1/4 수집기를 뺀 나머지 시작 (COMPOSE_PROFILES={rc_cfg['first_profiles']})", flush=True)
    rc, out, err = compose(rc_cfg["first_profiles"], ["start"], runner=runner)
    if rc != 0:
        msg = (err or out).strip()
        print(f"   실패: {msg[-400:]}")
        if "no container" in msg.lower() or "no such" in msg.lower():
            print("   컨테이너가 지워진 것 같다. mac-on-off.md 2-B (scripts/stack.sh up-*) 로 다시 만든다.")
        return 1
    print(f"2/4 모두 healthy 가 될 때까지 기다림 (최대 {rc_cfg['healthy_wait_sec']}초)", flush=True)
    ok, bad = wait_healthy(first, float(rc_cfg["healthy_wait_sec"]), runner=runner, sleep=sleep, clock_fn=clock_fn)
    if not ok:
        print(f"   아직 정상이 아니다: {', '.join(bad)}. 수집기는 켜지 않았다.\n   다음: python scripts/recover.py diagnose")
        return 1
    print(f"3/4 수집기 시작 (COMPOSE_PROFILES={rc_cfg['collector_profiles']})", flush=True)
    rc, out, err = compose(rc_cfg["collector_profiles"], ["start", "collector"], runner=runner)
    if rc != 0:
        print(f"   실패: {(err or out).strip()[-400:]}\n   다음: scripts/stack.sh up-collector")
        return 1
    print("4/4 scripts/stack.sh check", flush=True)
    rc, out, err = H.run(["bash", str(H.ROOT / "scripts" / "stack.sh"), "check"], timeout=180, runner=runner)
    print(out.rstrip())
    if rc != 0:
        print(err.rstrip()[-400:])
        return 1
    print("\n다시 시작했다. 호스트 감시가 꺼져 있으면: python scripts/host_watch.py --start")
    return 0


# ------------------------------------------------------------------ bronze

def bronze(tables: list[str], apply: bool, *, yes: bool = False, runner=subprocess.run, sleep=time.sleep,
           clock_fn=time.monotonic, ask: Callable[[str], str] = input, verify_sec: float = 180) -> int:
    script = [sys.executable, str(H.ROOT / "scripts" / "bronze_recover.py")]
    names = tables or ["all"]
    rc, out, err = H.run([*script, "reset", *names], timeout=60, runner=runner)
    print(out.rstrip() or err.rstrip())
    if rc not in (0,):
        return 1
    need = sorted({x.split()[0] for x in out.splitlines() if "지워진 구간" in x})
    if not need:
        print("\n고칠 것이 없다.")
        return 0
    if not apply:
        print(f"\n미리보기였다. 고치려면 (spark-bronze 를 멈췄다 다시 켠다, 수집은 계속된다):\n  python scripts/recover.py bronze {' '.join(need)} --apply")
        return 0
    held = measure_lock_held()
    if held:
        print(f"측정·훈련 잠금이 있어 하지 않는다: {held}")
        return 2
    if not yes and ask("\n위 '잃은 메시지' 는 Kafka 에서 이미 지워져 되찾을 수 없다. 그 사실을 확정하고 고친다. 계속하려면 yes: ").strip() != "yes":
        print("취소했다.")
        return 0
    print("1/5 spark-bronze 멈춤", flush=True)
    rc, _o, err = H.run(["docker", "stop", "urbanstream-spark-bronze"], timeout=120, runner=runner)
    if rc != 0 and "zombie" in err:
        print(zombie_hint("urbanstream-spark-bronze"))
        return 1
    rows = H.containers(10, runner=runner) or []
    st = (H.by_service(rows).get("spark-bronze") or {}).get("state")
    if st == "running":
        print("   멈추지 않았다. 체크포인트를 고치지 않는다. 다음: docker kill urbanstream-spark-bronze 뒤 다시 실행")
        return 1
    print("2/5 체크포인트 고침 (바꾸기 전 파일은 data/_recovery/<시각>/ 에 남는다)", flush=True)
    before = time.time()
    rc, out, err = H.run([*script, "reset", *need, "--apply"], timeout=120, runner=runner)
    print(out.rstrip())
    backups = [x.split("바꾸기 전 파일:")[-1].strip() for x in out.splitlines() if "바꾸기 전 파일:" in x]
    if rc != 0:
        print(f"   실패: {err.strip()[-400:]}\n   spark-bronze 는 멈춘 상태다. 원인을 본 뒤: docker start urbanstream-spark-bronze")
        return 1
    print("3/5 spark-bronze 시작", flush=True)
    H.run(["docker", "start", "urbanstream-spark-bronze"], timeout=120, runner=runner)
    print(f"4/5 {int(verify_sec)}초 안에 healthy 와 새 Bronze 파일 확인", flush=True)
    ok, _ = wait_healthy(["spark-bronze"], verify_sec, runner=runner, sleep=sleep, clock_fn=clock_fn, poll_sec=10)
    fresh = [t for t in need if newest_file(H.ROOT / "data" / "bronze" / t) > before]
    t0 = clock_fn()
    while ok and len(fresh) < len(need) and clock_fn() - t0 < verify_sec:
        sleep(10)
        fresh = [t for t in need if newest_file(H.ROOT / "data" / "bronze" / t) > before]
    if ok and len(fresh) == len(need):
        print("5/5 확인: spark-bronze healthy, 새 파일이 생겼다. 잃은 건수는 위 출력 그대로 기록에 남긴다(docs/recovery-drills.md '자연').")
        return 0
    print("5/5 확인 실패: " + ("healthy 가 아니다" if not ok else f"새 파일이 없는 표 {sorted(set(need) - set(fresh))}"))
    print("   자동으로 되돌리지 않는다(되돌리면 다시 OffsetOutOfRange). 상태 보기: scripts/stack.sh logs spark-bronze")
    for b in backups:
        print(f"   정말 되돌려야 하면 (spark-bronze 를 멈춘 뒤): cp -p {b}/offsets/* data/checkpoints/bronze/{Path(b).name}/offsets/")
    return 1


def zombie_hint(name: str) -> str:
    """'is zombie and can not be killed' (D-289) 를 만났을 때. mac-on-off.md 1-A 를 그대로 보여 준다."""
    return (f"   docker stop 이 'is zombie' 로 실패했다 (D-289, mac-on-off.md 1-A). 체크포인트는 고치지 않았다.\n"
            f"   1) docker kill {name}\n"
            "   2) 같은 오류면 Docker Desktop 을 끈다: osascript -e 'quit app \"Docker\"' → open -a Docker → python scripts/recover.py resume\n"
            "   3) 그 뒤 이 명령을 다시 실행한다")


def newest_file(d: Path) -> float:
    """d 아래 Parquet 파일 중 가장 최근 수정 시각 (_spark_metadata 제외). 없으면 0."""
    best = 0.0
    if not d.is_dir():
        return best
    for p in d.glob("event_date=*/*.parquet"):
        try:
            best = max(best, p.stat().st_mtime)
        except OSError:
            continue
    return best


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(H.CONFIG))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("diagnose", help="읽기만 하는 점검")
    sub.add_parser("clock", help="시계 비교")
    rs = sub.add_parser("resume", help="멈춘 스택을 순서대로 시작")
    rs.add_argument("--in-drill", action="store_true", help=argparse.SUPPRESS)
    b = sub.add_parser("bronze", help="Bronze 체크포인트 복구 (D-255)")
    b.add_argument("tables", nargs="*")
    b.add_argument("--apply", action="store_true")
    b.add_argument("--yes", action="store_true", help="확인 질문 없이 (훈련 자동화용)")
    a = ap.parse_args(argv)
    cfg = H.load_config(a.config)
    if shutil.which("docker") is None:
        print("docker 명령이 없다. Docker Desktop 이 설치돼 있는지 본다.")
        return 1
    if a.cmd == "diagnose":
        return print_findings(diagnose(cfg))
    if a.cmd == "clock":
        return clock(cfg)
    if a.cmd == "resume":
        return resume(cfg, in_drill=a.in_drill)
    return bronze(a.tables, a.apply, yes=a.yes)


if __name__ == "__main__":
    sys.exit(main())
