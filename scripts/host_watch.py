"""호스트 감시 (STEP 3B, 설계안 6-2). Mac 에서 1분마다 돈다. Docker Desktop 이나 monitor 컨테이너가 멈춰도 알림이 오게 한다.

왜 필요한가 (D-126)
  알림은 monitor 컨테이너가 보낸다. Docker Desktop 이 멈추면 monitor 도 같이 멈추므로 가장 큰 장애에서 알림이 0건이었다(11-24~25).
  이 감시자는 Docker 밖(호스트 Python)에서 돌고, 웹훅으로 직접 보낸다. Mac 이 잠들면 이것도 같이 멈춘다(그건 Mac 한 대의 한계, R14).

1분마다 보는 것 (값은 config/recovery.yaml host_watch)
  docker version 이 3번 연속 응답 없음                  → critical "Docker 가 응답하지 않습니다"
  늘 켜져 있어야 할 12개 중 꺼짐·unhealthy 가 5분 넘게   → error (컨테이너 이름)
  ops.monitor_state 의 monitor 박동이 10분 넘게 오래됨     → error "감시자가 멈췄습니다"
  Docker VM 시계와 Mac 시계 차이 60초 / 10분 넘음 (5분마다) → warning / error (R8, D-44)
  airflow-scheduler·dag-processor 메모리 90% 또는 PIDS 300 이 5분 넘게 → warning (R9, D-280)
  위 문제가 사라지면                                        → info "해소" (문제마다 한 번)
  같은 문제는 config/alerts.yaml 의 cooldown(error 2시간) 안에 다시 보내지 않는다. 깨어난 직후 10분은 컨테이너 알림을 미룬다
  (켠 직후 잠깐 unhealthy 인 것은 정상, mac-on-off 3절). 시계 차이는 깨어난 직후가 가장 중요하므로 미루지 않는다.

상태와 기록: data/host_watch/state.json (마지막 확인 시각, 진행 중 문제), alerts.jsonl (보낸 알림), host_watch.log, host_watch.pid.
비밀 값(웹훅 URL, 비밀번호)은 출력하지 않는다. 웹훅이 없으면 기록만 남긴다.

사용 (프로젝트 폴더, conda urbanstream)
  python scripts/host_watch.py --start      백그라운드로 시작 (caffeinate -i 아래, 터미널을 닫아도 계속)
  python scripts/host_watch.py --status     돌고 있는지, 마지막 확인 시각, 진행 중 문제
  python scripts/host_watch.py --stop       멈춘다
  python scripts/host_watch.py --once       한 번만 확인하고 끝 (알림도 실제로 보낸다)
  python scripts/host_watch.py --print      한 번 확인하고 판정만 보여 준다 (보내지 않고 상태도 바꾸지 않는다)
Mac 을 재시작하거나 로그아웃하면 저절로 켜지지 않는다(launchd 를 쓰지 않는다, D-290). mac-on-off.md 2-D 에서 다시 켠다.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hostlib as H  # noqa: E402

DIR = H.ROOT / "data" / "host_watch"
STATE = "state.json"
ALERTS = "alerts.jsonl"
PID = "host_watch.pid"
LOG = "host_watch.log"
DIAG = "python scripts/recover.py diagnose"


# ------------------------------------------------------------------ 관찰 (Docker·DB 를 실제로 읽는다)

def observe(cfg: dict, st: dict, *, runner=None, now: Optional[float] = None, mono: Optional[float] = None,
            monitor_age: Optional[Callable[[], Optional[float]]] = None) -> dict:
    hw = cfg["host_watch"]
    kw = {"runner": runner} if runner else {}
    obs: dict[str, Any] = {"ts": time.time() if now is None else now, "mono": time.monotonic() if mono is None else mono}
    ok, why = H.docker_version(hw["docker_timeout_sec"], **kw)
    obs["docker_ok"], obs["docker_detail"] = ok, why
    if ok:
        obs["containers"] = H.containers(hw["docker_timeout_sec"], with_stats=True, **kw)
        if obs["ts"] - float(st.get("last_clock") or 0) >= hw["clock_every_sec"]:
            skew, detail = H.vm_clock(hw["docker_timeout_sec"], **kw)
            obs["clock_skew"], obs["clock_detail"] = skew, detail
        obs["monitor_age"] = (monitor_age or H.monitor_heartbeat_age)()
    return obs


# ------------------------------------------------------------------ 판정 (순수: 관찰 + 이전 상태 → 문제 목록 + 새 상태)

def _get(d: Optional[dict], k: str, default: float) -> float:
    v = (d or {}).get(k)
    return default if v is None else v


def issues_of(obs: dict, st: dict, cfg: dict, expected: list[str]) -> tuple[dict[str, tuple[str, str, str]], set[str], dict]:
    """반환 = (문제 {키: (수준, 제목, 내용)}, 이번에 판정하지 못한 키 접두사(이전 상태 유지), 새 상태 일부)."""
    hw = cfg["host_watch"]
    t = float(obs["ts"])
    out: dict[str, tuple[str, str, str]] = {}
    unknown: set[str] = set()
    new: dict[str, Any] = {}

    fails = 0 if obs.get("docker_ok") else int(st.get("docker_fail") or 0) + 1
    new["docker_fail"] = fails
    if fails >= hw["docker_fail_limit"]:
        out["host:docker"] = ("critical", "Docker 가 응답하지 않습니다",
                              f"docker version 이 {fails}번 연속 실패했습니다 ({obs.get('docker_detail', '')}). 수집·처리·감시가 모두 멈춘 상태입니다.\n"
                              "Mac 에서: pkill -x Docker; pkill -f com.docker.backend → open -a Docker → python scripts/recover.py resume")
    if not obs.get("docker_ok"):
        unknown |= {"host:container:", "host:monitor", "host:airflow:", "host:clock"}
        new.update(bad_since=st.get("bad_since") or {}, hot_since=st.get("hot_since") or {})
        return out, unknown, new

    rows = obs.get("containers")
    if rows is None:
        unknown |= {"host:container:", "host:airflow:"}
        new.update(bad_since=st.get("bad_since") or {}, hot_since=st.get("hot_since") or {})
    else:
        svc = H.by_service(rows)
        bad_since: dict[str, float] = {}
        for s in expected:
            r = svc.get(s)
            if H.healthy(r):
                continue
            since = float(_get(st.get("bad_since"), s, t))
            bad_since[s] = since
            if t - since >= hw["container_bad_sec"]:
                what = "없음(지워졌거나 만든 적 없음)" if r is None else f"{r.get('state')}/{r.get('health') or '-'}"
                extra = " OOM 으로 죽었습니다. 다시 만들기 전에 diagnose 가 보여 주는 inspect 한 줄로 기록부터 남깁니다." if r and r.get("oom") else ""
                out[f"host:container:{s}"] = ("error", f"컨테이너 {s} 가 {int((t - since) // 60)}분째 정상이 아닙니다",
                                              f"상태 {what}.{extra}\n다음: {DIAG}")
        new["bad_since"] = bad_since
        hot_since: dict[str, float] = {}
        for s in hw["airflow_services"]:
            r = svc.get(s)
            if not r or r.get("state") != "running":
                continue
            lim, mem, pids = int(r.get("mem_limit") or 0), int(r.get("mem_bytes") or 0), int(r.get("pids") or 0)
            hot = (lim > 0 and mem >= hw["airflow_mem_ratio"] * lim) or pids >= hw["airflow_pids"]
            if not hot:
                continue
            since = float(_get(st.get("hot_since"), s, t))
            hot_since[s] = since
            if t - since >= hw["airflow_sustain_sec"]:
                out[f"host:airflow:{s}"] = ("warning", f"{s} 가 폭주하는 것 같습니다",
                                            f"메모리 {mem / 2**30:.2f}/{lim / 2**30:.2f} GiB, PIDS {pids} 가 {int((t - since) // 60)}분째. "
                                            "한도에 닿으면: docker compose stop airflow-scheduler (D-280, mac-on-off 5절)")
        new["hot_since"] = hot_since

    age = obs.get("monitor_age")
    if age is None:
        unknown.add("host:monitor")
    elif age >= hw["monitor_stale_sec"]:
        out["host:monitor"] = ("error", "감시자(monitor)가 멈췄습니다",
                               f"monitor 박동이 {int(age // 60)}분째 없습니다. 이 사이 DAG 실패·수집 정체 알림이 오지 않습니다.\n"
                               f"다음: docker start urbanstream-monitor (또는 {DIAG})")

    if "clock_skew" in obs:
        new["last_clock"] = t
        new["skew"] = obs["clock_skew"]
    skew = new.get("skew", st.get("skew"))
    if skew is None:
        unknown.add("host:clock")
    else:
        a = abs(float(skew))
        lvl = "error" if a >= hw["clock_error_sec"] else "warning" if a >= hw["clock_warn_sec"] else None
        if lvl:
            side = "늦습니다" if skew < 0 else "빠릅니다"
            out["host:clock"] = (lvl, f"Docker 시계가 Mac 보다 {int(a)}초 {side}",
                                 "수집 시각(fetched_at)이 Docker 시계를 따릅니다. 늦으면 버스 표가 '미래 시각'으로 걸려 마트가 막힐 수 있습니다(11-24).\n"
                                 "다음: python scripts/recover.py clock")
    return out, unknown, new


def decide(obs: dict, st: dict, cfg: dict, expected: list[str], cooldown: Callable[[str], float]) -> tuple[list[dict], dict]:
    """보낼 알림 목록과 새 상태. 알림 = {key, level, title, body, resolved}."""
    hw = cfg["host_watch"]
    t, m = float(obs["ts"]), float(obs["mono"])
    st = dict(st)
    grace_until = float(st.get("grace_until") or 0)
    if st.get("last_wall") is not None and st.get("last_mono") is not None:
        slept = (t - float(st["last_wall"])) - (m - float(st["last_mono"]))
        if slept >= hw["wake_gap_sec"]:
            grace_until = t + hw["wake_grace_sec"]
    found, unknown, new = issues_of(obs, st, cfg, expected)
    in_grace = t < grace_until
    held = set(unknown) | ({"host:container:"} if in_grace else set())

    def is_held(k: str) -> bool:
        return any(k.startswith(p) for p in held)
    active: dict[str, dict] = dict(st.get("active") or {})
    send: list[dict] = []
    for k, (lvl, title, body) in found.items():
        if is_held(k):
            continue
        a = active.get(k)
        if a is None or a.get("level") != lvl or t - float(a.get("last_sent") or 0) >= cooldown(lvl):
            send.append({"key": k, "level": lvl, "title": title, "body": body, "resolved": False})
            active[k] = {"level": lvl, "title": title, "since": (a or {}).get("since", t), "last_sent": t}
    for k in list(active):
        if k in found or is_held(k):
            continue
        a = active.pop(k)
        send.append({"key": k, "level": "info", "title": f"해소: {a.get('title')}",
                     "body": f"{H.fmt(float(a.get('since') or t))} 부터 {int((t - float(a.get('since') or t)) // 60)}분", "resolved": True})
    st.update(new, active=active, last_wall=t, last_mono=m, grace_until=grace_until, last_tick=t)
    return send, st


# ------------------------------------------------------------------ 상태 파일·전송

def read_state(d: Path) -> dict:
    try:
        return json.loads((d / STATE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_state(d: Path, st: dict) -> None:
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / f".{STATE}.tmp"
    tmp.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, d / STATE)


def deliver(alerts: list[dict], st: dict, d: Path, send: Callable[..., tuple[bool, str]]) -> dict:
    failed = int(st.get("failed") or 0)
    for a in alerts:
        body = a["body"] + (f"\n(이전 전달 실패 {failed}건)" if failed else "")
        ok, why = send(a["level"], f"[host_watch] {a['title']}", body)
        failed = 0 if ok else failed + 1
        with open(d / ALERTS, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": round(time.time(), 1), "kst": H.now_kst().isoformat(timespec="seconds"), "key": a["key"],
                                "level": a["level"], "title": a["title"], "sent": ok, "why": why}, ensure_ascii=False) + "\n")
    st["failed"] = failed
    return st


def tick(cfg: dict, d: Path = DIR, *, runner=None, send=None, expected: Optional[list[str]] = None, dry: bool = False,
         now: Optional[float] = None, mono: Optional[float] = None, monitor_age=None) -> tuple[list[dict], dict]:
    from urbanstream.notify.policy import load_policy
    st = read_state(d)
    obs = observe(cfg, st, runner=runner, now=now, mono=mono, monitor_age=monitor_age)
    alerts, st = decide(obs, st, cfg, expected or H.expected_services(), load_policy().cooldown)
    st["last_obs"] = {k: v for k, v in obs.items() if k != "containers"}
    if not dry:
        d.mkdir(parents=True, exist_ok=True)
        st = deliver(alerts, st, d, send or H.send_webhook)
        write_state(d, st)
    return alerts, st


def loop(cfg: dict, d: Path = DIR, max_iter: Optional[int] = None, **kw) -> int:
    pid_p = H.take_pid(d / PID)
    stop = {"flag": False}

    def _term(signum, frame):  # noqa: ARG001
        stop["flag"] = True
        raise KeyboardInterrupt
    old = signal.signal(signal.SIGTERM, _term)
    interval = float(cfg["host_watch"]["interval_sec"])
    n = 0
    print(f"[{H.now_kst():%m-%d %H:%M:%S}] 호스트 감시 시작: {interval:.0f}초 간격, PID {os.getpid()}", flush=True)
    try:
        while max_iter is None or n < max_iter:
            try:
                alerts, st = tick(cfg, d, **kw)
                for a in alerts:
                    print(f"[{H.now_kst():%m-%d %H:%M:%S}] {a['level']}: {a['title']}", flush=True)
            except Exception as e:                                          # noqa: BLE001 (한 회차 실패가 감시를 끝내면 안 된다)
                print(f"[{H.now_kst():%m-%d %H:%M:%S}] 확인 실패: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
            n += 1
            if max_iter is not None and n >= max_iter:
                break
            now = time.time()
            time.sleep(max(0.0, (now // interval + 1) * interval - now))
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, old)
        if H.read_pid(pid_p) == os.getpid() or not pid_p.exists():
            pid_p.unlink(missing_ok=True)
        print(f"[{H.now_kst():%m-%d %H:%M:%S}] 호스트 감시 끝 ({n}회)", flush=True)
    return 0


def status(cfg: dict, d: Path = DIR, now: Optional[float] = None) -> int:
    """0 = 돌고 있고 마지막 확인이 간격의 3배 안, 1 = 안 돈다, 2 = 돌지만 늦다."""
    pid = H.read_pid(d / PID)
    st = read_state(d)
    now = time.time() if now is None else now
    last = st.get("last_tick")
    when = "없음" if not last else f"{H.fmt(float(last))} ({(now - float(last)) / 60:.0f}분 전)"
    print(f"호스트 감시: {'돌고 있음 (PID ' + str(pid) + ')' if pid else '멈춤'} · 마지막 확인: {when}")
    for k, a in sorted((st.get("active") or {}).items()):
        print(f"  진행 중  {a.get('level'):<8} {a.get('title')}  ({H.fmt(float(a.get('since') or now))} 부터)")
    if pid is None:
        print("시작: python scripts/host_watch.py --start")
        return 1
    return 0 if last and now - float(last) <= 3 * float(cfg["host_watch"]["interval_sec"]) else 2


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(H.CONFIG))
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--start", action="store_true")
    g.add_argument("--stop", action="store_true")
    g.add_argument("--status", action="store_true")
    g.add_argument("--once", action="store_true")
    g.add_argument("--print", action="store_true")
    g.add_argument("--loop", action="store_true", help="앞에서 반복 (Ctrl+C 로 끝. --start 가 이것을 띄운다)")
    ap.add_argument("--no-caffeinate", action="store_true")
    a = ap.parse_args(argv)
    cfg = H.load_config(a.config)
    if a.start:
        pid = H.read_pid(DIR / PID)
        if pid is not None:
            print(f"이미 돌고 있다 (PID {pid}). 멈추려면: python scripts/host_watch.py --stop")
            return 1
        pid = H.start_background([sys.executable, str(Path(__file__).resolve()), "--config", a.config, "--loop"], DIR / PID, DIR / LOG,
                                 caffeinate=not a.no_caffeinate)
        if pid is None:
            print(f"시작하지 못했다. {DIR / LOG} 끝부분을 본다")
            return 1
        enabled, _kind, url = H.webhook_config()
        print(f"시작했다 (PID {pid}). 기록: {DIR / LOG}" + ("" if enabled and url else "\n주의: 웹훅이 없어 알림은 기록만 남는다(.env 의 ALERT_WEBHOOK_URL)"))
        return 0
    if a.stop:
        ok, pid = H.stop_background(DIR / PID)
        print("돌고 있지 않다" if pid is None else (f"멈췄다 (PID {pid})" if ok else f"끝나지 않았다 (PID {pid}). 확인: ps -p {pid}"))
        return 0 if ok else 1
    if a.status:
        return status(cfg)
    if a.loop:
        return loop(cfg)
    alerts, st = tick(cfg, dry=a.print)
    o = st.get("last_obs") or {}
    age = o.get("monitor_age")
    skew = o.get("clock_skew")
    clock = "이번에 안 잼" if "clock_skew" not in o else ("모름" if skew is None else f"{skew:+.0f}초")
    beat = "모름" if age is None else f"{int(age)}초 전"
    print(f"Docker: {'응답' if o.get('docker_ok') else '응답 없음'} ({o.get('docker_detail', '')}) · monitor 박동: {beat} · 시계 차이: {clock}")
    for x in alerts:
        print(f"{'(보내지 않음) ' if a.print else ''}{x['level']}: {x['title']}")
    if not alerts:
        print("새로 보낼 알림 없음" + (f" (진행 중 {len(st.get('active') or {})}건)" if st.get("active") else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
