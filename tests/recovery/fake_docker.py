"""시험용 가짜 `docker`. tests/recovery 의 시험이 PATH 앞에 이 파일을 'docker' 로 둔다. 실제 Docker 를 부르지 않는다.

상태는 FAKE_DOCKER_STATE(JSON 파일)에 있다:
  {"mode": "ok" | "hang" | "down",
   "containers": [{"name", "service", "state", "health", "restarts", "oom", "init", "mem_limit", "mem_bytes", "pids", "exit_code"}],
   "vm_skew": 초(컨테이너 date 가 호스트보다 이만큼 다름), "fail": {"stop urbanstream-x": "stderr 문장"},
   "on_start_healthy": true  (start 하면 running/healthy 로 바뀜)}
부른 명령은 FAKE_DOCKER_LOG 에 한 줄씩(JSON 배열) 남는다.
"""
from __future__ import annotations

import json
import os
import sys
import time


def main() -> int:
    path = os.environ["FAKE_DOCKER_STATE"]
    st = json.load(open(path, encoding="utf-8"))
    args = sys.argv[1:]
    with open(os.environ["FAKE_DOCKER_LOG"], "a", encoding="utf-8") as f:
        f.write(json.dumps({"args": args, "profiles": os.environ.get("COMPOSE_PROFILES")}, ensure_ascii=False) + "\n")
    mode = st.get("mode", "ok")
    if mode == "hang":
        time.sleep(60)
        return 0
    if mode == "down":
        print("Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?", file=sys.stderr)
        return 1
    cs = st.get("containers", [])
    by = {c["name"]: c for c in cs}
    key = " ".join(args[:2])
    if key in (st.get("fail") or {}):
        print(st["fail"][key], file=sys.stderr)
        return 1
    if args[:1] == ["version"]:
        print("29.0.1")
        return 0
    if args[:1] == ["ps"]:
        print("\n".join(c["name"] for c in cs))
        return 0
    if args[:1] == ["inspect"]:
        out = []
        for n in args[1:]:
            c = by.get(n)
            if not c:
                continue
            out.append({"Name": "/" + c["name"], "RestartCount": c.get("restarts", 0),
                        "Config": {"Labels": {"com.docker.compose.service": c["service"]}},
                        "State": {"Status": c.get("state", "running"), "OOMKilled": c.get("oom", False), "ExitCode": c.get("exit_code", 0),
                                  "FinishedAt": "2026-11-26T01:00:00Z", **({"Health": {"Status": c["health"]}} if c.get("health") else {})},
                        "HostConfig": {"Init": c.get("init", True), "Memory": c.get("mem_limit", 0)}})
        print(json.dumps(out))
        return 0
    if args[:1] == ["stats"]:
        for c in cs:
            if c.get("state") == "running":
                mem = c.get("mem_bytes", 100 * 2**20)
                print(json.dumps({"Name": c["name"], "CPUPerc": "1.0%", "MemUsage": f"{mem}B / 4GiB", "PIDs": str(c.get("pids", 10))}))
        return 0
    if args[:1] == ["exec"] and args[2:] == ["date", "+%s"]:
        print(int(time.time() + float(st.get("vm_skew", 0))))
        return 0
    if args[:1] in (["stop"], ["kill"], ["start"]):
        for n in args[1:]:
            c = by.get(n)
            if c is None:
                print(f"Error: No such container: {n}", file=sys.stderr)
                return 1
            if args[0] == "start":
                c["state"] = "running"
                c["health"] = "healthy" if st.get("on_start_healthy", True) else "starting"
            else:
                c["state"] = "exited"
        json.dump(st, open(path, "w", encoding="utf-8"))
        print("\n".join(args[1:]))
        return 0
    if args[:2] == ["compose", "start"]:
        names = args[2:]
        for c in cs:
            if (not names and c["service"] != "collector") or c["service"] in names:
                c["state"] = "running"
                c["health"] = "healthy" if st.get("on_start_healthy", True) else "starting"
        json.dump(st, open(path, "w", encoding="utf-8"))
        return 0
    if args[:1] == ["compose"]:
        return 0
    print(f"fake docker: 모르는 명령 {args}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
