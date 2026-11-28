"""tests/recovery 공통: 가짜 docker 를 PATH 앞에 두고 상태 파일로 움직인다 (conftest 이름 충돌을 피해 일반 모듈로 둔다)."""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

TWELVE = ["postgres", "kafka", "spark-bronze", "spark-silver", "gold-runner", "airflow-scheduler", "airflow-apiserver",
          "airflow-dag-processor", "monitor", "collector", "api", "dashboard"]


def container(service: str, **kw) -> dict:
    c = {"name": f"urbanstream-{service}", "service": service, "state": "running", "health": "healthy", "restarts": 0,
         "oom": False, "init": True, "mem_limit": 2 * 2**30, "mem_bytes": 300 * 2**20, "pids": 20}
    c.update(kw)
    return c


class FakeDocker:
    def __init__(self, tmp: Path, monkeypatch, containers=None, **state):
        self.dir = tmp / "fakebin"
        self.dir.mkdir(exist_ok=True)
        exe = self.dir / "docker"
        exe.write_text(f"#!{sys.executable}\nimport runpy, sys\nsys.argv[0] = 'docker'\n"
                       f"runpy.run_path({str(Path(__file__).with_name('fake_docker.py'))!r}, run_name='__main__')\n", encoding="utf-8")
        exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
        self.state_p, self.log_p = tmp / "docker_state.json", tmp / "docker_log.jsonl"
        self.log_p.write_text("", encoding="utf-8")
        self.write({"mode": "ok", "containers": containers if containers is not None else [container(s) for s in TWELVE], **state})
        monkeypatch.setenv("PATH", f"{self.dir}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("FAKE_DOCKER_STATE", str(self.state_p))
        monkeypatch.setenv("FAKE_DOCKER_LOG", str(self.log_p))

    def write(self, st: dict) -> None:
        self.state_p.write_text(json.dumps(st), encoding="utf-8")

    def state(self) -> dict:
        return json.loads(self.state_p.read_text(encoding="utf-8"))

    def update(self, **kw) -> None:
        st = self.state()
        st.update(kw)
        self.write(st)

    def calls(self) -> list[dict]:
        return [json.loads(x) for x in self.log_p.read_text(encoding="utf-8").splitlines() if x.strip()]
