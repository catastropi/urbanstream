"""stack_check.main() 전체 흐름을 가짜 `docker` 명령으로 검사한다 (실제 컨테이너 없이 출력·종료코드·정리 동작 확인)."""
import importlib.util
import os
import stat
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("stack_check_flow", ROOT / "scripts" / "stack_check.py")
sc = importlib.util.module_from_spec(spec)
sys.modules["stack_check_flow"] = sc
spec.loader.exec_module(sc)

FAKE_DOCKER = r"""#!/usr/bin/env bash
# 가짜 docker: 호출 내용을 로그에 남기고, 시나리오(SCENARIO)에 따라 응답한다.
echo "$*" >> "$FAKE_DIR/calls.log"
args="$*"
if [ "$SCENARIO" = "daemon_down" ]; then
  echo "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?" >&2
  exit 1
fi
if [[ "$args" == *"ps --all --format json"* ]]; then
  echo '{"Service":"postgres","State":"running","Health":"healthy"}'
  if [ "$SCENARIO" = "kafka_down" ]; then echo '{"Service":"kafka","State":"exited","Health":""}'
  else echo '{"Service":"kafka","State":"running","Health":"healthy"}'; fi
elif [[ "$args" == *"exec -T postgres"* ]]; then
  cat > /dev/null; echo "postgis=3.6.1 | postgres=17.6 | timezone=Etc/UTC"
elif [[ "$args" == *"kafka-broker-api-versions.sh"* ]]; then exit 0
elif [[ "$args" == *"kafka-topics.sh"*"--create"* ]]; then exit 0
elif [[ "$args" == *"kafka-topics.sh"*"--delete"* ]]; then touch "$FAKE_DIR/deleted"; exit 0
elif [[ "$args" == *"kafka-console-producer.sh"* ]]; then cat > "$FAKE_DIR/msg"; exit 0
elif [[ "$args" == *"kafka-console-consumer.sh"* ]]; then
  if [ "$SCENARIO" = "lost_message" ]; then exit 0; fi
  cat "$FAKE_DIR/msg"
fi
"""


def _setup(tmp_path, monkeypatch, scenario):
    fake = tmp_path / "docker"
    fake.write_text(FAKE_DOCKER)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DIR", str(tmp_path))
    monkeypatch.setenv("SCENARIO", scenario)
    monkeypatch.setattr(sc, "port_open", lambda host, port, timeout=3.0: True)


def test_all_pass(tmp_path, monkeypatch, capsys):
    _setup(tmp_path, monkeypatch, "ok")
    assert sc.main() == 0
    out = capsys.readouterr().out
    assert "FAIL" not in out and "postgis=3.6.1" in out and "메시지 1건 왕복 확인" in out
    assert (tmp_path / "deleted").exists()  # 점검용 임시 토픽은 항상 지운다


def test_kafka_down_is_reported_and_skips_kafka_checks(tmp_path, monkeypatch, capsys):
    _setup(tmp_path, monkeypatch, "kafka_down")
    assert sc.main() == 1
    out = capsys.readouterr().out
    assert "FAIL  container:kafka" in out and "kafka:produce-consume" not in out


def test_lost_message_fails_but_still_cleans_up(tmp_path, monkeypatch, capsys):
    _setup(tmp_path, monkeypatch, "lost_message")
    assert sc.main() == 1
    assert "FAIL  kafka:produce-consume" in capsys.readouterr().out
    assert (tmp_path / "deleted").exists()


def test_docker_daemon_down_gives_actionable_message(tmp_path, monkeypatch, capsys):
    _setup(tmp_path, monkeypatch, "daemon_down")
    assert sc.main() == 2
    out = capsys.readouterr().out
    assert "Docker Desktop" in out and "open -a Docker" in out
