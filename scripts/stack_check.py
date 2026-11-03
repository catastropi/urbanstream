"""Docker Compose 스택 점검: 컨테이너 상태, Postgres(PostGIS), Kafka 왕복, 호스트 포트.

실행:  python scripts/stack_check.py          (프로젝트 루트에서, 스택이 떠 있는 상태)
출력:  PASS/FAIL 표. 비밀번호 등은 출력하지 않는다 (컨테이너 안의 환경변수로 접속하므로 호스트에서 다루지 않는다).
종료코드: 하나라도 FAIL 이면 1.

Kafka 점검은 임시 토픽(stack-check-tmp)을 만들어 메시지 1개를 보내고 읽은 뒤 지운다.
운영 토픽은 Step 3 에서 만든다. (auto create 가 꺼져 있으므로 점검용 토픽도 명시적으로 만든다)
"""
from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KAFKA_BIN = "/opt/kafka/bin"
BOOTSTRAP_INTERNAL = "localhost:19092"   # 컨테이너 안에서의 INTERNAL 리스너
BOOTSTRAP_EXTERNAL = "localhost:9092"    # 컨테이너 안에서의 EXTERNAL 리스너 (호스트에 광고되는 쪽)
TOPIC = "stack-check-tmp"
CORE_SERVICES = ("postgres", "kafka")


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


def parse_ps(text: str) -> list[dict]:
    """`docker compose ps --format json` 출력. 버전에 따라 JSON 배열 또는 줄마다 JSON 객체(NDJSON)다."""
    text = text.strip()
    if not text:
        return []
    if text.startswith("["):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def evaluate_ps(rows: list[dict]) -> list[Check]:
    by_service = {r.get("Service"): r for r in rows}
    checks = []
    for svc in list(CORE_SERVICES) + (["kafka-ui"] if "kafka-ui" in by_service else []):
        r = by_service.get(svc)
        if r is None:
            checks.append(Check(f"container:{svc}", False, "컨테이너가 없습니다 (scripts/stack.sh up)"))
            continue
        state, health = r.get("State", ""), r.get("Health", "")
        ok = state == "running" and health in ("healthy", "")
        checks.append(Check(f"container:{svc}", ok, f"state={state} health={health or '-'}"))
    return checks


def port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def load_ports() -> dict[str, int]:
    ports = {"postgres": 5433, "kafka": 9092, "kafka-ui": 8090}
    env = ROOT / ".env"
    if env.exists():
        try:
            from dotenv import dotenv_values
            v = dotenv_values(env)
            ports["postgres"] = int(v.get("POSTGRES_PORT") or ports["postgres"])
            ports["kafka"] = int(v.get("KAFKA_EXTERNAL_PORT") or ports["kafka"])
            ports["kafka-ui"] = int(v.get("KAFKA_UI_PORT") or ports["kafka-ui"])
        except Exception:  # python-dotenv 가 없거나 값이 이상하면 기본 포트로 점검
            pass
    return ports


def _run(cmd: list[str], *, input_text: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, input=input_text, capture_output=True, text=True, timeout=timeout)


def _exec(service: str, args: list[str], **kw) -> subprocess.CompletedProcess:
    return _run(["docker", "compose", "--profile", "*", "exec", "-T", service, *args], **kw)


def check_postgres() -> list[Check]:
    # 비밀번호가 호스트를 거치지 않도록, 컨테이너 안의 POSTGRES_USER/POSTGRES_DB 환경변수로 접속한다
    sql = ("select 'postgis=' || postgis_lib_version() || ' | postgres=' || current_setting('server_version')"
           " || ' | timezone=' || current_setting('TimeZone');\n")
    p = _exec("postgres", ["sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA'], input_text=sql)
    ok = p.returncode == 0 and "postgis=" in p.stdout
    return [Check("postgres:postgis", ok, p.stdout.strip() if ok else (p.stderr.strip()[:200] or "psql 실패"))]


def check_kafka() -> list[Check]:
    out: list[Check] = []
    p = _exec("kafka", [f"{KAFKA_BIN}/kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP_EXTERNAL], timeout=60)
    out.append(Check("kafka:external-listener", p.returncode == 0,
                     "EXTERNAL(광고 주소 localhost) 리스너 응답" if p.returncode == 0 else p.stderr.strip()[-200:]))

    marker = f"ping-{int(time.time())}"
    try:
        c = _exec("kafka", [f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", BOOTSTRAP_INTERNAL, "--create",
                            "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1"])
        out.append(Check("kafka:create-topic", c.returncode == 0, c.stderr.strip()[-200:] if c.returncode else TOPIC))
        if c.returncode != 0:
            return out
        pr = _exec("kafka", [f"{KAFKA_BIN}/kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP_INTERNAL,
                             "--topic", TOPIC], input_text=marker + "\n")
        co = _exec("kafka", [f"{KAFKA_BIN}/kafka-console-consumer.sh", "--bootstrap-server", BOOTSTRAP_INTERNAL,
                             "--topic", TOPIC, "--from-beginning", "--max-messages", "1", "--timeout-ms", "20000"],
                   timeout=90)
        out.append(Check("kafka:produce-consume", pr.returncode == 0 and marker in co.stdout,
                         "메시지 1건 왕복 확인" if marker in co.stdout else (co.stderr.strip()[-200:] or "메시지를 읽지 못함")))
    finally:
        _exec("kafka", [f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", BOOTSTRAP_INTERNAL, "--delete",
                        "--topic", TOPIC])
    return out


def main() -> int:
    ps = _run(["docker", "compose", "--profile", "*", "ps", "--all", "--format", "json"])
    if ps.returncode != 0:
        err = ps.stderr.strip()
        if "docker daemon" in err.lower():
            print("Docker 데몬에 연결할 수 없습니다. Docker Desktop 을 실행하세요 (open -a Docker) — "
                  "고래 아이콘이 'running' 이 된 뒤 다시 실행합니다.")
        else:
            print("docker compose ps 실패:", err[:300])
        return 2
    checks = evaluate_ps(parse_ps(ps.stdout))
    running = {c.name.split(":")[1] for c in checks if c.ok}
    ports = load_ports()
    for svc in ("postgres", "kafka") + (("kafka-ui",) if "kafka-ui" in running else ()):
        checks.append(Check(f"host-port:{svc}", port_open("127.0.0.1", ports[svc]), f"127.0.0.1:{ports[svc]}"))
    if "postgres" in running:
        checks += check_postgres()
    if "kafka" in running:
        checks += check_kafka()

    width = max(len(c.name) for c in checks)
    for c in checks:
        print(f"{'PASS' if c.ok else 'FAIL'}  {c.name:<{width}}  {c.detail}")
    failed = [c for c in checks if not c.ok]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} 통과")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
