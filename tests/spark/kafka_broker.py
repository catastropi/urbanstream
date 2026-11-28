"""시험 전용 Kafka 브로커 (STEP 3B, R1·R2·R4). 운영 브로커가 아니다. 시험이 직접 띄우고 kill -9 로 죽였다 다시 띄운다.

URBANSTREAM_TEST_KAFKA_HOME 에 Kafka 배포판(운영 이미지와 같은 3.9.2, kafka_2.13-3.9.2)이 풀려 있어야 한다. CI 는 weekly 의
recovery-kafka 잡이 내려받는다(.github/workflows/weekly.yml). KRaft 단일 노드, 127.0.0.1 의 빈 포트 두 개, 데이터는 시험 폴더.
보존 기간 검사를 1초마다 하게 둔다(test_bronze_recover_kafka 의 '보존 기간이 지나 지워짐' 재현이 2분 안에 끝나게).
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import Optional

HOME = os.environ.get("URBANSTREAM_TEST_KAFKA_HOME")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Broker:
    def __init__(self, workdir: Path, home: Optional[str] = None):
        self.home = Path(home or HOME or "")
        if not (self.home / "bin" / "kafka-server-start.sh").exists():
            raise RuntimeError(f"Kafka 배포판이 없다: {self.home}")
        self.dir = Path(workdir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.port, self.cport = free_port(), free_port()
        self.bootstrap = f"127.0.0.1:{self.port}"
        self.props = self.dir / "server.properties"
        self.props.write_text("\n".join([
            "process.roles=broker,controller", "node.id=1", f"controller.quorum.voters=1@127.0.0.1:{self.cport}",
            f"listeners=PLAINTEXT://127.0.0.1:{self.port},CONTROLLER://127.0.0.1:{self.cport}",
            f"advertised.listeners=PLAINTEXT://127.0.0.1:{self.port}", "controller.listener.names=CONTROLLER",
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT", "inter.broker.listener.name=PLAINTEXT",
            f"log.dirs={self.dir / 'data'}", "num.partitions=1", "offsets.topic.replication.factor=1",
            "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1", "auto.create.topics.enable=false",
            "log.retention.check.interval.ms=1000", "group.initial.rebalance.delay.ms=0", ""]), encoding="utf-8")
        subprocess.run([str(self.home / "bin" / "kafka-storage.sh"), "format", "-t", uuid.uuid4().hex[:22], "-c", str(self.props)],
                       check=True, capture_output=True, env=self._env())
        self.proc: Optional[subprocess.Popen] = None

    def _env(self) -> dict:
        e = dict(os.environ)
        e.setdefault("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m")
        e["LOG_DIR"] = str(self.dir / "logs")
        return e

    def start(self, wait_sec: float = 90) -> "Broker":
        self.proc = subprocess.Popen([str(self.home / "bin" / "kafka-server-start.sh"), str(self.props)], env=self._env(),
                                     stdout=open(self.dir / "stdout.log", "ab"), stderr=subprocess.STDOUT, start_new_session=True)
        self.wait_ready(wait_sec)
        return self

    def wait_ready(self, wait_sec: float) -> None:
        from confluent_kafka.admin import AdminClient
        t0 = time.time()
        while time.time() - t0 < wait_sec:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f"브로커가 바로 끝났다. {self.dir / 'stdout.log'} 를 본다")
            try:
                AdminClient({"bootstrap.servers": self.bootstrap, "broker.address.family": "v4"}).list_topics(timeout=3)
                return
            except Exception:                                                 # noqa: BLE001
                time.sleep(1)
        raise RuntimeError(f"{wait_sec}초 안에 브로커가 뜨지 않았다")

    def kill9(self) -> None:
        """브로커 JVM 을 SIGKILL (정상 종료 없이). 같은 데이터 폴더로 다시 start() 하면 이어서 뜬다."""
        if self.proc is None:
            return
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait(30)
        self.proc = None

    def stop(self) -> None:
        self.kill9()
