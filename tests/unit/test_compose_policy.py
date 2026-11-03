"""docker-compose.yml 이 지켜야 할 규칙. 규칙을 깨는 수정은 여기서 걸린다."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
SERVICES = COMPOSE["services"]
CORE = ("postgres", "kafka")


def test_expected_services_and_profiles():
    assert set(SERVICES) == {"postgres", "kafka", "kafka-ui", "spark-bronze", "spark-silver"}
    assert SERVICES["postgres"]["profiles"] == ["core"] and SERVICES["kafka"]["profiles"] == ["core"]
    assert SERVICES["kafka-ui"]["profiles"] == ["dev"]
    assert SERVICES["spark-bronze"]["profiles"] == ["streaming"]    # core 만 켜면 Spark 는 뜨지 않는다 (메모리)
    assert SERVICES["spark-silver"]["profiles"] == ["silver"]       # Silver 는 따로 켠다 (Bronze 만 돌릴 때 메모리를 더 쓰지 않는다)


def test_ports_are_bound_to_localhost_only():
    for name, svc in SERVICES.items():
        for p in svc.get("ports", []):
            assert p.startswith("127.0.0.1:"), f"{name}: {p} 는 127.0.0.1 로 한정해야 합니다"


def test_no_hardcoded_secrets_and_password_is_required():
    pw = SERVICES["postgres"]["environment"]["POSTGRES_PASSWORD"]
    assert re.fullmatch(r"\$\{POSTGRES_PASSWORD:\?.+\}", pw), "비밀번호는 .env 에서 필수로 읽어야 합니다"
    assert re.fullmatch(r"\$\{KAFKA_CLUSTER_ID:\?.+\}", SERVICES["kafka"]["environment"]["CLUSTER_ID"])
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert not re.search(r"POSTGRES_PASSWORD:\s*[A-Za-z0-9]", text)


def test_core_images_are_pinned_not_latest():
    for name in CORE:
        image = SERVICES[name]["image"]
        default = re.search(r"\$\{[A-Z_]+:-([^}]+)\}", image).group(1)  # ${VAR:-기본값} 의 기본값
        assert ":" in default and not default.endswith(":latest"), f"{name}: 태그를 고정하세요 ({default})"


def test_every_service_has_memory_limit_and_restart_policy():
    for name, svc in SERVICES.items():
        assert "mem_limit" in svc, f"{name}: mem_limit 이 없습니다 (Docker VM 메모리 보호)"
        assert svc.get("restart", "").startswith("on-failure")


def test_core_services_have_healthchecks_and_ui_waits_for_kafka():
    for name in CORE:
        assert "healthcheck" in SERVICES[name]
    assert SERVICES["kafka-ui"]["depends_on"]["kafka"]["condition"] == "service_healthy"


def test_kafka_listener_and_topic_policy():
    env = SERVICES["kafka"]["environment"]
    assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert "INTERNAL://kafka:19092" in env["KAFKA_ADVERTISED_LISTENERS"]
    assert "EXTERNAL://localhost:" in env["KAFKA_ADVERTISED_LISTENERS"]
    assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == 1  # 단일 브로커
    assert SERVICES["kafka-ui"]["environment"]["KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"] == "kafka:19092"


def test_data_lives_in_named_volumes():
    assert set(COMPOSE["volumes"]) == {"postgres-data", "kafka-data"}
    mounts = [v for n in CORE for v in SERVICES[n]["volumes"]]
    assert {m.split(":")[0] for m in mounts} == {"postgres-data", "kafka-data"}


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker 없음")
def test_compose_file_is_valid_for_docker(tmp_path):
    probe = subprocess.run(["docker", "compose", "version"], capture_output=True)
    if probe.returncode != 0:
        pytest.skip("docker compose 플러그인 없음")
    env = tmp_path / "t.env"
    env.write_text("POSTGRES_PASSWORD=dummy\nKAFKA_CLUSTER_ID=C7xqjrvSS4upP61um16UhQ\nCOMPOSE_PROFILES=core,dev\n")
    r = subprocess.run(["docker", "compose", "--env-file", str(env), "-f", str(ROOT / "docker-compose.yml"), "config", "-q"],
                       capture_output=True, text=True, cwd=ROOT)
    assert r.returncode == 0, r.stderr


def test_kafka_log_dirs_live_inside_the_named_volume():
    # KAFKA_LOG_DIRS 가 없으면 이미지 기본값(/tmp/kafka-logs)에 써서 볼륨이 비어 있고, 컨테이너를 지우면 데이터도 사라진다.
    kafka = SERVICES["kafka"]
    log_dirs = kafka["environment"].get("KAFKA_LOG_DIRS")
    assert log_dirs, "KAFKA_LOG_DIRS 를 볼륨 마운트 경로로 지정하세요 (지정하지 않으면 볼륨 밖에 저장됩니다)"
    mounts = dict(v.split(":")[:2] for v in kafka["volumes"])
    assert "kafka-data" in mounts
    target = mounts["kafka-data"].rstrip("/")
    assert log_dirs.rstrip("/") == target or log_dirs.startswith(target + "/"), \
        f"KAFKA_LOG_DIRS({log_dirs}) 가 kafka-data 마운트({target}) 안에 있어야 합니다"



# ---------------------------------------------------------------- Step 4: spark-bronze / Dockerfile

SPARK = SERVICES["spark-bronze"]
DOCKERFILE = (ROOT / "docker" / "spark-base" / "Dockerfile").read_text(encoding="utf-8")


def _default(expr: str) -> str:
    return re.search(r"\$\{[A-Z_]+:-([^}]+)\}", expr).group(1)


def _mem_mib(v: str) -> float:
    m = re.fullmatch(r"(\d+)([gm])", v.lower())
    return int(m.group(1)) * (1024 if m.group(2) == "g" else 1)


def test_spark_image_tag_is_pinned_and_versions_agree_everywhere():
    args = SPARK["build"]["args"]
    image, version = _default(args["SPARK_IMAGE"]), _default(args["SPARK_VERSION"])
    assert not image.endswith(":latest") and re.search(r":\d+\.\d+\.\d+-", image), f"태그를 고정하세요: {image}"
    tag = image.split(":")[1]
    assert tag.startswith(version + "-"), f"SPARK_VERSION({version}) 과 이미지 태그({tag}) 가 다릅니다"
    # Dockerfile 기본값(직접 docker build 할 때)도 같은 값이어야 한다
    assert re.search(rf"^ARG SPARK_IMAGE={re.escape(image)}$", DOCKERFILE, re.M)
    assert re.search(rf"^ARG SPARK_VERSION={re.escape(version)}$", DOCKERFILE, re.M)
    scala = re.search(r"^ARG SCALA_BINARY=(\S+)$", DOCKERFILE, re.M).group(1)
    assert f"scala{scala}" in tag, f"Kafka 커넥터 Scala 버전({scala}) 이 이미지 태그({tag}) 와 달라 클래스 오류가 납니다"
    assert _default(SPARK["image"]) == version


def test_spark_connector_is_baked_into_the_image_not_downloaded_at_runtime():
    assert "spark-sql-kafka-0-10_${SCALA_BINARY}:${SPARK_VERSION}" in DOCKERFILE
    assert "--packages" not in " ".join(SPARK["command"]), "실행 때마다 내려받지 말고 이미지에 넣으세요 (재현성)"


def test_spark_waits_for_kafka_and_memory_limit_covers_driver_heap():
    assert SPARK["depends_on"]["kafka"]["condition"] == "service_healthy"
    cmd = SPARK["command"]
    heap = _mem_mib(cmd[cmd.index("--driver-memory") + 1])
    limit = _mem_mib(_default(SPARK["mem_limit"]))
    assert limit >= heap * 1.4, f"컨테이너 한도({limit} MiB) 가 드라이버 힙({heap} MiB) 에 비해 너무 작으면 OOM-kill 됩니다 (힙 밖 메모리 필요)"


def test_spark_container_mounts_only_data_dirs_and_gets_no_secrets():
    mounts = [v.split(":")[0] for v in SPARK["volumes"]]
    assert sorted(mounts) == ["./data/bronze", "./data/checkpoints"]
    env_text = str(SPARK["environment"]).upper()
    assert not any(w in env_text for w in ("KEY", "PASSWORD", "SECRET", "TOKEN"))
    assert "env_file" not in SPARK, ".env(API 키)를 통째로 컨테이너에 넣지 마세요"


def test_dockerfile_runs_unprivileged_and_never_copies_secrets_or_data():
    last_user = re.findall(r"^USER (\S+)$", DOCKERFILE, re.M)[-1]
    assert last_user == "spark", "마지막 USER 가 루트면 안 됩니다"
    copies = re.findall(r"^COPY (.+)$", DOCKERFILE, re.M)
    assert not any(".env" in c or c.split()[0].startswith("data") for c in copies)
    ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in ignore and "data/" in ignore, ".dockerignore 가 .env 와 data/ 를 제외해야 합니다"


# ---------------------------------------------------------------- Step 5: spark-silver

SILVER = SERVICES["spark-silver"]


def test_silver_reuses_the_bronze_image_and_never_builds_its_own():
    assert SILVER["image"] == SPARK["image"], "코드가 같은 이미지에 들어 있다 — 이미지가 둘로 갈라지면 Bronze/Silver 코드 버전이 어긋난다"
    assert "build" not in SILVER


def test_silver_mounts_bronze_read_only_and_only_data_dirs():
    vols = {v.split(":")[0]: v for v in SILVER["volumes"]}
    assert sorted(vols) == ["./data/bronze", "./data/checkpoints", "./data/silver"]
    assert vols["./data/bronze"].endswith(":ro"), "Silver 가 Bronze 폴더에 쓰지 못하게 읽기 전용으로 마운트해야 합니다"
    env_text = str(SILVER.get("environment", {})).upper()
    assert not any(w in env_text for w in ("KEY", "PASSWORD", "SECRET", "TOKEN")) and "env_file" not in SILVER


def test_silver_ui_port_differs_from_bronze_and_memory_limit_covers_heap():
    assert _default(SILVER["ports"][0]) != _default(SPARK["ports"][0]), "호스트 포트가 같으면 둘이 동시에 뜨지 못합니다"
    cmd = SILVER["command"]
    assert _mem_mib(_default(SILVER["mem_limit"])) >= _mem_mib(cmd[cmd.index("--driver-memory") + 1]) * 1.4
    assert cmd[-1] == "/app/streaming/silver_writer.py"
