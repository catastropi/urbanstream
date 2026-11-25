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
    assert set(SERVICES) == {"postgres", "kafka", "kafka-ui", "spark-bronze", "spark-silver", "spark-gold",
                             "airflow-scheduler", "airflow-apiserver", "airflow-dag-processor", "gold-runner", "monitor", "api", "dashboard",
                             "collector"}
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


def test_restart_policy_has_no_retry_cap():
    """Mac 훈련(D-142): on-failure:3 은 Kafka 가 돌아오기 전에 3번을 다 써서 spark-bronze 가 영구 정지했다. 횟수 제한(`:N`)을 두지 않는다."""
    for name, svc in SERVICES.items():
        assert svc.get("restart") == "on-failure", f"{name}: restart={svc.get('restart')!r} — 횟수 제한 없는 on-failure 여야 합니다"


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
    env.write_text("POSTGRES_PASSWORD=dummy\nKAFKA_CLUSTER_ID=C7xqjrvSS4upP61um16UhQ\nCOMPOSE_PROFILES=core,dev,airflow\n"
                   "AIRFLOW_FERNET_KEY=x\nAIRFLOW_JWT_SECRET=x\nAIRFLOW_DB_PASSWORD=x\nGOLD_LOADER_PASSWORD=x\nAIRFLOW_ADMIN_PASSWORD=x\n")
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


# ---------------------------------------------------------------- Step 6: spark-gold

GOLD = SERVICES["spark-gold"]


def test_gold_is_a_oneshot_profile_service_reusing_the_image_without_secrets_or_ports():
    assert GOLD["profiles"] == ["gold"] and GOLD["image"] == SPARK["image"] and "build" not in GOLD
    assert "ports" not in GOLD
    env_text = str(GOLD.get("environment", {})).upper()
    assert not any(w in env_text for w in ("KEY", "PASSWORD", "SECRET", "TOKEN")) and "env_file" not in GOLD
    assert "depends_on" not in GOLD, "DB·Kafka 에 의존하지 않는다 (Parquet 만 읽고 쓴다)"


def test_gold_mounts_silver_and_bronze_read_only_and_writes_only_gold_stage():
    vols = {v.split(":")[0]: v for v in GOLD["volumes"]}
    assert sorted(vols) == ["./data/bronze", "./data/gold_stage", "./data/silver", "./data/silver_rebuild"]
    assert vols["./data/bronze"].endswith(":ro") and vols["./data/silver"].endswith(":ro")
    assert vols["./data/silver_rebuild"].endswith(":ro"), "STEP 1: 재처리 버전은 읽기만 한다 (게시는 gold-runner)"
    assert not vols["./data/gold_stage"].endswith(":ro")
    cmd = GOLD["command"]
    assert cmd[-1] == "/app/batch/gold_stage.py"
    assert _mem_mib(_default(GOLD["mem_limit"])) >= _mem_mib(cmd[cmd.index("--driver-memory") + 1]) * 1.4


def test_dockerfile_copies_batch_code_only_not_sql_scripts_or_db_loader_secrets():
    copies = re.findall(r"^COPY (.+)$", DOCKERFILE, re.M)
    assert "batch /app/batch" in copies and not any(c.startswith(("sql", "scripts", ".env")) for c in copies)


# ---------------------------------------------------------------- Step 7: Airflow

AIRFLOW = {n: SERVICES[n] for n in ("airflow-scheduler", "airflow-apiserver", "airflow-dag-processor")}
AF_DOCKERFILE = (ROOT / "docker" / "airflow" / "Dockerfile").read_text(encoding="utf-8")


def test_airflow_services_are_opt_in_via_profile():
    for name, svc in AIRFLOW.items():
        assert svc["profiles"] == ["airflow"], f"{name}: core 만 켜면 Airflow 가 뜨지 않아야 합니다 (메모리)"


def test_airflow_secrets_are_required_from_env_and_no_env_file_or_docker_sock():
    env = AIRFLOW["airflow-scheduler"]["environment"]
    for key in ("AIRFLOW__CORE__FERNET_KEY", "AIRFLOW__API_AUTH__JWT_SECRET", "GOLD_WRITER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD"):
        assert re.fullmatch(r"\$\{[A-Z_]+:\?.+\}", env[key]), f"{key}: .env 에서 필수로 읽어야 합니다"
    assert ":?" in env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    for name, svc in AIRFLOW.items():
        assert "env_file" not in svc, f"{name}: .env 전체(API 키 포함)가 컨테이너에 들어갑니다"
        assert not any("docker.sock" in v for v in svc.get("volumes", [])), f"{name}: docker.sock 마운트 금지"
        assert svc.get("privileged") is not True


def test_airflow_uses_least_privilege_db_role_not_admin():
    env = AIRFLOW["airflow-scheduler"]["environment"]
    assert env["GOLD_WRITER_USER"] == "gold_loader"
    assert "POSTGRES_PASSWORD" not in env and "POSTGRES_USER" not in env, "업무 DB 관리자 계정을 Airflow 에 주지 마세요 (D-79)"
    assert "@postgres:5432/airflow" in env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"], "메타데이터는 별도 DB(airflow)"


def test_airflow_data_mounts_are_read_only_except_stage_and_logs():
    for name, svc in AIRFLOW.items():
        for v in svc["volumes"]:
            src, dst, *mode = v.split(":")
            rw = not mode or mode[0] != "ro"
            if rw:
                assert dst in ("/data/gold_stage", "/opt/airflow/logs"), f"{name}: {v} 는 읽기 전용이어야 합니다"
        assert any(v.startswith("./data/silver:/data/silver:ro") for v in svc["volumes"])
        assert any(v.startswith("./data/bronze:/data/bronze:ro") for v in svc["volumes"])
        assert "./data/silver_rebuild:/data/silver_rebuild:ro" in svc["volumes"], "STEP 1: silver_verify·inline 스테이징이 재처리 버전을 봐야 한다"


def test_airflow_scheduler_memory_covers_spark_heap_and_start_order():
    sch = AIRFLOW["airflow-scheduler"]
    # gold_stage 의 Spark JVM 이 스케줄러 컨테이너 안에서 뜬다: 힙(batch/jobs.py 의 driver-memory) × 1.4 + Airflow 자신 몫
    jobs_src = (ROOT / "batch" / "jobs.py").read_text(encoding="utf-8")
    heap = _mem_mib(re.search(r'driver_memory: str = "(\d+[gm])"', jobs_src).group(1))
    limit = _mem_mib(_default(sch["mem_limit"]))
    assert limit >= heap * 1.4 + 1024, f"스케줄러 한도({limit} MiB) 가 Spark 힙({heap} MiB)×1.4 + Airflow 1 GiB 보다 작습니다"
    for name in ("airflow-apiserver", "airflow-dag-processor"):
        assert AIRFLOW[name]["depends_on"]["airflow-scheduler"]["condition"] == "service_healthy"
    assert sch["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert "service_completed_successfully" not in (ROOT / "docker-compose.yml").read_text(encoding="utf-8").replace(
        "(Compose v2.10 에서 service_completed_successfully", "")  # 구버전 Compose 에서 불안정


def test_airflow_scheduler_prepares_db_user_and_single_slot_pool():
    cmd = "\n".join(AIRFLOW["airflow-scheduler"]["command"])
    assert "airflow db migrate" in cmd and "airflow pools set gold_io 1" in cmd and "scheduler" in cmd
    assert "$$AIRFLOW_ADMIN_PASSWORD" in cmd, "비밀번호를 명령줄에 직접 쓰지 말고 환경 변수로 넘기세요"
    assert AIRFLOW["airflow-apiserver"]["ports"] == ["127.0.0.1:${AIRFLOW_UI_PORT:-8080}:8080"]


def test_airflow_dockerfile_pins_versions_and_uses_constraints():
    assert re.search(r"^ARG AIRFLOW_IMAGE=apache/airflow:\d+\.\d+\.\d+-python\d+\.\d+$", AF_DOCKERFILE, re.M)
    assert "--constraint" in AF_DOCKERFILE and "constraints-${AIRFLOW_VERSION}" in AF_DOCKERFILE
    assert re.search(r"pyspark==\$\{PYSPARK_VERSION\}", AF_DOCKERFILE)
    assert "default-jre-headless" in AF_DOCKERFILE
    pyspark_default = re.search(r"^ARG PYSPARK_VERSION=(\S+)$", AF_DOCKERFILE, re.M).group(1)
    assert pyspark_default == _default(SPARK["build"]["args"]["SPARK_VERSION"]), "Silver/Bronze 의 Spark 와 같은 버전이어야 합니다"


def test_monitor_service_is_minimal_and_read_only():
    m = SERVICES["monitor"]
    assert m["profiles"] == ["monitor"] and m["image"].startswith("urbanstream/airflow:")
    assert "env_file" not in m and not any("docker.sock" in str(v) for v in m.get("volumes", []))
    assert "ports" not in m
    for v in m["volumes"]:
        assert v.endswith(":ro"), f"monitor 마운트는 읽기 전용이어야 합니다: {v}"
    env = m["environment"]
    for secret_like in ("SEOUL_API_KEY", "KMA_API_KEY", "POSTGRES_PASSWORD", "AIRFLOW__CORE__FERNET_KEY"):
        assert secret_like not in env, f"monitor 에 {secret_like} 를 넣지 않습니다"
    assert env["GOLD_WRITER_USER"] == "gold_loader" and env["KAFKA_BOOTSTRAP"] == "kafka:19092"
    assert "healthcheck" in m and m["depends_on"]["kafka"]["condition"] == "service_healthy"


def test_alert_webhook_url_is_referenced_by_name_only():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert not re.search(r"https://(?:discord|hooks\.slack)", text), "웹훅 URL 을 compose 에 직접 쓰지 않습니다"
    assert SERVICES["airflow-scheduler"]["environment"]["ALERT_WEBHOOK_URL"].startswith("${ALERT_WEBHOOK_URL")
    assert SERVICES["monitor"]["environment"]["ALERT_WEBHOOK_URL"].startswith("${ALERT_WEBHOOK_URL")


# ------------------------------------------------------------------ Step 9: api
def test_api_service_is_opt_in_localhost_only_and_memory_limited():
    api = SERVICES["api"]
    assert api["profiles"] == ["api"]                                   # core 만 켜면 API 는 뜨지 않는다
    assert api["ports"] == ["127.0.0.1:${API_PORT:-8010}:8000"]
    assert api["mem_limit"] == "${API_MEM_LIMIT:-256m}"
    assert api["container_name"] == "urbanstream-api"
    assert api["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert "healthcheck" in api


def test_api_is_hardened_and_gets_no_admin_secrets():
    api = SERVICES["api"]
    assert api["read_only"] is True and api["cap_drop"] == ["ALL"] and "no-new-privileges:true" in api["security_opt"]
    assert api["tmpfs"] == ["/tmp"]
    assert "env_file" not in api and not api.get("volumes"), "API 는 .env 전체도 데이터 폴더도 받지 않는다"
    env = api["environment"]
    assert env["API_DB_USER"] == "api_service" and env["API_DB_HOST"] == "postgres"
    text = str(env)
    for forbidden in ("POSTGRES_PASSWORD", "GOLD_LOADER_PASSWORD", "GOLD_WRITER_PASSWORD", "AIRFLOW_", "ALERT_WEBHOOK"):
        assert forbidden not in text, f"api 환경에 {forbidden} 가 들어 있다"
    assert "docker.sock" not in str(api), "소켓 마운트 금지"


def test_api_secrets_come_from_env_by_name_and_compose_does_not_require_them():
    """`:?` 로 필수 처리하면 stack.sh 의 ps/down/stats(--profile '*')가 API 키 없이 전부 깨진다 → `:-` + 앱의 기동 거부(fail-closed)."""
    env = SERVICES["api"]["environment"]
    assert env["API_KEYS"] == "${API_KEYS:-}" and env["API_DB_PASSWORD"] == "${API_DB_PASSWORD:-}"
    assert not re.search(r"API_(KEYS|DB_PASSWORD):\s*[A-Za-z0-9]", (ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def test_api_dockerfile_is_pinned_unprivileged_and_copies_only_the_api():
    df = (ROOT / "docker/api/Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"ARG PYTHON_IMAGE=python:3\.\d+-slim-bookworm", df)
    assert re.search(r"^USER 10001$", df, re.M)
    copies = re.findall(r"^COPY (.+)$", df, re.M)
    assert copies == ["requirements-api.txt ./", "api ./api"], copies
    assert "--workers\", \"1\"" in df, "속도 제한·캐시가 프로세스 안에 있어 워커는 1개여야 한다"
    assert ".env" not in " ".join(copies) and "data" not in " ".join(copies)


def test_api_requirements_are_exactly_pinned():
    lines = [l.strip() for l in (ROOT / "requirements-api.txt").read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]
    assert lines and all("==" in l for l in lines), lines
    assert {l.split("==")[0].split("[")[0] for l in lines} >= {"fastapi", "uvicorn", "pydantic", "psycopg", "psycopg-pool"}


def test_stack_sh_up_api_checks_keys_before_starting():
    sh = (ROOT / "scripts/stack.sh").read_text(encoding="utf-8")
    block = sh.split("up-api)")[1].split(";;")[0]
    assert block.index("API_KEYS") < block.index("docker compose up") and "db_bootstrap.py --api" in block and "--build" in block


# ------------------------------------------------------------------ Step 10: dashboard
def test_dashboard_service_is_opt_in_localhost_only_and_waits_for_api():
    d = SERVICES["dashboard"]
    assert d["profiles"] == ["dashboard"]                                # 켜지 않으면 뜨지 않는다
    assert d["ports"] == ["127.0.0.1:${DASHBOARD_PORT:-8501}:8501"]
    assert d["mem_limit"] == "${DASHBOARD_MEM_LIMIT:-512m}"
    assert d["container_name"] == "urbanstream-dashboard"
    assert d["depends_on"]["api"]["condition"] == "service_healthy" and "postgres" not in d["depends_on"]
    assert "_stcore/health" in str(d["healthcheck"])


def test_dashboard_is_hardened_and_cannot_reach_the_database():
    d = SERVICES["dashboard"]
    assert d["read_only"] is True and d["cap_drop"] == ["ALL"] and "no-new-privileges:true" in d["security_opt"]
    assert d["tmpfs"] == ["/tmp"]
    assert "env_file" not in d and not d.get("volumes")
    env = d["environment"]
    assert env["DASHBOARD_API_URL"] == "http://api:8000"
    text = str(env)
    for forbidden in ("POSTGRES", "GOLD_", "API_DB_", "AIRFLOW_", "ALERT_WEBHOOK", "SEOUL_API_KEY", "API_KEYS"):
        assert forbidden not in text, f"dashboard 환경에 {forbidden} 가 들어 있다 (데이터 입구는 API 키 하나뿐)"
    assert "docker.sock" not in str(d)


def test_dashboard_key_comes_from_env_by_name_and_compose_does_not_require_it():
    env = SERVICES["dashboard"]["environment"]
    assert env["DASHBOARD_API_KEY"] == "${DASHBOARD_API_KEY:-}"
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert not re.search(r"DASHBOARD_API_KEY:\s*[A-Za-z0-9]", text) and "${DASHBOARD_API_KEY:?" not in text


def test_dashboard_dockerfile_is_pinned_unprivileged_and_copies_only_the_dashboard():
    df = (ROOT / "docker/dashboard/Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"ARG PYTHON_IMAGE=python:3\.(1[1-9]|[2-9]\d)-slim-bookworm", df), "streamlit 은 Python 3.11 이상"
    assert re.search(r"^USER 10001$", df, re.M)
    copies = re.findall(r"^COPY (.+)$", df, re.M)
    assert copies == ["requirements-dashboard.txt ./", "dashboard ./dashboard"], copies
    assert "--server.fileWatcherType=none" in df and "--browser.gatherUsageStats=false" in df and "HOME=/tmp" in df


def test_dashboard_requirements_are_exactly_pinned_and_have_no_db_driver():
    lines = [l.strip() for l in (ROOT / "requirements-dashboard.txt").read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]
    assert lines and all("==" in l for l in lines), lines
    names = {l.split("==")[0] for l in lines}
    assert {"streamlit", "pandas", "pydeck", "requests"} <= names
    assert not names & {"psycopg", "psycopg2", "psycopg-pool", "sqlalchemy", "fastapi"}, "DB 드라이버·서버 프레임워크는 대시보드에 없다"


def test_stack_sh_up_dashboard_checks_keys_before_starting_and_enables_all_needed_profiles():
    sh = (ROOT / "scripts/stack.sh").read_text(encoding="utf-8")
    block = sh.split("up-dashboard)")[1].split(";;")[0]
    assert block.index("DASHBOARD_API_KEY") < block.index("docker compose up")
    assert "--check-dashboard-key" in block and "db_bootstrap.py --api" in block and "--build" in block
    assert 'COMPOSE_PROFILES="core,api,dashboard"' in block


# ------------------------------------------------------------------ STEP 3A: gold-runner
RUNNER = SERVICES["gold-runner"]


def test_gold_runner_reuses_the_spark_image_and_starts_with_airflow():
    assert RUNNER["image"] == SPARK["image"] and "build" not in RUNNER, "Bronze/Silver 와 같은 이미지(같은 Spark·커넥터)"
    assert RUNNER["profiles"] == ["airflow"], "Airflow 가 요청을 남기므로 같이 켜져야 한다"
    assert RUNNER["command"][:3] == ["python3", "-m", "batch.stage_runner"]
    assert "ports" not in RUNNER and "depends_on" not in RUNNER


def test_gold_runner_gets_no_secrets_and_writes_only_gold_stage_and_silver_rebuild():
    env_text = str(RUNNER.get("environment", {})).upper()
    assert not any(w in env_text for w in ("KEY", "PASSWORD", "SECRET", "TOKEN", "POSTGRES")) and "env_file" not in RUNNER
    assert "docker.sock" not in str(RUNNER)
    for v in RUNNER["volumes"]:
        src, dst, *mode = v.split(":")
        if not mode or mode[0] != "ro":
            assert dst in ("/data/gold_stage", "/data/silver_rebuild"), f"gold-runner 가 쓸 수 있는 곳은 스테이징·재처리 폴더뿐이다: {v}"
    assert "./data/silver:/data/silver:ro" in RUNNER["volumes"], "스트리밍 Silver 는 재처리 중에도 읽기 전용 (ADR-0015)"
    assert RUNNER["environment"]["GOLD_SILVER_REBUILD_ROOT"] == "/data/silver_rebuild"
    srcs = {v.split(":")[0] for v in RUNNER["volumes"]}
    assert {"./batch", "./src", "./streaming", "./config"} <= srcs, "Airflow 와 같은 코드를 써야 한다 (이미지 안 코드와 어긋나지 않게)"


def test_gold_runner_memory_covers_its_spark_heap_and_airflow_defaults_to_runner():
    heap = _mem_mib(RUNNER["environment"]["GOLD_DRIVER_MEMORY"])
    assert _mem_mib(_default(RUNNER["mem_limit"])) >= heap * 1.4
    assert SERVICES["airflow-scheduler"]["environment"]["GOLD_STAGE_MODE"] == "${GOLD_STAGE_MODE:-runner}"
    assert "/data/gold_stage/_requests/.runner_alive" in str(RUNNER["healthcheck"])


# ------------------------------------------------------------------ STEP 3A: collector
COLLECTOR = SERVICES["collector"]


def test_collector_is_opt_in_and_waits_for_kafka_without_ports():
    assert COLLECTOR["profiles"] == ["collector"]
    assert COLLECTOR["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert COLLECTOR["environment"]["KAFKA_BOOTSTRAP"] == "kafka:19092"
    assert "ports" not in COLLECTOR


def test_collector_gets_only_its_api_keys_by_name():
    env = COLLECTOR["environment"]
    assert env["SEOUL_API_KEY"] == "${SEOUL_API_KEY:-}" and env["DATA_GO_KR_KEY"] == "${DATA_GO_KR_KEY:-}"
    assert "env_file" not in COLLECTOR and "docker.sock" not in str(COLLECTOR)
    text = str(env)
    for forbidden in ("POSTGRES", "GOLD_", "AIRFLOW", "ALERT_WEBHOOK", "API_KEYS", "API_DB_", "DASHBOARD"):
        assert forbidden not in text, f"collector 환경에 {forbidden} 가 들어 있다"
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert not re.search(r"(SEOUL_API_KEY|DATA_GO_KR_KEY):\s*[A-Za-z0-9]", compose_text)


def test_collector_is_hardened_and_writes_only_raw_and_budget():
    assert COLLECTOR["read_only"] is True and COLLECTOR["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in COLLECTOR["security_opt"]
    vols = {v.split(":")[0]: v for v in COLLECTOR["volumes"]}
    assert sorted(vols) == ["./config", "./data/collector", "./data/raw"]
    assert vols["./config"].endswith(":ro")
    # 호스트에서 돌던 때와 같은 경로(collector/__main__.py 의 ROOT/data/...)여야 예산을 이어 센다
    assert vols["./data/collector"] == "./data/collector:/app/data/collector"
    assert vols["./data/raw"] == "./data/raw:/app/data/raw"


def test_collector_dockerfile_is_pinned_unprivileged_and_copies_no_secrets():
    df = (ROOT / "docker/collector/Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"ARG PYTHON_IMAGE=python:3\.\d+-slim-bookworm", df)
    assert re.search(r"^USER 10001$", df, re.M)
    copies = re.findall(r"^COPY (.+)$", df, re.M)
    assert copies == ["requirements-collector.txt ./", "src ./src", "collector ./collector", "config ./config"], copies
    lines = [l.strip() for l in (ROOT / "requirements-collector.txt").read_text(encoding="utf-8").splitlines()
             if l.strip() and not l.startswith("#")]
    assert lines and all("==" in l for l in lines), lines
    assert {l.split("==")[0] for l in lines} == {"confluent-kafka", "python-dotenv", "PyYAML", "requests"}


# ------------------------------------------------------------------ STEP 1: 재처리 폴더
def test_streaming_silver_never_sees_the_rebuild_folder():
    """spark-silver 는 data/silver 와 체크포인트만 쓴다. 재처리 버전 폴더를 주면 sink 로그와 섞일 위험이 생긴다."""
    assert not any("silver_rebuild" in v for v in SERVICES["spark-silver"]["volumes"])
    assert not any("silver_rebuild" in v for v in SERVICES["spark-bronze"]["volumes"])


def test_every_gold_stage_runner_can_see_the_rebuild_pointer():
    """read_silver 는 포인터 폴더가 없으면 스트리밍 Silver 만 읽는다 → Gold 를 만드는 모든 곳이 같은 폴더를 마운트해야 한다."""
    for name in ("gold-runner", "spark-gold", "airflow-scheduler"):
        assert any(v.split(":")[1] == "/data/silver_rebuild" for v in SERVICES[name]["volumes"]), name
