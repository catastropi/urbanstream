"""scripts/stack_env.py: .env 에 Compose 항목을 안전하게 채운다."""
import importlib.util
import sys
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("stack_env", ROOT / "scripts" / "stack_env.py")
stack_env = importlib.util.module_from_spec(spec)
sys.modules["stack_env"] = stack_env  # dataclass 가 모듈을 찾을 수 있게 먼저 등록
spec.loader.exec_module(stack_env)

EXAMPLE = (ROOT / ".env.example").read_text(encoding="utf-8")


def test_example_has_stack_section_with_expected_keys():
    keys = [k for k, _ in stack_env.stack_defaults(EXAMPLE)]
    assert keys == ["COMPOSE_PROFILES", "POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PASSWORD", "POSTGRES_PORT",
                    "KAFKA_EXTERNAL_PORT", "KAFKA_UI_PORT", "KAFKA_CLUSTER_ID",
                    "AIRFLOW_UI_PORT", "AIRFLOW_ADMIN_USER", "AIRFLOW_ADMIN_PASSWORD", "AIRFLOW_DB_PASSWORD",
                    "GOLD_LOADER_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET",
                    "API_PORT", "API_DB_PASSWORD", "API_KEYS"]
    assert dict(stack_env.stack_defaults(EXAMPLE))["POSTGRES_PASSWORD"] == ""  # 예제에 비밀번호를 적어 두지 않는다


def test_adds_missing_keys_generates_secrets_and_keeps_other_lines_untouched():
    env = "SEOUL_API_KEY=abc\nDATA_GO_KR_KEY=def\n\nKMA_NY=127"  # 마지막 줄바꿈 없음 (실제 사용자 .env 모양)
    new, added, filled = stack_env.plan_env(EXAMPLE, env)
    assert new.startswith("SEOUL_API_KEY=abc\nDATA_GO_KR_KEY=def\n\nKMA_NY=127\n")  # 기존 줄 그대로
    assert added == [k for k, _ in stack_env.stack_defaults(EXAMPLE)] and filled == []
    values = dict(line.split("=", 1) for line in new.splitlines() if "=" in line)
    assert len(values["POSTGRES_PASSWORD"]) == 32 and re.fullmatch(r"[0-9a-f]{32}", values["POSTGRES_PASSWORD"])
    assert re.fullmatch(r"[A-Za-z0-9_-]{22}", values["KAFKA_CLUSTER_ID"])  # KRaft 클러스터 ID 형식
    import base64
    assert len(base64.urlsafe_b64decode(values["AIRFLOW_FERNET_KEY"])) == 32 and len(values["AIRFLOW_FERNET_KEY"]) == 44  # Fernet 키 형식
    for k in ("AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD"):
        assert re.fullmatch(r"[0-9a-f]{32}", values[k]), k
    assert len(values["AIRFLOW_JWT_SECRET"]) >= 32
    # Step 9: API 비밀 — 로그인 비밀번호는 다른 비밀번호와 달라야 하고, 키는 24자 이상이며 목록 구분자(쉼표)가 없다
    assert re.fullmatch(r"[0-9a-f]{32}", values["API_DB_PASSWORD"]) and values["API_PORT"] == "8010"
    assert len(values["API_KEYS"]) >= 24 and "," not in values["API_KEYS"]
    assert len({values[k] for k in ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD", "API_DB_PASSWORD")}) == 5
    assert len({values[k] for k in ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD")}) == 4  # 서로 달라야 한다
    assert values["POSTGRES_PORT"] == "5433" and values["COMPOSE_PROFILES"] == "core"


def test_never_overwrites_existing_values_and_is_idempotent():
    env = "POSTGRES_PASSWORD=keepme\nPOSTGRES_PORT=6000\nKAFKA_CLUSTER_ID=abcdefghijklmnopqrstuv\n"
    new, added, filled = stack_env.plan_env(EXAMPLE, env)
    assert "POSTGRES_PASSWORD=keepme" in new and "POSTGRES_PORT=6000" in new
    assert "POSTGRES_PASSWORD" not in added + filled and "KAFKA_CLUSTER_ID" not in added + filled
    again, added2, filled2 = stack_env.plan_env(EXAMPLE, new)
    assert again == new and added2 == [] and filled2 == []


def test_fills_empty_secret_in_place():
    env = "POSTGRES_PASSWORD=\nSEOUL_API_KEY=x\n"
    new, added, filled = stack_env.plan_env(EXAMPLE, env)
    assert filled == ["POSTGRES_PASSWORD"] and "POSTGRES_PASSWORD" not in added
    assert new.splitlines()[0].startswith("POSTGRES_PASSWORD=") and len(new.splitlines()[0]) > len("POSTGRES_PASSWORD=")
    assert new.splitlines()[1] == "SEOUL_API_KEY=x"  # 순서와 다른 줄 유지


def test_main_does_not_print_secret_values(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("SEOUL_API_KEY=abc\n", encoding="utf-8")
    assert stack_env.main(["--env", str(env), "--example", str(ROOT / ".env.example")]) == 0
    out = capsys.readouterr().out
    pw = dict(l.split("=", 1) for l in env.read_text().splitlines() if "=" in l)["POSTGRES_PASSWORD"]
    assert pw and pw not in out
    assert oct(env.stat().st_mode & 0o777) == "0o600"


def test_missing_env_file_gives_clear_message(tmp_path, capsys):
    assert stack_env.main(["--env", str(tmp_path / "nope"), "--example", str(ROOT / ".env.example")]) == 2
    assert "cp .env.example .env" in capsys.readouterr().out
