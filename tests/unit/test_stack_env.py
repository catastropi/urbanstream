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
                    "API_PORT", "API_DB_PASSWORD", "API_KEYS", "DASHBOARD_PORT", "DASHBOARD_API_KEY"]
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
    api_keys = values["API_KEYS"].split(",")      # 일반 키 + (Step 10) 대시보드 키가 목록 끝에 붙는다
    assert all(len(k) >= 24 for k in api_keys) and api_keys[0] != values["DASHBOARD_API_KEY"]
    assert len({values[k] for k in ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD", "API_DB_PASSWORD")}) == 5
    assert len({values[k] for k in ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "GOLD_LOADER_PASSWORD", "AIRFLOW_ADMIN_PASSWORD")}) == 4  # 서로 달라야 한다
    assert values["POSTGRES_PORT"] == "5433" and values["COMPOSE_PROFILES"] == "core"
    # Step 10: 대시보드 전용 키 — 24자 이상, 다른 API 키와 다르고, API_KEYS 목록에 들어 있어야 API 에서 통한다
    assert len(values["DASHBOARD_API_KEY"]) >= 24 and values["DASHBOARD_PORT"] == "8501"
    assert [k for k in values["API_KEYS"].split(",")][-1] == values["DASHBOARD_API_KEY"] and len(values["API_KEYS"].split(",")) == 2


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


# ------------------------------------------------------------------ Step 10: 대시보드 키 (추가 전용 예외)
def _env_values(text):
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#"))


def test_dashboard_key_is_appended_to_existing_api_keys_without_touching_existing_keys():
    old = "k" * 40
    env = f"SEOUL_API_KEY=abc\nAPI_KEYS={old}\n"
    new, added, filled = stack_env.plan_env(EXAMPLE, env)
    v = _env_values(new)
    assert v["API_KEYS"].startswith(old + ",") and v["API_KEYS"].split(",")[1] == v["DASHBOARD_API_KEY"]
    assert "API_KEYS" not in added and "API_KEYS(대시보드 키 추가)" in filled
    assert old not in "".join(added + filled), "비밀 값이 보고에 섞이면 안 된다"
    again, a2, f2 = stack_env.plan_env(EXAMPLE, new)
    assert again == new and a2 == [] and f2 == [], "두 번째 실행은 아무것도 바꾸지 않는다(멱등)"


def test_dashboard_key_not_duplicated_when_already_listed_and_user_key_is_kept():
    mine = "m" * 30
    env = f"DASHBOARD_API_KEY={mine}\nAPI_KEYS=\"{'o' * 30},{mine}\"\n"
    new, added, filled = stack_env.plan_env(EXAMPLE, env)
    v = _env_values(new)
    assert v["DASHBOARD_API_KEY"] == mine
    assert new.count(mine) == 2, "이미 목록에 있으면 다시 덧붙이지 않는다"
    assert "API_KEYS(대시보드 키 추가)" not in filled


def test_dashboard_key_check_flag_reports_membership(tmp_path):
    k = "d" * 30
    env = tmp_path / ".env"
    env.write_text(f"API_KEYS={'o' * 30}\nDASHBOARD_API_KEY={k}\n", encoding="utf-8")
    assert stack_env.main(["--env", str(env), "--check-dashboard-key"]) == 1
    env.write_text(f"API_KEYS={'o' * 30},{k}\nDASHBOARD_API_KEY={k}\n", encoding="utf-8")
    assert stack_env.main(["--env", str(env), "--check-dashboard-key"]) == 0
    before = env.read_text(encoding="utf-8")
    stack_env.main(["--env", str(env), "--check-dashboard-key"])
    assert env.read_text(encoding="utf-8") == before, "점검은 파일을 바꾸지 않는다"
