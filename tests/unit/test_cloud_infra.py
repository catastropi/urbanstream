"""Step 12 (Azure 최소 증거 배포) 정적 시험 — Terraform·cloud-init·compose 덮어쓰기·배포 스크립트·CI 도구 (ADR-0014).

샌드박스와 CI 는 Azure 에 접근하지 못하므로 apply·배포·접속은 시험하지 않는다(런북에서 사용자가 실행하고 [미확인] 으로 둔다).
여기서는 '안전장치가 코드에 남아 있는가' 를 지킨다: SSH 는 내 IP 만, 비밀번호 로그인 없음, 포트는 터널로만, 비밀·상태 파일은 git 밖.
Terraform 바이너리가 없어도 돌도록 정규식·yaml 로만 본다(문법은 CI 의 terraform 잡이 본다).
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import ci_checks as CC  # noqa: E402
import ci_terraform as CT  # noqa: E402
import pin_terraform as PT  # noqa: E402

TF = ROOT / "infra" / "terraform"


def tf(name: str) -> str:
    return (TF / name).read_text(encoding="utf-8")


def sh(name: str) -> str:
    return (ROOT / "scripts" / name).read_text(encoding="utf-8")


def all_tf() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted(TF.glob("*.tf")))


def block(text: str, header: str) -> str:
    """header 로 시작하는 { ... } 블록의 본문 (중괄호 짝을 센다)."""
    i = text.index(header)
    j = text.index("{", i)
    depth, k = 0, j
    while k < len(text):
        depth += (text[k] == "{") - (text[k] == "}")
        if depth == 0:
            return text[j + 1:k]
        k += 1
    raise AssertionError("중괄호가 닫히지 않음")


# --- Terraform --------------------------------------------------------------------------------------------

def test_terraform_files_exist():
    for n in ("versions.tf", "variables.tf", "main.tf", "outputs.tf", "cloud-init.yaml.tftpl", "terraform.tfvars.example"):
        assert (TF / n).exists(), n


def test_nsg_has_exactly_one_inbound_rule_ssh_from_my_ip_only():
    nsg = block(tf("main.tf"), 'resource "azurerm_network_security_group" "this"')
    rules = re.findall(r"\bsecurity_rule\s*\{", nsg)
    assert len(rules) == 1, "인바운드 규칙은 SSH 하나뿐이어야 한다"
    rule = block(nsg, "security_rule")
    assert re.search(r'destination_port_range\s*=\s*"22"', rule)
    assert re.search(r'direction\s*=\s*"Inbound"', rule) and re.search(r'access\s*=\s*"Allow"', rule)
    assert re.search(r"source_address_prefix\s*=\s*var\.allowed_ssh_cidr\b", rule)


def test_no_world_open_sources_anywhere():
    text = all_tf()
    for line in text.splitlines():   # 0.0.0.0/0 은 '거부'하는 설명·검증식에만 나올 수 있다
        if "0.0.0.0/0" in line:
            assert any(k in line for k in ("description", "contains(", "error_message")), line
    assert "0.0.0.0/0" not in tf("main.tf")
    assert not re.search(r'source_address_prefix(es)?\s*=\s*(\[\s*)?"(\*|Internet|Any)"', text)


def test_ssh_cidr_variable_has_no_default_and_rejects_open_cidr():
    v = block(tf("variables.tf"), 'variable "allowed_ssh_cidr"')
    assert not re.search(r"^\s*default\s*=", v, re.M), "기본값이 있으면 실수로 아무 IP 나 열릴 수 있다"
    val = block(v, "validation")
    assert 'contains(["0.0.0.0/0", "::/0"], var.allowed_ssh_cidr)' in val and "cidrnetmask" in val


def test_vm_is_key_only_and_has_no_secrets_in_code():
    vm = block(tf("main.tf"), 'resource "azurerm_linux_virtual_machine" "this"')
    assert re.search(r"disable_password_authentication\s*=\s*true", vm)
    text = all_tf()
    for bad in ("admin_password", "client_secret", "subscription_id", "private_key", "BEGIN OPENSSH", "BEGIN RSA"):
        assert bad not in text, bad
    assert re.search(r'ssh_public_key_path"?[\s\S]*?default\s*=\s*"[^"]*\.pub"', tf("variables.tf"))


def test_defaults_match_the_decisions():
    v = tf("variables.tf")
    assert re.search(r'variable "location"[\s\S]*?default\s*=\s*"koreacentral"', v)
    assert re.search(r'variable "vm_size"[\s\S]*?default\s*=\s*"Standard_B2s_v2"', v)


def test_public_ip_is_standard_static_and_no_other_public_exposure():
    pip = block(tf("main.tf"), 'resource "azurerm_public_ip" "this"')
    assert 'sku                 = "Standard"' in pip and 'allocation_method   = "Static"' in pip
    assert all_tf().count('resource "azurerm_public_ip"') == 1
    assert "azurerm_lb" not in all_tf() and "azurerm_application_gateway" not in all_tf()


def test_everything_lives_in_one_resource_group_and_is_tagged():
    t = tf("main.tf")
    assert t.count('resource "azurerm_resource_group"') == 1
    # 리소스 그룹 밖의 것(구독 수준 역할·정책)을 만들지 않는다 — destroy 한 번으로 전부 사라져야 한다
    for forbidden in ("azurerm_role_assignment", "azurerm_policy", "azurerm_management_lock", "azurerm_consumption_budget"):
        assert forbidden not in t, forbidden
    assert "evidence-deploy-destroy-after" in t


def test_outputs_needed_by_cloud_sh_exist():
    o = tf("outputs.tf")
    for name in ("public_ip", "admin_username", "resource_group"):
        assert f'output "{name}"' in o
    for name in re.findall(r"tf_out (\w+)", sh("cloud.sh")):
        assert f'output "{name}"' in o, f"cloud.sh 가 읽는 output {name} 이 없다"


def test_cloud_init_installs_docker_only_and_has_no_secrets():
    raw = tf("cloud-init.yaml.tftpl")
    assert raw.startswith("#cloud-config")
    doc = yaml.safe_load(raw.replace("${admin_username}", "azureuser"))
    assert {"docker.io", "docker-compose-v2"} <= set(doc["packages"])
    assert re.findall(r"\$\{(\w+)\}", raw) == ["admin_username"]
    for bad in ("password", "ssh_authorized_keys", "API_KEY", "token"):
        assert bad.lower() not in raw.lower(), bad
    assert "/var/lib/urbanstream-cloud-init-done" in raw and "/var/lib/urbanstream-cloud-init-done" in sh("cloud.sh")


# --- compose 덮어쓰기 -----------------------------------------------------------------------------------------

def test_cloud_override_touches_only_serving_services_and_only_restart():
    doc = yaml.safe_load((ROOT / "docker-compose.cloud.yml").read_text(encoding="utf-8"))
    assert set(doc) == {"services"}
    assert set(doc["services"]) == {"postgres", "api", "dashboard"}
    for name, svc in doc["services"].items():
        assert svc == {"restart": "unless-stopped"}, f"{name}: 덮어쓰기는 재시작 정책만 — 포트·이미지를 건드리지 않는다(기존 127.0.0.1 바인딩 유지)"


def test_base_compose_still_binds_every_published_port_to_loopback():
    base = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    for name, svc in base["services"].items():
        for p in svc.get("ports", []):
            assert str(p).startswith("127.0.0.1:"), f"{name}: {p}"


# --- 배포 스크립트 --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["cloud.sh", "cloud_remote.sh"])
def test_scripts_parse_and_are_strict_and_quiet(name):
    text = sh(name)
    assert subprocess.run(["bash", "-n", str(ROOT / "scripts" / name)], capture_output=True).returncode == 0
    assert "set -euo pipefail" in text
    assert not re.search(r"^\s*set\s+-[a-z]*x", text, re.M), "set -x 는 비밀을 출력한다"
    assert not re.search(r"^\s*(set -x|bash -x)", text, re.M)
    assert "StrictHostKeyChecking=no" not in text


def test_remote_script_brings_up_only_the_serving_stack():
    t = sh("cloud_remote.sh")
    assert "kafka" not in t.lower() and "spark" not in t.lower() and "airflow" not in t.lower()
    assert 'up -d --wait postgres' in t and "up -d --build --wait api dashboard" in t
    assert '--profile "*"' not in t and "COMPOSE_PROFILES" not in t
    assert "docker-compose.cloud.yml" in t


def test_remote_script_migrates_from_empty_db_in_a_readonly_container():
    t = sh("cloud_remote.sh")
    m = re.search(r"python scripts/migrate\.py", t)
    assert m and ':/work:ro' in t
    assert t.index("scripts/migrate.py") < t.index("db_bootstrap.py --api") < t.index("TRUNCATE") < t.index("up -d --build --wait api dashboard")


def test_remote_restore_is_atomic_and_keeps_migration_history():
    t = sh("cloud_remote.sh")
    assert re.search(r"gunzip -c .*psql -v ON_ERROR_STOP=1 -q -1 -U", t), "복원은 한 트랜잭션(-1)이어야 한다(중간 실패 시 반쪽 데이터 금지)"
    assert "NOT (schemaname = 'ops' AND tablename = 'schema_migrations')" in t
    assert "'dim','fact','mart','ops'" in t


def test_remote_script_never_prints_the_api_key():
    for line in sh("cloud_remote.sh").splitlines():
        if re.match(r"\s*(echo|printf)\b", line):
            assert "KEY" not in line and "PASSWORD" not in line, line
    assert "chmod 600 .env" in sh("cloud_remote.sh")


def test_dump_is_data_only_and_skips_migration_table():
    t = sh("cloud.sh")
    d = t[t.index("  dump)"):t.index("  up)")]
    assert "--data-only" in d and "--disable-triggers" in d
    assert "--exclude-table-data=ops.schema_migrations" in d
    for s in ("-n dim", "-n fact", "-n mart", "-n ops"):
        assert s in d
    assert "data/cloud/" in d


def test_ship_excludes_secrets_and_state_by_construction():
    t = sh("cloud.sh")
    u = t[t.index("  up)"):t.index("  tunnel)")]
    assert "git ls-files -z --cached --others --exclude-standard" in u, ".gitignore 를 따르는 목록만 보낸다"
    guard = re.search(r"grep -Eq '([^']+)'", u)
    assert guard, "목록 검증이 있어야 한다"
    g = re.compile(guard.group(1))
    for must_block in (".env", ".env.bak", "infra/terraform/terraform.tfstate", "infra/terraform/terraform.tfvars", "data/bronze/x.parquet", "a/.env.local"):
        assert g.search(must_block), must_block
    for must_allow in (".env.example", "scripts/cloud.sh", "infra/terraform/terraform.tfvars.example", "sql/001_base.sql"):
        assert not g.search(must_allow), must_allow


def test_tunnel_is_loopback_only_and_does_not_collide_with_mac_stack():
    t = sh("cloud.sh")
    tun = t[t.index("  tunnel)"):t.index("  ssh)")]
    assert re.search(r'-L "127\.0\.0\.1:\$\{LOCAL_API_PORT\}:127\.0\.0\.1:8010"', tun)
    assert re.search(r'-L "127\.0\.0\.1:\$\{LOCAL_DASH_PORT\}:127\.0\.0\.1:8501"', tun)
    ports = dict(re.findall(r"^(LOCAL_\w+_PORT)=(\d+)", t, re.M))
    assert ports == {"LOCAL_API_PORT": "18010", "LOCAL_DASH_PORT": "18501"}, "Mac 로컬 스택(8010·8501)과 겹치면 안 된다"
    assert "0.0.0.0" not in t


def test_destroy_verifies_resource_group_is_gone():
    t = sh("cloud.sh")
    d = t[t.index("  destroy)"):t.index("  *)")]
    assert "terraform -chdir" in d and "destroy" in d and "az group exists" in d and "exit 1" in d


# --- gitignore · tracked 검사 ----------------------------------------------------------------------------------

def test_gitignore_covers_terraform_state_and_vars_but_not_examples():
    g = (ROOT / ".gitignore").read_text(encoding="utf-8")
    for pat in ("*.tfstate", "*.tfstate.*", "*.tfvars", "!*.tfvars.example", "infra/**/.terraform/"):
        assert pat in g.splitlines(), pat
    assert "*.zip" in g.splitlines()


def test_tracked_check_blocks_terraform_state_and_allows_lock_and_example():
    bad = {p for p, _ in CC.forbidden_tracked([
        "infra/terraform/terraform.tfstate", "infra/terraform/terraform.tfstate.backup", "infra/terraform/terraform.tfvars",
        "infra/terraform/.terraform/providers/x/terraform-provider-azurerm", "prod.auto.tfvars"])}
    assert len(bad) == 5
    ok = CC.forbidden_tracked(["infra/terraform/.terraform.lock.hcl", "infra/terraform/terraform.tfvars.example", "infra/terraform/main.tf"])
    assert ok == []


# --- CI 도구 ----------------------------------------------------------------------------------------------------

def test_terraform_check_never_applies_or_plans():
    cmds = CT.check_commands("tf")
    flat = [" ".join(c) for c in cmds]
    assert any("fmt -check" in c for c in flat) and any("init -backend=false" in c for c in flat) and any(c.endswith("validate") for c in flat)
    for c in flat:
        assert not re.search(r"\b(apply|plan|destroy|import|login)\b", c), c


def test_terraform_pin_is_fail_closed(tmp_path, monkeypatch):
    missing = tmp_path / "terraform.pin"
    with pytest.raises(CT.PinError):
        CT.read_pin(missing)
    missing.write_text("version=\nsha256=UNPINNED\n", encoding="utf-8")
    with pytest.raises(CT.PinError):
        CT.read_pin(missing)
    missing.write_text(f"version=1.9.8\nsha256={'a' * 64}\n", encoding="utf-8")
    assert CT.read_pin(missing) == ("1.9.8", "a" * 64)
    monkeypatch.setattr(CT, "PIN", tmp_path / "none.pin")
    called = []
    monkeypatch.setattr(CT.subprocess, "run", lambda *a, **k: called.append(a))
    assert CT.cmd_check(dl=lambda url: b"x") == 1 and not called, "핀이 없으면 아무것도 실행하지 않고 실패한다"


def test_terraform_download_must_match_pin(tmp_path, monkeypatch):
    pin = tmp_path / "t.pin"
    pin.write_text(f"version=1.9.8\nsha256={'0' * 64}\n", encoding="utf-8")
    monkeypatch.setattr(CT, "PIN", pin)
    called = []
    monkeypatch.setattr(CT.subprocess, "run", lambda *a, **k: called.append(a))
    assert CT.cmd_check(dl=lambda url: b"tampered") == 1 and not called
    with pytest.raises(CT.PinError):
        CT.verify_sha256(b"tampered", "0" * 64)


def test_pin_terraform_parses_sums_and_writes_expected_format():
    sums = f"{'b' * 64}  terraform_1.9.8_linux_amd64.zip\n{'c' * 64}  terraform_1.9.8_darwin_arm64.zip\n"
    assert PT.parse_sums(sums, "1.9.8") == "b" * 64
    with pytest.raises(ValueError):
        PT.parse_sums(sums, "9.9.9")
    text = PT.pin_text("1.9.8", "b" * 64)
    assert "version=1.9.8" in text and f"sha256={'b' * 64}" in text
    assert PT.PIN == CT.PIN, "pin 스크립트가 쓰는 파일과 CI 가 읽는 파일이 같아야 한다"


def test_ci_terraform_job_uses_only_first_party_actions_and_the_pinned_script():
    doc = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    job = doc["jobs"]["terraform"]
    assert job["needs"] == "changes" and "infra" in job["if"]
    uses = {s["uses"].split("@")[0] for s in job["steps"] if "uses" in s}
    assert uses <= {"actions/checkout", "actions/setup-python"}, "hashicorp/setup-terraform 같은 서드파티 액션 금지"
    assert any("scripts/ci_terraform.py check" in s.get("run", "") for s in job["steps"])
    assert "infra" in doc["jobs"]["changes"]["outputs"]
