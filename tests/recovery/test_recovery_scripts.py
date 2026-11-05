"""복구 훈련 스크립트의 정적 점검(실제 고장 내기는 사람이 Mac 에서 한다: docs/runbooks/step8-recovery.md)."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "recovery_drill.sh"


def test_script_is_valid_bash_and_never_prints_secrets():
    assert subprocess.run(["bash", "-n", str(SCRIPT)]).returncode == 0
    text = SCRIPT.read_text(encoding="utf-8")
    assert "echo \"$ALERT_WEBHOOK_URL\"" not in text and "cat .env" not in text and "printenv" not in text
    assert "drill-invalid-key" in text                                  # 훈련용 가짜 키만 쓰고 .env 의 실제 키는 건드리지 않는다


def test_help_lists_every_subcommand():
    out = subprocess.run(["bash", str(SCRIPT), "help"], capture_output=True, text=True).stdout
    for sub in ("status", "webhook-test", "bad-key", "kafka-down", "kafka-up", "spark-down", "spark-up", "monitor-down", "monitor-up"):
        assert sub in out
