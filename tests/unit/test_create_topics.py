"""scripts/create_topics.py 의 설정 비교. 토픽은 만들 때 넣은 설정을 계속 쓰므로, 파일을 고친 뒤 실제 값과 비교하는 기능이 필요하다(D-256)."""
import importlib.util
import sys
from pathlib import Path

from collector.config import TopicSpec, load_topics

ROOT = Path(__file__).resolve().parents[2]


def _mod():
    spec = importlib.util.spec_from_file_location("create_topics_mod", ROOT / "scripts" / "create_topics.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["create_topics_mod"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_data_topics_keep_seven_days_so_a_long_shutdown_does_not_outlive_kafka():
    topics = load_topics().topics
    for name, t in topics.items():
        assert t.retention_ms >= 7 * 24 * 3600 * 1000, f"{name}: 보존이 7일보다 짧다"


def test_drift_is_empty_when_broker_matches_the_file():
    m = _mod()
    specs = {"a": TopicSpec("a", 1, 1, 604800000)}
    assert m.config_drift(specs, {"a": {"retention.ms": "604800000", "cleanup.policy": "delete"}}) == []


def test_drift_reports_old_retention_left_from_topic_creation():
    m = _mod()
    specs = {"a": TopicSpec("a", 1, 1, 604800000), "b": TopicSpec("b", 1, 1, 604800000)}
    drift = m.config_drift(specs, {"a": {"retention.ms": "259200000"}, "b": {"retention.ms": "604800000"}})
    assert drift == [("a", "retention.ms", "604800000", "259200000")]


def test_drift_marks_missing_topics_with_none():
    m = _mod()
    specs = {"a": TopicSpec("a", 1, 1, 604800000)}
    assert m.config_drift(specs, {}) == [("a", "retention.ms", "604800000", None)]


def test_check_and_apply_are_mutually_exclusive(capsys):
    m = _mod()
    import pytest
    with pytest.raises(SystemExit):
        m.main(["--check", "--apply-config"])
