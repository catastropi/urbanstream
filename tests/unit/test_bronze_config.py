"""Bronze 설정/토픽 목록 (pyspark 없이 호스트에서 돈다)."""
from pathlib import Path

import pytest
import yaml

from streaming.bronze_config import BronzeConfig, load_tables

CONFIG = Path(__file__).resolve().parents[2] / "config"


def test_tables_cover_every_topic_exactly_once_and_dlq_audit_are_raw_only():
    tables = load_tables(CONFIG)
    topics_cfg = yaml.safe_load((CONFIG / "topics.yaml").read_text(encoding="utf-8"))
    assert sorted(t.topic for t in tables) == sorted(topics_cfg["topics"]), "모든 토픽이 Bronze 로 가야 합니다 (빠진 토픽 = 조용한 유실)"
    assert len({t.name for t in tables}) == len(tables)
    raw_only = {t.name for t in tables if t.source_system is None}
    assert raw_only == {"dlq", "audit"}
    by_name = {t.name: t for t in tables}
    assert by_name["dlq"].topic == topics_cfg["dlq_topic"] and by_name["audit"].topic == topics_cfg["audit_topic"]
    assert all(t.source_system == t.name for t in tables if t.source_system)


def test_topic_missing_from_topics_yaml_fails_fast(tmp_path):
    (tmp_path / "topics.yaml").write_text("topics: {a.v1: {partitions: 1}}\ndlq_topic: a.v1\naudit_topic: a.v1\n", encoding="utf-8")
    (tmp_path / "sources.yaml").write_text("sources:\n  bus_position: {topic: nope.v1}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="topics.yaml 에 없습니다"):
        load_tables(tmp_path)


def test_unknown_source_fails_fast(tmp_path):
    (tmp_path / "topics.yaml").write_text("topics: {a.v1: {partitions: 1}}\ndlq_topic: a.v1\naudit_topic: a.v1\n", encoding="utf-8")
    (tmp_path / "sources.yaml").write_text("sources:\n  mystery: {topic: a.v1}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="계약 레지스트리에 없습니다"):
        load_tables(tmp_path)


def test_config_defaults_and_env_overrides():
    d = BronzeConfig.from_env({})
    assert (d.bootstrap, d.starting_offsets, d.max_offsets_per_trigger, d.trigger_sec) == ("kafka:19092", "earliest", 50_000, 60)
    c = BronzeConfig.from_env({"BRONZE_TRIGGER_SEC": "300", "BRONZE_MAX_OFFSETS_PER_TRIGGER": "0", "BRONZE_STARTING_OFFSETS": "latest"})
    assert (c.trigger_sec, c.max_offsets_per_trigger, c.starting_offsets) == (300, None, "latest")   # 0 = 제한 없음


def test_compose_environment_matches_config_defaults():
    """compose 가 컨테이너에 주는 기본값과 코드의 기본값이 어긋나지 않게."""
    import re
    compose = yaml.safe_load((CONFIG.parent / "docker-compose.yml").read_text(encoding="utf-8"))
    env = compose["services"]["spark-bronze"]["environment"]
    default = lambda v: re.search(r":-([^}]+)\}", v).group(1)  # noqa: E731
    cfg = BronzeConfig.from_env({k: (default(v) if "${" in v else v) for k, v in env.items()})
    base = BronzeConfig()
    assert (cfg.bootstrap, cfg.trigger_sec, cfg.max_offsets_per_trigger, cfg.starting_offsets) == \
           (base.bootstrap, base.trigger_sec, base.max_offsets_per_trigger, base.starting_offsets)


def test_script_entry_modules_use_absolute_imports():
    """spark-submit 은 파일을 스크립트로 실행한다(패키지가 아님) → 상대 import 가 있으면 시작하자마자 실패한다 (2026-11-04 silver_verify 에서 실제로 발생)."""
    import re
    root = Path(__file__).resolve().parents[2] / "streaming"
    for name in ("bronze_writer", "silver_writer", "verify_bronze", "silver_verify", "profile_bronze"):
        text = (root / f"{name}.py").read_text(encoding="utf-8")
        assert not re.search(r"^\s*from \.", text, re.M), f"{name}.py: 스크립트로 실행되는 파일에 상대 import 가 있습니다"
