"""Bronze 설정과 표(토픽) 목록. pyspark 를 가져오지 않아서 호스트(Mac)에서도 테스트할 수 있다."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from urbanstream.contracts import SOURCES

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BronzeTable:
    name: str                      # data/bronze/<name>/ 폴더 이름
    topic: str
    source_system: Optional[str]   # None 이면 원문만 보존 (dlq, audit: 계약 스키마가 없다)


@dataclass(frozen=True)
class BronzeConfig:
    bootstrap: str = "kafka:19092"
    starting_offsets: str = "earliest"      # 새 쿼리가 처음 시작할 때만 적용. 재시작하면 체크포인트에서 이어간다
    max_offsets_per_trigger: Optional[int] = 50_000   # 한 번에 읽을 최대 메시지 수 (멈췄다 켜진 뒤 밀린 양을 나눠 읽는다)
    trigger_sec: int = 60
    bronze_root: str = "/data/bronze"
    checkpoint_root: str = "/data/checkpoints/bronze"
    config_dir: str = str(ROOT / "config")

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "BronzeConfig":
        e = os.environ if env is None else env
        mo = e.get("BRONZE_MAX_OFFSETS_PER_TRIGGER", "50000").strip()
        return cls(
            bootstrap=e.get("BRONZE_KAFKA_BOOTSTRAP", cls.bootstrap),
            starting_offsets=e.get("BRONZE_STARTING_OFFSETS", cls.starting_offsets),
            max_offsets_per_trigger=int(mo) if mo and mo != "0" else None,
            trigger_sec=int(e.get("BRONZE_TRIGGER_SEC", cls.trigger_sec)),
            bronze_root=e.get("BRONZE_ROOT", cls.bronze_root),
            checkpoint_root=e.get("BRONZE_CHECKPOINT_ROOT", cls.checkpoint_root),
            config_dir=e.get("BRONZE_CONFIG_DIR", cls.config_dir),
        )


def load_tables(config_dir: str | Path) -> list[BronzeTable]:
    """config/sources.yaml 의 소스별 토픽 + config/topics.yaml 의 dlq/audit 토픽 → Bronze 표 목록.

    토픽 이름은 설정 파일에서만 읽는다 (코드에 따로 적지 않는다). topics.yaml 에 없는 토픽을 가리키면 바로 실패한다.
    """
    d = Path(config_dir)
    sources = yaml.safe_load((d / "sources.yaml").read_text(encoding="utf-8"))["sources"]
    topics_cfg = yaml.safe_load((d / "topics.yaml").read_text(encoding="utf-8"))
    known = set(topics_cfg["topics"])

    tables: list[BronzeTable] = []
    for name, cfg in sources.items():
        if name not in SOURCES:
            raise ValueError(f"sources.yaml 의 소스 {name!r} 가 계약 레지스트리에 없습니다")
        if cfg["topic"] not in known:
            raise ValueError(f"소스 {name!r} 의 토픽 {cfg['topic']!r} 가 topics.yaml 에 없습니다")
        tables.append(BronzeTable(name=name, topic=cfg["topic"], source_system=name))
    tables.append(BronzeTable("dlq", topics_cfg["dlq_topic"], None))
    tables.append(BronzeTable("audit", topics_cfg["audit_topic"], None))
    for t in tables:
        if t.topic not in known:
            raise ValueError(f"토픽 {t.topic!r} 가 topics.yaml 에 없습니다")
    return tables
