"""Silver 설정. pyspark 를 가져오지 않아서 호스트(Mac)에서도 테스트할 수 있다."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import yaml

from urbanstream.contracts import SOURCES

ROOT = Path(__file__).resolve().parents[1]
_DURATION = re.compile(r"^\d+ (seconds?|minutes?|hours?)$")
_WATERMARK_COLS = {"event_time", "fetched_at"}


@dataclass(frozen=True)
class SilverTable:
    source: str                 # 계약 소스 이름 = Bronze 폴더 이름
    name: str                   # data/silver/<name>/
    watermark_col: str
    watermark: str              # Spark 형식 ("10 minutes")
    dedup_keys: tuple[str, ...]

    @property
    def partition_col(self) -> str:
        return "observed_date" if self.source == "seoul_incident" else "event_date"


@dataclass(frozen=True)
class SilverConfig:
    bronze_root: str = "/data/bronze"
    silver_root: str = "/data/silver"
    checkpoint_root: str = "/data/checkpoints/silver"
    config_dir: str = str(ROOT / "config")
    trigger_sec: int = 60
    max_files_per_trigger: Optional[int] = 1000
    long_term_days: float = 7.0

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "SilverConfig":
        """경로만 환경변수(SILVER_*)로 바꿀 수 있다. 규칙 값(워터마크·기준 일수)은 config/silver.yaml 한 곳에서만 읽는다."""
        e = os.environ if env is None else env
        config_dir = e.get("SILVER_CONFIG_DIR", cls.config_dir)
        y = yaml.safe_load((Path(config_dir) / "silver.yaml").read_text(encoding="utf-8"))
        mf = y.get("max_files_per_trigger")
        return cls(
            bronze_root=e.get("SILVER_BRONZE_ROOT", cls.bronze_root),
            silver_root=e.get("SILVER_ROOT", cls.silver_root),
            checkpoint_root=e.get("SILVER_CHECKPOINT_ROOT", cls.checkpoint_root),
            config_dir=config_dir,
            trigger_sec=int(y["trigger_sec"]),
            max_files_per_trigger=int(mf) if mf else None,
            long_term_days=float(y["long_term_days"]),
        )


def verify_since(env: Optional[dict] = None, now: Optional[datetime] = None) -> Optional[datetime]:
    """silver_verify 가 비교할 시작 시각(UTC, naive). 둘 다 없으면 None = 전체 이력.

    SILVER_VERIFY_LOOKBACK_DAYS: 지금부터 N일 전.  SILVER_VERIFY_SINCE: 'YYYY-MM-DD'(한국 시간 0시).
    둘 다 있으면 더 늦은 쪽을 쓴다. 전체 이력을 읽으면 파일이 늘수록 오래 걸리고(2026-11-19 실측 75분),
    Bronze `_spark_metadata` 를 옮겨 복구한 표(D-255)는 복구 전 구간이 Bronze 쪽에서 보이지 않아 '초과'로 오탐한다.
    """
    e = os.environ if env is None else env
    cands: list[datetime] = []
    since = (e.get("SILVER_VERIFY_SINCE") or "").strip()
    if since:
        cands.append(datetime.strptime(since, "%Y-%m-%d") - timedelta(hours=9))
    days = (e.get("SILVER_VERIFY_LOOKBACK_DAYS") or "").strip()
    if days:
        n = float(days)
        if n <= 0:
            raise ValueError("SILVER_VERIFY_LOOKBACK_DAYS 는 0보다 커야 합니다")
        cands.append((now or datetime.utcnow()) - timedelta(days=n))
    return max(cands) if cands else None


def load_silver_tables(config_dir: str | Path) -> list[SilverTable]:
    y = yaml.safe_load((Path(config_dir) / "silver.yaml").read_text(encoding="utf-8"))
    tables: list[SilverTable] = []
    for source, t in y["tables"].items():
        if source not in SOURCES:
            raise ValueError(f"silver.yaml 의 소스 {source!r} 가 계약 레지스트리에 없습니다")
        if t["watermark_col"] not in _WATERMARK_COLS:
            raise ValueError(f"{source}: watermark_col 은 {sorted(_WATERMARK_COLS)} 중 하나여야 합니다")
        if not _DURATION.match(str(t["watermark"])):
            raise ValueError(f"{source}: watermark {t['watermark']!r} 형식이 잘못됨 (예: '10 minutes')")
        keys = tuple(t["dedup_keys"])
        if "dedup_key" not in keys:
            raise ValueError(f"{source}: dedup_keys 에 dedup_key 가 있어야 합니다")
        tables.append(SilverTable(source, t["name"], t["watermark_col"], str(t["watermark"]), keys))
    names = [t.name for t in tables]
    if len(set(names)) != len(names):
        raise ValueError("silver.yaml 의 표 이름이 겹칩니다")
    missing = set(SOURCES) - {t.source for t in tables}
    if missing:
        raise ValueError(f"silver.yaml 에 없는 소스: {sorted(missing)}")
    return tables
