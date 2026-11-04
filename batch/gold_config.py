"""Gold 스테이징 설정. 경로는 환경변수(GOLD_*)로만 바꾼다. DB 접속 정보는 여기 없다 (Spark 컨테이너에 비밀을 주지 않음, ADR-0008 ⑤)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class GoldConfig:
    silver_root: str = "/data/silver"
    bronze_root: str = "/data/bronze"
    stage_root: str = "/data/gold_stage"
    date_from: Optional[date] = None      # KST 날짜, 양 끝 포함. None 이면 제한 없음
    date_to: Optional[date] = None

    @classmethod
    def from_env(cls, date_from: Optional[date] = None, date_to: Optional[date] = None,
                 env: Optional[dict] = None) -> "GoldConfig":
        e = os.environ if env is None else env
        return cls(silver_root=e.get("GOLD_SILVER_ROOT", cls.silver_root),
                   bronze_root=e.get("GOLD_BRONZE_ROOT", cls.bronze_root),
                   stage_root=e.get("GOLD_STAGE_ROOT", cls.stage_root),
                   date_from=date_from, date_to=date_to)

    def __post_init__(self) -> None:
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from 이 date_to 보다 늦습니다")
