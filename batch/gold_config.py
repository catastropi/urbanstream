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
    rebuild_root: Optional[str] = "/data/silver_rebuild"   # 재처리 버전·포인터 (STEP 1). 폴더가 없으면 스트리밍 Silver 만 읽는다
    date_from: Optional[date] = None      # KST 날짜, 양 끝 포함. None 이면 제한 없음
    date_to: Optional[date] = None
    episode_lookback_days: int = 1        # 돌발 수명·audit 을 시작일보다 며칠 앞부터 읽을지 (gold_stage 머리말)
    files_since: Optional[float] = None   # 증분(D-278): 파일 수정 시각이 이 값 초과 ~ files_until 이하인 파일만 읽는다(epoch 초)
    files_until: Optional[float] = None

    @property
    def incremental(self) -> bool:
        return self.files_until is not None

    @classmethod
    def from_env(cls, date_from: Optional[date] = None, date_to: Optional[date] = None,
                 env: Optional[dict] = None, files_since: Optional[float] = None,
                 files_until: Optional[float] = None) -> "GoldConfig":
        e = os.environ if env is None else env
        return cls(silver_root=e.get("GOLD_SILVER_ROOT", cls.silver_root),
                   bronze_root=e.get("GOLD_BRONZE_ROOT", cls.bronze_root),
                   stage_root=e.get("GOLD_STAGE_ROOT", cls.stage_root),
                   rebuild_root=e.get("GOLD_SILVER_REBUILD_ROOT", cls.rebuild_root),
                   date_from=date_from, date_to=date_to,
                   episode_lookback_days=int(e.get("GOLD_EPISODE_LOOKBACK_DAYS", cls.episode_lookback_days)),
                   files_since=files_since, files_until=files_until)

    def __post_init__(self) -> None:
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from 이 date_to 보다 늦습니다")
        if self.episode_lookback_days < 1:
            raise ValueError("episode_lookback_days 는 1 이상이어야 합니다 (자정을 넘는 사라짐 판정에 전날이 필요하다)")
        if self.files_until is not None and not (self.date_from and self.date_to):
            raise ValueError("증분(files_until)에는 날짜 범위가 필요합니다 (어느 파티션 폴더를 볼지 정한다)")
        if self.files_since is not None and self.files_until is not None and self.files_since >= self.files_until:
            raise ValueError("files_since 가 files_until 보다 늦습니다")
