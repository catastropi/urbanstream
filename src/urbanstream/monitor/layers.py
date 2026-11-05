"""Bronze/Silver 정체 판정. 새 parquet 파일의 수정 시각(mtime)을 본다 — 호스트에서 `:ro` 로 마운트한 폴더를 읽을 뿐 쓰지 않는다.

- 폴더가 없거나 파일이 하나도 없으면 'unknown' (정상으로 치지 않는다).
- 원천 소스가 ok 가 아니면 'unknown/source_not_ok' — 소스가 죽었는데 레이어까지 알림을 내면 알림이 폭주한다. 레이어 알림은 '소스는 멀쩡한데 안 쌓일 때'만.
- 파티션 폴더(dt=…/hour=…)는 이름 내림차순으로 내려가 최신 파일만 찾는다 (전체를 훑지 않는다).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def newest_mtime(root: Path) -> Optional[float]:
    if not root.is_dir():
        return None
    files: list[float] = []
    dirs: list[Path] = []
    try:
        with os.scandir(root) as it:
            for e in it:
                if e.name.startswith((".", "_")):
                    continue
                if e.is_dir(follow_symlinks=False):
                    dirs.append(Path(e.path))
                elif e.name.endswith(".parquet"):
                    files.append(e.stat().st_mtime)
    except OSError:
        return None
    for d in sorted(dirs, key=lambda p: p.name, reverse=True):
        got = newest_mtime(d)
        if got is not None:
            files.append(got)
            break
    return max(files) if files else None


@dataclass(frozen=True)
class LayerRow:
    layer: str
    table_name: str
    source_system: str
    status: str
    reason: str
    last_write_at: Optional[datetime]
    age_sec: Optional[float]


def layer_status(layer: str, table: str, source: str, mtime: Optional[float], now: datetime, *, source_ok: bool,
                 source_in_window: bool, threshold_sec: float, window_start: Optional[datetime] = None) -> LayerRow:
    """window_start: 수집 시간대가 오늘 열린 시각(종일 수집이면 None). 시간대가 막 열렸을 때 어제 마지막 파일을 기준으로 재면 첫 호출 직후 바로 '정체'가 되므로
    정체 판정은 max(마지막 파일 시각, 시간대 시작) 부터 잰다 (Mac 04:00 오경보, D-141). 표시용 age_sec 는 실제 마지막 파일 기준 그대로."""
    last = datetime.fromtimestamp(mtime, tz=timezone.utc) if mtime else None
    age = (now - last).total_seconds() if last else None
    ref = max(last, window_start) if (last and window_start) else last
    stale_age = (now - ref).total_seconds() if ref else None

    def row(st: str, why: str) -> LayerRow:
        return LayerRow(layer, table, source, st, why, last, age)

    if not source_in_window:
        return row("idle", "source_out_of_window")
    if last is None:
        return row("unknown", "no_files")
    if not source_ok:
        return row("unknown", "source_not_ok")
    if stale_age >= threshold_sec:
        return row("down", f"no_new_file_{int(stale_age)}s>={int(threshold_sec)}s")
    return row("ok", "")
