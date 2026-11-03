"""원본 응답 보관 (Raw). 파싱하기 전의 응답 본문을 그대로 gzip 으로 저장한다.

경로: data/raw/<source>/<YYYY>/<MM>/<DD>/<HHMMSS>_<ingest_id 앞 8자>.<xml|json|txt>.gz   (날짜·시각은 KST)
이유: 파서가 틀렸거나 스키마가 바뀌었을 때 Kafka 보관 기간과 무관하게 다시 처리할 수 있어야 한다 (원칙 '재처리 가능').
오류 응답도 저장한다 (장애 원인 조사). 응답 본문에는 키가 들어 있지 않다 (요청에만 있다).
"""
from __future__ import annotations

import gzip
import os
from datetime import datetime
from pathlib import Path

from urbanstream.contracts.common import KST


def _ext(body: bytes) -> str:
    head = body.lstrip()[:1]
    if head in (b"{", b"["):
        return "json"
    if head == b"<":
        return "xml"
    return "txt"


class RawStore:
    def __init__(self, root: Path, project_root: Path):
        self.root, self.project_root = Path(root), Path(project_root)

    def write(self, source: str, fetched_at: datetime, ingest_id: str, body: bytes) -> str:
        t = fetched_at.astimezone(KST)
        d = self.root / source / f"{t:%Y}" / f"{t:%m}" / f"{t:%d}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{t:%H%M%S}_{ingest_id[:8]}.{_ext(body)}.gz"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(gzip.compress(body, mtime=0))
        os.replace(tmp, path)
        try:
            return path.relative_to(self.project_root).as_posix()
        except ValueError:
            return path.as_posix()
