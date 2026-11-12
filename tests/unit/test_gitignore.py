"""data/ 안쪽 파일(수집 원본·Parquet·공공누리 데이터 파일)과 .env 가 git 에 올라가지 않는지 실제 git 으로 확인한다."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git 없음")


def _ignored(tmp_path, paths):
    (tmp_path / ".gitignore").write_text((ROOT / ".gitignore").read_text(encoding="utf-8"), encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    out = {}
    for p in paths:
        f = tmp_path / p
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("x")
        out[p] = subprocess.run(["git", "check-ignore", "-q", p], cwd=tmp_path).returncode == 0
    return out


def test_nested_data_files_and_env_are_ignored_but_gitkeep_is_not(tmp_path):
    r = _ignored(tmp_path, ["data/bronze/seoul_traffic/event_date=2026-11-03/p.parquet", "data/reference/LINK_VERTEX_2025.xlsx",
                            "data/gold_stage/traffic_speed/p.parquet", "data/raw/a.gz", ".env", "data/silver/.gitkeep"])
    assert [k for k, v in r.items() if not v] == ["data/silver/.gitkeep"]


def test_tableau_export_output_is_ignored(tmp_path):
    r = _ignored(tmp_path, ["tableau/export/link_speed_hourly.csv", "tableau/export/manifest.json", "tableau/README.md"])
    assert [k for k, v in r.items() if not v] == ["tableau/README.md"]


def test_delivery_zip_is_ignored(tmp_path):
    r = _ignored(tmp_path, ["urbanstream_full.zip", "backup/old.zip", "README.md"])
    assert [k for k, v in r.items() if not v] == ["README.md"]
