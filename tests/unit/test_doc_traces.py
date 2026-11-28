"""scripts/doc_traces.py: 작업 환경 흔적이 기준보다 늘면 실패하는 래칫 (STEP 5). 이 파일은 패턴을 담고 있어 검사에서 제외된다."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import doc_traces as DT  # noqa: E402


def test_patterns_match_the_audit_examples_and_not_ordinary_words():
    assert DT.count_text("개발 샌드박스는 PostgreSQL 16") == {"샌드박스": 1}
    assert DT.count_text("결과를 채팅에 붙여넣어 주세요") == {"채팅": 1, "붙여넣기 요청": 1}
    assert DT.count_text("사용자 지시: 승인") == {"사용자 지시": 1}
    assert DT.count_text("DEFERRED 14차 O장") == {"대화 회차": 1}
    assert DT.count_text("Claude 가 채운다, Blueprint H절") == {"AI 이름": 1, "Blueprint": 1}
    assert DT.count_text("API 사용자, 2차선 도로, 3차원, 차량 120대, 1차로") == {}


def test_compare_reports_growth_and_shrink():
    up, down = DT.compare({"a.md": 3, "new.md": 1, "b.md": 1}, {"a.md": 2, "b.md": 4, "gone.md": 1})
    assert up == ["a.md: 2 → 3", "new.md: 0 → 1"]
    assert down == ["b.md: 4 → 1", "gone.md: 1 → 0"]


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text, encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    return tmp_path


def test_check_and_update_on_a_small_repo(tmp_path, monkeypatch, capsys):
    root = _repo(tmp_path, {"docs/a.md": "샌드박스 샌드박스", "docs/b.md": "깨끗함", "data/x.md": "샌드박스", "scripts/doc_traces.py": "샌드박스"})
    base = root / "tests" / "fixtures" / "doc_traces_baseline.json"
    monkeypatch.setattr(DT, "ROOT", root)
    monkeypatch.setattr(DT, "BASELINE", base)
    assert DT.current(root) == {"docs/a.md": 2}, "data/ 와 검사 스크립트 자신은 세지 않는다"
    DT.save({"docs/a.md": 2})
    assert DT.main(["check"]) == 0
    (root / "docs" / "b.md").write_text("Blueprint 참고", encoding="utf-8")
    assert DT.main(["check"]) == 1 and "docs/b.md: 0 → 1" in capsys.readouterr().out
    assert DT.main(["update"]) == 1, "늘어난 파일이 있으면 기준을 올리지 않는다"
    (root / "docs" / "b.md").write_text("", encoding="utf-8")
    (root / "docs" / "a.md").write_text("샌드박스", encoding="utf-8")
    assert DT.main(["update"]) == 0
    assert json.loads(base.read_text(encoding="utf-8")) == {"docs/a.md": 1}


def test_repo_baseline_holds():
    """기준 파일은 3B-1 적용 상태에서 만든 값이다. 이 테스트가 실패하면 새 흔적이 들어온 것(report 로 위치 확인)."""
    up, _ = DT.compare(DT.current(), DT.load_baseline())
    assert up == []
