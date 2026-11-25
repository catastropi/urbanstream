"""재처리 포인터 (STEP 1, streaming/silver_pointer.py). pyspark 없이 돈다. 설계안 5-1 의 U1~U5."""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone

import pytest

from streaming import silver_pointer as P

T = "silver_traffic_speed"
D1, D2 = date(2026, 11, 10), date(2026, 11, 11)


def fake_build(root, version, dates, *, status="ok", table=T, part="event_date"):
    vd = P.version_dir(root, table, version)
    vd.mkdir(parents=True, exist_ok=True)
    for d, n in dates.items():
        if n:
            (vd / f"{part}={d}").mkdir(exist_ok=True)
            (vd / f"{part}={d}" / "part-0.parquet").write_bytes(b"x")
    P.write_json_atomic(vd / P.BUILD_FILE, {"status": status, "partition_col": part, "dates": dates, "version": version})
    return vd


def test_publish_is_one_atomic_replace_and_a_leftover_temp_file_is_ignored(tmp_path):
    fake_build(tmp_path, "r0001t1", {"2026-11-10": 3})
    assert P.publish(tmp_path, T, "r0001t1", [D1], reprocess_id=1) == {"2026-11-10": None}
    assert P.active_dates(tmp_path, T) == {"2026-11-10": "r0001t1"}
    # 게시 도중 중단: 임시 파일만 남은 상태 → 읽는 쪽은 예전(=마지막으로 이름이 바뀐) 포인터를 본다
    (tmp_path / P.ACTIVE_DIR / f".{T}.json.999.tmp").write_text('{"dates": {"2026-11-10": "r9999t1"}}')
    assert P.active_dates(tmp_path, T) == {"2026-11-10": "r0001t1"}
    assert not list((tmp_path / P.ACTIVE_DIR).glob(f".{T}.json.{os.getpid()}.tmp"))


def test_no_pointer_file_means_streaming_root_for_every_date(tmp_path):
    assert P.active_dates(tmp_path, T) == {} and P.active_dates(None, T) == {}
    assert P.snapshot_sha(P.snapshot(tmp_path, [T])) == "none"


def test_broken_pointer_or_missing_version_fails_loudly(tmp_path):
    (tmp_path / P.ACTIVE_DIR).mkdir()
    P.pointer_path(tmp_path, T).write_text("{not json")
    with pytest.raises(P.PointerError):
        P.active_dates(tmp_path, T)
    P.pointer_path(tmp_path, T).write_text(json.dumps({"dates": {"2026-11-10": "r0404t1"}}))
    with pytest.raises(P.PointerError, match="빌드 기록이 없습니다"):
        P.version_inputs(tmp_path, T, P.active_dates(tmp_path, T))
    fake_build(tmp_path, "r0404t1", {"2026-11-10": 5})
    (P.version_dir(tmp_path, T, "r0404t1") / "event_date=2026-11-10" / "part-0.parquet").unlink()
    (P.version_dir(tmp_path, T, "r0404t1") / "event_date=2026-11-10").rmdir()
    with pytest.raises(P.PointerError, match="폴더가 없습니다"):
        P.version_inputs(tmp_path, T, P.active_dates(tmp_path, T))


def test_only_ok_builds_can_be_published_and_only_their_dates(tmp_path):
    fake_build(tmp_path, "r0002t1", {"2026-11-10": 3}, status="shrink_blocked")
    with pytest.raises(P.PointerError, match="ok 가 아니라"):
        P.publish(tmp_path, T, "r0002t1", [D1])
    fake_build(tmp_path, "r0003t1", {"2026-11-10": 3})
    with pytest.raises(P.PointerError, match="빌드에 없는 날짜"):
        P.publish(tmp_path, T, "r0003t1", [D1, D2])
    assert P.active_dates(tmp_path, T) == {}


def test_republishing_the_same_version_is_idempotent_and_keeps_previous(tmp_path):
    fake_build(tmp_path, "r0001t1", {"2026-11-10": 3})
    fake_build(tmp_path, "r0002t1", {"2026-11-10": 3})
    P.publish(tmp_path, T, "r0001t1", [D1], reprocess_id=1)
    assert P.publish(tmp_path, T, "r0002t1", [D1], reprocess_id=2) == {"2026-11-10": "r0001t1"}
    assert P.publish(tmp_path, T, "r0002t1", [D1], reprocess_id=2) == {"2026-11-10": "r0001t1"}     # 재시도
    assert len(P.load_pointer(tmp_path, T)["history"]) == 2


def test_rollback_restores_previous_and_drops_first_time_dates(tmp_path):
    fake_build(tmp_path, "r0001t1", {"2026-11-10": 3})
    fake_build(tmp_path, "r0002t1", {"2026-11-10": 3, "2026-11-11": 2})
    P.publish(tmp_path, T, "r0001t1", [D1], reprocess_id=1)
    prev = P.publish(tmp_path, T, "r0002t1", [D1, D2], reprocess_id=2)
    assert prev == {"2026-11-10": "r0001t1", "2026-11-11": None}
    h = P.find_publish(tmp_path, T, 2)
    assert h["previous"] == prev and h["version"] == "r0002t1"
    P.rollback(tmp_path, T, prev, expect_version="r0002t1", reprocess_id=2)
    assert P.active_dates(tmp_path, T) == {"2026-11-10": "r0001t1"}         # 11-11 은 다시 스트리밍 루트


def test_rollback_refuses_dates_that_a_later_run_republished_unless_forced(tmp_path):
    for v in ("r0001t1", "r0002t1"):
        fake_build(tmp_path, v, {"2026-11-10": 3})
    prev1 = P.publish(tmp_path, T, "r0001t1", [D1], reprocess_id=1)
    P.publish(tmp_path, T, "r0002t1", [D1], reprocess_id=2)
    with pytest.raises(P.PointerError, match="다른 재처리"):
        P.rollback(tmp_path, T, prev1, expect_version="r0001t1")
    P.rollback(tmp_path, T, prev1, expect_version="r0001t1", force=True)
    assert P.active_dates(tmp_path, T) == {}


def test_prune_keeps_pointed_recent_and_young_versions(tmp_path):
    now = time.time()
    for i, v in enumerate(["r0001t1", "r0002t1", "r0003t1", "r0004t1", "r0005t1"]):
        vd = fake_build(tmp_path, v, {"2026-11-10": 1})
        old = now - (200 - i) * 3600                                          # r0001 이 가장 오래됨, 모두 72시간 넘음
        os.utime(vd, (old, old))
    P.publish(tmp_path, T, "r0001t1", [D1])
    os.utime(P.version_dir(tmp_path, T, "r0001t1"), (now - 300 * 3600,) * 2)
    young = fake_build(tmp_path, "r0006t1", {"2026-11-10": 1})                 # 방금 만든 것
    # 최근 2개 = r0006(방금), r0005. 포인터 = r0001. 나머지(r0002~r0004)만 지운다
    assert sorted(P.prune(tmp_path, T, now=now, dry_run=True)) == ["r0002t1", "r0003t1", "r0004t1"]
    removed = P.prune(tmp_path, T, now=now)
    assert sorted(removed) == ["r0002t1", "r0003t1", "r0004t1"] and young.exists()
    assert P.prune(tmp_path, T, now=now, keep_recent=0) == ["r0005t1"]          # 72시간 안의 r0006 은 keep_recent=0 이어도 남는다
    assert P.version_dir(tmp_path, T, "r0001t1").exists()                    # 포인터가 가리킴


def test_date_guard_rejects_today_and_yesterday_kst_unless_allowed():
    now = datetime(2026, 11, 22, 1, 0, tzinfo=timezone.utc)                   # KST 11-22 10:00
    assert P.check_dates(date(2026, 11, 19), date(2026, 11, 20), now=now) == [date(2026, 11, 19), date(2026, 11, 20)]
    for d in (date(2026, 11, 21), date(2026, 11, 22)):
        with pytest.raises(P.PointerError, match="너무 최근"):
            P.check_dates(d, d, now=now)
        assert P.check_dates(d, d, now=now, allow_recent=True) == [d]
    with pytest.raises(P.PointerError, match="31일"):
        P.check_dates(date(2026, 9, 1), date(2026, 10, 15), now=now)
    with pytest.raises(P.PointerError):
        P.check_dates(D2, D1, now=now)


def test_names_are_checked_so_paths_cannot_escape(tmp_path):
    for bad in ("../x", "", "a/b", ".."):
        with pytest.raises(P.PointerError):
            P.version_dir(tmp_path, T, bad)
    with pytest.raises(P.PointerError):
        P.pointer_path(tmp_path, "../../etc")
    assert P.version_name(17, 2) == "r0017t2" and P.version_name(None).startswith("m")
