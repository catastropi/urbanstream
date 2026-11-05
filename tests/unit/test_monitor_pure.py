"""monitor 순수 계산 시험: 상태 판정·공백·분류·레이어·감사 파싱. 빈 경우(데이터 없음, 창 밖, 증거 없음)를 포함한다."""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from urbanstream.monitor import audit, gaps as G, health as H, layers as L
from urbanstream.monitor.classify import make_classifier
from urbanstream.monitor.config import MonitorConfig, SourceCfg

UTC = timezone.utc
NOW = datetime(2026, 11, 5, 3, 0, tzinfo=UTC)          # 12:00 KST
CFG = MonitorConfig(freshness={"warn_factor": 2.5, "down_factor": 5.0, "fail_rate_degraded": 0.5, "fail_rate_min_calls": 4,
                               "budget_degraded_ratio": 0.8}, expected_gap_override_sec={"kma_ncst": 4500})
TRAFFIC = SourceCfg("seoul_traffic", 450, 4, (0, 1440), 1000)          # 기대 간격 112.5초
BUS = SourceCfg("bus_position", 90, 1, (240, 1440), 1000)              # 04:00~24:00 KST


def S(sec_ago_ok=10, **kw):
    ok = NOW - timedelta(seconds=sec_ago_ok) if sec_ago_ok is not None else None
    base = dict(last_ok_at=ok, last_call_at=ok, last_outcome="ok", calls_1h=30, fail_1h=0)
    base.update(kw)
    return H.Summary(**base)


def test_status_thresholds_use_interval_not_divided_by_targets():
    # 링크 4개가 거의 동시에 불리므로 호출 사이 빈 시간은 interval(450초) 이다. 350초 공백은 정상이다 (Mac 에서 ÷4 로 계산해 오경보가 반복됐다, D-140).
    assert H.source_status(TRAFFIC, S(350), NOW, CFG).status == "ok"
    assert H.source_status(TRAFFIC, S(1100), NOW, CFG).status == "ok"            # 2.5 × 450 = 1125 미만
    assert H.source_status(TRAFFIC, S(1200), NOW, CFG).status == "degraded"
    assert H.source_status(TRAFFIC, S(2300), NOW, CFG).status == "down"          # 5 × 450 = 2250


def test_no_data_is_unknown_not_ok():
    r = H.source_status(TRAFFIC, H.Summary(), NOW, CFG)
    assert (r.status, r.reason) == ("unknown", "no_data")


def test_out_of_window_is_idle_even_if_old():
    kst3 = datetime(2026, 11, 4, 18, 0, tzinfo=UTC)                              # 03:00 KST, bus 창(04:00~) 밖
    assert H.source_status(BUS, S(5 * 3600), kst3, CFG).status == "idle"


def test_permanent_failure_is_down_immediately():
    assert H.source_status(TRAFFIC, S(5, last_outcome="permanent_failure"), NOW, CFG).status == "down"


def test_grace_after_gap_is_unknown():
    assert H.source_status(TRAFFIC, S(900), NOW, CFG, in_grace=True).status == "unknown"


def test_fail_rate_needs_min_calls_and_budget_degraded():
    assert H.source_status(TRAFFIC, S(5, calls_1h=3, fail_1h=3), NOW, CFG).status == "ok"          # 표본 부족
    assert H.source_status(TRAFFIC, S(5, calls_1h=10, fail_1h=6), NOW, CFG).status == "degraded"
    assert H.source_status(TRAFFIC, S(5, budget_used=850, budget_limit=1000), NOW, CFG).status == "degraded"
    assert H.source_status(TRAFFIC, S(5, last_outcome="skipped", last_reason="budget_exhausted"), NOW, CFG).reason == "budget_exhausted"


def test_kma_override_gap():
    kma = SourceCfg("kma_ncst", 600, 1, (0, 1440), 10000)
    assert H.source_status(kma, S(3000), NOW, CFG).status == "ok"                # 600초 기준이었다면 down 이었을 값
    assert H.source_status(kma, S(25000), NOW, CFG).status == "down"


def test_window_just_opened_does_not_use_yesterdays_success():
    opened = datetime(2026, 11, 4, 19, 1, tzinfo=UTC)                            # 04:01 KST (창 시작 1분 뒤)
    r = H.source_status(BUS, S(8 * 3600).__class__(last_ok_at=opened - timedelta(hours=8), last_call_at=opened - timedelta(hours=8),
                                                   last_outcome="ok", calls_1h=0), opened, CFG)
    assert r.status == "ok" and r.age_sec <= 60


def test_clock_behind_host_stamp_gives_age_zero_not_negative():
    r = H.source_status(TRAFFIC, S(-30), NOW, CFG)
    assert r.age_sec == 0 and r.status == "ok"


# ---- 공백 ----
def T(minutes):
    return NOW + timedelta(minutes=minutes)


def test_host_stall_from_resume_and_idempotent_start():
    g1 = G.host_stalls([(T(0), 3600.4)], 120)
    g2 = G.host_stalls([(T(0), 3600.4)], 120)
    assert g1 == g2 and g1[0].start.microsecond == 0 and g1[0].duration_sec in (3600, 3601)
    assert G.host_stalls([(T(0), 60)], 120) == [] and G.host_stalls([(T(0), None)], 120) == []


def test_silences_and_blackout_excludes_stall_overlap():
    times = [T(0), T(1), T(2), T(60), T(61)]
    assert G.silences(times, 600) == [(T(2), T(60))]
    assert G.silences([], 600) == [] and G.silences([T(0)], 600) == []
    stall = G.host_stalls([(T(60), 3500)], 120)
    assert G.blackouts(times, stall, 600, 600) == []
    b = G.blackouts(times, [], 600, 600)
    assert len(b) == 1 and b[0].kind == "collector_blackout"


def test_ongoing_blackout():
    assert G.ongoing_blackout(None, NOW, 600) is None                         # 기록 없음 = 모름 (침묵 아님)
    assert G.ongoing_blackout(NOW - timedelta(seconds=599), NOW, 600) is None
    assert G.ongoing_blackout(NOW - timedelta(seconds=601), NOW, 600) == pytest.approx(601)


# ---- 분류 ----
def fail_row(end, level="error", key="fail:d", start=None):
    meta = {"ended_at": end.isoformat()}
    if start:
        meta["started_at"] = start.isoformat()
    return {"level": level, "dedup_key": key, "meta": meta, "created_at": end}


def test_classify_downgrades_only_with_stall_evidence():
    stall = G.Gap("host_stall", T(-120), T(-10), "")
    black = G.Gap("collector_blackout", T(-120), T(-10), "")
    row = fail_row(T(0), start=T(-5))
    assert make_classifier([stall], margin_sec=600)(row).level == "warning"
    d = make_classifier([black], margin_sec=600)(row)
    assert d.level == "error" and d.dedup_key.startswith("collector_blackout_fail:")            # 묶기만, 격하 없음
    assert make_classifier([], margin_sec=600)(row).level == "error"                          # 증거 없음 → 그대로(fail-safe)
    far = fail_row(T(600), start=T(590))
    assert make_classifier([stall], margin_sec=600)(far).level == "error"                     # 잠자기와 무관한 시각


def test_classify_without_meta_times_uses_created_at():
    stall = G.Gap("host_stall", T(-60), T(-1), "")
    row = {"level": "error", "dedup_key": "k", "meta": {}, "created_at": T(0)}
    assert make_classifier([stall], margin_sec=600)(row).level == "warning"


# ---- 레이어 ----
def test_newest_mtime_empty_missing_and_partitions(tmp_path):
    assert L.newest_mtime(tmp_path / "nope") is None
    t = tmp_path / "tbl"
    t.mkdir()
    assert L.newest_mtime(t) is None                                          # 빈 폴더
    (t / "dt=2026-11-04").mkdir(); (t / "dt=2026-11-05").mkdir(); (t / "_spark_metadata").mkdir()
    (t / "dt=2026-11-04" / "a.parquet").write_text("x")
    (t / "_spark_metadata" / "9.parquet").write_text("x")
    os.utime(t / "dt=2026-11-04" / "a.parquet", (1000, 1000))
    assert L.newest_mtime(t) == 1000                                          # 최신 파티션이 비어 있으면 그 전 파티션을 본다, _ 폴더는 무시
    (t / "dt=2026-11-05" / "b.parquet").write_text("x"); (t / "dt=2026-11-05" / "b.parquet.crc").write_text("x")
    os.utime(t / "dt=2026-11-05" / "b.parquet", (2000, 2000))
    assert L.newest_mtime(t) == 2000


def test_layer_stale_is_measured_from_window_start_not_from_yesterdays_last_file():
    # Mac 04:00 오경보: 시간대가 막 열렸고 소스는 ok, 마지막 파일은 4시간 전(어제). 시간대 시작부터 재므로 아직 정상이다 (D-141).
    ws = NOW - timedelta(seconds=120)
    old = NOW.timestamp() - 14500
    kw = dict(source_ok=True, source_in_window=True, threshold_sec=1050)
    assert L.layer_status("bronze", "t", "s", old, NOW, window_start=ws, **kw).status == "ok"
    assert L.layer_status("bronze", "t", "s", old, NOW, **kw).status == "down"               # 시간대 정보가 없으면 예전 동작
    # 시간대가 열린 뒤 임계값이 지나도 파일이 안 생기면 진짜 정체다
    ws2 = NOW - timedelta(seconds=1100)
    assert L.layer_status("bronze", "t", "s", old, NOW, window_start=ws2, **kw).status == "down"
    # 시간대 안에서 새 파일이 있으면 파일 시각이 기준이다
    assert L.layer_status("bronze", "t", "s", NOW.timestamp() - 1200, NOW, window_start=NOW - timedelta(hours=5), **kw).status == "down"


def test_layer_status_rules():
    m = NOW.timestamp() - 5000
    kw = dict(source_in_window=True, threshold_sec=1000)
    assert L.layer_status("bronze", "t", "s", m, NOW, source_ok=True, **kw).status == "down"
    assert L.layer_status("bronze", "t", "s", m, NOW, source_ok=False, **kw).reason == "source_not_ok"   # 소스가 죽었으면 레이어는 알리지 않는다
    assert L.layer_status("bronze", "t", "s", None, NOW, source_ok=True, **kw).status == "unknown"       # 파일 없음 ≠ 정상
    assert L.layer_status("bronze", "t", "s", NOW.timestamp() - 10, NOW, source_ok=True, **kw).status == "ok"
    assert L.layer_status("bronze", "t", "s", m, NOW, source_ok=True, source_in_window=False, threshold_sec=1000).status == "idle"


# ---- 감사 파싱 ----
def msg(**kw):
    d = {"event": "call", "recorded_at": "2026-11-05T12:00:00.123+09:00", "source_system": "bus_position", "target": "bus_position:1",
         "final": True, "outcome": "ok", "attempt": 1, "latency_ms": 250, "n_records": 13, "breaker": {"state": "closed"}}
    d.update(kw)
    return json.dumps(d).encode()


def test_audit_parse_fields_and_garbage():
    r = dict(zip(audit.COLUMNS, audit.parse(msg(), 0, 7)))
    assert r["kafka_offset"] == 7 and r["outcome"] == "ok" and r["breaker_state"] == "closed" and r["n_records"] == 13
    assert r["recorded_at"].utcoffset() == timedelta(hours=9)
    assert audit.parse(b"not json", 0, 1) is None and audit.parse(b"[1]", 0, 1) is None
    assert audit.parse(msg(recorded_at="garbage"), 0, 1) is None
    h = dict(zip(audit.COLUMNS, audit.parse(msg(event="host_resume", source_system="collector", slept_sec=3600, final=None), 0, 8)))
    assert h["event"] == "host_resume" and h["slept_sec"] == 3600.0
    r2 = dict(zip(audit.COLUMNS, audit.parse(msg(latency_ms="x", attempt=True, new_field=1), 0, 9)))
    assert r2["latency_ms"] is None and r2["attempt"] is None
