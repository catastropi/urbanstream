"""meta→표시 문구, API 응답→화면 구조. null 을 0/정상으로 만들지 않는 것이 핵심."""
from datetime import timedelta

import dashboard_support as D
from dashboard import freshness as F
from dashboard import shape as S

NOW = D.NOW


def m(**kw):
    base = {"as_of": D.z(NOW - timedelta(minutes=45)), "age_sec": 2700.0, "stale": False, "loaded_at": D.z(NOW - timedelta(minutes=40)), "notes": []}
    base.update(kw)
    return base


def test_levels_and_text_not_color_only():
    ok = F.describe(m(), now=NOW)
    assert ok.level == "ok" and "●" in ok.badge and "최신" in ok.badge
    assert "기준 11-05 14:35 KST" in ok.detail and "경과 45분" in ok.detail and "적재 11-05 14:40 KST" in ok.detail
    st = F.describe(m(stale=True), now=NOW)
    assert st.level == "stale" and "오래됨" in st.text
    assert F.describe(m(stale=None), now=NOW).level == "unknown"          # 서버가 판정 안 하면 우리도 안 한다
    nd = F.describe(m(as_of=None, age_sec=None, stale=None), now=NOW)
    assert nd.level == "nodata" and "데이터 없음" in nd.text
    assert F.describe(m(as_of="이상한 값"), now=NOW).level == "unknown"
    assert F.describe(None, now=NOW).level == "nodata"


def test_age_is_recomputed_from_as_of_not_taken_from_stale_age_sec():
    later = NOW + timedelta(minutes=14)                    # 캐시된 값을 14분 뒤에 그린다
    d = F.describe(m(age_sec=2700.0), now=later)
    assert "경과 59분" in d.detail and "45분" not in d.detail


def test_refresh_failure_is_shown_with_the_time_of_the_shown_value():
    d = F.describe(m(), now=NOW, fetched_at=NOW - timedelta(minutes=9), error="server / HTTP 503")
    assert "갱신 실패" in d.detail and "11-05 15:11 KST" in d.detail and "server / HTTP 503" in d.detail


def test_notes_known_unknown_and_empty():
    out = F.explain_notes(["baseline_provisional", "brand_new_code"])
    assert "잠정" in out[0] and out[1] == "API 안내 코드: brand_new_code"
    assert F.explain_notes(None) == [] and "실시간이 아닙니다" in F.GOLD_LAG_NOTICE and "80분" in F.GOLD_LAG_NOTICE


def test_null_is_never_zero_or_normal():
    assert S.fmt_num(None) == "—" and S.fmt_num(0) == "0.0" and S.fmt_num(True) == "—" and S.fmt_num("x") == "—"
    assert S.fmt_int(None) == "—" and S.fmt_int(0) == "0"
    assert "원활" not in S.level_text(None) and "판정 불가" in S.level_text(None)
    assert S.level_text("severe").startswith("◆") and S.level_text("brand_new") == "? brand_new"
    assert S.level_color("brand_new") == S.GRAY and S.level_color(None) == S.GRAY
    assert S.status_text("ok").startswith("●") and S.status_text("future") == "? future" and S.status_text(None) == "—"


def test_traffic_rows_keep_null_as_dash_and_mark_provisional():
    rows = S.traffic_rows(D.payload_for("/v1/traffic/current", {})["data"])
    r = rows[0]
    assert r["평소 중앙값(km/h)"] == "—" and r["평소 대비"] == "—" and "판정 불가" in r["단계"] and r["잠정"] == "예" and r["기준선 표본"] == "8"
    full = S.traffic_rows([{"link_id": "L", "event_time": D.z(NOW), "speed_kmh": 20, "baseline_p50": 40, "ratio_to_baseline": 0.5, "level": "heavy",
                            "threshold_n_obs": 99, "threshold_provisional": False}])[0]
    assert full["평소 대비"] == "50%" and full["단계"] == "▲ 혼잡" and full["잠정"] == "아니오"


def test_level_counts_keep_unknown_separate():
    c = S.level_counts([{"level": "heavy"}, {"level": None}, {"level": "weird"}, {"level": "normal"}])
    assert c["heavy"] == 1 and c["normal"] == 1 and c["unknown_level"] == 2


def test_link_paths_lonlat_order_and_skips_without_geometry():
    links = D.payload_for("/v1/links", {"geometry": "true"})["data"] + [{"link_id": "X", "geometry": None}, {"link_id": "Y", "geometry": {"type": "Point", "coordinates": [1, 2]}}]
    paths, skipped = S.link_paths(links, {"1220003800": "severe"})
    assert len(paths) == 4 and skipped == 2
    assert paths[0]["path"][0] == [126.97, 37.55] and paths[0]["color"] == list(S.LEVELS["severe"][1])      # [경도, 위도]
    assert paths[1]["color"] == list(S.GRAY) and "판정 불가" in paths[1]["단계"]


def test_points_skip_missing_coordinates_and_hide_vehicle_identity():
    inc, no = S.incident_points(D.payload_for("/v1/incidents/active", {})["data"])
    assert len(inc) == 2 and no == 1
    bus, bno = S.bus_points(D.payload_for("/v1/bus/positions", {})["data"])
    assert len(bus) == 1 and bno == 1
    blob = str(bus)
    assert "서울74사1234" not in blob and "V1" not in blob


def test_speed_series_keeps_null_converts_to_naive_kst_and_sorts():
    pts = [{"event_time": "2026-11-04T15:30:00Z", "speed_kmh": None, "dq_flags": ["x"]}, {"event_time": "2026-11-04T14:30:00Z", "speed_kmh": 0, "dq_flags": []}]
    s = S.speed_series(pts)
    assert [r["speed"] for r in s] == [0, None]                 # 0 은 0, 결측은 None
    assert s[0]["t"].hour == 23 and s[1]["t"].hour == 0 and s[1]["t"].day == 5 and s[0]["t"].tzinfo is None
    assert s[1]["dq"] == 1 and s[0]["day_type"] == "weekday"


def test_attach_baseline_matches_day_type_and_hour_or_reports_none():
    series = S.speed_series(D.payload_for("/v1/traffic/speeds", {})["data"])
    out, hit = S.attach_baseline(series, D.payload_for("/v1/traffic/thresholds", {"link_id": "L"})["data"])
    assert hit and out[0]["p50"] == 32.0 and out[0]["p10"] == 20.0 and out[0]["p90"] == 40.0
    none_out, none_hit = S.attach_baseline(series, [{"day_type": "weekday", "kst_hour": h, "speed_p50": None} for h in range(24)])
    assert not none_hit and "p50" not in none_out[0]
    partial, ph = S.attach_baseline(series, [{"day_type": "sun", "kst_hour": 3, "speed_p50": 30.0, "speed_p10": 1, "speed_p25": 2, "speed_p90": 5}])
    assert ph is False and partial[0]["p50"] is None


def test_kst_native_fields_are_not_converted_again():
    rows = S.drift_series([{"event_date": "2026-11-04", "kst_hour": 0, "drift_p50_sec": 1, "drift_p90_sec": 2, "n_approaches": 3, "station_id": "S"},
                           {"event_date": None, "kst_hour": 1}])
    assert rows == [{"일시(KST)": "2026-11-04 00시", "p50(초)": 1, "p90(초)": 2, "도착 건수": 3, "정류소": "S"}]
    assert S.section_rows([{"event_date": "2026-11-04", "kst_hour": 23, "section_id": "s"}])[0]["일시(KST)"] == "2026-11-04 23시"


def test_weather_rows_order_and_insufficient_flag():
    rows = S.weather_effect_rows([{"precip_class": "heavy", "insufficient": True}, {"precip_class": "none", "insufficient": False}, {"precip_class": "zzz"}])
    assert [r["_key"] for r in rows] == ["none", "heavy", "zzz"] and rows[1]["표본 부족"] is True and rows[2]["강수"] == "? zzz"


def test_component_rows_survive_unknown_status_and_missing_times():
    rows = S.component_rows(D.comps(), NOW)
    assert rows[0]["경과"] == "1분" and rows[2]["상태"] == "? future_status" and rows[2]["마지막 정상(KST)"] == "—" and rows[2]["경과"] == "—"


def test_incident_rows_pending_impact_and_missing_impact():
    rows = S.incident_rows(D.payload_for("/v1/incidents", {})["data"])
    assert rows[0]["상태"] == "영향 확정" and rows[0]["속도 변화(km/h)"] == "-4.2" and rows[0]["표본(전/중)"] == "6/5"
    assert rows[1]["속도 변화(km/h)"] == "—" and rows[1]["충분"] == "—" and rows[1]["표본(전/중)"] == "—"


def test_acc_type_text_known_unknown_and_empty():
    from dashboard import shape as S
    assert S.acc_type_text("A01") == "사고 (A01)" and S.acc_type_text("A04") == "공사·작업 (A04)"
    assert S.acc_type_text("A99") == "미확인 코드 (A99)", "모르는 코드를 아는 척 번역하지 않는다"
    assert S.acc_type_text(None) == "—" and S.acc_type_text("") == "—"
    rows = S.impact_type_rows([{"acc_type": "A10", "n_episodes": 2}])
    assert rows[0]["유형"] == "행사·통제 (A10)"
