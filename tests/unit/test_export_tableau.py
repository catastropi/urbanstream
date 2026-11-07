"""Tableau CSV 내보내기 — API 만 읽고, 집계 열만 내고, 게시하지 않는다 (Step 10, D10-8)."""
from __future__ import annotations

import csv
import importlib.util
import io
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import dashboard_support as D
from dashboard.client import FetchResponse

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("export_tableau", ROOT / "scripts" / "export_tableau.py")
X = importlib.util.module_from_spec(spec)
sys.modules["export_tableau"] = X
spec.loader.exec_module(X)

NOW = D.NOW


def _client(handler=None):
    c, f, clk = D.make_client(D.FakeFetch(handler), ttl_scale=0.0, on_budget="wait", budget_per_min=100)
    return c, f, clk


def _read(path):
    with open(path, encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------- 열 목록 · 안전
def test_column_lists_are_aggregate_only_and_start_with_snapshot_at():
    X.assert_columns_safe()
    for name, cols in X.COLUMNS.items():
        assert cols[0] == "snapshot_at" and not (X.FORBIDDEN_COLUMNS & set(cols)), name


def test_forbidden_vehicle_columns_from_api_never_reach_the_csv():
    rows = [{"event_date": "2026-11-04", "kst_hour": 9, "route_id": "R", "station_id": "S", "n_approaches": 3, "drift_p50_sec": 1.0,
             "drift_p90_sec": 2.0, "provisional": True, "veh_id": "V1", "plain_no": "서울74사1234"}]
    out = X.pick(rows, "bus_arrival_drift_hourly", "2026-11-05T06:20:30Z")
    assert set(out[0]) == set(X.COLUMNS["bus_arrival_drift_hourly"]) and "V1" not in str(out) and "서울74" not in str(out)


def test_exporter_never_asks_for_vehicle_endpoints():
    c, f, _ = _client()
    X.collect(c, NOW, 7)
    assert not [p for p in f.paths() if "/bus/positions" in p or "/bus/section" in p or "/dq/" in p or "/pipeline/" in p or "/anomalies" in p]


# ---------------------------------------------------------------- 창 나누기
def test_windows_are_contiguous_quantized_and_at_most_seven_days():
    w = X.windows(NOW, 20)
    assert len(w) == 3
    for (a, b) in w:
        da, db = X.parse_utc(a), X.parse_utc(b)
        assert timedelta(0) < db - da <= timedelta(days=7)
        assert da.second == 0 and da.minute % 5 == 0
    assert all(w[i][1] == w[i + 1][0] for i in range(len(w) - 1)), "조각이 이어져야 한다(빠짐·겹침 없음)"
    assert X.parse_utc(w[-1][1]) - X.parse_utc(w[0][0]) == timedelta(days=20)


@pytest.mark.parametrize("days", [0, 29, -1])
def test_windows_reject_out_of_range_days(days):
    with pytest.raises(ValueError):
        X.windows(NOW, days)


# ---------------------------------------------------------------- 시간별 집계
def _pt(t, v, flags=()):
    return {"event_time": t, "speed_kmh": v, "dq_flags": list(flags)}


def test_hourly_aggregation_uses_kst_hours_keeps_null_out_of_the_mean_and_dedups_boundaries():
    # UTC 14:50 = KST 23:50, UTC 15:10 = KST 00:10(다음 날)
    pts = {"L1": [_pt("2026-11-04T14:50:00Z", 30.0), _pt("2026-11-04T14:55:00Z", 20.0), _pt("2026-11-04T14:55:00Z", 20.0),
                  _pt("2026-11-04T15:10:00Z", None), _pt("2026-11-04T15:15:00Z", 40.0, ["stale"])]}
    rows = X.hourly_speed_rows(pts, "S")
    assert [(r["kst_date"], r["kst_hour"]) for r in rows] == [("2026-11-04", 23), ("2026-11-05", 0)]
    a, b = rows
    assert a["n_points"] == 2 and a["speed_mean_kmh"] == 25.0 and a["speed_min_kmh"] == 20.0 and a["speed_max_kmh"] == 30.0, "경계 중복은 한 번만"
    assert b["n_points"] == 1 and b["n_null"] == 1 and b["speed_mean_kmh"] == 40.0 and b["n_dq_flagged"] == 1


def test_all_null_hour_keeps_none_not_zero():
    r = X.hourly_speed_rows({"L1": [_pt("2026-11-05T01:00:00Z", None)]}, "S")[0]
    assert r["n_points"] == 0 and r["n_null"] == 1 and r["speed_mean_kmh"] is None and r["speed_min_kmh"] is None


def test_non_finite_and_bool_speeds_are_treated_as_missing():
    r = X.hourly_speed_rows({"L1": [_pt("2026-11-05T01:00:00Z", float("nan")), _pt("2026-11-05T01:05:00Z", True), _pt("2026-11-05T01:10:00Z", 33.0)]}, "S")[0]
    assert r["n_points"] == 1 and r["n_null"] == 2 and r["speed_mean_kmh"] == 33.0


# ---------------------------------------------------------------- 수집 · 쓰기
def test_collect_calls_speeds_per_link_and_window_and_reads_only_the_api():
    c, f, _ = _client()
    col = X.collect(c, NOW, 14)
    speeds = [(p, q) for p, q in f.calls if p == "/v1/traffic/speeds"]
    assert len(speeds) == 4 * 2, "링크 4개 × 7일 조각 2개"
    assert all(q.get("limit") == "1000" for _, q in speeds)
    assert set(col.datasets) == set(X.COLUMNS) and not col.incomplete


def test_write_all_produces_csvs_manifest_and_checklist_without_secrets(tmp_path):
    c, f, _ = _client()
    col = X.collect(c, NOW, 7)
    m = X.write_all(col, tmp_path, now=NOW, days=7, api_calls=len(c.call_log))
    assert {p.name for p in tmp_path.iterdir()} == {f"{n}.csv" for n in X.COLUMNS} | {"manifest.json"}
    for n, cols in X.COLUMNS.items():
        rows = _read(tmp_path / f"{n}.csv")
        assert list(rows[0].keys()) == cols and all(r["snapshot_at"] == "2026-11-05T06:20:30Z" for r in rows)
    imp = {r["acc_type"]: r for r in _read(tmp_path / "incident_impact_by_type.csv")}
    assert imp["공사"]["delta_p50"] == "" and imp["사고"]["provisional"] == "true", "None 은 빈 칸, 0 으로 바꾸지 않는다 / 한글이 깨지지 않는다"
    assert all("acc_type_label" in r for r in imp.values())
    assert X.pick([{"acc_type": "A01"}, {"acc_type": "A77"}, {"acc_type": None}], "incident_impact_by_type", "t")[0]["acc_type_label"] == "사고"
    assert [r["acc_type_label"] for r in X.pick([{"acc_type": "A77"}, {"acc_type": None}], "incident_impact_by_type", "t")] == ["미확인 코드", None]
    man = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert man["contains_vehicle_identifiers"] is False and man["complete"] is True and len(man["publish_checklist"]) >= 5
    assert any("누구나" in s for s in man["publish_checklist"]) and any("미확인" in s for s in man["publish_checklist"])
    blob = (tmp_path / "manifest.json").read_text(encoding="utf-8") + "".join(p.read_text(encoding="utf-8-sig") for p in tmp_path.glob("*.csv"))
    assert D.KEY not in blob and "V1" not in blob and "서울74" not in blob
    assert all(re.fullmatch(r"[0-9a-f]{64}", x["sha256"]) for x in m["files"])


def test_truncated_paging_marks_the_export_incomplete_and_returns_4(tmp_path):
    def handler(path, params):
        if path == "/v1/traffic/speeds":
            body = {"data": [_pt("2026-11-05T01:00:00Z", 30.0)], "meta": D.meta(NOW - timedelta(minutes=45)),
                    "page": {"has_more": True, "limit": 1000, "next_cursor": f"c{len(params)}x"}}
            return FetchResponse(200, {}, json.dumps(body).encode())
        return None
    c, f, _ = _client(handler)
    col = X.collect(c, NOW, 7)
    assert any(s.startswith("traffic_speeds:") for s in col.incomplete)
    rc = X.main(["--out", str(tmp_path)], client=c, now=NOW)
    assert rc == 4 and json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))["complete"] is False


def test_preview_writes_nothing(tmp_path, capsys):
    c, f, _ = _client()
    out = tmp_path / "x"
    assert X.main(["--preview", "--out", str(out)], client=c, now=NOW) == 0
    assert not out.exists() and "link_speed_hourly" in capsys.readouterr().out


def test_api_auth_error_exits_3_with_a_hint_and_without_the_key(tmp_path, capsys):
    def handler(path, params):
        return FetchResponse(401, {}, json.dumps({"detail": "invalid api key"}).encode())
    c, f, _ = _client(handler)
    rc = X.main(["--out", str(tmp_path / "o")], client=c, now=NOW)
    out = capsys.readouterr().out
    assert rc == 3 and "init-env" in out and D.KEY not in out and not (tmp_path / "o").exists()


# ---------------------------------------------------------------- 설정 · .env
def test_settings_come_from_env_and_only_local_api_is_allowed():
    env = {"DASHBOARD_API_KEY": "z" * 40, "API_PORT": "8123"}
    s = X.settings_from_env(env)
    assert s.api_url == "http://127.0.0.1:8123" and s.ttl_scale == 0.0 and "z" * 40 not in repr(s)
    with pytest.raises(X.ExportConfigError):
        X.settings_from_env({**env, "DASHBOARD_API_KEY": ""})
    with pytest.raises(X.ExportConfigError):
        X.settings_from_env({**env, "DASHBOARD_API_KEY": "short"})
    with pytest.raises(X.ExportConfigError):
        X.settings_from_env(env, api_url="https://example.com")


def test_main_reports_missing_env_and_key_without_traceback(tmp_path, capsys):
    assert X.main(["--env", str(tmp_path / "nope")]) == 2
    e = tmp_path / ".env"
    e.write_text("SEOUL_API_KEY=abc\n", encoding="utf-8")
    assert X.main(["--env", str(e)]) == 2
    assert "DASHBOARD_API_KEY" in capsys.readouterr().out


def test_parse_env_text_handles_quotes_comments_and_blank_lines():
    assert X.parse_env_text("# c\n\nA=1\nB=\"two\"\nC='3'\nbad line\n") == {"A": "1", "B": "two", "C": "3"}


def test_script_has_no_network_upload_or_db_imports():
    src = (ROOT / "scripts/export_tableau.py").read_text(encoding="utf-8")
    code = re.sub(r'""".*?"""', "", src, flags=re.S)
    assert not re.search(r"^\s*(import|from)\s+(psycopg|sqlalchemy|tableauserverclient|boto3|smtplib|ftplib)", code, re.M)
    assert not re.search(r"tableau\.com|public\.tableau", code), "게시 주소를 코드에 두지 않는다"
