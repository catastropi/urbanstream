"""scripts/analyze_smoke_raw.py — cadence 원본을 필드 단위로 비교하는 순수 로직."""
import importlib.util
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("analyze_smoke_raw", ROOT / "scripts" / "analyze_smoke_raw.py")
az = importlib.util.module_from_spec(spec)
sys.modules["analyze_smoke_raw"] = az
spec.loader.exec_module(az)


def inc(acc_id, clr="000000", info="공사", link="1"):
    return {"acc_id": acc_id, "exp_clr_date": "20270301", "exp_clr_time": clr, "acc_type": "A04",
            "acc_dtype": "04B01", "link_id": link, "acc_info": info, "grs80tm_x": "1"}


def test_incident_diff_counts_added_removed_and_new_versions():
    prev = [inc("1"), inc("2"), inc("3")]
    cur = [inc("1"), inc("2", clr="070000"), inc("4")]       # 2: 해제 예정 시각 변경, 3 사라짐, 4 새로 등장
    d = az.diff_incident(prev, cur)
    assert (d["added"], d["removed"], d["changed_ids"], d["new_versions"]) == (1, 1, 1, 1)
    assert d["fields"] == {"exp_clr_time": 1}


def test_incident_non_hash_field_change_is_not_a_new_version():
    prev = [inc("1")]
    cur = [dict(inc("1"), grs80tm_x="2")]                    # 좌표만 변경: content_hash 입력이 아님
    d = az.diff_incident(prev, cur)
    assert d["changed_ids"] == 1 and d["new_versions"] == 0


def test_incident_order_shift_is_not_a_change():
    # 순서가 바뀌어도(위치 비교였다면 '변경') acc_id 로 맞추면 변경이 없다.
    d = az.diff_incident([inc("1"), inc("2")], [inc("2"), inc("1")])
    assert (d["added"], d["removed"], d["changed_ids"]) == (0, 0, 0)


def test_stamp_and_time_parsers():
    assert az.stamp_of(Path("20261102T132641_KST.xml")) == datetime(2026, 11, 2, 13, 26, 41)
    assert az.parse_data_tm("20261101215259") == datetime(2026, 11, 1, 21, 52, 59)
    assert az.parse_data_tm("bad") is None
    assert az.parse_mk_tm("2026-11-01 21:53:00.0") == datetime(2026, 11, 1, 21, 53, 0)


def test_parse_rows_reads_children_as_strings():
    rows = az.parse_rows("<a><row><acc_id> 7 </acc_id><x/></row></a>", "row")
    assert rows == [{"acc_id": "7", "x": ""}]


def test_bus_position_report_counts_repeated_datatm():
    t0 = datetime(2026, 11, 2, 13, 0, 0)
    s = [(t0, [{"vehId": "1", "dataTm": "20261102125950"}, {"vehId": "2", "dataTm": "20261102125950"}]),
         (datetime(2026, 11, 2, 13, 1, 0), [{"vehId": "1", "dataTm": "20261102130010"},
                                            {"vehId": "2", "dataTm": "20261102125950"}])]
    out = "\n".join(az.report_bus_position(s))
    assert "1/2" in out          # 차량 2의 dataTm 이 그대로
    assert "20" in out           # 차량 1의 증가량 20초
