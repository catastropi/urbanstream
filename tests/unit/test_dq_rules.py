import copy
from datetime import date

import pytest
import yaml

from urbanstream.dq import rules as R
from batch import dq_jobs

RAW = yaml.safe_load(open(R.__file__.replace("src/urbanstream/dq/rules.py", "config/dq_rules.yaml"), encoding="utf-8"))


def test_shipped_rules_pass_the_linter_and_are_restricted():
    rs = R.parse(RAW)
    assert len(rs.rules) >= 20 and len({r.id for r in rs.rules}) == len(rs.rules)
    assert {r.kind for r in rs.rules if r.action == "block_mart"} == {"future_time"}          # 마트를 막는 것은 시간 이상뿐
    assert rs.shadow_until == date(2026, 11, 12)


def bad(mutator, expect):
    raw = copy.deepcopy(RAW)
    mutator(raw)
    with pytest.raises(R.RuleError) as e:
        R.parse(raw)
    assert expect in str(e.value)


def test_linter_catches_mistakes():
    bad(lambda r: r["rules"].append(dict(r["rules"][0])), "중복")
    bad(lambda r: r["rules"][0].update(kind="nope"), "모르는 kind")
    bad(lambda r: r["rules"][0].update(severity="fatal"), "severity")
    bad(lambda r: r["rules"][0].update(action="block_mart"), "block_mart")            # null_rate 는 마트를 막을 수 없다
    bad(lambda r: r["rules"][0].pop("max_rate"), "max_rate")
    bad(lambda r: r["rules"][0].update(table="fact.x; DROP TABLE fact.y"), "스키마.표")      # SQL 끼워넣기 방지
    bad(lambda r: r["rules"][0].update(column="a b"), "열 이름")
    bad(lambda r: r.update(shadow_until="내일"), "날짜")
    bad(lambda r: r["rules"][0].update(mode="maybe"), "mode")


def test_shadow_window_and_enforce_override():
    rs = R.parse(RAW)
    r0 = rs.rules[0]
    assert rs.is_shadow(r0, date(2026, 11, 12)) and not rs.is_shadow(r0, date(2026, 11, 13))
    enforced = R.Rule("x", "null_rate", "warning", "alert", "enforce", {})
    assert not rs.is_shadow(enforced, date(2026, 11, 5))


def test_parse_silver_verify_output():
    text = ("=== Silver 검증 결과 ===\nsilver_traffic_speed         OK   Bronze 1,000행 = 정상 1,000 + 읽기불가 0\n"
            "silver_weather               FAIL Bronze 3행 ... 유실 2\n    [좌표 확인용] x\n\n전체: 실패")
    assert dq_jobs.parse_silver_verify(text) == [("silver_traffic_speed", True, "Bronze 1,000행 = 정상 1,000 + 읽기불가 0"), ("silver_weather", False, "Bronze 3행 ... 유실 2")]
    assert dq_jobs.parse_silver_verify("") == []
