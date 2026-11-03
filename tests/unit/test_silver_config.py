"""Silver 설정 (pyspark 없이 호스트에서 돈다)."""
from pathlib import Path

import pytest
import yaml

from streaming.silver_config import SilverConfig, load_silver_tables
from urbanstream.contracts import SOURCES

CONFIG = Path(__file__).resolve().parents[2] / "config"


def _copy(tmp_path, mutate):
    y = yaml.safe_load((CONFIG / "silver.yaml").read_text(encoding="utf-8"))
    mutate(y)
    (tmp_path / "silver.yaml").write_text(yaml.safe_dump(y, allow_unicode=True), encoding="utf-8")
    return tmp_path


def test_every_source_has_exactly_one_silver_table():
    tables = load_silver_tables(CONFIG)
    assert sorted(t.source for t in tables) == sorted(SOURCES)
    assert len({t.name for t in tables}) == len(tables)


def test_adr_0007_values():
    """ADR-0007 에 적은 값과 설정 파일이 어긋나면 알린다 (둘 중 하나만 바뀌는 것을 막는다)."""
    t = {x.source: x for x in load_silver_tables(CONFIG)}
    for s in ("seoul_traffic", "bus_position", "bus_arrival"):
        assert (t[s].watermark_col, t[s].watermark) == ("event_time", "10 minutes")
    assert (t["seoul_incident"].watermark_col, t["seoul_incident"].watermark) == ("fetched_at", "10 minutes")
    assert (t["kma_ncst"].watermark_col, t["kma_ncst"].watermark) == ("event_time", "3 hours")
    assert t["seoul_incident"].dedup_keys == ("dedup_key", "ingest_id")      # 관측 로그: 호출 사이 반복은 정상
    assert all(t[s].dedup_keys == ("dedup_key",) for s in SOURCES if s != "seoul_incident")
    assert t["seoul_incident"].partition_col == "observed_date" and t["bus_position"].partition_col == "event_date"
    assert SilverConfig.from_env({}).long_term_days == 7.0


@pytest.mark.parametrize("mutate,msg", [
    (lambda y: y["tables"]["bus_position"].update(watermark="10 mins"), "형식"),
    (lambda y: y["tables"]["bus_position"].update(watermark_col="ingested_at"), "watermark_col"),
    (lambda y: y["tables"]["bus_position"].update(dedup_keys=["ingest_id"]), "dedup_key"),
    (lambda y: y["tables"].pop("kma_ncst"), "없는 소스"),
    (lambda y: y["tables"].update(nope=dict(y["tables"]["bus_position"])), "레지스트리"),
    (lambda y: y["tables"]["bus_arrival"].update(name="silver_bus_position"), "겹칩"),
])
def test_bad_config_fails_loudly(tmp_path, mutate, msg):
    d = _copy(tmp_path, mutate)
    with pytest.raises(ValueError, match=msg):
        load_silver_tables(d)


def test_paths_come_from_env_but_rules_do_not():
    c = SilverConfig.from_env({"SILVER_ROOT": "/x", "SILVER_LONG_TERM_DAYS": "99"})
    assert c.silver_root == "/x" and c.long_term_days == 7.0
