import pytest

from dashboard.config import ConfigError, load_settings

OK = {"DASHBOARD_API_KEY": "x" * 30}


def test_defaults_and_secret_not_in_repr():
    s = load_settings(OK)
    assert s.api_url == "http://api:8000" and s.ttl_scale == 1.0 and s.budget_per_min == 100
    assert "x" * 30 not in repr(s)


@pytest.mark.parametrize("env", [{}, {"DASHBOARD_API_KEY": ""}, {"DASHBOARD_API_KEY": "short"},
                                 {"DASHBOARD_API_KEY": "a b" + "c" * 30}, {"DASHBOARD_API_KEY": "a" * 30 + "," + "b" * 30}])
def test_missing_or_weak_key_refuses_to_start(env):
    with pytest.raises(ConfigError):
        load_settings(env)


def test_bad_values_are_rejected():
    for extra in ({"DASHBOARD_API_URL": "ftp://x"}, {"DASHBOARD_TTL_SCALE": "-1"}, {"DASHBOARD_TTL_SCALE": "abc"},
                  {"DASHBOARD_BUDGET_PER_MIN": "0"}, {"DASHBOARD_BUDGET_PER_MIN": "1.5"}):
        with pytest.raises(ConfigError):
            load_settings({**OK, **extra})
    assert load_settings({**OK, "DASHBOARD_API_URL": "http://127.0.0.1:8010/"}).api_url == "http://127.0.0.1:8010"


def test_bus_route_id_defaults_to_753_and_can_be_overridden_but_not_garbage():
    base = {"DASHBOARD_API_KEY": "k" * 40}
    assert load_settings(base).bus_route_id == "100100118"
    assert load_settings({**base, "DASHBOARD_BUS_ROUTE_ID": " 200000001 "}).bus_route_id == "200000001"
    for bad in ("a b", "x" * 41, "1;DROP", "한글"):
        with pytest.raises(ConfigError):
            load_settings({**base, "DASHBOARD_BUS_ROUTE_ID": bad})
