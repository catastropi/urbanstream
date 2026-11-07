"""대시보드가 읽는 API 엔드포인트·쿼리 매개변수·응답 필드의 목록 (ADR-0012 D10-2).

화면 코드는 경로 문자열을 직접 쓰지 않고 여기의 Endpoint 만 쓴다. 두 가지가 이 목록을 지킨다.
- 실행 시: client 가 선언되지 않은 쿼리 매개변수를 거부한다.
- 시험 시: tests/dashboard/test_contract.py 가 이 목록을 OpenAPI 스냅샷(tests/contract/snapshots/openapi_v1.json)과 대조한다.
  → API 가 필드를 지우거나 이름을 바꾸면(= 비호환, 원래는 /v2) 대시보드 쪽 시험이 먼저 실패한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Endpoint:
    name: str
    path: str
    ttl: str                                   # config.TTL_SECONDS 의 키
    params: frozenset = frozenset()            # 허용하는 쿼리 매개변수
    reads: tuple = ()                          # ((스키마 이름, (필드, …)), …) — 화면이 실제로 읽는 필드

    def reads_map(self) -> Mapping[str, tuple]:
        return dict(self.reads)


def _ep(name, path, ttl, params, **reads):
    return Endpoint(name, path, ttl, frozenset(params), tuple((k, tuple(v)) for k, v in reads.items()))


PIPELINE_HEALTH = _ep("pipeline_health", "/v1/pipeline/health", "ops", (),
                      PipelineHealth=("overall", "counts", "components"),
                      PipelineComponent=("component", "component_kind", "status", "reason", "age_sec", "last_ok_at"))
PIPELINE_SOURCES = _ep("pipeline_sources", "/v1/pipeline/sources", "ops", (),
                       SourceStatus=("source_system", "status", "age_sec", "last_ok_at", "calls_1h", "fail_1h",
                                     "p95_latency_ms", "budget_used", "budget_limit", "reason"))
PIPELINE_LAYERS = _ep("pipeline_layers", "/v1/pipeline/layers", "ops", (),
                      LayerStatus=("layer", "table_name", "status", "age_sec", "last_write_at", "reason"))
PIPELINE_MARTS = _ep("pipeline_marts", "/v1/pipeline/marts", "ops", (),
                     MartFreshness=("mart", "status", "age_sec", "last_computed_at", "max_age_hours"))
PIPELINE_GAPS = _ep("pipeline_gaps", "/v1/pipeline/gaps", "ops", ("limit",),
                    DataGap=("kind", "gap_start", "gap_end", "duration_sec", "recoverable"))
PIPELINE_ALERTS = _ep("pipeline_alerts", "/v1/pipeline/alerts", "ops", ("limit", "level"),
                      AlertRecent=("created_at", "level", "status", "title"))
DQ_SUMMARY = _ep("dq_summary", "/v1/dq/summary", "ops", ("days",),
                 DqSummary=("run_date", "n_checks", "n_pass", "n_warn", "n_fail", "n_fail_error", "n_unknown", "checked_at"))
DQ_LATEST = _ep("dq_latest", "/v1/dq/latest", "ops", ("status", "include"),
                DqLatest=("rule_id", "status", "severity", "shadow", "value", "threshold", "target", "checked_at"))

LINKS = _ep("links", "/v1/links", "static", ("limit", "cursor", "collected_only", "geometry"),
            Link=("link_id", "length_m", "is_collected", "geometry"))
TRAFFIC_CURRENT = _ep("traffic_current", "/v1/traffic/current", "gold", ("link_id", "level", "limit", "cursor"),
                      Congestion=("link_id", "event_time", "speed_kmh", "baseline_p50", "ratio_to_baseline", "level",
                                  "threshold_n_obs", "threshold_provisional", "baseline_provisional", "day_type", "kst_hour"))
TRAFFIC_SPEEDS = _ep("traffic_speeds", "/v1/traffic/speeds", "gold", ("link_id", "from", "to", "limit", "cursor"),
                     SpeedPoint=("event_time", "speed_kmh", "dq_flags"))
TRAFFIC_THRESHOLDS = _ep("traffic_thresholds", "/v1/traffic/thresholds", "mart", ("link_id", "day_type"),
                         Threshold=("link_id", "day_type", "kst_hour", "speed_p10", "speed_p25", "speed_p50", "speed_p90",
                                    "n_obs", "provisional", "sufficient"))

INCIDENTS_ACTIVE = _ep("incidents_active", "/v1/incidents/active", "gold", ("limit", "include_long_term"),
                       IncidentEpisode=("acc_id", "acc_type", "occurred_at", "expected_clear_at", "lat", "lon", "link_id",
                                        "still_listed", "is_long_term", "last_seen_at"))
INCIDENTS = _ep("incidents", "/v1/incidents", "mart", ("from", "to", "acc_type", "limit", "cursor"),
                IncidentImpactItem=("episode", "impact", "status"),
                IncidentEpisode=("acc_id", "acc_type", "occurred_at", "still_listed"),
                IncidentImpact=("delta", "ratio_in", "ratio_pre", "n_in", "n_pre", "sufficient", "provisional", "match_kind",
                                "distance_m", "duration_min"))
IMPACT_BY_TYPE = _ep("impact_by_type", "/v1/incidents/impact-by-type", "mart", (),
                     ImpactByType=("acc_type", "n_episodes", "n_linked", "n_sufficient", "delta_p50", "ratio_in_p50", "provisional"))

BUS_POSITIONS = _ep("bus_positions", "/v1/bus/positions", "gold", ("route_id", "within_min", "limit"),
                    BusPosition=("event_time", "lat", "lon", "sect_ord", "section_id", "congestion", "is_full", "is_running"))
BUS_DRIFT = _ep("bus_arrival_drift", "/v1/bus/arrival-drift", "mart", ("route_id", "station_id", "from", "to", "limit", "cursor"),
                ArrivalDrift=("event_date", "kst_hour", "route_id", "station_id", "n_approaches", "drift_p50_sec",
                              "drift_p90_sec", "provisional"))
BUS_SECTION = _ep("bus_section_slowness", "/v1/bus/section-slowness", "mart",
                  ("route_id", "from", "to", "min_ratio", "limit", "cursor"),
                  SectionSlowness=("event_date", "kst_hour", "route_id", "section_id", "sect_ord", "traversal_sec",
                                   "base_p50_sec", "ratio_to_base", "base_provisional", "day_type"))

WEATHER_EFFECT = _ep("weather_effect", "/v1/weather/effect", "mart", (),
                     WeatherEffect=("precip_class", "n_hours", "n_links", "ratio_p25", "ratio_p50", "ratio_p75",
                                    "insufficient", "provisional"))
WEATHER_OBS = _ep("weather_observations", "/v1/weather/observations", "gold", ("from", "to", "limit", "cursor"),
                  WeatherObs=("base_time", "event_date", "t1h", "rn1", "pty", "reh", "wsd"))
ANOMALIES = _ep("anomalies", "/v1/anomalies", "gold", ("link_id", "status", "direction", "from", "to", "limit", "cursor"),
                Anomaly=("link_id", "event_time", "detected_at", "speed_kmh", "baseline_p50", "robust_z", "direction",
                         "status", "n_obs", "provisional"))

ALL_ENDPOINTS = (PIPELINE_HEALTH, PIPELINE_SOURCES, PIPELINE_LAYERS, PIPELINE_MARTS, PIPELINE_GAPS, PIPELINE_ALERTS,
                 DQ_SUMMARY, DQ_LATEST, LINKS, TRAFFIC_CURRENT, TRAFFIC_SPEEDS, TRAFFIC_THRESHOLDS, INCIDENTS_ACTIVE,
                 INCIDENTS, IMPACT_BY_TYPE, BUS_POSITIONS, BUS_DRIFT, BUS_SECTION, WEATHER_EFFECT, WEATHER_OBS, ANOMALIES)
