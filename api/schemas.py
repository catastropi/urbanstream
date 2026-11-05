"""응답 모델. 필드 이름은 뷰 열 이름 그대로(snake_case). enum 은 Literal 로 고정해 OpenAPI 스냅샷에 잠근다."""
from datetime import date, datetime
from typing import Any, Generic, List, Literal, Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")

Status5 = Literal["ok", "degraded", "down", "idle", "unknown"]
DayType = Literal["weekday", "sat", "sun", "holiday"]
CongestionLevel = Literal["severe", "heavy", "slow", "normal"]


class Problem(BaseModel):
    type: str = Field(description="urn:urbanstream:error:<code>")
    title: str
    status: int
    detail: str
    code: str = Field(description="기계가 읽는 안정된 오류 코드")
    request_id: Optional[str] = None
    errors: Optional[List[dict]] = Field(default=None, description="422 일 때 매개변수별 사유(입력값은 되돌려 주지 않음)")


class Meta(BaseModel):
    as_of: Optional[datetime] = Field(None, description="이 응답이 대표하는 데이터의 시각. '현재' 엔드포인트는 전체 최신 관측, 기간 조회는 해당 소스의 최신 시각, 마트는 마지막 계산 시각. 데이터가 없으면 null")
    age_sec: Optional[float] = Field(None, description="as_of 부터 응답을 만든 시각까지의 초. as_of 가 없으면 null")
    stale: Optional[bool] = Field(None, description="age_sec 가 기준(기본 3시간, 운영 스냅샷은 5분)을 넘으면 true. 기준이 적용되지 않는 엔드포인트는 null")
    loaded_at: Optional[datetime] = Field(None, description="Gold 적재(gold_load)가 마지막으로 성공한 시각. 'current' 값은 이 적재 기준이라 실시간이 아니다(매시 20분 적재, 최대 약 80분 지연)")
    total: Optional[int] = Field(None, description="필터 후 전체 행 수(top 적용 전). 해당 없으면 null")
    notes: List[str] = Field(default_factory=list, description="해석 주의 코드. 예: baseline_provisional, empty_result, outside_collection_window")


class PageInfo(BaseModel):
    limit: int
    has_more: bool
    next_cursor: Optional[str] = None


class Envelope(BaseModel, Generic[T]):
    data: T
    meta: Meta


class PagedEnvelope(BaseModel, Generic[T]):
    data: List[T]
    meta: Meta
    page: PageInfo


# ------------------------------------------------------------------ health
class HealthOut(BaseModel):
    status: Literal["ok"]
    version: str


class ReadyOut(BaseModel):
    status: Literal["ok"]
    db: Literal["ok"]


# ------------------------------------------------------------------ pipeline / ops
class SourceStatus(BaseModel):
    source_system: str
    status: Status5
    reason: Optional[str] = None
    last_call_at: Optional[datetime] = None
    last_ok_at: Optional[datetime] = None
    age_sec: Optional[float] = None
    expected_gap_sec: Optional[float] = None
    calls_1h: Optional[int] = None
    fail_1h: Optional[int] = None
    p95_latency_ms: Optional[int] = None
    budget_used: Optional[int] = None
    budget_limit: Optional[int] = None
    checked_at: datetime


class LayerStatus(BaseModel):
    layer: Literal["bronze", "silver"]
    table_name: str
    source_system: Optional[str] = None
    status: Status5
    reason: Optional[str] = None
    last_write_at: Optional[datetime] = None
    age_sec: Optional[float] = None
    checked_at: datetime


class PipelineComponent(BaseModel):
    component_kind: Literal["source", "layer", "monitor", "load", "mart"]
    component: str
    status: Status5
    reason: Optional[str] = None
    last_ok_at: Optional[datetime] = None
    age_sec: Optional[float] = None
    checked_at: Optional[datetime] = None


class PipelineHealth(BaseModel):
    overall: Status5 = Field(description="가장 나쁜 상태. down > degraded > unknown > ok. idle(수집 창 밖)은 무시. 구성요소가 없으면 unknown")
    counts: dict[str, int]
    components: List[PipelineComponent]


class MartFreshness(BaseModel):
    mart: str
    last_computed_at: Optional[datetime] = None
    age_sec: Optional[float] = None
    max_age_hours: float
    status: Status5


class LoadRun(BaseModel):
    run_id: int
    job: str
    dag_id: Optional[str] = None
    date_from: date
    date_to: date
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: Literal["running", "ok", "failed"]


class DataGap(BaseModel):
    gap_id: int
    kind: Literal["host_stall", "collector_blackout"]
    gap_start: datetime
    gap_end: datetime
    duration_sec: int
    recoverable: bool


class AlertRecent(BaseModel):
    alert_id: int
    created_at: datetime
    level: Literal["info", "warning", "error", "critical"]
    title: str
    status: Literal["pending", "sent", "digest", "digested", "suppressed", "failed", "dropped"]
    dedup_key: Optional[str] = None


class DqSummary(BaseModel):
    run_date: date
    n_checks: int
    n_pass: int
    n_warn: int
    n_fail: int
    n_unknown: int
    n_fail_error: int = Field(description="관찰(shadow) 기간이 아닌 error 수준 규칙의 fail 수")
    checked_at: Optional[datetime] = None


class DqLatest(BaseModel):
    rule_id: str
    kind: str
    target: str
    severity: Literal["info", "warning", "error"]
    action: Literal["record", "alert", "block_mart"]
    status: Literal["pass", "warn", "fail", "unknown", "skipped"]
    shadow: bool = Field(description="true 면 관찰 기간이라 알림·마트 차단에 쓰이지 않는다")
    run_date: date
    checked_at: datetime
    value: Optional[float] = None
    n_checked: Optional[int] = None
    threshold: Optional[str] = None
    detail: Optional[dict[str, Any]] = Field(None, description="include=detail 일 때만 채워진다")


# ------------------------------------------------------------------ traffic
class Link(BaseModel):
    link_id: str
    length_m: float
    vertex_count: int
    is_collected: bool
    source_note: Optional[str] = None
    geometry: Optional[dict[str, Any]] = Field(None, description="GeoJSON LineString (WGS84). geometry=true 일 때만")


class SpeedPoint(BaseModel):
    event_time: datetime
    speed_kmh: Optional[float] = None
    travel_time_sec: Optional[int] = None
    dq_flags: List[str] = Field(default_factory=list, description="Silver 가 붙인 품질 표시(예: speed_zero). 값이 틀릴 수 있다는 전제로 그대로 노출")
    fetched_at: datetime


class Threshold(BaseModel):
    version_date: date
    link_id: str
    day_type: DayType
    kst_hour: int
    n_obs: int
    n_days: int
    span_days: int
    speed_p10: Optional[float] = None
    speed_p25: Optional[float] = None
    speed_p50: Optional[float] = None
    speed_p90: Optional[float] = None
    sufficient: bool
    provisional: bool
    computed_at: datetime


class Congestion(BaseModel):
    link_id: str
    event_time: datetime
    speed_kmh: Optional[float] = None
    level: Optional[CongestionLevel] = Field(None, description="표본이 부족하면 null(계산하지 않음). p10 이하 severe, p25 이하 heavy, p50 이하 slow, 그 위 normal")
    day_type: Optional[DayType] = None
    kst_hour: Optional[int] = None
    threshold_n_obs: Optional[int] = None
    threshold_provisional: Optional[bool] = None
    threshold_version: Optional[date] = None
    baseline_p50: Optional[float] = None
    baseline_provisional: Optional[bool] = None
    ratio_to_baseline: Optional[float] = Field(None, description="현재 속도 / 같은 요일·시각 기준선 중앙값. 1 = 평소와 같음, 작을수록 느림")


# ------------------------------------------------------------------ incidents
class IncidentEpisode(BaseModel):
    acc_id: str
    occurred_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    absent_since: Optional[datetime] = Field(None, description="해제 시각이 아니라 '목록에서 처음 안 보인 시각'이다(근사)")
    expected_clear_at: Optional[datetime] = None
    obs_count: int
    is_long_term: bool
    still_listed: bool
    acc_type: Optional[str] = None
    link_id: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None


class IncidentImpact(BaseModel):
    duration_min: int
    link_id: Optional[str] = None
    match_kind: Literal["link_id", "distance", "no_link"]
    distance_m: Optional[float] = None
    n_in: int
    n_pre: int
    ratio_in: Optional[float] = Field(None, description="돌발 구간 속도 / 평소(기준선) 중앙값. 1 = 평소와 같음, 작을수록 느림")
    ratio_pre: Optional[float] = None
    delta: Optional[float] = None
    sufficient: bool
    provisional: bool
    finalized_at: datetime


class IncidentImpactItem(BaseModel):
    status: Literal["listed", "pending", "finalized", "excluded_long_term"] = Field(
        description="listed=아직 목록에 있음 / pending=해제됐지만 24시간 확정 전 / finalized=영향 계산 완료 / excluded_long_term=장기 공사라 영향으로 읽지 않음")
    episode: IncidentEpisode
    impact: Optional[IncidentImpact] = None


class ImpactByType(BaseModel):
    acc_type: str
    n_episodes: int
    n_linked: int
    n_sufficient: int
    ratio_in_p50: Optional[float] = None
    delta_p50: Optional[float] = None
    provisional: bool


# ------------------------------------------------------------------ bus
class BusPosition(BaseModel):
    veh_id: str
    plain_no: Optional[str] = None
    event_time: datetime
    lon: Optional[float] = None
    lat: Optional[float] = None
    section_id: Optional[str] = None
    sect_ord: Optional[int] = None
    last_stop_id: Optional[str] = None
    next_stop_id: Optional[str] = None
    sect_dist: Optional[float] = None
    full_sect_dist: Optional[float] = None
    congestion: Optional[int] = None
    stop_flag: Optional[int] = None
    is_running: Optional[bool] = None
    is_last: Optional[bool] = None
    is_full: Optional[bool] = None
    dq_flags: List[str] = Field(default_factory=list)


class ArrivalDrift(BaseModel):
    route_id: str
    station_id: str
    event_date: date
    kst_hour: int
    n_approaches: int
    drift_p50_sec: Optional[float] = Field(None, description="마지막 예측 − 처음 예측(초). 양수 = 처음 예측보다 늦게 도착하는 쪽으로 밀림. 시간표 대비 지연이 아니다")
    drift_p90_sec: Optional[float] = None
    provisional: bool = Field(description="접근 횟수가 10 미만")


class SectionSlowness(BaseModel):
    route_id: str
    veh_id: str
    section_id: str
    sect_ord: int
    entered_at: datetime
    traversal_sec: int
    event_date: date
    kst_hour: int
    day_type: DayType
    base_p50_sec: Optional[float] = None
    base_n_obs: Optional[int] = None
    base_provisional: Optional[bool] = None
    ratio_to_base: Optional[float] = Field(None, description="통과 시간 / 평소 중앙값. 평소 표본이 10건 미만이면 null. 관측 주기(약 90초) 때문에 ±90초 오차")


# ------------------------------------------------------------------ weather
class WeatherObs(BaseModel):
    base_time: datetime
    nx: int
    ny: int
    t1h: Optional[float] = None
    rn1: Optional[float] = None
    uuu: Optional[float] = None
    vvv: Optional[float] = None
    reh: Optional[float] = None
    pty: Optional[float] = None
    vec: Optional[float] = None
    wsd: Optional[float] = None
    value_unparsed_count: int
    event_date: date
    iso_dow: int
    is_weekend: bool


class WeatherEffect(BaseModel):
    precip_class: Literal["none", "light", "moderate", "heavy", "snow_mix"]
    n_hours: int
    n_links: int
    ratio_p25: Optional[float] = None
    ratio_p50: Optional[float] = None
    ratio_p75: Optional[float] = None
    insufficient: bool = Field(description="n_hours < 20 이면 true — 읽지 말 것")
    provisional: Optional[bool] = None


# ------------------------------------------------------------------ anomalies
class Anomaly(BaseModel):
    link_id: str
    event_time: datetime
    speed_kmh: float
    baseline_p50: float
    baseline_iqr: float
    robust_z: Optional[float] = None
    direction: Optional[Literal["slow", "fast"]] = None
    status: Literal["anomaly", "candidate", "degenerate_baseline"] = Field(
        description="anomaly=기준선이 충분(28일 이상, 표본 20 이상)할 때의 이탈 / candidate=기준선이 잠정이거나 표본이 적을 때의 이탈 / degenerate_baseline=IQR 이 0이라 z 계산 불가")
    n_obs: int
    provisional: bool
    detected_at: datetime
