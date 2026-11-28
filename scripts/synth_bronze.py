"""STEP 2 합성 Bronze (설계안 3장). Kafka·수집기·운영 폴더를 거치지 않고, 실제 하루의 '모양'을 본뜬 Bronze Parquet 를 만든다.

하위 명령
  profile   실제 하루 Bronze(읽기만) → data/measure/profile/<날짜>.json
            소스별 호출 수·시간대 분포·호출당 레코드 수·대상(링크·차량·정류장·격자) 목록·값 분포(분위수·빈도).
            행을 그대로 복사하지 않는다: 숫자는 분위수 21개, 문자열은 값 종류가 20개 이하일 때만 빈도로 남긴다.
  generate  프로파일 → data/measure/synthetic/<run_id>/bronze/<표>/event_date=<날짜>/*.parquet + manifest.json
            규모 N× 는 호출 수가 아니라 대상 수를 N배로 늘린다. 늘린 대상의 ID 는 'S<j>-' 로 시작한다.
            호출마다 1건인 소스(도로 소통: 링크마다 호출)는 대상이 늘면 호출도 늘고, 한 호출에 여러 건인 소스는 호출당 건수가 는다.
            Bronze audit(호출 결과)도 같이 만든다: Gold 의 call_audit·돌발 사라짐 판정이 이것을 읽는다.

합성 표시 네 곳 (스키마는 바꾸지 않는다, 결정 2)
  폴더          data/measure/synthetic/<run_id>/
  manifest      <run_id>/manifest.json: "synthetic": true, 생성기 버전, 시드, 프로파일, 규모, 파일·행 수, 파일 해시
  Parquet 메타  키 urbanstream.synthetic=true, urbanstream.run_id
  레코드        ingest_id 는 'syn-' 로 시작, raw_uri 는 'synthetic://<run_id>'

가드: 출력은 data/measure/ 아래만(운영 폴더 거부), 날짜는 config/measure.yaml 의 first_date 이상·real_data_start 미만만.
같은 프로파일·시드·규모·날짜·배치면 같은 파일(같은 해시)이 나온다. pyarrow 만 쓴다(JVM 없음).

사용 (프로젝트 루트, 호스트 Python)
  python scripts/synth_bronze.py profile --date 2026-11-04
  python scripts/synth_bronze.py generate --profile data/measure/profile/2026-11-04.json --scale 5 --date 2026-01-05 --run-id s5-a
  python scripts/synth_bronze.py generate ... --layout hour        표·시간당 파일 1개 (기본 minute = 스트리밍처럼 표·분당 1개)
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import random
import sys
import zlib
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "src", Path(__file__).resolve().parent):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from measure_guard import KST, ensure_measure_path, ensure_synthetic_date, load_config  # noqa: E402
from urbanstream.contracts import SOURCES  # noqa: E402
from urbanstream.contracts import bus_arrival, bus_position, incident, traffic, weather  # noqa: E402
from urbanstream.contracts.base import CallContext  # noqa: E402
from urbanstream.contracts.common import second_key  # noqa: E402
from urbanstream.contracts.registry import schema_of  # noqa: E402

GENERATOR_VERSION = "1"
UTC = timezone.utc
QN = 21                                   # 분위수 개수 (0, 5, ..., 100%)
TOP_MAX = 20                              # 이보다 값 종류가 많은 문자열은 빈도를 남기지 않는다
LAYOUTS = ("minute", "hour")
AUDIT_SOURCE = "audit"

# 소스 → (Kafka 키 함수, 대상 필드로 따로 만드는 payload 필드). 키 함수는 collector/runner.py KEY_FUNCS 와 같다
ENTITY_FIELDS = {
    "seoul_traffic": {"link_id"},
    "bus_position": {"route_id", "veh_id", "plain_no"},
    "bus_arrival": {"route_id", "route_name", "station_id", "station_ars_id", "station_name", "bus_station_name",
                    "station_order", "slot", "veh_id", "plain_no", "first_bus_time", "last_bus_time"},
    "kma_ncst": {"nx", "ny", "category", "value", "value_raw", "category_name", "unit"},
    "seoul_incident": {"acc_id", "occurred_at", "expected_clear_at", "content_hash", "description", "link_id"},
}
KEY_FUNCS = {
    "seoul_traffic": lambda p: p["link_id"],
    "seoul_incident": lambda p: p["acc_id"],
    "bus_position": lambda p: p["veh_id"],
    "bus_arrival": lambda p: f"{p['route_id']}|{p['station_id']}",
    "kma_ncst": lambda p: f"{p['nx']}|{p['ny']}",
}


# ------------------------------------------------------------------ 공통

def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def quantiles(vals: list[float], n: int = QN) -> list[float]:
    """선형 보간 분위수 n개 (0%~100%)."""
    if not vals:
        return []
    s = sorted(vals)
    out = []
    for i in range(n):
        x = i * (len(s) - 1) / (n - 1)
        lo = int(math.floor(x))
        hi = min(lo + 1, len(s) - 1)
        out.append(s[lo] + (s[hi] - s[lo]) * (x - lo))
    return out


def sample_q(q: list[float], rng: random.Random) -> float:
    """분위수 표에서 하나 뽑는다 (구간 안은 선형)."""
    if not q:
        return 0.0
    if len(q) == 1:
        return q[0]
    x = rng.random() * (len(q) - 1)
    i = int(x)
    if i >= len(q) - 1:
        return q[-1]
    return q[i] + (q[i + 1] - q[i]) * (x - i)


def kst_day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=KST)


def _to_kst(ts) -> Optional[datetime]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(KST)


# ------------------------------------------------------------------ 프로파일

def _field_spec(name: str, typ: str, values: list) -> dict:
    n = len(values)
    nn = [v for v in values if v is not None]
    spec: dict[str, Any] = {"t": None, "null": round(1 - len(nn) / n, 4) if n else 1.0}
    if typ in ("long", "double"):
        spec.update(t="num", int=typ == "long", q=[round(float(x), 6) for x in quantiles([float(v) for v in nn])])
    elif typ == "boolean":
        spec.update(t="bool", p=round(sum(1 for v in nn if v) / len(nn), 4) if nn else 0.0)
    elif typ == "string":
        c = Counter(nn)
        if 0 < len(c) <= TOP_MAX:
            spec.update(t="cat", top=[[k, round(v / len(nn), 6)] for k, v in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))])
        else:
            spec.update(t="text")
    else:
        spec.update(t="time")
    return spec


def _bronze_files(bronze_root: Path, source: str, d: date) -> list[Path]:
    p = bronze_root / source / f"event_date={d.isoformat()}"
    return sorted(f for f in p.glob("*.parquet") if not f.name.startswith((".", "_"))) if p.is_dir() else []


def _read_rows(files: list[Path]) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    cols = ["ingest_id", "event_time", "fetched_at", "latency_ms", "_parse_ok", "payload"]
    tables = []
    for f in files:
        try:
            t = pq.read_table(str(f))
        except Exception:                                          # noqa: BLE001 (깨진 파일은 Bronze 배치도 건너뛴다)
            continue
        tables.append(t.select([c for c in cols if c in t.column_names]))
    if not tables:
        return []
    return pa.concat_tables(tables, promote_options="permissive").to_pylist()


def profile_source(source: str, rows: list[dict], files: list[Path]) -> dict:
    ok = [r for r in rows if r.get("_parse_ok")]
    by_call: dict[str, int] = Counter(r["ingest_id"] for r in ok)
    call_hour: dict[str, int] = {}
    for r in ok:
        call_hour.setdefault(r["ingest_id"], _to_kst(r["fetched_at"]).hour)
    hours = [0] * 24
    for h in call_hour.values():
        hours[h] += 1
    rpc = sorted(by_call.values())
    lag = [(r["fetched_at"] - r["event_time"]).total_seconds() for r in ok if r.get("fetched_at") and r.get("event_time")]
    lat_by_call = {r["ingest_id"]: r.get("latency_ms") for r in ok}
    pays = [r["payload"] or {} for r in ok]
    fields = {}
    skip = ENTITY_FIELDS[source]
    for f in schema_of(source)["payload"]:
        if f["name"] not in skip:
            fields[f["name"]] = _field_spec(f["name"], f["type"], [p.get(f["name"]) for p in pays])
    prof: dict[str, Any] = {
        "rows": len(rows), "rows_parse_ok": len(ok), "files": len(files), "bytes": sum(f.stat().st_size for f in files),
        "calls": len(by_call), "calls_by_hour": hours,
        "records_per_call": {"median": rpc[len(rpc) // 2] if rpc else 0, "p90": rpc[int(len(rpc) * 0.9)] if rpc else 0},
        "lag_sec_q": [round(x, 3) for x in quantiles(lag)],
        "latency_ms_q": [round(float(x), 1) for x in quantiles([float(v) for v in lat_by_call.values() if v is not None])],
        "fields": fields,
    }
    if source == "seoul_traffic":
        prof["entities"] = sorted({p["link_id"] for p in pays if p.get("link_id")})
    elif source == "bus_position":
        prof["routes"] = sorted({p["route_id"] for p in pays if p.get("route_id")})
        prof["vehicles"] = len({p["veh_id"] for p in pays if p.get("veh_id")})
    elif source == "bus_arrival":
        ent = {(p["route_id"], p["station_id"], int(p["station_order"]), p.get("station_ars_id"), int(p["slot"]))
               for p in pays if p.get("route_id") and p.get("station_id") and p.get("station_order") is not None and p.get("slot") is not None}
        prof["entities"] = [list(e) for e in sorted(ent, key=lambda e: (e[0], e[2], e[4]))]
        prof["vehicles"] = len({p["veh_id"] for p in pays if p.get("veh_id")})
    elif source == "kma_ncst":
        prof["grids"] = sorted({(int(p["nx"]), int(p["ny"])) for p in pays if p.get("nx") is not None})
        per_cat: dict[str, list[float]] = defaultdict(list)
        for p in pays:
            if p.get("category") and p.get("value") is not None:
                per_cat[p["category"]].append(float(p["value"]))
        prof["categories"] = {c: [round(x, 4) for x in quantiles(v)] for c, v in sorted(per_cat.items())}
        prof["grids"] = [list(g) for g in prof["grids"]]
    elif source == "seoul_incident":
        seen: dict[str, list[datetime]] = defaultdict(list)
        for r in ok:
            seen[(r["payload"] or {}).get("acc_id")].append(r["fetched_at"])
        life = [(max(v) - min(v)).total_seconds() for k, v in seen.items() if k]
        prof["incidents"] = len([k for k in seen if k])
        prof["lifetime_sec_q"] = [round(x, 1) for x in quantiles(life)]
        prof["link_null"] = round(sum(1 for p in pays if not p.get("link_id")) / len(pays), 4) if pays else 1.0
        clear = [(p["expected_clear_at"] - p["occurred_at"]).total_seconds() for p in pays
                 if p.get("expected_clear_at") and p.get("occurred_at")]
        prof["clear_after_sec_q"] = [round(x, 1) for x in quantiles(clear)]
        prof["clear_null"] = round(sum(1 for p in pays if not p.get("expected_clear_at")) / len(pays), 4) if pays else 1.0
    return prof


def build_profile(bronze_root: Path, d: date) -> dict:
    out: dict[str, Any] = {"profile_date": d.isoformat(), "generator_version": GENERATOR_VERSION,
                           "created_at": datetime.now(KST).isoformat(timespec="seconds"), "sources": {}}
    for source in SOURCES:
        files = _bronze_files(bronze_root, source, d)
        out["sources"][source] = profile_source(source, _read_rows(files), files)
    af = _bronze_files(bronze_root, "audit", d)
    out["audit"] = {"files": len(af), "bytes": sum(f.stat().st_size for f in af)}
    return out


# ------------------------------------------------------------------ 생성: 값

def gen_value(spec: dict, rng: random.Random, name: str = "") -> Any:
    if spec.get("null", 0) and rng.random() < spec["null"]:
        return None
    t = spec.get("t")
    if t == "num":
        v = sample_q(spec.get("q") or [0.0], rng)
        return int(round(v)) if spec.get("int") else round(v, 6)
    if t == "bool":
        return rng.random() < spec.get("p", 0.0)
    if t == "cat":
        top = spec.get("top") or [["", 1.0]]
        x, acc = rng.random() * sum(w for _, w in top), 0.0
        for v, w in top:
            acc += w
            if x <= acc:
                return v
        return top[-1][0]
    if t == "text":
        return f"syn-{name}"
    return None


def scaled_ids(base: list[str], scale: int) -> list[str]:
    """원래 ID + 'S<j>-<ID>' (j = 1..scale-1)."""
    return list(base) + [f"S{j}-{b}" for j in range(1, scale) for b in base]


@dataclasses.dataclass
class Call:
    source: str
    fetched_at: datetime
    ingest_id: str
    latency_ms: int
    payloads: list[tuple[datetime, str, Any]]       # (event_time, dedup_key, payload 객체)


def call_times(hours: list[int], n_mult: float, d: date, rng: random.Random) -> list[datetime]:
    """시간대별 호출 수 × n_mult 만큼 그 시간 안에 고르게(약간의 흔들림) 둔다."""
    out = []
    base = kst_day_start(d)
    for h, n in enumerate(hours):
        k = int(round(n * n_mult))
        for i in range(k):
            sec = (i + 0.5 + (rng.random() - 0.5) * 0.4) * 3600 / k
            out.append(base + timedelta(hours=h, seconds=min(max(sec, 0.0), 3599.999)))
    return sorted(out)


def _floor_minute(t: datetime) -> datetime:
    return t.replace(second=0, microsecond=0)


def _floor_second(t: datetime) -> datetime:
    return t.replace(microsecond=0)


def gen_calls(source: str, prof: dict, scale: int, d: date, rng: random.Random, run_id: str) -> Iterator[Call]:
    """한 소스의 하루치 호출을 시간 순서로."""
    fields = prof.get("fields", {})
    rpc = max(1, int(prof.get("records_per_call", {}).get("median") or 1))
    per_call_source = rpc <= 1                     # 호출마다 1건(대상마다 호출) → 대상이 늘면 호출이 는다
    times = call_times(prof["calls_by_hour"], scale if per_call_source else 1, d, rng)
    lat_q = prof.get("latency_ms_q") or [100.0]
    lag_q = prof.get("lag_sec_q") or [0.0]
    generic = {k: v for k, v in fields.items()}
    short = {"seoul_traffic": "trf", "seoul_incident": "inc", "bus_position": "pos", "bus_arrival": "arr", "kma_ncst": "wea"}[source]

    def gp(name: str) -> Any:
        return gen_value(generic[name], rng, name) if name in generic else None

    if source == "seoul_traffic":
        pool = scaled_ids(prof.get("entities") or ["L0"], scale)
    elif source == "bus_position":
        routes = prof.get("routes") or ["R0"]
        pool = [f"SV{j:05d}" if j >= int(prof.get("vehicles") or 1) else f"V{j:05d}" for j in range(int(prof.get("vehicles") or 1) * scale)]
    elif source == "bus_arrival":
        base_e = [tuple(e) for e in (prof.get("entities") or [["R0", "ST0", 1, None, 1]])]
        pool = list(base_e) + [(r, f"S{j}-{s}", o, (f"S{j}-{a}" if a else None), sl) for j in range(1, scale) for r, s, o, a, sl in base_e]
    elif source == "kma_ncst":
        grids = [tuple(g) for g in (prof.get("grids") or [[60, 127]])]
        grids = grids + [(nx + j * 1000, ny) for j in range(1, scale) for nx, ny in grids]
        cats = prof.get("categories") or {c: [0.0] for c in weather.CATEGORIES}
        pool = [(nx, ny, c) for nx, ny in grids for c in sorted(cats)]
    else:
        n_inc = max(1, int(round((prof.get("incidents") or 0) * scale))) if prof.get("incidents") else 0
        life_q = prof.get("lifetime_sec_q") or [3600.0, 4 * 3600.0]
        clear_q = prof.get("clear_after_sec_q") or [3600.0]
        links = scaled_ids(prof.get("_links") or [], scale)
        day0 = kst_day_start(d)
        inc_pool = []
        for i in range(n_inc):
            start = day0 + timedelta(seconds=rng.random() * 23 * 3600)
            life = max(300.0, sample_q(life_q, rng))
            occurred = _floor_minute(start - timedelta(seconds=rng.random() * 1800))
            clear = None if rng.random() < float(prof.get("clear_null", 0.0)) else _floor_minute(occurred + timedelta(seconds=max(60.0, sample_q(clear_q, rng))))
            link = None if (not links or rng.random() < float(prof.get("link_null", 1.0))) else links[rng.randrange(len(links))]
            inc_pool.append({"acc_id": f"S{i:06d}", "start": start, "end": start + timedelta(seconds=life), "occurred_at": occurred,
                             "expected_clear_at": clear, "link_id": link, "acc_type": gp("acc_type") or "A01",
                             "acc_detail_type": gp("acc_detail_type"), "tm_x": gp("tm_x"), "tm_y": gp("tm_y"),
                             "road_code": gp("road_code"), "description": f"합성 돌발 {i}"})
        pool = inc_pool

    cursor = 0
    for n, t in enumerate(times):
        ingest_id = f"syn-{run_id}-{short}-{n:06d}"
        lat = max(1, int(round(sample_q(lat_q, rng))))
        out: list[tuple[datetime, str, Any]] = []
        if source == "seoul_traffic":
            for _ in range(rpc):
                link = pool[cursor % len(pool)]
                cursor += 1
                et = _floor_minute(t)
                p = traffic.TrafficSpeed(link_id=link, speed_kmh=gp("speed_kmh"), travel_time_sec=gp("travel_time_sec"))
                out.append((et, traffic.dedup_key(link, et), p))
        elif source == "bus_position":
            k = max(1, int(round(rpc * scale)))
            for j in range(k):
                veh = pool[(cursor + j) % len(pool)]
                et = _floor_second(t - timedelta(seconds=max(0.0, sample_q(lag_q, rng))))
                kw = {f.name: gp(f.name) for f in dataclasses.fields(bus_position.BusPosition)}
                kw.update(route_id=routes[j % len(routes)], veh_id=veh, plain_no=f"합성{veh[-4:]}")
                out.append((et, bus_position.dedup_key(veh, second_key(et)), bus_position.BusPosition(**kw)))
            cursor += k
        elif source == "bus_arrival":
            k = max(1, int(round(rpc * scale)))
            for j in range(k):
                r, st, order, ars, slot = pool[(cursor + j) % len(pool)]
                et = _floor_second(t - timedelta(seconds=max(0.0, sample_q(lag_q, rng))))
                kw = {f.name: gp(f.name) for f in dataclasses.fields(bus_arrival.BusArrivalPrediction)}
                kw.update(route_id=r, route_name=None, station_id=st, station_ars_id=ars, station_name=f"정류장{order}",
                          bus_station_name=None, station_order=int(order), slot=int(slot), veh_id=f"V{(order * 7 + slot) % 997:05d}",
                          plain_no=None, first_bus_time=kst_day_start(d) + timedelta(hours=4),
                          last_bus_time=kst_day_start(d) + timedelta(hours=22, minutes=30))
                out.append((et, bus_arrival.dedup_key(r, st, int(order), int(slot), et), bus_arrival.BusArrivalPrediction(**kw)))
            cursor += k
        elif source == "kma_ncst":
            lag = max(0.0, sample_q(lag_q, rng))
            et = (t - timedelta(seconds=lag)).replace(minute=0, second=0, microsecond=0)
            for nx, ny, c in pool:
                v = round(sample_q(cats[c], rng), 1)
                name, unit = weather.CATEGORIES.get(c, (None, None))
                p = weather.WeatherObservation(nx=nx, ny=ny, category=c, value_raw=str(v), value=v, category_name=name, unit=unit)
                out.append((et, weather.dedup_key(nx, ny, c, et), p))
        else:
            for inc in pool:
                if not (inc["start"] <= t < inc["end"]):
                    continue
                chash = hashlib.sha256(f"{inc['acc_id']}|{inc['occurred_at']}|{inc['expected_clear_at']}".encode()).hexdigest()[:16]
                kw = {f.name: gp(f.name) for f in dataclasses.fields(incident.Incident)}
                kw.update({k: inc[k] for k in ("acc_id", "occurred_at", "expected_clear_at", "link_id", "acc_type",
                                               "acc_detail_type", "tm_x", "tm_y", "road_code", "description")}, content_hash=chash)
                out.append((inc["occurred_at"], incident.dedup_key(inc["acc_id"], chash), incident.Incident(**kw)))
        yield Call(source, t, ingest_id, lat, out)


# ------------------------------------------------------------------ 생성: Bronze 행·파일

def arrow_type(t: str):
    import pyarrow as pa
    return {"string": pa.string(), "long": pa.int64(), "double": pa.float64(), "boolean": pa.bool_(),
            "timestamp": pa.timestamp("us", tz="UTC")}[t]


def bronze_arrow_schema(source: str):
    """streaming.bronze.to_bronze_typed 와 같은 열 이름·순서·타입 (event_date 는 폴더). tests/spark/test_synth_bronze.py 가 대조한다."""
    import pyarrow as pa
    s = schema_of(source)
    env = [pa.field(f["name"], arrow_type(f["type"])) for f in s["envelope"]]
    payload = pa.field("payload", pa.struct([pa.field(f["name"], arrow_type(f["type"])) for f in s["payload"]]))
    tail = [pa.field("_raw_value", pa.string()), pa.field("_kafka_topic", pa.string()), pa.field("_kafka_partition", pa.int32()),
            pa.field("_kafka_offset", pa.int64()), pa.field("_kafka_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("_kafka_key", pa.string()), pa.field("_parse_ok", pa.bool_()), pa.field("_parse_error", pa.string())]
    return pa.schema(env + [payload] + tail)


def audit_arrow_schema():
    import pyarrow as pa
    return pa.schema([pa.field("_raw_value", pa.string()), pa.field("_kafka_topic", pa.string()), pa.field("_kafka_partition", pa.int32()),
                      pa.field("_kafka_offset", pa.int64()), pa.field("_kafka_timestamp", pa.timestamp("us", tz="UTC")),
                      pa.field("_kafka_key", pa.string()), pa.field("ingested_at", pa.timestamp("us", tz="UTC"))])


def _payload_dict(p: Any) -> dict:
    return {f.name: getattr(p, f.name) for f in dataclasses.fields(p)}


def _utc(t: Optional[datetime]) -> Optional[datetime]:
    return t.astimezone(UTC) if t is not None else None


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class Writer:
    """(표, 파티션 날짜, 버킷) 별로 행을 모았다가 버킷이 지나면 파일로 쓴다. 메모리는 버킷 몇 개 분량만 든다."""

    def __init__(self, out_root: Path, run_id: str, layout: str, meta: dict):
        self.out_root, self.run_id, self.layout, self.meta = out_root, run_id, layout, meta
        self.buckets: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        self.files: list[dict] = []

    def bucket(self, t_kst: datetime) -> str:
        """적재 시각(KST)의 분 또는 시. 파일은 (표, 파티션 날짜, 이 값) 마다 하나다."""
        return f"{t_kst:%Y%m%d%H%M}" if self.layout == "minute" else f"{t_kst:%Y%m%d%H}00"

    def add(self, table: str, part: date, t_kst: datetime, row: dict) -> None:
        self.buckets[(table, part.isoformat(), self.bucket(t_kst))].append(row)

    def flush(self, before: Optional[str] = None, schema_for=None) -> None:
        """before 가 있으면 그보다 앞선 버킷만 쓴다 (그 뒤 버킷은 아직 행이 더 올 수 있다)."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        for key in sorted(list(self.buckets)):
            table, part, b = key
            if before is not None and b >= before:
                continue
            rows = self.buckets.pop(key)
            schema = schema_for(table).with_metadata({b"urbanstream.synthetic": b"true", b"urbanstream.run_id": self.run_id.encode(),
                                                      b"urbanstream.generator_version": GENERATOR_VERSION.encode()})
            d = self.out_root / table / f"event_date={part}"
            d.mkdir(parents=True, exist_ok=True)
            path = d / f"part-{b}-syn.parquet"
            if path.exists():                                     # 같은 버킷을 두 번 쓰지 않는다(덮어쓰기 방지)
                raise RuntimeError(f"같은 파일을 두 번 쓰려 함: {path}")
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), str(path), compression="snappy")
            self.files.append({"path": str(path.relative_to(self.out_root.parent)), "rows": len(rows), "bytes": path.stat().st_size,
                               "sha256": _sha(path.read_bytes())})


def generate(profile: dict, *, scale: int, d: date, seed: int, run_id: str, out_dir: Path, layout: str = "minute",
             topics: Optional[dict[str, tuple[str, int]]] = None, sources: Optional[list[str]] = None) -> dict:
    """out_dir = data/measure/synthetic/<run_id>. 호출 쪽이 가드를 이미 통과시켰다고 본다(main 참고). 반환 = manifest."""
    if layout not in LAYOUTS:
        raise ValueError(f"layout 은 {LAYOUTS} 중 하나: {layout}")
    if scale < 1:
        raise ValueError("scale 은 1 이상")
    topics = topics or default_topics()
    bronze = out_dir / "bronze"
    w = Writer(bronze, run_id, layout, {})
    audit_topic, audit_parts = topics[AUDIT_SOURCE]
    audit_off = 0
    offsets: dict[tuple[str, int], int] = defaultdict(int)
    schemas = {s: bronze_arrow_schema(s) for s in SOURCES}
    schemas[AUDIT_SOURCE] = audit_arrow_schema()
    srcs = sources or list(SOURCES)
    prof_sources = profile["sources"]
    links = prof_sources.get("seoul_traffic", {}).get("entities") or []
    audit_rows: list[tuple[datetime, dict]] = []
    for source in srcs:
        prof = dict(prof_sources[source])
        if source == "seoul_incident":
            prof["_links"] = links
        if not prof.get("calls"):
            continue
        rng = random.Random(f"{seed}|{source}|{scale}|{d.isoformat()}")
        topic, nparts = topics[source]
        version = SOURCES[source].schema_version
        for call in gen_calls(source, prof, scale, d, rng, run_id):
            ctx = CallContext(fetched_at=call.fetched_at, latency_ms=call.latency_ms, raw_uri=f"synthetic://{run_id}", ingest_id=call.ingest_id)
            for et, dk, payload in call.payloads:
                rec = ctx.record(source, version, et, dk, payload)
                pd = _payload_dict(payload)
                key = KEY_FUNCS[source](pd)
                part = zlib.crc32(key.encode()) % nparts
                off = offsets[(topic, part)]
                offsets[(topic, part)] += 1
                ingested = call.fetched_at + timedelta(seconds=1 + rng.random() * 59)
                env = rec.envelope
                row = {f.name: getattr(env, f.name) for f in dataclasses.fields(env)}
                for k in ("event_time", "fetched_at"):
                    row[k] = _utc(row[k])
                row["ingested_at"] = _utc(ingested)
                row["payload"] = {k: (_utc(v) if isinstance(v, datetime) else v) for k, v in pd.items()}
                row.update(_raw_value=_json(rec.to_dict()), _kafka_topic=topic, _kafka_partition=part, _kafka_offset=off,
                           _kafka_timestamp=_utc(et), _kafka_key=key, _parse_ok=True, _parse_error=None)
                w.add(source, et.astimezone(KST).date(), ingested.astimezone(KST), row)
            recorded = call.fetched_at + timedelta(milliseconds=call.latency_ms + 50)
            audit_rows.append((recorded, {"event": "call", "recorded_at": recorded.isoformat(timespec="milliseconds"),
                                          "source_system": source, "target": f"{source}:syn", "final": True, "outcome": "ok",
                                          "ingest_id": call.ingest_id, "fetched_at": call.fetched_at.isoformat(), "http_status": 200,
                                          "latency_ms": call.latency_ms, "status_kind": "ok", "n_records": len(call.payloads),
                                          "truncated": False, "raw_uri": f"synthetic://{run_id}"}))
            # 다음 버킷으로 넘어간 행이 생길 수 있으니 한 버킷 앞까지만 쓴다
            cur = (call.fetched_at.astimezone(KST) - timedelta(minutes=2 if layout == "minute" else 61))
            w.flush(before=w.bucket(cur), schema_for=lambda t: schemas[t])
        w.flush(schema_for=lambda t: schemas[t])
    for recorded, msg in sorted(audit_rows, key=lambda x: (x[0], x[1]["ingest_id"])):
        ing = recorded + timedelta(seconds=30)
        w.add(AUDIT_SOURCE, recorded.astimezone(KST).date(), ing.astimezone(KST),
              {"_raw_value": _json(msg), "_kafka_topic": audit_topic, "_kafka_partition": 0, "_kafka_offset": audit_off,
               "_kafka_timestamp": _utc(recorded), "_kafka_key": msg["source_system"], "ingested_at": _utc(ing)})
        audit_off += 1
    w.flush(schema_for=lambda t: schemas[t])
    files = sorted(w.files, key=lambda f: f["path"])
    per_table: dict[str, dict] = {}
    for f in files:
        t = Path(f["path"]).parts[1]
        x = per_table.setdefault(t, {"files": 0, "rows": 0, "bytes": 0, "partitions": set()})
        x["files"] += 1
        x["rows"] += f["rows"]
        x["bytes"] += f["bytes"]
        x["partitions"].add(Path(f["path"]).parts[2].split("=", 1)[1])
    for x in per_table.values():
        x["partitions"] = sorted(x["partitions"])
    manifest = {"synthetic": True, "run_id": run_id, "generator_version": GENERATOR_VERSION, "seed": seed, "scale": scale,
                "date": d.isoformat(), "layout": layout, "profile_date": profile.get("profile_date"),
                "profile_sha256": _sha(_json(profile).encode()), "tables": per_table,
                "total": {"files": len(files), "rows": sum(f["rows"] for f in files), "bytes": sum(f["bytes"] for f in files)},
                "content_sha256": _sha("\n".join(f"{f['path']} {f['sha256']}" for f in files).encode()),
                "files": files, "limits": ["모든 행 _parse_ok=true (실제의 손상 행·DLQ 는 만들지 않는다)",
                                           "늘린 링크(S 접두)는 dim_link 에 없다: 링크 조인 마트는 재지 않는다"]}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return manifest


def default_topics(config_dir: Path = ROOT / "config") -> dict[str, tuple[str, int]]:
    import yaml
    src = yaml.safe_load((config_dir / "sources.yaml").read_text(encoding="utf-8"))["sources"]
    top = yaml.safe_load((config_dir / "topics.yaml").read_text(encoding="utf-8"))
    out = {name: (c["topic"], int(top["topics"][c["topic"]]["partitions"])) for name, c in src.items()}
    out[AUDIT_SOURCE] = (top["audit_topic"], int(top["topics"][top["audit_topic"]]["partitions"]))
    return out


def estimate_bytes(profile: dict, scale: int) -> int:
    """예상 출력 크기(실제 하루 Bronze 바이트 × 규모). 실행 전 디스크 검사에 쓴다."""
    return int(sum(s.get("bytes", 0) for s in profile["sources"].values()) * scale + profile.get("audit", {}).get("bytes", 0))


# ------------------------------------------------------------------ CLI

def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "measure.yaml"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("profile")
    p.add_argument("--date", type=date.fromisoformat, default=None)
    p.add_argument("--bronze", default=str(ROOT / "data" / "bronze"))
    p.add_argument("--out", default=None)
    g = sub.add_parser("generate")
    g.add_argument("--profile", required=True)
    g.add_argument("--scale", type=int, default=1)
    g.add_argument("--date", type=date.fromisoformat, default=None)
    g.add_argument("--seed", type=int, default=20260105)
    g.add_argument("--run-id", required=True)
    g.add_argument("--layout", choices=LAYOUTS, default="minute")
    g.add_argument("--out-root", default=None, help="기본 config paths.synthetic")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    if a.cmd == "profile":
        d = a.date or date.fromisoformat(cfg["synthetic"]["profile_date"])
        out = ensure_measure_path(a.out or Path(cfg["paths"]["profile"]) / f"{d.isoformat()}.json", cfg)
        prof = build_profile(Path(a.bronze), d)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(prof, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"프로파일: {out}  " + ", ".join(f"{k} {v['rows_parse_ok']}행/{v['calls']}호출" for k, v in prof["sources"].items()))
        return 0
    if not all(c.isalnum() or c in "-_" for c in a.run_id):
        raise SystemExit("run-id 는 영문·숫자·-·_ 만")
    d = ensure_synthetic_date(a.date or date.fromisoformat(cfg["synthetic"]["first_date"]), cfg)
    out_dir = ensure_measure_path(Path(a.out_root or cfg["paths"]["synthetic"]) / a.run_id, cfg)
    if (out_dir / "bronze").exists():
        raise SystemExit(f"이미 있습니다: {out_dir}/bronze (다른 run-id 를 쓰거나 지운다)")
    prof = json.loads(Path(a.profile).read_text(encoding="utf-8"))
    m = generate(prof, scale=a.scale, d=d, seed=a.seed, run_id=a.run_id, out_dir=out_dir, layout=a.layout)
    print(f"합성 Bronze: {out_dir}  {m['total']['rows']}행, 파일 {m['total']['files']}개, {m['total']['bytes'] / 1024 ** 2:.1f} MiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
