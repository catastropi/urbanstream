
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, quote_plus, unquote

import requests
from dotenv import load_dotenv

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "smoke"

ALL_APIS = ["seoul_traffic", "seoul_incident", "bus_position", "bus_arrival", "kma_ncst"]
SEOUL_APIS = {"seoul_traffic", "seoul_incident"}
BUS_APIS = {"bus_position", "bus_arrival"}
TAGO_BASE = "http://apis.data.go.kr/1613000"  # Phase 2(서울 외 지역) 예비용. 서울은 커버 안 됨 — 위 안내 참고.
SEOUL_BUS_BASE = "http://ws.bus.go.kr/api/rest"  # 서울시 버스운행정보 공유서비스 (TOPIS)

LABELS = {
    "seoul_traffic": "서울 실시간 도로 소통 정보 (TrafficInfo, data.seoul.go.kr)",
    "seoul_incident": "서울 실시간 돌발 정보 (AccInfo, data.seoul.go.kr)",
    "bus_position": "서울시 버스운행정보 공유서비스 - 버스위치 (ws.bus.go.kr/buspos/getBusPosByRtid)",
    "bus_arrival": "서울시 버스운행정보 공유서비스 - 버스도착 (ws.bus.go.kr/arrive/getArrInfoByRouteAll)",
    "kma_ncst": "기상청 단기예보 조회서비스 - 초단기실황 (getUltraSrtNcst)",
}

# 결과 코드 해석 (소스마다 체계가 다름)
SUCCESS_CODES = {"INFO-000", "00", "0", "NORMAL_CODE"}
NODATA_CODES = {"INFO-200", "03"}
CODE_KEYS = ("CODE", "code", "resultCode", "headerCd", "returnReasonCode", "errorCode")
MSG_KEYS = ("MESSAGE", "message", "resultMsg", "headerMsg", "returnAuthMsg", "errMsg")
RECORD_TAGS = ("item", "itemList", "row")

TIME_NAME_RE = re.compile(
    r"(time|_tm$|^tm|date|dttm|_dt$|tmfc|base_|occr|clr|reg_|upd|expect|exp_)", re.I
)
TIME_VALUE_RE = re.compile(r"^(19|20)\d{2}[-./]?\d{2}[-./]?\d{2}")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "urbanstream-smoke/0.1"})


# ----------------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------------
def load_config() -> dict:
    load_dotenv(ROOT / ".env", encoding="utf-8-sig")

    def g(name: str, default: str = "") -> str:
        return os.getenv(name, default).strip()

    seoul_key = g("SEOUL_API_KEY")
    dg_raw = g("DATA_GO_KR_KEY")
    # 공공데이터포털은 Encoding/Decoding 두 종류 키를 준다.
    # requests가 params를 다시 인코딩하므로 항상 Decoding(원본) 형태로 통일한다.
    dg_key = unquote(dg_raw) if "%" in dg_raw else dg_raw

    secrets = {s for s in (seoul_key, dg_raw, dg_key) if len(s) >= 4}
    for s in list(secrets):
        secrets.update({quote(s, safe=""), quote_plus(s), quote(s, safe="/")})

    return {
        "seoul_key": seoul_key,
        "dg_key": dg_key,
        "secrets": secrets,
        "seoul_link_id": g("SEOUL_TRAFFIC_LINK_ID"),
        "kma_nx": g("KMA_NX", "60"),
        "kma_ny": g("KMA_NY", "127"),
        # 서울 버스(메인 소스). route_id/stop_id는 'seoul-bus-ids' 명령으로 찾아서 채운다.
        "seoul_bus_route_no": g("SEOUL_BUS_ROUTE_NO", "143"),
        "seoul_bus_stop_name": g("SEOUL_BUS_STOP_NAME", "강남역"),
        "seoul_bus_route_id": g("SEOUL_BUS_ROUTE_ID"),
        "seoul_bus_stop_id": g("SEOUL_BUS_STOP_ID"),
        # TAGO(국토교통부) — Phase 2(서울 외 지역) 예비용. city_code/route_id/node_id는
        # 'tago-ids' 명령으로 찾아서 채운다. 서울 버스에는 쓰지 않는다(안내 참고).
        "tago_city_code": g("TAGO_CITY_CODE"),
        "tago_route_no": g("TAGO_ROUTE_NO", "143"),
        "tago_node_name": g("TAGO_NODE_NAME", "강남역"),
        "tago_route_id": g("TAGO_ROUTE_ID"),
        "tago_node_id": g("TAGO_NODE_ID"),
    }


def key_for(api: str, cfg: dict) -> str:
    return cfg["seoul_key"] if api in SEOUL_APIS else cfg["dg_key"]


def redact(text: str, cfg: dict) -> str:
    if not text:
        return text
    for s in sorted(cfg["secrets"], key=len, reverse=True):
        text = text.replace(s, "***KEY***")
    return text


def default_kma_base(now: datetime) -> tuple[str, str]:
    """현재 시각 기준 조회할 발표시각(정시). 아직 미생성일 수 있어 45분 전이면 직전 정시를 쓴다."""
    base = now.replace(minute=0, second=0, microsecond=0)
    if now.minute < 45:
        base -= timedelta(hours=1)
    return base.strftime("%Y%m%d"), base.strftime("%H%M")


def build_request(api: str, cfg: dict, kma_base: tuple[str, str] | None = None):
    """(url, params) 반환. 환경변수 <API명>_URL 이 있으면 그 URL을 우선 사용한다.

    override URL 안의 치환자: {KEY} {ROUTE_ID} {LINK_ID} {BASE_DATE} {BASE_TIME} {NX} {NY}
    """
    key = key_for(api, cfg)
    base_date, base_time = kma_base or default_kma_base(datetime.now(KST))

    override = os.getenv(f"{api.upper()}_URL", "").strip()
    if override:
        repl = {
            "{KEY}": quote(key, safe=""),
            "{BUS_ROUTE_ID}": cfg["seoul_bus_route_id"],
            "{BUS_STOP_ID}": cfg["seoul_bus_stop_id"],
            "{CITY_CODE}": cfg["tago_city_code"],
            "{ROUTE_ID}": cfg["tago_route_id"],
            "{NODE_ID}": cfg["tago_node_id"],
            "{LINK_ID}": cfg["seoul_link_id"],
            "{BASE_DATE}": base_date,
            "{BASE_TIME}": base_time,
            "{NX}": cfg["kma_nx"],
            "{NY}": cfg["kma_ny"],
        }
        for k, v in repl.items():
            override = override.replace(k, v)
        return override, None

    if api == "seoul_traffic":
        link = cfg["seoul_link_id"] or "1220003800"  # 샘플 링크(존재 여부 미확인)
        qk = quote(key, safe="")
        return f"http://openapi.seoul.go.kr:8088/{qk}/xml/TrafficInfo/1/5/{link}", None
    if api == "seoul_incident":
        qk = quote(key, safe="")
        return f"http://openapi.seoul.go.kr:8088/{qk}/xml/AccInfo/1/50/", None
    if api == "bus_position":
        # ws.bus.go.kr (서울시 버스운행정보 공유서비스): busRouteId(노선ID)에 속한 버스들의 실시간 위치
        # 이 API는 XML만 돌려준다(_type=json 같은 파라미터 없음) — 스크립트의 범용 XML 파서가 처리한다.
        return (
            f"{SEOUL_BUS_BASE}/buspos/getBusPosByRtid",
            {"ServiceKey": key, "busRouteId": cfg["seoul_bus_route_id"]},
        )
    if api == "bus_arrival":
        # ws.bus.go.kr: 정류소(stId)+노선(busRouteId) 기준 도착예정정보
        return (
            f"{SEOUL_BUS_BASE}/arrive/getArrInfoByRouteAll",
            {"ServiceKey": key, "busRouteId": cfg["seoul_bus_route_id"]},
        )
    if api == "kma_ncst":
        return (
            "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst",
            {
                "serviceKey": key, "pageNo": 1, "numOfRows": 100, "dataType": "JSON",
                "base_date": base_date, "base_time": base_time,
                "nx": cfg["kma_nx"], "ny": cfg["kma_ny"],
            },
        )
    raise ValueError(f"알 수 없는 API: {api}")


# ----------------------------------------------------------------------------
# 결과 구조
# ----------------------------------------------------------------------------
@dataclass
class CallResult:
    api: str
    called_at_kst: str = ""
    url_redacted: str = ""
    http_status: int | None = None
    latency_ms: int | None = None
    size_bytes: int | None = None
    content_type: str | None = None
    headers_of_interest: dict = field(default_factory=dict)
    fmt: str = "none"
    error: str | None = None
    code: str | None = None
    message: str | None = None
    n_records: int = 0
    fields: list = field(default_factory=list)
    time_candidates: list = field(default_factory=list)
    raw_path: str | None = None
    verdict: str = "FAIL"
    notes: list = field(default_factory=list)
    # 아래 둘은 리포트(JSON)에서 제외
    records: list = field(default_factory=list, repr=False)
    raw_text: str = field(default="", repr=False)


# ----------------------------------------------------------------------------
# 파싱
# ----------------------------------------------------------------------------
def parse_body(content: bytes, text: str):
    s = text.lstrip("﻿ \r\n\t")
    if s.startswith(("{", "[")):
        try:
            return "json", json.loads(s)
        except json.JSONDecodeError:
            return "json-invalid", None
    if s.startswith("<"):
        try:
            return "xml", ET.fromstring(content)
        except ET.ParseError:
            return "xml-invalid", None
    return "unknown", None


def find_records_json(obj) -> list:
    best: list = []

    def walk(o, key=None):
        nonlocal best
        if isinstance(o, list):
            if o and all(isinstance(x, dict) for x in o) and len(o) > len(best):
                best = o
            for x in o:
                walk(x)
        elif isinstance(o, dict):
            # 결과가 1건일 때 리스트 대신 dict로 오는 경우
            if key in RECORD_TAGS and not best and o and all(
                not isinstance(v, (dict, list)) for v in o.values()
            ):
                best = [o]
            for k, v in o.items():
                walk(v, k)

    walk(obj)
    return best


def find_records_xml(root) -> list:
    groups: dict[str, list] = {}
    for el in root.iter():
        kids = list(el)
        if kids and all(len(list(k)) == 0 for k in kids):
            groups.setdefault(el.tag, []).append({k.tag: (k.text or "").strip() for k in kids})
    if not groups:
        return []
    for tag in RECORD_TAGS:
        if tag in groups:
            return groups[tag]
    non_header = {t: v for t, v in groups.items() if "header" not in t.lower()}
    if not non_header:
        # 모든 그룹이 header류 태그뿐이면(에러 응답 등) 실제 데이터 레코드는 없다고 본다.
        return []
    pool = non_header
    best_tag = None
    for t, v in pool.items():  # 동률이면 나중에 나온 것을 선택
        if best_tag is None or len(v) >= len(pool[best_tag]):
            best_tag = t
    return pool[best_tag]


def find_status_json(obj) -> tuple[str | None, str | None]:
    """가장 얕은 위치의 결과 코드/메시지를 찾는다(BFS)."""
    code = msg = None
    queue = [obj]
    while queue and (code is None or msg is None):
        nxt = []
        for o in queue:
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, (dict, list)):
                        nxt.append(v)
                    elif code is None and k in CODE_KEYS and v not in (None, ""):
                        code = str(v)
                    elif msg is None and k in MSG_KEYS and v not in (None, ""):
                        msg = str(v)
            elif isinstance(o, list):
                nxt.extend(o[:3])  # 리스트는 앞쪽만 (레코드 내부 오탐 방지)
        queue = nxt
    return code, msg


def find_status_xml(root) -> tuple[str | None, str | None]:
    code = msg = None
    queue = [root]
    while queue and (code is None or msg is None):
        nxt = []
        for el in queue:
            for k in el:
                if len(list(k)):
                    nxt.append(k)
                else:
                    text = (k.text or "").strip()
                    if code is None and k.tag in CODE_KEYS and text:
                        code = text
                    elif msg is None and k.tag in MSG_KEYS and text:
                        msg = text
        queue = nxt
    return code, msg


def looks_like_time(name: str, value) -> bool:
    if name and TIME_NAME_RE.search(name):
        return True
    return isinstance(value, str) and bool(TIME_VALUE_RE.match(value))


def analyze_fields(records: list) -> tuple[list, list]:
    sample = records[:200]
    names: list[str] = []
    for r in sample:
        for k in r:
            if k not in names:
                names.append(k)
    fields, time_cands = [], []
    for n in names:
        vals = [r.get(n) for r in sample]
        empty = sum(1 for v in vals if v is None or v == "")
        first = next((v for v in vals if v not in (None, "")), None)
        info = {
            "name": n,
            "sample": (str(first)[:40] if first is not None else ""),
            "null_pct": round(100 * empty / len(vals), 1),
            "n_unique": len({json.dumps(v, sort_keys=True, ensure_ascii=False) for v in vals}),
        }
        fields.append(info)
        if looks_like_time(n, first):
            time_cands.append({"name": n, "sample": info["sample"]})
    return fields, time_cands


# ----------------------------------------------------------------------------
# 호출
# ----------------------------------------------------------------------------
def do_call(api: str, cfg: dict, save: bool = True, kma_base=None) -> CallResult:
    key = key_for(api, cfg)
    if not key:
        res = CallResult(api=api, called_at_kst=datetime.now(KST).isoformat(timespec="seconds"))
        res.error = "인증키가 .env에 없습니다"
        res.verdict = "SKIP"
        return res
    if api == "bus_position" and not cfg["seoul_bus_route_id"] \
            and not os.getenv(f"{api.upper()}_URL"):
        res = CallResult(api=api, called_at_kst=datetime.now(KST).isoformat(timespec="seconds"))
        res.error = "SEOUL_BUS_ROUTE_ID 가 .env에 없습니다 (먼저 'seoul-bus-ids' 명령 실행)"
        res.verdict = "SKIP"
        return res
    if api == "bus_arrival" and not cfg["seoul_bus_route_id"] \
            and not os.getenv(f"{api.upper()}_URL"):
        res = CallResult(api=api, called_at_kst=datetime.now(KST).isoformat(timespec="seconds"))
        res.error = "SEOUL_BUS_STOP_ID / SEOUL_BUS_ROUTE_ID 가 .env에 없습니다 (먼저 'seoul-bus-ids' 명령 실행)"
        res.verdict = "SKIP"
        return res

    url, params = build_request(api, cfg, kma_base)
    return raw_call(api, url, params, cfg, save=save)


def raw_call(tag: str, url: str, params: dict | None, cfg: dict, save: bool = True) -> CallResult:
    """임의의 (url, params)를 호출하고 공통 파싱/판정을 적용한다.

    do_call()은 ALL_APIS에 등록된 이름에 대해서만 쓸 수 있지만,
    raw_call()은 tago-ids처럼 미리 정의되지 않은 1회성 조회 호출에도 쓸 수 있다.
    """
    res = CallResult(api=tag, called_at_kst=datetime.now(KST).isoformat(timespec="seconds"))
    t0 = time.perf_counter()
    try:
        resp = SESSION.get(url, params=params, timeout=(5, 20))
    except requests.RequestException as e:
        res.latency_ms = int((time.perf_counter() - t0) * 1000)
        res.error = redact(f"{type(e).__name__}: {e}", cfg)
        res.url_redacted = redact(url, cfg)
        return res

    res.latency_ms = int((time.perf_counter() - t0) * 1000)
    res.url_redacted = redact(resp.url, cfg)
    res.http_status = resp.status_code
    res.size_bytes = len(resp.content)
    res.content_type = resp.headers.get("Content-Type")
    for h, v in resp.headers.items():
        hl = h.lower()
        if hl.startswith("x-ratelimit") or hl in ("retry-after", "date", "last-modified", "etag", "cache-control"):
            res.headers_of_interest[h] = v

    m = re.search(r"charset=([\w-]+)", res.content_type or "", re.I)
    try:
        text = resp.content.decode(m.group(1) if m else "utf-8", errors="replace")
    except LookupError:
        text = resp.content.decode("utf-8", errors="replace")
    text = redact(text, cfg)
    res.raw_text = text

    fmt, parsed = parse_body(resp.content, text)
    res.fmt = fmt
    if fmt == "json":
        res.records = find_records_json(parsed)
        res.code, res.message = find_status_json(parsed)
    elif fmt == "xml":
        res.records = find_records_xml(parsed)
        res.code, res.message = find_status_xml(parsed)
    else:
        res.notes.append("응답이 JSON/XML로 해석되지 않음 (앞 200자: %r)" % text[:200])

    res.n_records = len(res.records)
    if res.records:
        res.fields, res.time_candidates = analyze_fields(res.records)

    # 판정
    if res.http_status != 200:
        res.verdict = "FAIL"
    elif res.code is not None and res.code not in SUCCESS_CODES:
        res.verdict = "WARN" if res.code in NODATA_CODES else "FAIL"
    elif fmt not in ("json", "xml"):
        res.verdict = "FAIL"
    elif res.n_records == 0:
        res.verdict = "WARN"
        res.notes.append("호출은 성공했으나 레코드 0건 (시간대/파라미터 문제일 수 있음)")
    else:
        res.verdict = "PASS"

    if save:
        save_raw(res)
    return res


def save_raw(res: CallResult) -> None:
    ext = {"json": "json", "xml": "xml"}.get(res.fmt, "txt")
    d = OUT_DIR / "raw" / res.api
    d.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    p = d / f"{stamp}_KST.{ext}"
    p.write_text(res.raw_text, encoding="utf-8")
    res.raw_path = str(p.relative_to(ROOT))


# ----------------------------------------------------------------------------
# 리포트
# ----------------------------------------------------------------------------
def result_to_dict(r: CallResult) -> dict:
    d = asdict(r)
    d.pop("records", None)
    d.pop("raw_text", None)
    return d


def render_result_md(r: CallResult) -> str:
    L = [f"### {LABELS.get(r.api, r.api)}  →  **{r.verdict}**", ""]
    L.append(f"- 호출 시각(KST): {r.called_at_kst}")
    L.append(f"- HTTP: {r.http_status} / 지연: {r.latency_ms} ms / 크기: {r.size_bytes} B / 형식: {r.fmt}")
    L.append(f"- Content-Type: {r.content_type}")
    L.append(f"- 결과 코드: {r.code} / 메시지: {r.message}")
    L.append(f"- 레코드 수: {r.n_records}")
    if r.error:
        L.append(f"- 오류: {r.error}")
    if r.headers_of_interest:
        L.append(f"- 주요 헤더: {json.dumps(r.headers_of_interest, ensure_ascii=False)}")
    L.append(f"- 요청 URL(키 마스킹): `{r.url_redacted}`")
    if r.raw_path:
        L.append(f"- 원본 저장: `{r.raw_path}`")
    for n in r.notes:
        L.append(f"- 참고: {n}")
    if r.time_candidates:
        L.append("- **시각 후보 필드:** " + ", ".join(f"{c['name']}={c['sample']}" for c in r.time_candidates))
    elif r.n_records:
        L.append("- **시각 후보 필드: 없음**")
    if r.fields:
        L += ["", "| 필드 | 샘플 | null% | 고유값 수 |", "|---|---|---|---|"]
        for f in r.fields:
            L.append(f"| {f['name']} | {str(f['sample']).replace('|', '/')} | {f['null_pct']} | {f['n_unique']} |")
    L.append("")
    return "\n".join(L)


def env_header() -> str:
    return (
        f"- 실행 시각(KST): {datetime.now(KST).isoformat(timespec='seconds')}\n"
        f"- Python {platform.python_version()} / {platform.system()} {platform.release()}\n"
        f"- requests {requests.__version__}\n"
    )


# ----------------------------------------------------------------------------
# 명령: check-env
# ----------------------------------------------------------------------------
def cmd_check_env(_args) -> int:
    cfg = load_config()
    env_path = ROOT / ".env"
    print(f"프로젝트 루트: {ROOT}")
    print(f".env 파일: {'있음' if env_path.exists() else '없음 (.env.example 을 복사해 만드세요)'}")
    print(f"Python {platform.python_version()} / 콘솔 인코딩: {sys.stdout.encoding}")
    rows = [
        ("SEOUL_API_KEY", cfg["seoul_key"]),
        ("DATA_GO_KR_KEY", cfg["dg_key"]),
        ("SEOUL_BUS_ROUTE_ID", cfg["seoul_bus_route_id"]),
        ("SEOUL_BUS_STOP_ID", cfg["seoul_bus_stop_id"]),
        ("SEOUL_TRAFFIC_LINK_ID (선택)", cfg["seoul_link_id"]),
        ("TAGO_CITY_CODE (Phase 2 예비)", cfg["tago_city_code"]),
        ("TAGO_ROUTE_ID (Phase 2 예비)", cfg["tago_route_id"]),
        ("TAGO_NODE_ID (Phase 2 예비)", cfg["tago_node_id"]),
    ]
    for name, val in rows:
        print(f"  {name:32s}: {'설정됨 (길이 %d)' % len(val) if val else '비어 있음'}")
    print(f"  KMA_NX / KMA_NY                 : {cfg['kma_nx']} / {cfg['kma_ny']}")
    if not cfg["seoul_key"]:
        print("  참고: SEOUL_API_KEY 가 없으면 seoul_traffic / seoul_incident 는 계속 SKIP 됩니다.")
        print("        (data.seoul.go.kr 에서 별도로 발급받아야 합니다 — data.go.kr 키와는 다른 시스템)")
    if not (cfg["seoul_bus_route_id"] and cfg["seoul_bus_stop_id"]):
        print("  참고: SEOUL_BUS_* 값이 비어 있으면 'python scripts/smoke_test.py seoul-bus-ids' 로 먼저 조회하세요.")
    gi = ROOT / ".gitignore"
    ignored = gi.exists() and any(l.strip() == ".env" for l in gi.read_text(encoding="utf-8").splitlines())
    print(f".gitignore 에 .env 포함: {'예' if ignored else '아니오  <-- 반드시 추가하세요'}")
    if "%" in os.getenv("DATA_GO_KR_KEY", ""):
        print("참고: DATA_GO_KR_KEY 에 '%' 가 있어 Encoding 키로 보입니다. 자동으로 Decoding 형태로 변환해 사용합니다.")
    return 0


# ----------------------------------------------------------------------------
# 명령: run
# ----------------------------------------------------------------------------
def cmd_run(args) -> int:
    cfg = load_config()
    apis = [a.strip() for a in args.only.split(",")] if args.only else ALL_APIS
    bad = [a for a in apis if a not in ALL_APIS]
    if bad:
        print(f"알 수 없는 API: {bad}. 사용 가능: {ALL_APIS}")
        return 2

    results: list[CallResult] = []
    for api in apis:
        print(f"[{api}] 호출 중 ...", flush=True)
        r = do_call(api, cfg, save=True)
        results.append(r)
        print(f"   -> {r.verdict}  http={r.http_status} code={r.code} records={r.n_records} {r.latency_ms}ms"
              + (f"  ({r.error})" if r.error else ""))
        time.sleep(1.0)

    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    md = ["# UrbanStream Step 0 스모크 테스트 리포트", "", env_header(), "## 요약", "",
          "| API | 판정 | HTTP | 코드 | 레코드 | 지연(ms) |", "|---|---|---|---|---|---|"]
    for r in results:
        md.append(f"| {r.api} | {r.verdict} | {r.http_status} | {r.code} | {r.n_records} | {r.latency_ms} |")
    md += ["", "## 상세", ""] + [render_result_md(r) for r in results]
    md_text = "\n".join(md)
    (OUT_DIR / f"report_{stamp}.md").write_text(md_text, encoding="utf-8")
    (OUT_DIR / f"report_{stamp}.json").write_text(
        json.dumps([result_to_dict(r) for r in results], ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 70)
    print(f"리포트 저장: data/smoke/report_{stamp}.md")
    print("아래 구분선 사이 내용을 통째로 복사해서 채팅에 붙여넣어 주세요 (키는 마스킹되어 있습니다).")
    print("=" * 70)
    print(md_text)
    print("=" * 70)
    return 0


# ----------------------------------------------------------------------------
# 명령: cadence  (소스의 실제 갱신 주기 측정)
# ----------------------------------------------------------------------------
def _records_hash(records: list) -> str:
    return hashlib.sha1(json.dumps(records, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _changed_fraction(prev: list, cur: list) -> float:
    if not prev or not cur or len(prev) != len(cur):
        return 1.0
    diff = sum(1 for a, b in zip(prev, cur) if a != b)
    return diff / len(cur)


def cmd_cadence(args) -> int:
    cfg = load_config()
    api = args.api
    if api not in ALL_APIS:
        print(f"알 수 없는 API: {api}. 사용 가능: {ALL_APIS}")
        return 2
    if api == "kma_ncst":
        print("기상청 초단기실황은 정시 발표라 cadence 대신 'kma-lag' 명령을 사용하세요.")
        return 2
    if args.interval < 10:
        print("interval 은 10초 이상으로 설정하세요 (초당 제한 보호).")
        return 2
    planned = int(args.minutes * 60 / args.interval) + 1
    if api in BUS_APIS and planned > 150 and not args.force:
        print(f"버스 API는 개발계정 일 1,000건 제한이 있습니다. 계획된 호출 {planned}건은 과합니다.")
        print("interval 을 늘리거나 minutes 를 줄이세요. (무시하려면 --force)")
        return 2

    print(f"[{api}] {args.minutes}분 동안 {args.interval}초 간격으로 약 {planned}회 호출합니다. (Ctrl+C 로 중단 가능)")
    end = time.monotonic() + args.minutes * 60
    next_t = time.monotonic()
    obs, latencies, change_times = [], [], []
    prev_records, prev_hash, errors, n = None, None, 0, 0
    try:
        while time.monotonic() <= end:
            r = do_call(api, cfg, save=False)
            n += 1
            now = datetime.now(KST)
            if r.verdict in ("PASS", "WARN") and r.http_status == 200:
                h = _records_hash(r.records)
                frac = 0.0 if prev_hash is None else _changed_fraction(prev_records, r.records)
                changed = prev_hash is not None and h != prev_hash
                first = prev_hash is None
                if r.latency_ms is not None:
                    latencies.append(r.latency_ms)
                obs.append({"t": now.isoformat(timespec="seconds"), "hash": h[:8],
                            "changed": changed, "changed_fraction": round(frac, 3), "records": r.n_records})
                if changed:
                    change_times.append(now)
                if first or changed:
                    save_raw(r)
                print(f"  {now:%H:%M:%S} records={r.n_records} hash={h[:8]} "
                      f"{'(첫 관측)' if first else ('변경 %.0f%%' % (frac * 100) if changed else '동일')}", flush=True)
                prev_hash, prev_records = h, r.records
            else:
                errors += 1
                print(f"  {now:%H:%M:%S} 실패: {r.verdict} http={r.http_status} code={r.code} {r.error or ''}", flush=True)
            next_t += args.interval
            time.sleep(max(0.0, next_t - time.monotonic()))
    except KeyboardInterrupt:
        print("\n중단됨. 지금까지의 결과로 리포트를 만듭니다.")

    diffs = [(b - a).total_seconds() for a, b in zip(change_times, change_times[1:])]
    lat_sorted = sorted(latencies)
    p95 = lat_sorted[int(0.95 * (len(lat_sorted) - 1))] if lat_sorted else None
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    L = [f"# cadence 리포트: {api}", "", env_header(),
         f"- 호출 {n}회 / 실패 {errors}회 / 내용 변경 {len(change_times)}회 / 호출 간격 {args.interval}s",
         f"- 지연(ms): 평균 {statistics.mean(latencies):.0f} / p95 {p95}" if latencies else "- 지연: 측정 없음"]
    if len(diffs) >= 2:
        L.append(f"- 변경 간격(초): 중앙값 {statistics.median(diffs):.0f} / 최소 {min(diffs):.0f} / 최대 {max(diffs):.0f}")
        L.append(f"- 해석: 소스의 갱신 주기는 대략 {statistics.median(diffs):.0f}초 부근 (측정 해상도 = {args.interval}초)")
    else:
        L.append("- 변경 횟수가 부족해 갱신 주기를 추정할 수 없음 (minutes 를 늘리거나 interval 을 줄여 재측정)")
    L += ["", "| 시각 | hash | 변경 | 변경 비율 | 레코드 |", "|---|---|---|---|---|"]
    for o in obs:
        L.append(f"| {o['t'][11:]} | {o['hash']} | {'Y' if o['changed'] else ''} | {o['changed_fraction']} | {o['records']} |")
    md = "\n".join(L)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"cadence_{api}_{stamp}.md").write_text(md, encoding="utf-8")
    print("\n" + "=" * 70)
    print(md)
    print("=" * 70)
    print(f"저장: data/smoke/cadence_{api}_{stamp}.md  (위 내용을 채팅에 붙여넣어 주세요)")
    return 0


# ----------------------------------------------------------------------------
# 명령: seoul-bus-ids  (서울 버스 busRouteId / stId 조회) — 버스 메인 소스
# ----------------------------------------------------------------------------
def cmd_seoul_bus_ids(args) -> int:
    cfg = load_config()
    if not cfg["dg_key"]:
        print("DATA_GO_KR_KEY 가 .env에 없습니다. 먼저 채워주세요.")
        return 2
    key = cfg["dg_key"]
    route_no = args.route_no or cfg["seoul_bus_route_no"]
    stop_name = args.stop_name or cfg["seoul_bus_stop_name"]

    print(f"1/2 노선 번호 '{route_no}' 로 busRouteId 조회 중 (getBusRouteList) ...")
    r1 = raw_call(
        "seoul_bus_route_lookup", f"{SEOUL_BUS_BASE}/busRouteInfo/getBusRouteList",
        {"ServiceKey": key, "strSrch": route_no}, cfg, save=True,
    )
    _print_lookup_result(f"노선 '{route_no}' 검색 결과 (busRouteId 값을 SEOUL_BUS_ROUTE_ID 에 넣으세요)", r1, "route")

    print(f"\n2/2 정류소 이름 '{stop_name}' 으로 stId 조회 중 (getStationByName) ...")
    r2 = raw_call(
        "seoul_bus_stop_lookup", f"{SEOUL_BUS_BASE}/stationinfo/getStationByName",
        {"ServiceKey": key, "stSrch": stop_name}, cfg, save=True,
    )
    _print_lookup_result(f"정류소 '{stop_name}' 검색 결과 (stId 값을 SEOUL_BUS_STOP_ID 에 넣으세요)", r2, "st")

    print("\n" + "=" * 70)
    print("위 2개 표에서 원하는 값을 골라 .env 에 아래처럼 채운 뒤 'run' 명령을 실행하세요:")
    print("  SEOUL_BUS_ROUTE_ID=<1/2 표에서 찾은 busRouteId>")
    print("  SEOUL_BUS_STOP_ID=<2/2 표에서 찾은 stId>")
    print("이 출력 전체를 채팅에 붙여넣어 주시면, 실제 필드명이 예상과 다를 경우 바로 스크립트를 고치겠습니다.")
    print("(code/message가 이상하면(예: 인증 오류) 공공데이터포털에서 이 서비스 활용신청이 '승인' 상태인지도 확인해 주세요.)")
    print("=" * 70)
    return 0


# ----------------------------------------------------------------------------
# 명령: tago-ids  (TAGO cityCode / routeId / nodeId 조회) — Phase 2(서울 외 지역) 예비용
# ----------------------------------------------------------------------------
def _print_lookup_result(label: str, r: CallResult, id_field_hint: str, search: str = "") -> None:
    """조회 결과를 표로 출력한다.

    search 가 주어지면: 전체 레코드 중 어떤 필드값이든 search 문자열을 포함하는 행만
    위치(순서)와 무관하게 전부 보여준다 (예: 138건 중 '서울'이 포함된 행이 몇 번째에
    있든 찾아서 보여줌). search 로 하나도 못 찾으면 그 사실을 알리고 상위 15건을
    참고용으로 보여준다. search 가 비어 있으면 기존처럼 상위 15건만 보여준다.
    """
    print(f"\n--- {label} ---")
    print(f"  verdict={r.verdict} http={r.http_status} code={r.code} message={r.message} "
          f"records={r.n_records}" + (f" error={r.error}" if r.error else ""))
    if not r.records:
        print("  (레코드 없음. 위 code/message/error 를 보고 파라미터명이 틀렸는지 확인해 주세요.)")
        return
    id_cols = [f["name"] for f in r.fields if id_field_hint.lower() in f["name"].lower()]
    cols = (id_cols + [f["name"] for f in r.fields if f["name"] not in id_cols])[:6]
    print("  " + " | ".join(cols))

    def _print_rows(records) -> None:
        for rec in records:
            print("  " + " | ".join(str(rec.get(c, ""))[:20] for c in cols))

    if search:
        needle = search.lower()
        matches = [
            rec for rec in r.records
            if any(needle in str(rec.get(c, "")).lower() for c in cols)
        ]
        if matches:
            print(f"  (전체 {len(r.records)}건 중 '{search}' 포함 {len(matches)}건 — 위치와 무관하게 전부 표시)")
            _print_rows(matches)
            return
        print(f"  '{search}' 이 포함된 행을 전체 {len(r.records)}건 중에서 찾지 못했습니다.")
        print("  (참고로 상위 15건만 보여드립니다 — 필드명이 예상과 다를 수 있으니 직접 확인해 주세요.)")
        _print_rows(r.records[:15])
        if len(r.records) > 15:
            print(f"  ... 외 {len(r.records) - 15}건")
        return

    _print_rows(r.records[:15])
    if len(r.records) > 15:
        print(f"  ... 외 {len(r.records) - 15}건")


def cmd_tago_ids(args) -> int:
    cfg = load_config()
    if not cfg["dg_key"]:
        print("DATA_GO_KR_KEY 가 .env에 없습니다. 먼저 채워주세요.")
        return 2
    key = cfg["dg_key"]
    route_no = args.route_no or cfg["tago_route_no"]
    node_name = args.node_name or cfg["tago_node_name"]
    city_search = args.city_search

    print("1/3 도시 코드 조회 중 (getCtyCodeList) ...")
    r1 = raw_call(
        "tago_citycode", f"{TAGO_BASE}/BusRouteInfoInqireService/getCtyCodeList",
        # numOfRows를 넉넉히 줘도 TAGO가 실제로는 전체(138건 등)를 그냥 다 돌려주는 경우가
        # 있었다 (요청 50 → 응답 138). 어차피 파서가 전체를 다 읽으므로 방어적으로 크게 요청.
        {"serviceKey": key, "numOfRows": 200, "pageNo": 1, "_type": "json"}, cfg, save=True,
    )
    _print_lookup_result(
        "도시 코드 목록 (서울 포함 행을 찾아 TAGO_CITY_CODE 에 넣으세요)", r1, "code",
        search=city_search,
    )

    city_code = args.city_code or cfg["tago_city_code"]
    if not city_code:
        print("\nTAGO_CITY_CODE 가 아직 없어 2/3, 3/3 단계는 건너뜁니다.")
        print("위 목록에서 서울 코드 값을 확인해 --city-code 옵션으로 다시 실행하거나 .env 의 TAGO_CITY_CODE 를 채운 뒤 재실행하세요.")
        print(f"(다른 도시를 찾고 싶으면 --city-search 옵션에 다른 이름을 넣어보세요. 기본값은 '{city_search}' 입니다.)")
        return 0

    print(f"\n2/3 노선 번호 '{route_no}' 로 routeId 조회 중 (getRouteNoList, cityCode={city_code}) ...")
    r2 = raw_call(
        "tago_route_lookup", f"{TAGO_BASE}/BusRouteInfoInqireService/getRouteNoList",
        {"serviceKey": key, "cityCode": city_code, "routeNo": route_no,
         "numOfRows": 20, "pageNo": 1, "_type": "json"}, cfg, save=True,
    )
    _print_lookup_result(f"노선 '{route_no}' 검색 결과 (routeId 값을 TAGO_ROUTE_ID 에 넣으세요)", r2, "route")

    print(f"\n3/3 정류소 이름 '{node_name}' 으로 nodeId 조회 중 (getSttnNoList, cityCode={city_code}) ...")
    r3 = raw_call(
        "tago_stop_lookup", f"{TAGO_BASE}/BusSttnInfoInqireService/getSttnNoList",
        {"serviceKey": key, "cityCode": city_code, "nodeNm": node_name,
         "numOfRows": 20, "pageNo": 1, "_type": "json"}, cfg, save=True,
    )
    _print_lookup_result(f"정류소 '{node_name}' 검색 결과 (nodeId 값을 TAGO_NODE_ID 에 넣으세요)", r3, "node")

    print("\n" + "=" * 70)
    print("위 3개 표에서 원하는 값을 골라 .env 에 아래처럼 채운 뒤 'run' 명령을 실행하세요:")
    print("  TAGO_CITY_CODE=<1/3 표에서 찾은 서울 코드>")
    print("  TAGO_ROUTE_ID=<2/3 표에서 찾은 routeId>")
    print("  TAGO_NODE_ID=<3/3 표에서 찾은 nodeId>")
    print("이 출력 전체를 채팅에 붙여넣어 주시면, 실제 필드명이 예상과 다를 경우 바로 스크립트를 고치겠습니다.")
    print("=" * 70)
    return 0


# ----------------------------------------------------------------------------
# 명령: kma-lag  (초단기실황이 정시 후 몇 분에 조회 가능해지는지)
# ----------------------------------------------------------------------------
def cmd_kma_lag(args) -> int:
    cfg = load_config()
    if args.wait_top:
        now = datetime.now(KST)
        top = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        print(f"다음 정시({top:%H:%M})까지 대기합니다. (Ctrl+C 로 중단)")
        while datetime.now(KST) < top:
            time.sleep(min(30, max(1.0, (top - datetime.now(KST)).total_seconds())))
    base_dt = datetime.now(KST).replace(minute=0, second=0, microsecond=0)
    base = (base_dt.strftime("%Y%m%d"), base_dt.strftime("%H%M"))
    print(f"발표시각 {base[0]} {base[1]} 자료가 조회 가능해질 때까지 1분마다 확인합니다 (최대 {args.max_minutes}분).")

    attempts, first_ok = [], None
    try:
        for _ in range(args.max_minutes + 1):
            r = do_call("kma_ncst", cfg, save=False, kma_base=base)
            elapsed = (datetime.now(KST) - base_dt).total_seconds() / 60
            attempts.append({"minute_after_top": round(elapsed, 1), "verdict": r.verdict,
                             "code": r.code, "records": r.n_records,
                             "http": r.http_status, "error": (r.error or r.message or "")[:80]})
            print(f"  정시+{elapsed:4.1f}분  {r.verdict}  http={r.http_status} code={r.code} "
                  f"records={r.n_records} {(r.error or r.message or '')[:80]}", flush=True)
            if r.verdict == "PASS":
                first_ok = elapsed
                save_raw(r)
                break
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n중단됨.")

    L = ["# kma-lag 리포트", "", env_header(),
         f"- 대상 발표시각: {base[0]} {base[1]} / 격자 nx={cfg['kma_nx']}, ny={cfg['kma_ny']}",
         f"- 최초 조회 성공: 정시 + {first_ok:.1f}분" if first_ok is not None else "- 제한 시간 내 조회 성공 없음",
         "- 주의: 시작 시점에 이미 성공했다면 실제 가용 시각은 그 이전일 수 있음 (--wait-top 으로 정시부터 측정 권장)",
         "", "| 정시 후(분) | 판정 | HTTP | 코드 | 레코드 | 메시지 |", "|---|---|---|---|---|---|"]
    prev = None
    for a in attempts:
        gap = "" if prev is None else (f" ⚠ 직전 시도와 {a['minute_after_top'] - prev:.1f}분 간격 (잠자기?)"
                                       if a["minute_after_top"] - prev > 3 else "")
        L.append(f"| {a['minute_after_top']}{gap} | {a['verdict']} | {a['http']} | {a['code']} | "
                 f"{a['records']} | {a['error']} |")
        prev = a["minute_after_top"]
    md = "\n".join(L)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    (OUT_DIR / f"kma_lag_{stamp}.md").write_text(md, encoding="utf-8")
    print("\n" + "=" * 70)
    print(md)
    print("=" * 70)
    return 0


# ----------------------------------------------------------------------------
def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    p = argparse.ArgumentParser(description="UrbanStream Step 0 API 스모크 테스트")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("check-env", help=".env 설정 점검 (네트워크 호출 없음)").set_defaults(fn=cmd_check_env)

    ps = sub.add_parser("seoul-bus-ids", help="서울 버스 busRouteId/stId 조회 (버스 테스트 전 먼저 실행, 메인 소스)")
    ps.add_argument("--route-no", default="", help="조회할 버스 노선 번호 (기본: .env의 SEOUL_BUS_ROUTE_NO 또는 143)")
    ps.add_argument("--stop-name", default="", help="조회할 정류소 이름 (기본: .env의 SEOUL_BUS_STOP_NAME 또는 강남역)")
    ps.set_defaults(fn=cmd_seoul_bus_ids)

    pt = sub.add_parser("tago-ids", help="[Phase 2 예비용] TAGO cityCode/routeId/nodeId 조회 (서울은 TAGO에 없음)")
    pt.add_argument("--city-code", default="", help="이미 알고 있는 cityCode (없으면 1단계 결과를 보고 재실행)")
    pt.add_argument("--route-no", default="", help="조회할 버스 노선 번호 (기본: .env의 TAGO_ROUTE_NO 또는 143)")
    pt.add_argument("--node-name", default="", help="조회할 정류소 이름 (기본: .env의 TAGO_NODE_NAME 또는 강남역)")
    pt.add_argument("--city-search", default="서울",
                     help="1단계 도시 코드 목록에서 찾을 문자열 (기본: 서울). 전체 레코드 중 이 문자열이 "
                          "포함된 행만 위치와 무관하게 전부 보여준다. 빈 문자열('')을 주면 상위 15건만 보여주는 "
                          "이전 방식으로 동작한다.")
    pt.set_defaults(fn=cmd_tago_ids)

    pr = sub.add_parser("run", help="각 API를 1회씩 호출하고 리포트 생성")
    pr.add_argument("--only", default="", help="쉼표로 구분한 API 이름 (기본: 전체)")
    pr.set_defaults(fn=cmd_run)

    pc = sub.add_parser("cadence", help="소스의 실제 갱신 주기 측정")
    pc.add_argument("--api", required=True, choices=ALL_APIS)
    pc.add_argument("--interval", type=int, default=60, help="호출 간격(초), 최소 10")
    pc.add_argument("--minutes", type=int, default=20)
    pc.add_argument("--force", action="store_true", help="버스 API 호출 수 제한 무시")
    pc.set_defaults(fn=cmd_cadence)

    pk = sub.add_parser("kma-lag", help="기상청 초단기실황 제공 시각 측정")
    pk.add_argument("--wait-top", action="store_true", help="다음 정시까지 기다렸다가 측정 시작")
    pk.add_argument("--max-minutes", type=int, default=60)
    pk.set_defaults(fn=cmd_kma_lag)

    args = p.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
