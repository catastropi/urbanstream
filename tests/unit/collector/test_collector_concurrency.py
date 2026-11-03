"""26초 지연 대책: 호출 총 제한시간, Connection: close, 스레드 안전 예산, 소스별 스레드 실행."""
import dataclasses
import json
import sys
import threading
import time
from datetime import datetime

import requests

from collector.config import load_config
from collector.fetch import HttpFetcher, Redactor
from collector.resilience import DailyBudget
from collector.runner import Scheduler, run_forever_parallel
from collector_helpers import make_collector, router_for_fixtures
from urbanstream.contracts.common import KST


# ---------------------------------------------------------------- HttpFetcher
class FakeResp:
    def __init__(self, chunks, delay=0.0, status=200, exc=None):
        self.chunks, self.delay, self.status_code, self.exc, self.closed = chunks, delay, status, exc, False

    def iter_content(self, chunk_size=1):
        for c in self.chunks:
            time.sleep(self.delay)
            if self.exc:
                raise self.exc
            yield c

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, resp=None, exc=None):
        self.headers, self.resp, self.exc, self.kwargs = {}, resp, exc, None

    def get(self, url, params=None, timeout=None, stream=False):
        self.kwargs = dict(timeout=timeout, stream=stream)
        if self.exc:
            raise self.exc
        return self.resp


def fetcher(session, total=15.0):
    return HttpFetcher((5, 10), Redactor(["SECRETKEY1234"]), session=session, total_timeout=total)


def test_fetch_reads_whole_body_and_closes():
    resp = FakeResp([b"<a>", b"ok", b"</a>"])
    s = FakeSession(resp)
    fr = fetcher(s).get("http://x", {"k": 1})
    assert (fr.http_status, fr.body, fr.error) == (200, b"<a>ok</a>", None)
    assert resp.closed and s.kwargs == {"timeout": (5, 10), "stream": True}


def test_fetch_sends_connection_close_and_user_agent():
    s = FakeSession(FakeResp([b"x"]))
    fetcher(s)
    assert s.headers["Connection"] == "close" and "urbanstream" in s.headers["User-Agent"]


def test_fetch_total_deadline_cuts_slow_trickle():
    # 바이트 사이 간격은 짧아서 read timeout 에 안 걸리지만 총 시간이 길어지는 응답 (26초 사례의 모양)
    resp = FakeResp([b"x"] * 40, delay=0.03)
    t0 = time.perf_counter()
    fr = fetcher(FakeSession(resp), total=0.2).get("http://x", None)
    assert fr.http_status is None and fr.error_kind == "timeout" and "TotalTimeout" in fr.error
    assert time.perf_counter() - t0 < 0.6          # 40×0.03=1.2초를 기다리지 않는다
    assert resp.closed


def test_fetch_maps_requests_exceptions_and_masks_key():
    fr = fetcher(FakeSession(exc=requests.ReadTimeout("read timed out"))).get("http://x", None)
    assert (fr.http_status, fr.error_kind) == (None, "timeout")
    fr = fetcher(FakeSession(exc=requests.ConnectionError("Remote end closed SECRETKEY1234"))).get("http://x", None)
    assert fr.error_kind == "network_error" and "SECRETKEY1234" not in fr.error and "***KEY***" in fr.error
    resp = FakeResp([b"a", b"b"], exc=requests.exceptions.ChunkedEncodingError("cut"))
    fr = fetcher(FakeSession(resp)).get("http://x", None)       # 본문을 읽다가 끊긴 경우
    assert fr.error_kind == "network_error" and resp.closed


# ---------------------------------------------------------------- 예산
def test_budget_is_thread_safe(tmp_path):
    now = lambda: datetime(2026, 11, 2, 10, 0, tzinfo=KST)
    b = DailyBudget(tmp_path / "budget.json", {"seoul_traffic": 10_000}, now)
    errors = []

    def work():
        try:
            for _ in range(60):
                b.try_consume("seoul_traffic")
        except Exception as e:      # 임시 파일 교체가 겹치면 여기로 온다
            errors.append(e)

    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)     # 스레드가 자주 바뀌게 해서 경합을 일부러 만든다
    try:
        threads = [threading.Thread(target=work) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
    finally:
        sys.setswitchinterval(old)
    assert errors == []
    assert b.used("seoul_traffic") == 480
    assert json.loads((tmp_path / "budget.json").read_text())["calls"]["seoul_traffic"] == 480


# ---------------------------------------------------------------- 소스별 스레드
def _run_parallel(tmp_path, seconds, slow_sec):
    """seoul_traffic(0.3초 간격, 빠름)과 seoul_incident(느림: 호출마다 slow_sec). 둘 다 24시간 활성이라 실시간 시계로 돈다."""
    cfg = load_config(env={})
    clock = lambda: datetime.now(KST)
    live, peak, lock = {}, {}, threading.Lock()

    def router(url, params):
        kind = "incident" if "AccInfo" in url else "traffic"
        with lock:
            live[kind] = live.get(kind, 0) + 1
            peak[kind] = max(peak.get(kind, 0), live[kind])
        try:
            if kind == "incident":
                time.sleep(slow_sec)
            return router_for_fixtures(url, params)
        finally:
            with lock:
                live[kind] -= 1

    c, pub, f, _, _ = make_collector(tmp_path, router=router, cfg=cfg, clock=clock)
    fast = dataclasses.replace(cfg.sources["seoul_traffic"], interval_sec=0.3)
    slow = dataclasses.replace(cfg.sources["seoul_incident"], interval_sec=0.3)
    sched = Scheduler([fast, slow], clock(), stagger_sec=0, jitter=0)
    stop = threading.Event()
    th = threading.Thread(target=run_forever_parallel, args=(c, sched, stop, clock))
    th.start()
    time.sleep(seconds)
    t_stop = time.perf_counter()
    stop.set()
    th.join(timeout=15)
    assert not th.is_alive()
    n = lambda kw: sum(1 for url, _ in f.calls if kw in url)
    return n("TrafficInfo"), n("AccInfo"), peak, time.perf_counter() - t_stop


def test_slow_source_does_not_delay_other_sources(tmp_path):
    traffic, incident, peak, _ = _run_parallel(tmp_path, seconds=3.0, slow_sec=1.2)
    # 순차 실행이면 도로소통 호출 사이마다 돌발의 1.2초가 끼어 3초 동안 3~4번뿐이다.
    assert traffic >= 7, traffic
    assert 1 <= incident <= 3, incident


def test_same_source_calls_never_overlap(tmp_path):
    _, _, peak, _ = _run_parallel(tmp_path, seconds=2.0, slow_sec=0.9)
    assert peak["incident"] == 1 and peak["traffic"] == 1


def test_stop_finishes_in_flight_cycle_and_returns(tmp_path):
    _, _, _, stop_secs = _run_parallel(tmp_path, seconds=1.0, slow_sec=1.0)
    assert stop_secs < 5.0
