import json
from datetime import datetime, timezone

from collector.runner import SleepWatcher, report_resume


class Clocks:
    def __init__(self):
        self.w, self.m = 1000.0, 50.0

    def adv(self, wall, mono):
        self.w += wall
        self.m += mono


def test_no_sleep_means_zero_and_small_jitter_ignored():
    c = Clocks()
    sw = SleepWatcher(lambda: c.w, lambda: c.m, 30)
    c.adv(5, 5)
    assert sw.check() == 0
    c.adv(20.0, 0.5)          # 20초 지연(GC·부하)은 임계(30) 미만
    assert sw.check() == 0


def test_sleep_detected_once_then_resets():
    c = Clocks()
    sw = SleepWatcher(lambda: c.w, lambda: c.m, 30)
    c.adv(3700, 1)            # 덮개를 닫고 약 1시간
    assert round(sw.check()) == 3699
    c.adv(5, 5)
    assert sw.check() == 0    # 같은 잠자기를 두 번 세지 않는다


def test_monotonic_that_does_not_stop_is_not_detected_not_false_positive():
    c = Clocks()
    sw = SleepWatcher(lambda: c.w, lambda: c.m, 30)
    c.adv(3600, 3600)         # monotonic 이 잠자는 동안 흐르는 환경(전제가 틀린 경우) → 감지 못함, 오탐도 없음
    assert sw.check() == 0


def test_report_resume_emits_host_resume_audit():
    c = Clocks()
    sw = SleepWatcher(lambda: c.w, lambda: c.m, 30)

    class Col:
        got = []
        def _audit(self, **f): self.got.append(f)
    col = Col()
    assert report_resume(col, sw) == 0 and col.got == []
    c.adv(600, 0)
    assert report_resume(col, sw) == 600
    assert col.got == [{"event": "host_resume", "source_system": "collector", "slept_sec": 600.0}]
