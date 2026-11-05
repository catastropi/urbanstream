"""맥 잠자기 동안 time.monotonic() 이 멈추는지 확인한다 (ADR-0010 D8-5 의 전제 [추정] 확인용).

사용: python scripts/sleep_probe.py  → 실행해 둔 채 덮개를 닫고 1~2분 뒤 연다 → 출력의 '잠잔 시간' 이 덮개를 닫은 시간과 비슷하면 전제가 맞다.
0 에 가까우면 monotonic 이 잠자는 동안도 흐르는 것이다 → 호스트 감지(host_resume)는 작동하지 않고, 모든 수집원이 600초 이상 조용한 것(collector_blackout)으로만 잡힌다.
"""
import time

w0, m0 = time.time(), time.monotonic()
print("시작했습니다. 덮개를 닫고 1~2분 뒤 여세요. 종료: Ctrl-C")
try:
    while True:
        time.sleep(1)
        w, m = time.time(), time.monotonic()
        diff = (w - w0) - (m - m0)
        if diff >= 5:
            print(f"깨어남 감지: 벽시계 {w - w0:.0f}초 / 단조시계 {m - m0:.0f}초 → 잠잔 시간 {diff:.0f}초")
        w0, m0 = w, m
except KeyboardInterrupt:
    pass
