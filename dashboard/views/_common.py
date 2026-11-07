"""여러 화면이 같은 매개변수로 부르는 요청 — 같아야 캐시를 공유해 API 호출이 늘지 않는다."""
LINKS_PARAMS = {"collected_only": True, "geometry": True, "limit": 100}   # geometry=true 는 limit ≤ 100
CURRENT_PARAMS = {"limit": 200}
ACTIVE_PARAMS = {"limit": 200}
BUS_PARAMS = {"within_min": 15, "limit": 300}
ALERT_PARAMS = {"limit": 10}
QUANT_SEC = 300          # 조회 기간의 from/to 를 내림하는 단위(= gold 캐시 수명)
