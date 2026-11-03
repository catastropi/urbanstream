# api

FastAPI Serving Layer. 도메인 중심 router 구성 (Blueprint P절).

- routers/traffic.py, bus.py, incidents.py, weather.py, mobility.py, platform.py
- models/ — Pydantic 응답 모델 (버전 /v1)
- auth.py — API key, rate limit
- dashboard/ — 운영 Dashboard 정적 파일 (기존 ecommerce-pipeline의 api/templates, api/static 계승)

기존 ecommerce-pipeline의 api/main.py, database.py, schemas.py 구조를 계승하되,
POST /ingest/orders 같은 수동 주입 endpoint는 만들지 않는다 (collector가 유일한 쓰기 경로).

Blueprint 참고: P절(FastAPI Endpoint), Q절(Dashboard)
