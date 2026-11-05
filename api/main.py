"""FastAPI 앱 팩토리. 읽기 전용, 모든 경로(/health, /ready, /docs 제외)는 X-API-Key 필요."""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Callable, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from starlette.responses import Response

from .config import API_VERSION, Settings, load_settings
from .db import Database
from .deps import Services
from .errors import install_error_handlers, problem

log = logging.getLogger("urbanstream.api")
_RID = re.compile(r"^[A-Za-z0-9._-]{8,64}$")

TAGS = [
    {"name": "system", "description": "상태 확인(인증 불필요)"},
    {"name": "pipeline", "description": "파이프라인·데이터 품질 운영 상태 (Step 8 ops 뷰)"},
    {"name": "traffic", "description": "도로 속도·혼잡 (Step 6/7 마트)"},
    {"name": "incidents", "description": "돌발 상황과 속도 영향"},
    {"name": "bus", "description": "버스 도착 예측 변동·구간 통과 시간"},
    {"name": "weather", "description": "날씨와 도로 속도"},
    {"name": "anomalies", "description": "속도 이상 후보"},
]

DESCRIPTION = (
    "UrbanStream Gold/Mart 읽기 전용 API. 값은 Gold 적재(매시 20분) 기준이라 실시간이 아니며, 모든 응답의 meta 에 "
    "as_of / age_sec / stale / loaded_at 이 들어 있다. 버스의 '지연'은 시간표 대비가 아니라 도착 예측 변동·구간 통과 시간이다."
)


def _operation_id(route: APIRoute) -> str:
    """operationId = 함수 이름 (OpenAPI 스냅샷·클라이언트 생성이 안정되도록)."""
    return route.name


def create_app(settings: Optional[Settings] = None, db: Optional[Database] = None,
               clock: Optional[Callable[[], datetime]] = None) -> FastAPI:
    settings = settings or load_settings()
    db = db or Database(settings)
    svc = Services(settings, db, clock=clock)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if hasattr(db, "open"):
            db.open()
        try:
            yield
        finally:
            if hasattr(db, "close"):
                db.close()

    app = FastAPI(
        title="UrbanStream API", version=API_VERSION, description=DESCRIPTION, openapi_tags=TAGS, lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        generate_unique_id_function=_operation_id,
    )
    app.state.services = svc
    install_error_handlers(app)

    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=list(settings.cors_origins), allow_methods=["GET"],
                           allow_headers=["X-API-Key", "X-Request-ID"], expose_headers=["X-Request-ID", "Retry-After"], max_age=600)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        incoming = request.headers.get("x-request-id", "")
        request.state.request_id = incoming if _RID.match(incoming) else uuid.uuid4().hex
        t0 = time.perf_counter()
        try:
            response: Response = await call_next(request)
        except Exception:                                               # noqa: BLE001 — 내부 오류는 본문에 싣지 않는다
            log.exception("unhandled error")
            response = problem(request, 500, "internal_error", "서버 내부 오류입니다. request_id 와 함께 문의하세요")
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        log.info(json.dumps({"event": "request", "method": request.method, "path": request.url.path, "status": response.status_code,
                             "ms": round((time.perf_counter() - t0) * 1000, 1), "request_id": request.state.request_id,
                             "key_id": getattr(request.state, "key_id", None)}, ensure_ascii=False))
        return response

    from .routers import system   # 순환 import 방지: 라우터는 deps/security 를 쓴다
    app.include_router(system.router)
    for name in ("pipeline", "traffic", "incidents", "bus", "weather", "anomalies"):
        try:
            mod = __import__(f"api.routers.{name}", fromlist=["router"])
        except ModuleNotFoundError as e:
            if e.name != f"api.routers.{name}":
                raise
            continue                                                    # 아직 없는 라우터(개발 중)만 건너뛴다
        app.include_router(mod.router)
    return app


def app_factory() -> FastAPI:       # uvicorn --factory api.main:app_factory
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    return create_app()
