from fastapi import APIRouter, Request

from ..config import API_VERSION
from ..deps import services
from ..errors import ApiError
from ..schemas import HealthOut, ReadyOut

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthOut, summary="프로세스 생존 확인(DB 를 보지 않음)")
def health() -> HealthOut:
    return HealthOut(status="ok", version=API_VERSION)


@router.get("/ready", response_model=ReadyOut, summary="DB 연결 확인", responses={503: {"description": "DB 에 연결할 수 없음"}})
def ready(request: Request) -> ReadyOut:
    if not services(request).db.ping():
        raise ApiError(503, "database_unavailable", "데이터베이스에 연결할 수 없습니다", headers={"Retry-After": "5"})
    return ReadyOut(status="ok", db="ok")
