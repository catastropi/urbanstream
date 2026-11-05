"""오류 형식 통일: RFC 9457 problem+json (D9-8). 내부 정보(예외 문자열·SQL)는 응답에 넣지 않는다."""
from __future__ import annotations

from typing import Any, Mapping, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

PROBLEM_MEDIA_TYPE = "application/problem+json"

_TITLES = {400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 405: "Method Not Allowed", 422: "Unprocessable Entity",
           429: "Too Many Requests", 500: "Internal Server Error", 503: "Service Unavailable"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, detail: str = "", *, title: Optional[str] = None,
                 headers: Optional[Mapping[str, str]] = None, errors: Optional[list] = None):
        super().__init__(f"{status} {code}")
        self.status, self.code, self.detail = status, code, detail
        self.title = title or _TITLES.get(status, "Error")
        self.headers = dict(headers or {})
        self.errors = errors


def problem(request: Request, status: int, code: str, detail: str = "", *, title: Optional[str] = None,
            headers: Optional[Mapping[str, str]] = None, errors: Optional[list] = None) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"urn:urbanstream:error:{code}", "title": title or _TITLES.get(status, "Error"), "status": status,
        "detail": detail, "code": code, "request_id": getattr(request.state, "request_id", None),
    }
    if errors:
        body["errors"] = errors
    h = {"Cache-Control": "no-store", **(headers or {})}
    rid = body["request_id"]
    if rid:
        h["X-Request-ID"] = rid
    return JSONResponse(body, status_code=status, media_type=PROBLEM_MEDIA_TYPE, headers=h)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        return problem(request, exc.status, exc.code, exc.detail, title=exc.title, headers=exc.headers, errors=exc.errors)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        # 입력값(input)·ctx 는 되돌려 주지 않는다: 잘못된 값을 그대로 반사하지 않기 위해서다.
        errs = [{"loc": [str(x) for x in e.get("loc", ())], "msg": str(e.get("msg", "")), "type": str(e.get("type", ""))} for e in exc.errors()]
        return problem(request, 422, "validation_error", "요청 매개변수가 올바르지 않습니다", errors=errs)

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        detail = "없는 경로입니다" if exc.status_code == 404 else ("허용되지 않는 메서드입니다" if exc.status_code == 405 else "요청을 처리할 수 없습니다")
        return problem(request, exc.status_code, code, detail, headers=getattr(exc, "headers", None))
