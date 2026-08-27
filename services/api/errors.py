from __future__ import annotations

import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from services.api.models import FieldError, ProblemDetails
from services.api.service import ServiceError


logger = logging.getLogger(__name__)


_STATUS_TITLES = {
    400: "Bad request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not found",
    409: "Conflict",
    422: "Unprocessable entity",
    429: "Too many requests",
    500: "Internal server error",
    503: "Service unavailable",
}


def _trace_id(request: Request) -> str:
    return str(getattr(request.state, "request_id", "unknown"))


def problem_response(
    request: Request,
    *,
    status: int,
    code: str,
    title: str | None = None,
    detail: str | None = None,
    field_errors: list[FieldError] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    problem = ProblemDetails(
        type=f"https://imagetracker.app/problems/{code.casefold().replace('_', '-')}",
        title=title or _STATUS_TITLES.get(status, "Request failed"),
        status=status,
        code=code,
        detail=detail,
        instance=request.url.path,
        trace_id=_trace_id(request),
        field_errors=field_errors or [],
    )
    return JSONResponse(
        status_code=status,
        content=problem.model_dump(mode="json", by_alias=True, exclude_none=True),
        media_type="application/problem+json",
        headers=headers,
    )


async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
    if exc.status_code >= 500:
        logger.error(
            "ImageTracker service failure",
            extra={
                "request_id": _trace_id(request),
                "path": request.url.path,
                "service_code": exc.code,
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )
    return problem_response(
        request,
        status=exc.status_code,
        code=exc.code,
        title=exc.title,
        detail=exc.detail,
    )


async def validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    errors = exc.errors()
    is_body_error = any(error.get("loc", (None,))[0] == "body" for error in errors)
    status = 422 if is_body_error else 400
    field_errors: list[FieldError] = []
    for error in errors:
        location = [str(part) for part in error.get("loc", ()) if part != "body"]
        field_errors.append(
            FieldError(
                field=".".join(location) or "request",
                code=str(error.get("type", "INVALID_VALUE")).upper(),
                message=str(error.get("msg", "Invalid value")),
            )
        )
    return problem_response(
        request,
        status=status,
        code="VALIDATION_FAILED",
        detail="One or more request values are invalid",
        field_errors=field_errors,
    )


async def http_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else None
    return problem_response(
        request,
        status=exc.status_code,
        code=f"HTTP_{exc.status_code}",
        detail=detail,
        headers=dict(exc.headers or {}),
    )


async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
    # The exception itself belongs in structured server logs. Never expose it
    # to a consumer client, where it could reveal database or provider details.
    logger.exception(
        "Unhandled ImageTracker API request failure",
        extra={"request_id": _trace_id(request), "path": request.url.path},
    )
    return problem_response(
        request,
        status=500,
        code="INTERNAL_ERROR",
        detail="The request could not be completed",
    )
