from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException

from services.api.composition import build_default_phase1_service
from services.api.errors import (
    http_error_handler,
    service_error_handler,
    unexpected_error_handler,
    validation_error_handler,
)
from services.api.routes import create_phase1_router
from services.api.service import AuthIdentity, Phase1Service, ServiceError
from services.common.settings import AppSettings, get_settings


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    service: str
    version: str
    status: Literal["Ok"]
    time_utc: datetime = Field(alias="timeUtc")


def create_app(
    settings: AppSettings | None = None,
    *,
    phase1_service: Phase1Service | None = None,
    test_identity: AuthIdentity | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    if test_identity is not None and resolved_settings.stage.casefold() not in {
        "local",
        "test",
    }:
        raise ValueError("Injected identities are allowed only in local or test stages")
    app = FastAPI(
        title="ImageTracker API",
        version=resolved_settings.service_version,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = resolved_settings
    app.state.phase1_service = (
        phase1_service
        if phase1_service is not None
        else build_default_phase1_service(resolved_settings)
    )
    app.state.test_identity = test_identity

    app.add_exception_handler(ServiceError, service_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(HTTPException, http_error_handler)
    app.add_exception_handler(Exception, unexpected_error_handler)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next) -> Response:
        raw_request_id = request.headers.get("x-request-id", "")
        try:
            request_id = UUID(raw_request_id)
        except (TypeError, ValueError):
            request_id = uuid4()
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["x-request-id"] = str(request_id)
        return response

    def health() -> HealthResponse:
        return HealthResponse(
            service=resolved_settings.service_name,
            version=resolved_settings.service_version,
            status="Ok",
            time_utc=datetime.now(timezone.utc),
        )

    app.add_api_route("/health", health, methods=["GET"], response_model=HealthResponse)
    app.add_api_route("/v1/health", health, methods=["GET"], response_model=HealthResponse)
    app.include_router(create_phase1_router())
    return app


app = create_app()
