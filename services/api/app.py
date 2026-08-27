from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from services.common.settings import AppSettings, get_settings


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    service: str
    version: str
    status: Literal["Ok"]
    time_utc: datetime = Field(alias="timeUtc")


def create_app(settings: AppSettings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    app = FastAPI(
        title="ImageTracker API",
        version=resolved_settings.service_version,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = resolved_settings

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
    return app


app = create_app()
