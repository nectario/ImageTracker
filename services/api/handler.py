from __future__ import annotations

from typing import Any

from mangum import Mangum

from services.api.app import app


_adapter = Mangum(app, lifespan="off")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda entry point for the ImageTracker HTTP API."""

    return _adapter(event, context)
