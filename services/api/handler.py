from __future__ import annotations

from typing import Any

from mangum import Mangum

from services.api.app import app


_adapter = Mangum(app, lifespan="off")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda entry point used by the Phase 0 serverless stack."""

    return _adapter(event, context)
