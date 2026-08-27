#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

step "Calling the FastAPI health route in memory"
"${IMAGETRACKER_PYTHON}" -B - <<'PY'
import asyncio
import json

import httpx

from services.api.app import app


async def main() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://local") as client:
        response = await client.get(
            "/v1/health",
            headers={"x-request-id": "66b4e699-58e2-4d16-9ddf-b0d11d69cb67"},
        )

    payload = response.json()
    expected_keys = {"service", "version", "status", "timeUtc"}
    if response.status_code != 200 or set(payload) != expected_keys or payload["status"] != "Ok":
        raise SystemExit(f"Unexpected health response: {response.status_code} {payload}")
    if response.headers.get("x-request-id") != "66b4e699-58e2-4d16-9ddf-b0d11d69cb67":
        raise SystemExit("Request ID was not preserved")

    print(json.dumps(payload, indent=2, sort_keys=True))


asyncio.run(main())
PY
