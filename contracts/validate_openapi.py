"""Dependency-free structural checks for the ImageTracker OpenAPI contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


CONTRACT = Path(__file__).resolve().parent / "v1" / "openapi.json"
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}
MUTATING_METHODS = {"post", "put", "patch", "delete"}
IDEMPOTENCY_EXEMPT_PATHS = {"/v1/webhooks/elevenlabs"}
CURSOR_PATHS = {
    "/v1/devices",
    "/v1/sources",
    "/v1/changes",
    "/v1/media",
    "/v1/media/search",
    "/v1/jobs",
}


def walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def parameter_names(operation: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for parameter in operation.get("parameters", []):
        if "$ref" in parameter:
            names.add(parameter["$ref"].rsplit("/", 1)[-1])
        else:
            names.add(parameter.get("name", ""))
    return names


def main() -> int:
    document = json.loads(CONTRACT.read_text(encoding="utf-8"))
    errors: list[str] = []

    if document.get("openapi") != "3.0.3":
        errors.append("openapi must be 3.0.3")
    if document.get("info", {}).get("version") != "0.1.0":
        errors.append("foundation contract version must be 0.1.0")

    paths = document.get("paths", {})
    operation_ids: dict[str, str] = {}
    for path, path_item in paths.items():
        if not path.startswith("/v1/"):
            errors.append(f"path is not versioned: {path}")
        for method, operation in path_item.items():
            if method not in HTTP_METHODS:
                continue
            label = f"{method.upper()} {path}"
            operation_id = operation.get("operationId")
            if not operation_id:
                errors.append(f"{label} has no operationId")
            elif operation_id in operation_ids:
                errors.append(
                    f"duplicate operationId {operation_id}: "
                    f"{operation_ids[operation_id]} and {label}"
                )
            else:
                operation_ids[operation_id] = label

            names = parameter_names(operation)
            if method in MUTATING_METHODS and path not in IDEMPOTENCY_EXEMPT_PATHS:
                if "IdempotencyKeyHeader" not in names and "Idempotency-Key" not in names:
                    errors.append(f"{label} does not require Idempotency-Key")
            if method == "get" and path in CURSOR_PATHS:
                if "CursorQuery" not in names or "LimitQuery" not in names:
                    errors.append(f"{label} does not expose cursor and limit")

    schemas = document.get("components", {}).get("schemas", {})
    parameters = document.get("components", {}).get("parameters", {})
    responses = document.get("components", {}).get("responses", {})
    request_bodies = document.get("components", {}).get("requestBodies", {})
    headers = document.get("components", {}).get("headers", {})
    security_schemes = document.get("components", {}).get("securitySchemes", {})
    component_maps = {
        "schemas": schemas,
        "parameters": parameters,
        "responses": responses,
        "requestBodies": request_bodies,
        "headers": headers,
        "securitySchemes": security_schemes,
    }
    for value in walk(document):
        if not isinstance(value, dict) or "$ref" not in value:
            continue
        reference = value["$ref"]
        prefix = "#/components/"
        if not reference.startswith(prefix):
            errors.append(f"external reference is not allowed in the foundation contract: {reference}")
            continue
        remainder = reference[len(prefix) :]
        try:
            component_type, component_name = remainder.split("/", 1)
        except ValueError:
            errors.append(f"invalid component reference: {reference}")
            continue
        if component_name not in component_maps.get(component_type, {}):
            errors.append(f"unresolved component reference: {reference}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    operation_count = len(operation_ids)
    print(
        f"Validated {CONTRACT}: {operation_count} operations, "
        f"{len(schemas)} schemas, {len(paths)} paths."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
