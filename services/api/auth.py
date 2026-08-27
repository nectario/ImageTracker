from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fastapi import Depends, Request

from services.api.models import CurrentUser
from services.api.service import (
    AuthIdentity,
    AuthenticationError,
    ForbiddenError,
    Phase1Service,
)


def _normalize_groups(raw: Any) -> frozenset[str]:
    if raw is None:
        return frozenset()
    if isinstance(raw, (list, tuple, set, frozenset)):
        return frozenset(str(item) for item in raw if str(item).strip())
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith("["):
            try:
                decoded = json.loads(stripped)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, list):
                return frozenset(str(item) for item in decoded if str(item).strip())
            if stripped.endswith("]"):
                stripped = stripped[1:-1]
        return frozenset(
            item.strip().strip("'\"")
            for item in stripped.split(",")
            if item.strip().strip("'\"")
        )
    return frozenset()


def _extract_gateway_claims(request: Request) -> Mapping[str, Any] | None:
    """Read only claims already verified by API Gateway.

    Mangum exposes the original Lambda event on the ASGI scope. Raw bearer
    tokens are deliberately not decoded here: production trusts only the JWT
    authorizer result, keeping signature and issuer validation at the gateway.
    """

    event = request.scope.get("aws.event")
    if not isinstance(event, Mapping):
        return None
    request_context = event.get("requestContext")
    if not isinstance(request_context, Mapping):
        return None
    authorizer = request_context.get("authorizer")
    if not isinstance(authorizer, Mapping):
        return None

    # HTTP API JWT authorizer (the deployed ImageTracker shape).
    jwt = authorizer.get("jwt")
    if isinstance(jwt, Mapping) and isinstance(jwt.get("claims"), Mapping):
        return jwt["claims"]

    # Cognito REST API authorizer shape, accepted for portability.
    claims = authorizer.get("claims")
    if isinstance(claims, Mapping):
        return claims
    return None


def _identity_from_claims(claims: Mapping[str, Any]) -> AuthIdentity:
    subject = str(claims.get("sub") or "").strip()
    email_value = str(claims.get("email") or "").strip()
    display_name_value = str(
        claims.get("name") or claims.get("preferred_username") or ""
    ).strip()
    if not subject:
        raise AuthenticationError(
            "The verified identity is missing required claims",
            code="INVALID_IDENTITY_CLAIMS",
        )

    groups = _normalize_groups(claims.get("cognito:groups"))
    normalized_groups = {group.casefold() for group in groups}
    explicit_admin = str(
        claims.get("custom:imagetracker_admin")
        or claims.get("imagetracker:admin")
        or ""
    ).casefold() in {"1", "true", "yes"}
    is_admin = explicit_admin or bool(
        normalized_groups
        & {"imagetrackeradmin", "imagetracker-admin", "imagetracker_admin"}
    )
    return AuthIdentity(
        subject=subject,
        email=email_value or None,
        display_name=display_name_value or None,
        groups=groups,
        is_admin=is_admin,
    )


async def get_identity(request: Request) -> AuthIdentity:
    claims = _extract_gateway_claims(request)
    if claims is not None:
        return _identity_from_claims(claims)

    test_identity = getattr(request.app.state, "test_identity", None)
    if test_identity is not None:
        return test_identity
    raise AuthenticationError(
        "A verified Cognito identity is required",
        code="AUTHENTICATION_REQUIRED",
    )


def get_phase1_service(request: Request) -> Phase1Service:
    return request.app.state.phase1_service


async def get_current_user(
    identity: AuthIdentity = Depends(get_identity),
    service: Phase1Service = Depends(get_phase1_service),
) -> CurrentUser:
    return await service.current_user(identity)


async def require_admin(
    identity: AuthIdentity = Depends(get_identity),
) -> AuthIdentity:
    if not identity.is_admin:
        raise ForbiddenError(
            "This operation requires an ImageTracker administrator",
            code="ADMIN_REQUIRED",
        )
    return identity
