from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, status

from services.api.auth import get_current_user
from services.api.manifest_import_service import ManifestImportService
from services.api.models import (
    CurrentUser,
    ManifestImport,
    ManifestImportCreateRequest,
    ManifestImportResultDownload,
)
from services.api.service import MutationContext


IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    ),
]


def get_manifest_import_service(request: Request) -> ManifestImportService:
    return request.app.state.manifest_import_service


def create_manifest_import_router() -> APIRouter:
    router = APIRouter(prefix="/v1")

    @router.post(
        "/sources/{source_id}/manifest-imports",
        response_model=ManifestImport,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_import(
        source_id: UUID,
        payload: ManifestImportCreateRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: ManifestImportService = Depends(get_manifest_import_service),
    ) -> ManifestImport:
        # Import creation has no patch semantics. Hash the fully materialized,
        # normalized model so an explicit default or checksum letter casing
        # cannot turn a retry into a different idempotent request.
        canonical_values = payload.model_dump(mode="python")
        canonical_values["checksum_sha256"] = payload.checksum_sha256.lower()
        canonical_payload = ManifestImportCreateRequest.model_validate(
            canonical_values
        )
        mutation = MutationContext.build(
            request_id=request.state.request_id,
            idempotency_key=idempotency_key,
            operation=request.method.upper(),
            target=f"/v1/sources/{source_id}/manifest-imports",
            body=canonical_payload,
        )
        value, replayed = service.create(
            user_id=user.user_id,
            source_id=source_id,
            payload=payload,
            mutation=mutation,
        )
        response.status_code = 200 if replayed else 201
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        return value

    @router.post(
        "/sources/{source_id}/manifest-imports/{import_id}/upload-url",
        response_model=ManifestImport,
    )
    async def refresh_upload(
        source_id: UUID,
        import_id: UUID,
        _idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: ManifestImportService = Depends(get_manifest_import_service),
    ) -> ManifestImport:
        return service.refresh_upload(
            user_id=user.user_id,
            source_id=source_id,
            import_id=import_id,
        )

    @router.post(
        "/sources/{source_id}/manifest-imports/{import_id}/complete",
        response_model=ManifestImport,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def complete_import(
        source_id: UUID,
        import_id: UUID,
        _idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: ManifestImportService = Depends(get_manifest_import_service),
    ) -> ManifestImport:
        return service.complete(
            user_id=user.user_id,
            source_id=source_id,
            import_id=import_id,
        )

    @router.get(
        "/sources/{source_id}/manifest-imports/{import_id}",
        response_model=ManifestImport,
    )
    async def get_import(
        source_id: UUID,
        import_id: UUID,
        user: CurrentUser = Depends(get_current_user),
        service: ManifestImportService = Depends(get_manifest_import_service),
    ) -> ManifestImport:
        return service.get(
            user_id=user.user_id,
            source_id=source_id,
            import_id=import_id,
        )

    @router.get(
        "/sources/{source_id}/manifest-imports/{import_id}/result",
        response_model=ManifestImportResultDownload,
    )
    async def get_result(
        source_id: UUID,
        import_id: UUID,
        user: CurrentUser = Depends(get_current_user),
        service: ManifestImportService = Depends(get_manifest_import_service),
    ) -> ManifestImportResultDownload:
        return service.result(
            user_id=user.user_id,
            source_id=source_id,
            import_id=import_id,
        )

    return router
