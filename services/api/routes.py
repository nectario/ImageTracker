from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response, status

from services.api.auth import (
    get_current_user,
    get_phase1_service,
    require_admin,
)
from services.api.models import (
    AdminAuditResponse,
    AdminHealthResponse,
    AuditCheck,
    AuditSeverity,
    ChangePage,
    CurrentUser,
    DependencyHealth,
    Device,
    DevicePage,
    DeviceRegistrationRequest,
    HealthStatus,
    ManifestRequest,
    ManifestResponse,
    MediaAssetDetail,
    MediaAssetPage,
    MediaSearchPage,
    MediaSource,
    MediaSourceCreateRequest,
    MediaSourcePage,
    MediaSourceUpdateRequest,
    MediaType,
    ProcessingJob,
    ProcessingJobPage,
    ProcessingJobStatus,
    ProcessingJobType,
    JobCancelRequest,
    StorageMode,
    UploadCancelRequest,
    UploadCompleteRequest,
    UploadCompleteResponse,
    UploadPartSigningRequest,
    UploadPlan,
    UploadPlanRequest,
    UploadSessionStatus,
)
from services.api.service import (
    AuthIdentity,
    BadRequestError,
    JobQuery,
    MediaQuery,
    MediaSearchQuery,
    MutationContext,
    MutationResult,
    Phase1Service,
)


Cursor = Annotated[str | None, Query(min_length=1, max_length=2048)]
Limit = Annotated[int, Query(ge=1, le=200)]
DeviceId = Annotated[UUID, Header(alias="X-ImageTracker-Device-Id")]
IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    ),
]


def _mutation(
    request: Request,
    idempotency_key: str,
    target: str,
    body: DeviceRegistrationRequest
    | MediaSourceCreateRequest
    | MediaSourceUpdateRequest
    | ManifestRequest
    | UploadPlanRequest
    | UploadCompleteRequest
    | UploadCancelRequest
    | JobCancelRequest
    | None = None,
) -> MutationContext:
    return MutationContext.build(
        request_id=request.state.request_id,
        idempotency_key=idempotency_key,
        operation=request.method.upper(),
        target=target,
        body=body,
    )


def _apply_mutation_response(
    response: Response,
    result: MutationResult[object],
    *,
    allowed_statuses: set[int],
    default_status: int,
) -> None:
    response.status_code = (
        result.status_code if result.status_code in allowed_statuses else default_status
    )
    response.headers["Idempotency-Replayed"] = (
        "true" if result.replayed else "false"
    )


def _validate_date_window(
    captured_after_utc: datetime | None,
    captured_before_utc: datetime | None,
) -> None:
    if (
        captured_after_utc is not None
        and captured_before_utc is not None
        and captured_after_utc >= captured_before_utc
    ):
        raise BadRequestError(
            "capturedAfterUtc must be earlier than capturedBeforeUtc",
            code="INVALID_CAPTURE_WINDOW",
        )


def create_phase1_router() -> APIRouter:
    router = APIRouter(prefix="/v1")

    @router.get("/me", response_model=CurrentUser)
    async def get_me(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        return user

    @router.get("/devices", response_model=DevicePage)
    async def list_devices(
        cursor: Cursor = None,
        limit: Limit = 50,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> DevicePage:
        return await service.list_devices(user.user_id, cursor, limit)

    @router.post("/devices", response_model=Device, status_code=status.HTTP_201_CREATED)
    async def register_device(
        payload: DeviceRegistrationRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> Device:
        result = await service.register_device(
            user.user_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                "/v1/devices",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200, 201}, default_status=201
        )
        return result.value

    @router.get("/sources", response_model=MediaSourcePage)
    async def list_sources(
        cursor: Cursor = None,
        limit: Limit = 50,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaSourcePage:
        return await service.list_sources(user.user_id, cursor, limit)

    @router.post(
        "/sources", response_model=MediaSource, status_code=status.HTTP_201_CREATED
    )
    async def create_source(
        payload: MediaSourceCreateRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaSource:
        result = await service.create_source(
            user.user_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                "/v1/sources",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200, 201}, default_status=201
        )
        return result.value

    @router.get("/sources/{source_id}", response_model=MediaSource)
    async def get_source(
        source_id: UUID,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaSource:
        return await service.get_source(user.user_id, source_id)

    @router.patch("/sources/{source_id}", response_model=MediaSource)
    async def update_source(
        source_id: UUID,
        payload: MediaSourceUpdateRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaSource:
        result = await service.update_source(
            user.user_id,
            source_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                f"/v1/sources/{source_id}",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200}, default_status=200
        )
        return result.value

    @router.delete("/sources/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def remove_source(
        source_id: UUID,
        request: Request,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> Response:
        result = await service.remove_source(
            user.user_id,
            source_id,
            _mutation(
                request,
                idempotency_key,
                f"/v1/sources/{source_id}",
            ),
        )
        return Response(
            status_code=204,
            headers={
                "Idempotency-Replayed": "true" if result.replayed else "false"
            },
        )

    @router.post("/sources/{source_id}/manifest", response_model=ManifestResponse)
    async def submit_manifest(
        source_id: UUID,
        payload: ManifestRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ManifestResponse:
        result = await service.submit_manifest(
            user.user_id,
            source_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                f"/v1/sources/{source_id}/manifest",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200}, default_status=200
        )
        return result.value

    @router.post("/uploads/plan", response_model=UploadPlan)
    async def create_upload_plan(
        payload: UploadPlanRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> UploadPlan:
        result = await service.create_upload_plan(
            user.user_id,
            payload,
            _mutation(request, idempotency_key, "/v1/uploads/plan", payload),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200}, default_status=200
        )
        return result.value

    @router.get("/uploads/{upload_session_id}", response_model=UploadSessionStatus)
    async def get_upload_session(
        upload_session_id: UUID,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> UploadSessionStatus:
        return await service.get_upload_session(user.user_id, upload_session_id)

    @router.post("/uploads/{upload_session_id}/parts")
    async def reject_upload_parts(
        upload_session_id: UUID,
        payload: UploadPartSigningRequest,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
    ) -> None:
        del upload_session_id, payload, idempotency_key, user
        raise BadRequestError(
            "Temporary scene previews must use a single PUT",
            code="MULTIPART_NOT_SUPPORTED",
        )

    @router.post(
        "/uploads/{upload_session_id}/complete",
        response_model=UploadCompleteResponse,
    )
    async def complete_upload(
        upload_session_id: UUID,
        payload: UploadCompleteRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> UploadCompleteResponse:
        result = await service.complete_upload(
            user.user_id,
            upload_session_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                f"/v1/uploads/{upload_session_id}/complete",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200}, default_status=200
        )
        return result.value

    @router.post(
        "/uploads/{upload_session_id}/cancel",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    async def cancel_upload(
        upload_session_id: UUID,
        request: Request,
        idempotency_key: IdempotencyKey,
        payload: UploadCancelRequest | None = None,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> Response:
        body = payload or UploadCancelRequest()
        result = await service.cancel_upload(
            user.user_id,
            upload_session_id,
            body,
            _mutation(
                request,
                idempotency_key,
                f"/v1/uploads/{upload_session_id}/cancel",
                body,
            ),
        )
        return Response(
            status_code=204,
            headers={
                "Idempotency-Replayed": "true" if result.replayed else "false"
            },
        )

    @router.get("/changes", response_model=ChangePage)
    async def list_changes(
        requesting_device_id: DeviceId,
        cursor: Cursor = None,
        limit: Limit = 50,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ChangePage:
        return await service.list_changes(
            user.user_id, requesting_device_id, cursor, limit
        )

    # Register the static search route before /media/{media_asset_id}.
    @router.get("/media/search", response_model=MediaSearchPage)
    async def search_media(
        requesting_device_id: DeviceId,
        q: Annotated[str, Query(min_length=1, max_length=500)],
        cursor: Cursor = None,
        limit: Limit = 50,
        source_id: Annotated[UUID | None, Query(alias="sourceId")] = None,
        media_type: Annotated[MediaType | None, Query(alias="mediaType")] = None,
        storage_mode: Annotated[StorageMode | None, Query(alias="storageMode")] = None,
        captured_after_utc: Annotated[
            datetime | None, Query(alias="capturedAfterUtc")
        ] = None,
        captured_before_utc: Annotated[
            datetime | None, Query(alias="capturedBeforeUtc")
        ] = None,
        category: Annotated[
            str | None, Query(min_length=1, max_length=100)
        ] = None,
        has_location: Annotated[bool | None, Query(alias="hasLocation")] = None,
        trash_state: Annotated[
            Literal["Active", "Trashed", "All"], Query(alias="trashState")
        ] = "Active",
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaSearchPage:
        if not q.strip():
            raise BadRequestError("q cannot contain only whitespace", code="INVALID_QUERY")
        _validate_date_window(captured_after_utc, captured_before_utc)
        return await service.search_media(
            user.user_id,
            requesting_device_id,
            MediaSearchQuery(
                text=q,
                cursor=cursor,
                limit=limit,
                source_id=source_id,
                media_type=media_type,
                storage_mode=storage_mode,
                captured_after_utc=captured_after_utc,
                captured_before_utc=captured_before_utc,
                category=category,
                has_location=has_location,
                trash_state=trash_state,
                sort="CapturedAtDesc",
            ),
        )

    @router.get("/media", response_model=MediaAssetPage)
    async def list_media(
        requesting_device_id: DeviceId,
        cursor: Cursor = None,
        limit: Limit = 50,
        source_id: Annotated[UUID | None, Query(alias="sourceId")] = None,
        media_type: Annotated[MediaType | None, Query(alias="mediaType")] = None,
        storage_mode: Annotated[StorageMode | None, Query(alias="storageMode")] = None,
        captured_after_utc: Annotated[
            datetime | None, Query(alias="capturedAfterUtc")
        ] = None,
        captured_before_utc: Annotated[
            datetime | None, Query(alias="capturedBeforeUtc")
        ] = None,
        category: Annotated[
            str | None, Query(min_length=1, max_length=100)
        ] = None,
        has_location: Annotated[bool | None, Query(alias="hasLocation")] = None,
        trash_state: Annotated[
            Literal["Active", "Trashed", "All"], Query(alias="trashState")
        ] = "Active",
        sort: Literal["CapturedAtDesc", "CapturedAtAsc", "UpdatedAtDesc"] = (
            "CapturedAtDesc"
        ),
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaAssetPage:
        _validate_date_window(captured_after_utc, captured_before_utc)
        return await service.list_media(
            user.user_id,
            requesting_device_id,
            MediaQuery(
                cursor=cursor,
                limit=limit,
                source_id=source_id,
                media_type=media_type,
                storage_mode=storage_mode,
                captured_after_utc=captured_after_utc,
                captured_before_utc=captured_before_utc,
                category=category,
                has_location=has_location,
                trash_state=trash_state,
                sort=sort,
            ),
        )

    @router.get("/media/{media_asset_id}", response_model=MediaAssetDetail)
    async def get_media_asset(
        media_asset_id: UUID,
        requesting_device_id: DeviceId,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> MediaAssetDetail:
        return await service.get_media_asset(
            user.user_id, requesting_device_id, media_asset_id
        )

    @router.get("/jobs", response_model=ProcessingJobPage)
    async def list_jobs(
        cursor: Cursor = None,
        limit: Limit = 50,
        job_status: Annotated[
            ProcessingJobStatus | None, Query(alias="status")
        ] = None,
        job_type: Annotated[
            ProcessingJobType | None, Query(alias="jobType")
        ] = None,
        media_asset_id: Annotated[
            UUID | None, Query(alias="mediaAssetId")
        ] = None,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ProcessingJobPage:
        return await service.list_jobs(
            user.user_id,
            JobQuery(
                cursor=cursor,
                limit=limit,
                status=job_status,
                job_type=job_type,
                media_asset_id=media_asset_id,
            ),
        )

    @router.get("/jobs/{job_id}", response_model=ProcessingJob)
    async def get_job(
        job_id: UUID,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ProcessingJob:
        return await service.get_job(user.user_id, job_id)

    @router.post(
        "/jobs/{job_id}/retry",
        response_model=ProcessingJob,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def retry_job(
        job_id: UUID,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ProcessingJob:
        result = await service.retry_job(
            user.user_id,
            job_id,
            _mutation(
                request,
                idempotency_key,
                f"/v1/jobs/{job_id}/retry",
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={202}, default_status=202
        )
        return result.value

    @router.post(
        "/jobs/{job_id}/cancel",
        response_model=ProcessingJob,
        status_code=status.HTTP_200_OK,
    )
    async def cancel_job(
        job_id: UUID,
        payload: JobCancelRequest,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        user: CurrentUser = Depends(get_current_user),
        service: Phase1Service = Depends(get_phase1_service),
    ) -> ProcessingJob:
        result = await service.cancel_job(
            user.user_id,
            job_id,
            payload,
            _mutation(
                request,
                idempotency_key,
                f"/v1/jobs/{job_id}/cancel",
                payload,
            ),
        )
        _apply_mutation_response(
            response, result, allowed_statuses={200}, default_status=200
        )
        return result.value

    @router.get("/admin/health", response_model=AdminHealthResponse)
    async def admin_health(
        request: Request,
        identity: AuthIdentity = Depends(require_admin),
    ) -> AdminHealthResponse:
        settings = request.app.state.settings
        return AdminHealthResponse(
            service=settings.service_name,
            version=settings.service_version,
            status=HealthStatus.DEGRADED,
            time_utc=datetime.now(timezone.utc),
            dependencies=[
                DependencyHealth(
                    name="phase1-data-audit",
                    status=HealthStatus.UNAVAILABLE,
                    message="Read-only dependency probes are not wired yet",
                )
            ],
        )

    @router.get("/admin/audit", response_model=AdminAuditResponse)
    async def admin_audit(
        identity: AuthIdentity = Depends(require_admin),
    ) -> AdminAuditResponse:
        return AdminAuditResponse(
            run_at_utc=datetime.now(timezone.utc),
            read_only=True,
            status=HealthStatus.DEGRADED,
            checks=[
                AuditCheck(
                    code="AUDIT_NOT_WIRED",
                    severity=AuditSeverity.INFO,
                    title="Read-only Phase 1 audit is not wired",
                    count=0,
                    details="The endpoint is intentionally non-mutating and exposes no secrets.",
                )
            ],
        )

    return router
