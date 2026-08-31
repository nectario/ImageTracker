from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Generic, Literal, Protocol, TypeVar
from uuid import UUID

from pydantic import BaseModel

from services.api.models import (
    ChangePage,
    CurrentUser,
    Device,
    DevicePage,
    DeviceRegistrationRequest,
    EnrichmentPrepareRequest,
    EnrichmentPrepareResponse,
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
    JobCancelRequest,
    ProcessingJobStatus,
    ProcessingJobType,
    StorageMode,
    UploadCancelRequest,
    UploadCompleteRequest,
    UploadCompleteResponse,
    UploadPlan,
    UploadPlanRequest,
    UploadSessionStatus,
)


@dataclass(frozen=True, slots=True)
class AuthIdentity:
    subject: str
    email: str | None = None
    display_name: str | None = None
    groups: frozenset[str] = frozenset()
    is_admin: bool = False


@dataclass(frozen=True, slots=True)
class MutationContext:
    request_id: UUID
    idempotency_key: str
    operation: str
    target: str
    request_hash: str

    @classmethod
    def build(
        cls,
        *,
        request_id: UUID,
        idempotency_key: str,
        operation: str,
        target: str,
        body: BaseModel | None = None,
    ) -> "MutationContext":
        canonical_body = (
            body.model_dump(mode="json", by_alias=True, exclude_unset=True)
            if body is not None
            else None
        )
        canonical = json.dumps(
            {"operation": operation, "target": target, "body": canonical_body},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return cls(
            request_id=request_id,
            idempotency_key=idempotency_key,
            operation=operation,
            target=target,
            request_hash=sha256(canonical).hexdigest(),
        )


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class MutationResult(Generic[T]):
    value: T
    status_code: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class MediaQuery:
    cursor: str | None
    limit: int
    source_id: UUID | None
    media_type: MediaType | None
    storage_mode: StorageMode | None
    captured_after_utc: datetime | None
    captured_before_utc: datetime | None
    category: str | None
    has_location: bool | None
    trash_state: Literal["Active", "Trashed", "All"]
    sort: Literal["CapturedAtDesc", "CapturedAtAsc", "UpdatedAtDesc"]


@dataclass(frozen=True, slots=True)
class MediaSearchQuery(MediaQuery):
    text: str


@dataclass(frozen=True, slots=True)
class JobQuery:
    cursor: str | None
    limit: int
    status: ProcessingJobStatus | None
    job_type: ProcessingJobType | None
    media_asset_id: UUID | None


class ServiceError(Exception):
    status_code = 500
    title = "Service error"
    default_code = "SERVICE_ERROR"

    def __init__(self, detail: str | None = None, *, code: str | None = None) -> None:
        super().__init__(detail or self.title)
        self.detail = detail
        self.code = code or self.default_code


class BadRequestError(ServiceError):
    status_code = 400
    title = "Bad request"
    default_code = "BAD_REQUEST"


class AuthenticationError(ServiceError):
    status_code = 401
    title = "Unauthorized"
    default_code = "UNAUTHORIZED"


class NotFoundError(ServiceError):
    status_code = 404
    title = "Not found"
    default_code = "NOT_FOUND"


class ForbiddenError(ServiceError):
    status_code = 403
    title = "Forbidden"
    default_code = "FORBIDDEN"


class ConflictError(ServiceError):
    status_code = 409
    title = "Conflict"
    default_code = "CONFLICT"


class RateLimitError(ServiceError):
    status_code = 429
    title = "Too many requests"
    default_code = "RATE_LIMITED"


class ServiceUnavailableError(ServiceError):
    status_code = 503
    title = "Service unavailable"
    default_code = "SERVICE_UNAVAILABLE"


class Phase1Service(Protocol):
    async def current_user(self, identity: AuthIdentity) -> CurrentUser: ...

    async def list_devices(
        self, user_id: UUID, cursor: str | None, limit: int
    ) -> DevicePage: ...

    async def register_device(
        self,
        user_id: UUID,
        payload: DeviceRegistrationRequest,
        mutation: MutationContext,
    ) -> MutationResult[Device]: ...

    async def list_sources(
        self, user_id: UUID, cursor: str | None, limit: int
    ) -> MediaSourcePage: ...

    async def create_source(
        self,
        user_id: UUID,
        payload: MediaSourceCreateRequest,
        mutation: MutationContext,
    ) -> MutationResult[MediaSource]: ...

    async def get_source(self, user_id: UUID, source_id: UUID) -> MediaSource: ...

    async def update_source(
        self,
        user_id: UUID,
        source_id: UUID,
        payload: MediaSourceUpdateRequest,
        mutation: MutationContext,
    ) -> MutationResult[MediaSource]: ...

    async def remove_source(
        self, user_id: UUID, source_id: UUID, mutation: MutationContext
    ) -> MutationResult[None]: ...

    async def submit_manifest(
        self,
        user_id: UUID,
        source_id: UUID,
        payload: ManifestRequest,
        mutation: MutationContext,
    ) -> MutationResult[ManifestResponse]: ...

    async def prepare_enrichment(
        self,
        user_id: UUID,
        source_id: UUID,
        requesting_device_id: UUID,
        payload: EnrichmentPrepareRequest,
        mutation: MutationContext,
    ) -> MutationResult[EnrichmentPrepareResponse]: ...

    async def create_upload_plan(
        self,
        user_id: UUID,
        payload: UploadPlanRequest,
        mutation: MutationContext,
    ) -> MutationResult[UploadPlan]: ...

    async def get_upload_session(
        self, user_id: UUID, upload_session_id: UUID
    ) -> UploadSessionStatus: ...

    async def complete_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        payload: UploadCompleteRequest,
        mutation: MutationContext,
    ) -> MutationResult[UploadCompleteResponse]: ...

    async def cancel_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        payload: UploadCancelRequest,
        mutation: MutationContext,
    ) -> MutationResult[None]: ...

    async def list_changes(
        self,
        user_id: UUID,
        requesting_device_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> ChangePage: ...

    async def list_media(
        self, user_id: UUID, requesting_device_id: UUID, query: MediaQuery
    ) -> MediaAssetPage: ...

    async def search_media(
        self, user_id: UUID, requesting_device_id: UUID, query: MediaSearchQuery
    ) -> MediaSearchPage: ...

    async def get_media_asset(
        self, user_id: UUID, requesting_device_id: UUID, media_asset_id: UUID
    ) -> MediaAssetDetail: ...

    async def list_jobs(self, user_id: UUID, query: JobQuery) -> ProcessingJobPage: ...

    async def get_job(self, user_id: UUID, job_id: UUID) -> ProcessingJob: ...

    async def retry_job(
        self, user_id: UUID, job_id: UUID, mutation: MutationContext
    ) -> MutationResult[ProcessingJob]: ...

    async def cancel_job(
        self,
        user_id: UUID,
        job_id: UUID,
        payload: JobCancelRequest,
        mutation: MutationContext,
    ) -> MutationResult[ProcessingJob]: ...


class UnavailablePhase1Service:
    """Fail closed until the data-core composition root injects an implementation."""

    def __getattr__(self, name: str) -> Any:
        async def unavailable(*args: Any, **kwargs: Any) -> Any:
            raise ServiceUnavailableError(
                "The Phase 1 data service is not configured",
                code="DATA_SERVICE_NOT_CONFIGURED",
            )

        return unavailable
