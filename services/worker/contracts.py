from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Protocol
from uuid import UUID

from services.enrichment.models import (
    ProviderFailureClass,
    ReverseGeocodeResult,
)
from services.enrichment.openai_scene import SceneDescriptionResult


@dataclass(frozen=True)
class GeocodeJob:
    """The small, ownership-scoped projection a worker needs to process a job."""

    job_id: UUID
    user_id: int
    media_asset_id: int
    latitude: float
    longitude: float
    coordinate_revision: str
    lease_owner: str
    attempt_count: int
    max_attempts: int


@dataclass(frozen=True)
class GeocodeJobFailure:
    failure_class: ProviderFailureClass
    code: str
    user_message: str
    retryable: bool


@dataclass(frozen=True)
class DescriptionJob:
    """Validated, ownership-scoped scene-description work leased from MySQL.

    Staging coordinates are deliberately loaded from ``ProcessingJob.RequestJson``
    by the repository instead of trusted from the SQS envelope. The asset revision
    is based on content identity and is therefore independent of GPS enrichment.
    Model and prompt identities are selected by the server when the job is created,
    never accepted from a client upload-completion request.
    """

    job_id: UUID
    user_id: int
    media_asset_id: int
    asset_revision: str
    staging_bucket: str
    staging_object_key: str
    preview_sha256: str
    preview_byte_size: int
    preview_mime_type: str
    model: str
    prompt_version: str
    detail: str
    service_tier: str
    max_words: int
    monthly_call_limit: int
    lease_owner: str
    attempt_count: int
    max_attempts: int
    monthly_usd_limit: Decimal = Decimal("230.000000")
    reserved_usd_per_request: Decimal = Decimal("0.010000")
    input_usd_per_million: Decimal = Decimal("2.000000")
    cached_input_usd_per_million: Decimal = Decimal("0.200000")
    output_usd_per_million: Decimal = Decimal("12.000000")


@dataclass(frozen=True)
class DescriptionJobFailure:
    failure_class: ProviderFailureClass
    code: str
    user_message: str
    retryable: bool


class DescriptionCleanupDecision(str, Enum):
    """Whether a durable repository transition made preview deletion safe."""

    RETAIN = "Retain"
    DELETE = "Delete"


@dataclass(frozen=True)
class DescriptionFailureOutcome:
    """Atomic failure result returned by the persistence boundary."""

    retry_requested: bool
    cleanup: DescriptionCleanupDecision


class MessageDisposition(str, Enum):
    ACK = "Ack"
    RETRY = "Retry"


class GeocodeJobRepository(Protocol):
    """Atomic persistence boundary implemented by the domain/data layer.

    Every lookup and mutation must be scoped through the claimed job's
    ``user_id`` and ``media_asset_id``. A claim returns ``None`` when a stale or
    duplicate message no longer needs work.
    """

    def claim_geocode_job(
        self, *, job_id: UUID, message_id: str
    ) -> GeocodeJob | None: ...

    def find_reusable_location(
        self, *, job: GeocodeJob, radius_meters: float
    ) -> ReverseGeocodeResult | None: ...

    def reserve_provider_call(
        self,
        *,
        job: GeocodeJob,
        provider: str,
        monthly_limit: int,
    ) -> bool: ...

    def consume_provider_call(
        self,
        *,
        job: GeocodeJob,
        provider: str,
    ) -> bool:
        """Charge one reserved request immediately before the provider call."""

        ...

    def complete_geocode(
        self,
        *,
        job: GeocodeJob,
        result: ReverseGeocodeResult,
        reused: bool,
    ) -> None: ...

    def fail_geocode(
        self, *, job: GeocodeJob, failure: GeocodeJobFailure
    ) -> bool:
        """Persist a failure and return whether SQS should retry this message."""

        ...

    def defer_geocode_quota(
        self,
        *,
        job: GeocodeJob,
        failure: GeocodeJobFailure,
        provider_called: bool,
    ) -> None: ...


class DescriptionJobRepository(Protocol):
    """Atomic scene-description persistence and quota boundary.

    A claim must validate the job type, lease/staleness, current asset content
    revision, and every staging field stored in ``ProcessingJob.RequestJson``.
    Methods that return a cleanup decision do so only after their database
    transition is durable; the worker never infers that deletion is safe.
    """

    def claim_description_job(
        self, *, job_id: UUID, message_id: str
    ) -> DescriptionJob | None: ...

    def reserve_description_provider_call(
        self,
        *,
        job: DescriptionJob,
        provider: str,
        monthly_limit: int,
    ) -> bool:
        """Reuse a matching pre-reservation or reserve exactly once.

        Preview staging may reserve quota before uploading bytes. A duplicate
        delivery or worker claim must never increment that reservation twice.
        """

        ...

    def consume_description_provider_call(
        self,
        *,
        job: DescriptionJob,
        provider: str,
    ) -> bool:
        """Charge one reserved request immediately before the provider call."""

        ...

    def complete_description(
        self,
        *,
        job: DescriptionJob,
        result: SceneDescriptionResult,
    ) -> DescriptionCleanupDecision: ...

    def fail_description(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool,
    ) -> DescriptionFailureOutcome: ...

    def defer_description_quota(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool,
    ) -> DescriptionCleanupDecision: ...


class ScenePreviewStore(Protocol):
    """Minimal object-store capability required by the description worker."""

    def create_presigned_get_url(
        self,
        *,
        bucket: str,
        object_key: str,
        expires_seconds: int,
    ) -> str: ...

    def delete_object(self, *, bucket: str, object_key: str) -> None: ...


class WorkerMessageProcessor(Protocol):
    def process_message(
        self, *, message_id: str, body: str | Mapping[str, Any]
    ) -> MessageDisposition: ...


class DueJobRepository(Protocol):
    def redispatch_due_jobs(self, *, limit: int = 100) -> int:
        """Recover expired leases and publish durable jobs that are still due."""

        ...
