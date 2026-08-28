from __future__ import annotations

import json
import logging
from threading import Lock
from typing import Any, Callable, Mapping
from uuid import UUID

from services.enrichment.models import (
    GeocodeProviderError,
    ProviderFailureClass,
    ReverseGeocoder,
)
from services.enrichment.normalization import LocationNormalizer
from services.enrichment.openai_scene import (
    OpenAISceneDescriptionProvider,
    SceneDescriptionProviderError,
)
from services.worker.contracts import (
    DescriptionCleanupDecision,
    DescriptionJob,
    DescriptionJobFailure,
    DescriptionJobRepository,
    DueJobRepository,
    GeocodeJobFailure,
    GeocodeJobRepository,
    MessageDisposition,
    ScenePreviewStore,
    WorkerMessageProcessor,
)
from services.worker.staging import ScenePreviewStoreError


logger = logging.getLogger(__name__)


class InvalidWorkerMessage(ValueError):
    pass


class InvalidGeocodeMessage(InvalidWorkerMessage):
    pass


class InvalidDescriptionMessage(InvalidWorkerMessage):
    pass


class InvalidMaintenanceMessage(InvalidWorkerMessage):
    pass


class GeocodeMessageProcessor:
    def __init__(
        self,
        *,
        repository: GeocodeJobRepository,
        geocoder: ReverseGeocoder,
        normalizer: LocationNormalizer,
        reuse_radius_meters: float = 5.0,
        monthly_call_limit: int = 1_000,
    ) -> None:
        if reuse_radius_meters < 0:
            raise ValueError("The geocode reuse radius cannot be negative")
        if monthly_call_limit < 0:
            raise ValueError("The monthly geocode call limit cannot be negative")
        self._repository = repository
        self._geocoder = geocoder
        self._normalizer = normalizer
        self._reuse_radius_meters = reuse_radius_meters
        self._monthly_call_limit = monthly_call_limit

    def process_message(
        self,
        *,
        message_id: str,
        body: str | Mapping[str, Any],
    ) -> MessageDisposition:
        job_id = self._job_id_from_body(body)
        job = self._repository.claim_geocode_job(
            job_id=job_id,
            message_id=message_id,
        )
        if job is None:
            return MessageDisposition.ACK

        reusable = self._repository.find_reusable_location(
            job=job,
            radius_meters=self._reuse_radius_meters,
        )
        if reusable is not None:
            normalized = self._normalizer.normalize_result(reusable)
            self._repository.complete_geocode(
                job=job,
                result=normalized,
                reused=True,
            )
            return MessageDisposition.ACK

        reserved = self._repository.reserve_provider_call(
            job=job,
            provider=self._geocoder.provider,
            monthly_limit=self._monthly_call_limit,
        )
        if not reserved:
            self._repository.defer_geocode_quota(
                job=job,
                failure=GeocodeJobFailure(
                    failure_class=ProviderFailureClass.QUOTA,
                    code="MonthlyGeocodeLimitReached",
                    user_message=(
                        "Location enrichment is waiting for the monthly provider quota."
                    ),
                    retryable=False,
                ),
                provider_called=False,
            )
            return MessageDisposition.ACK

        if not self._repository.consume_provider_call(
            job=job,
            provider=self._geocoder.provider,
        ):
            return MessageDisposition.ACK

        try:
            result = self._geocoder.reverse_geocode(job.latitude, job.longitude)
        except GeocodeProviderError as exc:
            failure = GeocodeJobFailure(
                failure_class=exc.failure.failure_class,
                code=exc.failure.code,
                user_message=exc.failure.user_message,
                retryable=exc.failure.retryable,
            )
            if failure.failure_class is ProviderFailureClass.QUOTA:
                self._repository.defer_geocode_quota(
                    job=job, failure=failure, provider_called=True
                )
                return MessageDisposition.ACK
            repository_requests_retry = self._repository.fail_geocode(
                job=job,
                failure=failure,
            )
            return (
                MessageDisposition.RETRY
                if failure.retryable and repository_requests_retry
                else MessageDisposition.ACK
            )

        normalized = self._normalizer.normalize_result(result)
        self._repository.complete_geocode(
            job=job,
            result=normalized,
            reused=False,
        )
        return MessageDisposition.ACK

    @staticmethod
    def _job_id_from_body(body: str | Mapping[str, Any]) -> UUID:
        if isinstance(body, str):
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise InvalidGeocodeMessage("The SQS message body is not valid JSON") from exc
        else:
            payload = body
        if not isinstance(payload, Mapping):
            raise InvalidGeocodeMessage("The SQS message body must be a JSON object")
        if payload.get("jobType") != "Geocode":
            raise InvalidGeocodeMessage("The SQS message is not a Geocode job")
        raw_job_id = payload.get("jobId")
        try:
            return UUID(str(raw_job_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise InvalidGeocodeMessage("The SQS message has an invalid jobId") from exc


class DescriptionMessageProcessor:
    """Lease and execute a single scene-description job safely."""

    def __init__(
        self,
        *,
        repository: DescriptionJobRepository,
        provider: OpenAISceneDescriptionProvider,
        preview_store: ScenePreviewStore,
        monthly_call_limit: int = 1_000,
        preview_url_ttl_seconds: int = 300,
    ) -> None:
        if monthly_call_limit < 0:
            raise ValueError("The monthly scene-description limit cannot be negative")
        if (
            isinstance(preview_url_ttl_seconds, bool)
            or not isinstance(preview_url_ttl_seconds, int)
            or not 1 <= preview_url_ttl_seconds <= 900
        ):
            raise ValueError("The preview URL lifetime must be from 1 through 900 seconds")
        self._repository = repository
        self._provider = provider
        self._preview_store = preview_store
        self._monthly_call_limit = monthly_call_limit
        self._preview_url_ttl_seconds = preview_url_ttl_seconds

    def process_message(
        self,
        *,
        message_id: str,
        body: str | Mapping[str, Any],
    ) -> MessageDisposition:
        job_id = self._job_id_from_body(body)
        job = self._repository.claim_description_job(
            job_id=job_id,
            message_id=message_id,
        )
        if job is None:
            return MessageDisposition.ACK

        if (
            job.model != self._provider.model
            or job.prompt_version != self._provider.prompt_version
            or job.detail != self._provider.detail
            or job.service_tier != self._provider.service_tier
            or job.max_words != self._provider.max_words
            or job.monthly_call_limit != self._monthly_call_limit
        ):
            outcome = self._repository.fail_description(
                job=job,
                failure=DescriptionJobFailure(
                    failure_class=ProviderFailureClass.INTERNAL,
                    code="SceneDescriptionConfigurationChanged",
                    user_message=(
                        "Scene description configuration changed; request it again."
                    ),
                    retryable=False,
                ),
                provider_called=False,
            )
            self._cleanup_if_safe(job=job, decision=outcome.cleanup)
            return MessageDisposition.ACK

        try:
            preview_url = self._preview_store.create_presigned_get_url(
                bucket=job.staging_bucket,
                object_key=job.staging_object_key,
                expires_seconds=self._preview_url_ttl_seconds,
            )
        except ScenePreviewStoreError as exc:
            return self._handle_failure(
                job=job,
                failure=DescriptionJobFailure(
                    failure_class=exc.failure.failure_class,
                    code=exc.failure.code,
                    user_message=exc.failure.user_message,
                    retryable=exc.failure.retryable,
                ),
                provider_called=False,
            )

        reserved = self._repository.reserve_description_provider_call(
            job=job,
            provider=self._provider.provider,
            monthly_limit=self._monthly_call_limit,
        )
        if not reserved:
            cleanup = self._repository.defer_description_quota(
                job=job,
                failure=DescriptionJobFailure(
                    failure_class=ProviderFailureClass.QUOTA,
                    code="MonthlySceneDescriptionLimitReached",
                    user_message=(
                        "Scene description is waiting for the monthly provider quota."
                    ),
                    retryable=False,
                ),
                provider_called=False,
            )
            self._cleanup_if_safe(job=job, decision=cleanup)
            return MessageDisposition.ACK

        if not self._repository.consume_description_provider_call(
            job=job,
            provider=self._provider.provider,
        ):
            return MessageDisposition.ACK

        try:
            result = self._provider.describe(preview_url)
        except SceneDescriptionProviderError as exc:
            return self._handle_failure(
                job=job,
                failure=DescriptionJobFailure(
                    failure_class=exc.failure.failure_class,
                    code=exc.failure.code,
                    user_message=exc.failure.user_message,
                    retryable=exc.failure.retryable,
                ),
                provider_called=True,
            )

        cleanup = self._repository.complete_description(job=job, result=result)
        self._cleanup_if_safe(job=job, decision=cleanup)
        return MessageDisposition.ACK


    def _handle_failure(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool,
    ) -> MessageDisposition:
        if failure.failure_class is ProviderFailureClass.QUOTA:
            cleanup = self._repository.defer_description_quota(
                job=job,
                failure=failure,
                provider_called=provider_called,
            )
            self._cleanup_if_safe(job=job, decision=cleanup)
            return MessageDisposition.ACK

        outcome = self._repository.fail_description(
            job=job,
            failure=failure,
            provider_called=provider_called,
        )
        if failure.failure_class in {
            ProviderFailureClass.AUTHENTICATION,
            ProviderFailureClass.INTERNAL,
        }:
            self._cleanup_if_safe(job=job, decision=outcome.cleanup)
        return (
            MessageDisposition.RETRY
            if failure.retryable and outcome.retry_requested
            else MessageDisposition.ACK
        )

    def _cleanup_if_safe(
        self,
        *,
        job: DescriptionJob,
        decision: DescriptionCleanupDecision,
    ) -> None:
        if decision is not DescriptionCleanupDecision.DELETE:
            return
        try:
            self._preview_store.delete_object(
                bucket=job.staging_bucket,
                object_key=job.staging_object_key,
            )
        except Exception as exc:
            # The job transition is already durable. Retrying the message could
            # repeat a paid provider call, so bucket lifecycle handles leftovers.
            logger.warning(
                "Scene preview cleanup deferred jobId=%s errorType=%s",
                job.job_id,
                type(exc).__name__,
            )
            return

    @staticmethod
    def _job_id_from_body(body: str | Mapping[str, Any]) -> UUID:
        payload = _message_payload(body, InvalidDescriptionMessage)
        if payload.get("jobType") != "Description":
            raise InvalidDescriptionMessage(
                "The SQS message is not a Description job"
            )
        raw_job_id = payload.get("jobId")
        try:
            return UUID(str(raw_job_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise InvalidDescriptionMessage(
                "The SQS message has an invalid jobId"
            ) from exc


class DueJobMessageProcessor:
    """Republish due DB jobs so SQS delivery is never the sole source of truth."""

    def __init__(self, *, repository: DueJobRepository, limit: int = 100) -> None:
        if isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("The due-job sweep limit must be from 1 through 500")
        self._repository = repository
        self._limit = limit

    def process_message(
        self,
        *,
        message_id: str,
        body: str | Mapping[str, Any],
    ) -> MessageDisposition:
        del message_id
        payload = _message_payload(body, InvalidMaintenanceMessage)
        if payload.get("jobType") != "RetryDueJobs":
            raise InvalidMaintenanceMessage(
                "The SQS message is not a due-job maintenance sweep"
            )
        self._repository.redispatch_due_jobs(limit=self._limit)
        return MessageDisposition.ACK


class EnrichmentMessageRouter:
    """Route the shared processing queue without trusting message details."""

    def __init__(
        self,
        *,
        geocode: WorkerMessageProcessor,
        description: WorkerMessageProcessor,
        due_jobs: WorkerMessageProcessor | None = None,
    ) -> None:
        self._geocode = geocode
        self._description = description
        self._due_jobs = due_jobs

    def process_message(
        self,
        *,
        message_id: str,
        body: str | Mapping[str, Any],
    ) -> MessageDisposition:
        payload = _message_payload(body, InvalidWorkerMessage)
        job_type = payload.get("jobType")
        if job_type == "Geocode":
            return self._geocode.process_message(message_id=message_id, body=payload)
        if job_type == "Description":
            return self._description.process_message(message_id=message_id, body=payload)
        if job_type == "RetryDueJobs" and self._due_jobs is not None:
            return self._due_jobs.process_message(message_id=message_id, body=payload)
        raise InvalidWorkerMessage("The SQS message has an unsupported jobType")


class LazyEnrichmentMessageRouter:
    """Resolve and cache each provider only when its job type is received."""

    def __init__(
        self,
        *,
        geocode_factory: Callable[[], WorkerMessageProcessor],
        description_factory: Callable[[], WorkerMessageProcessor],
        due_jobs_factory: Callable[[], WorkerMessageProcessor] | None = None,
    ) -> None:
        self._geocode_factory = geocode_factory
        self._description_factory = description_factory
        self._due_jobs_factory = due_jobs_factory
        self._geocode: WorkerMessageProcessor | None = None
        self._description: WorkerMessageProcessor | None = None
        self._due_jobs: WorkerMessageProcessor | None = None
        self._geocode_lock = Lock()
        self._description_lock = Lock()
        self._due_jobs_lock = Lock()

    def process_message(
        self,
        *,
        message_id: str,
        body: str | Mapping[str, Any],
    ) -> MessageDisposition:
        payload = _message_payload(body, InvalidWorkerMessage)
        job_type = payload.get("jobType")
        if job_type == "Geocode":
            processor = self._get_geocode()
        elif job_type == "Description":
            processor = self._get_description()
        elif job_type == "RetryDueJobs" and self._due_jobs_factory is not None:
            processor = self._get_due_jobs()
        else:
            raise InvalidWorkerMessage("The SQS message has an unsupported jobType")
        return processor.process_message(message_id=message_id, body=payload)

    def _get_geocode(self) -> WorkerMessageProcessor:
        if self._geocode is not None:
            return self._geocode
        with self._geocode_lock:
            if self._geocode is None:
                self._geocode = self._geocode_factory()
        return self._geocode

    def _get_description(self) -> WorkerMessageProcessor:
        if self._description is not None:
            return self._description
        with self._description_lock:
            if self._description is None:
                self._description = self._description_factory()
        return self._description

    def _get_due_jobs(self) -> WorkerMessageProcessor:
        if self._due_jobs is not None:
            return self._due_jobs
        with self._due_jobs_lock:
            if self._due_jobs is None:
                assert self._due_jobs_factory is not None
                self._due_jobs = self._due_jobs_factory()
        return self._due_jobs


def _message_payload(
    body: str | Mapping[str, Any],
    error_type: type[InvalidWorkerMessage],
) -> Mapping[str, Any]:
    if isinstance(body, str):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise error_type("The SQS message body is not valid JSON") from exc
    else:
        payload = body
    if not isinstance(payload, Mapping):
        raise error_type("The SQS message body must be a JSON object")
    return payload
