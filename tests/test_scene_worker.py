from __future__ import annotations

from dataclasses import dataclass, replace
import json
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import NoCredentialsError

from services.enrichment.models import ProviderFailure, ProviderFailureClass
from services.enrichment.openai_scene import (
    SceneDescriptionProviderError,
    SceneDescriptionResult,
)
from services.worker.contracts import (
    DescriptionCleanupDecision,
    DescriptionFailureOutcome,
    DescriptionJob,
    DescriptionJobFailure,
    MessageDisposition,
)
from services.worker.processor import (
    DescriptionMessageProcessor,
    DueJobMessageProcessor,
    EnrichmentMessageRouter,
    InvalidDescriptionMessage,
    InvalidWorkerMessage,
    LazyEnrichmentMessageRouter,
)
from services.worker.staging import (
    S3ScenePreviewStore,
    ScenePreviewStoreError,
    ScenePreviewStoreFailure,
)


JOB_ID = UUID("00000000-0000-0000-0000-000000000456")


def description_result() -> SceneDescriptionResult:
    return SceneDescriptionResult(
        description="A red bicycle rests beside a sunny lakeside path.",
        provider="OpenAI",
        model="gpt-5.6-terra",
        prompt_version="scene-search-v1",
        usage={"input_tokens": 800, "output_tokens": 12, "total_tokens": 812},
    )


class FakeDescriptionRepository:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.job: DescriptionJob | None = DescriptionJob(
            job_id=JOB_ID,
            user_id=10,
            media_asset_id=20,
            asset_revision="a" * 64,
            staging_bucket="private-preview-bucket",
            staging_object_key="staging/redacted/preview.jpg",
            preview_sha256="b" * 64,
            preview_byte_size=12_345,
            preview_mime_type="image/jpeg",
            model="gpt-5.6-terra",
            prompt_version="scene-search-v1",
            detail="high",
            service_tier="flex",
            max_words=24,
            monthly_call_limit=1_000,
            lease_owner="message-1",
            attempt_count=1,
            max_attempts=5,
        )
        self.reservation_allowed = True
        self.complete_cleanup = DescriptionCleanupDecision.DELETE
        self.failure_outcome = DescriptionFailureOutcome(
            retry_requested=False,
            cleanup=DescriptionCleanupDecision.RETAIN,
        )
        self.quota_cleanup = DescriptionCleanupDecision.RETAIN
        self.completed_result: SceneDescriptionResult | None = None
        self.last_failure: DescriptionJobFailure | None = None
        self.last_provider_called: bool | None = None

    def claim_description_job(self, *, job_id: UUID, message_id: str):
        self.events.append("claim")
        assert job_id == JOB_ID
        assert message_id
        return self.job

    def reserve_description_provider_call(
        self, *, job: DescriptionJob, provider: str, monthly_limit: int
    ) -> bool:
        self.events.append("reserve")
        assert job is self.job
        assert provider == "OpenAI"
        assert monthly_limit == 1_000
        return self.reservation_allowed

    def consume_description_provider_call(self, *, job, provider):
        self.events.append("consume")
        assert job is self.job
        assert provider == "OpenAI"
        return True

    def complete_description(
        self, *, job: DescriptionJob, result: SceneDescriptionResult
    ) -> DescriptionCleanupDecision:
        self.events.append("complete")
        assert job is self.job
        self.completed_result = result
        return self.complete_cleanup

    def fail_description(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool,
    ) -> DescriptionFailureOutcome:
        self.events.append("fail")
        assert job is self.job
        self.last_failure = failure
        self.last_provider_called = provider_called
        return self.failure_outcome

    def defer_description_quota(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool,
    ) -> DescriptionCleanupDecision:
        self.events.append("defer")
        assert job is self.job
        self.last_failure = failure
        self.last_provider_called = provider_called
        return self.quota_cleanup


@dataclass
class FakeSceneProvider:
    outcome: SceneDescriptionResult | SceneDescriptionProviderError
    events: list[str]
    provider: str = "OpenAI"
    model: str = "gpt-5.6-terra"
    prompt_version: str = "scene-search-v1"
    detail: str = "high"
    service_tier: str = "flex"
    max_words: int = 24

    def __post_init__(self) -> None:
        self.urls: list[str] = []

    def describe(self, preview_url: str) -> SceneDescriptionResult:
        self.events.append("describe")
        self.urls.append(preview_url)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class FakePreviewStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.url = "https://preview.example.test/object.jpg?signature=short-lived"
        self.presign_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.presign_calls: list[tuple[str, str, int]] = []
        self.delete_calls: list[tuple[str, str]] = []

    def create_presigned_get_url(
        self, *, bucket: str, object_key: str, expires_seconds: int
    ) -> str:
        self.events.append("presign")
        self.presign_calls.append((bucket, object_key, expires_seconds))
        if self.presign_error is not None:
            raise self.presign_error
        return self.url

    def delete_object(self, *, bucket: str, object_key: str) -> None:
        self.events.append("delete")
        self.delete_calls.append((bucket, object_key))
        if self.delete_error is not None:
            raise self.delete_error


def provider_error(
    failure_class: ProviderFailureClass,
    *,
    retryable: bool,
) -> SceneDescriptionProviderError:
    return SceneDescriptionProviderError(
        ProviderFailure(
            failure_class=failure_class,
            code=f"{failure_class.value}Code",
            user_message="Safe scene-description failure.",
            retryable=retryable,
        )
    )


def processor(
    repository: FakeDescriptionRepository,
    provider: FakeSceneProvider,
    store: FakePreviewStore,
) -> DescriptionMessageProcessor:
    return DescriptionMessageProcessor(
        repository=repository,
        provider=provider,  # type: ignore[arg-type]
        preview_store=store,
        monthly_call_limit=1_000,
        preview_url_ttl_seconds=300,
    )


def body() -> str:
    return json.dumps({"jobId": str(JOB_ID), "jobType": "Description"})


def test_description_success_persists_usage_before_deleting_preview() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="message-1",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events == [
        "claim",
        "presign",
        "reserve",
        "consume",
        "describe",
        "complete",
        "delete",
    ]
    assert repository.completed_result == description_result()
    assert repository.completed_result.usage == {
        "input_tokens": 800,
        "output_tokens": 12,
        "total_tokens": 812,
    }
    assert store.presign_calls == [
        ("private-preview-bucket", "staging/redacted/preview.jpg", 300)
    ]
    assert store.delete_calls == [
        ("private-preview-bucket", "staging/redacted/preview.jpg")
    ]


def test_duplicate_or_stale_claim_acks_without_store_or_provider_work() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.job = None
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="duplicate",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events == ["claim"]
    assert provider.urls == []
    assert store.presign_calls == []


def test_server_owned_model_version_mismatch_fails_before_paid_work() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    assert repository.job is not None
    repository.job = replace(repository.job, model="superseded-model")
    repository.failure_outcome = DescriptionFailureOutcome(
        retry_requested=False,
        cleanup=DescriptionCleanupDecision.DELETE,
    )
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="configuration-changed",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events == ["claim", "fail", "delete"]
    assert provider.urls == []
    assert store.presign_calls == []
    assert repository.last_failure is not None
    assert repository.last_failure.code == "SceneDescriptionConfigurationChanged"
    assert repository.last_provider_called is False


@pytest.mark.parametrize(
    ("cleanup", "expected_deletes"),
    [
        (DescriptionCleanupDecision.RETAIN, 0),
        (DescriptionCleanupDecision.DELETE, 1),
    ],
)
def test_local_monthly_quota_acks_and_repository_decides_cleanup(
    cleanup: DescriptionCleanupDecision,
    expected_deletes: int,
) -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.reservation_allowed = False
    repository.quota_cleanup = cleanup
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="quota",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert provider.urls == []
    assert len(store.presign_calls) == 1
    assert len(store.delete_calls) == expected_deletes
    assert repository.last_failure is not None
    assert repository.last_failure.failure_class is ProviderFailureClass.QUOTA
    assert repository.last_failure.code == "MonthlySceneDescriptionLimitReached"
    assert repository.last_provider_called is False


def test_provider_quota_defers_and_uses_repository_cleanup_decision() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.quota_cleanup = DescriptionCleanupDecision.DELETE
    provider = FakeSceneProvider(
        provider_error(ProviderFailureClass.QUOTA, retryable=False),
        events,
    )
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="provider-quota",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events == [
        "claim",
        "presign",
        "reserve",
        "consume",
        "describe",
        "defer",
        "delete",
    ]
    assert repository.last_provider_called is True


def test_transient_failure_requests_retry_and_always_retains_preview() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.failure_outcome = DescriptionFailureOutcome(
        retry_requested=True,
        cleanup=DescriptionCleanupDecision.DELETE,
    )
    provider = FakeSceneProvider(
        provider_error(ProviderFailureClass.TRANSIENT, retryable=True),
        events,
    )
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="transient",
        body=body(),
    )

    assert disposition is MessageDisposition.RETRY
    assert events == ["claim", "presign", "reserve", "consume", "describe", "fail"]
    assert store.delete_calls == []
    assert repository.last_provider_called is True


def test_exhausted_transient_failure_acks_but_leaves_preview_to_lifecycle() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.failure_outcome = DescriptionFailureOutcome(
        retry_requested=False,
        cleanup=DescriptionCleanupDecision.DELETE,
    )
    provider = FakeSceneProvider(
        provider_error(ProviderFailureClass.TRANSIENT, retryable=True),
        events,
    )
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="transient-exhausted",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events == ["claim", "presign", "reserve", "consume", "describe", "fail"]
    assert store.delete_calls == []


@pytest.mark.parametrize(
    "failure_class",
    [ProviderFailureClass.AUTHENTICATION, ProviderFailureClass.INTERNAL],
)
def test_terminal_auth_or_internal_failure_deletes_only_after_durable_decision(
    failure_class: ProviderFailureClass,
) -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.failure_outcome = DescriptionFailureOutcome(
        retry_requested=False,
        cleanup=DescriptionCleanupDecision.DELETE,
    )
    provider = FakeSceneProvider(provider_error(failure_class, retryable=False), events)
    store = FakePreviewStore(events)

    disposition = processor(repository, provider, store).process_message(
        message_id="terminal",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events[-2:] == ["fail", "delete"]
    assert len(store.delete_calls) == 1
    assert repository.last_provider_called is True


def test_store_authentication_failure_is_safely_persisted_and_cleaned() -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    repository.failure_outcome = DescriptionFailureOutcome(
        retry_requested=False,
        cleanup=DescriptionCleanupDecision.DELETE,
    )
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)
    store.presign_error = ScenePreviewStoreError(
        ScenePreviewStoreFailure(
            failure_class=ProviderFailureClass.AUTHENTICATION,
            code="ScenePreviewCredentialUnavailable",
            user_message="Preview access could not be authorized.",
            retryable=False,
        )
    )

    disposition = processor(repository, provider, store).process_message(
        message_id="store-auth",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert provider.urls == []
    assert events == ["claim", "presign", "fail", "delete"]
    assert repository.last_provider_called is False


def test_cleanup_failure_never_retries_or_logs_object_details(caplog) -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    provider = FakeSceneProvider(description_result(), events)
    store = FakePreviewStore(events)
    store.delete_error = RuntimeError("object-key-must-not-escape")

    disposition = processor(repository, provider, store).process_message(
        message_id="cleanup-failure",
        body=body(),
    )

    assert disposition is MessageDisposition.ACK
    assert events[-2:] == ["complete", "delete"]
    assert "object-key-must-not-escape" not in caplog.text
    assert "RuntimeError" in caplog.text


class RecordingProcessor:
    def __init__(self, disposition: MessageDisposition) -> None:
        self.disposition = disposition
        self.calls: list[tuple[str, Any]] = []

    def process_message(self, *, message_id: str, body: Any) -> MessageDisposition:
        self.calls.append((message_id, body))
        return self.disposition


def test_router_dispatches_identity_only_jobs_and_rejects_unknown_types() -> None:
    geocode = RecordingProcessor(MessageDisposition.ACK)
    description = RecordingProcessor(MessageDisposition.RETRY)
    router = EnrichmentMessageRouter(geocode=geocode, description=description)

    result = router.process_message(
        message_id="description-message",
        body=body(),
    )

    assert result is MessageDisposition.RETRY
    assert geocode.calls == []
    assert description.calls == [
        (
            "description-message",
            {"jobId": str(JOB_ID), "jobType": "Description"},
        )
    ]
    with pytest.raises(InvalidWorkerMessage):
        router.process_message(
            message_id="unknown",
            body=json.dumps({"jobId": str(JOB_ID), "jobType": "Transcript"}),
        )


class FakeDueJobRepository:
    def __init__(self) -> None:
        self.limits: list[int] = []

    def redispatch_due_jobs(self, *, limit: int = 100) -> int:
        self.limits.append(limit)
        return 2


def test_due_job_processor_runs_bounded_recovery_and_router_accepts_schedule() -> None:
    repository = FakeDueJobRepository()
    due_jobs = DueJobMessageProcessor(repository=repository, limit=25)
    geocode = RecordingProcessor(MessageDisposition.ACK)
    description = RecordingProcessor(MessageDisposition.ACK)
    router = EnrichmentMessageRouter(
        geocode=geocode,
        description=description,
        due_jobs=due_jobs,
    )

    result = router.process_message(
        message_id="schedule-message",
        body={"jobType": "RetryDueJobs", "source": "schedule"},
    )

    assert result is MessageDisposition.ACK
    assert repository.limits == [25]
    assert geocode.calls == []
    assert description.calls == []


def test_location_provider_initialization_does_not_block_description_jobs() -> None:
    factory_calls: list[str] = []
    description = RecordingProcessor(MessageDisposition.ACK)

    def missing_location() -> RecordingProcessor:
        factory_calls.append("location")
        raise RuntimeError("Location provider is unavailable")

    def available_openai() -> RecordingProcessor:
        factory_calls.append("openai")
        return description

    router = LazyEnrichmentMessageRouter(
        geocode_factory=missing_location,
        description_factory=available_openai,
    )

    first = router.process_message(message_id="one", body=body())
    second = router.process_message(message_id="two", body=body())

    assert first is MessageDisposition.ACK
    assert second is MessageDisposition.ACK
    assert factory_calls == ["openai"]
    assert len(description.calls) == 2


def test_missing_openai_configuration_does_not_block_geocode_jobs() -> None:
    factory_calls: list[str] = []
    geocode = RecordingProcessor(MessageDisposition.ACK)

    def available_location() -> RecordingProcessor:
        factory_calls.append("location")
        return geocode

    def missing_openai() -> RecordingProcessor:
        factory_calls.append("openai")
        raise RuntimeError("OpenAI key is unavailable")

    router = LazyEnrichmentMessageRouter(
        geocode_factory=available_location,
        description_factory=missing_openai,
    )
    geocode_body = json.dumps({"jobId": str(JOB_ID), "jobType": "Geocode"})

    first = router.process_message(message_id="one", body=geocode_body)
    second = router.process_message(message_id="two", body=geocode_body)

    assert first is MessageDisposition.ACK
    assert second is MessageDisposition.ACK
    assert factory_calls == ["location"]
    assert len(geocode.calls) == 2


@pytest.mark.parametrize(
    "invalid",
    [
        "not-json",
        json.dumps([]),
        json.dumps({"jobId": str(JOB_ID), "jobType": "Geocode"}),
        json.dumps({"jobId": "not-a-uuid", "jobType": "Description"}),
    ],
)
def test_description_message_validation_is_permanent(invalid: str) -> None:
    events: list[str] = []
    repository = FakeDescriptionRepository(events)
    worker = processor(
        repository,
        FakeSceneProvider(description_result(), events),
        FakePreviewStore(events),
    )

    with pytest.raises(InvalidDescriptionMessage):
        worker.process_message(message_id="invalid", body=invalid)

    assert events == []


class FakeS3Client:
    def __init__(self) -> None:
        self.presign_error: Exception | None = None
        self.url = "https://bucket.s3.us-east-2.amazonaws.com/key?X-Amz-Signature=redacted"
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def generate_presigned_url(self, operation: str, **kwargs: Any) -> str:
        self.calls.append((operation, kwargs))
        if self.presign_error is not None:
            raise self.presign_error
        return self.url

    def delete_object(self, **kwargs: Any) -> None:
        self.calls.append(("delete_object", kwargs))


def test_s3_store_generates_https_get_and_deletes_exact_claimed_object() -> None:
    client = FakeS3Client()
    store = S3ScenePreviewStore(client, allowed_bucket="bucket")

    url = store.create_presigned_get_url(
        bucket="bucket",
        object_key="staging/private/preview.jpg",
        expires_seconds=300,
    )
    store.delete_object(bucket="bucket", object_key="staging/private/preview.jpg")

    assert url == client.url
    assert client.calls == [
        (
            "get_object",
            {
                "Params": {
                    "Bucket": "bucket",
                    "Key": "staging/private/preview.jpg",
                },
                "ExpiresIn": 300,
                "HttpMethod": "GET",
            },
        ),
        (
            "delete_object",
            {"Bucket": "bucket", "Key": "staging/private/preview.jpg"},
        ),
    ]


def test_s3_store_sanitizes_missing_credentials_and_rejects_http_url() -> None:
    secret_key = "staging/private/secret-filename.jpg"
    client = FakeS3Client()
    client.presign_error = NoCredentialsError()
    store = S3ScenePreviewStore(client, allowed_bucket="private-bucket")

    with pytest.raises(ScenePreviewStoreError) as missing:
        store.create_presigned_get_url(
            bucket="private-bucket",
            object_key=secret_key,
            expires_seconds=300,
        )

    assert missing.value.failure.failure_class is ProviderFailureClass.AUTHENTICATION
    assert secret_key not in str(missing.value)

    client.presign_error = None
    client.url = "http://private-bucket.example.test/secret-preview.jpg"
    with pytest.raises(ScenePreviewStoreError) as insecure:
        store.create_presigned_get_url(
            bucket="private-bucket",
            object_key=secret_key,
            expires_seconds=300,
        )
    assert insecure.value.failure.failure_class is ProviderFailureClass.INTERNAL
    assert secret_key not in str(insecure.value)


def test_s3_store_rejects_claim_bucket_outside_configured_media_bucket() -> None:
    client = FakeS3Client()
    store = S3ScenePreviewStore(client, allowed_bucket="configured-media-bucket")

    with pytest.raises(ScenePreviewStoreError) as presign_failure:
        store.create_presigned_get_url(
            bucket="untrusted-other-bucket",
            object_key="staging/private/preview.jpg",
            expires_seconds=300,
        )
    with pytest.raises(ScenePreviewStoreError) as delete_failure:
        store.delete_object(
            bucket="untrusted-other-bucket",
            object_key="staging/private/preview.jpg",
        )

    assert presign_failure.value.failure.code == "InvalidScenePreviewReference"
    assert delete_failure.value.failure.code == "InvalidScenePreviewReference"
    assert client.calls == []
