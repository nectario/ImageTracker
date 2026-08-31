from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from services.data.database import transaction_scope
from services.data.models import (
    Base,
    MediaAsset,
    MediaDescription,
    ProcessingJob,
    ProviderUsageMonth,
    UploadSession,
)
from services.domain.errors import ConflictError
from services.domain.models import (
    AccountIdentity,
    DeviceRegistration,
    ManifestCommand,
    ManifestUpsert,
    MutationContext,
    SourceCreate,
    TemporaryObjectMetadata,
    TemporaryObjectUpload,
    UploadCompleteCommand,
    UploadPlanCommand,
)
from services.domain.service import Phase1DomainService
from services.enrichment.models import ProviderFailureClass
from services.enrichment.openai_scene import SceneDescriptionResult
from services.worker.contracts import (
    DescriptionCleanupDecision,
    DescriptionJobFailure,
)


NOW = datetime(2026, 8, 28, 16, 0, 0)
PHOTO_HASH = "a" * 64
PREVIEW_HASH = "b" * 64


def run(awaitable):
    return asyncio.run(awaitable)


def context(key: str) -> MutationContext:
    return MutationContext(
        request_id=uuid4(),
        idempotency_key=key,
        operation="POST",
        target=f"/test/{key}",
        request_hash=(key.encode().hex() + "0" * 64)[:64],
    )


class RecordingDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[UUID, ...], str]] = []

    def dispatch(self, *, job_ids: tuple[UUID, ...], job_type: str) -> None:
        self.calls.append((job_ids, job_type))


class MemoryTemporaryStore:
    def __init__(self) -> None:
        self.plans: list[dict[str, object]] = []
        self.deleted: list[tuple[str, str]] = []
        self.objects: dict[tuple[str, str], TemporaryObjectMetadata] = {}

    def create_presigned_put(self, **kwargs) -> TemporaryObjectUpload:
        self.plans.append(dict(kwargs))
        bucket = "test-staging-bucket"
        key = f"staging/{kwargs['upload_session_id']}.jpg"
        return TemporaryObjectUpload(
            bucket=bucket,
            object_key=key,
            url=f"https://uploads.example.test/{key}?signature=test",
            headers={
                "Content-Type": str(kwargs["content_type"]),
                "Content-Length": str(kwargs["content_length"]),
                "x-amz-checksum-sha256": str(kwargs["checksum_sha256_base64"]),
            },
            expires_at_utc=kwargs["url_expires_at_utc"],
        )

    def head_object(
        self, *, bucket: str, object_key: str
    ) -> TemporaryObjectMetadata | None:
        return self.objects.get((bucket, object_key))

    def delete_object(self, *, bucket: str, object_key: str) -> None:
        self.deleted.append((bucket, object_key))
        self.objects.pop((bucket, object_key), None)


@pytest.fixture()
def session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    return sessionmaker(
        bind=engine,
        class_=Session,
        autoflush=False,
        expire_on_commit=False,
    )


def setup_photo(service: Phase1DomainService):
    user = run(
        service.current_user(
            AccountIdentity(cognito_subject="scene-user", email="scene@example.com")
        )
    )
    device = run(
        service.register_device(
            user.user_id,
            DeviceRegistration(
                installation_id=UUID("10000000-0000-4000-8000-000000000001"),
                platform="LinuxCLI",
                display_name="Scene CLI",
                app_version="0.3.0",
                os_version="Ubuntu",
            ),
            context("scene-device"),
        )
    ).value
    source = run(
        service.create_source(
            user.user_id,
            SourceCreate(
                device_id=device.device_id,
                source_key="scene-folder",
                source_type="Folder",
                display_name="Scene folder",
                storage_mode="Local",
            ),
            context("scene-source"),
        )
    ).value
    manifest = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    ManifestUpsert(
                        source_item_id="first",
                        source_revision="1",
                        file_name="first.jpg",
                        media_type="Photo",
                        mime_type="image/jpeg",
                        byte_size=1_234,
                        local_locator="/photos/first.jpg",
                        content_sha256=PHOTO_HASH,
                    ),
                    ManifestUpsert(
                        source_item_id="duplicate",
                        source_revision="1",
                        file_name="duplicate.jpg",
                        media_type="Photo",
                        mime_type="image/jpeg",
                        byte_size=1_234,
                        local_locator="/photos/duplicate.jpg",
                        content_sha256=PHOTO_HASH,
                    ),
                ),
            ),
            context("scene-manifest"),
        )
    ).value
    return user, source, manifest


def upload_command(source, result) -> UploadPlanCommand:
    return UploadPlanCommand(
        source_id=source.source_id,
        occurrence_id=result.occurrence_id,
        asset_content_sha256=PHOTO_HASH,
        object_sha256=PREVIEW_HASH,
        file_name="scene-preview.jpg",
        media_type="Photo",
        object_mime_type="image/jpeg",
        object_byte_size=512,
        purpose="TemporaryProcessing",
        processing_job_id=result.description_job_id,
    )


def stage_and_claim(
    service: Phase1DomainService,
    store: MemoryTemporaryStore,
    user,
    source,
    result,
    *,
    key: str,
    message_id: str,
):
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, result),
            context(f"{key}-plan"),
        )
    ).value
    object_key = f"staging/{planned.upload_session_id}.jpg"
    store.objects[("test-staging-bucket", object_key)] = TemporaryObjectMetadata(
        byte_size=512,
        content_type="image/jpeg",
        checksum_sha256_hex=PREVIEW_HASH,
    )
    run(
        service.complete_upload(
            user.user_id,
            planned.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            context(f"{key}-complete"),
        )
    )
    return service.claim_description_job(
        job_id=result.description_job_id,
        message_id=message_id,
    )


def test_duplicate_manifest_paths_share_one_preparing_description_job(
    session_factory,
) -> None:
    service = Phase1DomainService(session_factory, clock=lambda: NOW)
    user, source, manifest = setup_photo(service)

    first, duplicate = manifest.results
    assert first.media_asset_id == duplicate.media_asset_id
    assert first.description_job_id == duplicate.description_job_id
    assert first.description_job_id is not None
    job = run(service.get_job(user.user_id, first.description_job_id))
    assert job.status == "Preparing"
    assert job.state == "Preparing"
    with transaction_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(MediaAsset)) == 1
        assert session.scalar(select(func.count()).select_from(ProcessingJob)) == 1
        stored = session.scalar(select(ProcessingJob))
        assert stored.request_json["model"] == "gpt-5.6-terra"
        assert stored.request_json["promptVersion"] == "scene-search-v1"


def test_raw_photo_is_indexed_without_an_unfulfillable_description_job(
    session_factory,
) -> None:
    service = Phase1DomainService(session_factory, clock=lambda: NOW)
    user, source, _manifest = setup_photo(service)
    raw = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    ManifestUpsert(
                        source_item_id="camera-raw",
                        source_revision="1",
                        file_name="DSC_1001.CR3",
                        media_type="Photo",
                        mime_type="image/jpeg",
                        byte_size=25_000_000,
                        local_locator="/photos/DSC_1001.CR3",
                        content_sha256="e" * 64,
                    ),
                ),
            ),
            context("raw-photo-manifest"),
        )
    ).value.results[0]

    assert raw.media_asset_id is not None
    assert raw.description_job_id is None
    with transaction_scope(session_factory) as session:
        assert session.scalar(
            select(func.count()).select_from(ProcessingJob).where(
                ProcessingJob.job_type == "Description"
            )
        ) == 1


def test_plan_complete_and_replay_verify_bytes_and_dispatch_once(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    dispatcher = RecordingDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        job_dispatcher=dispatcher,
        scene_description_monthly_call_limit=10,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    command = upload_command(source, result)

    mutation = context("scene-plan")
    planned = run(service.create_upload_plan(user.user_id, command, mutation))
    assert planned.value.disposition == "UploadRequired"
    assert planned.value.strategy == "SinglePart"
    assert planned.value.single_part is not None
    assert len(store.plans) == 1
    replay = run(service.create_upload_plan(user.user_id, command, mutation))
    assert replay.replayed is True
    assert replay.value == planned.value
    assert len(store.plans) == 1

    held = run(
        service.create_upload_plan(user.user_id, command, context("scene-plan-held"))
    )
    assert held.value.disposition == "LeaseHeld"
    assert held.value.upload_session_id == planned.value.upload_session_id
    assert len(store.plans) == 1

    target = store.plans[0]
    bucket = "test-staging-bucket"
    key = f"staging/{planned.value.upload_session_id}.jpg"
    store.objects[(bucket, key)] = TemporaryObjectMetadata(
        byte_size=512,
        content_type="image/jpeg",
        checksum_sha256_hex=PREVIEW_HASH,
    )
    complete_context = context("scene-complete")
    completed = run(
        service.complete_upload(
            user.user_id,
            planned.value.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            complete_context,
        )
    )
    assert completed.value.storage_state == "LocalOnly"
    assert completed.value.processing_jobs == (result.description_job_id,)
    assert dispatcher.calls == [((result.description_job_id,), "Description")]
    complete_replay = run(
        service.complete_upload(
            user.user_id,
            planned.value.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            complete_context,
        )
    )
    assert complete_replay.replayed is True
    assert len(dispatcher.calls) == 1

    with transaction_scope(session_factory) as session:
        asset = session.scalar(select(MediaAsset))
        job = session.scalar(select(ProcessingJob))
        upload = session.scalar(select(UploadSession))
        assert asset.storage_state == "LocalOnly"
        assert asset.s3_bucket is None
        assert asset.original_s3_object_key is None
        assert asset.preview_s3_object_key is None
        assert job.status == "Queued"
        assert job.request_json["stagingBucket"] == bucket
        assert job.request_json["stagingObjectKey"] == key
        assert job.request_json["previewSha256"] == PREVIEW_HASH
        assert job.request_json["assetRevision"] == PHOTO_HASH
        assert upload.status == "Completed"
        assert upload.active_lease_marker is None


def test_quota_defers_without_creating_upload_or_presigned_url(session_factory) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=0,
    )
    user, source, manifest = setup_photo(service)
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, manifest.results[0]),
            context("quota-plan"),
        )
    )
    assert planned.value.disposition == "Deferred"
    assert planned.value.strategy == "None"
    assert planned.value.single_part is None
    assert store.plans == []
    with transaction_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(UploadSession)) == 0
        usage = session.scalar(select(ProviderUsageMonth))
        job = session.scalar(select(ProcessingJob))
        assert usage.processed_units == Decimal("0.000000")
        assert usage.reserved_units == Decimal("0.000000")
        assert job.status == "DeferredQuota"
        assert job.next_attempt_at_utc == datetime(2026, 9, 1)
    public_job = run(
        service.get_job(
            user.user_id, manifest.results[0].description_job_id
        )
    )
    assert public_job.state == "WaitingForMonthlyQuota"
    assert public_job.can_retry is True


def test_usd_ceiling_blocks_preview_before_any_provider_or_s3_work(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=100_000,
        scene_description_monthly_usd_limit=Decimal("230.000000"),
        scene_description_reserved_usd_per_request=Decimal("0.010000"),
    )
    user, source, manifest = setup_photo(service)
    with transaction_scope(session_factory) as session:
        job = session.scalar(select(ProcessingJob))
        session.add(
            ProviderUsageMonth(
                user_id=job.user_id,
                provider="OpenAI",
                usage_month=date(2026, 8, 1),
                unit_type="Usd",
                processed_units=Decimal("229.998000"),
                reserved_units=Decimal("0.000000"),
                hard_limit_units=Decimal("230.000000"),
                created_at_utc=NOW,
                updated_at_utc=NOW,
            )
        )

    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, manifest.results[0]),
            context("usd-limit-plan"),
        )
    ).value

    assert planned.disposition == "Deferred"
    assert store.plans == []
    with transaction_scope(session_factory) as session:
        usages = {
            row.unit_type: row
            for row in session.scalars(select(ProviderUsageMonth))
        }
        assert usages["Usd"].processed_units == Decimal("229.998000")
        assert usages["Usd"].reserved_units == Decimal("0.000000")
        assert usages["Request"].reserved_units == Decimal("0.000000")


def test_reservation_must_cover_provable_maximum_scene_request(session_factory) -> None:
    with pytest.raises(ValueError, match="below the maximum request cost"):
        Phase1DomainService(
            session_factory,
            scene_description_reserved_usd_per_request=Decimal("0.006000"),
        )


def test_cancel_and_expiry_release_reserved_quota(session_factory) -> None:
    clock = [NOW]
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: clock[0],
        temporary_object_store=store,
        scene_description_monthly_call_limit=1,
    )
    user, source, manifest = setup_photo(service)
    command = upload_command(source, manifest.results[0])
    first = run(
        service.create_upload_plan(user.user_id, command, context("cancel-plan"))
    ).value
    run(
        service.cancel_upload(
            user.user_id, first.upload_session_id, context("cancel-upload")
        )
    )
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        assert usage.reserved_units == Decimal("0.000000")
        assert session.scalar(select(ProcessingJob)).status == "Preparing"

    second = run(
        service.create_upload_plan(user.user_id, command, context("expire-plan"))
    ).value
    assert second.disposition == "UploadRequired"
    clock[0] = NOW + timedelta(days=2)
    third = run(
        service.create_upload_plan(user.user_id, command, context("replace-expired"))
    ).value
    assert third.disposition == "UploadRequired"
    assert third.upload_session_id != second.upload_session_id
    assert len(store.deleted) >= 2
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        assert usage.reserved_units == Decimal("1.000000")
        expired = session.scalar(
            select(UploadSession).where(
                UploadSession.public_id == str(second.upload_session_id)
            )
        )
        assert expired.status == "Expired"
        assert expired.active_lease_marker is None


def test_cancel_description_job_releases_preview_lease_and_quota(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=10,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, result),
            context("cancel-job-plan"),
        )
    ).value

    cancellation_context = context("cancel-description-job")
    cancelled = run(
        service.cancel_job(
            user.user_id,
            result.description_job_id,
            "UserSkipped",
            cancellation_context,
        )
    )
    assert cancelled.value.status == "Cancelled"
    assert cancelled.value.error_code == "UserSkipped"
    replay = run(
        service.cancel_job(
            user.user_id,
            result.description_job_id,
            "UserSkipped",
            cancellation_context,
        )
    )
    assert replay.replayed is True
    with transaction_scope(session_factory) as session:
        upload = session.scalar(
            select(UploadSession).where(
                UploadSession.public_id == str(planned.upload_session_id)
            )
        )
        usage = session.scalar(select(ProviderUsageMonth))
        assert upload.status == "Cancelled"
        assert upload.active_lease_marker is None
        assert usage.reserved_units == Decimal("0.000000")
    assert store.deleted == [
        (
            "test-staging-bucket",
            f"staging/{planned.upload_session_id}.jpg",
        )
    ]


def test_staging_rejects_wrong_purpose_hash_and_multipart(session_factory) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory, clock=lambda: NOW, temporary_object_store=store
    )
    user, source, manifest = setup_photo(service)
    command = upload_command(source, manifest.results[0])
    with pytest.raises(ConflictError, match="Only temporary"):
        run(
            service.create_upload_plan(
                user.user_id,
                UploadPlanCommand(**{**command.__dict__, "purpose": "Original"}),
                context("wrong-purpose"),
            )
        )
    with pytest.raises(ConflictError, match="asset content hash"):
        run(
            service.create_upload_plan(
                user.user_id,
                UploadPlanCommand(
                    **{**command.__dict__, "asset_content_sha256": "c" * 64}
                ),
                context("wrong-hash"),
            )
        )
    planned = run(
        service.create_upload_plan(user.user_id, command, context("parts-plan"))
    ).value
    with pytest.raises(ConflictError, match="single PUT"):
        run(
            service.complete_upload(
                user.user_id,
                planned.upload_session_id,
                UploadCompleteCommand(object_sha256=PREVIEW_HASH, parts=(object(),)),
                context("parts-complete"),
            )
        )


@pytest.mark.parametrize(
    ("provider_called", "expected_processed"),
    [(False, Decimal("0.000000")), (True, Decimal("1.000000"))],
)
def test_description_failure_releases_or_consumes_prereservation_exactly_once(
    session_factory, provider_called: bool, expected_processed: Decimal
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=1,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, result),
            context(f"failure-plan-{provider_called}"),
        )
    ).value
    key = f"staging/{planned.upload_session_id}.jpg"
    store.objects[("test-staging-bucket", key)] = TemporaryObjectMetadata(
        byte_size=512,
        content_type="image/jpeg",
        checksum_sha256_hex=PREVIEW_HASH,
    )
    run(
        service.complete_upload(
            user.user_id,
            planned.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            context(f"failure-complete-{provider_called}"),
        )
    )
    claimed = service.claim_description_job(
        job_id=result.description_job_id, message_id=f"failure-{provider_called}"
    )
    assert claimed is not None
    assert service.reserve_description_provider_call(
        job=claimed, provider="OpenAI", monthly_limit=1
    )
    assert service.consume_description_provider_call(
        job=claimed, provider="OpenAI"
    )
    outcome = service.fail_description(
        job=claimed,
        failure=DescriptionJobFailure(
            failure_class=ProviderFailureClass.INTERNAL,
            code="ScenePreviewUnavailable" if not provider_called else "ProviderFailure",
            user_message="Scene description could not be completed.",
            retryable=False,
        ),
        provider_called=provider_called,
    )
    assert outcome.cleanup is DescriptionCleanupDecision.DELETE
    assert outcome.retry_requested is False
    with transaction_scope(session_factory) as session:
        usages = {
            row.unit_type: row
            for row in session.scalars(select(ProviderUsageMonth))
        }
        assert usages["Request"].reserved_units == Decimal("0.000000")
        assert usages["Request"].processed_units == expected_processed
        assert usages["Usd"].reserved_units == Decimal("0.000000")
        assert usages["Usd"].processed_units == (
            Decimal("0.010000") if provider_called else Decimal("0.000000")
        )
        stored_job = session.scalar(select(ProcessingJob))
        assert stored_job.request_json["providerCost"]["basis"] == (
            "ConservativeReservation"
            if provider_called
            else "ReleasedNoProviderCall"
        )
        assert stored_job.request_json["providerRequestConsumption"]["state"] == (
            "Consumed" if provider_called else "Reversed"
        )


def test_expired_running_lease_conservatively_settles_started_cost_before_retry(
    session_factory,
) -> None:
    clock = [NOW]
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: clock[0],
        temporary_object_store=store,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    claimed = stage_and_claim(
        service,
        store,
        user,
        source,
        result,
        key="expired-cost",
        message_id="expired-first",
    )
    assert claimed is not None
    assert service.reserve_description_provider_call(
        job=claimed, provider="OpenAI", monthly_limit=100_000
    )
    assert service.consume_description_provider_call(job=claimed, provider="OpenAI")
    with transaction_scope(session_factory) as session:
        job = session.scalar(select(ProcessingJob))
        job.lease_expires_at_utc = NOW - timedelta(seconds=1)
    clock[0] = NOW + timedelta(minutes=1)

    reclaimed = service.claim_description_job(
        job_id=result.description_job_id,
        message_id="expired-second",
    )

    assert reclaimed is not None
    assert reclaimed.attempt_count == 2
    with transaction_scope(session_factory) as session:
        usages = {
            row.unit_type: row
            for row in session.scalars(select(ProviderUsageMonth))
        }
        job = session.scalar(select(ProcessingJob))
        assert usages["Request"].processed_units == Decimal("1.000000")
        assert usages["Usd"].processed_units == Decimal("0.010000")
        assert usages["Usd"].reserved_units == Decimal("0.000000")
        assert job.request_json["providerCost"]["basis"] == (
            "ConservativeReservation"
        )


def test_description_success_persists_current_result_usage_and_settles_quota(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=1,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, result),
            context("success-plan"),
        )
    ).value
    key = f"staging/{planned.upload_session_id}.jpg"
    store.objects[("test-staging-bucket", key)] = TemporaryObjectMetadata(
        byte_size=512,
        content_type="image/jpeg",
        checksum_sha256_hex=PREVIEW_HASH,
    )
    run(
        service.complete_upload(
            user.user_id,
            planned.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            context("success-complete"),
        )
    )
    with transaction_scope(session_factory) as session:
        stored = session.scalar(select(ProcessingJob))
        request = dict(stored.request_json)
        for key in (
            "monthlyUsdLimit",
            "reservedUsdPerRequest",
            "inputUsdPerMillion",
            "cachedInputUsdPerMillion",
            "outputUsdPerMillion",
        ):
            request.pop(key, None)
        stored.request_json = request
    claimed = service.claim_description_job(
        job_id=result.description_job_id, message_id="success-message"
    )
    assert claimed is not None
    assert claimed.monthly_usd_limit == Decimal("230.000000")
    assert claimed.reserved_usd_per_request == Decimal("0.010000")
    assert service.reserve_description_provider_call(
        job=claimed, provider="OpenAI", monthly_limit=1
    )
    assert service.consume_description_provider_call(
        job=claimed, provider="OpenAI"
    )
    cleanup = service.complete_description(
        job=claimed,
        result=SceneDescriptionResult(
            description="A red bicycle rests beside a sunny lakeside path.",
            provider="OpenAI",
            model="gpt-5.6-terra",
            prompt_version="scene-search-v1",
            usage={"input_tokens": 800, "output_tokens": 12, "total_tokens": 812},
        ),
    )
    assert cleanup is DescriptionCleanupDecision.DELETE
    with transaction_scope(session_factory) as session:
        description = session.scalar(select(MediaDescription))
        job = session.scalar(select(ProcessingJob))
        usages = {
            row.unit_type: row
            for row in session.scalars(select(ProviderUsageMonth))
        }
        assert description.status == "Succeeded"
        assert description.is_current == 1
        assert description.description.startswith("A red bicycle")
        assert job.status == "Succeeded"
        assert job.request_json["providerUsage"]["total_tokens"] == 812
        assert "providerUsageReservation" not in job.request_json
        assert usages["Request"].reserved_units == Decimal("0.000000")
        assert usages["Request"].processed_units == Decimal("1.000000")
        assert usages["Usd"].reserved_units == Decimal("0.000000")
        assert usages["Usd"].processed_units == Decimal("0.001744")
        assert job.request_json["providerCost"] == {
            "currency": "USD",
            "amount": "0.001744",
            "basis": "ActualUsage",
            "ratesPerMillion": {
                "input": "2.000000",
                "cachedInput": "0.200000",
                "output": "12.000000",
            },
        }


def test_out_of_bound_provider_usage_fails_and_charges_only_reservation(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
    )
    user, source, manifest = setup_photo(service)
    result = manifest.results[0]
    claimed = stage_and_claim(
        service,
        store,
        user,
        source,
        result,
        key="usage-bound",
        message_id="usage-bound-worker",
    )
    assert claimed is not None
    assert service.reserve_description_provider_call(
        job=claimed, provider="OpenAI", monthly_limit=100_000
    )
    assert service.consume_description_provider_call(job=claimed, provider="OpenAI")

    cleanup = service.complete_description(
        job=claimed,
        result=SceneDescriptionResult(
            description="A red bicycle rests beside a sunny lakeside path.",
            provider="OpenAI",
            model="gpt-5.6-terra",
            prompt_version="scene-search-v1",
            usage={"input_tokens": 3_000, "output_tokens": 12},
        ),
    )

    assert cleanup is DescriptionCleanupDecision.DELETE
    with transaction_scope(session_factory) as session:
        job = session.scalar(select(ProcessingJob))
        usd = session.scalar(
            select(ProviderUsageMonth).where(ProviderUsageMonth.unit_type == "Usd")
        )
        assert job.status == "Failed"
        assert job.failure_code == "InvalidSceneDescriptionResult"
        assert job.request_json["providerCost"]["amount"] == "0.010000"
        assert usd.processed_units == Decimal("0.010000")


def test_provider_auth_failure_opens_circuit_until_explicit_retry(
    session_factory,
) -> None:
    store = MemoryTemporaryStore()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: NOW,
        temporary_object_store=store,
        scene_description_monthly_call_limit=10,
    )
    user, source, manifest = setup_photo(service)
    first = manifest.results[0]
    planned = run(
        service.create_upload_plan(
            user.user_id,
            upload_command(source, first),
            context("circuit-first-plan"),
        )
    ).value
    key = f"staging/{planned.upload_session_id}.jpg"
    store.objects[("test-staging-bucket", key)] = TemporaryObjectMetadata(
        byte_size=512,
        content_type="image/jpeg",
        checksum_sha256_hex=PREVIEW_HASH,
    )
    run(
        service.complete_upload(
            user.user_id,
            planned.upload_session_id,
            UploadCompleteCommand(object_sha256=PREVIEW_HASH),
            context("circuit-first-complete"),
        )
    )
    claimed = service.claim_description_job(
        job_id=first.description_job_id, message_id="circuit-first-worker"
    )
    assert claimed is not None
    assert service.reserve_description_provider_call(
        job=claimed, provider="OpenAI", monthly_limit=10
    )
    assert service.consume_description_provider_call(
        job=claimed, provider="OpenAI"
    )
    service.fail_description(
        job=claimed,
        failure=DescriptionJobFailure(
            failure_class=ProviderFailureClass.AUTHENTICATION,
            code="OpenAIAuthenticationFailed",
            user_message="Scene description provider credentials need attention.",
            retryable=False,
        ),
        provider_called=True,
    )
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        assert usage.circuit_state == "Open"
        assert usage.circuit_failure_code == "OpenAIAuthenticationFailed"

    second_hash = "c" * 64
    second_manifest = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    ManifestUpsert(
                        source_item_id="second-circuit-photo",
                        source_revision="1",
                        file_name="second.jpg",
                        media_type="Photo",
                        mime_type="image/jpeg",
                        byte_size=2_000,
                        local_locator="/photos/second.jpg",
                        content_sha256=second_hash,
                    ),
                ),
            ),
            context("circuit-second-manifest"),
        )
    ).value.results[0]
    deferred = run(
        service.create_upload_plan(
            user.user_id,
            UploadPlanCommand(
                source_id=source.source_id,
                occurrence_id=second_manifest.occurrence_id,
                asset_content_sha256=second_hash,
                object_sha256="d" * 64,
                file_name="second-preview.jpg",
                media_type="Photo",
                object_mime_type="image/jpeg",
                object_byte_size=512,
                purpose="TemporaryProcessing",
                processing_job_id=second_manifest.description_job_id,
            ),
            context("circuit-second-plan"),
        )
    ).value
    assert deferred.disposition == "Deferred"
    assert run(
        service.get_job(user.user_id, second_manifest.description_job_id)
    ).status == "DeferredQuota"
    assert len(store.plans) == 1

    retried = run(
        service.retry_job(
            user.user_id,
            first.description_job_id,
            context("circuit-explicit-retry"),
        )
    ).value
    assert retried.status == "Preparing"
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        second_job = session.scalar(
            select(ProcessingJob).where(
                ProcessingJob.public_id
                == str(second_manifest.description_job_id)
            )
        )
        assert usage.circuit_state == "Closed"
        assert usage.circuit_failure_code is None
        assert second_job.status == "Preparing"
        assert second_job.failure_code is None
