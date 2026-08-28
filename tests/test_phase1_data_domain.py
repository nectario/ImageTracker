from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from services.data.database import (
    DatabaseConfigurationError,
    SsmParameterResolver,
    build_database_runtime,
    create_mysql_engine,
    database_config_from_secret,
    transaction_scope,
)
from services.common.settings import AppSettings
from services.data.models import (
    Base,
    Device,
    IdempotencyRecord,
    LegacyImageAssetMap,
    MediaAsset,
    MediaChange,
    MediaLocation,
    MediaOccurrence,
    MediaSource,
    ProcessingJob,
    ProviderUsageMonth,
    UserAccount,
)
from services.enrichment.models import GeocodeResolution, ReverseGeocodeResult
from services.domain.errors import ConflictError, NotFoundError
from services.domain.models import (
    AccountIdentity,
    DeviceRegistration,
    GeoPoint,
    JobQuery,
    ManifestCommand,
    ManifestDelete,
    ManifestUpsert,
    MediaQuery,
    MutationContext,
    SourceCreate,
)
from services.domain.service import Phase1DomainService
from services.domain.repositories import AccountRepository
from services.worker.contracts import GeocodeJobFailure
from services.enrichment.models import ProviderFailureClass


FIXED_NOW = datetime(2026, 8, 27, 16, 0, 0)
PHOTO_HASH = "a" * 64


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


@pytest.fixture()
def service(session_factory: sessionmaker[Session]) -> Phase1DomainService:
    return Phase1DomainService(session_factory, clock=lambda: FIXED_NOW)


def run(awaitable):
    return asyncio.run(awaitable)


def context(key: str, *, request_hash: str | None = None) -> MutationContext:
    return MutationContext(
        request_id=uuid4(),
        idempotency_key=key,
        operation="POST",
        target=f"/test/{key}",
        request_hash=request_hash or (key.encode().hex() + "0" * 64)[:64],
    )


def bootstrap(
    service: Phase1DomainService,
    *,
    subject: str = "cognito-user-1",
    email: str | None = "user@example.com",
):
    return run(
        service.current_user(
            AccountIdentity(cognito_subject=subject, email=email)
        )
    )


def register_device(
    service: Phase1DomainService,
    user_id: UUID,
    *,
    key: str,
    name: str,
):
    return run(
        service.register_device(
            user_id,
            DeviceRegistration(
                installation_id=UUID(key),
                platform="WindowsCLI",
                display_name=name,
                app_version="0.3.0",
                os_version="11",
            ),
            context(f"device-{key}"),
        )
    ).value


def create_source(
    service: Phase1DomainService,
    user_id: UUID,
    device_id: UUID,
    *,
    source_key: str,
    mode: str = "Local",
):
    return run(
        service.create_source(
            user_id,
            SourceCreate(
                device_id=device_id,
                source_key=source_key,
                source_type="Folder",
                display_name=source_key,
                storage_mode=mode,
            ),
            context(f"source-{source_key}"),
        )
    ).value


def manifest_upsert(
    source_item_id: str,
    *,
    revision: str = "r1",
    content_hash: str = PHOTO_HASH,
    latitude: Decimal = Decimal("40.668700"),
    longitude: Decimal = Decimal("-74.114300"),
):
    return ManifestUpsert(
        source_item_id=source_item_id,
        source_revision=revision,
        file_name=f"{source_item_id}.JPG",
        local_locator=f"C:/Photos/{source_item_id}.JPG",
        content_sha256=content_hash,
        media_type="Photo",
        mime_type="image/jpeg",
        byte_size=1234,
        width_pixels=100,
        height_pixels=80,
        captured_at_local=datetime(2026, 8, 20, 12, 30),
        captured_at_utc=datetime(2026, 8, 20, 16, 30, tzinfo=timezone.utc),
        time_zone_id="America/New_York",
        utc_offset_minutes=-240,
        location=GeoPoint(
            latitude=latitude,
            longitude=longitude,
        ),
    )


class RecordingJobDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[UUID, ...], str]] = []

    def dispatch(self, *, job_ids: tuple[UUID, ...], job_type: str) -> None:
        self.calls.append((job_ids, job_type))


class FailOnceJobDispatcher(RecordingJobDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 1

    def dispatch(self, *, job_ids: tuple[UUID, ...], job_type: str) -> None:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("simulated dispatch failure")
        super().dispatch(job_ids=job_ids, job_type=job_type)


class FakeSsm:
    def __init__(self, value: str) -> None:
        self.value = value
        self.calls = 0

    def get_parameter(self, **kwargs):
        self.calls += 1
        assert kwargs == {"Name": "/test/mysql", "WithDecryption": True}
        return {"Parameter": {"Value": self.value}}


def test_ssm_resolution_is_cached_and_secret_is_database_restricted(monkeypatch) -> None:
    fake = FakeSsm(
        '{"host":"db.example","port":3306,"username":"app",'
        '"password":"secret","database":"ImageTracker","tls":true}'
    )
    resolver = SsmParameterResolver(region_name="us-east-2", client=fake)
    assert resolver.resolve("/test/mysql") == resolver.resolve("/test/mysql")
    assert fake.calls == 1

    config = database_config_from_secret(
        resolver.resolve("/test/mysql"), required_database="ImageTracker"
    )
    assert config.url.database == "ImageTracker"
    assert config.url.drivername == "mysql+pymysql"
    assert config.tls_enabled is True

    captured = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("services.data.database.create_engine", fake_create_engine)
    create_mysql_engine(config)
    assert captured["pool_size"] == 1
    assert captured["max_overflow"] == 0
    assert captured["pool_pre_ping"] is True
    assert captured["pool_recycle"] == 300
    assert captured["connect_args"]["ssl_verify_cert"] is True
    assert captured["connect_args"]["ssl_verify_identity"] is True

    captured.clear()
    plaintext_config = database_config_from_secret(
        "mysql://app:secret@db.example/ImageTracker?tls=false",
        required_database="ImageTracker",
    )
    assert plaintext_config.tls_enabled is False
    assert "tls" not in plaintext_config.url.query
    create_mysql_engine(plaintext_config)
    assert "ssl_verify_cert" not in captured["connect_args"]
    assert "ssl_verify_identity" not in captured["connect_args"]

    with pytest.raises(DatabaseConfigurationError, match="only to the ImageTracker"):
        database_config_from_secret(
            "mysql://app:secret@db.example/DeepTradingAI",
            required_database="ImageTracker",
        )


def test_database_runtime_uses_the_bundled_regional_rds_ca(monkeypatch) -> None:
    raw_secret = (
        '{"host":"db.example","port":3306,"username":"app",'
        '"password":"secret","database":"ImageTracker","tls":true}'
    )

    class StaticResolver:
        def resolve(self, parameter_name: str) -> str:
            assert parameter_name == "/test/mysql"
            return raw_secret

    captured = {}

    def fake_mysql_engine(config):
        captured["config"] = config
        return create_engine("sqlite+pysqlite:///:memory:")

    monkeypatch.setattr(
        "services.data.database.create_mysql_engine", fake_mysql_engine
    )
    runtime = build_database_runtime(
        AppSettings(
            aws_region="us-east-2",
            db_secret_parameter="/test/mysql",
        ),
        resolver=StaticResolver(),
    )
    try:
        ca_path = Path(captured["config"].ssl_ca)
        assert ca_path.name == "us-east-2-bundle.pem"
        assert ca_path.is_file()
    finally:
        runtime.engine.dispose()


def test_all_required_pascal_case_tables_are_mapped() -> None:
    required = {
        "UserAccount",
        "Device",
        "MediaSource",
        "MediaAsset",
        "MediaOccurrence",
        "MediaLocation",
        "MediaChange",
        "ProcessingJob",
        "IdempotencyRecord",
        "LegacyImageAssetMap",
    }
    assert required <= set(Base.metadata.tables)


def test_transaction_scope_rolls_back(session_factory) -> None:
    with pytest.raises(RuntimeError):
        with transaction_scope(session_factory) as session:
            session.add(
                UserAccount(
                    cognito_subject="rolled-back",
                    email=None,
                )
            )
            raise RuntimeError("stop")
    with transaction_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(UserAccount)) == 0


def test_account_bootstrap_accepts_missing_email_and_throttles_writes(
    service, session_factory
) -> None:
    first = bootstrap(service, email=None)
    assert first.email is None
    with transaction_scope(session_factory) as session:
        row = session.scalar(select(UserAccount))
        original_updated = row.updated_at_utc
        original_sign_in = row.last_sign_in_at_utc
    second = bootstrap(service, email=None)
    assert second.user_id == first.user_id
    with transaction_scope(session_factory) as session:
        row = session.scalar(select(UserAccount))
        assert row.updated_at_utc == original_updated
        assert row.last_sign_in_at_utc == original_sign_in


def test_account_bootstrap_recovers_concurrent_unique_winner_without_outer_rollback(
    session_factory, monkeypatch
) -> None:
    with transaction_scope(session_factory) as session:
        winner = UserAccount(
            cognito_subject="racing-subject",
            email=None,
            account_status="Active",
            last_sign_in_at_utc=FIXED_NOW,
            created_at_utc=FIXED_NOW,
            updated_at_utc=FIXED_NOW,
        )
        session.add(winner)

    original_lookup = AccountRepository.by_cognito_subject
    first_lookup = True

    def simulate_race(self, subject, *, for_update=False):
        nonlocal first_lookup
        if first_lookup and not for_update:
            first_lookup = False
            # The initial consistent read missed a row that another request
            # committed immediately afterward.
            return None
        return original_lookup(self, subject, for_update=for_update)

    monkeypatch.setattr(
        AccountRepository,
        "by_cognito_subject",
        simulate_race,
    )
    with transaction_scope(session_factory) as session:
        account, created = AccountRepository(session).bootstrap(
            cognito_subject="racing-subject",
            email="winner@example.com",
            display_name="Winner",
            now=FIXED_NOW,
        )
        assert created is False
        # This unrelated write proves the duplicate insert rolled back only
        # its nested savepoint, not the request's outer transaction.
        session.add(
            Device(
                user_id=account.id,
                device_key="00000000-0000-0000-0000-000000009999",
                display_name="Winner device",
                platform="WindowsCLI",
                operating_system_version="11",
                app_version="1.0",
                last_activity_at_utc=FIXED_NOW,
                created_at_utc=FIXED_NOW,
                updated_at_utc=FIXED_NOW,
            )
        )

    with transaction_scope(session_factory) as session:
        accounts = session.scalars(
            select(UserAccount).where(
                UserAccount.cognito_subject == "racing-subject"
            )
        ).all()
        assert len(accounts) == 1
        assert accounts[0].email == "winner@example.com"
        assert session.scalar(select(func.count()).select_from(Device)) == 1


def test_source_create_is_idempotent_and_reactivates_removed_identity(
    service, session_factory
) -> None:
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000061",
        name="Sources",
    )
    command = SourceCreate(
        device_id=device.device_id,
        source_key="reusable-source",
        source_type="Folder",
        display_name="Original",
    )
    created = run(
        service.create_source(
            user.user_id,
            command,
            context("source-reactivate-create"),
        )
    )
    assert created.status_code == 201

    compatible = run(
        service.create_source(
            user.user_id,
            command,
            context("source-reactivate-compatible"),
        )
    )
    assert compatible.status_code == 200
    assert compatible.value.source_id == created.value.source_id

    with pytest.raises(ConflictError, match="incompatible source"):
        run(
            service.create_source(
                user.user_id,
                SourceCreate(
                    device_id=device.device_id,
                    source_key="reusable-source",
                    source_type="Folder",
                    display_name="Different while active",
                ),
                context("source-reactivate-incompatible"),
            )
        )

    removed = run(
        service.remove_source(
            user.user_id,
            created.value.source_id,
            context("source-reactivate-remove"),
        )
    )
    assert removed.status_code == 204

    reactivated = run(
        service.create_source(
            user.user_id,
            SourceCreate(
                device_id=device.device_id,
                source_key="reusable-source",
                source_type="Folder",
                display_name="Reactivated",
            ),
            context("source-reactivate-new-invocation"),
        )
    )
    assert reactivated.status_code == 200
    assert reactivated.value.source_id == created.value.source_id
    assert reactivated.value.display_name == "Reactivated"
    assert reactivated.value.status == "Active"

    with transaction_scope(session_factory) as session:
        source = session.scalar(
            select(MediaSource).where(
                MediaSource.public_id == str(created.value.source_id)
            )
        )
        assert source.removed_at_utc is None
        assert source.source_status == "Active"
        assert session.scalar(select(func.count()).select_from(MediaSource)) == 1
        assert (
            session.scalar(
                select(func.count())
                .select_from(MediaChange)
                .where(MediaChange.entity_type == "MediaSource")
            )
            == 3
        )


def test_local_manifest_deduplicates_exact_bytes_and_replays_idempotently(
    service, session_factory
) -> None:
    user = bootstrap(service)
    first_device = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000001", name="One"
    )
    second_device = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000002", name="Two"
    )
    first_source = create_source(
        service, user.user_id, first_device.device_id, source_key="folder-one"
    )
    second_source = create_source(
        service, user.user_id, second_device.device_id, source_key="folder-two"
    )

    first_context = context("manifest-one")
    first = run(
        service.submit_manifest(
            user.user_id,
            first_source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("a"),),
            ),
            first_context,
        )
    )
    assert first.value.results[0].outcome == "CreatedOccurrence"
    assert first.value.results[0].upload_required is False

    replay = run(
        service.submit_manifest(
            user.user_id,
            first_source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("a"),),
            ),
            first_context,
        )
    )
    assert replay.replayed is True
    assert replay.value == first.value

    duplicate = run(
        service.submit_manifest(
            user.user_id,
            second_source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("b"),),
            ),
            context("manifest-two"),
        )
    )
    assert duplicate.value.results[0].outcome == "DuplicateLinked"

    rerun = run(
        service.submit_manifest(
            user.user_id,
            second_source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("b"),),
            ),
            context("manifest-two-rerun"),
        )
    )
    assert rerun.value.results[0].outcome == "Unchanged"

    with transaction_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(MediaAsset)) == 1
        assert session.scalar(select(func.count()).select_from(MediaOccurrence)) == 2
        assert session.scalar(select(func.count()).select_from(MediaLocation)) == 1


def test_geocode_jobs_dispatch_once_and_reuse_full_nearby_resolution(
    session_factory,
) -> None:
    dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: FIXED_NOW,
        job_dispatcher=dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000061",
        name="Geocode",
    )
    source = create_source(
        service, user.user_id, device.device_id, source_key="geocode-source"
    )
    request_context = context("geocode-first")
    first = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("first"),),
            ),
            request_context,
        )
    )
    assert len(dispatcher.calls) == 1
    assert dispatcher.calls[0][1] == "Geocode"
    assert len(dispatcher.calls[0][0]) == 1

    replay = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("first"),),
            ),
            request_context,
        )
    )
    assert replay.replayed is True
    assert len(dispatcher.calls) == 1

    with transaction_scope(session_factory) as session:
        first_location = session.scalar(select(MediaLocation))
        first_location.location_display_name = "Bayonne, NJ, USA"
        first_location.street_address = "99 Prospect Avenue"
        first_location.original_street_number = "101"
        first_location.neighborhood = "Constable Hook"
        first_location.city = "Bayonne"
        first_location.county = "Hudson County"
        first_location.state = "New Jersey"
        first_location.postal_code = "07002"
        first_location.country = "United States"
        first_location.country_code = "US"
        first_location.provider = "AmazonLocationPlacesV2"
        first_location.provider_place_id = "place-1"
        first_location.normalization_rule_version = "bayonne-v1"
        first_location.confidence = Decimal("0.9900")
        first_location.raw_provider_json = {
            "status": "OK",
            "marker": "raw",
            "OriginalAddress": {
                "DisplayName": "Bayonne, NJ, USA",
                "StreetAddress": "99 Prospect Avenue",
                "StreetNumber": "101",
                "Neighborhood": "Constable Hook",
                "City": "Bayonne",
                "County": "Hudson County",
                "State": "New Jersey",
                "PostalCode": "07002",
                "Country": "United States",
                "CountryCode": "US",
            },
        }
        first_location.provider_updated_at_utc = FIXED_NOW

    second = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    manifest_upsert(
                        "second",
                        content_hash="b" * 64,
                        latitude=Decimal("40.668730"),
                    ),
                ),
            ),
            context("geocode-nearby"),
        )
    ).value
    assert second.results[0].outcome == "CreatedOccurrence"
    assert len(dispatcher.calls) == 1
    with transaction_scope(session_factory) as session:
        copied = session.scalar(
            select(MediaLocation).where(
                MediaLocation.media_asset_id
                == session.scalar(
                    select(MediaAsset.id).where(
                        MediaAsset.public_id
                        == str(second.results[0].media_asset_id)
                    )
                )
            )
        )
        assert copied.latitude == Decimal("40.668730")
        assert copied.street_address == "99 Prospect Avenue"
        assert copied.original_street_number == "101"
        assert copied.provider == "AmazonLocationPlacesV2"
        assert copied.provider_place_id == "place-1"
        assert copied.normalization_rule_version is None
        assert copied.confidence is None
        assert copied.raw_provider_json["marker"] == "raw"
        assert copied.provider_updated_at_utc == FIXED_NOW
        assert session.scalar(
            select(func.count()).select_from(ProcessingJob).where(
                ProcessingJob.job_type == "Geocode"
            )
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(ProcessingJob).where(
                ProcessingJob.job_type == "Description"
            )
        ) == 2

    other_user = bootstrap(
        service, subject="geocode-other-user", email="other@example.com"
    )
    other_device = register_device(
        service,
        other_user.user_id,
        key="00000000-0000-0000-0000-000000000065",
        name="Other geocode user",
    )
    other_source = create_source(
        service,
        other_user.user_id,
        other_device.device_id,
        source_key="other-geocode-source",
    )
    run(
        service.submit_manifest(
            other_user.user_id,
            other_source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("other-user"),),
            ),
            context("other-user-geocode"),
        )
    )
    assert len(dispatcher.calls) == 2
    with transaction_scope(session_factory) as session:
        other_account_id = session.scalar(
            select(UserAccount.id).where(
                UserAccount.public_id == str(other_user.user_id)
            )
        )
        other_location = session.scalar(
            select(MediaLocation).where(MediaLocation.user_id == other_account_id)
        )
        assert other_location.provider is None
        assert session.scalar(
            select(func.count()).select_from(ProcessingJob).where(
                ProcessingJob.job_type == "Geocode"
            )
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(ProcessingJob).where(
                ProcessingJob.job_type == "Description"
            )
        ) == 3


def test_geocode_worker_claim_quota_and_completion_are_transactional(
    service, session_factory
) -> None:
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000062",
        name="Worker",
    )
    source = create_source(
        service, user.user_id, device.device_id, source_key="worker-source"
    )
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("worker-one"),),
            ),
            context("worker-one"),
        )
    )
    with transaction_scope(session_factory) as session:
        first_job_id = UUID(
            session.scalar(
                select(ProcessingJob.public_id).where(
                    ProcessingJob.job_type == "Geocode"
                )
            )
        )

    assert service.claim_geocode_job(job_id=uuid4(), message_id="orphan") is None
    claimed = service.claim_geocode_job(job_id=first_job_id, message_id="message-one")
    assert claimed is not None
    assert claimed.coordinate_revision
    assert service.find_reusable_location(job=claimed, radius_meters=15) is None
    assert service.reserve_provider_call(
        job=claimed,
        provider="AmazonLocationPlacesV2",
        monthly_limit=1,
    )
    # Reservation is idempotent for a duplicate delivery of the same lease.
    assert service.reserve_provider_call(
        job=claimed,
        provider="AmazonLocationPlacesV2",
        monthly_limit=1,
    )
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        assert usage.processed_units == Decimal("0.000000")
        assert usage.reserved_units == Decimal("1.000000")
        asset = session.scalar(select(MediaAsset))
        asset.time_zone = None
        asset.utc_offset_minutes = None
        asset.capture_datetime_utc = None
    assert service.consume_provider_call(
        job=claimed,
        provider="AmazonLocationPlacesV2",
    )
    with transaction_scope(session_factory) as session:
        usage = session.scalar(select(ProviderUsageMonth))
        assert usage.processed_units == Decimal("1.000000")
        assert usage.reserved_units == Decimal("0.000000")

    result = ReverseGeocodeResult(
        provider="AmazonLocationPlacesV2",
        provider_status="OK",
        resolution=GeocodeResolution(
            location_display_name="Bayonne, NJ, USA",
            street_address="99 Prospect Avenue",
            original_street_number="101",
            neighborhood="Constable Hook",
            city="Bayonne",
            county="Hudson County",
            state="New Jersey",
            postal_code="07002",
            country="United States",
            country_code="US",
            provider="AmazonLocationPlacesV2",
            provider_place_id="place-worker",
            raw_provider_json={"formatted_address": "99 Prospect Avenue"},
            normalization_rule_version="bayonne-v1",
            time_zone_id="America/New_York",
        ),
        raw_provider_json={"status": "OK", "results": [{"place_id": "place-worker"}]},
    )
    service.complete_geocode(job=claimed, result=result, reused=False)
    with transaction_scope(session_factory) as session:
        location = session.scalar(select(MediaLocation))
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.public_id == str(first_job_id))
        )
        usage = session.scalar(select(ProviderUsageMonth))
        asset = session.scalar(select(MediaAsset))
        assert location.street_address == "99 Prospect Avenue"
        assert location.original_street_number == "101"
        assert location.provider_place_id == "place-worker"
        assert location.normalization_rule_version == "bayonne-v1"
        assert location.raw_provider_json["status"] == "OK"
        assert job.status == "Succeeded"
        assert job.lease_token_hash is None
        assert usage.processed_units == Decimal("1.000000")
        assert usage.reserved_units == Decimal("0.000000")
        assert asset.time_zone == "America/New_York"
        assert asset.utc_offset_minutes == -240
        assert asset.capture_datetime_utc == datetime(2026, 8, 20, 16, 30)

    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    manifest_upsert(
                        "worker-two",
                        content_hash="c" * 64,
                        latitude=Decimal("41.000000"),
                        longitude=Decimal("-75.000000"),
                    ),
                ),
            ),
            context("worker-two"),
        )
    )
    with transaction_scope(session_factory) as session:
        second_job_id = UUID(
            session.scalar(
                select(ProcessingJob.public_id)
                .where(ProcessingJob.status == "Queued")
                .order_by(ProcessingJob.id.desc())
            )
        )
    second_claim = service.claim_geocode_job(
        job_id=second_job_id, message_id="message-two"
    )
    assert second_claim is not None
    assert not service.reserve_provider_call(
        job=second_claim,
        provider="AmazonLocationPlacesV2",
        monthly_limit=1,
    )
    service.defer_geocode_quota(
        job=second_claim,
        failure=GeocodeJobFailure(
            failure_class=ProviderFailureClass.QUOTA,
            code="MonthlyGeocodeLimitReached",
            user_message="Waiting for the monthly provider quota.",
            retryable=False,
        ),
        provider_called=False,
    )
    with transaction_scope(session_factory) as session:
        deferred = session.scalar(
            select(ProcessingJob).where(ProcessingJob.public_id == str(second_job_id))
        )
        usage = session.scalar(select(ProviderUsageMonth))
        assert deferred.status == "DeferredQuota"
        assert deferred.next_attempt_at_utc == datetime(2026, 9, 1)
        assert usage.processed_units == Decimal("1.000000")
        assert usage.reserved_units == Decimal("0.000000")


def test_geocode_completion_cannot_overwrite_newer_coordinates(
    service, session_factory
) -> None:
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000063",
        name="Stale",
    )
    source = create_source(service, user.user_id, device.device_id, source_key="stale")
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("stale"),),
            ),
            context("stale"),
        )
    )
    with transaction_scope(session_factory) as session:
        job_id = UUID(
            session.scalar(
                select(ProcessingJob.public_id).where(
                    ProcessingJob.job_type == "Geocode"
                )
            )
        )
    claim = service.claim_geocode_job(job_id=job_id, message_id="stale-message")
    assert claim is not None
    with transaction_scope(session_factory) as session:
        location = session.scalar(select(MediaLocation))
        location.latitude = Decimal("42.000000")
        location.longitude = Decimal("-76.000000")
    service.complete_geocode(
        job=claim,
        result=ReverseGeocodeResult(
            provider="AmazonLocationPlacesV2",
            provider_status="ZERO_RESULTS",
            resolution=None,
            raw_provider_json={"status": "ZERO_RESULTS", "results": []},
        ),
        reused=False,
    )
    with transaction_scope(session_factory) as session:
        location = session.scalar(select(MediaLocation))
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.job_type == "Geocode")
        )
        assert location.latitude == Decimal("42.000000")
        assert location.provider is None
        assert job.status == "Cancelled"
        assert job.failure_code == "StaleCoordinates"


def test_returning_to_an_earlier_coordinate_requeues_its_terminal_job(
    session_factory,
) -> None:
    dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: FIXED_NOW,
        job_dispatcher=dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000068",
        name="Coordinate return",
    )
    source = create_source(
        service, user.user_id, device.device_id, source_key="coordinate-return"
    )
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("moving-photo", revision="a"),),
            ),
            context("coordinate-a"),
        )
    )
    with transaction_scope(session_factory) as session:
        first_job = session.scalar(select(ProcessingJob))
        first_job_id = UUID(first_job.public_id)
        first_job.status = "Succeeded"
        first_job.completed_at_utc = FIXED_NOW
        location = session.scalar(select(MediaLocation))
        location.provider = "AmazonLocationPlacesV2"
        location.provider_updated_at_utc = FIXED_NOW
        location.street_address = "99 Prospect Avenue"

    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    manifest_upsert(
                        "moving-photo",
                        revision="b",
                        latitude=Decimal("41.000000"),
                        longitude=Decimal("-75.000000"),
                    ),
                ),
            ),
            context("coordinate-b"),
        )
    )
    with transaction_scope(session_factory) as session:
        second_job = session.scalar(
            select(ProcessingJob).where(
                ProcessingJob.public_id != str(first_job_id)
            )
        )
        second_job.status = "Succeeded"
        second_job.completed_at_utc = FIXED_NOW
        location = session.scalar(select(MediaLocation))
        location.provider = "AmazonLocationPlacesV2"
        location.provider_updated_at_utc = FIXED_NOW
        location.street_address = "200 Different Street"

    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("moving-photo", revision="c"),),
            ),
            context("coordinate-a-again"),
        )
    )

    assert dispatcher.calls[-1] == ((first_job_id,), "Geocode")
    with transaction_scope(session_factory) as session:
        first_job = session.scalar(
            select(ProcessingJob).where(
                ProcessingJob.public_id == str(first_job_id)
            )
        )
        location = session.scalar(select(MediaLocation))
        assert first_job.status == "Queued"
        assert first_job.attempt_count == 0
        assert first_job.completed_at_utc is None
        assert location.latitude == Decimal("40.668700")
        assert location.street_address is None
        assert location.provider is None


def test_local_visibility_and_safe_deletion(service) -> None:
    user = bootstrap(service)
    owner = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000011", name="Owner"
    )
    other = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000012", name="Other"
    )
    source = create_source(
        service, user.user_id, owner.device_id, source_key="private-folder"
    )
    created = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("private"),),
            ),
            context("private-create"),
        )
    ).value
    asset_id = created.results[0].media_asset_id
    assert len(run(service.list_media(user.user_id, owner.device_id, MediaQuery())).items) == 1
    assert len(run(service.list_media(user.user_id, other.device_id, MediaQuery())).items) == 0
    with pytest.raises(NotFoundError):
        run(service.get_media_asset(user.user_id, other.device_id, asset_id))

    ignored = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="Denied",
                deletion_detection_reliable=False,
                entries=(ManifestDelete("private", "r2"),),
            ),
            context("private-ignore-delete"),
        )
    ).value
    assert ignored.results[0].outcome == "IgnoredDeletion"
    assert len(run(service.list_media(user.user_id, owner.device_id, MediaQuery())).items) == 1

    deleted = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(ManifestDelete("private", "r3"),),
            ),
            context("private-delete"),
        )
    ).value
    assert deleted.results[0].outcome == "DeletedOccurrence"
    assert len(run(service.list_media(user.user_id, owner.device_id, MediaQuery())).items) == 0
    trash = run(
        service.list_media(
            user.user_id,
            owner.device_id,
            MediaQuery(trash_state="Trashed"),
        )
    )
    assert len(trash.items) == 1
    assert trash.items[0].is_trashed is True


def test_unhashed_occurrence_stays_pending_then_links_without_duplication(
    service, session_factory
) -> None:
    user = bootstrap(service)
    device = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000013", name="Hashing"
    )
    source = create_source(service, user.user_id, device.device_id, source_key="hashing")
    pending_entry = manifest_upsert("later")
    pending_entry = ManifestUpsert(
        source_item_id=pending_entry.source_item_id,
        source_revision=pending_entry.source_revision,
        file_name=pending_entry.file_name,
        local_locator=pending_entry.local_locator,
        content_sha256=None,
        media_type=pending_entry.media_type,
        mime_type=pending_entry.mime_type,
        byte_size=pending_entry.byte_size,
    )
    pending = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(pending_entry,),
            ),
            context("pending-hash"),
        )
    ).value
    assert pending.results[0].media_asset_id is None
    with transaction_scope(session_factory) as session:
        occurrence = session.scalar(select(MediaOccurrence))
        assert occurrence.hash_status == "Pending"
        assert occurrence.media_asset_id is None
        assert session.scalar(select(func.count()).select_from(MediaAsset)) == 0

    linked = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("later"),),
            ),
            context("completed-hash"),
        )
    ).value
    assert linked.results[0].media_asset_id is not None
    with transaction_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(MediaAsset)) == 1
        assert session.scalar(select(func.count()).select_from(MediaOccurrence)) == 1


def test_every_resource_query_is_user_scoped(service) -> None:
    first = bootstrap(service, subject="first", email="first@example.com")
    second = bootstrap(service, subject="second", email="second@example.com")
    device = register_device(
        service, first.user_id, key="00000000-0000-0000-0000-000000000021", name="First"
    )
    source = create_source(
        service, first.user_id, device.device_id, source_key="first-folder"
    )
    with pytest.raises(NotFoundError):
        run(service.get_source(second.user_id, source.source_id))
    with pytest.raises(NotFoundError):
        run(
            service.create_source(
                second.user_id,
                SourceCreate(
                    device_id=device.device_id,
                    source_key="cross-user",
                    source_type="Folder",
                    display_name="No",
                ),
                context("cross-user-source"),
            )
        )


def test_change_cursor_is_opaque_and_invalid_cursor_is_rejected(service) -> None:
    user = bootstrap(service)
    device = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000031", name="Cursor"
    )
    create_source(service, user.user_id, device.device_id, source_key="cursor-source")
    page = run(service.list_changes(user.user_id, device.device_id, limit=1))
    assert page.items
    assert page.items[0].cursor != "1"
    with pytest.raises(Exception, match="cursor"):
        run(service.list_changes(user.user_id, device.device_id, cursor="not-a-cursor"))


def test_failed_job_retry_is_scoped_and_idempotent(service, session_factory) -> None:
    user = bootstrap(service)
    device = register_device(
        service, user.user_id, key="00000000-0000-0000-0000-000000000041", name="Jobs"
    )
    source = create_source(service, user.user_id, device.device_id, source_key="jobs")
    created = run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("job-media"),),
            ),
            context("job-media"),
        )
    ).value
    with transaction_scope(session_factory) as session:
        account = session.scalar(
            select(UserAccount).where(UserAccount.public_id == str(user.user_id))
        )
        asset = session.scalar(
            select(MediaAsset).where(
                MediaAsset.public_id == str(created.results[0].media_asset_id)
            )
        )
        job = ProcessingJob(
            user_id=account.id,
            media_asset_id=asset.id,
            idempotency_key="seed-job",
            job_type="Metadata",
            status="Failed",
            attempt_count=1,
            max_attempts=5,
            failure_class="Transient",
            failure_code="Timeout",
            failure_message="Please retry",
            created_at_utc=FIXED_NOW,
            updated_at_utc=FIXED_NOW,
        )
        session.add(job)
        session.flush()
        job_id = UUID(job.public_id)

    mutation_context = context("retry-job")
    retried = run(service.retry_job(user.user_id, job_id, mutation_context))
    assert retried.value.status == "Queued"
    assert retried.status_code == 202
    replay = run(service.retry_job(user.user_id, job_id, mutation_context))
    assert replay.replayed is True
    assert replay.value == retried.value
    jobs = run(service.list_jobs(user.user_id, JobQuery(job_type="Metadata")))
    assert len(jobs.items) == 1


def test_quota_deferred_job_manual_retry_dispatches_once(session_factory) -> None:
    dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: FIXED_NOW,
        job_dispatcher=dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000064",
        name="Retry quota",
    )
    source = create_source(service, user.user_id, device.device_id, source_key="quota")
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("quota"),),
            ),
            context("quota-create"),
        )
    )
    assert len(dispatcher.calls) == 1
    with transaction_scope(session_factory) as session:
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.job_type == "Geocode")
        )
        job.status = "DeferredQuota"
        job.attempt_count = job.max_attempts
        job.failure_class = "Quota"
        job.failure_code = "MonthlyGeocodeLimitReached"
        job_id = UUID(job.public_id)

    retry_context = context("quota-manual-retry")
    retried = run(service.retry_job(user.user_id, job_id, retry_context))
    assert retried.value.status == "Queued"
    assert dispatcher.calls[-1] == ((job_id,), "Geocode")
    assert len(dispatcher.calls) == 2
    replay = run(service.retry_job(user.user_id, job_id, retry_context))
    assert replay.replayed is True
    assert len(dispatcher.calls) == 2


def test_due_job_sweep_recovers_orphans_and_promotes_description_restage(
    session_factory,
) -> None:
    dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: FIXED_NOW,
        job_dispatcher=dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000065",
        name="Recovery sweep",
    )
    source = create_source(
        service, user.user_id, device.device_id, source_key="recovery"
    )
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(
                    manifest_upsert("recover-one", content_hash="d" * 64),
                    manifest_upsert(
                        "recover-two",
                        content_hash="e" * 64,
                        latitude=Decimal("41.000000"),
                        longitude=Decimal("-75.000000"),
                    ),
                ),
            ),
            context("recovery-create"),
        )
    )
    dispatcher.calls.clear()
    with transaction_scope(session_factory) as session:
        geocodes = list(
            session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.job_type == "Geocode")
                .order_by(ProcessingJob.id)
            )
        )
        descriptions = list(
            session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.job_type == "Description")
                .order_by(ProcessingJob.id)
            )
        )
        geocodes[0].status = "Queued"
        geocodes[0].next_attempt_at_utc = FIXED_NOW - timedelta(minutes=10)
        geocodes[0].updated_at_utc = FIXED_NOW - timedelta(minutes=10)
        geocodes[1].status = "DeferredQuota"
        geocodes[1].attempt_count = geocodes[1].max_attempts
        geocodes[1].next_attempt_at_utc = FIXED_NOW - timedelta(seconds=1)
        geocodes[1].updated_at_utc = FIXED_NOW - timedelta(minutes=10)
        descriptions[0].status = "DeferredQuota"
        descriptions[0].attempt_count = descriptions[0].max_attempts
        descriptions[0].next_attempt_at_utc = FIXED_NOW - timedelta(seconds=1)
        descriptions[0].request_json = {
            **(descriptions[0].request_json or {}),
            "stagingBucket": "temporary",
            "stagingObjectKey": "temporary/object.jpg",
            "uploadSessionId": str(uuid4()),
        }

    assert service.redispatch_due_jobs(limit=100) == 2
    assert len(dispatcher.calls) == 1
    dispatched_ids, dispatched_type = dispatcher.calls[0]
    assert dispatched_type == "Geocode"
    assert len(dispatched_ids) == 2
    with transaction_scope(session_factory) as session:
        geocodes = list(
            session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.job_type == "Geocode")
                .order_by(ProcessingJob.id)
            )
        )
        description = session.scalar(
            select(ProcessingJob)
            .where(ProcessingJob.job_type == "Description")
            .order_by(ProcessingJob.id)
        )
        assert all(job.status == "Queued" for job in geocodes)
        assert geocodes[1].attempt_count == 0
        assert description.status == "Preparing"
        assert description.attempt_count == 0
        assert description.next_attempt_at_utc is None
        assert "stagingBucket" not in description.request_json
        assert "stagingObjectKey" not in description.request_json
        assert "uploadSessionId" not in description.request_json


def test_due_job_sweep_conservatively_charges_expired_exhausted_attempt(
    session_factory,
) -> None:
    dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: FIXED_NOW,
        job_dispatcher=dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000066",
        name="Expired attempt",
    )
    source = create_source(service, user.user_id, device.device_id, source_key="expired")
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("expired-attempt", content_hash="f" * 64),),
            ),
            context("expired-attempt-create"),
        )
    )
    dispatcher.calls.clear()
    with transaction_scope(session_factory) as session:
        job_id = UUID(
            session.scalar(
                select(ProcessingJob.public_id).where(
                    ProcessingJob.job_type == "Geocode"
                )
            )
        )
    claim = service.claim_geocode_job(job_id=job_id, message_id="expired-message")
    assert claim is not None
    assert service.reserve_provider_call(
        job=claim,
        provider="AmazonLocationPlacesV2",
        monthly_limit=10,
    )
    with transaction_scope(session_factory) as session:
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.public_id == str(job_id))
        )
        job.attempt_count = job.max_attempts
        job.lease_expires_at_utc = FIXED_NOW - timedelta(seconds=1)

    assert service.redispatch_due_jobs(limit=100) == 0
    assert dispatcher.calls == []
    with transaction_scope(session_factory) as session:
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.public_id == str(job_id))
        )
        usage = session.scalar(select(ProviderUsageMonth))
        assert job.status == "Failed"
        assert job.failure_code == "AttemptsExhausted"
        assert usage.reserved_units == Decimal("0.000000")
        assert usage.processed_units == Decimal("1.000000")


def test_due_job_dispatch_failure_is_recovered_by_the_next_sweep(
    session_factory,
) -> None:
    clock = [FIXED_NOW]
    initial_dispatcher = RecordingJobDispatcher()
    service = Phase1DomainService(
        session_factory,
        clock=lambda: clock[0],
        job_dispatcher=initial_dispatcher,
    )
    user = bootstrap(service)
    device = register_device(
        service,
        user.user_id,
        key="00000000-0000-0000-0000-000000000067",
        name="Dispatch retry",
    )
    source = create_source(
        service, user.user_id, device.device_id, source_key="dispatch-retry"
    )
    run(
        service.submit_manifest(
            user.user_id,
            source.source_id,
            ManifestCommand(
                kind="Incremental",
                permission_state="NotApplicable",
                deletion_detection_reliable=True,
                entries=(manifest_upsert("dispatch-retry", content_hash="1" * 64),),
            ),
            context("dispatch-retry-create"),
        )
    )
    with transaction_scope(session_factory) as session:
        job = session.scalar(
            select(ProcessingJob).where(ProcessingJob.job_type == "Geocode")
        )
        job.next_attempt_at_utc = FIXED_NOW - timedelta(minutes=10)
        job.updated_at_utc = FIXED_NOW - timedelta(minutes=10)

    fail_once = FailOnceJobDispatcher()
    service._job_dispatcher = fail_once
    with pytest.raises(RuntimeError, match="simulated dispatch failure"):
        service.redispatch_due_jobs(limit=100)

    clock[0] = FIXED_NOW + timedelta(minutes=10)
    assert service.redispatch_due_jobs(limit=100) == 1
    assert len(fail_once.calls) == 1


def test_idempotency_key_cannot_be_reused_for_a_different_request(service) -> None:
    user = bootstrap(service)
    command = DeviceRegistration(
        installation_id=UUID("00000000-0000-0000-0000-000000000051"),
        platform="WindowsCLI",
        display_name="One",
        app_version="1",
        os_version="11",
    )
    run(service.register_device(user.user_id, command, context("same-key", request_hash="1" * 64)))
    with pytest.raises(ConflictError, match="different request"):
        run(
            service.register_device(
                user.user_id,
                command,
                context("same-key", request_hash="2" * 64),
            )
        )
