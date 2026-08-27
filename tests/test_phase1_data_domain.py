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
    UserAccount,
)
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


def manifest_upsert(source_item_id: str, *, content_hash: str = PHOTO_HASH):
    return ManifestUpsert(
        source_item_id=source_item_id,
        source_revision="r1",
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
            latitude=Decimal("40.668700"),
            longitude=Decimal("-74.114300"),
        ),
    )


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
            job_type="Description",
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
    jobs = run(service.list_jobs(user.user_id, JobQuery()))
    assert len(jobs.items) == 1


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
