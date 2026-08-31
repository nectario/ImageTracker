from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from uuid import UUID

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from services.api.app import create_app
from services.api.job_dispatcher import SqsManifestImportDispatcher
from services.api.manifest_import_service import SqlAlchemyManifestImportService
from services.api.manifest_store import (
    ManifestObjectDownload,
    ManifestObjectMetadata,
    ManifestObjectUpload,
)
from services.api.models import CurrentUser, ManifestImportCreateRequest
from services.api.service import (
    AuthIdentity,
    BadRequestError,
    ConflictError,
    MutationContext,
    NotFoundError,
)
from services.common.settings import AppSettings
from services.data.database import transaction_scope
from services.data.models import Base, Device, ManifestImport, MediaSource, UserAccount


USER_ID = UUID("00000000-0000-0000-0000-000000000501")
SOURCE_ID = UUID("00000000-0000-0000-0000-000000000502")
IMPORT_ID = UUID("00000000-0000-0000-0000-000000000503")
NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
OTHER_USER_ID = UUID("00000000-0000-0000-0000-000000000511")
OTHER_SOURCE_ID = UUID("00000000-0000-0000-0000-000000000512")


class ObjectStore:
    def __init__(self) -> None:
        self.metadata: ManifestObjectMetadata | None = None

    def create_input_upload(self, **kwargs):
        import_id = kwargs["import_id"]
        return ManifestObjectUpload(
            bucket="media-bucket",
            object_key=f"manifests/input/{kwargs['user_id']}/{kwargs['source_id']}/{import_id}.ndjson.gz",
            url="https://upload.invalid/manifest",
            headers={"Content-Length": str(kwargs["content_length"])},
            expires_at_utc=kwargs["expires_at_utc"],
        )

    def head_object(self, **_kwargs):
        return self.metadata

    def create_result_download(self, **kwargs):
        return ManifestObjectDownload(
            url="https://download.invalid/result",
            expires_at_utc=kwargs["expires_at_utc"],
        )


class Dispatcher:
    def __init__(self) -> None:
        self.import_ids: list[UUID] = []

    def dispatch(self, import_id: UUID) -> None:
        self.import_ids.append(import_id)


class FailingDispatcher(Dispatcher):
    def dispatch(self, import_id: UUID) -> None:
        del import_id
        raise RuntimeError("queue unavailable")


class CurrentUserService:
    async def current_user(self, _identity: AuthIdentity) -> CurrentUser:
        return CurrentUser(
            user_id=USER_ID,
            email="bulk@example.com",
            display_name="Bulk API",
            created_at_utc=NOW,
        )


def _request(
    app,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json: object | None = None,
) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            return await client.request(method, path, headers=headers, json=json)

    return asyncio.run(send())


def _seed_runtime(
    factory: sessionmaker[Session],
    *,
    user_id: UUID = USER_ID,
    source_id: UUID = SOURCE_ID,
    subject: str = "bulk-api-user",
) -> None:
    with transaction_scope(factory) as session:
        account = UserAccount(
            public_id=str(user_id),
            cognito_subject=subject,
            account_status="Active",
            created_at_utc=NOW.replace(tzinfo=None),
            updated_at_utc=NOW.replace(tzinfo=None),
        )
        session.add(account)
        session.flush()
        device = Device(
            public_id=str(UUID(int=user_id.int + 100)),
            user_id=account.id,
            device_key=f"bulk-api-device-{user_id}",
            display_name="Bulk API",
            platform="LinuxCLI",
            created_at_utc=NOW.replace(tzinfo=None),
            updated_at_utc=NOW.replace(tzinfo=None),
        )
        session.add(device)
        session.flush()
        session.add(
            MediaSource(
                public_id=str(source_id),
                user_id=account.id,
                device_id=device.id,
                source_key=f"bulk-api-source-{source_id}",
                display_name="Bulk API Source",
                source_type="Folder",
                storage_mode="Local",
                permission_state="NotApplicable",
                source_status="Active",
                created_at_utc=NOW.replace(tzinfo=None),
                updated_at_utc=NOW.replace(tzinfo=None),
            )
        )


@pytest.fixture
def runtime():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)
    _seed_runtime(factory)
    store = ObjectStore()
    dispatcher = Dispatcher()
    service = SqlAlchemyManifestImportService(
        factory,
        object_store=store,  # type: ignore[arg-type]
        dispatcher=dispatcher,
        clock=lambda: NOW,
    )
    return factory, store, dispatcher, service


def _payload(checksum: str = "ab" * 32) -> ManifestImportCreateRequest:
    return ManifestImportCreateRequest(
        snapshot_id="00000000-0000-0000-0000-000000000505",
        permission_state="NotApplicable",
        checksum_sha256=checksum,
        byte_size=1234,
        entry_count=25,
    )


def _mutation(payload: ManifestImportCreateRequest, key: str = "bulk-key-0001"):
    return MutationContext.build(
        request_id=UUID("00000000-0000-0000-0000-000000000506"),
        idempotency_key=key,
        operation="POST",
        target=f"/v1/sources/{SOURCE_ID}/manifest-imports",
        body=payload,
    )


def test_manifest_import_create_replay_complete_and_result(runtime):
    factory, store, dispatcher, service = runtime
    payload = _payload()
    created, replayed = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload),
    )

    assert replayed is False
    assert created.status == "AwaitingUpload"
    assert created.upload is not None
    import_id = created.import_id

    replay, replayed = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload),
    )
    assert replayed is True
    assert replay.import_id == import_id

    store.metadata = ManifestObjectMetadata(
        byte_size=1234,
        checksum_sha256_hex="ab" * 32,
        content_type="application/x-ndjson",
        content_encoding="gzip",
        version_id="version-1",
    )
    queued = service.complete(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        import_id=import_id,
    )
    assert queued.status == "Queued"
    assert dispatcher.import_ids == [import_id]

    with transaction_scope(factory) as session:
        row = session.scalar(
            select(ManifestImport).where(ManifestImport.public_id == str(import_id))
        )
        assert row is not None
        row.status = "Succeeded"
        row.phase = "Complete"
        row.result_s3_bucket = "media-bucket"
        row.result_s3_object_key = "manifests/result/result.ndjson.gz"
        row.result_checksum_sha256 = "cd" * 32
        row.result_byte_size = 321
        row.active_marker = None
        row.completed_at_utc = NOW.replace(tzinfo=None)

    terminal = service.get(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        import_id=import_id,
    )
    assert terminal.result_available is True
    download = service.result(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        import_id=import_id,
    )
    assert download.url == "https://download.invalid/result"
    assert download.checksum_sha256 == "cd" * 32
    assert download.byte_size == 321


def test_manifest_import_rejects_idempotency_key_reuse(runtime):
    _factory, _store, _dispatcher, service = runtime
    first = _payload()
    service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=first,
        mutation=_mutation(first),
    )
    second = _payload("ef" * 32)

    with pytest.raises(ConflictError, match="idempotency key"):
        service.create(
            user_id=USER_ID,
            source_id=SOURCE_ID,
            payload=second,
            mutation=_mutation(second),
        )


def test_manifest_import_routes_are_authenticated_and_create_is_idempotent(runtime):
    _factory, _store, _dispatcher, service = runtime
    app = create_app(
        AppSettings(stage="test"),
        phase1_service=CurrentUserService(),  # type: ignore[arg-type]
        manifest_import_service=service,
        test_identity=AuthIdentity(subject="bulk-api-user"),
    )
    registered_routes = []
    for candidate in app.routes:
        included = getattr(candidate, "original_router", None)
        registered_routes.extend(
            getattr(included, "routes", [candidate])
            if included is not None
            else [candidate]
        )
    route_methods = {
        (route.path, method)
        for route in registered_routes
        if hasattr(route, "path")
        for method in getattr(route, "methods", set())
    }
    base = f"/v1/sources/{SOURCE_ID}/manifest-imports"
    assert {
        ("/v1/sources/{source_id}/manifest-imports", "POST"),
        ("/v1/sources/{source_id}/manifest-imports/{import_id}/upload-url", "POST"),
        ("/v1/sources/{source_id}/manifest-imports/{import_id}/complete", "POST"),
        ("/v1/sources/{source_id}/manifest-imports/{import_id}", "GET"),
        ("/v1/sources/{source_id}/manifest-imports/{import_id}/result", "GET"),
    }.issubset(route_methods)
    body = _payload().model_dump(mode="json", by_alias=True)
    headers = {
        "Idempotency-Key": "bulk-route-0001",
        "X-Request-Id": "00000000-0000-0000-0000-000000000599",
    }

    created = _request(app, "POST", base, headers=headers, json=body)
    equivalent_minimal = {
        "snapshotId": body["snapshotId"],
        "permissionState": body["permissionState"],
        "checksumSha256": str(body["checksumSha256"]).upper(),
        "byteSize": body["byteSize"],
        "entryCount": body["entryCount"],
    }
    replayed = _request(
        app, "POST", base, headers=headers, json=equivalent_minimal
    )
    missing_key = _request(app, "POST", base, json=body)

    assert created.status_code == 201
    assert created.headers["idempotency-replayed"] == "false"
    assert replayed.status_code == 200
    assert replayed.headers["idempotency-replayed"] == "true"
    assert replayed.json()["importId"] == created.json()["importId"]
    assert missing_key.status_code == 400

    unauthenticated = create_app(
        AppSettings(stage="test"),
        phase1_service=CurrentUserService(),  # type: ignore[arg-type]
        manifest_import_service=service,
    )
    denied = _request(
        unauthenticated,
        "GET",
        f"{base}/{created.json()['importId']}",
    )
    assert denied.status_code == 401


def test_one_active_import_expires_before_a_new_snapshot(runtime):
    factory, store, dispatcher, _service = runtime
    clock = [NOW]
    service = SqlAlchemyManifestImportService(
        factory,
        object_store=store,  # type: ignore[arg-type]
        dispatcher=dispatcher,
        clock=lambda: clock[0],
    )
    first_payload = _payload()
    first, _ = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=first_payload,
        mutation=_mutation(first_payload, "bulk-active-0001"),
    )
    second_payload = _payload()
    second_payload.snapshot_id = UUID("00000000-0000-0000-0000-000000000507")

    with pytest.raises(ConflictError, match="active manifest import"):
        service.create(
            user_id=USER_ID,
            source_id=SOURCE_ID,
            payload=second_payload,
            mutation=_mutation(second_payload, "bulk-active-0002"),
        )

    clock[0] = NOW + timedelta(minutes=16)
    second, replayed = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=second_payload,
        mutation=_mutation(second_payload, "bulk-active-0002"),
    )
    assert replayed is False
    assert second.status == "AwaitingUpload"
    with transaction_scope(factory) as session:
        expired = session.scalar(
            select(ManifestImport).where(
                ManifestImport.public_id == str(first.import_id)
            )
        )
        assert expired.status == "Expired"
        assert expired.phase == "Complete"
        assert expired.active_marker is None


def test_checksum_mismatch_leaves_import_awaiting_upload(runtime):
    factory, store, dispatcher, service = runtime
    payload = _payload()
    created, _ = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload),
    )
    store.metadata = ManifestObjectMetadata(
        byte_size=1234,
        checksum_sha256_hex="ff" * 32,
        content_type="application/x-ndjson",
        content_encoding="gzip",
        version_id="version-mismatch",
    )

    with pytest.raises(BadRequestError, match="does not match"):
        service.complete(
            user_id=USER_ID,
            source_id=SOURCE_ID,
            import_id=created.import_id,
        )

    assert dispatcher.import_ids == []
    with transaction_scope(factory) as session:
        row = session.scalar(
            select(ManifestImport).where(
                ManifestImport.public_id == str(created.import_id)
            )
        )
        assert row.status == "AwaitingUpload"


def test_dispatch_failure_keeps_queued_import_durable(runtime):
    factory, store, _dispatcher, _service = runtime
    service = SqlAlchemyManifestImportService(
        factory,
        object_store=store,  # type: ignore[arg-type]
        dispatcher=FailingDispatcher(),
        clock=lambda: NOW,
    )
    payload = _payload()
    created, _ = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload),
    )
    store.metadata = ManifestObjectMetadata(
        byte_size=1234,
        checksum_sha256_hex="ab" * 32,
        content_type="application/x-ndjson",
        content_encoding="gzip",
        version_id="version-queued",
    )

    queued = service.complete(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        import_id=created.import_id,
    )

    assert queued.status == "Queued"
    with transaction_scope(factory) as session:
        row = session.scalar(
            select(ManifestImport).where(
                ManifestImport.public_id == str(created.import_id)
            )
        )
        assert row.status == "Queued"
        assert row.input_s3_version_id == "version-queued"


def test_manifest_import_lookup_is_cross_user_scoped(runtime):
    factory, _store, _dispatcher, service = runtime
    payload = _payload()
    created, _ = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload),
    )
    _seed_runtime(
        factory,
        user_id=OTHER_USER_ID,
        source_id=OTHER_SOURCE_ID,
        subject="bulk-api-other-user",
    )

    with pytest.raises(NotFoundError):
        service.get(
            user_id=OTHER_USER_ID,
            source_id=SOURCE_ID,
            import_id=created.import_id,
        )
    with pytest.raises(NotFoundError):
        service.get(
            user_id=OTHER_USER_ID,
            source_id=OTHER_SOURCE_ID,
            import_id=created.import_id,
        )


def test_pool_size_one_create_response_does_not_open_a_nested_session(tmp_path):
    engine = create_engine(
        f"sqlite+pysqlite:///{tmp_path / 'manifest-pool.sqlite3'}",
        connect_args={"check_same_thread": False},
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)
    _seed_runtime(factory)
    store = ObjectStore()
    service = SqlAlchemyManifestImportService(
        factory,
        object_store=store,  # type: ignore[arg-type]
        dispatcher=Dispatcher(),
        clock=lambda: NOW,
    )
    payload = _payload()

    created, replayed = service.create(
        user_id=USER_ID,
        source_id=SOURCE_ID,
        payload=payload,
        mutation=_mutation(payload, "bulk-pool-0001"),
    )

    assert replayed is False
    assert created.upload is not None


def test_manifest_dispatcher_publishes_only_the_durable_import_identity():
    class Sqs:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def send_message(self, **kwargs):
            self.calls.append(kwargs)
            return {"MessageId": "message-1"}

    client = Sqs()
    dispatcher = SqsManifestImportDispatcher(
        client=client, queue_url="manifest-import-queue"
    )

    dispatcher.dispatch(IMPORT_ID)

    assert client.calls == [
        {
            "QueueUrl": "manifest-import-queue",
            "MessageBody": json.dumps(
                {"jobType": "BulkManifest", "importId": str(IMPORT_ID)},
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
    ]
