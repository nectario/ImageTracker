from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from typing import Any
from uuid import UUID

import httpx
from mangum import Mangum
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from services.api.app import create_app
from services.api.domain_adapter import DomainServiceAdapter
from services.api.models import (
    ChangePage,
    CurrentUser,
    Device,
    DevicePage,
    DeviceStatus,
    ManifestCounts,
    ManifestEntryResult,
    ManifestOutcome,
    ManifestResponse,
    MediaAssetDetail,
    MediaAssetPage,
    MediaAvailability,
    MediaOccurrence,
    MediaSearchHit,
    MediaSearchPage,
    MediaSource,
    MediaSourcePage,
    PageInfo,
    PermissionState,
    ProcessingJob,
    ProcessingJobPage,
    ProcessingJobType,
    SourceStatus,
    SourceType,
    SyncSettings,
    TemporalMetadata,
)
from services.api.service import AuthIdentity, MutationResult
from services.common.enums import (
    MediaType,
    ProcessingJobStatus,
    SourcePlatform,
    StorageMode,
    StorageState,
    UserFacingState,
)
from services.common.settings import AppSettings
from services.data.models import Base
from services.domain.service import Phase1DomainService


NOW = datetime(2026, 8, 27, 16, 30, tzinfo=timezone.utc)
USER_ID = UUID("10000000-0000-4000-8000-000000000001")
DEVICE_ID = UUID("20000000-0000-4000-8000-000000000001")
INSTALLATION_ID = UUID("20000000-0000-4000-8000-000000000002")
SOURCE_ID = UUID("30000000-0000-4000-8000-000000000001")
ASSET_ID = UUID("40000000-0000-4000-8000-000000000001")
OCCURRENCE_ID = UUID("50000000-0000-4000-8000-000000000001")
JOB_ID = UUID("60000000-0000-4000-8000-000000000001")
REQUEST_ID = "d27598e0-2607-45a7-a6c0-f12bb44a2cf0"


def _user() -> CurrentUser:
    return CurrentUser(
        user_id=USER_ID,
        email="owner@example.com",
        display_name="Owner",
        created_at_utc=NOW,
    )


def _device() -> Device:
    return Device(
        device_id=DEVICE_ID,
        installation_id=INSTALLATION_ID,
        platform=SourcePlatform.WINDOWS_CLI,
        display_name="Archive PC",
        app_version="0.3.0",
        os_version="Windows 11",
        status=DeviceStatus.ACTIVE,
        registered_at_utc=NOW,
        last_seen_at_utc=NOW,
    )


def _source() -> MediaSource:
    return MediaSource(
        source_id=SOURCE_ID,
        device_id=DEVICE_ID,
        source_key="archive-main",
        source_type=SourceType.FOLDER,
        display_name="Photo Archive",
        storage_mode=StorageMode.LOCAL,
        permission_state=PermissionState.NOT_APPLICABLE,
        status=SourceStatus.ACTIVE,
        sync_settings=SyncSettings(
            automatic_sync=True,
            network_policy="WiFiOnly",
            require_charging_for_historical_upload=True,
        ),
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


def _media_summary() -> dict[str, Any]:
    return {
        "mediaAssetId": str(ASSET_ID),
        "contentSha256": "a" * 64,
        "mediaType": "Photo",
        "mimeType": "image/jpeg",
        "byteSize": 1234,
        "displayFileName": "IMG_0001.JPG",
        "storageMode": "Local",
        "storageState": "LocalOnly",
        "availability": "LocalOnThisDevice",
        "state": "Ready",
        "temporal": TemporalMetadata(captured_at_utc=NOW),
        "isTrashed": False,
        "createdAtUtc": NOW,
        "updatedAtUtc": NOW,
    }


def _job() -> ProcessingJob:
    return ProcessingJob(
        job_id=JOB_ID,
        media_asset_id=ASSET_ID,
        job_type=ProcessingJobType.METADATA,
        status=ProcessingJobStatus.FAILED,
        state=UserFacingState.NEEDS_ATTENTION,
        attempt_count=3,
        can_retry=True,
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


class FakePhase1Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.last_identity: AuthIdentity | None = None

    async def current_user(self, identity: AuthIdentity) -> CurrentUser:
        self.last_identity = identity
        return _user()

    async def list_devices(self, *args: Any) -> DevicePage:
        self.calls.append(("list_devices", args))
        return DevicePage(items=[_device()], page=PageInfo(has_more=False))

    async def register_device(self, *args: Any) -> MutationResult[Device]:
        self.calls.append(("register_device", args))
        return MutationResult(_device(), 201, replayed=True)

    async def list_sources(self, *args: Any) -> MediaSourcePage:
        self.calls.append(("list_sources", args))
        return MediaSourcePage(items=[_source()], page=PageInfo(has_more=False))

    async def create_source(self, *args: Any) -> MutationResult[MediaSource]:
        self.calls.append(("create_source", args))
        return MutationResult(_source(), 201)

    async def get_source(self, *args: Any) -> MediaSource:
        self.calls.append(("get_source", args))
        return _source()

    async def update_source(self, *args: Any) -> MutationResult[MediaSource]:
        self.calls.append(("update_source", args))
        return MutationResult(_source(), 200)

    async def remove_source(self, *args: Any) -> MutationResult[None]:
        self.calls.append(("remove_source", args))
        return MutationResult(None, 204, replayed=True)

    async def submit_manifest(self, *args: Any) -> MutationResult[ManifestResponse]:
        self.calls.append(("submit_manifest", args))
        return MutationResult(
            ManifestResponse(
                source_id=SOURCE_ID,
                accepted_at_utc=NOW,
                counts=ManifestCounts(
                    created=1,
                    updated=0,
                    duplicates_linked=0,
                    deleted=0,
                    ignored_deletions=0,
                    unchanged=0,
                    rejected=0,
                ),
                results=[
                    ManifestEntryResult(
                        source_item_id="item-1",
                        outcome=ManifestOutcome.CREATED_OCCURRENCE,
                        occurrence_id=OCCURRENCE_ID,
                        media_asset_id=ASSET_ID,
                        upload_required=False,
                    )
                ],
            ),
            200,
        )

    async def list_changes(self, *args: Any) -> ChangePage:
        self.calls.append(("list_changes", args))
        return ChangePage(items=[], page=PageInfo(has_more=False))

    async def list_media(self, *args: Any) -> MediaAssetPage:
        self.calls.append(("list_media", args))
        return MediaAssetPage(
            items=[_media_summary()], page=PageInfo(has_more=False)
        )

    async def search_media(self, *args: Any) -> MediaSearchPage:
        self.calls.append(("search_media", args))
        return MediaSearchPage(
            items=[
                MediaSearchHit(
                    asset=_media_summary(),
                    matched_field="FileName",
                    highlight="IMG_0001.JPG",
                )
            ],
            page=PageInfo(has_more=False),
        )

    async def get_media_asset(self, *args: Any) -> MediaAssetDetail:
        self.calls.append(("get_media_asset", args))
        return MediaAssetDetail(
            **_media_summary(),
            occurrences=[
                MediaOccurrence(
                    occurrence_id=OCCURRENCE_ID,
                    source_id=SOURCE_ID,
                    source_item_id="item-1",
                    source_revision="1",
                    exact_file_name="IMG_0001.JPG",
                    local_locator="C:/Photos/IMG_0001.JPG",
                    first_seen_at_utc=NOW,
                    last_seen_at_utc=NOW,
                    is_deleted=False,
                )
            ],
            provenance=[],
        )

    async def list_jobs(self, *args: Any) -> ProcessingJobPage:
        self.calls.append(("list_jobs", args))
        return ProcessingJobPage(items=[_job()], page=PageInfo(has_more=False))

    async def get_job(self, *args: Any) -> ProcessingJob:
        self.calls.append(("get_job", args))
        return _job()

    async def retry_job(self, *args: Any) -> MutationResult[ProcessingJob]:
        self.calls.append(("retry_job", args))
        return MutationResult(_job(), 202, replayed=True)


IDENTITY = AuthIdentity(subject="cognito-subject", email="owner@example.com")


def _app(service: FakePhase1Service | None = None, identity: AuthIdentity = IDENTITY):
    return create_app(
        AppSettings(stage="test"),
        phase1_service=service or FakePhase1Service(),
        test_identity=identity,
    )


def _request(
    app: Any,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json: Any = None,
) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, headers=headers, json=json)

    return asyncio.run(send())


def _headers(*, device: bool = False, idempotency: bool = False) -> dict[str, str]:
    values = {"X-Request-Id": REQUEST_ID}
    if device:
        values["X-ImageTracker-Device-Id"] = str(DEVICE_ID)
    if idempotency:
        values["Idempotency-Key"] = "request-0001"
    return values


def test_cognito_claims_are_read_from_mangum_scope_not_raw_bearer_header():
    service = FakePhase1Service()
    base_app = create_app(AppSettings(stage="test"), phase1_service=service)
    event = {
        "version": "2.0",
        "routeKey": "GET /v1/me",
        "rawPath": "/v1/me",
        "rawQueryString": "",
        "headers": {
            "host": "example.execute-api.us-east-2.amazonaws.com",
            "authorization": "Bearer ignored-unverified-value",
        },
        "requestContext": {
            "accountId": "test",
            "apiId": "test",
            "domainName": "example.execute-api.us-east-2.amazonaws.com",
            "domainPrefix": "example",
            "http": {
                "method": "GET",
                "path": "/v1/me",
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": "test",
            },
            "requestId": "gateway-request",
            "routeKey": "GET /v1/me",
            "stage": "test",
            "time": "",
            "timeEpoch": 0,
            "authorizer": {
                "jwt": {
                    "claims": {
                        "sub": "verified-subject",
                        "cognito:groups": ["ImageTrackerAdmin"],
                    },
                    "scopes": [],
                }
            },
        },
        "isBase64Encoded": False,
    }
    event_loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(event_loop)
        lambda_response = Mangum(base_app, lifespan="off")(event, object())
    finally:
        asyncio.set_event_loop(None)
        event_loop.close()

    assert lambda_response["statusCode"] == 200
    assert json.loads(lambda_response["body"])["userId"] == str(USER_ID)
    assert service.last_identity == AuthIdentity(
        subject="verified-subject",
        email=None,
        groups=frozenset({"ImageTrackerAdmin"}),
        is_admin=True,
    )

    unauthenticated = _request(
        create_app(AppSettings(stage="test"), phase1_service=FakePhase1Service()),
        "GET",
        "/v1/me",
        headers={"Authorization": "Bearer not-trusted-directly"},
    )
    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["content-type"].startswith(
        "application/problem+json"
    )


def test_injected_identity_is_impossible_in_production():
    with pytest.raises(ValueError, match="only in local or test"):
        create_app(AppSettings(stage="prod"), test_identity=IDENTITY)

    with pytest.raises(ValueError, match="only in local or test"):
        create_app(AppSettings(stage="staging"), test_identity=IDENTITY)


def test_problem_details_and_request_id_are_consistent():
    response = _request(
        create_app(AppSettings(stage="test"), phase1_service=FakePhase1Service()),
        "GET",
        "/v1/me",
        headers={"X-Request-Id": REQUEST_ID},
    )

    assert response.status_code == 401
    assert response.headers["x-request-id"] == REQUEST_ID
    assert response.json() == {
        "type": "https://imagetracker.app/problems/authentication-required",
        "title": "Unauthorized",
        "status": 401,
        "code": "AUTHENTICATION_REQUIRED",
        "detail": "A verified Cognito identity is required",
        "instance": "/v1/me",
        "traceId": REQUEST_ID,
        "fieldErrors": [],
    }


def test_device_registration_requires_and_forwards_idempotency():
    service = FakePhase1Service()
    payload = {
        "installationId": str(INSTALLATION_ID),
        "platform": "WindowsCLI",
        "displayName": "Archive PC",
        "appVersion": "0.3.0",
        "osVersion": "Windows 11",
    }
    missing = _request(_app(service), "POST", "/v1/devices", json=payload)
    assert missing.status_code == 400
    assert missing.json()["code"] == "VALIDATION_FAILED"
    assert missing.json()["fieldErrors"][0]["field"] == "header.Idempotency-Key"

    response = _request(
        _app(service),
        "POST",
        "/v1/devices",
        headers=_headers(idempotency=True),
        json=payload,
    )
    assert response.status_code == 201
    assert response.headers["idempotency-replayed"] == "true"
    assert response.json()["installationId"] == str(INSTALLATION_ID)
    _, args = service.calls[-1]
    mutation = args[2]
    assert mutation.idempotency_key == "request-0001"
    assert mutation.request_id == UUID(REQUEST_ID)
    assert mutation.operation == "POST"
    assert mutation.target == "/v1/devices"
    assert len(mutation.request_hash) == 64


def test_phase1_reads_are_reachable_and_paginated_through_the_service():
    service = FakePhase1Service()

    devices = _request(_app(service), "GET", "/v1/devices?limit=25")
    sources = _request(_app(service), "GET", "/v1/sources?limit=25")
    changes = _request(
        _app(service),
        "GET",
        "/v1/changes?limit=25",
        headers=_headers(device=True),
    )

    assert devices.status_code == 200
    assert devices.json()["items"][0]["deviceId"] == str(DEVICE_ID)
    assert sources.status_code == 200
    assert sources.json()["items"][0]["sourceId"] == str(SOURCE_ID)
    assert changes.status_code == 200
    assert changes.json() == {"items": [], "page": {"nextCursor": None, "hasMore": False}}
    assert ("list_devices", (USER_ID, None, 25)) in service.calls
    assert ("list_sources", (USER_ID, None, 25)) in service.calls
    assert ("list_changes", (USER_ID, DEVICE_ID, None, 25)) in service.calls


def test_source_crud_uses_contract_models_and_camel_case_responses():
    service = FakePhase1Service()
    create_payload = {
        "deviceId": str(DEVICE_ID),
        "sourceKey": "archive-main",
        "sourceType": "Folder",
        "displayName": "Photo Archive",
    }
    created = _request(
        _app(service),
        "POST",
        "/v1/sources",
        headers=_headers(idempotency=True),
        json=create_payload,
    )
    fetched = _request(_app(service), "GET", f"/v1/sources/{SOURCE_ID}")
    updated = _request(
        _app(service),
        "PATCH",
        f"/v1/sources/{SOURCE_ID}",
        headers=_headers(idempotency=True),
        json={"displayName": "Renamed"},
    )
    removed = _request(
        _app(service),
        "DELETE",
        f"/v1/sources/{SOURCE_ID}",
        headers=_headers(idempotency=True),
    )

    assert created.status_code == 201
    assert created.json()["storageMode"] == "Local"
    assert fetched.status_code == 200
    assert updated.status_code == 200
    assert removed.status_code == 204
    assert removed.headers["idempotency-replayed"] == "true"

    empty_patch = _request(
        _app(service),
        "PATCH",
        f"/v1/sources/{SOURCE_ID}",
        headers=_headers(idempotency=True),
        json={},
    )
    assert empty_patch.status_code == 422

    explicit_null_settings = _request(
        _app(service),
        "POST",
        "/v1/sources",
        headers=_headers(idempotency=True),
        json={**create_payload, "syncSettings": None},
    )
    assert explicit_null_settings.status_code == 422
    assert "syncSettings cannot be null" in explicit_null_settings.text


def test_create_source_allows_reactivation_status() -> None:
    class ReactivatingService(FakePhase1Service):
        async def create_source(self, *args: Any) -> MutationResult[MediaSource]:
            self.calls.append(("create_source", args))
            return MutationResult(_source(), 200)

    response = _request(
        _app(ReactivatingService()),
        "POST",
        "/v1/sources",
        headers=_headers(idempotency=True),
        json={
            "deviceId": str(DEVICE_ID),
            "sourceKey": "archive-main",
            "sourceType": "Folder",
            "displayName": "Photo Archive",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "Active"


def test_manifest_forwards_deletions_for_the_domain_safety_decision_and_valid_upserts():
    service = FakePhase1Service()
    unsafe = {
        "kind": "Incremental",
        "permissionState": "Limited",
        "deletionDetectionReliable": False,
        "entries": [
            {
                "operation": "Deleted",
                "sourceItemId": "item-1",
                "sourceRevision": "2",
            }
        ],
    }
    ignored_by_fake_boundary = _request(
        _app(service),
        "POST",
        f"/v1/sources/{SOURCE_ID}/manifest",
        headers=_headers(idempotency=True),
        json=unsafe,
    )
    assert ignored_by_fake_boundary.status_code == 200
    _, unsafe_args = service.calls[-1]
    assert unsafe_args[2].deletion_detection_reliable is False

    accepted = dict(unsafe)
    accepted.update(
        permissionState="Full",
        deletionDetectionReliable=True,
        entries=[
            {
                "operation": "Upsert",
                "sourceItemId": "item-1",
                "sourceRevision": "1",
                "fileName": "IMG_0001.JPG",
                "localLocator": "C:/Photos/IMG_0001.JPG",
                "contentSha256": "a" * 64,
                "mediaType": "Photo",
                "mimeType": "image/jpeg",
                "byteSize": 1234,
            }
        ],
    )
    response = _request(
        _app(service),
        "POST",
        f"/v1/sources/{SOURCE_ID}/manifest",
        headers=_headers(idempotency=True),
        json=accepted,
    )
    assert response.status_code == 200
    assert response.json()["counts"]["created"] == 1
    assert response.json()["results"][0]["uploadRequired"] is False


def test_manifest_requires_aware_and_consistent_capture_utc():
    service = FakePhase1Service()
    entry = {
        "operation": "Upsert",
        "sourceItemId": "time-test",
        "sourceRevision": "1",
        "fileName": "IMG_TIME.JPG",
        "contentSha256": "c" * 64,
        "mediaType": "Photo",
        "mimeType": "image/jpeg",
        "byteSize": 1234,
        "capturedAtLocal": "2026-08-27T12:30:00",
        "utcOffsetMinutes": -240,
    }

    def submit(captured_at_utc: str) -> httpx.Response:
        return _request(
            _app(service),
            "POST",
            f"/v1/sources/{SOURCE_ID}/manifest",
            headers=_headers(idempotency=True),
            json={
                "kind": "Incremental",
                "permissionState": "Full",
                "deletionDetectionReliable": True,
                "entries": [{**entry, "capturedAtUtc": captured_at_utc}],
            },
        )

    missing_offset = submit("2026-08-27T16:30:00")
    assert missing_offset.status_code == 422
    assert "must include Z or a time-zone offset" in missing_offset.text

    inconsistent = submit("2026-08-27T16:31:00Z")
    assert inconsistent.status_code == 422
    assert "are inconsistent" in inconsistent.text

    within_tolerance = submit("2026-08-27T16:30:01Z")
    assert within_tolerance.status_code == 200


def test_local_visibility_endpoints_require_registered_device_header_shape():
    for path in (
        "/v1/changes",
        "/v1/media",
        "/v1/media/search?q=IMG",
        f"/v1/media/{ASSET_ID}",
    ):
        response = _request(_app(), "GET", path)
        assert response.status_code == 400, path
        assert response.json()["code"] == "VALIDATION_FAILED"

    response = _request(
        _app(), "GET", "/v1/media?limit=201", headers=_headers(device=True)
    )
    assert response.status_code == 400


def test_media_filters_search_and_local_locator_are_forwarded():
    service = FakePhase1Service()
    listed = _request(
        _app(service),
        "GET",
        "/v1/media?mediaType=Photo&storageMode=Local&trashState=All&sort=UpdatedAtDesc",
        headers=_headers(device=True),
    )
    searched = _request(
        _app(service),
        "GET",
        "/v1/media/search?q=IMG&hasLocation=false",
        headers=_headers(device=True),
    )
    detailed = _request(
        _app(service),
        "GET",
        f"/v1/media/{ASSET_ID}",
        headers=_headers(device=True),
    )

    assert listed.status_code == 200
    assert listed.json()["items"][0]["displayFileName"] == "IMG_0001.JPG"
    assert searched.status_code == 200
    assert searched.json()["items"][0]["matchedField"] == "FileName"
    assert detailed.status_code == 200
    assert detailed.json()["occurrences"][0]["localLocator"].endswith(
        "IMG_0001.JPG"
    )
    _, list_args = next(call for call in service.calls if call[0] == "list_media")
    assert list_args[1] == DEVICE_ID
    assert list_args[2].sort == "UpdatedAtDesc"

    invalid_window = _request(
        _app(service),
        "GET",
        "/v1/media?capturedAfterUtc=2026-08-28T00:00:00Z&capturedBeforeUtc=2026-08-27T00:00:00Z",
        headers=_headers(device=True),
    )
    assert invalid_window.status_code == 400
    assert invalid_window.json()["code"] == "INVALID_CAPTURE_WINDOW"


def test_jobs_support_filters_get_and_idempotent_manual_retry():
    service = FakePhase1Service()
    listed = _request(
        _app(service),
        "GET",
        f"/v1/jobs?status=Failed&jobType=Metadata&mediaAssetId={ASSET_ID}",
    )
    fetched = _request(_app(service), "GET", f"/v1/jobs/{JOB_ID}")
    retried = _request(
        _app(service),
        "POST",
        f"/v1/jobs/{JOB_ID}/retry",
        headers=_headers(idempotency=True),
    )

    assert listed.status_code == 200
    assert fetched.status_code == 200
    assert retried.status_code == 202
    assert retried.headers["idempotency-replayed"] == "true"


def test_admin_placeholders_are_restricted_and_never_expose_secrets():
    forbidden = _request(_app(), "GET", "/v1/admin/health")
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == "ADMIN_REQUIRED"

    admin = AuthIdentity(
        subject="admin-subject",
        email="admin@example.com",
        groups=frozenset({"ImageTrackerAdmin"}),
        is_admin=True,
    )
    health = _request(_app(identity=admin), "GET", "/v1/admin/health")
    audit = _request(_app(identity=admin), "GET", "/v1/admin/audit")

    assert health.status_code == 200
    assert health.json()["status"] == "Degraded"
    assert audit.status_code == 200
    assert audit.json()["readOnly"] is True
    combined = health.text + audit.text
    assert "password" not in combined.casefold()
    assert "dsn" not in combined.casefold()


def test_phase3_upload_and_media_lifecycle_routes_are_not_accidentally_enabled():
    for method, path in (
        ("POST", "/v1/uploads/plan"),
        ("GET", f"/v1/uploads/{ASSET_ID}"),
        ("POST", f"/v1/media/{ASSET_ID}/trash"),
        ("POST", f"/v1/media/{ASSET_ID}/restore"),
    ):
        response = _request(_app(), method, path)
        assert response.status_code == 404, (method, path, response.text)
        assert response.headers["content-type"].startswith(
            "application/problem+json"
        )


def test_unwired_data_service_fails_closed_without_exposing_configuration():
    app = create_app(AppSettings(stage="test"), test_identity=IDENTITY)

    response = _request(app, "GET", "/v1/me")

    assert response.status_code == 503
    assert response.json()["code"] == "DATA_SERVICE_NOT_CONFIGURED"
    assert "password" not in response.text.casefold()


def test_http_adapter_runs_a_local_ingestion_slice_with_durable_replay():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        class_=Session,
        autoflush=False,
        expire_on_commit=False,
    )
    app = create_app(
        AppSettings(stage="test"),
        phase1_service=DomainServiceAdapter(Phase1DomainService(factory)),
        test_identity=AuthIdentity(subject="subject-without-email"),
    )

    me = _request(app, "GET", "/v1/me")
    assert me.status_code == 200
    assert me.json()["email"] is None

    registration = {
        "installationId": str(INSTALLATION_ID),
        "platform": "WindowsCLI",
        "displayName": "Archive PC",
        "appVersion": "0.3.0",
        "osVersion": "Windows 11",
    }
    device_headers = {"Idempotency-Key": "register-real-1"}
    first = _request(
        app, "POST", "/v1/devices", headers=device_headers, json=registration
    )
    replay = _request(
        app, "POST", "/v1/devices", headers=device_headers, json=registration
    )
    assert first.status_code == 201
    assert first.headers["idempotency-replayed"] == "false"
    assert replay.status_code == 201
    assert replay.headers["idempotency-replayed"] == "true"
    device_id = first.json()["deviceId"]

    changed_registration = dict(registration, displayName="Different PC")
    conflict = _request(
        app,
        "POST",
        "/v1/devices",
        headers=device_headers,
        json=changed_registration,
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "IDEMPOTENCY_KEY_REUSED"

    source = _request(
        app,
        "POST",
        "/v1/sources",
        headers={"Idempotency-Key": "source-real-1"},
        json={
            "deviceId": device_id,
            "sourceKey": "archive-main",
            "sourceType": "Folder",
            "displayName": "Photo Archive",
        },
    )
    assert source.status_code == 201
    source_id = source.json()["sourceId"]

    manifest = _request(
        app,
        "POST",
        f"/v1/sources/{source_id}/manifest",
        headers={"Idempotency-Key": "manifest-real-1"},
        json={
            "kind": "Incremental",
            "permissionState": "NotApplicable",
            "deletionDetectionReliable": True,
            "entries": [
                {
                    "operation": "Upsert",
                    "sourceItemId": "photo-1",
                    "sourceRevision": "r1",
                    "fileName": "Original Name.JPG",
                    "localLocator": "C:/Photos/Original Name.JPG",
                    "contentSha256": "b" * 64,
                    "mediaType": "Photo",
                    "mimeType": "image/jpeg",
                    "byteSize": 4321,
                    "capturedAtLocal": "2026-08-20T12:30:00",
                },
                {
                    "operation": "Upsert",
                    "sourceItemId": "photo-2",
                    "sourceRevision": "r1",
                    "fileName": "Exact Duplicate.JPG",
                    "localLocator": "C:/Photos/Copies/Exact Duplicate.JPG",
                    "contentSha256": "b" * 64,
                    "mediaType": "Photo",
                    "mimeType": "image/jpeg",
                    "byteSize": 4321,
                    "capturedAtLocal": "2026-08-20T12:30:00",
                },
            ],
        },
    )
    assert manifest.status_code == 200
    assert manifest.json()["counts"]["created"] == 1
    assert manifest.json()["counts"]["duplicatesLinked"] == 1

    timeline = _request(
        app,
        "GET",
        "/v1/media",
        headers={"X-ImageTracker-Device-Id": device_id},
    )
    assert timeline.status_code == 200
    assert len(timeline.json()["items"]) == 1
    assert timeline.json()["items"][0]["displayFileName"] in {
        "Original Name.JPG",
        "Exact Duplicate.JPG",
    }
    assert timeline.json()["items"][0]["availability"] == "LocalOnThisDevice"

    detail = _request(
        app,
        "GET",
        f"/v1/media/{timeline.json()['items'][0]['mediaAssetId']}",
        headers={"X-ImageTracker-Device-Id": device_id},
    )
    assert detail.status_code == 200
    assert len(detail.json()["occurrences"]) == 2
    assert {item["exactFileName"] for item in detail.json()["occurrences"]} == {
        "Original Name.JPG",
        "Exact Duplicate.JPG",
    }

    bad_cursor = _request(app, "GET", "/v1/devices?cursor=not-valid")
    assert bad_cursor.status_code == 400
    assert bad_cursor.json()["code"] == "INVALID_CURSOR"
