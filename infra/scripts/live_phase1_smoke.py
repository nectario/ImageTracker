#!/usr/bin/env python
"""Destructive-but-self-cleaning Phase 1 acceptance smoke test.

Run this script from WSL so boto3 uses the AWS credentials already configured
there. The test creates only a disposable Cognito user and rows owned by that
user. It never invokes upload endpoints and never reads or writes ImageAsset.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import sys
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from sqlalchemy import delete, func, select, text  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402

from services.common.settings import AppSettings  # noqa: E402
from services.data.database import (  # noqa: E402
    DatabaseRuntime,
    SsmParameterResolver,
    build_database_runtime,
    transaction_scope,
)
from services.data.models import (  # noqa: E402
    Device,
    IdempotencyRecord,
    MediaAsset,
    MediaChange,
    MediaDescription,
    MediaLocation,
    MediaOccurrence,
    MediaSource,
    MediaTranscript,
    MediaTranscriptSegment,
    ProcessingJob,
    UserAccount,
)


class AcceptanceError(RuntimeError):
    """An acceptance assertion failed with a safe-to-print explanation."""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: Any
    headers: Mapping[str, str]


@dataclass(frozen=True)
class BucketSnapshot:
    object_count: int
    total_bytes: int
    fingerprint_sha256: str

    def as_json(self) -> dict[str, Any]:
        return {
            "objectCount": self.object_count,
            "totalBytes": self.total_bytes,
            "fingerprintSha256": self.fingerprint_sha256,
        }


@dataclass
class DisposableCognitoUser:
    email: str
    password: str
    username: str | None = None
    subject: str | None = None


SCOPED_MODELS = (
    Device,
    IdempotencyRecord,
    MediaSource,
    MediaAsset,
    MediaOccurrence,
    MediaLocation,
    MediaDescription,
    MediaTranscript,
    MediaTranscriptSegment,
    ProcessingJob,
    MediaChange,
)


def _is_wsl() -> bool:
    if os.environ.get("WSL_INTEROP") or os.environ.get("WSL_DISTRO_NAME"):
        return True
    try:
        release = platform.release().casefold()
        version = Path("/proc/version").read_text(encoding="utf-8").casefold()
    except OSError:
        return False
    return "microsoft" in release or "microsoft" in version


def _require_wsl() -> None:
    if not _is_wsl():
        raise AcceptanceError(
            "Run this smoke test inside WSL Ubuntu so it uses the configured WSL AWS credentials"
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceError(message)


def _aws_error_summary(error: ClientError) -> str:
    details = error.response.get("Error", {})
    code = str(details.get("Code") or "Unknown")
    operation = getattr(error, "operation_name", "AWS operation")
    return f"{operation} failed with AWS error {code}"


def _safe_error(error: BaseException) -> str:
    if isinstance(error, AcceptanceError):
        return str(error)
    if isinstance(error, ClientError):
        return _aws_error_summary(error)
    return f"{type(error).__name__}: the operation failed; no secret-bearing details were emitted"


def _stack_outputs(
    cloudformation: Any, *, stack_name: str
) -> tuple[str, dict[str, str]]:
    response = cloudformation.describe_stacks(StackName=stack_name)
    stacks = response.get("Stacks") or []
    _require(len(stacks) == 1, "CloudFormation did not return exactly one requested stack")
    stack = stacks[0]
    status = str(stack.get("StackStatus") or "")
    _require(
        status in {"CREATE_COMPLETE", "UPDATE_COMPLETE"},
        f"CloudFormation stack is not ready: {status or 'unknown'}",
    )
    outputs = {
        str(item["OutputKey"]): str(item["OutputValue"])
        for item in stack.get("Outputs") or []
        if item.get("OutputKey") and item.get("OutputValue")
    }
    required = {
        "ImageTrackerHttpApiUrl",
        "CognitoUserPoolId",
        "CognitoUserPoolClientId",
        "MediaBucketName",
        "ConfigurationParameterPrefix",
    }
    missing = sorted(required - outputs.keys())
    _require(not missing, f"CloudFormation outputs are missing: {', '.join(missing)}")
    return status, outputs


def _bucket_snapshot(s3: Any, *, bucket: str) -> BucketSnapshot:
    digest = hashlib.sha256()
    object_count = 0
    total_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents") or []:
            size = int(item.get("Size") or 0)
            last_modified = item.get("LastModified")
            record = [
                str(item.get("Key") or ""),
                str(item.get("ETag") or ""),
                size,
                last_modified.isoformat() if last_modified is not None else "",
            ]
            digest.update(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            digest.update(b"\n")
            object_count += 1
            total_bytes += size
    return BucketSnapshot(
        object_count=object_count,
        total_bytes=total_bytes,
        fingerprint_sha256=digest.hexdigest(),
    )


def _http_json(
    url: str,
    *,
    method: str = "GET",
    token: str | None = None,
    payload: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float,
) -> HttpResponse:
    encoded = None
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "ImageTracker-Phase1-Live-Smoke/1",
    }
    if payload is not None:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    if token is not None:
        request_headers["Authorization"] = f"Bearer {token}"
    if headers:
        request_headers.update(headers)
    request = Request(url, data=encoded, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw_body = response.read()
            status = int(response.status)
            response_headers = {
                key.casefold(): value for key, value in response.headers.items()
            }
    except HTTPError as error:
        raw_body = error.read()
        status = int(error.code)
        response_headers = {
            key.casefold(): value for key, value in error.headers.items()
        }
    except URLError as error:
        raise AcceptanceError(f"HTTP request failed before a response from {url}") from error

    if not raw_body:
        body: Any = None
    else:
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = None
    return HttpResponse(status=status, body=body, headers=response_headers)


def _expect_status(response: HttpResponse, expected: int, operation: str) -> None:
    if response.status == expected:
        return
    code = response.body.get("code") if isinstance(response.body, dict) else None
    suffix = f" ({code})" if isinstance(code, str) and code else ""
    raise AcceptanceError(
        f"{operation} returned HTTP {response.status}; expected {expected}{suffix}"
    )


def _expect_replay(response: HttpResponse, expected: bool, operation: str) -> None:
    actual = response.headers.get("idempotency-replayed", "").casefold()
    _require(
        actual == ("true" if expected else "false"),
        f"{operation} returned an invalid Idempotency-Replayed header",
    )


def _create_confirmed_user(
    cognito: Any, *, user_pool_id: str, user_state: DisposableCognitoUser
) -> None:
    # Set the cleanup handle before the network call. If the response is lost
    # after Cognito accepted the request, the finally block can still delete
    # the uniquely named disposable user by its email alias.
    user_state.username = user_state.email
    response = cognito.admin_create_user(
        UserPoolId=user_pool_id,
        Username=user_state.email,
        UserAttributes=[
            {"Name": "email", "Value": user_state.email},
            {"Name": "email_verified", "Value": "true"},
        ],
        MessageAction="SUPPRESS",
    )
    user = response.get("User") or {}
    user_state.username = str(user.get("Username") or user_state.email)
    attributes = {
        str(item.get("Name")): str(item.get("Value"))
        for item in user.get("Attributes") or []
    }
    if not attributes.get("sub"):
        fetched = cognito.admin_get_user(
            UserPoolId=user_pool_id, Username=user_state.username
        )
        attributes = {
            str(item.get("Name")): str(item.get("Value"))
            for item in fetched.get("UserAttributes") or []
        }
    user_state.subject = attributes.get("sub", "")
    _require(bool(user_state.subject), "The disposable Cognito user has no subject claim")
    cognito.admin_set_user_password(
        UserPoolId=user_pool_id,
        Username=user_state.username,
        Password=user_state.password,
        Permanent=True,
    )
    confirmed = cognito.admin_get_user(
        UserPoolId=user_pool_id, Username=user_state.username
    )
    _require(
        confirmed.get("UserStatus") == "CONFIRMED",
        "The disposable Cognito user was not confirmed",
    )


def _authenticate(
    cognito: Any, *, client_id: str, email: str, password: str
) -> str:
    response = cognito.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": email, "PASSWORD": password},
    )
    result = response.get("AuthenticationResult") or {}
    access_token = str(result.get("AccessToken") or "")
    _require(bool(access_token), "Cognito did not return an access token")
    return access_token


def _verify_database(runtime: DatabaseRuntime) -> None:
    with runtime.engine.connect() as connection:
        selected_database = connection.execute(
            text("SELECT DATABASE()")
        ).scalar_one()
    _require(
        selected_database == "ImageTracker",
        "The verified database runtime is not connected to ImageTracker",
    )


def _scoped_counts(session: Session, *, user_id: int) -> dict[str, int]:
    counts = {
        "UserAccount": int(
            session.scalar(
                select(func.count(UserAccount.id)).where(UserAccount.id == user_id)
            )
            or 0
        )
    }
    for model in SCOPED_MODELS:
        counts[model.__tablename__] = int(
            session.scalar(
                select(func.count(model.id)).where(model.user_id == user_id)
            )
            or 0
        )
    return counts


def _database_acceptance_state(
    session_factory: sessionmaker[Session], *, cognito_subject: str
) -> tuple[int, dict[str, Any]]:
    with transaction_scope(session_factory) as session:
        account_id = session.scalar(
            select(UserAccount.id).where(
                UserAccount.cognito_subject == cognito_subject
            )
        )
        _require(account_id is not None, "The authenticated user was not persisted in MySQL")
        counts = _scoped_counts(session, user_id=account_id)
        assets = list(
            session.execute(
                select(
                    MediaAsset.id,
                    MediaAsset.storage_state,
                    MediaAsset.s3_bucket,
                    MediaAsset.original_s3_object_key,
                    MediaAsset.original_s3_version_id,
                    MediaAsset.original_s3_etag,
                    MediaAsset.original_s3_checksum_algorithm,
                    MediaAsset.original_s3_checksum_type,
                    MediaAsset.original_s3_checksum_value,
                    MediaAsset.preview_s3_object_key,
                    MediaAsset.preview_s3_checksum_algorithm,
                    MediaAsset.preview_s3_checksum_type,
                    MediaAsset.preview_s3_checksum_value,
                ).where(MediaAsset.user_id == account_id)
            )
        )
        occurrences = list(
            session.execute(
                select(
                    MediaOccurrence.media_asset_id,
                    MediaOccurrence.source_item_id,
                    MediaOccurrence.local_locator,
                    MediaOccurrence.deletion_state,
                ).where(MediaOccurrence.user_id == account_id)
            )
        )

    _require(counts["UserAccount"] == 1, "MySQL does not contain one scoped account")
    _require(counts["Device"] == 1, "MySQL does not contain one scoped device")
    _require(counts["MediaSource"] == 1, "MySQL does not contain one scoped source")
    _require(
        counts["IdempotencyRecord"] == 4,
        "MySQL does not contain the four expected durable mutation records",
    )
    _require(counts["MediaAsset"] == 1, "MySQL exact deduplication did not produce one asset")
    _require(
        counts["MediaOccurrence"] == 2,
        "MySQL exact deduplication did not preserve two occurrences",
    )
    _require(len(assets) == 1, "The scoped asset inspection was not singular")
    asset = assets[0]
    _require(asset.storage_state == "LocalOnly", "The Local asset is not LocalOnly")
    _require(
        all(value is None for value in asset[2:]),
        "The Local asset unexpectedly contains an S3 locator",
    )
    _require(
        len({item.media_asset_id for item in occurrences}) == 1,
        "The two occurrences are not linked to the same asset",
    )
    _require(
        len({item.source_item_id for item in occurrences}) == 2,
        "The two occurrences do not retain distinct source paths",
    )
    _require(
        len({item.local_locator for item in occurrences}) == 2
        and all(item.local_locator for item in occurrences),
        "The two occurrences do not retain distinct Local locators",
    )
    _require(
        all(item.deletion_state == "Active" for item in occurrences),
        "A smoke-test occurrence is not active",
    )
    return account_id, {
        "rowCounts": counts,
        "localAssetState": asset.storage_state,
        "distinctOccurrencePaths": len(
            {item.source_item_id for item in occurrences}
        ),
    }


def _cleanup_database_user(
    session_factory: sessionmaker[Session],
    *,
    cognito_subject: str,
    known_user_id: int | None,
) -> dict[str, Any]:
    with transaction_scope(session_factory) as session:
        account_id = session.scalar(
            select(UserAccount.id).where(
                UserAccount.cognito_subject == cognito_subject
            )
        )
        target_user_id = account_id if account_id is not None else known_user_id
        if account_id is not None:
            # ProcessingJob owns optional source/asset foreign keys whose
            # MySQL delete action is restrictive. Remove only this disposable
            # user's jobs before relying on the account's cascade graph.
            session.execute(
                delete(ProcessingJob).where(ProcessingJob.user_id == account_id)
            )
            session.flush()
            session.execute(
                delete(UserAccount).where(UserAccount.id == account_id)
            )
            session.flush()
        after_counts = (
            _scoped_counts(session, user_id=target_user_id)
            if target_user_id is not None
            else {}
        )
    _require(
        all(value == 0 for value in after_counts.values()),
        "Disposable MySQL rows did not cascade-delete completely",
    )
    return {
        "accountExisted": account_id is not None,
        "rowCountsAfter": after_counts,
    }


def _delete_cognito_user(
    cognito: Any, *, user_pool_id: str, username: str
) -> dict[str, bool]:
    try:
        cognito.admin_delete_user(UserPoolId=user_pool_id, Username=username)
    except cognito.exceptions.UserNotFoundException:
        pass
    try:
        cognito.admin_get_user(UserPoolId=user_pool_id, Username=username)
    except cognito.exceptions.UserNotFoundException:
        return {"confirmedAbsent": True}
    raise AcceptanceError("The disposable Cognito user still exists after cleanup")


def _exercise_api(
    *,
    base_url: str,
    token: str,
    run_id: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    unauthenticated = _http_json(
        f"{base_url}/v1/me", timeout_seconds=timeout_seconds
    )
    _expect_status(unauthenticated, 401, "Unauthenticated /me")

    # Make the first account-bound calls concurrently so the deployed test also
    # exercises unique-subject bootstrap convergence across Lambda runtimes.
    with ThreadPoolExecutor(max_workers=2) as executor:
        bootstrap_requests = [
            executor.submit(
                _http_json,
                f"{base_url}/v1/me",
                token=token,
                timeout_seconds=timeout_seconds,
            )
            for _ in range(2)
        ]
        bootstrap_responses = [request.result() for request in bootstrap_requests]
    for index, response in enumerate(bootstrap_responses, start=1):
        _expect_status(response, 200, f"Concurrent authenticated /me #{index}")
        _require(
            isinstance(response.body, dict),
            f"Concurrent authenticated /me #{index} returned invalid JSON",
        )
    user_ids = {str(response.body.get("userId")) for response in bootstrap_responses}
    _require(len(user_ids) == 1, "Concurrent account bootstrap returned different users")
    me = bootstrap_responses[0]
    try:
        UUID(str(me.body.get("userId")))
    except (TypeError, ValueError) as error:
        raise AcceptanceError("Authenticated /me returned an invalid userId") from error

    installation_id = str(uuid4())
    device_key = f"smoke-device-{run_id}"
    device_payload = {
        "installationId": installation_id,
        "platform": "Windows",
        "displayName": "Phase 1 Live Smoke",
        "appVersion": "phase1-smoke",
        "osVersion": "WSL",
    }
    device = _http_json(
        f"{base_url}/v1/devices",
        method="POST",
        token=token,
        payload=device_payload,
        headers={"Idempotency-Key": device_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(device, 201, "Device registration")
    _expect_replay(device, False, "Device registration")
    _require(isinstance(device.body, dict), "Device registration returned invalid JSON")
    device_id = str(device.body.get("deviceId") or "")
    try:
        UUID(device_id)
    except ValueError as error:
        raise AcceptanceError("Device registration returned an invalid deviceId") from error

    device_replay = _http_json(
        f"{base_url}/v1/devices",
        method="POST",
        token=token,
        payload=device_payload,
        headers={"Idempotency-Key": device_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(device_replay, 201, "Device registration replay")
    _expect_replay(device_replay, True, "Device registration replay")
    _require(device_replay.body == device.body, "Device replay changed its response body")

    source_key = f"live-smoke-{run_id}"
    source_idempotency_key = f"smoke-source-{run_id}"
    source_payload = {
        "deviceId": device_id,
        "sourceKey": source_key,
        "sourceType": "Folder",
        "displayName": "Phase 1 Duplicate Acceptance",
        "storageMode": "Local",
        "permissionState": "Full",
        "syncSettings": {
            "automaticSync": False,
            "networkPolicy": "WiFiOnly",
            "requireChargingForHistoricalUpload": True,
        },
    }
    source = _http_json(
        f"{base_url}/v1/sources",
        method="POST",
        token=token,
        payload=source_payload,
        headers={"Idempotency-Key": source_idempotency_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(source, 201, "Local source creation")
    _expect_replay(source, False, "Local source creation")
    _require(isinstance(source.body, dict), "Source creation returned invalid JSON")
    source_id = str(source.body.get("sourceId") or "")
    try:
        UUID(source_id)
    except ValueError as error:
        raise AcceptanceError("Source creation returned an invalid sourceId") from error
    _require(source.body.get("storageMode") == "Local", "Source was not created in Local mode")

    source_replay = _http_json(
        f"{base_url}/v1/sources",
        method="POST",
        token=token,
        payload=source_payload,
        headers={"Idempotency-Key": source_idempotency_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(source_replay, 201, "Local source creation replay")
    _expect_replay(source_replay, True, "Local source creation replay")
    _require(source_replay.body == source.body, "Source replay changed its response body")

    identical_bytes = b"ImageTracker Phase 1 exact duplicate live smoke\n"
    content_hash = hashlib.sha256(identical_bytes).hexdigest()
    common_entry = {
        "operation": "Upsert",
        "sourceRevision": f"sha256:{content_hash}",
        "contentSha256": content_hash,
        "mediaType": "Photo",
        "mimeType": "image/jpeg",
        "byteSize": len(identical_bytes),
        "capturedAtLocal": "2026-08-27T12:00:00",
        "capturedAtUtc": "2026-08-27T16:00:00Z",
        "timeZoneId": "America/New_York",
        "utcOffsetMinutes": -240,
        "provenance": [
            {
                "field": "capturedAtUtc",
                "source": "Device",
                "confidence": 1.0,
                "processorVersion": "live-smoke-v1",
            }
        ],
    }
    manifest_payload = {
        "snapshotId": str(uuid4()),
        "kind": "Full",
        "permissionState": "Full",
        "deletionDetectionReliable": True,
        "clientCursor": f"live-smoke:{run_id}:1",
        "entries": [
            {
                **common_entry,
                "sourceItemId": "photos/duplicate-a.jpg",
                "fileName": "duplicate-a.jpg",
                "localLocator": "/tmp/imagetracker-live-smoke/a/duplicate.jpg",
            },
            {
                **common_entry,
                "sourceItemId": "photos/duplicate-b.jpg",
                "fileName": "duplicate-b.jpg",
                "localLocator": "/tmp/imagetracker-live-smoke/b/duplicate.jpg",
            },
        ],
    }
    manifest_key = f"smoke-manifest-{run_id}"
    manifest_url = f"{base_url}/v1/sources/{source_id}/manifest"
    manifest = _http_json(
        manifest_url,
        method="POST",
        token=token,
        payload=manifest_payload,
        headers={"Idempotency-Key": manifest_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(manifest, 200, "Duplicate manifest")
    _expect_replay(manifest, False, "Duplicate manifest")
    _require(isinstance(manifest.body, dict), "Manifest returned invalid JSON")
    counts = manifest.body.get("counts") or {}
    _require(
        counts.get("created") == 1 and counts.get("duplicatesLinked") == 1,
        "First duplicate manifest did not create one occurrence and link one duplicate",
    )
    results = manifest.body.get("results") or []
    _require(len(results) == 2, "Manifest did not return two entry results")
    asset_ids = {item.get("mediaAssetId") for item in results}
    occurrence_ids = {item.get("occurrenceId") for item in results}
    _require(len(asset_ids) == 1 and None not in asset_ids, "Manifest did not reuse one asset")
    _require(
        len(occurrence_ids) == 2 and None not in occurrence_ids,
        "Manifest did not retain two distinct occurrences",
    )
    _require(
        all(item.get("uploadRequired") is False for item in results),
        "A Local manifest unexpectedly requested an upload",
    )

    manifest_replay = _http_json(
        manifest_url,
        method="POST",
        token=token,
        payload=manifest_payload,
        headers={"Idempotency-Key": manifest_key},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(manifest_replay, 200, "Manifest replay")
    _expect_replay(manifest_replay, True, "Manifest replay")
    _require(manifest_replay.body == manifest.body, "Manifest replay changed its response body")

    manifest_rerun = _http_json(
        manifest_url,
        method="POST",
        token=token,
        payload=manifest_payload,
        headers={"Idempotency-Key": f"smoke-rerun-{run_id}"},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(manifest_rerun, 200, "Manifest rerun")
    _expect_replay(manifest_rerun, False, "Manifest rerun")
    rerun_counts = (
        manifest_rerun.body.get("counts")
        if isinstance(manifest_rerun.body, dict)
        else {}
    ) or {}
    _require(
        rerun_counts.get("unchanged") == 2,
        "A new-key rerun did not classify both occurrences as unchanged",
    )
    rerun_results = manifest_rerun.body.get("results") or []
    _require(
        {item.get("mediaAssetId") for item in rerun_results} == asset_ids
        and {item.get("occurrenceId") for item in rerun_results} == occurrence_ids,
        "A new-key rerun created different asset or occurrence identities",
    )

    media = _http_json(
        f"{base_url}/v1/media?sourceId={source_id}&limit=10",
        token=token,
        headers={"X-ImageTracker-Device-Id": device_id},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(media, 200, "Media listing")
    media_items = media.body.get("items") if isinstance(media.body, dict) else None
    _require(isinstance(media_items, list) and len(media_items) == 1, "Media listing is not deduplicated")
    media_asset_id = str(media_items[0].get("mediaAssetId") or "")
    _require(media_asset_id in asset_ids, "Media listing returned the wrong asset")
    _require(media_items[0].get("storageMode") == "Local", "Listed asset is not Local")
    _require(
        media_items[0].get("availability") == "LocalOnThisDevice",
        "Listed asset is not locally available on the registering device",
    )

    detail = _http_json(
        f"{base_url}/v1/media/{media_asset_id}",
        token=token,
        headers={"X-ImageTracker-Device-Id": device_id},
        timeout_seconds=timeout_seconds,
    )
    _expect_status(detail, 200, "Media detail")
    detail_occurrences = (
        detail.body.get("occurrences") if isinstance(detail.body, dict) else None
    )
    _require(
        isinstance(detail_occurrences, list) and len(detail_occurrences) == 2,
        "Media detail did not preserve both occurrences",
    )

    return {
        "unauthenticatedStatus": unauthenticated.status,
        "authenticatedStatus": me.status,
        "concurrentBootstrapStatuses": [
            response.status for response in bootstrap_responses
        ],
        "assetCount": len(media_items),
        "occurrenceCount": len(detail_occurrences),
        "firstManifestCounts": counts,
        "rerunManifestCounts": rerun_counts,
        "replayVerified": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the self-cleaning deployed Phase 1 Local acceptance smoke test."
    )
    parser.add_argument("--stack", default="image-tracker-prod")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument(
        "--profile",
        help="Optional AWS profile from the WSL credential/config files.",
    )
    parser.add_argument(
        "--http-timeout-seconds",
        type=float,
        default=45.0,
        help="Per-request HTTP timeout, including Lambda cold starts (default: 45).",
    )
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    _require_wsl()
    _require(args.http_timeout_seconds > 0, "HTTP timeout must be positive")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cloudformation = session.client("cloudformation", region_name=args.region)
    cognito = session.client("cognito-idp", region_name=args.region)
    s3 = session.client("s3", region_name=args.region)
    status, outputs = _stack_outputs(cloudformation, stack_name=args.stack)
    base_url = outputs["ImageTrackerHttpApiUrl"].rstrip("/")
    bucket = outputs["MediaBucketName"]
    db_parameter = outputs["ConfigurationParameterPrefix"].rstrip("/") + "/mysql"

    settings = AppSettings(
        stage="prod",
        aws_region=args.region,
        mysql_database="ImageTracker",
        media_bucket=bucket,
        db_secret_parameter=db_parameter,
    )
    resolver = SsmParameterResolver(
        region_name=args.region, client=session.client("ssm", region_name=args.region)
    )
    runtime = build_database_runtime(settings, resolver=resolver)
    _verify_database(runtime)

    run_id = uuid4().hex
    user_state = DisposableCognitoUser(
        email=f"imagetracker-smoke-{run_id}@example.com",
        password=f"It-{secrets.token_urlsafe(30)}-Aa1!",
    )
    user_pool_id = outputs["CognitoUserPoolId"]
    client_id = outputs["CognitoUserPoolClientId"]
    known_user_id: int | None = None
    baseline = _bucket_snapshot(s3, bucket=bucket)
    report: dict[str, Any] = {
        "status": "passed",
        "stack": {"name": args.stack, "region": args.region, "status": status},
    }
    primary_error: BaseException | None = None
    cleanup_errors: list[BaseException] = []

    try:
        _create_confirmed_user(
            cognito,
            user_pool_id=user_pool_id,
            user_state=user_state,
        )
        token = _authenticate(
            cognito,
            client_id=client_id,
            email=user_state.email,
            password=user_state.password,
        )
        report["api"] = _exercise_api(
            base_url=base_url,
            token=token,
            run_id=run_id,
            timeout_seconds=args.http_timeout_seconds,
        )
        _require(user_state.subject is not None, "Disposable Cognito subject is unavailable")
        known_user_id, report["database"] = _database_acceptance_state(
            runtime.session_factory, cognito_subject=user_state.subject
        )
    except BaseException as error:
        primary_error = error
    finally:
        if user_state.subject is not None:
            try:
                report.setdefault("cleanup", {})["database"] = _cleanup_database_user(
                    runtime.session_factory,
                    cognito_subject=user_state.subject,
                    known_user_id=known_user_id,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        if user_state.username is not None:
            try:
                report.setdefault("cleanup", {})["cognito"] = _delete_cognito_user(
                    cognito,
                    user_pool_id=user_pool_id,
                    username=user_state.username,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            final_snapshot = _bucket_snapshot(s3, bucket=bucket)
            unchanged = final_snapshot == baseline
            report["s3"] = {
                "before": baseline.as_json(),
                "after": final_snapshot.as_json(),
                "unchanged": unchanged,
            }
            if not unchanged:
                cleanup_errors.append(
                    AcceptanceError("The media bucket inventory changed during Local acceptance")
                )
        except BaseException as error:
            cleanup_errors.append(error)
        runtime.engine.dispose()

    if primary_error is not None or cleanup_errors:
        report["status"] = "failed"
        if primary_error is not None:
            report["error"] = _safe_error(primary_error)
        if cleanup_errors:
            report["cleanupErrors"] = [_safe_error(item) for item in cleanup_errors]
        raise AcceptanceRunFailed(report)
    return report


class AcceptanceRunFailed(Exception):
    def __init__(self, report: Mapping[str, Any]) -> None:
        super().__init__("Phase 1 live acceptance failed")
        self.report = dict(report)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = run(args)
    except AcceptanceRunFailed as error:
        print(json.dumps(error.report, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1
    except BaseException as error:
        report = {"status": "failed", "error": _safe_error(error)}
        print(json.dumps(report, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
