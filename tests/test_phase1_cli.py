from __future__ import annotations

import os
import stat
import uuid
import base64
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import httpx
import pytest
import typer
from typer.testing import CliRunner

import cli.imagetracker_cli.app as cli_app_module
from cli.imagetracker_cli.api_client import ApiClient, ApiError, ApiProblem
from cli.imagetracker_cli.app import ExitCode, app, _register_device
from cli.imagetracker_cli.auth import (
    CognitoAuth,
    FileTokenBackend,
    TokenSet,
    TokenStore,
    id_token_subject,
)
from cli.imagetracker_cli.config import ConfigStore, config_from_stack
from cli.imagetracker_cli.media import MediaScanner, ScanResult, source_item_id, stream_sha256
from cli.imagetracker_cli.legacy import load_legacy_db_config
from cli.imagetracker_cli.state import LocalState
from cli.imagetracker_cli.sync import MANIFEST_BATCH_SIZE, SyncEngine


SOURCE_ID = "c132f11e-976c-42f0-b2d8-61e88f1757d2"


def fake_id_token(subject: str) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"sub": subject}).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def test_interrupt_message_does_not_claim_in_progress_scan_was_saved(capsys):
    message = (
        "Stopped. The current discovery/stat pass restarts; "
        "completed cache batches remain saved."
    )
    with pytest.raises(typer.Exit) as raised:
        with cli_app_module.command_errors(interrupt_message=message):
            raise KeyboardInterrupt

    assert raised.value.exit_code == 0
    assert message == " ".join(capsys.readouterr().out.split())


def test_cli_uses_bright_accessible_status_palette():
    styles = cli_app_module.CLI_THEME.styles

    assert "bright_cyan" in str(styles["progress"])
    assert "bright_green" in str(styles["success"])
    assert "bright_yellow" in str(styles["warning"])
    assert "bright_red" in str(styles["error"])
    assert cli_app_module._state_text("Succeeded").startswith("[success]")
    assert cli_app_module._state_text("Preparing").startswith("[progress]")
    assert cli_app_module._state_text("Failed").startswith("[error]")


class FakeCloudFormation:
    def describe_stacks(self, **kwargs: Any) -> Mapping[str, Any]:
        assert kwargs == {"StackName": "image-tracker-prod"}
        return {
            "Stacks": [
                {
                    "Outputs": [
                        {"OutputKey": "ImageTrackerHttpApiUrl", "OutputValue": "https://api.example/v1/"},
                        {"OutputKey": "CognitoUserPoolId", "OutputValue": "us-east-2_pool"},
                        {"OutputKey": "CognitoUserPoolClientId", "OutputValue": "client-123"},
                    ]
                }
            ]
        }


class MemoryTokens:
    def __init__(self, tokens: TokenSet | None = None):
        self.tokens = tokens

    def load(self) -> TokenSet | None:
        return self.tokens

    def save(self, tokens: TokenSet) -> None:
        self.tokens = tokens

    def delete(self) -> None:
        self.tokens = None


class FakeCognito:
    def __init__(self):
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def initiate_auth(self, **kwargs: Any) -> Mapping[str, Any]:
        self.calls.append(("initiate_auth", kwargs))
        if kwargs["AuthFlow"] == "USER_PASSWORD_AUTH":
            return {
                "AuthenticationResult": {
                    "AccessToken": "access-1",
                    "IdToken": fake_id_token("account-one"),
                    "RefreshToken": "refresh-1",
                    "ExpiresIn": 3600,
                }
            }
        return {
            "AuthenticationResult": {
                "AccessToken": "access-2",
                "IdToken": fake_id_token("account-one"),
                "ExpiresIn": 3600,
            }
        }

    def global_sign_out(self, **_kwargs: Any) -> Mapping[str, Any]:
        raise RuntimeError("offline")


class FakeMetadata:
    def __init__(self):
        self.calls = 0

    def extract(self, path: Path, **_kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        return {
            "capturedAtLocal": "2026-08-27T12:00:00",
            "provenance": [{"field": "capturedAt", "source": "FileMtime"}],
        }


class FakeManifestApi:
    def __init__(self, *, upload_required: bool = False, fail_first: bool = False):
        self.upload_required = upload_required
        self.fail_first = fail_first
        self.calls: list[tuple[str, Mapping[str, Any], str]] = []

    def submit_manifest(self, source_id: str, payload: Mapping[str, Any], *, key: str):
        self.calls.append((source_id, payload, key))
        if self.fail_first and len(self.calls) == 1:
            raise ApiError(ApiProblem(0, "Offline", "connection lost"))
        results = [
            {
                "sourceItemId": entry["sourceItemId"],
                "outcome": (
                    "DeletedOccurrence"
                    if entry["operation"] == "Deleted"
                    else "CreatedOccurrence"
                ),
                "uploadRequired": self.upload_required,
            }
            for entry in payload["entries"]
        ]
        return {
            "counts": {
                "created": len(results),
                "updated": 0,
                "duplicatesLinked": max(0, len(results) - 1),
                "deleted": 0,
                "ignoredDeletions": 0,
                "unchanged": 0,
                "rejected": 0,
            },
            "results": results,
        }


class MixedManifestApi:
    def __init__(self):
        self.calls: list[tuple[str, Mapping[str, Any], str]] = []

    def submit_manifest(self, source_id: str, payload: Mapping[str, Any], *, key: str):
        self.calls.append((source_id, payload, key))
        results = []
        for index, entry in enumerate(payload["entries"]):
            if index == 0:
                results.append(
                    {
                        "sourceItemId": entry["sourceItemId"],
                        "outcome": "CreatedOccurrence",
                        "uploadRequired": False,
                    }
                )
            else:
                results.append(
                    {
                        "sourceItemId": entry["sourceItemId"],
                        "outcome": "Rejected",
                        "uploadRequired": False,
                        "errorCode": "UNSUPPORTED_MEDIA",
                        "errorMessage": "The test service rejected this entry.",
                    }
                )
        return {
            "counts": {
                "created": 1,
                "updated": 0,
                "duplicatesLinked": 0,
                "deleted": 0,
                "ignoredDeletions": 0,
                "unchanged": 0,
                "rejected": max(0, len(results) - 1),
            },
            "results": results,
        }


class IgnoredDeletionApi:
    def submit_manifest(self, source_id: str, payload: Mapping[str, Any], *, key: str):
        del source_id, key
        return {
            "counts": {
                "created": 0,
                "updated": 0,
                "duplicatesLinked": 0,
                "deleted": 0,
                "ignoredDeletions": len(payload["entries"]),
                "unchanged": 0,
                "rejected": 0,
            },
            "results": [
                {
                    "sourceItemId": entry["sourceItemId"],
                    "outcome": "IgnoredDeletion",
                    "uploadRequired": False,
                }
                for entry in payload["entries"]
            ],
        }


class RequestErrorApi:
    def __init__(self, status: int, code: str | None):
        self.status = status
        self.code = code
        self.calls = 0

    def submit_manifest(self, source_id: str, payload: Mapping[str, Any], *, key: str):
        del source_id, payload, key
        self.calls += 1
        raise ApiError(
            ApiProblem(
                self.status,
                "Manifest rejected\nby service",
                "The request cannot be accepted.\nCorrect the local metadata and retry.",
                code=self.code,
                request_id="request-123",
            )
        )


def source_payload(root: Path) -> dict[str, Any]:
    return {
        "sourceId": SOURCE_ID,
        "sourceKey": "source-key",
        "displayName": "Camera Roll",
        "storageMode": "Local",
    }


def test_configuration_reads_only_public_stack_outputs_and_saves_private_file(tmp_path: Path):
    config = config_from_stack(
        FakeCloudFormation(),
        stack_name="image-tracker-prod",
        region="us-east-2",
        profile="deeptrading",
    )
    assert config.api_url == "https://api.example/v1"
    assert config.cognito_client_id == "client-123"

    store = ConfigStore(tmp_path / "config")
    store.save(config)
    assert store.load().cognito_user_pool_id == "us-east-2_pool"
    if os.name != "nt":
        assert stat.S_IMODE(store.path.stat().st_mode) == 0o600


def test_private_file_token_backend_enforces_0600(tmp_path: Path):
    path = tmp_path / "credentials.json"
    backend = FileTokenBackend(path)
    tokens = TokenSet("access", "id", "refresh", "2099-01-01T00:00:00Z", "me@example.com")
    backend.save(tokens)
    assert backend.load() == tokens
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_token_subject_is_backward_compatible_and_used_only_for_local_namespacing():
    token = TokenSet(
        "access",
        fake_id_token("cognito-subject-123"),
        "refresh",
        "2099-01-01T00:00:00Z",
        "me@example.com",
    )
    assert token.subject is None
    assert token.local_subject == "cognito-subject-123"
    assert id_token_subject("not-a-jwt") is None


def test_account_state_paths_are_hashed_deterministic_and_isolated(tmp_path: Path):
    store = ConfigStore(tmp_path / "config")
    first_path = store.state_path_for_subject("account-one")
    repeated_path = store.state_path_for_subject("account-one")
    second_path = store.state_path_for_subject("account-two")
    assert first_path == repeated_path
    assert first_path != second_path
    assert "account-one" not in str(first_path)

    root = tmp_path / "same-library"
    root.mkdir()
    first = LocalState(first_path)
    second = LocalState(second_path)
    first.bind_source(source_payload(root), root)
    second.bind_source({**source_payload(root), "sourceId": str(uuid.uuid4())}, root)
    assert len(first.list_bindings()) == len(second.list_bindings()) == 1


def test_legacy_database_config_refuses_another_deeptrading_database():
    with pytest.raises(ValueError, match="restricted to the ImageTracker database"):
        load_legacy_db_config(
            environ={
                "MYSQL_HOST": "shared-rds.example",
                "MYSQL_USER": "reader",
                "MYSQL_DATABASE": "DeepTradingAI",
            }
        )


def test_legacy_checkpoint_is_local_and_resumable(tmp_path: Path):
    state = LocalState(tmp_path / "state.sqlite3")
    assert state.legacy_checkpoint() == (0, 0)
    state.save_legacy_checkpoint(742, 700)
    assert state.legacy_checkpoint() == (742, 700)


def test_cognito_login_and_refresh_preserve_refresh_token():
    client = FakeCognito()
    store = MemoryTokens()
    auth = CognitoAuth(client, "native-client", store)  # type: ignore[arg-type]
    original = auth.login("Me@Example.com", "password")
    refreshed = auth.refresh(original)
    assert original.email == "Me@Example.com"
    assert original.subject == "account-one"
    assert refreshed.access_token == "access-2"
    assert refreshed.refresh_token == "refresh-1"
    assert client.calls[0][1]["AuthFlow"] == "USER_PASSWORD_AUTH"
    assert client.calls[1][1]["AuthFlow"] == "REFRESH_TOKEN_AUTH"


def test_logout_always_removes_local_session_when_cognito_is_offline():
    store = MemoryTokens(
        TokenSet("access", "id", "refresh", "2099-01-01T00:00:00Z", "me@example.com")
    )
    auth = CognitoAuth(FakeCognito(), "native-client", store)  # type: ignore[arg-type]
    auth.logout()
    assert store.load() is None


def test_api_client_refreshes_once_after_401():
    calls: list[str] = []
    tokens = MemoryTokens(
        TokenSet(
            "old-access",
            "old-id",
            "refresh",
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )
    )

    def refresh(current: TokenSet) -> TokenSet:
        updated = TokenSet("new-access", "new-id", current.refresh_token, current.expires_at_utc)
        tokens.save(updated)
        return updated

    tokens.current_tokens = lambda refresh_if_needed=True: tokens.load()  # type: ignore[attr-defined]
    tokens.refresh = refresh  # type: ignore[attr-defined]

    def handler(request: httpx.Request) -> httpx.Response:
        authorization = request.headers["Authorization"]
        calls.append(authorization)
        if authorization == "Bearer old-access":
            return httpx.Response(401, json={"detail": "expired"})
        return httpx.Response(200, json={"items": [], "page": {"nextCursor": None}})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    api = ApiClient("https://example.test", tokens, http_client=http)  # type: ignore[arg-type]
    assert api.list_sources() == []
    assert calls == ["Bearer old-access", "Bearer new-access"]


def test_media_api_calls_include_registered_device_header():
    tokens = MemoryTokens(
        TokenSet(
            "access",
            fake_id_token("account"),
            "refresh",
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )
    )
    tokens.current_tokens = lambda refresh_if_needed=True: tokens.load()  # type: ignore[attr-defined]
    tokens.refresh = lambda current: current  # type: ignore[attr-defined]
    seen: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.method,
                request.url.path,
                request.headers.get("X-ImageTracker-Device-Id"),
            )
        )
        if request.url.path.endswith("/search") or request.url.path == "/v1/media":
            return httpx.Response(200, json={"items": [], "page": {"nextCursor": None}})
        return httpx.Response(200, json={"mediaAssetId": "asset"})

    api = ApiClient(
        "https://example.test",
        tokens,  # type: ignore[arg-type]
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    device_id = "66bf29e1-8c76-42aa-bd9b-d7487f914276"
    api.list_media(device_id)
    api.search_media("beach", device_id)
    api.get_media("e54ec40a-9c30-43eb-aec4-ed7b43754342", device_id)
    assert [item[2] for item in seen] == [device_id, device_id, device_id]


def test_scanner_streams_hashes_preserves_names_and_uses_cache(tmp_path: Path):
    root = tmp_path / "library"
    nested = root / "Mixed Case"
    nested.mkdir(parents=True)
    first = nested / "IMG_Exact Name.JPG"
    second = nested / "copy.jpeg"
    first.write_bytes(b"identical bytes")
    second.write_bytes(b"identical bytes")
    (nested / "notes.txt").write_text("ignore", encoding="utf-8")
    state = LocalState(tmp_path / "state.sqlite3")
    metadata = FakeMetadata()
    scanner = MediaScanner(state, metadata_extractor=metadata)  # type: ignore[arg-type]

    initial = scanner.scan(SOURCE_ID, root)
    repeated = scanner.scan(SOURCE_ID, root)

    assert initial.complete_read is True
    assert initial.hashed == 2
    assert repeated.cached == 2
    assert metadata.calls == 2
    assert {entry["fileName"] for entry in initial.entries} == {"IMG_Exact Name.JPG", "copy.jpeg"}
    assert len({entry["contentSha256"] for entry in initial.entries}) == 1
    assert all(Path(entry["localLocator"]).is_absolute() for entry in initial.entries)
    assert stream_sha256(first) == initial.entries[0]["contentSha256"]


def test_force_rehash_bypasses_unchanged_file_cache(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    photo = root / "one.jpg"
    photo.write_bytes(b"photo")
    state = LocalState(tmp_path / "state.sqlite3")
    hash_calls = 0

    def counted_hash(path: Path) -> str:
        nonlocal hash_calls
        hash_calls += 1
        return stream_sha256(path)

    scanner = MediaScanner(
        state,
        metadata_extractor=FakeMetadata(),  # type: ignore[arg-type]
        hash_file=counted_hash,
    )
    scanner.scan(SOURCE_ID, root)
    cached = scanner.scan(SOURCE_ID, root)
    forced = scanner.scan(SOURCE_ID, root, force_rehash=True)
    assert cached.cached == 1
    assert forced.hashed == 1
    assert hash_calls == 2


def test_scanner_hashes_and_caches_with_multiple_workers(tmp_path: Path):
    root = tmp_path / "parallel-library"
    root.mkdir()
    for index in range(8):
        (root / f"photo-{index:02d}.jpg").write_bytes(
            (f"parallel-photo-{index}".encode("ascii")) * 128
        )

    state = LocalState(tmp_path / "parallel-state.sqlite3")
    barrier = threading.Barrier(4)
    names: set[str] = set()
    names_lock = threading.Lock()

    def synchronized_hash(path: Path) -> str:
        with names_lock:
            names.add(threading.current_thread().name)
        barrier.wait(timeout=5)
        return stream_sha256(path)

    scanner = MediaScanner(
        state,
        metadata_extractor=FakeMetadata(),  # type: ignore[arg-type]
        hash_file=synchronized_hash,
        workers=4,
    )

    first = scanner.scan(SOURCE_ID, root)
    repeated = scanner.scan(SOURCE_ID, root)

    assert first.hashed == 8
    assert first.worker_count == 4
    assert first.files_per_second > 0
    assert len(names) == 4
    assert len(state.cached_files(SOURCE_ID)) == 8
    assert repeated.hashed == 0
    assert repeated.cached == 8


def test_fast_add_discovers_without_reading_contents_then_normal_scan_enriches(
    tmp_path: Path,
):
    root = tmp_path / "instant-library"
    root.mkdir()
    first = root / "one.jpg"
    second = root / "two.jpg"
    first.write_bytes(b"first-photo")
    second.write_bytes(b"second-photo")
    state = LocalState(tmp_path / "instant-state.sqlite3")
    hash_calls = 0

    def counted_hash(path: Path) -> str:
        nonlocal hash_calls
        hash_calls += 1
        return stream_sha256(path)

    scanner = MediaScanner(
        state,
        metadata_extractor=FakeMetadata(),  # type: ignore[arg-type]
        hash_file=counted_hash,
        workers=4,
    )

    instant = scanner.scan(SOURCE_ID, root, fast_add=True)
    assert instant.scanned == 2
    assert instant.hashed == 0
    assert instant.pending_hash == 2
    assert all(entry["contentSha256"] is None for entry in instant.entries)
    assert state.cached_files(SOURCE_ID) == {}

    enriched = scanner.scan(SOURCE_ID, root)

    assert len(state.cached_files(SOURCE_ID)) == 2
    assert enriched.hashed == 2
    assert enriched.pending_hash == 0
    assert all(entry["contentSha256"] for entry in enriched.entries)
    assert hash_calls == 2


def test_empty_media_placeholder_is_skipped_without_failing_library_scan(
    tmp_path: Path,
):
    root = tmp_path / "library-with-placeholder"
    root.mkdir()
    (root / "empty.jpg").touch()
    (root / "real.jpg").write_bytes(b"photo")
    state = LocalState(tmp_path / "placeholder-state.sqlite3")

    result = MediaScanner(
        state,
        metadata_extractor=FakeMetadata(),  # type: ignore[arg-type]
        workers=4,
    ).scan(SOURCE_ID, root, fast_add=True)

    assert result.scanned == 2
    assert result.failed == 0
    assert result.skipped == 1
    assert len(result.entries) == 1


def test_one_file_import_can_suspend_and_restore_pending_manifest_batches(
    tmp_path: Path,
):
    state = LocalState(tmp_path / "suspend-state.sqlite3")
    scan_id = state.begin_scan(SOURCE_ID, tmp_path)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        (
            {"kind": "Incremental", "entries": []},
            {"kind": "Incremental", "entries": []},
        ),
    )

    batch_ids = state.suspend_pending_batches(SOURCE_ID)

    assert len(batch_ids) == 2
    assert state.pending_count() == 0
    assert len(state.list_outbox(state="Discarded")) == 2

    state.restore_suspended_batches(batch_ids)

    assert state.pending_count() == 2
    assert state.list_outbox(state="Discarded") == []


def test_local_sync_links_exact_duplicates_and_is_idempotent(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"same")
    (root / "two.jpg").write_bytes(b"same")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    api = FakeManifestApi()
    scanner = MediaScanner(state, metadata_extractor=FakeMetadata())  # type: ignore[arg-type]
    engine = SyncEngine(api, state, scanner)  # type: ignore[arg-type]

    first = engine.sync(binding)
    second = engine.sync(binding)

    assert first.upserts == 2
    assert first.duplicates_linked == 1
    assert first.upload_required == 0
    assert len(api.calls) == 1
    assert second.unchanged == 2
    assert second.batches_sent == 0
    assert state.pending_count() == 0


def test_remove_then_readd_resubmits_occurrences_but_reuses_hash_cache(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"same")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    scanner = MediaScanner(state, metadata_extractor=FakeMetadata())  # type: ignore[arg-type]
    initial_api = FakeManifestApi()
    SyncEngine(initial_api, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert len(state.known_occurrences(SOURCE_ID)) == 1

    state.remove_binding(SOURCE_ID)
    assert state.known_occurrences(SOURCE_ID) == {}
    assert state.pending_count() == state.failed_count() == 0
    assert state.recent_scans() == []

    readded = state.bind_source(source_payload(root), root)
    readd_api = FakeManifestApi()
    summary = SyncEngine(readd_api, state, scanner).sync(readded)  # type: ignore[arg-type]
    assert summary.cached == 1
    assert summary.upserts == 1
    assert len(readd_api.calls) == 1
    assert len(state.known_occurrences(SOURCE_ID)) == 1


def test_manifest_batches_never_exceed_500_entries():
    entries = [
        {
            "operation": "Deleted",
            "sourceItemId": f"path:{index}",
            "sourceRevision": "a" * 64,
        }
        for index in range(1001)
    ]
    batches = SyncEngine._manifest_payloads(scan_id=str(uuid.uuid4()), entries=entries, complete_read=True)
    assert [len(item["entries"]) for item in batches] == [MANIFEST_BATCH_SIZE, 500, 1]
    assert all(item["deletionDetectionReliable"] for item in batches)


def test_interrupted_manifest_resumes_with_same_idempotency_key(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"data")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    scanner = MediaScanner(state, metadata_extractor=FakeMetadata())  # type: ignore[arg-type]
    failing_api = FakeManifestApi(fail_first=True)

    with pytest.raises(ApiError, match="connection lost"):
        SyncEngine(failing_api, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert state.pending_count() == 1
    original_key = failing_api.calls[0][2]

    recovered_api = FakeManifestApi()
    summary = SyncEngine(recovered_api, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert recovered_api.calls[0][2] == original_key
    assert summary.resumed_batches == 1
    assert state.pending_count() == 0


def test_reliable_ignored_deletion_converges_after_lost_acknowledgement(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    item_id = source_item_id("already-deleted.jpg")
    prior = {
        "kind": "Full",
        "permissionState": "NotApplicable",
        "deletionDetectionReliable": True,
        "entries": [
            {
                "operation": "Upsert",
                "sourceItemId": item_id,
                "sourceRevision": "a" * 64,
                "fileName": "already-deleted.jpg",
                "localLocator": str(root / "already-deleted.jpg"),
                "mediaType": "Photo",
                "mimeType": "image/jpeg",
                "byteSize": 10,
            }
        ],
    }
    scan_id = state.begin_scan(SOURCE_ID, root)
    state.queue_batches(SOURCE_ID, scan_id, [prior])
    state.acknowledge_batch(state.pending_batches(SOURCE_ID)[0])
    assert item_id in state.known_occurrences(SOURCE_ID)

    summary = SyncEngine(
        IgnoredDeletionApi(),  # type: ignore[arg-type]
        state,
        MediaScanner(state, metadata_extractor=FakeMetadata()),  # type: ignore[arg-type]
    ).sync(binding)
    assert summary.deletions == 1
    assert summary.failed == 0
    assert item_id not in state.known_occurrences(SOURCE_ID)
    assert state.failed_count() == 0


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "INVALID_MANIFEST"),
        (403, "FORBIDDEN"),
        (404, "SOURCE_NOT_FOUND"),
        (422, "VALIDATION_FAILED"),
        (409, "IDEMPOTENCY_CONFLICT"),
    ],
)
def test_permanent_request_errors_quarantine_whole_batch(
    tmp_path: Path, status: int, code: str
):
    root = tmp_path / f"library-{status}"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"data")
    state = LocalState(tmp_path / f"state-{status}.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    api = RequestErrorApi(status, code)
    summary = SyncEngine(
        api,  # type: ignore[arg-type]
        state,
        MediaScanner(state, metadata_extractor=FakeMetadata()),  # type: ignore[arg-type]
    ).sync(binding)
    assert summary.failed == 1
    assert summary.quarantined_batches == 1
    assert state.pending_count() == 0
    assert state.failed_count() == 1
    failure = state.list_outbox(state="Failed")[0].failure
    assert failure["reason"] == "RequestRejected"
    assert failure["requestError"]["status"] == status
    assert failure["requestError"]["code"] == code
    assert "\n" not in failure["requestError"]["detail"]
    assert failure["entries"][0]["sourceRevision"]


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (0, "NETWORK_ERROR"),
        (401, "UNAUTHORIZED"),
        (408, "TIMEOUT"),
        (429, "RATE_LIMITED"),
        (500, "SERVER_ERROR"),
        (503, "UNAVAILABLE"),
        (409, "REQUEST_IN_PROGRESS"),
    ],
)
def test_transient_request_errors_remain_pending(
    tmp_path: Path, status: int, code: str
):
    root = tmp_path / f"library-{status}-{code}"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"data")
    state = LocalState(tmp_path / f"state-{status}-{code}.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    api = RequestErrorApi(status, code)
    with pytest.raises(ApiError):
        SyncEngine(
            api,  # type: ignore[arg-type]
            state,
            MediaScanner(state, metadata_extractor=FakeMetadata()),  # type: ignore[arg-type]
        ).sync(binding)
    assert state.pending_count() == 1
    assert state.failed_count() == 0


def test_cli_renders_permanent_request_quarantine_then_exits_partial(
    tmp_path: Path, monkeypatch
):
    root = tmp_path / "library"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"data")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    runtime = SimpleNamespace(state=state, api=RequestErrorApi(422, "VALIDATION_FAILED"))
    monkeypatch.setattr(cli_app_module, "_runtime", lambda: runtime)
    result = CliRunner().invoke(app, ["sync", binding.source_id, "--json"])
    assert result.exit_code == 5
    payload = json.loads(result.stdout.splitlines()[0])
    assert payload["quarantined_batches"] == 1
    assert payload["failed"] == 1
    assert state.pending_count() == 0
    assert state.failed_count() == 1


def test_rejected_entries_are_quarantined_without_blocking_new_sync(
    tmp_path: Path, monkeypatch
):
    root = tmp_path / "library"
    root.mkdir()
    (root / "accepted.jpg").write_bytes(b"accepted")
    (root / "rejected.jpg").write_bytes(b"rejected")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    scanner = MediaScanner(state, metadata_extractor=FakeMetadata())  # type: ignore[arg-type]
    mixed = MixedManifestApi()

    first = SyncEngine(mixed, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert first.quarantined_batches == 1
    assert first.rejected_entries == 1
    assert state.pending_count() == 0
    assert state.failed_count() == 1
    assert len(state.known_occurrences(SOURCE_ID)) == 1
    failed = state.list_outbox(state="Failed")
    assert failed[0].failure["entries"][0]["errorCode"] == "UNSUPPORTED_MEDIA"
    original_key = mixed.calls[0][2]

    second = SyncEngine(mixed, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert len(mixed.calls) == 1
    assert second.unchanged == 1
    assert second.quarantined_entries == 1

    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state),
    )
    listed = CliRunner().invoke(app, ["outbox", "list", "--json"])
    assert listed.exit_code == 0
    assert json.loads(listed.stdout)[0]["state"] == "Failed"
    discarded = CliRunner().invoke(
        app, ["outbox", "discard", failed[0].batch_id, "--yes"]
    )
    assert discarded.exit_code == 0
    assert state.failed_count() == 0

    accepting = FakeManifestApi()
    third = SyncEngine(accepting, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert third.upserts == 1
    assert accepting.calls[0][2] != original_key
    assert len(state.known_occurrences(SOURCE_ID)) == 2


def test_cli_sync_renders_quarantine_then_exits_partial(tmp_path: Path, monkeypatch):
    root = tmp_path / "library"
    root.mkdir()
    (root / "accepted.jpg").write_bytes(b"accepted")
    (root / "rejected.jpg").write_bytes(b"rejected")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    runtime = SimpleNamespace(state=state, api=MixedManifestApi())
    monkeypatch.setattr(cli_app_module, "_runtime", lambda: runtime)

    result = CliRunner().invoke(app, ["sync", binding.source_id, "--json"])
    assert result.exit_code == 5
    payload = json.loads(result.stdout.splitlines()[0])
    assert payload["quarantined_entries"] == 1
    assert "outbox list" in result.stderr
    repeated = CliRunner().invoke(app, ["sync", binding.source_id, "--json"])
    assert repeated.exit_code == 5
    repeated_payload = json.loads(repeated.stdout.splitlines()[0])
    assert repeated_payload["quarantined_entries"] == 1
    assert len(runtime.api.calls) == 1


def test_device_registration_key_changes_only_when_payload_changes(tmp_path: Path, monkeypatch):
    state = LocalState(tmp_path / "state.sqlite3")

    class RegistrationApi:
        def __init__(self):
            self.keys: list[str] = []

        def register_device(self, payload, *, key):
            self.keys.append(key)
            return {"deviceId": "ce7cc79c-0713-4d71-a226-9bba28e62cc6"}

    api = RegistrationApi()
    runtime = SimpleNamespace(state=state, api=api)
    monkeypatch.setattr(cli_app_module, "package_version", lambda: "1.0.0")
    _register_device(runtime)  # type: ignore[arg-type]
    _register_device(runtime)  # type: ignore[arg-type]
    monkeypatch.setattr(cli_app_module, "package_version", lambda: "1.1.0")
    _register_device(runtime)  # type: ignore[arg-type]
    assert len(api.keys) == 2
    assert api.keys[1] != api.keys[0]


def test_legacy_preview_checkpoint_can_be_saved_locally_and_resumed(
    tmp_path: Path, monkeypatch
):
    state = LocalState(tmp_path / "state.sqlite3")

    class PreviewInspector:
        def __init__(self):
            self.after_ids: list[int] = []

        def migration_preview(self, *, checkpoint_legacy_id: int, limit: int):
            self.after_ids.append(checkpoint_legacy_id)
            return {
                "dryRun": True,
                "checkpointLegacyId": checkpoint_legacy_id,
                "batchLimit": limit,
                "batchRows": 10,
                "firstLegacyId": checkpoint_legacy_id + 1,
                "lastLegacyId": checkpoint_legacy_id + 10,
                "nextCheckpointLegacyId": checkpoint_legacy_id + 10,
                "hasMore": True,
                "alreadyMapped": 0,
                "unmapped": 10,
                "missingLocalPath": 0,
                "writesPerformed": 0,
            }

    inspector = PreviewInspector()
    monkeypatch.setattr(cli_app_module, "_legacy_runtime", lambda: (inspector, state))
    runner = CliRunner()
    first = runner.invoke(
        app,
        ["legacy", "migrate", "--dry-run", "--limit", "10", "--save-checkpoint", "--json"],
    )
    assert first.exit_code == 0
    assert json.loads(first.stdout)["checkpointScope"] == "LocalPreviewOnly"
    assert state.legacy_checkpoint() == (10, 10)
    second = runner.invoke(app, ["legacy", "migrate", "--dry-run", "--limit", "10", "--json"])
    assert second.exit_code == 0
    assert inspector.after_ids == [0, 10]
    assert state.legacy_checkpoint() == (10, 10)


def test_media_and_job_commands_use_existing_api_surface(tmp_path: Path, monkeypatch):
    state = LocalState(tmp_path / "state.sqlite3")
    state.set_setting("device-id", "66bf29e1-8c76-42aa-bd9b-d7487f914276")

    class SurfaceApi:
        def __init__(self):
            self.calls: list[str] = []

        def list_media(self, device_id, **_kwargs):
            self.calls.append(f"list:{device_id}")
            return []

        def register_device(self, _payload, *, key):
            self.calls.append(f"device-register:{key}")
            return {"deviceId": "66bf29e1-8c76-42aa-bd9b-d7487f914276"}

        def get_media(self, media_asset_id, device_id):
            self.calls.append(f"show:{media_asset_id}:{device_id}")
            return {"mediaAssetId": media_asset_id, "displayFileName": "IMG_0001.JPG"}

        def search_media(self, query, device_id, **_kwargs):
            self.calls.append(f"search:{query}:{device_id}")
            return []

        def list_jobs(self, **_kwargs):
            self.calls.append("jobs-list")
            return []

        def get_job(self, job_id):
            self.calls.append(f"jobs-get:{job_id}")
            return {
                "jobId": job_id,
                "jobType": "Geocode",
                "status": "Failed",
                "attemptCount": 1,
            }

        def retry_job(self, job_id, *, key):
            self.calls.append(f"jobs-retry:{job_id}:{key}")
            return {"jobId": job_id, "status": "Queued"}

    api = SurfaceApi()
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=api),
    )
    runner = CliRunner()
    assert runner.invoke(app, ["media", "list", "--json"]).exit_code == 0
    assert runner.invoke(app, ["media", "show", "asset-1", "--json"]).exit_code == 0
    assert runner.invoke(app, ["media", "search", "beach", "--json"]).exit_code == 0
    assert runner.invoke(app, ["jobs", "list", "--json"]).exit_code == 0
    assert runner.invoke(app, ["jobs", "retry", "job-1", "--json"]).exit_code == 0
    assert api.calls[1:] == [
        "list:66bf29e1-8c76-42aa-bd9b-d7487f914276",
        "show:asset-1:66bf29e1-8c76-42aa-bd9b-d7487f914276",
        "search:beach:66bf29e1-8c76-42aa-bd9b-d7487f914276",
        "jobs-list",
        "jobs-get:job-1",
        "jobs-retry:job-1:job-retry:job-1:Failed:1",
    ]
    assert api.calls[0].startswith("device-register:device:")


def test_local_sync_stops_if_server_requests_object_upload(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    (root / "one.jpg").write_bytes(b"data")
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    api = FakeManifestApi(upload_required=True)

    with pytest.raises(ApiError, match="Local source"):
        SyncEngine(
            api,  # type: ignore[arg-type]
            state,
            MediaScanner(state, metadata_extractor=FakeMetadata()),  # type: ignore[arg-type]
        ).sync(binding)
    assert state.pending_count() == 0
    assert state.failed_count() == 1


def test_incomplete_scan_never_generates_deletion_entries(tmp_path: Path):
    root = tmp_path / "library"
    root.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")
    binding = state.bind_source(source_payload(root), root)
    prior = {
        "kind": "Full",
        "permissionState": "NotApplicable",
        "deletionDetectionReliable": True,
        "entries": [
            {
                "operation": "Upsert",
                "sourceItemId": source_item_id("old.jpg"),
                "sourceRevision": "a" * 64,
                "fileName": "old.jpg",
                "localLocator": str(root / "old.jpg"),
                "mediaType": "Photo",
                "mimeType": "image/jpeg",
                "byteSize": 10,
            }
        ],
    }
    scan_id = state.begin_scan(SOURCE_ID, root)
    state.queue_batches(SOURCE_ID, scan_id, [prior])
    batch = state.pending_batches(SOURCE_ID)[0]
    state.acknowledge_batch(batch)

    scanner = SimpleNamespace(
        scan=lambda *_args, **_kwargs: ScanResult(
            complete_read=False, errors=["permission denied"]
        )
    )
    api = FakeManifestApi()
    summary = SyncEngine(api, state, scanner).sync(binding)  # type: ignore[arg-type]
    assert summary.deletions == 0
    assert summary.deletion_detection_reliable is False
    assert api.calls == []


def test_cli_exposes_phase1_command_groups():
    runner = CliRunner()
    root_help = runner.invoke(app, ["--help"])
    assert root_help.exit_code == 0
    for command in ("configure", "auth", "source", "sync", "status", "legacy"):
        assert command in root_help.stdout


def test_source_add_rejects_remote_before_any_api_mutation(tmp_path: Path, monkeypatch):
    library = tmp_path / "library"
    library.mkdir()
    runtime_called = False

    def forbidden_runtime():
        nonlocal runtime_called
        runtime_called = True
        raise AssertionError("runtime must not be built for unavailable Remote mode")

    monkeypatch.setattr(cli_app_module, "_runtime", forbidden_runtime)
    result = CliRunner().invoke(app, ["source", "add", str(library), "--mode", "Remote"])
    assert result.exit_code == 2
    assert "Remote mode is planned for Phase 3" in result.stderr
    assert "no files were uploaded or changed" in result.stderr
    assert runtime_called is False


def test_source_add_retries_same_lifecycle_with_same_mutation_key(
    tmp_path: Path, monkeypatch
) -> None:
    library = tmp_path / "library"
    library.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")

    class SourceApi:
        def __init__(self) -> None:
            self.keys: list[str] = []
            self.sequence = 0

        def register_device(self, _payload, *, key):
            return {"deviceId": "66bf29e1-8c76-42aa-bd9b-d7487f914276"}

        def create_source(self, payload, *, key):
            self.keys.append(key)
            return {
                "sourceId": SOURCE_ID,
                **payload,
                "status": "Active",
            }

    api = SourceApi()
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=api),
    )
    runner = CliRunner()
    first = runner.invoke(app, ["source", "add", str(library), "--json"])
    second = runner.invoke(app, ["source", "add", str(library), "--json"])
    assert first.exit_code == second.exit_code == 0
    assert len(api.keys) == 2
    assert api.keys[0] == api.keys[1]
    assert api.keys[0].endswith(":g0")


def test_source_lifecycle_keys_change_only_after_successful_remove(
    tmp_path: Path, monkeypatch
) -> None:
    library = tmp_path / "library"
    library.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")

    class LifecycleApi:
        def __init__(self) -> None:
            self.create_keys: list[str] = []
            self.remove_keys: list[str] = []

        def register_device(self, _payload, *, key):
            return {"deviceId": "66bf29e1-8c76-42aa-bd9b-d7487f914276"}

        def create_source(self, payload, *, key):
            self.create_keys.append(key)
            return {"sourceId": SOURCE_ID, **payload, "status": "Active"}

        def remove_source(self, source_id, *, key):
            assert source_id == SOURCE_ID
            self.remove_keys.append(key)

    api = LifecycleApi()
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=api),
    )
    runner = CliRunner()
    assert runner.invoke(app, ["source", "add", str(library), "--json"]).exit_code == 0
    assert runner.invoke(app, ["source", "remove", SOURCE_ID, "--yes"]).exit_code == 0
    assert state.source_lifecycle_generation(library) == 1
    assert runner.invoke(app, ["source", "add", str(library), "--json"]).exit_code == 0
    assert runner.invoke(app, ["source", "remove", SOURCE_ID, "--yes"]).exit_code == 0
    assert state.source_lifecycle_generation(library) == 2
    assert api.create_keys[0].endswith(":g0")
    assert api.create_keys[1].endswith(":g1")
    assert api.create_keys[0] != api.create_keys[1]
    assert api.remove_keys == [
        f"source-remove:{SOURCE_ID}:g0",
        f"source-remove:{SOURCE_ID}:g1",
    ]
    assert state.list_bindings() == []


def test_lost_remove_response_retries_same_generation_key(
    tmp_path: Path, monkeypatch
) -> None:
    library = tmp_path / "library"
    library.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")
    state.bind_source(source_payload(library), library)

    class FlakyRemoveApi:
        def __init__(self) -> None:
            self.keys: list[str] = []

        def remove_source(self, source_id, *, key):
            del source_id
            self.keys.append(key)
            if len(self.keys) == 1:
                raise ApiError(ApiProblem(0, "Network error", "Response was lost"))

    api = FlakyRemoveApi()
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=api),
    )
    runner = CliRunner()
    first = runner.invoke(app, ["source", "remove", SOURCE_ID, "--yes"])
    assert first.exit_code == ExitCode.NETWORK
    assert state.source_lifecycle_generation(library) == 0
    assert len(state.list_bindings()) == 1
    second = runner.invoke(app, ["source", "remove", SOURCE_ID, "--yes"])
    assert second.exit_code == 0
    assert api.keys == [
        f"source-remove:{SOURCE_ID}:g0",
        f"source-remove:{SOURCE_ID}:g0",
    ]
    assert state.source_lifecycle_generation(library) == 1
    assert state.list_bindings() == []


def test_source_add_never_binds_removed_conflict_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    library = tmp_path / "library"
    library.mkdir()
    state = LocalState(tmp_path / "state.sqlite3")

    class RemovedSourceApi:
        def register_device(self, _payload, *, key):
            return {"deviceId": "66bf29e1-8c76-42aa-bd9b-d7487f914276"}

        @staticmethod
        def idempotency_key(prefix: str) -> str:
            return f"{prefix}:new-attempt"

        def create_source(self, payload, *, key):
            raise ApiError(
                ApiProblem(
                    409,
                    "Source exists",
                    "An incompatible source is already registered.",
                    code="SourceAlreadyExists",
                )
            )

        def list_sources(self):
            source_key = state.source_key_for_root(library)
            return [
                {
                    "sourceId": SOURCE_ID,
                    "deviceId": "66bf29e1-8c76-42aa-bd9b-d7487f914276",
                    "sourceKey": source_key,
                    "sourceType": "Folder",
                    "displayName": library.name,
                    "storageMode": "Local",
                    "permissionState": "NotApplicable",
                    "status": "Removed",
                    "syncSettings": {
                        "automaticSync": True,
                        "networkPolicy": "WiFiOnly",
                        "requireChargingForHistoricalUpload": True,
                    },
                }
            ]

    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=RemovedSourceApi()),
    )
    result = CliRunner().invoke(app, ["source", "add", str(library), "--json"])
    assert result.exit_code == ExitCode.SERVICE
    assert state.list_bindings() == []


def test_source_set_mode_rejects_remote_before_any_api_mutation(monkeypatch):
    runtime_called = False

    def forbidden_runtime():
        nonlocal runtime_called
        runtime_called = True
        raise AssertionError("runtime must not be built for unavailable Remote mode")

    monkeypatch.setattr(cli_app_module, "_runtime", forbidden_runtime)
    result = CliRunner().invoke(app, ["source", "set-mode", SOURCE_ID, "Remote"])
    assert result.exit_code == 2
    assert "Remote mode is planned for Phase 3" in result.stderr
    assert "no files were uploaded or changed" in result.stderr
    assert runtime_called is False


def test_legacy_migration_requires_explicit_dry_run():
    result = CliRunner().invoke(app, ["legacy", "migrate"])
    assert result.exit_code == 2
    assert "preview-only" in result.stderr
