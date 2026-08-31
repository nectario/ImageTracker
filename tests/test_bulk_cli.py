from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest
from typer.testing import CliRunner

import cli.imagetracker_cli.app as cli_app_module
from cli.imagetracker_cli.api_client import ApiClient, ApiError, ApiProblem
from cli.imagetracker_cli.app import app
from cli.imagetracker_cli.auth import TokenSet
from cli.imagetracker_cli.bulk import (
    BulkArtifactError,
    MANIFEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    iter_result_entries,
    parse_result_gzip,
    read_result_header,
    write_manifest_gzip,
)
from cli.imagetracker_cli.state import LocalState
from cli.imagetracker_cli.media import ScanResult
from cli.imagetracker_cli.sync import SyncEngine
from services.bulk.manifest import parse_manifest_gzip, write_result_gzip


SOURCE_ID = "4a63c5b3-9266-46d4-bddb-cf447ff93d90"
SNAPSHOT_ID = "740851e1-b22d-49f7-8f76-08d07d266cab"
IMPORT_ID = "39770150-2b38-492f-bfb6-b4634586e763"


def _entry(index: int) -> dict[str, Any]:
    return {
        "operation": "Upsert",
        "sourceItemId": f"path:photo-{index}",
        "sourceRevision": hashlib.sha256(f"revision-{index}".encode()).hexdigest(),
        "fileName": f"Φωτογραφία {index}.jpg",
        "localLocator": f"/mnt/d/Pictures/Φωτογραφία {index}.jpg",
        "contentSha256": hashlib.sha256(f"photo-{index}".encode()).hexdigest(),
        "mediaType": "Photo",
        "mimeType": "image/jpeg",
        "byteSize": 1000 + index,
        "widthPixels": 1600,
        "heightPixels": 900,
        "durationMs": None,
        "capturedAtLocal": "2026-08-30T12:30:00",
        "capturedAtUtc": "2026-08-30T16:30:00Z",
        "timeZoneId": "America/New_York",
        "utcOffsetMinutes": -240,
        "location": {"latitude": 40.7128, "longitude": -74.006},
        "provenance": [{"field": "location", "source": "Exif"}],
    }


def _result_file(path: Path, rows: list[dict[str, Any]]) -> tuple[int, str]:
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as archive:
            records = [
                {
                    "recordType": "Result",
                    "schemaVersion": RESULT_SCHEMA_VERSION,
                    "importId": IMPORT_ID,
                    "counts": {"created": len(rows), "rejected": 0},
                    "sourceCursor": "cursor-1",
                    "acceptedAtUtc": "2026-08-31T12:00:00Z",
                },
                *rows,
            ]
            for record in records:
                archive.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
    content = path.read_bytes()
    return len(content), hashlib.sha256(content).hexdigest()


def test_manifest_writer_is_deterministic_utf8_lf_and_transport_exact(
    tmp_path: Path,
):
    first = write_manifest_gzip(
        tmp_path / "first.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1), _entry(2)],
        permission_state="Full",
    )
    second = write_manifest_gzip(
        tmp_path / "second.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1), _entry(2)],
        permission_state="Full",
    )

    assert first.compressed_sha256 == hashlib.sha256(first.path.read_bytes()).hexdigest()
    assert first.compressed_bytes == first.path.stat().st_size
    assert first.entry_count == 2
    assert first.path.read_bytes() == second.path.read_bytes()
    with gzip.open(first.path, "rb") as archive:
        expanded = archive.read()
    assert expanded.endswith(b"\n")
    assert len(expanded.splitlines(keepends=True)) == 3
    assert all(line.endswith(b"\n") for line in expanded.splitlines(keepends=True))
    records = [json.loads(line) for line in expanded.decode("utf-8").splitlines()]
    assert records[0] == {
        "clientCursor": None,
        "deletionDetectionReliable": False,
        "entryCount": 2,
        "kind": "Full",
        "permissionState": "Full",
        "recordType": "Manifest",
        "schemaVersion": MANIFEST_SCHEMA_VERSION,
        "snapshotId": SNAPSHOT_ID,
        "sourceId": SOURCE_ID,
        "storageMode": "Local",
    }
    assert records[1]["rowNumber"] == 1
    assert records[1]["fileName"] == "Φωτογραφία 1.jpg"
    parsed = parse_manifest_gzip(
        first.path,
        tmp_path / "canonical.csv",
        expected_sha256=first.compressed_sha256,
        expected_compressed_bytes=first.compressed_bytes,
        expected_entry_count=first.entry_count,
    )
    assert parsed.entry_count == 2
    assert parsed.rejected_count == 0


def test_manifest_writer_rejects_non_hash_upserts_and_oversized_lines(
    tmp_path: Path,
):
    deleted = _entry(1)
    deleted["operation"] = "Deleted"
    with pytest.raises(BulkArtifactError, match="hash-enriched Upsert"):
        write_manifest_gzip(
            tmp_path / "deleted.gz",
            source_id=SOURCE_ID,
            snapshot_id=SNAPSHOT_ID,
            entries=[deleted],
        )
    unhashed = _entry(1)
    unhashed["contentSha256"] = None
    with pytest.raises(BulkArtifactError, match="requires a SHA-256"):
        write_manifest_gzip(
            tmp_path / "unhashed.gz",
            source_id=SOURCE_ID,
            snapshot_id=SNAPSHOT_ID,
            entries=[unhashed],
        )
    oversized = _entry(1)
    oversized["fileName"] = "x" * (256 * 1024)
    with pytest.raises(BulkArtifactError, match="exceeds"):
        write_manifest_gzip(
            tmp_path / "oversized.gz",
            source_id=SOURCE_ID,
            snapshot_id=SNAPSHOT_ID,
            entries=[oversized],
        )
    assert not list(tmp_path.glob(".*.part"))


def test_manifest_writer_rejects_noncanonical_granted_permission(tmp_path: Path):
    with pytest.raises(BulkArtifactError, match="permission state is invalid"):
        write_manifest_gzip(
            tmp_path / "granted.gz",
            source_id=SOURCE_ID,
            snapshot_id=SNAPSHOT_ID,
            entries=[_entry(1)],
            permission_state="Granted",
        )


def test_result_header_is_verified_then_rows_stream_from_resume_cursor(
    tmp_path: Path,
):
    result_path = tmp_path / "result.ndjson.gz"
    rows = [
        {
            "recordType": "EntryResult",
            "rowNumber": index,
            "sourceItemId": f"path:photo-{index}",
            "outcome": "CreatedOccurrence",
            "occurrenceId": str(uuid4()),
            "mediaAssetId": str(uuid4()),
            "descriptionJobId": str(uuid4()),
            "uploadRequired": False,
            "errorCode": None,
            "errorMessage": None,
        }
        for index in range(1, 4)
    ]
    size, checksum = _result_file(result_path, rows)

    header = read_result_header(
        result_path,
        expected_sha256=checksum,
        expected_compressed_bytes=size,
        expected_import_id=IMPORT_ID,
    )

    assert header.entry_count == 3
    assert header.counts == {"created": 3, "rejected": 0}
    assert header.source_cursor == "cursor-1"
    resumed = list(iter_result_entries(header, after_row_number=1))
    assert [entry.row_number for entry in resumed] == [2, 3]
    assert [entry.source_item_id for entry in resumed] == [
        "path:photo-2",
        "path:photo-3",
    ]
    small = parse_result_gzip(
        result_path,
        expected_sha256=checksum,
        expected_compressed_bytes=size,
        expected_import_id=IMPORT_ID,
    )
    assert len(small.entries) == 3


def test_result_parser_reads_server_writer_contract(tmp_path: Path):
    path = tmp_path / "server-result.gz"
    rows = [
        {
            "rowNumber": 1,
            "sourceItemId": "path:photo-1",
            "outcome": "Rejected",
            "uploadRequired": False,
            "occurrenceId": None,
            "mediaAssetId": None,
            "descriptionJobId": None,
            "errorCode": "InvalidManifestEntry",
            "errorMessage": "The row was invalid.",
        }
    ]
    size, checksum, count, _uncompressed = write_result_gzip(
        path,
        import_id=IMPORT_ID,
        counts={"created": 0, "rejected": 1},
        rows=rows,
    )

    header = read_result_header(
        path,
        expected_sha256=checksum,
        expected_compressed_bytes=size,
        expected_import_id=IMPORT_ID,
    )

    assert count == header.entry_count == 1
    result = next(iter_result_entries(header))
    assert result.row_number == 1
    assert result.outcome == "Rejected"
    assert result.error_code == "InvalidManifestEntry"


def test_result_parser_rejects_checksum_and_noncontiguous_rows(tmp_path: Path):
    result_path = tmp_path / "result.ndjson.gz"
    size, checksum = _result_file(
        result_path,
        [
            {
                "recordType": "EntryResult",
                "rowNumber": 2,
                "sourceItemId": "path:photo-2",
                "outcome": "Rejected",
                "uploadRequired": False,
                "errorCode": "InvalidManifestEntry",
                "errorMessage": "bad row",
            }
        ],
    )
    with pytest.raises(BulkArtifactError, match="checksum"):
        read_result_header(
            result_path,
            expected_sha256="0" * 64,
            expected_compressed_bytes=size,
            expected_import_id=IMPORT_ID,
        )
    with pytest.raises(BulkArtifactError, match="contiguous"):
        read_result_header(
            result_path,
            expected_sha256=checksum,
            expected_compressed_bytes=size,
            expected_import_id=IMPORT_ID,
        )


def _bound_state(tmp_path: Path) -> LocalState:
    state = LocalState(tmp_path / "state.sqlite3")
    state.bind_source(
        {
            "sourceId": SOURCE_ID,
            "sourceKey": "source-key",
            "displayName": "Photos",
            "storageMode": "Local",
        },
        tmp_path,
    )
    return state


def test_bulk_outbox_applies_pages_and_supersedes_members_only_after_success(
    tmp_path: Path,
):
    state = _bound_state(tmp_path)
    scan_id = state.begin_scan(SOURCE_ID, tmp_path)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        (
            {"kind": "Full", "entries": [_entry(1)]},
            {"kind": "Full", "entries": [_entry(2)]},
        ),
    )
    batch_ids = tuple(batch.batch_id for batch in state.pending_batches(SOURCE_ID))
    artifact = write_manifest_gzip(
        tmp_path / "manifest.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1), _entry(2)],
    )
    queued = state.queue_bulk_manifest(
        SOURCE_ID,
        SNAPSHOT_ID,
        artifact_path=artifact.path,
        artifact_sha256=artifact.compressed_sha256,
        artifact_bytes=artifact.compressed_bytes,
        entry_count=artifact.entry_count,
        superseded_batch_ids=batch_ids,
    )

    assert state.pending_count() == 2
    assert state.active_bulk_manifest(SOURCE_ID) == queued
    assert state.queue_bulk_manifest(
        SOURCE_ID,
        SNAPSHOT_ID,
        artifact_path=artifact.path,
        artifact_sha256=artifact.compressed_sha256,
        artifact_bytes=artifact.compressed_bytes,
        entry_count=artifact.entry_count,
        superseded_batch_ids=batch_ids,
    ).bulk_id == queued.bulk_id

    state.set_bulk_manifest_server_import(
        queued.bulk_id,
        server_import_id=IMPORT_ID,
    )
    state.update_bulk_manifest_status(queued.bulk_id, status="Succeeded")
    result_path = tmp_path / "result.gz"
    result_path.write_bytes(b"result")
    state.record_bulk_manifest_result(
        queued.bulk_id,
        result_path=result_path,
        result_sha256=hashlib.sha256(b"result").hexdigest(),
        result_bytes=6,
    )
    first_result = {
        "sourceItemId": _entry(1)["sourceItemId"],
        "outcome": "CreatedOccurrence",
        "occurrenceId": str(uuid4()),
        "mediaAssetId": str(uuid4()),
        "descriptionJobId": str(uuid4()),
        "uploadRequired": False,
    }
    with pytest.raises(ValueError, match="contiguous"):
        state.apply_bulk_result_rows(
            queued.bulk_id,
            (
                (1, _entry(1), first_result),
                (
                    3,
                    _entry(2),
                    {
                        "sourceItemId": _entry(2)["sourceItemId"],
                        "outcome": "Rejected",
                        "uploadRequired": False,
                    },
                ),
            ),
        )
    rolled_back = state.bulk_manifest(queued.bulk_id)
    assert rolled_back is not None and rolled_back.result_applied_through == 0
    assert state.known_occurrences(SOURCE_ID) == {}
    applied = state.apply_bulk_result_rows(
        queued.bulk_id,
        ((1, _entry(1), first_result),),
    )
    assert applied == 1
    assert state.pending_count() == 2
    assert len(state.known_occurrences(SOURCE_ID)) == 1
    with pytest.raises(ValueError, match="contiguous"):
        state.apply_bulk_result_rows(
            queued.bulk_id,
            ((1, _entry(1), first_result),),
        )
    state.apply_bulk_result_rows(
        queued.bulk_id,
        (
            (
                2,
                _entry(2),
                {
                    "sourceItemId": _entry(2)["sourceItemId"],
                    "outcome": "CreatedOccurrence",
                    "occurrenceId": str(uuid4()),
                    "mediaAssetId": str(uuid4()),
                    "descriptionJobId": str(uuid4()),
                    "uploadRequired": False,
                },
            ),
        ),
    )

    state.complete_bulk_manifest(queued.bulk_id)

    completed = state.bulk_manifest(queued.bulk_id)
    assert completed is not None and completed.state == "Applied"
    assert completed.result_applied_through == 2
    assert state.pending_count() == 0
    assert len(state.list_outbox(state="Discarded")) == 2
    assert state.description_counts(SOURCE_ID)["Pending"] == 2


def test_bulk_terminal_failure_preserves_captured_manifest_batches(tmp_path: Path):
    state = _bound_state(tmp_path)
    scan_id = state.begin_scan(SOURCE_ID, tmp_path)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        ({"kind": "Full", "entries": [_entry(1)]},),
    )
    batch_id = state.pending_batches(SOURCE_ID)[0].batch_id
    artifact = write_manifest_gzip(
        tmp_path / "manifest.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1)],
    )
    queued = state.queue_bulk_manifest(
        SOURCE_ID,
        SNAPSHOT_ID,
        artifact_path=artifact.path,
        artifact_sha256=artifact.compressed_sha256,
        artifact_bytes=artifact.compressed_bytes,
        entry_count=artifact.entry_count,
        superseded_batch_ids=(batch_id,),
    )

    state.fail_bulk_manifest(
        queued.bulk_id,
        state="FailedPermanent",
        code="Invalid\nManifest",
        message="The import failed.\nRetry with batch transport.",
    )

    failed = state.bulk_manifest(queued.bulk_id)
    assert failed is not None and failed.state == "FailedPermanent"
    assert failed.failure == {
        "code": "Invalid Manifest",
        "message": "The import failed. Retry with batch transport.",
    }
    assert state.pending_count() == 1
    assert state.list_outbox(state="Discarded") == []


def test_bulk_capture_rejects_deletions_and_error_results_preserve_batches(
    tmp_path: Path,
):
    state = _bound_state(tmp_path)
    scan_id = state.begin_scan(SOURCE_ID, tmp_path)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        (
            {
                "kind": "Full",
                "entries": [
                    {
                        "operation": "Deleted",
                        "sourceItemId": "path:deleted",
                        "sourceRevision": "deleted-revision",
                    }
                ],
            },
        ),
    )
    delete_batch = state.pending_batches(SOURCE_ID)[0]
    artifact = write_manifest_gzip(
        tmp_path / "manifest.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1)],
    )
    with pytest.raises(ValueError, match="hash-enriched upserts"):
        state.queue_bulk_manifest(
            SOURCE_ID,
            SNAPSHOT_ID,
            artifact_path=artifact.path,
            artifact_sha256=artifact.compressed_sha256,
            artifact_bytes=artifact.compressed_bytes,
            entry_count=artifact.entry_count,
            superseded_batch_ids=(delete_batch.batch_id,),
        )
    assert state.pending_count() == 1

    second_root = tmp_path / "second"
    second_root.mkdir()
    state = _bound_state(second_root)
    scan_id = state.begin_scan(SOURCE_ID, second_root)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        ({"kind": "Full", "entries": [_entry(1)]},),
    )
    batch_id = state.pending_batches(SOURCE_ID)[0].batch_id
    artifact = write_manifest_gzip(
        tmp_path / "second" / "manifest.ndjson.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=[_entry(1)],
    )
    queued = state.queue_bulk_manifest(
        SOURCE_ID,
        SNAPSHOT_ID,
        artifact_path=artifact.path,
        artifact_sha256=artifact.compressed_sha256,
        artifact_bytes=artifact.compressed_bytes,
        entry_count=artifact.entry_count,
        superseded_batch_ids=(batch_id,),
    )
    state.set_bulk_manifest_server_import(
        queued.bulk_id,
        server_import_id=IMPORT_ID,
        status="CompletedWithErrors",
    )
    with pytest.raises(ValueError, match="rejection-free"):
        state.complete_bulk_manifest(queued.bulk_id)
    assert state.pending_count() == 1
    assert state.list_outbox(state="Discarded") == []


class _Sessions:
    def __init__(self) -> None:
        self.tokens = TokenSet(
            "access",
            "id",
            "refresh",
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )

    def current_tokens(self, *, refresh_if_needed: bool = True) -> TokenSet:
        del refresh_if_needed
        return self.tokens

    def refresh(self, tokens: TokenSet) -> TokenSet:
        return tokens


def test_manifest_import_api_routes_and_signed_file_transfer_are_scoped(
    tmp_path: Path,
):
    seen: list[tuple[str, str, str | None]] = []
    signed_upload = b"deterministic-manifest"
    signed_result = b"deterministic-result"

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.method,
                request.url.path,
                request.headers.get("Authorization"),
            )
        )
        if request.url.host == "storage.example":
            if request.method == "PUT":
                assert request.read() == signed_upload
                return httpx.Response(200, headers={"etag": '"manifest-etag"'})
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(signed_result))},
                stream=httpx.ByteStream(signed_result),
            )
        if request.url.path.endswith("/result"):
            return httpx.Response(200, json={"url": "https://storage.example/result"})
        if request.url.path.endswith("/upload-url"):
            return httpx.Response(200, json={"url": "https://storage.example/upload"})
        return httpx.Response(
            200,
            json={"importId": IMPORT_ID, "status": "AwaitingUpload"},
        )

    api = ApiClient(
        "https://api.example",
        _Sessions(),
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    payload = {
        "snapshotId": SNAPSHOT_ID,
        "schemaVersion": MANIFEST_SCHEMA_VERSION,
        "checksumSha256": "a" * 64,
        "byteSize": len(signed_upload),
        "entryCount": 1,
    }
    api.create_manifest_import(SOURCE_ID, payload, key="create-key")
    api.refresh_manifest_import_upload(
        SOURCE_ID,
        IMPORT_ID,
        key="refresh-key",
    )
    api.complete_manifest_import(SOURCE_ID, IMPORT_ID, key="complete-key")
    api.get_manifest_import(SOURCE_ID, IMPORT_ID)
    api.get_manifest_import_result(SOURCE_ID, IMPORT_ID)
    upload_path = tmp_path / "manifest.gz"
    upload_path.write_bytes(signed_upload)
    assert api.put_signed_file(
        "https://storage.example/upload",
        upload_path,
        headers={"Content-Length": str(len(signed_upload))},
    ) == '"manifest-etag"'
    destination = tmp_path / "result.gz"
    api.get_signed_file(
        "https://storage.example/result",
        destination,
        expected_bytes=len(signed_result),
        max_bytes=1024,
    )

    assert destination.read_bytes() == signed_result
    api_calls = [item for item in seen if item[1].startswith("/v1/")]
    assert [item[:2] for item in api_calls] == [
        ("POST", f"/v1/sources/{SOURCE_ID}/manifest-imports"),
        ("POST", f"/v1/sources/{SOURCE_ID}/manifest-imports/{IMPORT_ID}/upload-url"),
        ("POST", f"/v1/sources/{SOURCE_ID}/manifest-imports/{IMPORT_ID}/complete"),
        ("GET", f"/v1/sources/{SOURCE_ID}/manifest-imports/{IMPORT_ID}"),
        ("GET", f"/v1/sources/{SOURCE_ID}/manifest-imports/{IMPORT_ID}/result"),
    ]
    assert all(item[2] == "Bearer access" for item in api_calls)
    signed_calls = [item for item in seen if item[1] in {"/upload", "/result"}]
    assert all(item[2] is None for item in signed_calls)


def test_signed_result_download_preserves_gzip_and_enforces_size(tmp_path: Path):
    compressed_path = tmp_path / "source.gz"
    size, checksum = _result_file(
        compressed_path,
        [
            {
                "recordType": "EntryResult",
                "rowNumber": 1,
                "sourceItemId": "path:photo-1",
                "outcome": "Rejected",
                "uploadRequired": False,
                "errorCode": "InvalidManifestEntry",
                "errorMessage": "bad row",
            }
        ],
    )
    compressed = compressed_path.read_bytes()

    def encoded_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "Content-Encoding": "gzip",
                "Content-Length": str(size),
            },
            stream=httpx.ByteStream(compressed),
        )

    api = ApiClient(
        "https://api.example",
        _Sessions(),
        http_client=httpx.Client(transport=httpx.MockTransport(encoded_handler)),
    )
    destination = tmp_path / "downloaded.gz"

    api.get_signed_file(
        "https://storage.example/result",
        destination,
        expected_bytes=size,
        max_bytes=size,
    )

    assert destination.read_bytes() == compressed
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == checksum
    assert read_result_header(
        destination,
        expected_sha256=checksum,
        expected_compressed_bytes=size,
        expected_import_id=IMPORT_ID,
    ).entry_count == 1

    def oversized_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": "20"},
            stream=httpx.ByteStream(b"x" * 20),
        )

    oversized_api = ApiClient(
        "https://api.example",
        _Sessions(),
        http_client=httpx.Client(transport=httpx.MockTransport(oversized_handler)),
    )
    rejected = tmp_path / "rejected.gz"
    with pytest.raises(ApiError, match="size did not match"):
        oversized_api.get_signed_file(
            "https://storage.example/result",
            rejected,
            expected_bytes=10,
            max_bytes=10,
        )
    assert not rejected.exists()
    assert not list(tmp_path.glob(".rejected.gz.*.part"))

    def unbounded_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(b"x" * 11))

    unbounded_api = ApiClient(
        "https://api.example",
        _Sessions(),
        http_client=httpx.Client(transport=httpx.MockTransport(unbounded_handler)),
    )
    streamed_rejection = tmp_path / "streamed-rejection.gz"
    with pytest.raises(ApiError, match="exceeded"):
        unbounded_api.get_signed_file(
            "https://storage.example/result",
            streamed_rejection,
            expected_bytes=10,
            max_bytes=10,
        )
    assert not streamed_rejection.exists()
    assert not list(tmp_path.glob(".streamed-rejection.gz.*.part"))


class _NoopScanner:
    def __init__(self) -> None:
        self.calls = 0

    def scan(self, *_args: Any, **_kwargs: Any) -> ScanResult:
        self.calls += 1
        return ScanResult(complete_read=False)


class _BulkTransportApi:
    def __init__(
        self,
        result_path: Path,
        *,
        terminal_status: str = "Succeeded",
        transient_poll: bool = False,
        interrupt_poll: bool = False,
    ) -> None:
        self.result_path = result_path
        with gzip.open(result_path, "rb") as archive:
            self.entry_count = max(0, len(archive.read().splitlines()) - 1)
        self.terminal_status = terminal_status
        self.transient_poll = transient_poll
        self.interrupt_poll = interrupt_poll
        self.create_calls = 0
        self.upload_calls = 0
        self.complete_calls = 0
        self.poll_calls = 0
        self.result_calls = 0
        self.batch_calls = 0
        self.create_payload: dict[str, Any] | None = None

    def create_manifest_import(self, _source_id: str, payload, *, key: str):
        assert key.startswith("manifest-import:")
        self.create_calls += 1
        self.create_payload = dict(payload)
        return {
            "importId": IMPORT_ID,
            "status": "AwaitingUpload",
            "phase": "Waiting",
            "upload": {
                "url": "https://storage.example/manifest",
                "method": "PUT",
                "headers": {},
            },
        }

    def refresh_manifest_import_upload(
        self, _source_id: str, _import_id: str, *, key: str
    ):
        assert key.endswith(":upload")
        return {
            "url": "https://storage.example/manifest",
            "method": "PUT",
            "headers": {},
        }

    def put_signed_file(self, _url: str, path: Path, *, headers):
        del headers
        self.upload_calls += 1
        assert path.is_file() and path.stat().st_size > 0
        return '"etag"'

    def complete_manifest_import(
        self, _source_id: str, _import_id: str, *, key: str
    ):
        assert key.endswith(":complete")
        self.complete_calls += 1
        return {"status": "Queued", "phase": "Preparing"}

    def get_manifest_import(self, _source_id: str, _import_id: str):
        self.poll_calls += 1
        if self.create_calls > 0 and self.complete_calls == 0:
            return {
                "status": "AwaitingUpload",
                "phase": "WaitingForUpload",
                "processedEntryCount": 0,
                "counts": {},
            }
        if self.interrupt_poll:
            raise KeyboardInterrupt
        if self.transient_poll:
            self.transient_poll = False
            raise ApiError(
                ApiProblem(503, "Unavailable", "Try again later", code="UNAVAILABLE")
            )
        if (
            self.poll_calls == 1
            and self.terminal_status == "Succeeded"
        ):
            return {
                "status": "Running",
                "phase": "Merging",
                "processedEntryCount": 1,
                "counts": {},
            }
        rejected = 1 if self.terminal_status == "CompletedWithErrors" else 0
        return {
            "status": self.terminal_status,
            "phase": "Complete",
            "processedEntryCount": self.entry_count,
            "counts": {"created": 0, "rejected": rejected},
            "failureCode": (
                "InvalidManifestEntry" if self.terminal_status != "Succeeded" else None
            ),
            "failureMessage": (
                "One or more rows failed."
                if self.terminal_status != "Succeeded"
                else None
            ),
        }

    def get_manifest_import_result(self, _source_id: str, _import_id: str):
        self.result_calls += 1
        content = self.result_path.read_bytes()
        return {
            "url": "https://storage.example/result",
            "checksumSha256": hashlib.sha256(content).hexdigest(),
            "byteSize": len(content),
        }

    def get_signed_file(
        self,
        _url: str,
        destination: Path,
        *,
        expected_bytes: int,
        max_bytes: int,
        headers,
    ):
        del headers
        content = self.result_path.read_bytes()
        assert len(content) == expected_bytes <= max_bytes
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        return {}

    def submit_manifest(self, _source_id: str, payload, *, key: str):
        del key
        self.batch_calls += 1
        results = []
        counts = {
            "created": 0,
            "updated": 0,
            "duplicatesLinked": 0,
            "deleted": 0,
            "ignoredDeletions": 0,
            "unchanged": 0,
            "rejected": 0,
        }
        for entry in payload["entries"]:
            deleted = entry["operation"] == "Deleted"
            counts["deleted" if deleted else "created"] += 1
            results.append(
                {
                    "sourceItemId": entry["sourceItemId"],
                    "outcome": (
                        "DeletedOccurrence" if deleted else "CreatedOccurrence"
                    ),
                    "uploadRequired": False,
                }
            )
        return {"counts": counts, "results": results}

    def list_jobs(self, *, limit: int):
        assert limit == 50
        return []


def _queue_transport_batches(
    state: LocalState,
    entries: list[dict[str, Any]],
    *,
    batches: int,
) -> None:
    scan_id = state.begin_scan(SOURCE_ID, Path(state.path).parent)
    payloads = []
    for index in range(batches):
        selected = entries[index::batches]
        payloads.append(
            {
                "kind": "Full",
                "permissionState": "NotApplicable",
                "deletionDetectionReliable": False,
                "entries": selected,
            }
        )
    state.queue_batches(SOURCE_ID, scan_id, payloads)
    state.finish_scan(scan_id, complete_read=False, summary={"test": True})


def _server_result(path: Path, entries: list[dict[str, Any]]) -> None:
    rows = [
        {
            "rowNumber": index,
            "sourceItemId": entry["sourceItemId"],
            "outcome": "CreatedOccurrence",
            "occurrenceId": str(uuid4()),
            "mediaAssetId": str(uuid4()),
            "descriptionJobId": None,
            "uploadRequired": False,
            "errorCode": None,
            "errorMessage": None,
        }
        for index, entry in enumerate(entries, start=1)
    ]
    write_result_gzip(
        path,
        import_id=IMPORT_ID,
        counts={"created": len(entries), "rejected": 0},
        rows=rows,
    )


def _prepare_active_bulk(
    state: LocalState,
    entries: list[dict[str, Any]],
    *,
    server_status: str | None,
):
    _queue_transport_batches(state, entries, batches=1)
    artifact = write_manifest_gzip(
        state.path.parent / "active-manifest.gz",
        source_id=SOURCE_ID,
        snapshot_id=SNAPSHOT_ID,
        entries=entries,
    )
    batches = state.pending_batches(SOURCE_ID)
    item = state.queue_bulk_manifest(
        SOURCE_ID,
        SNAPSHOT_ID,
        artifact_path=artifact.path,
        artifact_sha256=artifact.compressed_sha256,
        artifact_bytes=artifact.compressed_bytes,
        entry_count=artifact.entry_count,
        superseded_batch_ids=tuple(batch.batch_id for batch in batches),
    )
    if server_status is not None:
        state.set_bulk_manifest_server_import(
            item.bulk_id,
            server_import_id=IMPORT_ID,
            status=server_status,
            phase=server_status,
        )
        item = state.bulk_manifest(item.bulk_id)
    assert item is not None
    return item


def test_sync_auto_bulk_success_applies_result_and_exposes_summary(tmp_path: Path):
    state = _bound_state(tmp_path)
    entries = [_entry(index) for index in range(10)]
    _queue_transport_batches(state, entries, batches=10)
    result_path = tmp_path / "server-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path)
    scanner = _NoopScanner()

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        scanner,
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="auto")

    assert api.create_calls == api.upload_calls == api.complete_calls == 1
    assert api.batch_calls == 0
    assert scanner.calls == 1
    assert summary.bulk_completed is True
    assert summary.bulk_state == "Applied"
    assert summary.bulk_processed == summary.bulk_total == 10
    assert summary.bulk_captured_batches == 10
    assert state.pending_count() == 0
    history = state.list_bulk_manifests(SOURCE_ID)
    assert len(history) == 1 and history[0].state == "Applied"
    assert len(state.known_occurrences(SOURCE_ID)) == 10


def test_bulk_row_limit_processes_one_segment_and_stops_before_rescan(
    tmp_path: Path,
):
    state = _bound_state(tmp_path)
    entries = [_entry(index) for index in range(200)]
    _queue_transport_batches(state, entries, batches=20)
    pending = state.pending_batches(SOURCE_ID)
    captured_entries = [
        dict(entry)
        for batch in pending[:10]
        for entry in (batch.payload.get("entries") or [])
    ]
    assert len(captured_entries) == 100
    result_path = tmp_path / "segment-result.gz"
    _server_result(result_path, captured_entries)
    api = _BulkTransportApi(result_path)
    scanner = _NoopScanner()

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        scanner,
        sleep=lambda _seconds: None,
    ).sync(
        state.resolve_binding(SOURCE_ID),
        transport="bulk",
        bulk_max_rows=100,
    )

    assert summary.bulk_completed is True
    assert summary.bulk_total == 100
    assert summary.queued_batches == 10
    assert api.create_payload is not None
    assert api.create_payload["entryCount"] == 100
    assert state.pending_count() == 10
    assert scanner.calls == 0


def test_sync_bulk_resumes_after_transient_poll_without_reupload(tmp_path: Path):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _queue_transport_batches(state, entries, batches=1)
    result_path = tmp_path / "server-result.gz"
    _server_result(result_path, entries)
    first_api = _BulkTransportApi(result_path, transient_poll=True)
    first_scanner = _NoopScanner()

    paused = SyncEngine(  # type: ignore[arg-type]
        first_api,
        state,
        first_scanner,
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="bulk")

    assert paused.bulk_state == "Queued"
    assert paused.queued_batches == 1
    assert first_scanner.calls == 0
    active = state.active_bulk_manifest(SOURCE_ID)
    assert active is not None and active.server_import_id == IMPORT_ID

    resumed_api = _BulkTransportApi(result_path)
    resumed = SyncEngine(  # type: ignore[arg-type]
        resumed_api,
        state,
        _NoopScanner(),
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="auto")

    assert resumed.bulk_completed is True
    assert resumed_api.create_calls == 0
    assert resumed_api.upload_calls == 0
    assert resumed_api.complete_calls == 0
    assert state.pending_count() == 0


@pytest.mark.parametrize("terminal_status", ["CompletedWithErrors", "FailedPermanent"])
def test_sync_bulk_terminal_failure_closes_attempt_then_falls_back_to_batches(
    tmp_path: Path,
    terminal_status: str,
):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _queue_transport_batches(state, entries, batches=1)
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path, terminal_status=terminal_status)

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        _NoopScanner(),
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="bulk")

    assert api.batch_calls == 1
    assert api.result_calls == 0
    assert summary.batches_sent == 1
    assert state.active_bulk_manifest(SOURCE_ID) is None
    attempt = state.list_bulk_manifests(SOURCE_ID)[0]
    assert attempt.state == "FailedPermanent"
    assert attempt.server_status == terminal_status
    assert len(state.list_outbox(state="Sent")) == 1
    assert state.list_outbox(state="Discarded") == []


def test_sync_bulk_delete_candidate_uses_batch_without_creating_import(tmp_path: Path):
    state = _bound_state(tmp_path)
    deleted = {
        "operation": "Deleted",
        "sourceItemId": "path:deleted",
        "sourceRevision": "d" * 64,
    }
    _queue_transport_batches(state, [deleted], batches=1)
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, [_entry(1)])
    api = _BulkTransportApi(result_path)

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        _NoopScanner(),
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="bulk")

    assert api.create_calls == 0
    assert api.batch_calls == 1
    assert summary.batches_sent == 1
    assert state.list_bulk_manifests(SOURCE_ID) == []


def test_ctrl_c_after_bulk_complete_request_leaves_server_import_resumable(
    tmp_path: Path,
):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _queue_transport_batches(state, entries, batches=1)
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path, interrupt_poll=True)

    with pytest.raises(KeyboardInterrupt):
        SyncEngine(  # type: ignore[arg-type]
            api,
            state,
            _NoopScanner(),
            sleep=lambda _seconds: None,
        ).sync(state.resolve_binding(SOURCE_ID), transport="bulk")

    active = state.active_bulk_manifest(SOURCE_ID)
    assert active is not None
    assert active.server_import_id == IMPORT_ID
    assert active.server_status == "Queued"
    assert state.pending_count() == 1


@pytest.mark.parametrize("cached_status", ["RetryDue", "AwaitingUpload"])
def test_bound_bulk_refreshes_server_before_retry_or_reupload(
    tmp_path: Path,
    cached_status: str,
):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _prepare_active_bulk(state, entries, server_status=cached_status)
    result_path = tmp_path / "server-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path)

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        _NoopScanner(),
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="auto")

    assert summary.bulk_completed is True
    assert api.poll_calls >= 2
    assert api.create_calls == 0
    assert api.upload_calls == 0
    assert api.complete_calls == 0
    assert state.pending_count() == 0


def test_batch_transport_abandons_only_unsubmitted_bulk(tmp_path: Path):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    prepared = _prepare_active_bulk(state, entries, server_status=None)
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path)

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        _NoopScanner(),
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="batch")

    assert summary.batches_sent == 1
    assert api.create_calls == 0
    assert api.batch_calls == 1
    abandoned = state.bulk_manifest(prepared.bulk_id)
    assert abandoned is not None and abandoned.state == "Cancelled"
    assert len(state.list_outbox(state="Sent")) == 1


def test_batch_transport_does_not_race_submitted_bulk(tmp_path: Path):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _prepare_active_bulk(state, entries, server_status="Queued")
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path)
    progress: list[str] = []
    scanner = _NoopScanner()

    summary = SyncEngine(  # type: ignore[arg-type]
        api,
        state,
        scanner,
        progress=progress.append,
        sleep=lambda _seconds: None,
    ).sync(state.resolve_binding(SOURCE_ID), transport="batch")

    assert summary.bulk_state == "Running"
    assert summary.queued_batches == 1
    assert api.batch_calls == 0
    assert scanner.calls == 0
    assert any("will not race" in message for message in progress)
    assert state.pending_count() == 1


def test_status_refreshes_bulk_progress_and_marks_cached_fallback(
    tmp_path: Path,
    monkeypatch,
):
    state = _bound_state(tmp_path)
    entries = [_entry(1)]
    _prepare_active_bulk(state, entries, server_status="Queued")
    result_path = tmp_path / "unused-result.gz"
    _server_result(result_path, entries)
    api = _BulkTransportApi(result_path)
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=api),
    )

    refreshed = CliRunner().invoke(app, ["status", "--json"])

    assert refreshed.exit_code == 0
    payload = json.loads(refreshed.stdout)
    assert payload["bulkImports"][0]["cached"] is False
    assert payload["bulkImports"][0]["phase"] == "Merging"
    assert payload["bulkImports"][0]["processedEntries"] == 1

    cached_api = _BulkTransportApi(result_path, transient_poll=True)
    monkeypatch.setattr(
        cli_app_module,
        "_runtime",
        lambda: SimpleNamespace(state=state, api=cached_api),
    )
    cached = CliRunner().invoke(app, ["status", "--json"])

    assert cached.exit_code == 0
    cached_payload = json.loads(cached.stdout)
    assert cached_payload["bulkImports"][0]["cached"] is True
    assert cached_payload["bulkImports"][0]["phase"] == "Merging"
