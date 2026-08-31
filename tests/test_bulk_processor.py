from __future__ import annotations

from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import shutil
from uuid import UUID

from services.bulk.composition import BulkMessageRouter, SqsManifestImportDispatcher
from services.bulk.handler import handler
from services.bulk.processor import (
    BulkManifestProcessor,
    BulkMessageDisposition,
    BulkProcessorSettings,
    S3ManifestObjectStore,
)
from services.bulk.repository import (
    BulkImportDatabaseError,
    ManifestImportClaim,
    MergeResult,
    MergeSettings,
)


USER_ID = UUID("00000000-0000-0000-0000-000000000401")
SOURCE_ID = UUID("00000000-0000-0000-0000-000000000402")
IMPORT_ID = UUID("00000000-0000-0000-0000-000000000403")
SNAPSHOT_ID = UUID("00000000-0000-0000-0000-000000000404")


def _artifact(path: Path) -> tuple[int, str]:
    records = [
        {
            "recordType": "Manifest",
            "schemaVersion": "ManifestNdjsonV1",
            "storageMode": "Local",
            "sourceId": str(SOURCE_ID),
            "snapshotId": str(SNAPSHOT_ID),
            "entryCount": 1,
            "kind": "Full",
            "permissionState": "NotApplicable",
            "deletionDetectionReliable": False,
            "clientCursor": None,
        },
        {
            "recordType": "Entry",
            "operation": "Upsert",
            "sourceItemId": "path:one",
            "sourceRevision": "revision-one",
            "fileName": "one.jpg",
            "localLocator": "/mnt/d/one.jpg",
            "contentSha256": "a" * 64,
            "mediaType": "Photo",
            "mimeType": "image/jpeg",
            "byteSize": 123,
        },
    ]
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as archive:
            for record in records:
                archive.write(json.dumps(record, sort_keys=True).encode() + b"\n")
    content = path.read_bytes()
    return len(content), hashlib.sha256(content).hexdigest()


def _claim(byte_size: int, checksum: str) -> ManifestImportClaim:
    return ManifestImportClaim(
        internal_id=12,
        public_id=IMPORT_ID,
        snapshot_id=SNAPSHOT_ID,
        user_id=5,
        user_public_id=USER_ID,
        source_internal_id=6,
        source_public_id=SOURCE_ID,
        source_device_id=7,
        input_bucket="media-bucket",
        input_object_key=(
            f"manifests/input/{USER_ID}/{SOURCE_ID}/{IMPORT_ID}.ndjson.gz"
        ),
        input_version_id="version-one",
        input_sha256=checksum,
        input_byte_size=byte_size,
        declared_entry_count=1,
        phase="Downloading",
        attempt_count=1,
        max_attempts=5,
        lease_owner="message-one",
    )


class FakeRepository:
    def __init__(self, claim: ManifestImportClaim | None) -> None:
        self.claim_value = claim
        self.loaded = 0
        self.merged = 0
        self.completed: dict[str, object] | None = None
        self.failures: list[dict[str, object]] = []
        self.phases: list[str] = []
        self.merge_settings: list[MergeSettings] = []
        self.geocode_batch_queries = 0

    def claim(self, **_kwargs):
        return self.claim_value

    def load_stage(self, _claim, parsed):
        assert parsed.entry_count == 1
        self.loaded += 1

    def merge(self, _claim, **kwargs):
        self.merged += 1
        self.merge_settings.append(kwargs["settings"])
        return MergeResult(1, 1, 0, 0, 0, 0)

    def queued_geocode_job_batches(self, _claim):
        self.geocode_batch_queries += 1
        yield (UUID("00000000-0000-0000-0000-000000000405"),)

    def set_phase(self, _claim, *, phase, allowed_phases):
        assert phase in {"Merging", "WritingResult"}
        assert allowed_phases
        self.phases.append(phase)

    def iter_results(self, _claim):
        yield {
            "rowNumber": 1,
            "sourceItemId": "path:one",
            "outcome": "CreatedOccurrence",
        }

    def complete_result(self, _claim, **kwargs):
        self.completed = kwargs

    def fail(self, _claim, **kwargs):
        self.failures.append(kwargs)
        return bool(kwargs["retryable"])


class FakeStore:
    def __init__(self, source: Path) -> None:
        self.source = source
        self.download_bounds: tuple[int, int] | None = None
        self.upload: dict[str, object] | None = None

    def download(self, *, destination: Path, expected_bytes: int, maximum_bytes: int, **_kwargs):
        self.download_bounds = (expected_bytes, maximum_bytes)
        shutil.copyfile(self.source, destination)

    def upload_result(self, *, source: Path, **kwargs):
        self.upload = {**kwargs, "content": source.read_bytes()}


class FakeJobDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[UUID, ...], str]] = []

    def dispatch(self, *, job_ids, job_type):
        self.calls.append((tuple(job_ids), job_type))


def test_processor_runs_verified_stage_merge_and_deterministic_result(tmp_path: Path):
    source = tmp_path / "input.gz"
    byte_size, checksum = _artifact(source)
    claim = _claim(byte_size, checksum)
    repository = FakeRepository(claim)
    store = FakeStore(source)
    dispatcher = FakeJobDispatcher()
    processor = BulkManifestProcessor(
        repository=repository,  # type: ignore[arg-type]
        object_store=store,
        settings=BulkProcessorSettings(
            result_bucket="media-bucket", work_root=tmp_path
        ),
        job_dispatcher=dispatcher,
    )

    disposition = processor.process(import_id=IMPORT_ID, message_id="message-one")

    assert disposition is BulkMessageDisposition.ACK
    assert repository.loaded == 1
    assert repository.merged == 1
    assert repository.phases == ["Merging", "WritingResult"]
    assert repository.merge_settings == [MergeSettings()]
    assert repository.geocode_batch_queries == 0
    assert dispatcher.calls == []
    assert repository.completed is not None
    assert store.download_bounds == (byte_size, 256 * 1024 * 1024)
    assert store.upload is not None
    assert store.upload["object_key"] == (
        f"manifests/result/{USER_ID}/{SOURCE_ID}/{IMPORT_ID}.ndjson.gz"
    )


def test_explicit_bulk_enrichment_opt_in_dispatches_geocode_jobs(tmp_path: Path):
    source = tmp_path / "input-opt-in.gz"
    byte_size, checksum = _artifact(source)
    repository = FakeRepository(_claim(byte_size, checksum))
    dispatcher = FakeJobDispatcher()
    processor = BulkManifestProcessor(
        repository=repository,  # type: ignore[arg-type]
        object_store=FakeStore(source),
        settings=BulkProcessorSettings(
            result_bucket="media-bucket",
            work_root=tmp_path,
            merge=MergeSettings(enqueue_enrichment_jobs=True),
        ),
        job_dispatcher=dispatcher,
    )

    disposition = processor.process(import_id=IMPORT_ID, message_id="message-one")

    assert disposition is BulkMessageDisposition.ACK
    assert repository.merge_settings == [
        MergeSettings(enqueue_enrichment_jobs=True)
    ]
    assert repository.geocode_batch_queries == 1
    assert dispatcher.calls == [
        ((UUID("00000000-0000-0000-0000-000000000405"),), "Geocode")
    ]


def test_claim_time_database_failure_requests_sqs_retry(tmp_path: Path):
    class FailingRepository(FakeRepository):
        def claim(self, **_kwargs):
            raise BulkImportDatabaseError("DatabaseUnavailable", "Temporarily unavailable")

    processor = BulkManifestProcessor(
        repository=FailingRepository(None),  # type: ignore[arg-type]
        object_store=FakeStore(tmp_path / "unused"),
        settings=BulkProcessorSettings(result_bucket="media-bucket"),
    )

    assert processor.process(
        import_id=IMPORT_ID, message_id="message-one"
    ) is BulkMessageDisposition.RETRY


def test_s3_download_stops_before_declared_size_is_exceeded(tmp_path: Path):
    class Body:
        def __init__(self):
            self.calls = 0

        def read(self, _size):
            self.calls += 1
            return b"abcdef" if self.calls == 1 else b""

    class Client:
        def get_object(self, **_kwargs):
            return {"Body": Body()}

    store = S3ManifestObjectStore(Client())

    try:
        store.download(
            bucket="bucket",
            object_key="key",
            version_id=None,
            destination=tmp_path / "object",
            expected_bytes=5,
            maximum_bytes=10,
        )
    except Exception as exc:
        assert getattr(exc, "code", None) == "ManifestCompressedLimitExceeded"
    else:  # pragma: no cover - defensive
        raise AssertionError("Oversized S3 body was accepted")


def test_handler_reports_only_retryable_records():
    class Router:
        def process_message(self, *, message_id, body):
            del body
            return (
                BulkMessageDisposition.RETRY
                if message_id == "retry"
                else BulkMessageDisposition.ACK
            )

    response = handler(
        {
            "Records": [
                {"messageId": "ok", "body": "{}"},
                {"messageId": "retry", "body": "{}"},
            ]
        },
        None,
        processor=Router(),
    )

    assert response == {"batchItemFailures": [{"itemIdentifier": "retry"}]}
