from __future__ import annotations

import base64
from dataclasses import dataclass
from enum import Enum
import logging
from pathlib import Path
import tempfile
from typing import Any, Protocol
from uuid import UUID

from services.bulk.manifest import (
    BulkManifestError,
    ManifestGuardrails,
    parse_manifest_gzip,
    write_result_gzip,
)
from services.bulk.repository import (
    BulkImportDatabaseError,
    ManifestImportClaim,
    MergeResult,
    MergeSettings,
    MySqlManifestImportRepository,
)


class BulkMessageDisposition(str, Enum):
    ACK = "Ack"
    RETRY = "Retry"


class ManifestObjectStore(Protocol):
    def download(
        self,
        *,
        bucket: str,
        object_key: str,
        version_id: str | None,
        destination: Path,
        expected_bytes: int,
        maximum_bytes: int,
    ) -> None: ...

    def upload_result(
        self,
        *,
        source: Path,
        bucket: str,
        object_key: str,
        checksum_sha256: str,
    ) -> None: ...


class S3ManifestObjectStore:
    """Small checksum-aware S3 boundary for bulk input and result artifacts."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def download(
        self,
        *,
        bucket: str,
        object_key: str,
        version_id: str | None,
        destination: Path,
        expected_bytes: int,
        maximum_bytes: int,
    ) -> None:
        request: dict[str, str] = {"Bucket": bucket, "Key": object_key}
        if version_id:
            request["VersionId"] = version_id
        response = self._client.get_object(**request)
        content_length = response.get("ContentLength")
        if content_length is not None and int(content_length) != expected_bytes:
            raise BulkManifestError(
                "ManifestSizeMismatch",
                "The staged manifest size does not match its upload declaration.",
            )
        body = response["Body"]
        written = 0
        with destination.open("wb") as handle:
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > expected_bytes or written > maximum_bytes:
                    raise BulkManifestError(
                        "ManifestCompressedLimitExceeded",
                        "The staged manifest exceeded its permitted compressed size.",
                    )
                handle.write(chunk)
        if written != expected_bytes:
            raise BulkManifestError(
                "ManifestSizeMismatch",
                "The staged manifest size does not match its upload declaration.",
            )

    def upload_result(
        self,
        *,
        source: Path,
        bucket: str,
        object_key: str,
        checksum_sha256: str,
    ) -> None:
        checksum_base64 = base64.b64encode(bytes.fromhex(checksum_sha256)).decode(
            "ascii"
        )
        with source.open("rb") as handle:
            self._client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=handle,
                ContentType="application/x-ndjson",
                ContentEncoding="gzip",
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_base64,
            )


@dataclass(frozen=True)
class BulkProcessorSettings:
    result_bucket: str
    input_prefix: str = "manifests/input"
    result_prefix: str = "manifests/result"
    lease_seconds: int = 1200
    guardrails: ManifestGuardrails = ManifestGuardrails()
    merge: MergeSettings = MergeSettings()
    work_root: Path | None = None

    def __post_init__(self) -> None:
        if not self.result_bucket or len(self.result_bucket) > 63:
            raise ValueError("A valid result bucket is required")
        for prefix in (self.input_prefix, self.result_prefix):
            if not prefix or prefix.startswith("/") or ".." in prefix.split("/"):
                raise ValueError("Relative manifest prefixes are required")
        if not 60 <= self.lease_seconds <= 3600:
            raise ValueError("Bulk import leases must be between 60 and 3600 seconds")


class BulkManifestProcessor:
    def __init__(
        self,
        *,
        repository: MySqlManifestImportRepository,
        object_store: ManifestObjectStore,
        settings: BulkProcessorSettings,
        job_dispatcher: Any | None = None,
    ) -> None:
        self._repository = repository
        self._object_store = object_store
        self._settings = settings
        self._job_dispatcher = job_dispatcher

    def process(
        self, *, import_id: UUID | str, message_id: str
    ) -> BulkMessageDisposition:
        try:
            claim = self._repository.claim(
                import_id=import_id,
                lease_owner=message_id,
                lease_seconds=self._settings.lease_seconds,
            )
        except BulkImportDatabaseError as exc:
            return (
                BulkMessageDisposition.ACK
                if exc.code in {"BulkSourceUnavailable", "BulkImportOwnerMissing"}
                else BulkMessageDisposition.RETRY
            )
        except Exception:
            return BulkMessageDisposition.RETRY
        if claim is None:
            return BulkMessageDisposition.ACK
        try:
            return self._process_claim(claim)
        except BulkManifestError as exc:
            self._repository.fail(
                claim,
                failure_class="InvalidManifest",
                code=exc.code,
                message=exc.safe_message,
                retryable=False,
            )
            return BulkMessageDisposition.ACK
        except BulkImportDatabaseError as exc:
            if exc.code == "BulkImportLeaseLost":
                return BulkMessageDisposition.ACK
            retryable = exc.code not in {
                "ManifestSourceMismatch",
                "ManifestSnapshotMismatch",
                "ManifestEntryCountMismatch",
                "ManifestLoadMismatch",
                "BulkImportSchemaUnsupported",
                "BulkImportSemanticsUnsupported",
                "BulkSourceUnavailable",
                "BulkRelinkUnsupported",
            }
            should_retry = self._repository.fail(
                claim,
                failure_class="Database" if retryable else "InvalidManifest",
                code=exc.code,
                message=exc.safe_message,
                retryable=retryable,
            )
            return (
                BulkMessageDisposition.RETRY
                if should_retry
                else BulkMessageDisposition.ACK
            )
        except Exception:
            should_retry = self._repository.fail(
                claim,
                failure_class="Transient",
                code="BulkImportUnexpectedFailure",
                message="The bulk import encountered a temporary internal failure.",
                retryable=True,
            )
            return (
                BulkMessageDisposition.RETRY
                if should_retry
                else BulkMessageDisposition.ACK
            )

    def _process_claim(self, claim: ManifestImportClaim) -> BulkMessageDisposition:
        self._validate_object_ownership(claim)
        work_root = self._settings.work_root
        if work_root is not None:
            work_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="imagetracker-bulk-",
            dir=str(work_root) if work_root is not None else None,
        ) as temporary:
            directory = Path(temporary)
            counts: dict[str, int]
            if claim.phase not in {"Staged", "Merging", "Merged", "WritingResult"}:
                compressed = directory / "manifest.ndjson.gz"
                canonical = directory / "manifest.csv"
                self._object_store.download(
                    bucket=claim.input_bucket,
                    object_key=claim.input_object_key,
                    version_id=claim.input_version_id,
                    destination=compressed,
                    expected_bytes=claim.input_byte_size,
                    maximum_bytes=self._settings.guardrails.max_compressed_bytes,
                )
                parsed = parse_manifest_gzip(
                    compressed,
                    canonical,
                    expected_sha256=claim.input_sha256,
                    expected_compressed_bytes=claim.input_byte_size,
                    expected_entry_count=claim.declared_entry_count,
                    guardrails=self._settings.guardrails,
                )
                self._repository.load_stage(claim, parsed)
                self._repository.set_phase(
                    claim,
                    phase="Merging",
                    allowed_phases={"Staged", "Merging"},
                )
                merge = self._repository.merge(claim, settings=self._settings.merge)
                counts = merge.as_counts()
                self._dispatch_geocode_jobs(claim)
            elif claim.phase in {"Staged", "Merging"}:
                self._repository.set_phase(
                    claim,
                    phase="Merging",
                    allowed_phases={"Staged", "Merging"},
                )
                merge = self._repository.merge(claim, settings=self._settings.merge)
                counts = merge.as_counts()
                self._dispatch_geocode_jobs(claim)
            else:
                merge = self._repository.current_result(claim)
                counts = merge.as_counts()

            self._repository.set_phase(
                claim,
                phase="WritingResult",
                allowed_phases={"Merged", "WritingResult"},
            )
            result_path = directory / "result.ndjson.gz"
            (
                result_bytes,
                result_sha256,
                result_rows,
                result_uncompressed_bytes,
            ) = write_result_gzip(
                result_path,
                import_id=claim.public_id,
                counts=counts,
                rows=self._repository.iter_results(claim),
                max_uncompressed_bytes=(
                    self._settings.guardrails.max_uncompressed_bytes
                ),
                max_line_bytes=self._settings.guardrails.max_line_bytes,
            )
            if result_rows != claim.declared_entry_count:
                raise BulkManifestError(
                    "ManifestResultCountMismatch",
                    "The bulk result row count does not match the manifest.",
                )
            if (
                result_bytes > self._settings.guardrails.max_compressed_bytes
                or result_uncompressed_bytes
                > self._settings.guardrails.max_uncompressed_bytes
            ):
                raise BulkManifestError(
                    "ManifestResultLimitExceeded",
                    "The bulk result exceeds the supported download limit.",
                )
            result_key = (
                f"{self._settings.result_prefix.rstrip('/')}/"
                f"{claim.user_public_id}/{claim.source_public_id}/"
                f"{claim.public_id}.ndjson.gz"
            )
            self._object_store.upload_result(
                source=result_path,
                bucket=self._settings.result_bucket,
                object_key=result_key,
                checksum_sha256=result_sha256,
            )
            self._repository.complete_result(
                claim,
                bucket=self._settings.result_bucket,
                object_key=result_key,
                checksum_sha256=result_sha256,
                byte_size=result_bytes,
            )
        return BulkMessageDisposition.ACK

    def _validate_object_ownership(self, claim: ManifestImportClaim) -> None:
        expected_input = (
            f"{self._settings.input_prefix.rstrip('/')}/"
            f"{claim.user_public_id}/{claim.source_public_id}/"
            f"{claim.public_id}.ndjson.gz"
        )
        if claim.input_object_key != expected_input:
            raise BulkManifestError(
                "ManifestObjectOwnershipMismatch",
                "The staged manifest object does not belong to this import.",
            )

    def _dispatch_geocode_jobs(self, claim: ManifestImportClaim) -> None:
        if self._job_dispatcher is None:
            return
        maximum_direct_jobs = 2_000
        dispatched = 0
        try:
            for job_ids in self._repository.queued_geocode_job_batches(claim):
                remaining = maximum_direct_jobs - dispatched
                if remaining <= 0:
                    break
                selected = tuple(job_ids[:remaining])
                if not selected:
                    break
                self._job_dispatcher.dispatch(
                    job_ids=selected,
                    job_type="Geocode",
                )
                dispatched += len(selected)
                if len(selected) < len(job_ids):
                    break
            if dispatched >= maximum_direct_jobs:
                logging.getLogger(__name__).warning(
                    "Bulk geocode dispatch capped importId=%s jobs=%s",
                    claim.public_id,
                    dispatched,
                )
        except Exception as exc:
            # Jobs are already durable and RetryDueJobs will republish them.
            logging.getLogger(__name__).warning(
                "Deferred bulk geocode dispatch importId=%s errorType=%s",
                claim.public_id,
                type(exc).__name__,
            )
