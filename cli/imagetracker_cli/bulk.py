from __future__ import annotations

from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
from typing import Any, Iterator, Mapping, Sequence
from uuid import UUID
import zlib


MANIFEST_SCHEMA_VERSION = "ManifestNdjsonV1"
RESULT_SCHEMA_VERSION = "ManifestResultNdjsonV1"
MAX_COMPRESSED_BYTES = 256 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024
MAX_LINE_BYTES = 256 * 1024
MAX_ENTRIES = 250_000
MAX_IN_MEMORY_RESULT_ENTRIES = 10_000
_HEX_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_RESULT_OUTCOMES = {
    "CreatedOccurrence",
    "UpdatedOccurrence",
    "DuplicateLinked",
    "DeletedOccurrence",
    "IgnoredDeletion",
    "Unchanged",
    "Rejected",
}


class BulkArtifactError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BulkManifestArtifact:
    path: Path
    source_id: str
    snapshot_id: str
    entry_count: int
    compressed_bytes: int
    compressed_sha256: str
    uncompressed_bytes: int
    schema_version: str = MANIFEST_SCHEMA_VERSION


@dataclass(frozen=True)
class BulkResultEntry:
    row_number: int
    source_item_id: str
    outcome: str
    occurrence_id: str | None
    media_asset_id: str | None
    description_job_id: str | None
    upload_required: bool
    error_code: str | None
    error_message: str | None

    def as_api_result(self) -> dict[str, Any]:
        return {
            "sourceItemId": self.source_item_id,
            "outcome": self.outcome,
            "occurrenceId": self.occurrence_id,
            "mediaAssetId": self.media_asset_id,
            "descriptionJobId": self.description_job_id,
            "uploadRequired": self.upload_required,
            "errorCode": self.error_code,
            "errorMessage": self.error_message,
        }


@dataclass(frozen=True)
class BulkResultHeader:
    path: Path
    import_id: str
    counts: Mapping[str, int]
    entry_count: int
    compressed_bytes: int
    compressed_sha256: str
    uncompressed_bytes: int
    file_modified_ns: int
    source_cursor: str | None = None
    accepted_at_utc: str | None = None
    schema_version: str = RESULT_SCHEMA_VERSION


@dataclass(frozen=True)
class BulkManifestResult:
    path: Path
    import_id: str
    counts: Mapping[str, int]
    entries: tuple[BulkResultEntry, ...]
    compressed_bytes: int
    compressed_sha256: str
    uncompressed_bytes: int
    source_cursor: str | None = None
    accepted_at_utc: str | None = None
    schema_version: str = RESULT_SCHEMA_VERSION


def _uuid_text(value: UUID | str, field: str) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise BulkArtifactError(
            "BulkIdentifierInvalid",
            f"{field} must be a UUID.",
        ) from exc


def _json_line(value: Mapping[str, Any], *, line_number: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise BulkArtifactError(
            "BulkRecordInvalid",
            f"Bulk manifest line {line_number} is not JSON serializable.",
        ) from exc
    if len(encoded) > MAX_LINE_BYTES:
        raise BulkArtifactError(
            "BulkLineLimitExceeded",
            f"Bulk manifest line {line_number} exceeds {MAX_LINE_BYTES:,} bytes.",
        )
    return encoded


def _manifest_entry(value: Mapping[str, Any], row_number: int) -> dict[str, Any]:
    if value.get("operation") != "Upsert":
        raise BulkArtifactError(
            "BulkOperationUnsupported",
            "ManifestNdjsonV1 accepts hash-enriched Upsert entries only.",
        )
    content_hash = value.get("contentSha256")
    if not isinstance(content_hash, str) or not _HEX_SHA256.fullmatch(content_hash):
        raise BulkArtifactError(
            "BulkContentHashRequired",
            "ManifestNdjsonV1 requires a SHA-256 hash for every entry.",
        )
    if "recordType" in value or "rowNumber" in value:
        raise BulkArtifactError(
            "BulkReservedField",
            "Manifest entries cannot contain recordType or rowNumber.",
        )
    return {
        "recordType": "Entry",
        "rowNumber": row_number,
        **dict(value),
        "contentSha256": content_hash.lower(),
    }


def write_manifest_gzip(
    output_path: Path,
    *,
    source_id: UUID | str,
    snapshot_id: UUID | str,
    entries: Sequence[Mapping[str, Any]],
    permission_state: str = "NotApplicable",
    client_cursor: str | None = None,
) -> BulkManifestArtifact:
    """Atomically write one deterministic Local Full manifest artifact."""

    entry_count = len(entries)
    if not 1 <= entry_count <= MAX_ENTRIES:
        raise BulkArtifactError(
            "BulkEntryCountInvalid",
            f"Bulk entry count must be between 1 and {MAX_ENTRIES:,}.",
        )
    if permission_state not in {
        "NotApplicable",
        "Full",
        "Limited",
        "Denied",
        "Unavailable",
    }:
        raise BulkArtifactError(
            "BulkPermissionStateInvalid",
            "Bulk permission state is invalid.",
        )
    if client_cursor is not None and (
        not isinstance(client_cursor, str)
        or len(client_cursor) > 1024
        or "\x00" in client_cursor
    ):
        raise BulkArtifactError(
            "BulkClientCursorInvalid",
            "Bulk client cursor must contain at most 1024 characters.",
        )

    source_text = _uuid_text(source_id, "sourceId")
    snapshot_text = _uuid_text(snapshot_id, "snapshotId")
    selected = output_path.expanduser().resolve(strict=False)
    selected.parent.mkdir(parents=True, exist_ok=True)
    partial = selected.with_name(f".{selected.name}.{os.getpid()}.part")
    uncompressed_bytes = 0
    try:
        with partial.open("wb") as raw:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                compresslevel=6,
                fileobj=raw,
                mtime=0,
            ) as archive:
                header = {
                    "recordType": "Manifest",
                    "schemaVersion": MANIFEST_SCHEMA_VERSION,
                    "storageMode": "Local",
                    "sourceId": source_text,
                    "snapshotId": snapshot_text,
                    "entryCount": entry_count,
                    "kind": "Full",
                    "permissionState": permission_state,
                    "deletionDetectionReliable": False,
                    "clientCursor": client_cursor,
                }
                line = _json_line(header, line_number=1)
                archive.write(line)
                uncompressed_bytes += len(line)
                for row_number, entry in enumerate(entries, start=1):
                    line = _json_line(
                        _manifest_entry(entry, row_number),
                        line_number=row_number + 1,
                    )
                    uncompressed_bytes += len(line)
                    if uncompressed_bytes > MAX_UNCOMPRESSED_BYTES:
                        raise BulkArtifactError(
                            "BulkUncompressedLimitExceeded",
                            "The expanded bulk manifest exceeds the 1 GiB limit.",
                        )
                    archive.write(line)

        compressed_bytes = partial.stat().st_size
        if compressed_bytes > MAX_COMPRESSED_BYTES:
            raise BulkArtifactError(
                "BulkCompressedLimitExceeded",
                "The compressed bulk manifest exceeds the 256 MiB limit.",
            )
        compressed_sha256 = _file_sha256(partial)
        os.replace(partial, selected)
        if os.name != "nt":
            selected.chmod(0o600)
        return BulkManifestArtifact(
            path=selected,
            source_id=source_text,
            snapshot_id=snapshot_text,
            entry_count=entry_count,
            compressed_bytes=compressed_bytes,
            compressed_sha256=compressed_sha256,
            uncompressed_bytes=uncompressed_bytes,
        )
    except BaseException:
        partial.unlink(missing_ok=True)
        raise


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _BoundedGzipLines:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.uncompressed_bytes = 0

    def __iter__(self) -> Iterator[tuple[int, bytes]]:
        try:
            with gzip.open(self.path, "rb") as archive:
                line_number = 0
                while True:
                    line = archive.readline(MAX_LINE_BYTES + 1)
                    if not line:
                        return
                    line_number += 1
                    if len(line) > MAX_LINE_BYTES:
                        raise BulkArtifactError(
                            "BulkResultLineLimitExceeded",
                            f"Bulk result line {line_number} exceeds the limit.",
                        )
                    if not line.endswith(b"\n"):
                        raise BulkArtifactError(
                            "BulkResultLineTerminatorMissing",
                            f"Bulk result line {line_number} is not newline terminated.",
                        )
                    self.uncompressed_bytes += len(line)
                    if self.uncompressed_bytes > MAX_UNCOMPRESSED_BYTES:
                        raise BulkArtifactError(
                            "BulkResultExpandedLimitExceeded",
                            "The expanded bulk result exceeds the 1 GiB limit.",
                        )
                    yield line_number, line[:-1]
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            raise BulkArtifactError(
                "BulkResultGzipInvalid",
                "The bulk result is not a complete valid gzip stream.",
            ) from exc


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BulkArtifactError(
                "BulkResultDuplicateJsonKey",
                "A bulk result record contains a duplicate JSON field.",
            )
        result[key] = value
    return result


def _result_json(raw: bytes, line_number: int) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(text, object_pairs_hook=_unique_object)
    except BulkArtifactError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BulkArtifactError(
            "BulkResultJsonInvalid",
            f"Bulk result line {line_number} is not valid UTF-8 JSON.",
        ) from exc
    if not isinstance(value, dict):
        raise BulkArtifactError(
            "BulkResultRecordInvalid",
            f"Bulk result line {line_number} must be an object.",
        )
    return value


def _optional_result_uuid(value: Any, name: str) -> str | None:
    if value is None:
        return None
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise BulkArtifactError(
            "BulkResultEntryInvalid",
            f"Bulk result {name} must be a UUID when present.",
        ) from exc


def parse_result_gzip(
    input_path: Path,
    *,
    expected_sha256: str,
    expected_compressed_bytes: int,
    expected_import_id: UUID | str | None = None,
    max_entries: int = MAX_IN_MEMORY_RESULT_ENTRIES,
) -> BulkManifestResult:
    """Parse a small result in memory; production callers should stream it."""

    if not 1 <= max_entries <= MAX_ENTRIES:
        raise ValueError(f"max_entries must be between 1 and {MAX_ENTRIES:,}")

    selected = input_path.expanduser().resolve(strict=True)
    if not selected.is_file():
        raise BulkArtifactError("BulkResultMissing", "The bulk result file is missing.")
    if not _HEX_SHA256.fullmatch(expected_sha256):
        raise BulkArtifactError(
            "BulkResultChecksumInvalid",
            "The expected bulk result checksum is invalid.",
        )
    if not 0 < expected_compressed_bytes <= MAX_COMPRESSED_BYTES:
        raise BulkArtifactError(
            "BulkResultSizeInvalid",
            "The declared bulk result size is invalid.",
        )
    if selected.stat().st_size != expected_compressed_bytes:
        raise BulkArtifactError(
            "BulkResultSizeMismatch",
            "The bulk result size does not match its declaration.",
        )
    actual_sha256 = _file_sha256(selected)
    if actual_sha256.lower() != expected_sha256.lower():
        raise BulkArtifactError(
            "BulkResultChecksumMismatch",
            "The bulk result checksum does not match its declaration.",
        )

    expected_import = (
        _uuid_text(expected_import_id, "importId")
        if expected_import_id is not None
        else None
    )
    lines = _BoundedGzipLines(selected)
    import_id: str | None = None
    counts: dict[str, int] = {}
    source_cursor: str | None = None
    accepted_at_utc: str | None = None
    entries: list[BulkResultEntry] = []
    for line_number, raw in lines:
        record = _result_json(raw, line_number)
        if line_number == 1:
            if (
                record.get("recordType") != "Result"
                or record.get("schemaVersion") != RESULT_SCHEMA_VERSION
            ):
                raise BulkArtifactError(
                    "BulkResultHeaderInvalid",
                    "The bulk result header is missing or unsupported.",
                )
            import_id = _uuid_text(record.get("importId"), "importId")
            if expected_import is not None and import_id != expected_import:
                raise BulkArtifactError(
                    "BulkResultImportMismatch",
                    "The bulk result belongs to a different import.",
                )
            raw_counts = record.get("counts")
            if not isinstance(raw_counts, dict):
                raise BulkArtifactError(
                    "BulkResultHeaderInvalid",
                    "Bulk result counts must be an object.",
                )
            for key, value in raw_counts.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                ):
                    raise BulkArtifactError(
                        "BulkResultHeaderInvalid",
                        "Bulk result counts must be non-negative integers.",
                    )
                counts[key] = value
            cursor = record.get("sourceCursor")
            if cursor is not None and not isinstance(cursor, str):
                raise BulkArtifactError(
                    "BulkResultHeaderInvalid",
                    "Bulk result sourceCursor must be a string when present.",
                )
            source_cursor = cursor
            accepted = record.get("acceptedAtUtc")
            if accepted is not None and not isinstance(accepted, str):
                raise BulkArtifactError(
                    "BulkResultHeaderInvalid",
                    "Bulk result acceptedAtUtc must be a string when present.",
                )
            accepted_at_utc = accepted
            continue

        if import_id is None or record.get("recordType") != "EntryResult":
            raise BulkArtifactError(
                "BulkResultRecordInvalid",
                f"Bulk result line {line_number} is not an EntryResult.",
            )
        row_number = record.get("rowNumber")
        expected_row = len(entries) + 1
        if (
            isinstance(row_number, bool)
            or not isinstance(row_number, int)
            or row_number != expected_row
        ):
            raise BulkArtifactError(
                "BulkResultRowSequenceInvalid",
                "Bulk result row numbers must be contiguous and start at one.",
            )
        source_item_id = record.get("sourceItemId")
        outcome = record.get("outcome")
        if (
            not isinstance(source_item_id, str)
            or not source_item_id
            or len(source_item_id) > 512
            or outcome not in _RESULT_OUTCOMES
        ):
            raise BulkArtifactError(
                "BulkResultEntryInvalid",
                f"Bulk result row {row_number} has invalid identity fields.",
            )
        upload_required = record.get("uploadRequired", False)
        if not isinstance(upload_required, bool):
            raise BulkArtifactError(
                "BulkResultEntryInvalid",
                f"Bulk result row {row_number} has an invalid uploadRequired value.",
            )
        error_code = record.get("errorCode")
        error_message = record.get("errorMessage")
        if error_code is not None and not isinstance(error_code, str):
            raise BulkArtifactError(
                "BulkResultEntryInvalid",
                f"Bulk result row {row_number} has an invalid errorCode.",
            )
        if error_message is not None and not isinstance(error_message, str):
            raise BulkArtifactError(
                "BulkResultEntryInvalid",
                f"Bulk result row {row_number} has an invalid errorMessage.",
            )
        entries.append(
            BulkResultEntry(
                row_number=row_number,
                source_item_id=source_item_id,
                outcome=str(outcome),
                occurrence_id=_optional_result_uuid(
                    record.get("occurrenceId"), "occurrenceId"
                ),
                media_asset_id=_optional_result_uuid(
                    record.get("mediaAssetId"), "mediaAssetId"
                ),
                description_job_id=_optional_result_uuid(
                    record.get("descriptionJobId"), "descriptionJobId"
                ),
                upload_required=upload_required,
                error_code=error_code,
                error_message=error_message,
            )
        )
        if len(entries) > max_entries:
            raise BulkArtifactError(
                "BulkResultMemoryLimitExceeded",
                "Use read_result_header and iter_result_entries for this result.",
            )

    if import_id is None:
        raise BulkArtifactError(
            "BulkResultHeaderMissing",
            "The bulk result header is missing.",
        )
    if len(entries) == 0:
        raise BulkArtifactError(
            "BulkResultEntryCountInvalid",
            "The bulk result must contain at least one entry row.",
        )
    return BulkManifestResult(
        path=selected,
        import_id=import_id,
        counts=counts,
        entries=tuple(entries),
        compressed_bytes=expected_compressed_bytes,
        compressed_sha256=actual_sha256,
        uncompressed_bytes=lines.uncompressed_bytes,
        source_cursor=source_cursor,
        accepted_at_utc=accepted_at_utc,
    )


def _verified_result_path(
    input_path: Path,
    *,
    expected_sha256: str,
    expected_compressed_bytes: int,
) -> tuple[Path, str, os.stat_result]:
    selected = input_path.expanduser().resolve(strict=False)
    if not selected.is_file():
        raise BulkArtifactError("BulkResultMissing", "The bulk result file is missing.")
    if not _HEX_SHA256.fullmatch(expected_sha256):
        raise BulkArtifactError(
            "BulkResultChecksumInvalid",
            "The expected bulk result checksum is invalid.",
        )
    before = selected.stat()
    if (
        not 0 < expected_compressed_bytes <= MAX_COMPRESSED_BYTES
        or before.st_size != expected_compressed_bytes
    ):
        raise BulkArtifactError(
            "BulkResultSizeMismatch",
            "The bulk result size does not match its declaration.",
        )
    actual_sha256 = _file_sha256(selected)
    after = selected.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise BulkArtifactError(
            "BulkResultChanged",
            "The bulk result changed while it was being verified.",
        )
    if actual_sha256.lower() != expected_sha256.lower():
        raise BulkArtifactError(
            "BulkResultChecksumMismatch",
            "The bulk result checksum does not match its declaration.",
        )
    return selected, actual_sha256, after


def _stream_header_values(
    record: Mapping[str, Any],
    *,
    expected_import_id: str | None,
) -> tuple[str, dict[str, int], str | None, str | None]:
    if (
        record.get("recordType") != "Result"
        or record.get("schemaVersion") != RESULT_SCHEMA_VERSION
    ):
        raise BulkArtifactError(
            "BulkResultHeaderInvalid",
            "The bulk result header is missing or unsupported.",
        )
    import_id = _uuid_text(record.get("importId"), "importId")
    if expected_import_id is not None and import_id != expected_import_id:
        raise BulkArtifactError(
            "BulkResultImportMismatch",
            "The bulk result belongs to a different import.",
        )
    raw_counts = record.get("counts")
    if not isinstance(raw_counts, dict):
        raise BulkArtifactError(
            "BulkResultHeaderInvalid",
            "Bulk result counts must be an object.",
        )
    counts: dict[str, int] = {}
    for key, value in raw_counts.items():
        if (
            not isinstance(key, str)
            or not key
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise BulkArtifactError(
                "BulkResultHeaderInvalid",
                "Bulk result counts must be non-negative integers.",
            )
        counts[key] = value
    source_cursor = record.get("sourceCursor")
    accepted_at_utc = record.get("acceptedAtUtc")
    if source_cursor is not None and not isinstance(source_cursor, str):
        raise BulkArtifactError(
            "BulkResultHeaderInvalid",
            "Bulk result sourceCursor must be a string when present.",
        )
    if accepted_at_utc is not None and not isinstance(accepted_at_utc, str):
        raise BulkArtifactError(
            "BulkResultHeaderInvalid",
            "Bulk result acceptedAtUtc must be a string when present.",
        )
    return import_id, counts, source_cursor, accepted_at_utc


def _stream_result_entry(
    record: Mapping[str, Any],
    *,
    expected_row: int,
) -> BulkResultEntry:
    if record.get("recordType") != "EntryResult":
        raise BulkArtifactError(
            "BulkResultRecordInvalid",
            f"Bulk result row {expected_row} is not an EntryResult.",
        )
    row_number = record.get("rowNumber")
    source_item_id = record.get("sourceItemId")
    outcome = record.get("outcome")
    if (
        isinstance(row_number, bool)
        or not isinstance(row_number, int)
        or row_number != expected_row
    ):
        raise BulkArtifactError(
            "BulkResultRowSequenceInvalid",
            "Bulk result row numbers must be contiguous and start at one.",
        )
    if (
        not isinstance(source_item_id, str)
        or not source_item_id
        or len(source_item_id) > 512
        or outcome not in _RESULT_OUTCOMES
    ):
        raise BulkArtifactError(
            "BulkResultEntryInvalid",
            f"Bulk result row {row_number} has invalid identity fields.",
        )
    upload_required = record.get("uploadRequired", False)
    error_code = record.get("errorCode")
    error_message = record.get("errorMessage")
    if not isinstance(upload_required, bool):
        raise BulkArtifactError(
            "BulkResultEntryInvalid",
            f"Bulk result row {row_number} has invalid uploadRequired.",
        )
    if error_code is not None and not isinstance(error_code, str):
        raise BulkArtifactError(
            "BulkResultEntryInvalid",
            f"Bulk result row {row_number} has an invalid errorCode.",
        )
    if error_message is not None and not isinstance(error_message, str):
        raise BulkArtifactError(
            "BulkResultEntryInvalid",
            f"Bulk result row {row_number} has an invalid errorMessage.",
        )
    return BulkResultEntry(
        row_number=row_number,
        source_item_id=source_item_id,
        outcome=str(outcome),
        occurrence_id=_optional_result_uuid(record.get("occurrenceId"), "occurrenceId"),
        media_asset_id=_optional_result_uuid(record.get("mediaAssetId"), "mediaAssetId"),
        description_job_id=_optional_result_uuid(
            record.get("descriptionJobId"), "descriptionJobId"
        ),
        upload_required=upload_required,
        error_code=error_code,
        error_message=error_message,
    )


def read_result_header(
    input_path: Path,
    *,
    expected_sha256: str,
    expected_compressed_bytes: int,
    expected_import_id: UUID | str | None = None,
) -> BulkResultHeader:
    """Fully verify a result once without retaining its entry rows."""

    selected, actual_sha256, stat = _verified_result_path(
        input_path,
        expected_sha256=expected_sha256,
        expected_compressed_bytes=expected_compressed_bytes,
    )
    expected_import = (
        _uuid_text(expected_import_id, "importId")
        if expected_import_id is not None
        else None
    )
    lines = _BoundedGzipLines(selected)
    import_id: str | None = None
    counts: dict[str, int] = {}
    source_cursor: str | None = None
    accepted_at_utc: str | None = None
    entry_count = 0
    for line_number, raw in lines:
        record = _result_json(raw, line_number)
        if line_number == 1:
            (
                import_id,
                counts,
                source_cursor,
                accepted_at_utc,
            ) = _stream_header_values(
                record,
                expected_import_id=expected_import,
            )
            continue
        if import_id is None:
            raise BulkArtifactError(
                "BulkResultHeaderMissing",
                "The bulk result header is missing.",
            )
        entry_count += 1
        if entry_count > MAX_ENTRIES:
            raise BulkArtifactError(
                "BulkResultEntryLimitExceeded",
                f"The bulk result exceeds {MAX_ENTRIES:,} entries.",
            )
        _stream_result_entry(record, expected_row=entry_count)
    if import_id is None:
        raise BulkArtifactError(
            "BulkResultHeaderMissing",
            "The bulk result header is missing.",
        )
    if entry_count == 0:
        raise BulkArtifactError(
            "BulkResultEntryCountInvalid",
            "The bulk result must contain at least one entry row.",
        )
    final_stat = selected.stat()
    if (
        final_stat.st_size != stat.st_size
        or final_stat.st_mtime_ns != stat.st_mtime_ns
    ):
        raise BulkArtifactError(
            "BulkResultChanged",
            "The bulk result changed while it was being parsed.",
        )
    return BulkResultHeader(
        path=selected,
        import_id=import_id,
        counts=counts,
        entry_count=entry_count,
        compressed_bytes=expected_compressed_bytes,
        compressed_sha256=actual_sha256,
        uncompressed_bytes=lines.uncompressed_bytes,
        file_modified_ns=final_stat.st_mtime_ns,
        source_cursor=source_cursor,
        accepted_at_utc=accepted_at_utc,
    )


def iter_result_entries(
    header: BulkResultHeader,
    *,
    after_row_number: int = 0,
) -> Iterator[BulkResultEntry]:
    """Stream verified result rows after the durable SQLite resume cursor."""

    if (
        isinstance(after_row_number, bool)
        or not isinstance(after_row_number, int)
        or not 0 <= after_row_number <= header.entry_count
    ):
        raise ValueError("after_row_number is outside the verified result")
    stat = header.path.stat()
    if (
        stat.st_size != header.compressed_bytes
        or stat.st_mtime_ns != header.file_modified_ns
    ):
        raise BulkArtifactError(
            "BulkResultChanged",
            "The verified bulk result changed before it could be applied.",
        )
    lines = _BoundedGzipLines(header.path)
    observed_rows = 0
    for line_number, raw in lines:
        record = _result_json(raw, line_number)
        if line_number == 1:
            import_id, counts, source_cursor, accepted_at_utc = _stream_header_values(
                record,
                expected_import_id=header.import_id,
            )
            if (
                import_id != header.import_id
                or counts != dict(header.counts)
                or source_cursor != header.source_cursor
                or accepted_at_utc != header.accepted_at_utc
            ):
                raise BulkArtifactError(
                    "BulkResultChanged",
                    "The verified bulk result header changed before application.",
                )
            continue
        observed_rows += 1
        entry = _stream_result_entry(record, expected_row=observed_rows)
        if entry.row_number > after_row_number:
            yield entry
    if observed_rows != header.entry_count:
        raise BulkArtifactError(
            "BulkResultChanged",
            "The verified bulk result row count changed before application.",
        )
