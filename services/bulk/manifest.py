from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
from typing import Any, BinaryIO, Iterable, Mapping, Sequence
from uuid import UUID


MANIFEST_FORMAT = "ManifestNdjsonV1"
RESULT_FORMAT = "ManifestResultNdjsonV1"
HEX_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
PROVENANCE_SOURCES = {
    "Exif",
    "Device",
    "FileMtime",
    "Google",
    "Manual",
    "AI",
    "Legacy",
    "Unknown",
}
UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1

# These names deliberately mirror ManifestImportEntry. LOAD DATA maps them
# explicitly, so a future additive column cannot silently shift the file.
CANONICAL_CSV_COLUMNS = (
    "RowNumber",
    "OperationRaw",
    "SourceItemIdRaw",
    "SourceRevisionRaw",
    "OriginalFileNameRaw",
    "LocalLocatorRaw",
    "ContentSha256Raw",
    "MediaTypeRaw",
    "MimeTypeRaw",
    "ByteSizeRaw",
    "WidthPixelsRaw",
    "HeightPixelsRaw",
    "DurationMillisecondsRaw",
    "CaptureDateTimeLocalRaw",
    "CaptureDateTimeUtcRaw",
    "TimeZoneRaw",
    "UtcOffsetMinutesRaw",
    "LatitudeRaw",
    "LongitudeRaw",
    "AltitudeMetersRaw",
    "AccuracyMetersRaw",
    "ProvenanceJsonRaw",
    "CoordinateRevision",
    "LocationSource",
    "ValidationState",
    "ErrorCode",
    "ErrorMessage",
)


class BulkManifestError(ValueError):
    """Safe, classified failure for an entire uploaded manifest."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


@dataclass(frozen=True)
class ManifestGuardrails:
    max_compressed_bytes: int = 256 * 1024 * 1024
    max_uncompressed_bytes: int = 1024 * 1024 * 1024
    max_line_bytes: int = 256 * 1024
    max_entries: int = 250_000

    def __post_init__(self) -> None:
        if min(
            self.max_compressed_bytes,
            self.max_uncompressed_bytes,
            self.max_line_bytes,
            self.max_entries,
        ) <= 0:
            raise ValueError("Manifest guardrails must be positive")


@dataclass(frozen=True)
class ManifestHeader:
    source_id: UUID
    snapshot_id: UUID
    entry_count: int
    manifest_kind: str
    permission_state: str
    deletion_detection_reliable: bool
    client_cursor: str | None
    schema_version: str = MANIFEST_FORMAT


@dataclass(frozen=True)
class ParsedManifest:
    header: ManifestHeader
    canonical_csv_path: Path
    compressed_bytes: int
    uncompressed_bytes: int
    compressed_sha256: str
    entry_count: int
    rejected_count: int


class _HashingReader(io.RawIOBase):
    def __init__(self, raw: BinaryIO, *, maximum: int) -> None:
        self._raw = raw
        self._maximum = maximum
        self.count = 0
        self.digest = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:
        chunk = self._raw.read(len(buffer))
        if not chunk:
            return 0
        self.count += len(chunk)
        if self.count > self._maximum:
            raise BulkManifestError(
                "ManifestCompressedLimitExceeded",
                "The compressed manifest is larger than the permitted limit.",
            )
        self.digest.update(chunk)
        buffer[: len(chunk)] = chunk
        return len(chunk)


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BulkManifestError(
                "ManifestDuplicateJsonKey",
                "A manifest record contains a duplicate JSON field.",
            )
        result[key] = value
    return result


def _json_record(raw: bytes, *, row_number: int) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise BulkManifestError(
            "ManifestInvalidUtf8",
            f"Manifest line {row_number} is not valid UTF-8.",
        ) from exc
    try:
        value = json.loads(text, object_pairs_hook=_unique_object)
    except BulkManifestError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise BulkManifestError(
            "ManifestInvalidJson",
            f"Manifest line {row_number} is not a valid JSON object.",
        ) from exc
    if not isinstance(value, dict):
        raise BulkManifestError(
            "ManifestInvalidRecord",
            f"Manifest line {row_number} must be a JSON object.",
        )
    return value


def _required_string(
    value: Mapping[str, Any], name: str, *, maximum: int
) -> str:
    selected = value.get(name)
    if not isinstance(selected, str) or not selected or len(selected) > maximum:
        raise ValueError(f"{name} must contain between 1 and {maximum} characters")
    if "\x00" in selected:
        raise ValueError(f"{name} cannot contain a null character")
    return selected


def _optional_string(
    value: Mapping[str, Any], name: str, *, maximum: int
) -> str | None:
    selected = value.get(name)
    if selected is None:
        return None
    if not isinstance(selected, str) or len(selected) > maximum or "\x00" in selected:
        raise ValueError(f"{name} must contain at most {maximum} characters")
    return selected


def _integer(
    value: Any,
    name: str,
    *,
    minimum: int,
    maximum: int,
    optional: bool = False,
) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _decimal(
    value: Any,
    name: str,
    *,
    minimum: Decimal,
    maximum: Decimal,
    places: int,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{name} must be numeric")
    try:
        selected = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not selected.is_finite() or not minimum <= selected <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    quantum = Decimal(1).scaleb(-places)
    return format(selected.quantize(quantum), "f")


def _local_datetime(value: Any) -> tuple[str | None, datetime | None]:
    if value is None:
        return None, None
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("capturedAtLocal must be an ISO 8601 date-time")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("capturedAtLocal must be an ISO 8601 date-time") from exc
    if parsed.tzinfo is not None:
        raise ValueError("capturedAtLocal cannot include a time-zone offset")
    return parsed.isoformat(timespec="microseconds"), parsed


def _utc_datetime(value: Any) -> tuple[str | None, datetime | None]:
    if value is None:
        return None, None
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("capturedAtUtc must be an ISO 8601 date-time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("capturedAtUtc must be an ISO 8601 date-time") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("capturedAtUtc must include a time-zone offset")
    selected = parsed.astimezone(timezone.utc)
    return selected.isoformat(timespec="microseconds").replace("+00:00", "Z"), selected


def _provenance(value: Any) -> tuple[str, list[dict[str, Any]]]:
    if value is None:
        value = []
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError("provenance must be a bounded array")
    result: list[dict[str, Any]] = []
    known: set[tuple[str, str]] = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("provenance entries must be objects")
        field = _required_string(item, "field", maximum=100)
        source = _required_string(item, "source", maximum=32)
        if source not in PROVENANCE_SOURCES:
            raise ValueError("provenance source is unsupported")
        key = (field, source)
        if key in known:
            continue
        known.add(key)
        confidence = item.get("confidence")
        if confidence is not None:
            confidence = float(
                _decimal(
                    confidence,
                    "confidence",
                    minimum=Decimal("0"),
                    maximum=Decimal("1"),
                    places=4,
                )
            )
        processor = _optional_string(item, "processorVersion", maximum=100)
        observed = item.get("observedAtUtc")
        observed_text: str | None = None
        if observed is not None:
            observed_text, _ = _utc_datetime(observed)
        result.append(
            {
                "field": field,
                "source": source,
                "confidence": confidence,
                "processorVersion": processor,
                "observedAtUtc": observed_text,
            }
        )
    return json.dumps(result, sort_keys=True, separators=(",", ":")), result


def _header(value: Mapping[str, Any], *, guards: ManifestGuardrails) -> ManifestHeader:
    if (
        value.get("recordType") != "Manifest"
        or value.get("schemaVersion") != MANIFEST_FORMAT
    ):
        raise BulkManifestError(
            "ManifestHeaderInvalid",
            "The manifest header is missing or uses an unsupported format.",
        )
    if value.get("storageMode", "Local") != "Local":
        raise BulkManifestError(
            "BulkRemoteUnsupported",
            "ManifestNdjsonV1 currently accepts Local sources only.",
        )
    try:
        source_id = UUID(_required_string(value, "sourceId", maximum=36))
        snapshot_id = UUID(_required_string(value, "snapshotId", maximum=36))
        entry_count = _integer(
            value.get("entryCount"),
            "entryCount",
            minimum=1,
            maximum=guards.max_entries,
        )
    except (ValueError, TypeError) as exc:
        raise BulkManifestError("ManifestHeaderInvalid", str(exc)) from exc
    manifest_kind = value.get("kind")
    permission = value.get("permissionState")
    reliable = value.get("deletionDetectionReliable")
    cursor = value.get("clientCursor")
    if manifest_kind != "Full":
        raise BulkManifestError(
            "BulkManifestKindUnsupported",
            "ManifestNdjsonV1 requires a complete Full manifest.",
        )
    if permission not in {
        "NotApplicable",
        "Full",
        "Limited",
        "Denied",
        "Unavailable",
    }:
        raise BulkManifestError("ManifestHeaderInvalid", "permissionState is invalid")
    if reliable is not False:
        raise BulkManifestError(
            "BulkDeletionDetectionUnsupported",
            "ManifestNdjsonV1 does not infer deletions from missing rows.",
        )
    if cursor is not None and (
        not isinstance(cursor, str) or len(cursor) > 1024 or "\x00" in cursor
    ):
        raise BulkManifestError("ManifestHeaderInvalid", "clientCursor is invalid")
    assert isinstance(entry_count, int)
    return ManifestHeader(
        source_id=source_id,
        snapshot_id=snapshot_id,
        entry_count=entry_count,
        manifest_kind=manifest_kind,
        permission_state=permission,
        deletion_detection_reliable=False,
        client_cursor=cursor,
    )


def _raw_entry_row(value: Mapping[str, Any], row_number: int) -> tuple[list[Any], bool]:
    operation = value.get("operation")
    source_item = value.get("sourceItemId")
    revision = value.get("sourceRevision")
    base = [
        row_number,
        operation if isinstance(operation, str) else "",
        source_item if isinstance(source_item, str) else "",
        revision if isinstance(revision, str) else "",
    ]
    if operation != "Upsert":
        raise BulkManifestError(
            "UnsupportedBulkOperation",
            "ManifestNdjsonV1 accepts hash-enriched Upsert entries only; use incremental sync for deletions.",
        )
    try:
        source_item = _required_string(value, "sourceItemId", maximum=512)
        revision = _required_string(value, "sourceRevision", maximum=255)
        file_name = _required_string(value, "fileName", maximum=512)
        locator = _required_string(value, "localLocator", maximum=4096)
        content_hash = _required_string(value, "contentSha256", maximum=64).lower()
        if not HEX_SHA256.fullmatch(content_hash):
            raise ValueError("contentSha256 must contain 64 hexadecimal characters")
        media_type = _required_string(value, "mediaType", maximum=16)
        if media_type not in {"Photo", "Video"}:
            raise ValueError("mediaType is unsupported")
        mime_type = _required_string(value, "mimeType", maximum=255)
        byte_size = _integer(
            value.get("byteSize"), "byteSize", minimum=1, maximum=UINT64_MAX
        )
        width = _integer(
            value.get("widthPixels"),
            "widthPixels",
            minimum=1,
            maximum=UINT32_MAX,
            optional=True,
        )
        height = _integer(
            value.get("heightPixels"),
            "heightPixels",
            minimum=1,
            maximum=UINT32_MAX,
            optional=True,
        )
        duration = _integer(
            value.get("durationMs"),
            "durationMs",
            minimum=0,
            maximum=UINT64_MAX,
            optional=True,
        )
        local_text, local_value = _local_datetime(value.get("capturedAtLocal"))
        utc_text, utc_value = _utc_datetime(value.get("capturedAtUtc"))
        time_zone = _optional_string(value, "timeZoneId", maximum=64)
        offset = _integer(
            value.get("utcOffsetMinutes"),
            "utcOffsetMinutes",
            minimum=-840,
            maximum=840,
            optional=True,
        )
        if local_value is not None and utc_value is not None and offset is not None:
            expected = (local_value - _minutes(offset)).replace(tzinfo=timezone.utc)
            if abs((expected - utc_value).total_seconds()) > 1:
                raise ValueError("capture date-time fields are inconsistent")
        provenance_json, provenance = _provenance(value.get("provenance", []))
        location = value.get("location")
        latitude = longitude = altitude = accuracy = None
        if location is not None:
            if not isinstance(location, dict):
                raise ValueError("location must be an object")
            latitude = _decimal(
                location.get("latitude"),
                "latitude",
                minimum=Decimal("-90"),
                maximum=Decimal("90"),
                places=6,
            )
            longitude = _decimal(
                location.get("longitude"),
                "longitude",
                minimum=Decimal("-180"),
                maximum=Decimal("180"),
                places=6,
            )
            altitude = _decimal(
                location.get("altitudeMeters"),
                "altitudeMeters",
                minimum=Decimal("-9999999.999"),
                maximum=Decimal("9999999.999"),
                places=3,
                optional=True,
            )
            accuracy = _decimal(
                location.get("horizontalAccuracyMeters"),
                "horizontalAccuracyMeters",
                minimum=Decimal("0"),
                maximum=Decimal("9999999.999"),
                places=3,
                optional=True,
            )
        coordinate_revision = None
        if latitude is not None and longitude is not None:
            coordinate_revision = hashlib.sha256(
                f"{latitude},{longitude}".encode("ascii")
            ).hexdigest()
        location_source = next(
            (
                str(item["source"])
                for item in provenance
                if str(item["field"]).casefold()
                in {"location", "gps", "latitude", "longitude"}
            ),
            "Unknown" if latitude is not None else None,
        )
        return (
            [
                row_number,
                "Upsert",
                source_item,
                revision,
                file_name,
                locator,
                content_hash,
                media_type,
                mime_type,
                byte_size,
                width,
                height,
                duration,
                local_text,
                utc_text,
                time_zone,
                offset,
                latitude,
                longitude,
                altitude,
                accuracy,
                provenance_json,
                coordinate_revision,
                location_source,
                "Valid",
                None,
                None,
            ],
            False,
        )
    except (ValueError, TypeError, InvalidOperation) as exc:
        message = str(exc)[:1000] or "The manifest entry is invalid."
        # Retain safe identity fields for the result report; all other raw
        # values are deliberately blank so non-strict MySQL cannot coerce them.
        return (
            [
                *base,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "[]",
                "",
                "",
                "Rejected",
                "InvalidManifestEntry",
                message,
            ],
            True,
        )


def _minutes(value: int):
    from datetime import timedelta

    return timedelta(minutes=value)


def parse_manifest_gzip(
    input_path: Path,
    canonical_csv_path: Path,
    *,
    expected_sha256: str,
    expected_compressed_bytes: int,
    expected_entry_count: int | None = None,
    guardrails: ManifestGuardrails | None = None,
) -> ParsedManifest:
    """Verify and stream one gzip NDJSON manifest into canonical flat CSV.

    The destination is atomically replaced only after gzip CRC, byte count,
    SHA-256, header count, and every line boundary have been validated.
    Semantic entry failures remain explicit rejected rows; corrupt transport or
    malformed NDJSON fails the whole import.
    """

    guards = guardrails or ManifestGuardrails()
    selected = input_path.resolve(strict=True)
    if not selected.is_file():
        raise BulkManifestError("ManifestObjectMissing", "The staged manifest is missing.")
    if not HEX_SHA256.fullmatch(expected_sha256):
        raise BulkManifestError("ManifestChecksumInvalid", "The expected checksum is invalid.")
    if not 0 < expected_compressed_bytes <= guards.max_compressed_bytes:
        raise BulkManifestError(
            "ManifestCompressedLimitExceeded",
            "The declared compressed manifest size is invalid.",
        )
    if selected.stat().st_size != expected_compressed_bytes:
        raise BulkManifestError(
            "ManifestSizeMismatch",
            "The staged manifest size does not match its upload declaration.",
        )

    output = canonical_csv_path.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{os.getpid()}.part")
    compressed: _HashingReader | None = None
    header: ManifestHeader | None = None
    uncompressed_bytes = 0
    entry_count = 0
    rejected_count = 0
    try:
        with selected.open("rb") as raw, partial.open(
            "w", encoding="utf-8", newline=""
        ) as csv_handle:
            compressed = _HashingReader(raw, maximum=guards.max_compressed_bytes)
            buffered_compressed = io.BufferedReader(compressed, buffer_size=128 * 1024)
            writer = csv.writer(
                csv_handle,
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
                escapechar="\\",
                doublequote=False,
            )
            writer.writerow(CANONICAL_CSV_COLUMNS)
            try:
                with gzip.GzipFile(fileobj=buffered_compressed, mode="rb") as archive:
                    line_number = 0
                    while True:
                        line = archive.readline(guards.max_line_bytes + 1)
                        if not line:
                            break
                        line_number += 1
                        if len(line) > guards.max_line_bytes:
                            raise BulkManifestError(
                                "ManifestLineLimitExceeded",
                                f"Manifest line {line_number} exceeds the permitted limit.",
                            )
                        uncompressed_bytes += len(line)
                        if uncompressed_bytes > guards.max_uncompressed_bytes:
                            raise BulkManifestError(
                                "ManifestUncompressedLimitExceeded",
                                "The expanded manifest is larger than the permitted limit.",
                            )
                        if not line.endswith(b"\n"):
                            raise BulkManifestError(
                                "ManifestLineTerminatorMissing",
                                f"Manifest line {line_number} is not newline terminated.",
                            )
                        record = _json_record(line[:-1], row_number=line_number)
                        if line_number == 1:
                            header = _header(record, guards=guards)
                            continue
                        if header is None:
                            raise AssertionError("Manifest header was not parsed")
                        if record.get("recordType") != "Entry":
                            raise BulkManifestError(
                                "ManifestInvalidRecord",
                                f"Manifest line {line_number} is not an Entry record.",
                            )
                        entry_count += 1
                        if entry_count > guards.max_entries:
                            raise BulkManifestError(
                                "ManifestEntryLimitExceeded",
                                "The manifest contains too many entries.",
                            )
                        row, rejected = _raw_entry_row(record, entry_count)
                        writer.writerow(row)
                        rejected_count += int(rejected)
            except (gzip.BadGzipFile, EOFError, zlib_error()) as exc:
                raise BulkManifestError(
                    "ManifestGzipInvalid",
                    "The staged manifest is not a complete valid gzip stream.",
                ) from exc
        if compressed is None or header is None:
            raise BulkManifestError("ManifestHeaderMissing", "The manifest header is missing.")
        checksum = compressed.digest.hexdigest()
        if compressed.count != expected_compressed_bytes:
            raise BulkManifestError(
                "ManifestSizeMismatch",
                "The staged manifest size changed while it was being read.",
            )
        if checksum.lower() != expected_sha256.lower():
            raise BulkManifestError(
                "ManifestChecksumMismatch",
                "The staged manifest checksum does not match its upload declaration.",
            )
        if entry_count != header.entry_count or (
            expected_entry_count is not None and entry_count != expected_entry_count
        ):
            raise BulkManifestError(
                "ManifestEntryCountMismatch",
                "The manifest entry count does not match its declaration.",
            )
        os.replace(partial, output)
        if os.name != "nt":
            output.chmod(0o600)
        return ParsedManifest(
            header=header,
            canonical_csv_path=output,
            compressed_bytes=compressed.count,
            uncompressed_bytes=uncompressed_bytes,
            compressed_sha256=checksum,
            entry_count=entry_count,
            rejected_count=rejected_count,
        )
    except BulkManifestError:
        partial.unlink(missing_ok=True)
        raise
    except OSError as exc:
        partial.unlink(missing_ok=True)
        raise BulkManifestError(
            "ManifestReadFailed", "The staged manifest could not be read safely."
        ) from exc


def zlib_error() -> type[Exception]:
    # gzip exposes zlib.error without documenting it as part of gzip's public
    # exception tuple. Keeping the import local makes the parser easy to stub.
    import zlib

    return zlib.error


def write_result_gzip(
    output_path: Path,
    *,
    import_id: UUID | str,
    counts: Mapping[str, int],
    rows: Iterable[Mapping[str, Any]],
    max_uncompressed_bytes: int = 1024 * 1024 * 1024,
    max_line_bytes: int = 256 * 1024,
) -> tuple[int, str, int, int]:
    """Write a deterministic, line-bounded result artifact.

    Returns ``(compressed_bytes, sha256, result_rows, uncompressed_bytes)``.
    """

    selected = output_path.resolve()
    selected.parent.mkdir(parents=True, exist_ok=True)
    partial = selected.with_name(f".{selected.name}.{os.getpid()}.part")
    result_rows = 0
    uncompressed_bytes = 0
    try:
        with partial.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as archive:
                header = {
                    "recordType": "Result",
                    "schemaVersion": RESULT_FORMAT,
                    "importId": str(import_id),
                    "counts": {str(key): int(value) for key, value in sorted(counts.items())},
                }
                header_line = (
                    json.dumps(header, sort_keys=True, separators=(",", ":")).encode(
                        "utf-8"
                    )
                    + b"\n"
                )
                archive.write(header_line)
                uncompressed_bytes += len(header_line)
                for row in rows:
                    payload = {
                        "recordType": "EntryResult",
                        **{str(key): value for key, value in row.items()},
                    }
                    line = (
                        json.dumps(
                            payload,
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        ).encode("utf-8")
                        + b"\n"
                    )
                    if len(line) > max_line_bytes:
                        raise BulkManifestError(
                            "ManifestResultLineLimitExceeded",
                            "A bulk result row exceeds the supported line limit.",
                        )
                    uncompressed_bytes += len(line)
                    if uncompressed_bytes > max_uncompressed_bytes:
                        raise BulkManifestError(
                            "ManifestResultLimitExceeded",
                            "The expanded bulk result exceeds the supported limit.",
                        )
                    archive.write(line)
                    result_rows += 1
        os.replace(partial, selected)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    digest = hashlib.sha256()
    with selected.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return (
        selected.stat().st_size,
        digest.hexdigest(),
        result_rows,
        uncompressed_bytes,
    )
