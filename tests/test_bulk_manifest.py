from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from services.bulk.manifest import (
    BulkManifestError,
    CANONICAL_CSV_COLUMNS,
    ManifestGuardrails,
    parse_manifest_gzip,
    write_result_gzip,
)


SOURCE_ID = "00000000-0000-0000-0000-000000000101"
SNAPSHOT_ID = "00000000-0000-0000-0000-000000000102"


def _header(count: int) -> dict[str, object]:
    return {
        "recordType": "Manifest",
        "schemaVersion": "ManifestNdjsonV1",
        "storageMode": "Local",
        "sourceId": SOURCE_ID,
        "snapshotId": SNAPSHOT_ID,
        "entryCount": count,
        "kind": "Full",
        "permissionState": "NotApplicable",
        "deletionDetectionReliable": False,
        "clientCursor": None,
    }


def _entry(index: int = 1) -> dict[str, object]:
    return {
        "recordType": "Entry",
        "operation": "Upsert",
        "sourceItemId": f"path:{index}",
        "sourceRevision": f"revision-{index}",
        "fileName": f'photo, "{index}".JPG',
        "localLocator": f"/mnt/d/Photos/photo {index}.JPG",
        "contentSha256": f"{index:064x}",
        "mediaType": "Photo",
        "mimeType": "image/jpeg",
        "byteSize": 1000 + index,
        "widthPixels": 4032,
        "heightPixels": 3024,
        "capturedAtLocal": "2026-08-30T12:34:56",
        "capturedAtUtc": "2026-08-30T16:34:56Z",
        "timeZoneId": "America/New_York",
        "utcOffsetMinutes": -240,
        "location": {
            "latitude": 40.6687,
            "longitude": -74.1148,
            "altitudeMeters": 12.3454,
        },
        "provenance": [
            {"field": "capturedAt", "source": "Exif", "confidence": 1},
            {"field": "location", "source": "Exif", "confidence": 1},
        ],
    }


def _gzip(path: Path, records: list[dict[str, object]]) -> tuple[int, str]:
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as archive:
            for record in records:
                archive.write(
                    json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
                    + b"\n"
                )
    content = path.read_bytes()
    return len(content), hashlib.sha256(content).hexdigest()


def test_manifest_parser_verifies_transport_and_emits_canonical_csv(tmp_path: Path):
    source = tmp_path / "manifest.ndjson.gz"
    byte_size, checksum = _gzip(source, [_header(1), _entry()])
    output = tmp_path / "manifest.csv"

    parsed = parse_manifest_gzip(
        source,
        output,
        expected_sha256=checksum,
        expected_compressed_bytes=byte_size,
        expected_entry_count=1,
    )

    assert parsed.header.source_id == UUID(SOURCE_ID)
    assert parsed.entry_count == 1
    assert parsed.rejected_count == 0
    assert parsed.compressed_sha256 == checksum
    with output.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle, escapechar="\\", doublequote=False))
    assert tuple(rows[0]) == CANONICAL_CSV_COLUMNS
    assert rows[1][0:7] == [
        "1",
        "Upsert",
        "path:1",
        "revision-1",
        'photo, "1".JPG',
        "/mnt/d/Photos/photo 1.JPG",
        f"{1:064x}",
    ]
    assert rows[1][17:21] == ["40.668700", "-74.114800", "12.345", ""]
    assert len(rows[1][22]) == 64
    assert rows[1][23] == "Exif"
    assert rows[1][24] == "Valid"


def test_manifest_parser_rejects_a_delete_so_the_cli_can_use_incremental_sync(
    tmp_path: Path,
):
    source = tmp_path / "manifest.ndjson.gz"
    deleted = {
        "recordType": "Entry",
        "operation": "Deleted",
        "sourceItemId": "path:gone",
        "sourceRevision": "revision-gone",
    }
    byte_size, checksum = _gzip(source, [_header(1), deleted])
    output = tmp_path / "manifest.csv"

    with pytest.raises(BulkManifestError) as error:
        parse_manifest_gzip(
            source,
            output,
            expected_sha256=checksum,
            expected_compressed_bytes=byte_size,
        )

    assert error.value.code == "UnsupportedBulkOperation"
    assert not output.exists()


@pytest.mark.parametrize(
    ("checksum_delta", "size_delta", "expected_code"),
    [
        ("0" * 64, 0, "ManifestChecksumMismatch"),
        (None, 1, "ManifestSizeMismatch"),
    ],
)
def test_manifest_parser_rejects_transport_mismatch_without_publishing_csv(
    tmp_path: Path,
    checksum_delta: str | None,
    size_delta: int,
    expected_code: str,
):
    source = tmp_path / "manifest.ndjson.gz"
    byte_size, checksum = _gzip(source, [_header(1), _entry()])
    output = tmp_path / "manifest.csv"

    with pytest.raises(BulkManifestError) as error:
        parse_manifest_gzip(
            source,
            output,
            expected_sha256=checksum_delta or checksum,
            expected_compressed_bytes=byte_size + size_delta,
        )

    assert error.value.code == expected_code
    assert not output.exists()


def test_manifest_parser_enforces_expanded_and_line_limits(tmp_path: Path):
    source = tmp_path / "manifest.ndjson.gz"
    byte_size, checksum = _gzip(source, [_header(1), _entry()])

    with pytest.raises(BulkManifestError) as error:
        parse_manifest_gzip(
            source,
            tmp_path / "manifest.csv",
            expected_sha256=checksum,
            expected_compressed_bytes=byte_size,
            guardrails=ManifestGuardrails(
                max_compressed_bytes=byte_size + 1,
                max_uncompressed_bytes=10_000,
                max_line_bytes=32,
                max_entries=10,
            ),
        )

    assert error.value.code == "ManifestLineLimitExceeded"


def test_result_artifact_is_deterministic(tmp_path: Path):
    first = tmp_path / "first.result.gz"
    second = tmp_path / "second.result.gz"
    rows = [
        {
            "rowNumber": 1,
            "sourceItemId": "path:1",
            "outcome": "CreatedOccurrence",
            "occurrenceId": "00000000-0000-0000-0000-000000000201",
        }
    ]

    first_result = write_result_gzip(
        first,
        import_id="00000000-0000-0000-0000-000000000200",
        counts={"created": 1, "rejected": 0},
        rows=rows,
    )
    second_result = write_result_gzip(
        second,
        import_id="00000000-0000-0000-0000-000000000200",
        counts={"created": 1, "rejected": 0},
        rows=rows,
    )

    assert first_result == second_result
    assert first.read_bytes() == second.read_bytes()
    with gzip.open(first, "rt", encoding="utf-8") as handle:
        payloads = [json.loads(line) for line in handle]
    assert payloads[0]["schemaVersion"] == "ManifestResultNdjsonV1"
    assert payloads[1]["outcome"] == "CreatedOccurrence"
