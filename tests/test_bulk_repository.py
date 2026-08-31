from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import pymysql

from services.bulk.manifest import ManifestHeader, ParsedManifest
from services.bulk.repository import (
    BulkImportDatabaseError,
    ManifestImportClaim,
    MergeResult,
    MySqlManifestImportRepository,
)


FIXED_NOW = datetime(2026, 8, 31, 12, 0, 0)
IMPORT_ID = UUID("00000000-0000-0000-0000-000000000301")
SOURCE_ID = UUID("00000000-0000-0000-0000-000000000302")
SNAPSHOT_ID = UUID("00000000-0000-0000-0000-000000000303")


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


class FakeCursor:
    def __init__(
        self,
        *,
        warnings: int = 0,
        loaded: int = 2,
        relink: bool = False,
        claim_status: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 1
        self._row: dict[str, Any] | None = None
        self.warnings = warnings
        self.loaded = loaded
        self.relink = relink
        self.claim_status = claim_status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None):
        statement = _normalized(sql)
        self.calls.append((statement, params))
        self.rowcount = 1
        self._row = None
        if statement.startswith("SELECT ImportRow.Id") and self.claim_status is not None:
            self._row = {
                "Id": 11,
                "PublicId": str(IMPORT_ID),
                "SnapshotId": str(SNAPSHOT_ID),
                "UserId": 7,
                "MediaSourceId": 9,
                "Status": self.claim_status,
                "Phase": "WaitingForUpload",
                "AttemptCount": 0,
                "MaxAttempts": 5,
                "NextAttemptAtUtc": None,
                "LeaseExpiresAtUtc": None,
                "InputS3Bucket": "bucket",
                "InputS3ObjectKey": "key",
                "InputS3VersionId": None,
                "InputChecksumSha256": "a" * 64,
                "InputByteSize": 100,
                "DeclaredEntryCount": 2,
                "UserPublicId": "00000000-0000-0000-0000-000000000304",
                "AccountStatus": "Active",
                "AccountDeletedAtUtc": None,
                "SourcePublicId": str(SOURCE_ID),
                "SourceDeviceId": 8,
                "StorageMode": "Local",
                "SourceStatus": "Active",
            }
        elif statement.startswith("SELECT Id, AccountStatus, DeletedAtUtc"):
            self._row = {"Id": 7, "AccountStatus": "Active", "DeletedAtUtc": None}
        elif statement.startswith("SELECT Id, PublicId, DeviceId, StorageMode"):
            self._row = {
                "Id": 9,
                "PublicId": str(SOURCE_ID),
                "DeviceId": 8,
                "StorageMode": "Local",
                "SourceStatus": "Active",
            }
        elif statement.startswith("SELECT Id, UserId, MediaSourceId, Status"):
            self._row = {
                "Id": 11,
                "UserId": 7,
                "MediaSourceId": 9,
                "Status": "Running",
                "Phase": "Staged",
                "LeaseTokenHash": MySqlManifestImportRepository._lease_hash("message-1"),
                "LeaseExpiresAtUtc": FIXED_NOW + timedelta(minutes=15),
                "SchemaVersion": "ManifestNdjsonV1",
                "ManifestKind": "Full",
                "DeletionDetectionReliable": 0,
            }
        elif statement.startswith("SELECT EntryRow.RowNumber, EntryRow.SourceItemId"):
            self._row = (
                {"RowNumber": 1, "SourceItemId": "path:changed"}
                if self.relink
                else None
            )
        elif statement == "SHOW COUNT(*) WARNINGS":
            self._row = {"@@session.warning_count": self.warnings}
        elif statement.startswith("SELECT COUNT(*) AS Loaded"):
            self._row = {
                "Loaded": self.loaded,
                "Validated": self.loaded,
                "Rejected": 0,
            }
        elif statement.startswith("SELECT COUNT(*) AS Processed"):
            self._row = {
                "Processed": 2,
                "Created": 1,
                "Updated": 0,
                "DuplicateLinked": 1,
                "Unchanged": 0,
                "Rejected": 0,
            }

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(
        self,
        *,
        warnings: int = 0,
        loaded: int = 2,
        relink: bool = False,
        claim_status: str | None = None,
    ) -> None:
        self.cursor_value = FakeCursor(
            warnings=warnings,
            loaded=loaded,
            relink=relink,
            claim_status=claim_status,
        )
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def cursor(self, *_args):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


def _claim() -> ManifestImportClaim:
    return ManifestImportClaim(
        internal_id=11,
        public_id=IMPORT_ID,
        snapshot_id=SNAPSHOT_ID,
        user_id=7,
        user_public_id=UUID("00000000-0000-0000-0000-000000000304"),
        source_internal_id=9,
        source_public_id=SOURCE_ID,
        source_device_id=8,
        input_bucket="bucket",
        input_object_key="manifest.gz",
        input_version_id=None,
        input_sha256="a" * 64,
        input_byte_size=100,
        declared_entry_count=2,
        phase="Staged",
        attempt_count=1,
        max_attempts=5,
        lease_owner="message-1",
    )


def _parsed(path: Path) -> ParsedManifest:
    return ParsedManifest(
        header=ManifestHeader(
            source_id=SOURCE_ID,
            snapshot_id=SNAPSHOT_ID,
            entry_count=2,
            manifest_kind="Full",
            permission_state="NotApplicable",
            deletion_detection_reliable=False,
            client_cursor=None,
        ),
        canonical_csv_path=path,
        compressed_bytes=100,
        uncompressed_bytes=200,
        compressed_sha256="a" * 64,
        entry_count=2,
        rejected_count=0,
    )


def test_stage_load_is_one_exact_load_and_one_commit(tmp_path: Path):
    connection = FakeConnection()
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    repository.load_stage(_claim(), _parsed(tmp_path / "manifest.csv"))

    loads = [
        sql for sql, _ in connection.cursor_value.calls if sql.startswith("LOAD DATA LOCAL INFILE")
    ]
    assert len(loads) == 1
    assert "ManifestImportId = %s" in loads[0]
    assert "CoordinateRevision" in loads[0]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed == 1


def test_load_statement_mogrifies_with_pymysql_percent_date_formats():
    connection = pymysql.connect(defer_connect=True)
    connection.server_status = 0
    cursor = connection.cursor()

    rendered = cursor.mogrify(
        MySqlManifestImportRepository._load_sql(), ("/tmp/manifest.csv", 11)
    )

    assert "%Y-%m-%dT%H:%i:%s.%f" in rendered
    assert "ManifestImportId = 11" in rendered


def test_stage_load_rolls_back_on_any_mysql_warning(tmp_path: Path):
    connection = FakeConnection(warnings=1)
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    with pytest.raises(BulkImportDatabaseError) as error:
        repository.load_stage(_claim(), _parsed(tmp_path / "manifest.csv"))

    assert error.value.code == "ManifestLoadMismatch"
    assert connection.commits == 0
    assert connection.rollbacks >= 1


def test_merge_uses_bounded_set_statements_and_one_commit():
    connection = FakeConnection()
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    result = repository.merge(_claim())

    assert result == MergeResult(
        processed=2,
        created=1,
        updated=0,
        duplicates_linked=1,
        unchanged=0,
        rejected=0,
    )
    statements = [sql for sql, _ in connection.cursor_value.calls]
    assert any(sql.startswith("INSERT INTO MediaAsset") for sql in statements)
    assert any(sql.startswith("INSERT INTO MediaOccurrence") for sql in statements)
    assert any(sql.startswith("INSERT INTO MediaLocation") for sql in statements)
    assert any(sql.startswith("INSERT INTO ProcessingJob") for sql in statements)
    assert len(statements) < 40
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_merge_rejects_hash_relink_before_any_canonical_asset_write():
    connection = FakeConnection(relink=True)
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    with pytest.raises(BulkImportDatabaseError) as error:
        repository.merge(_claim())

    assert error.value.code == "BulkRelinkUnsupported"
    statements = [sql for sql, _ in connection.cursor_value.calls]
    assert not any(sql.startswith("INSERT INTO MediaAsset") for sql in statements)
    assert connection.commits == 0
    assert connection.rollbacks >= 1


def test_location_merge_preserves_resolved_evidence_and_jobs_filter_unresolved():
    connection = FakeConnection()
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    repository.merge(_claim())

    statements = [sql for sql, _ in connection.cursor_value.calls]
    location = next(sql for sql in statements if sql.startswith("INSERT INTO MediaLocation"))
    assert "LocationDisplayName = IF(" in location
    assert "Latitude <=> VALUES(Latitude)" in location
    geocode = [sql for sql in statements if "'geocode:'" in sql]
    assert geocode
    assert all("StoredLocation.Provider IS NULL" in sql for sql in geocode)
    asset_update = next(
        sql for sql in statements if sql.startswith("UPDATE MediaAsset AS Asset")
    )
    assert "Asset.OriginalS3ObjectKey IS NOT NULL" in asset_update
    assert "Asset.MetadataVersion = IF(" in asset_update


def test_stage_rejects_a_manifest_for_another_snapshot(tmp_path: Path):
    connection = FakeConnection()
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )
    parsed = _parsed(tmp_path / "manifest.csv")
    parsed = ParsedManifest(
        **{
            **parsed.__dict__,
            "header": ManifestHeader(
                **{
                    **parsed.header.__dict__,
                    "snapshot_id": UUID("00000000-0000-0000-0000-000000000399"),
                }
            ),
        }
    )

    with pytest.raises(BulkImportDatabaseError) as error:
        repository.load_stage(_claim(), parsed)

    assert error.value.code == "ManifestSnapshotMismatch"
    assert not connection.cursor_value.calls


def test_claim_never_starts_an_awaiting_upload_import():
    connection = FakeConnection(claim_status="AwaitingUpload")
    repository = MySqlManifestImportRepository(
        lambda: connection, clock=lambda: FIXED_NOW
    )

    claimed = repository.claim(import_id=IMPORT_ID, lease_owner="message-1")

    assert claimed is None
    assert not any(
        sql.startswith("UPDATE ManifestImport SET Status = 'Running'")
        for sql, _ in connection.cursor_value.calls
    )


def test_claim_starts_a_queued_import_and_commits_one_lease():
    connection = FakeConnection(claim_status="Queued")
    repository = MySqlManifestImportRepository(
        lambda: connection,
        clock=lambda: FIXED_NOW,
    )

    claimed = repository.claim(import_id=IMPORT_ID, lease_owner="message-1")

    assert claimed is not None
    assert claimed.public_id == IMPORT_ID
    assert claimed.phase == "Downloading"
    assert claimed.attempt_count == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert any(
        sql.startswith("UPDATE ManifestImport SET Status = 'Running'")
        for sql, _ in connection.cursor_value.calls
    )
