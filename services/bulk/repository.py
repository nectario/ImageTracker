from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping
from uuid import UUID

from pymysql.cursors import SSDictCursor

from services.bulk.manifest import ParsedManifest


TERMINAL_IMPORT_STATUSES = {
    "Succeeded",
    "CompletedWithErrors",
    "FailedPermanent",
    "Cancelled",
    "Expired",
}


class BulkImportDatabaseError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


@dataclass(frozen=True)
class ManifestImportClaim:
    internal_id: int
    public_id: UUID
    snapshot_id: UUID
    user_id: int
    user_public_id: UUID
    source_internal_id: int
    source_public_id: UUID
    source_device_id: int
    input_bucket: str
    input_object_key: str
    input_version_id: str | None
    input_sha256: str
    input_byte_size: int
    declared_entry_count: int
    phase: str
    attempt_count: int
    max_attempts: int
    lease_owner: str


@dataclass(frozen=True)
class MergeSettings:
    description_model: str = "gpt-5.6-terra"
    description_prompt_version: str = "scene-search-v1"
    description_detail: str = "high"
    description_service_tier: str = "flex"
    description_max_words: int = 24
    description_monthly_call_limit: int = 1_000
    trash_retention_days: int = 30


@dataclass(frozen=True)
class MergeResult:
    processed: int
    created: int
    updated: int
    duplicates_linked: int
    unchanged: int
    rejected: int

    def as_counts(self) -> dict[str, int]:
        return {
            "created": self.created,
            "updated": self.updated,
            "duplicatesLinked": self.duplicates_linked,
            "deleted": 0,
            "ignoredDeletions": 0,
            "unchanged": self.unchanged,
            "rejected": self.rejected,
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _value(row: Mapping[str, Any], key: str) -> Any:
    if key in row:
        return row[key]
    lowered = key.casefold()
    for selected, value in row.items():
        if str(selected).casefold() == lowered:
            return value
    raise KeyError(key)


class MySqlManifestImportRepository:
    """Set-oriented persistence boundary for one verified Local manifest.

    ``connection_factory`` must return a PyMySQL-compatible, non-autocommit
    connection configured with ``local_infile=True``. The worker never accepts
    a database path or SQL fragment from a manifest.
    """

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._connection_factory = connection_factory
        self._clock = clock

    @staticmethod
    def _lease_hash(lease_owner: str) -> str:
        return hashlib.sha256(lease_owner.encode("utf-8")).hexdigest()

    def claim(
        self,
        *,
        import_id: UUID | str,
        lease_owner: str,
        lease_seconds: int = 900,
    ) -> ManifestImportClaim | None:
        if not lease_owner or len(lease_owner) > 256:
            raise ValueError("A bounded import lease owner is required")
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        ImportRow.Id,
                        ImportRow.PublicId,
                        ImportRow.SnapshotId,
                        ImportRow.UserId,
                        ImportRow.MediaSourceId,
                        ImportRow.Status,
                        ImportRow.Phase,
                        ImportRow.AttemptCount,
                        ImportRow.MaxAttempts,
                        ImportRow.NextAttemptAtUtc,
                        ImportRow.LeaseExpiresAtUtc,
                        ImportRow.InputS3Bucket,
                        ImportRow.InputS3ObjectKey,
                        ImportRow.InputS3VersionId,
                        ImportRow.InputChecksumSha256,
                        ImportRow.InputByteSize,
                        ImportRow.DeclaredEntryCount,
                        AccountRow.PublicId AS UserPublicId,
                        AccountRow.AccountStatus,
                        AccountRow.DeletedAtUtc AS AccountDeletedAtUtc,
                        Source.PublicId AS SourcePublicId,
                        Source.DeviceId AS SourceDeviceId,
                        Source.StorageMode,
                        Source.SourceStatus
                    FROM ManifestImport AS ImportRow
                    JOIN MediaSource AS Source
                      ON Source.UserId = ImportRow.UserId
                     AND Source.Id = ImportRow.MediaSourceId
                    JOIN UserAccount AS AccountRow ON AccountRow.Id = ImportRow.UserId
                    WHERE ImportRow.PublicId = %s
                    FOR UPDATE
                    """,
                    (str(import_id),),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.rollback()
                    return None
                status = str(_value(row, "Status"))
                if status in TERMINAL_IMPORT_STATUSES:
                    connection.rollback()
                    return None
                if status not in {"Queued", "RetryDue", "Running"}:
                    connection.rollback()
                    return None
                next_attempt = _value(row, "NextAttemptAtUtc")
                if status == "RetryDue" and next_attempt is not None and next_attempt > now:
                    connection.rollback()
                    return None
                if (
                    _value(row, "AccountStatus") != "Active"
                    or _value(row, "AccountDeletedAtUtc") is not None
                ):
                    cursor.execute(
                        """
                        UPDATE ManifestImport
                        SET Status = 'FailedPermanent', ActiveMarker = NULL,
                            FailureClass = 'InvalidOwner',
                            FailureCode = 'BulkImportOwnerUnavailable',
                            FailureMessage = 'The import owner is no longer active.',
                            LeaseTokenHash = NULL, LeaseExpiresAtUtc = NULL,
                            CompletedAtUtc = %s, UpdatedAtUtc = %s
                        WHERE Id = %s
                        """,
                        (now, now, int(_value(row, "Id"))),
                    )
                    cursor.execute(
                        "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                        (int(_value(row, "Id")),),
                    )
                    connection.commit()
                    return None
                if _value(row, "StorageMode") != "Local" or _value(
                    row, "SourceStatus"
                ) != "Active":
                    cursor.execute(
                        """
                        UPDATE ManifestImport
                        SET Status = 'FailedPermanent', ActiveMarker = NULL,
                            FailureClass = 'InvalidSource',
                            FailureCode = 'BulkSourceUnavailable',
                            FailureMessage = 'The Local source is no longer available for import.',
                            LeaseTokenHash = NULL, LeaseExpiresAtUtc = NULL,
                            CompletedAtUtc = %s, UpdatedAtUtc = %s
                        WHERE Id = %s
                        """,
                        (now, now, int(_value(row, "Id"))),
                    )
                    cursor.execute(
                        "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                        (int(_value(row, "Id")),),
                    )
                    connection.commit()
                    return None
                lease_expiry = _value(row, "LeaseExpiresAtUtc")
                if status == "Running" and lease_expiry is not None and lease_expiry > now:
                    connection.rollback()
                    return None
                attempts = int(_value(row, "AttemptCount") or 0)
                maximum = int(_value(row, "MaxAttempts") or 0)
                if attempts >= maximum:
                    cursor.execute(
                        """
                        UPDATE ManifestImport
                        SET Status = 'FailedPermanent', ActiveMarker = NULL,
                            FailureClass = 'RetryExhausted',
                            FailureCode = 'BulkImportAttemptsExhausted',
                            FailureMessage = 'The bulk import exhausted its retry limit.',
                            LeaseTokenHash = NULL, LeaseExpiresAtUtc = NULL,
                            CompletedAtUtc = %s, UpdatedAtUtc = %s
                        WHERE Id = %s
                        """,
                        (now, now, int(_value(row, "Id"))),
                    )
                    cursor.execute(
                        "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                        (int(_value(row, "Id")),),
                    )
                    connection.commit()
                    return None
                next_phase = str(_value(row, "Phase") or "Downloading")
                if next_phase not in {"Staged", "Merging", "Merged", "WritingResult"}:
                    next_phase = "Downloading"
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Status = 'Running', Phase = %s,
                        AttemptCount = AttemptCount + 1,
                        LeaseTokenHash = %s, LeaseExpiresAtUtc = %s,
                        StartedAtUtc = COALESCE(StartedAtUtc, %s),
                        FailureClass = NULL, FailureCode = NULL, FailureMessage = NULL,
                        UpdatedAtUtc = %s
                    WHERE Id = %s
                    """,
                    (
                        next_phase,
                        self._lease_hash(lease_owner),
                        now + timedelta(seconds=lease_seconds),
                        now,
                        now,
                        int(_value(row, "Id")),
                    ),
                )
            connection.commit()
            return ManifestImportClaim(
                internal_id=int(_value(row, "Id")),
                public_id=UUID(str(_value(row, "PublicId"))),
                snapshot_id=UUID(str(_value(row, "SnapshotId"))),
                user_id=int(_value(row, "UserId")),
                user_public_id=UUID(str(_value(row, "UserPublicId"))),
                source_internal_id=int(_value(row, "MediaSourceId")),
                source_public_id=UUID(str(_value(row, "SourcePublicId"))),
                source_device_id=int(_value(row, "SourceDeviceId")),
                input_bucket=str(_value(row, "InputS3Bucket")),
                input_object_key=str(_value(row, "InputS3ObjectKey")),
                input_version_id=(
                    str(_value(row, "InputS3VersionId"))
                    if _value(row, "InputS3VersionId") is not None
                    else None
                ),
                input_sha256=str(_value(row, "InputChecksumSha256")).lower(),
                input_byte_size=int(_value(row, "InputByteSize")),
                declared_entry_count=int(_value(row, "DeclaredEntryCount")),
                phase=next_phase,
                attempt_count=attempts + 1,
                max_attempts=maximum,
                lease_owner=lease_owner,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def load_stage(self, claim: ManifestImportClaim, parsed: ParsedManifest) -> None:
        if parsed.header.source_id != claim.source_public_id:
            raise BulkImportDatabaseError(
                "ManifestSourceMismatch",
                "The uploaded manifest does not belong to this source.",
            )
        if parsed.header.snapshot_id != claim.snapshot_id:
            raise BulkImportDatabaseError(
                "ManifestSnapshotMismatch",
                "The uploaded manifest snapshot does not match its import.",
            )
        if parsed.entry_count != claim.declared_entry_count:
            raise BulkImportDatabaseError(
                "ManifestEntryCountMismatch",
                "The uploaded manifest row count does not match its import.",
            )
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                self._require_lease(cursor, claim)
                cursor.execute(
                    "DELETE FROM ManifestImportAssetWork WHERE ManifestImportId = %s",
                    (claim.internal_id,),
                )
                cursor.execute(
                    "DELETE FROM ManifestImportFailure WHERE ManifestImportId = %s",
                    (claim.internal_id,),
                )
                cursor.execute(
                    "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                    (claim.internal_id,),
                )
                cursor.execute(
                    self._load_sql(),
                    (str(parsed.canonical_csv_path), claim.internal_id),
                )
                cursor.execute("SHOW COUNT(*) WARNINGS")
                warnings_row = cursor.fetchone()
                warning_count = int(next(iter(warnings_row.values()))) if isinstance(
                    warnings_row, Mapping
                ) else int(warnings_row[0])
                cursor.execute(
                    """
                    SELECT COUNT(*) AS Loaded,
                           SUM(ValidationState = 'Valid') AS Validated,
                           SUM(ValidationState = 'Rejected') AS Rejected
                    FROM ManifestImportEntry
                    WHERE ManifestImportId = %s
                    """,
                    (claim.internal_id,),
                )
                counts = cursor.fetchone()
                loaded = int(_value(counts, "Loaded") or 0)
                validated = int(_value(counts, "Validated") or 0)
                rejected = int(_value(counts, "Rejected") or 0)
                if warning_count or loaded != parsed.entry_count:
                    raise BulkImportDatabaseError(
                        "ManifestLoadMismatch",
                        "MySQL did not load the verified manifest exactly.",
                    )
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Phase = 'Staged', ValidatedEntryCount = %s,
                        RejectedCount = %s, UpdatedAtUtc = %s
                    WHERE Id = %s AND LeaseTokenHash = %s
                    """,
                    (
                        validated,
                        rejected,
                        now,
                        claim.internal_id,
                        self._lease_hash(claim.lease_owner),
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _load_sql() -> str:
        return r"""
            LOAD DATA LOCAL INFILE %s
            INTO TABLE ManifestImportEntry
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (
                @RowNumber, @Operation, @SourceItemId, @SourceRevision,
                @OriginalFileName, @LocalLocator, @ContentSha256, @MediaType,
                @MimeType, @ByteSize, @WidthPixels, @HeightPixels,
                @DurationMilliseconds, @CaptureDateTimeLocal, @CaptureDateTimeUtc,
                @TimeZone, @UtcOffsetMinutes, @Latitude, @Longitude,
                @AltitudeMeters, @AccuracyMeters, @ProvenanceJson,
                @CoordinateRevision, @LocationSource,
                @ValidationState, @ErrorCode, @ErrorMessage
            )
            SET
                ManifestImportId = %s,
                RowNumber = CAST(@RowNumber AS UNSIGNED),
                OperationRaw = @Operation,
                SourceItemIdRaw = @SourceItemId,
                SourceRevisionRaw = NULLIF(@SourceRevision, ''),
                OriginalFileNameRaw = NULLIF(@OriginalFileName, ''),
                LocalLocatorRaw = NULLIF(@LocalLocator, ''),
                ContentSha256Raw = NULLIF(@ContentSha256, ''),
                MediaTypeRaw = NULLIF(@MediaType, ''),
                MimeTypeRaw = NULLIF(@MimeType, ''),
                ByteSizeRaw = NULLIF(@ByteSize, ''),
                WidthPixelsRaw = NULLIF(@WidthPixels, ''),
                HeightPixelsRaw = NULLIF(@HeightPixels, ''),
                DurationMillisecondsRaw = NULLIF(@DurationMilliseconds, ''),
                CaptureDateTimeLocalRaw = NULLIF(@CaptureDateTimeLocal, ''),
                CaptureDateTimeUtcRaw = NULLIF(@CaptureDateTimeUtc, ''),
                TimeZoneRaw = NULLIF(@TimeZone, ''),
                UtcOffsetMinutesRaw = NULLIF(@UtcOffsetMinutes, ''),
                LatitudeRaw = NULLIF(@Latitude, ''),
                LongitudeRaw = NULLIF(@Longitude, ''),
                AltitudeMetersRaw = NULLIF(@AltitudeMeters, ''),
                AccuracyMetersRaw = NULLIF(@AccuracyMeters, ''),
                ProvenanceJsonRaw = NULLIF(@ProvenanceJson, ''),
                Operation = IF(@ValidationState = 'Valid', @Operation, NULL),
                SourceItemId = IF(@ValidationState = 'Valid', @SourceItemId, NULL),
                SourceRevision = IF(@ValidationState = 'Valid', @SourceRevision, NULL),
                OriginalFileName = IF(@ValidationState = 'Valid', @OriginalFileName, NULL),
                LocalLocator = IF(@ValidationState = 'Valid', @LocalLocator, NULL),
                ContentSha256 = IF(@ValidationState = 'Valid', @ContentSha256, NULL),
                MediaType = IF(@ValidationState = 'Valid', @MediaType, NULL),
                MimeType = IF(@ValidationState = 'Valid', @MimeType, NULL),
                ByteSize = IF(@ValidationState = 'Valid', CAST(@ByteSize AS UNSIGNED), NULL),
                WidthPixels = IF(@ValidationState = 'Valid', CAST(NULLIF(@WidthPixels, '') AS UNSIGNED), NULL),
                HeightPixels = IF(@ValidationState = 'Valid', CAST(NULLIF(@HeightPixels, '') AS UNSIGNED), NULL),
                DurationMilliseconds = IF(@ValidationState = 'Valid', CAST(NULLIF(@DurationMilliseconds, '') AS UNSIGNED), NULL),
                CaptureDateTimeLocal = IF(@ValidationState = 'Valid', STR_TO_DATE(NULLIF(@CaptureDateTimeLocal, ''), '%%Y-%%m-%%dT%%H:%%i:%%s.%%f'), NULL),
                CaptureDateTimeUtc = IF(@ValidationState = 'Valid', STR_TO_DATE(REPLACE(NULLIF(@CaptureDateTimeUtc, ''), 'Z', ''), '%%Y-%%m-%%dT%%H:%%i:%%s.%%f'), NULL),
                TimeZone = IF(@ValidationState = 'Valid', NULLIF(@TimeZone, ''), NULL),
                UtcOffsetMinutes = IF(@ValidationState = 'Valid', CAST(NULLIF(@UtcOffsetMinutes, '') AS SIGNED), NULL),
                Latitude = IF(@ValidationState = 'Valid', CAST(NULLIF(@Latitude, '') AS DECIMAL(9,6)), NULL),
                Longitude = IF(@ValidationState = 'Valid', CAST(NULLIF(@Longitude, '') AS DECIMAL(10,6)), NULL),
                AltitudeMeters = IF(@ValidationState = 'Valid', CAST(NULLIF(@AltitudeMeters, '') AS DECIMAL(10,3)), NULL),
                AccuracyMeters = IF(@ValidationState = 'Valid', CAST(NULLIF(@AccuracyMeters, '') AS DECIMAL(10,3)), NULL),
                CoordinateRevision = IF(@ValidationState = 'Valid', NULLIF(@CoordinateRevision, ''), NULL),
                ProvenanceJson = IF(@ValidationState = 'Valid', JSON_EXTRACT(NULLIF(@ProvenanceJson, ''), '$'), NULL),
                LocationSource = IF(@ValidationState = 'Valid', NULLIF(@LocationSource, ''), NULL),
                ValidationState = @ValidationState,
                ErrorCode = NULLIF(@ErrorCode, ''),
                ErrorMessage = NULLIF(@ErrorMessage, '')
        """

    def merge(
        self,
        claim: ManifestImportClaim,
        *,
        settings: MergeSettings | None = None,
    ) -> MergeResult:
        selected = settings or MergeSettings()
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT Id, AccountStatus, DeletedAtUtc
                    FROM UserAccount WHERE Id = %s FOR UPDATE
                    """,
                    (claim.user_id,),
                )
                account = cursor.fetchone()
                if (
                    account is None
                    or _value(account, "AccountStatus") != "Active"
                    or _value(account, "DeletedAtUtc") is not None
                ):
                    raise BulkImportDatabaseError(
                        "BulkImportOwnerMissing", "The import owner is no longer active."
                    )
                cursor.execute(
                    """
                    SELECT Id, PublicId, DeviceId, StorageMode, SourceStatus
                    FROM MediaSource
                    WHERE UserId = %s AND Id = %s
                    FOR UPDATE
                    """,
                    (claim.user_id, claim.source_internal_id),
                )
                source = cursor.fetchone()
                if (
                    source is None
                    or str(_value(source, "PublicId")) != str(claim.source_public_id)
                    or int(_value(source, "DeviceId")) != claim.source_device_id
                    or _value(source, "StorageMode") != "Local"
                    or _value(source, "SourceStatus") != "Active"
                ):
                    raise BulkImportDatabaseError(
                        "BulkSourceUnavailable",
                        "The Local source is no longer available for import.",
                    )
                self._require_lease(cursor, claim, allowed_phases={"Staged", "Merging"})
                cursor.execute("SET @ImportNow = %s", (now,))
                cursor.execute(
                    "DELETE FROM ManifestImportAssetWork WHERE ManifestImportId = %s",
                    (claim.internal_id,),
                )
                self._reject_duplicate_source_items(cursor, claim.internal_id)
                self._reject_asset_relinks(cursor, claim)
                self._build_asset_work(cursor, claim)
                self._merge_assets(cursor, claim)
                self._merge_occurrences(cursor, claim)
                self._merge_locations(cursor, claim)
                self._merge_geocode_jobs(cursor, claim)
                self._merge_description_jobs(cursor, claim, selected)
                self._write_failures(cursor, claim)
                self._write_source_change(cursor, claim)
                result = self._counts(cursor, claim.internal_id)
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Phase = 'Merged', ProcessedEntryCount = %s,
                        CreatedCount = %s, UpdatedCount = %s,
                        DuplicateLinkedCount = %s, DeletedCount = 0,
                        IgnoredDeletionCount = 0, UnchangedCount = %s,
                        RejectedCount = %s, UpdatedAtUtc = @ImportNow
                    WHERE Id = %s AND LeaseTokenHash = %s
                    """,
                    (
                        result.processed,
                        result.created,
                        result.updated,
                        result.duplicates_linked,
                        result.unchanged,
                        result.rejected,
                        claim.internal_id,
                        self._lease_hash(claim.lease_owner),
                    ),
                )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def set_phase(
        self,
        claim: ManifestImportClaim,
        *,
        phase: str,
        allowed_phases: set[str],
    ) -> None:
        """Persist lightweight progress without changing the active lease."""

        if phase not in {"Merging", "WritingResult"}:
            raise ValueError("Bulk import progress phase is invalid")
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                self._require_lease(cursor, claim, allowed_phases=allowed_phases)
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Phase = %s, UpdatedAtUtc = %s
                    WHERE Id = %s AND LeaseTokenHash = %s
                    """,
                    (
                        phase,
                        now,
                        claim.internal_id,
                        self._lease_hash(claim.lease_owner),
                    ),
                )
                if cursor.rowcount != 1:
                    raise BulkImportDatabaseError(
                        "BulkImportLeaseLost",
                        "The bulk import lease is no longer active.",
                    )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _reject_duplicate_source_items(cursor: Any, import_id: int) -> None:
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN (
                SELECT Materialized.SourceItemId, Materialized.FirstRow
                FROM (
                    SELECT SourceItemId, MIN(RowNumber) AS FirstRow
                    FROM ManifestImportEntry
                    WHERE ManifestImportId = %s AND ValidationState = 'Valid'
                    GROUP BY SourceItemId
                    HAVING COUNT(*) > 1
                ) AS Materialized
            ) AS Duplicate
              ON Duplicate.SourceItemId = EntryRow.SourceItemId
             AND EntryRow.RowNumber <> Duplicate.FirstRow
            SET EntryRow.ValidationState = 'Rejected',
                EntryRow.Outcome = 'Rejected',
                EntryRow.ErrorCode = 'DuplicateManifestEntry',
                EntryRow.ErrorMessage = 'The source item appears more than once in this manifest.',
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
            """,
            (import_id, import_id),
        )

    @staticmethod
    def _reject_asset_relinks(cursor: Any, claim: ManifestImportClaim) -> None:
        """Keep bulk v1 away from destructive asset/job lifecycle transitions."""

        cursor.execute(
            """
            SELECT EntryRow.RowNumber, EntryRow.SourceItemId
            FROM ManifestImportEntry AS EntryRow
            JOIN MediaOccurrence AS Occurrence
              ON Occurrence.UserId = %s
             AND Occurrence.MediaSourceId = %s
             AND Occurrence.SourceItemId = EntryRow.SourceItemId
            JOIN MediaAsset AS ExistingAsset
              ON ExistingAsset.UserId = %s
             AND ExistingAsset.Id = Occurrence.MediaAssetId
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND Occurrence.MediaAssetId IS NOT NULL
              AND ExistingAsset.ContentSha256 <> EntryRow.ContentSha256
            LIMIT 1
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                claim.user_id,
                claim.internal_id,
            ),
        )
        if cursor.fetchone() is not None:
            raise BulkImportDatabaseError(
                "BulkRelinkUnsupported",
                "A source item changed content identity; incremental sync is required.",
            )

    @staticmethod
    def _build_asset_work(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            INSERT INTO ManifestImportAssetWork (
                ManifestImportId, ContentSha256, CanonicalStageId,
                CanonicalRowNumber, CreatedAtUtc, UpdatedAtUtc
            )
            SELECT EntryRow.ManifestImportId, EntryRow.ContentSha256,
                   EntryRow.StageId, EntryRow.RowNumber, @ImportNow, @ImportNow
            FROM ManifestImportEntry AS EntryRow
            JOIN (
                SELECT ContentSha256, MIN(RowNumber) AS CanonicalRowNumber
                FROM ManifestImportEntry
                WHERE ManifestImportId = %s AND ValidationState = 'Valid'
                  AND Operation = 'Upsert'
                GROUP BY ContentSha256
            ) AS Canonical
              ON Canonical.ContentSha256 = EntryRow.ContentSha256
             AND Canonical.CanonicalRowNumber = EntryRow.RowNumber
            WHERE EntryRow.ManifestImportId = %s
            """,
            (claim.internal_id, claim.internal_id),
        )
        cursor.execute(
            """
            UPDATE ManifestImportAssetWork AS Work
            JOIN MediaAsset AS Asset
              ON Asset.UserId = %s AND Asset.ContentSha256 = Work.ContentSha256
            SET Work.ResolvedMediaAssetId = Asset.Id,
                Work.ResolvedMediaAssetPublicId = Asset.PublicId,
                Work.AssetWasPreexisting = 1,
                Work.UpdatedAtUtc = @ImportNow
            WHERE Work.ManifestImportId = %s
            """,
            (claim.user_id, claim.internal_id),
        )
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN ManifestImportAssetWork AS Work
              ON Work.ManifestImportId = EntryRow.ManifestImportId
             AND Work.ContentSha256 = EntryRow.ContentSha256
            JOIN MediaAsset AS Asset ON Asset.Id = Work.ResolvedMediaAssetId
            SET EntryRow.ValidationState = 'Rejected', EntryRow.Outcome = 'Rejected',
                EntryRow.ErrorCode = 'ContentHashMetadataMismatch',
                EntryRow.ErrorMessage = 'The content hash is associated with a different byte size.',
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND Asset.ByteSize <> EntryRow.ByteSize
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN ManifestImportAssetWork AS Work
              ON Work.ManifestImportId = EntryRow.ManifestImportId
             AND Work.ContentSha256 = EntryRow.ContentSha256
            JOIN ManifestImportEntry AS Canonical
              ON Canonical.ManifestImportId = Work.ManifestImportId
             AND Canonical.StageId = Work.CanonicalStageId
            SET EntryRow.ValidationState = 'Rejected', EntryRow.Outcome = 'Rejected',
                EntryRow.ErrorCode = 'ContentHashMetadataMismatch',
                EntryRow.ErrorMessage = 'Rows with the same content hash disagree on byte size.',
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND Work.AssetWasPreexisting = 0
              AND EntryRow.ByteSize <> Canonical.ByteSize
            """,
            (claim.internal_id,),
        )

    @staticmethod
    def _merge_assets(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            INSERT INTO MediaAsset (
                PublicId, UserId, ContentSha256, ContentHashSource,
                MediaType, MimeType, ByteSize, WidthPixels, HeightPixels,
                DurationMilliseconds, CaptureDateTimeLocal, CaptureDateTimeUtc,
                TimeZone, UtcOffsetMinutes, CaptureTimeSource,
                CaptureTimeConfidence, MetadataJson, MetadataVersion,
                StorageState, LifecycleState, CreatedAtUtc, UpdatedAtUtc
            )
            SELECT UUID(), %s, Work.ContentSha256, 'ClientDeclared',
                   EntryRow.MediaType, EntryRow.MimeType, EntryRow.ByteSize,
                   EntryRow.WidthPixels, EntryRow.HeightPixels,
                   EntryRow.DurationMilliseconds, EntryRow.CaptureDateTimeLocal,
                   EntryRow.CaptureDateTimeUtc, EntryRow.TimeZone,
                   EntryRow.UtcOffsetMinutes,
                   IF(EntryRow.CaptureDateTimeLocal IS NULL
                      AND EntryRow.CaptureDateTimeUtc IS NULL, NULL, 'Unknown'),
                   NULL, JSON_OBJECT('provenance', EntryRow.ProvenanceJson),
                   'ManifestV1', 'LocalOnly', 'Active', @ImportNow, @ImportNow
            FROM ManifestImportAssetWork AS Work
            JOIN ManifestImportEntry AS EntryRow
              ON EntryRow.ManifestImportId = Work.ManifestImportId
             AND EntryRow.StageId = Work.CanonicalStageId
            WHERE Work.ManifestImportId = %s
              AND Work.ResolvedMediaAssetId IS NULL
              AND EntryRow.ValidationState = 'Valid'
            """,
            (claim.user_id, claim.internal_id),
        )
        cursor.execute(
            """
            UPDATE ManifestImportAssetWork AS Work
            JOIN MediaAsset AS Asset
              ON Asset.UserId = %s AND Asset.ContentSha256 = Work.ContentSha256
            SET Work.ResolvedMediaAssetId = Asset.Id,
                Work.ResolvedMediaAssetPublicId = Asset.PublicId,
                Work.AssetCreated = IF(Work.AssetWasPreexisting = 0, 1, 0),
                Work.UpdatedAtUtc = @ImportNow
            WHERE Work.ManifestImportId = %s
            """,
            (claim.user_id, claim.internal_id),
        )
        cursor.execute(
            """
            UPDATE ManifestImportAssetWork AS Work
            JOIN MediaAsset AS Asset ON Asset.Id = Work.ResolvedMediaAssetId
            JOIN ManifestImportEntry AS EntryRow
              ON EntryRow.ManifestImportId = Work.ManifestImportId
             AND EntryRow.StageId = Work.CanonicalStageId
            SET Work.AssetChanged = IF(
                    Work.AssetCreated = 1 OR Asset.LifecycleState = 'Trashed'
                    OR (Asset.WidthPixels IS NULL AND EntryRow.WidthPixels IS NOT NULL)
                    OR (Asset.HeightPixels IS NULL AND EntryRow.HeightPixels IS NOT NULL)
                    OR (Asset.DurationMilliseconds IS NULL AND EntryRow.DurationMilliseconds IS NOT NULL)
                    OR (Asset.CaptureDateTimeLocal IS NULL AND EntryRow.CaptureDateTimeLocal IS NOT NULL)
                    OR (Asset.CaptureDateTimeUtc IS NULL AND EntryRow.CaptureDateTimeUtc IS NOT NULL)
                    OR (Asset.TimeZone IS NULL AND EntryRow.TimeZone IS NOT NULL)
                    OR (Asset.UtcOffsetMinutes IS NULL AND EntryRow.UtcOffsetMinutes IS NOT NULL)
                    OR (Asset.MetadataJson IS NULL
                        AND JSON_LENGTH(EntryRow.ProvenanceJson) > 0),
                    1, Work.AssetChanged),
                Work.UpdatedAtUtc = @ImportNow
            WHERE Work.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            UPDATE MediaAsset AS Asset
            JOIN ManifestImportAssetWork AS Work
              ON Work.ResolvedMediaAssetId = Asset.Id
            JOIN ManifestImportEntry AS EntryRow
              ON EntryRow.ManifestImportId = Work.ManifestImportId
             AND EntryRow.StageId = Work.CanonicalStageId
            SET Asset.WidthPixels = COALESCE(Asset.WidthPixels, EntryRow.WidthPixels),
                Asset.HeightPixels = COALESCE(Asset.HeightPixels, EntryRow.HeightPixels),
                Asset.DurationMilliseconds = COALESCE(Asset.DurationMilliseconds, EntryRow.DurationMilliseconds),
                Asset.CaptureDateTimeLocal = COALESCE(Asset.CaptureDateTimeLocal, EntryRow.CaptureDateTimeLocal),
                Asset.CaptureDateTimeUtc = COALESCE(Asset.CaptureDateTimeUtc, EntryRow.CaptureDateTimeUtc),
                Asset.TimeZone = COALESCE(Asset.TimeZone, EntryRow.TimeZone),
                Asset.UtcOffsetMinutes = COALESCE(Asset.UtcOffsetMinutes, EntryRow.UtcOffsetMinutes),
                Asset.MetadataVersion = IF(
                    Asset.MetadataJson IS NULL
                    AND JSON_LENGTH(EntryRow.ProvenanceJson) > 0,
                    'ManifestV1', Asset.MetadataVersion),
                Asset.MetadataJson = IF(
                    Asset.MetadataJson IS NULL
                    AND JSON_LENGTH(EntryRow.ProvenanceJson) > 0,
                    JSON_OBJECT('provenance', EntryRow.ProvenanceJson),
                    Asset.MetadataJson),
                Asset.StorageState = IF(
                    Asset.LifecycleState = 'Trashed',
                    IF(Asset.OriginalS3ObjectKey IS NOT NULL,
                       'RemoteAvailable', 'LocalOnly'),
                    Asset.StorageState),
                Asset.LifecycleState = 'Active', Asset.TrashedAtUtc = NULL,
                Asset.PurgeAfterUtc = NULL,
                Asset.UpdatedAtUtc = IF(
                    Work.AssetCreated = 1, Asset.UpdatedAtUtc,
                    IF(Work.AssetChanged = 1, @ImportNow, Asset.UpdatedAtUtc))
            WHERE Work.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN ManifestImportAssetWork AS Work
              ON Work.ManifestImportId = EntryRow.ManifestImportId
             AND Work.ContentSha256 = EntryRow.ContentSha256
            SET EntryRow.ResolvedAssetId = Work.ResolvedMediaAssetId,
                EntryRow.MediaAssetPublicId = Work.ResolvedMediaAssetPublicId,
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            INSERT INTO MediaChange (
                PublicId, UserId, DeviceId, MediaSourceId, MediaAssetId,
                EntityType, EntityId, EntityPublicId, ChangeType, CreatedAtUtc
            )
            SELECT UUID(), %s, %s, %s, Work.ResolvedMediaAssetId,
                   'MediaAsset', Work.ResolvedMediaAssetId,
                   Work.ResolvedMediaAssetPublicId, 'Upsert', @ImportNow
            FROM ManifestImportAssetWork AS Work
            WHERE Work.ManifestImportId = %s AND Work.AssetChanged = 1
            """,
            (
                claim.user_id,
                claim.source_device_id,
                claim.source_internal_id,
                claim.internal_id,
            ),
        )

    @staticmethod
    def _merge_occurrences(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            LEFT JOIN MediaOccurrence AS Occurrence
              ON Occurrence.UserId = %s
             AND Occurrence.MediaSourceId = %s
             AND Occurrence.SourceItemId = EntryRow.SourceItemId
            JOIN ManifestImportAssetWork AS Work
              ON Work.ManifestImportId = EntryRow.ManifestImportId
             AND Work.ContentSha256 = EntryRow.ContentSha256
            SET EntryRow.ExistingOccurrenceId = Occurrence.Id,
                EntryRow.ExistingAssetId = Occurrence.MediaAssetId,
                EntryRow.Outcome = CASE
                    WHEN Occurrence.Id IS NULL THEN 'DuplicateLinked'
                    WHEN Occurrence.MediaAssetId = Work.ResolvedMediaAssetId
                     AND Occurrence.SourceRevision <=> EntryRow.SourceRevision
                     AND Occurrence.OriginalFileName <=> EntryRow.OriginalFileName
                     AND Occurrence.LocalLocator <=> EntryRow.LocalLocator
                     AND Occurrence.ObservedByteSize <=> EntryRow.ByteSize
                     AND Occurrence.DeletionState = 'Active'
                     AND Occurrence.HashStatus = 'Complete' THEN 'Unchanged'
                    ELSE 'UpdatedOccurrence'
                END,
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                claim.internal_id,
            ),
        )
        # The correlated outcome expression above cannot see ExistingOccurrenceId
        # assigned in the same UPDATE on every MySQL optimizer path. Normalize
        # the first-created outcome with one deterministic follow-up.
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN ManifestImportAssetWork AS Work
              ON Work.ManifestImportId = EntryRow.ManifestImportId
             AND Work.ContentSha256 = EntryRow.ContentSha256
            JOIN (
                SELECT Materialized.ContentSha256, Materialized.FirstNewRow
                FROM (
                    SELECT ContentSha256, MIN(RowNumber) AS FirstNewRow
                    FROM ManifestImportEntry
                    WHERE ManifestImportId = %s AND ValidationState = 'Valid'
                      AND ExistingOccurrenceId IS NULL
                    GROUP BY ContentSha256
                ) AS Materialized
            ) AS FirstNew ON FirstNew.ContentSha256 = EntryRow.ContentSha256
            SET EntryRow.Outcome = CASE
                WHEN Work.AssetCreated = 1 AND EntryRow.RowNumber = FirstNew.FirstNewRow
                    THEN 'CreatedOccurrence'
                WHEN EntryRow.ExistingOccurrenceId IS NULL THEN 'DuplicateLinked'
                ELSE EntryRow.Outcome END
            WHERE EntryRow.ManifestImportId = %s
            """,
            (claim.internal_id, claim.internal_id),
        )
        cursor.execute(
            """
            INSERT INTO MediaOccurrence (
                PublicId, UserId, MediaSourceId, MediaAssetId, SourceItemId,
                OriginalFileName, LocalLocator, SourceRevision, ObservedByteSize,
                HashStatus, AvailabilityState, DeletionState,
                FirstSeenAtUtc, LastSeenAtUtc, CreatedAtUtc, UpdatedAtUtc
            )
            SELECT UUID(), %s, %s, EntryRow.ResolvedAssetId, EntryRow.SourceItemId,
                   EntryRow.OriginalFileName, EntryRow.LocalLocator,
                   EntryRow.SourceRevision, EntryRow.ByteSize,
                   'Complete', 'Available', 'Active',
                   @ImportNow, @ImportNow, @ImportNow, @ImportNow
            FROM ManifestImportEntry AS EntryRow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND EntryRow.ExistingOccurrenceId IS NULL
            """,
            (claim.user_id, claim.source_internal_id, claim.internal_id),
        )
        cursor.execute(
            """
            UPDATE MediaOccurrence AS Occurrence
            JOIN ManifestImportEntry AS EntryRow
              ON EntryRow.ExistingOccurrenceId = Occurrence.Id
             AND EntryRow.ManifestImportId = %s
            SET Occurrence.MediaAssetId = EntryRow.ResolvedAssetId,
                Occurrence.OriginalFileName = EntryRow.OriginalFileName,
                Occurrence.LocalLocator = EntryRow.LocalLocator,
                Occurrence.SourceRevision = EntryRow.SourceRevision,
                Occurrence.ObservedByteSize = EntryRow.ByteSize,
                Occurrence.HashStatus = 'Complete', Occurrence.HashFailureCode = NULL,
                Occurrence.AvailabilityState = 'Available',
                Occurrence.DeletionState = 'Active', Occurrence.DeletedAtUtc = NULL,
                Occurrence.LastSeenAtUtc = @ImportNow,
                Occurrence.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ValidationState = 'Valid'
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            UPDATE ManifestImportEntry AS EntryRow
            JOIN MediaOccurrence AS Occurrence
              ON Occurrence.UserId = %s AND Occurrence.MediaSourceId = %s
             AND Occurrence.SourceItemId = EntryRow.SourceItemId
            SET EntryRow.ExistingOccurrenceId = Occurrence.Id,
                EntryRow.OccurrencePublicId = Occurrence.PublicId,
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
            """,
            (claim.user_id, claim.source_internal_id, claim.internal_id),
        )
        cursor.execute(
            """
            INSERT INTO MediaChange (
                PublicId, UserId, DeviceId, MediaSourceId, MediaAssetId,
                MediaOccurrenceId, EntityType, EntityId, EntityPublicId,
                ChangeType, CreatedAtUtc
            )
            SELECT UUID(), %s, %s, %s, EntryRow.ResolvedAssetId,
                   EntryRow.ExistingOccurrenceId, 'MediaOccurrence',
                   EntryRow.ExistingOccurrenceId, EntryRow.OccurrencePublicId,
                   'Upsert', @ImportNow
            FROM ManifestImportEntry AS EntryRow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND EntryRow.Outcome <> 'Unchanged'
            """,
            (
                claim.user_id,
                claim.source_device_id,
                claim.source_internal_id,
                claim.internal_id,
            ),
        )

    @staticmethod
    def _merge_locations(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            INSERT INTO MediaLocation (
                PublicId, UserId, MediaAssetId, Latitude, Longitude,
                AltitudeMeters, AccuracyMeters, LocationSource,
                CreatedAtUtc, UpdatedAtUtc
            )
            SELECT UUID(), %s, EntryRow.ResolvedAssetId, EntryRow.Latitude,
                   EntryRow.Longitude, EntryRow.AltitudeMeters,
                   EntryRow.AccuracyMeters, COALESCE(EntryRow.LocationSource, 'Unknown'),
                   @ImportNow, @ImportNow
            FROM ManifestImportEntry AS EntryRow
            LEFT JOIN ManifestImportEntry AS Later
              ON Later.ManifestImportId = EntryRow.ManifestImportId
             AND Later.ValidationState = 'Valid'
             AND Later.ResolvedAssetId = EntryRow.ResolvedAssetId
             AND Later.Latitude IS NOT NULL AND Later.Longitude IS NOT NULL
             AND Later.RowNumber > EntryRow.RowNumber
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND EntryRow.Latitude IS NOT NULL AND EntryRow.Longitude IS NOT NULL
              AND Later.StageId IS NULL
            ON DUPLICATE KEY UPDATE
                LocationDisplayName = IF(
                    Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), LocationDisplayName, NULL),
                StreetAddress = IF(
                    Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), StreetAddress, NULL),
                OriginalStreetNumber = IF(
                    Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), OriginalStreetNumber, NULL),
                Neighborhood = IF(
                    Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), Neighborhood, NULL),
                City = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), City, NULL),
                County = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), County, NULL),
                State = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), State, NULL),
                PostalCode = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), PostalCode, NULL),
                Country = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), Country, NULL),
                CountryCode = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), CountryCode, NULL),
                Provider = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), Provider, NULL),
                ProviderPlaceId = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), ProviderPlaceId, NULL),
                NormalizationRuleVersion = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), NormalizationRuleVersion, NULL),
                Confidence = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), Confidence, NULL),
                RawProviderJson = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), RawProviderJson, NULL),
                ProviderUpdatedAtUtc = IF(Latitude <=> VALUES(Latitude)
                    AND Longitude <=> VALUES(Longitude), ProviderUpdatedAtUtc, NULL),
                Latitude = VALUES(Latitude), Longitude = VALUES(Longitude),
                AltitudeMeters = VALUES(AltitudeMeters),
                AccuracyMeters = VALUES(AccuracyMeters),
                LocationSource = VALUES(LocationSource),
                UpdatedAtUtc = @ImportNow
            """,
            (claim.user_id, claim.internal_id),
        )

    @staticmethod
    def _merge_geocode_jobs(cursor: Any, claim: ManifestImportClaim) -> None:
        final_location = """
            SELECT EntryRow.*
            FROM ManifestImportEntry AS EntryRow
            LEFT JOIN ManifestImportEntry AS Later
              ON Later.ManifestImportId = EntryRow.ManifestImportId
             AND Later.ValidationState = 'Valid'
             AND Later.ResolvedAssetId = EntryRow.ResolvedAssetId
             AND Later.Latitude IS NOT NULL AND Later.Longitude IS NOT NULL
             AND Later.RowNumber > EntryRow.RowNumber
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND EntryRow.Latitude IS NOT NULL AND EntryRow.Longitude IS NOT NULL
              AND Later.StageId IS NULL
              AND EXISTS (
                  SELECT 1 FROM MediaLocation AS StoredLocation
                  WHERE StoredLocation.MediaAssetId = EntryRow.ResolvedAssetId
                    AND (StoredLocation.Provider IS NULL
                         OR StoredLocation.ProviderUpdatedAtUtc IS NULL)
              )
        """
        cursor.execute(
            f"""
            UPDATE ProcessingJob AS Job
            JOIN MediaAsset AS Asset ON Asset.Id = Job.MediaAssetId
            JOIN ({final_location}) AS FinalLocation
              ON FinalLocation.ResolvedAssetId = Asset.Id
             AND Job.IdempotencyKey = CONCAT(
                    'geocode:', Asset.PublicId, ':', FinalLocation.CoordinateRevision)
            JOIN MediaLocation AS Location
              ON Location.UserId = %s AND Location.MediaAssetId = Asset.Id
            SET Job.Status = 'Queued', Job.AttemptCount = 0,
                Job.NextAttemptAtUtc = @ImportNow, Job.LeaseTokenHash = NULL,
                Job.LeaseExpiresAtUtc = NULL, Job.FailureClass = NULL,
                Job.FailureCode = NULL, Job.FailureMessage = NULL,
                Job.StartedAtUtc = NULL, Job.CompletedAtUtc = NULL,
                Job.RequestJson = JSON_OBJECT(
                    'latitude', CAST(FinalLocation.Latitude AS CHAR),
                    'longitude', CAST(FinalLocation.Longitude AS CHAR),
                    'coordinateRevision', FinalLocation.CoordinateRevision,
                    'locationPublicId', Location.PublicId),
                Job.UpdatedAtUtc = @ImportNow
            WHERE Job.UserId = %s
              AND Job.Status IN ('Succeeded','Failed','Cancelled','DeferredQuota')
            """,
            (claim.internal_id, claim.user_id, claim.user_id),
        )
        cursor.execute(
            f"""
            INSERT INTO ProcessingJob (
                PublicId, UserId, MediaAssetId, MediaSourceId, IdempotencyKey,
                JobType, Status, Provider, AttemptCount, MaxAttempts,
                NextAttemptAtUtc, RequestJson, CreatedAtUtc, UpdatedAtUtc
            )
            SELECT UUID(), %s, Asset.Id, %s,
                   CONCAT('geocode:', Asset.PublicId, ':', FinalLocation.CoordinateRevision),
                   'Geocode', 'Queued', 'AmazonLocationPlacesV2', 0, 5,
                   @ImportNow,
                   JSON_OBJECT(
                       'latitude', CAST(FinalLocation.Latitude AS CHAR),
                       'longitude', CAST(FinalLocation.Longitude AS CHAR),
                       'coordinateRevision', FinalLocation.CoordinateRevision,
                       'locationPublicId', Location.PublicId),
                   @ImportNow, @ImportNow
            FROM ({final_location}) AS FinalLocation
            JOIN MediaAsset AS Asset
              ON Asset.UserId = %s AND Asset.Id = FinalLocation.ResolvedAssetId
            JOIN MediaLocation AS Location
              ON Location.UserId = %s AND Location.MediaAssetId = Asset.Id
            LEFT JOIN ProcessingJob AS Existing
              ON Existing.UserId = %s
             AND Existing.IdempotencyKey = CONCAT(
                    'geocode:', Asset.PublicId, ':', FinalLocation.CoordinateRevision)
            WHERE Existing.Id IS NULL
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                claim.internal_id,
                claim.user_id,
                claim.user_id,
                claim.user_id,
            ),
        )
        cursor.execute(
            f"""
            INSERT INTO MediaChange (
                PublicId, UserId, MediaSourceId, MediaAssetId, EntityType,
                EntityId, EntityPublicId, ChangeType, CreatedAtUtc
            )
            SELECT UUID(), %s, %s, Job.MediaAssetId, 'ProcessingJob',
                   Job.Id, Job.PublicId, 'Upsert', @ImportNow
            FROM ProcessingJob AS Job
            JOIN MediaAsset AS Asset ON Asset.Id = Job.MediaAssetId
            JOIN ({final_location}) AS FinalLocation
              ON FinalLocation.ResolvedAssetId = Asset.Id
             AND Job.IdempotencyKey = CONCAT(
                    'geocode:', Asset.PublicId, ':', FinalLocation.CoordinateRevision)
            WHERE Job.UserId = %s AND Job.UpdatedAtUtc = @ImportNow
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                claim.internal_id,
                claim.user_id,
            ),
        )

    @staticmethod
    def _merge_description_jobs(
        cursor: Any, claim: ManifestImportClaim, settings: MergeSettings
    ) -> None:
        eligible = r"""
            SELECT Candidate.ResolvedAssetId, MIN(Candidate.RowNumber) AS FirstRow
            FROM ManifestImportEntry AS Candidate
            JOIN MediaAsset AS CandidateAsset
              ON CandidateAsset.Id = Candidate.ResolvedAssetId
            LEFT JOIN MediaDescription AS CurrentDescription
              ON CurrentDescription.UserId = CandidateAsset.UserId
             AND CurrentDescription.MediaAssetId = CandidateAsset.Id
             AND CurrentDescription.IsCurrent = 1
             AND CurrentDescription.Status = 'Succeeded'
             AND LENGTH(TRIM(CurrentDescription.Description)) > 0
            WHERE Candidate.ManifestImportId = %s
              AND Candidate.ValidationState = 'Valid'
              AND CandidateAsset.MediaType = 'Photo'
              AND LOWER(Candidate.OriginalFileName)
                    REGEXP '\\.(avif|bmp|gif|heic|heif|jpeg|jpg|png|tif|tiff|webp)$'
              AND LOWER(Candidate.OriginalFileName)
                    NOT REGEXP '\\.(arw|cr2|cr3|dng|nef|rw2)$'
              AND CurrentDescription.Id IS NULL
            GROUP BY Candidate.ResolvedAssetId
        """
        cursor.execute(
            f"""
            INSERT INTO ProcessingJob (
                PublicId, UserId, MediaAssetId, MediaSourceId, IdempotencyKey,
                JobType, Status, Provider, AttemptCount, MaxAttempts,
                NextAttemptAtUtc, RequestJson, CreatedAtUtc, UpdatedAtUtc
            )
            SELECT UUID(), %s, Asset.Id, %s,
                   CONCAT('description:', Asset.PublicId),
                   'Description', 'Preparing', 'OpenAI', 0, 5, NULL,
                   JSON_OBJECT(
                       'assetRevision', LOWER(Asset.ContentSha256),
                       'sourceId', %s, 'model', %s, 'promptVersion', %s,
                       'detail', %s, 'serviceTier', %s, 'maxWords', %s,
                       'monthlyCallLimit', %s),
                   @ImportNow, @ImportNow
            FROM ({eligible}) AS Eligible
            JOIN MediaAsset AS Asset
              ON Asset.UserId = %s AND Asset.Id = Eligible.ResolvedAssetId
            LEFT JOIN ProcessingJob AS Existing
              ON Existing.UserId = %s
             AND Existing.IdempotencyKey = CONCAT('description:', Asset.PublicId)
            WHERE Existing.Id IS NULL
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                str(claim.source_public_id),
                settings.description_model,
                settings.description_prompt_version,
                settings.description_detail,
                settings.description_service_tier,
                settings.description_max_words,
                settings.description_monthly_call_limit,
                claim.internal_id,
                claim.user_id,
                claim.user_id,
            ),
        )
        cursor.execute(
            f"""
            UPDATE ManifestImportEntry AS EntryRow
            JOIN MediaAsset AS Asset ON Asset.Id = EntryRow.ResolvedAssetId
            JOIN ({eligible}) AS Eligible
              ON Eligible.ResolvedAssetId = EntryRow.ResolvedAssetId
            JOIN ProcessingJob AS Job
              ON Job.UserId = %s
             AND Job.IdempotencyKey = CONCAT('description:', Asset.PublicId)
            SET EntryRow.DescriptionJobPublicId = Job.PublicId,
                EntryRow.UpdatedAtUtc = @ImportNow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Valid'
              AND Job.Status = 'Preparing'
              AND Asset.MediaType = 'Photo'
              AND LOWER(EntryRow.OriginalFileName)
                    REGEXP '\\.(avif|bmp|gif|heic|heif|jpeg|jpg|png|tif|tiff|webp)$'
              AND LOWER(EntryRow.OriginalFileName)
                    NOT REGEXP '\\.(arw|cr2|cr3|dng|nef|rw2)$'
            """,
            (claim.internal_id, claim.user_id, claim.internal_id),
        )
        cursor.execute(
            f"""
            INSERT INTO MediaChange (
                PublicId, UserId, MediaSourceId, MediaAssetId, EntityType,
                EntityId, EntityPublicId, ChangeType, CreatedAtUtc
            )
            SELECT UUID(), %s, %s, Job.MediaAssetId, 'ProcessingJob',
                   Job.Id, Job.PublicId, 'Upsert', @ImportNow
            FROM ProcessingJob AS Job
            JOIN ({eligible}) AS Eligible ON Eligible.ResolvedAssetId = Job.MediaAssetId
            WHERE Job.UserId = %s AND Job.JobType = 'Description'
              AND Job.CreatedAtUtc = @ImportNow
            """,
            (
                claim.user_id,
                claim.source_internal_id,
                claim.internal_id,
                claim.user_id,
            ),
        )

    @staticmethod
    def _trash_orphaned_assets(
        cursor: Any, claim: ManifestImportClaim, settings: MergeSettings
    ) -> None:
        cursor.execute(
            """
            UPDATE MediaAsset AS Asset
            JOIN (
                SELECT DISTINCT ExistingAssetId AS AssetId
                FROM ManifestImportEntry
                WHERE ManifestImportId = %s AND ValidationState = 'Valid'
                  AND ExistingAssetId IS NOT NULL
                  AND ExistingAssetId <> ResolvedAssetId
            ) AS OldAsset ON OldAsset.AssetId = Asset.Id
            LEFT JOIN MediaOccurrence AS ActiveOccurrence
              ON ActiveOccurrence.UserId = %s
             AND ActiveOccurrence.MediaAssetId = Asset.Id
             AND ActiveOccurrence.DeletionState = 'Active'
            SET Asset.LifecycleState = 'Trashed', Asset.StorageState = 'Trashed',
                Asset.TrashedAtUtc = @ImportNow,
                Asset.PurgeAfterUtc = DATE_ADD(@ImportNow, INTERVAL %s DAY),
                Asset.UpdatedAtUtc = @ImportNow
            WHERE Asset.UserId = %s AND ActiveOccurrence.Id IS NULL
              AND Asset.LifecycleState <> 'Trashed'
            """,
            (
                claim.internal_id,
                claim.user_id,
                settings.trash_retention_days,
                claim.user_id,
            ),
        )
        cursor.execute(
            """
            UPDATE ProcessingJob AS Job
            JOIN MediaAsset AS Asset
              ON Asset.UserId = Job.UserId AND Asset.Id = Job.MediaAssetId
            SET Job.Status = 'Cancelled', Job.NextAttemptAtUtc = NULL,
                Job.LeaseTokenHash = NULL, Job.LeaseExpiresAtUtc = NULL,
                Job.FailureClass = 'InvalidMedia', Job.FailureCode = 'MediaTrashed',
                Job.FailureMessage = 'Processing stopped because the media was removed.',
                Job.CompletedAtUtc = @ImportNow, Job.UpdatedAtUtc = @ImportNow
            WHERE Job.UserId = %s AND Asset.LifecycleState = 'Trashed'
              AND Asset.TrashedAtUtc = @ImportNow
              AND Job.Status IN ('Preparing','Queued','Running','DeferredQuota')
            """,
            (claim.user_id,),
        )

    @staticmethod
    def _write_failures(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            UPDATE ManifestImportEntry
            SET Outcome = 'Rejected', UpdatedAtUtc = @ImportNow
            WHERE ManifestImportId = %s AND ValidationState = 'Rejected'
            """,
            (claim.internal_id,),
        )
        cursor.execute(
            """
            INSERT INTO ManifestImportFailure (
                PublicId, UserId, ManifestImportId, RowNumber, SourceItemId,
                SourceRevision, Operation, ErrorCode, ErrorMessage, CreatedAtUtc
            )
            SELECT UUID(), %s, EntryRow.ManifestImportId, EntryRow.RowNumber,
                   LEFT(EntryRow.SourceItemIdRaw, 512),
                   LEFT(EntryRow.SourceRevisionRaw, 255),
                   LEFT(EntryRow.OperationRaw, 16), EntryRow.ErrorCode,
                   EntryRow.ErrorMessage, @ImportNow
            FROM ManifestImportEntry AS EntryRow
            WHERE EntryRow.ManifestImportId = %s
              AND EntryRow.ValidationState = 'Rejected'
            ON DUPLICATE KEY UPDATE
                ErrorCode = VALUES(ErrorCode), ErrorMessage = VALUES(ErrorMessage)
            """,
            (claim.user_id, claim.internal_id),
        )

    @staticmethod
    def _write_source_change(cursor: Any, claim: ManifestImportClaim) -> None:
        cursor.execute(
            """
            UPDATE MediaSource AS Source
            JOIN ManifestImport AS ImportRow ON ImportRow.Id = %s
            SET Source.PermissionState = ImportRow.PermissionState,
                Source.SyncCursor = ImportRow.ClientCursor,
                Source.LastManifestAtUtc = @ImportNow,
                Source.LastSuccessAtUtc = @ImportNow,
                Source.UpdatedAtUtc = @ImportNow
            WHERE Source.UserId = %s AND Source.Id = %s
            """,
            (claim.internal_id, claim.user_id, claim.source_internal_id),
        )
        cursor.execute(
            """
            INSERT INTO MediaChange (
                PublicId, UserId, DeviceId, MediaSourceId, EntityType,
                EntityId, EntityPublicId, ChangeType, CreatedAtUtc
            )
            VALUES (UUID(), %s, %s, %s, 'MediaSource', %s, %s, 'Upsert', @ImportNow)
            """,
            (
                claim.user_id,
                claim.source_device_id,
                claim.source_internal_id,
                claim.source_internal_id,
                str(claim.source_public_id),
            ),
        )

    @staticmethod
    def _counts(cursor: Any, import_id: int) -> MergeResult:
        cursor.execute(
            """
            SELECT COUNT(*) AS Processed,
                   SUM(Outcome = 'CreatedOccurrence') AS Created,
                   SUM(Outcome = 'UpdatedOccurrence') AS Updated,
                   SUM(Outcome = 'DuplicateLinked') AS DuplicateLinked,
                   SUM(Outcome = 'Unchanged') AS Unchanged,
                   SUM(Outcome = 'Rejected') AS Rejected
            FROM ManifestImportEntry
            WHERE ManifestImportId = %s
            """,
            (import_id,),
        )
        row = cursor.fetchone()
        return MergeResult(
            processed=int(_value(row, "Processed") or 0),
            created=int(_value(row, "Created") or 0),
            updated=int(_value(row, "Updated") or 0),
            duplicates_linked=int(_value(row, "DuplicateLinked") or 0),
            unchanged=int(_value(row, "Unchanged") or 0),
            rejected=int(_value(row, "Rejected") or 0),
        )

    def current_result(self, claim: ManifestImportClaim) -> MergeResult:
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT ProcessedEntryCount AS Processed,
                           CreatedCount AS Created, UpdatedCount AS Updated,
                           DuplicateLinkedCount AS DuplicateLinked,
                           UnchangedCount AS Unchanged, RejectedCount AS Rejected
                    FROM ManifestImport
                    WHERE Id = %s AND UserId = %s AND MediaSourceId = %s
                    """,
                    (claim.internal_id, claim.user_id, claim.source_internal_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise BulkImportDatabaseError(
                        "BulkImportMissing", "The bulk import no longer exists."
                    )
                return MergeResult(
                    processed=int(_value(row, "Processed") or 0),
                    created=int(_value(row, "Created") or 0),
                    updated=int(_value(row, "Updated") or 0),
                    duplicates_linked=int(_value(row, "DuplicateLinked") or 0),
                    unchanged=int(_value(row, "Unchanged") or 0),
                    rejected=int(_value(row, "Rejected") or 0),
                )
        finally:
            connection.close()

    def iter_results(
        self, claim: ManifestImportClaim, *, fetch_size: int = 2_000
    ) -> Iterator[dict[str, Any]]:
        connection = self._connection_factory()
        try:
            with connection.cursor(SSDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT RowNumber AS rowNumber,
                           CASE
                               WHEN SourceItemId IS NOT NULL THEN SourceItemId
                               WHEN CHAR_LENGTH(SourceItemIdRaw) BETWEEN 1 AND 512
                                   THEN SourceItemIdRaw
                               ELSE CONCAT('__invalid_row__:',
                                           LPAD(RowNumber, 10, '0'))
                           END AS sourceItemId,
                           Outcome AS outcome,
                           OccurrencePublicId AS occurrenceId,
                           MediaAssetPublicId AS mediaAssetId,
                           DescriptionJobPublicId AS descriptionJobId,
                           ErrorCode AS errorCode, ErrorMessage AS errorMessage
                    FROM ManifestImportEntry
                    WHERE ManifestImportId = %s
                    ORDER BY RowNumber
                    """,
                    (claim.internal_id,),
                )
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    for row in rows:
                        yield {str(key): value for key, value in row.items()}
        finally:
            connection.close()

    def queued_geocode_job_batches(
        self, claim: ManifestImportClaim, *, batch_size: int = 100
    ) -> Iterator[tuple[UUID, ...]]:
        if not 1 <= batch_size <= 1_000:
            raise ValueError("Geocode dispatch batches must be between 1 and 1000")
        connection = self._connection_factory()
        try:
            with connection.cursor(SSDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT Job.PublicId
                    FROM ManifestImportEntry AS EntryRow
                    JOIN MediaAsset AS Asset
                      ON Asset.UserId = %s AND Asset.Id = EntryRow.ResolvedAssetId
                    JOIN ProcessingJob AS Job
                      ON Job.UserId = %s AND Job.MediaAssetId = Asset.Id
                     AND Job.IdempotencyKey = CONCAT(
                         'geocode:', Asset.PublicId, ':', EntryRow.CoordinateRevision)
                    WHERE EntryRow.ManifestImportId = %s
                      AND EntryRow.ValidationState = 'Valid'
                      AND EntryRow.CoordinateRevision IS NOT NULL
                      AND Job.JobType = 'Geocode' AND Job.Status = 'Queued'
                    ORDER BY Job.PublicId
                    """,
                    (claim.user_id, claim.user_id, claim.internal_id),
                )
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    yield tuple(UUID(str(_value(row, "PublicId"))) for row in rows)
        finally:
            connection.close()

    def due_import_ids(self, *, limit: int = 100) -> tuple[UUID, ...]:
        if not 1 <= limit <= 1_000:
            raise ValueError("Due import recovery limit must be between 1 and 1000")
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT PublicId
                    FROM ManifestImport
                    WHERE (
                        Status IN ('Queued','RetryDue')
                        AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= %s)
                    ) OR (
                        Status = 'Running' AND LeaseExpiresAtUtc <= %s
                    )
                    ORDER BY COALESCE(NextAttemptAtUtc, CreatedAtUtc), Id
                    LIMIT %s
                    """,
                    (now, now, limit),
                )
                return tuple(UUID(str(_value(row, "PublicId"))) for row in cursor.fetchall())
        finally:
            connection.close()

    def complete_result(
        self,
        claim: ManifestImportClaim,
        *,
        bucket: str,
        object_key: str,
        checksum_sha256: str,
        byte_size: int,
    ) -> None:
        if not bucket or not object_key or len(checksum_sha256) != 64 or byte_size <= 0:
            raise ValueError("The result artifact identity is invalid")
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                self._require_lease(cursor, claim, allowed_phases={"Merged", "WritingResult"})
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Status = IF(RejectedCount > 0, 'CompletedWithErrors', 'Succeeded'),
                        Phase = 'Complete', ActiveMarker = NULL,
                        ResultS3Bucket = %s, ResultS3ObjectKey = %s,
                        ResultChecksumSha256 = %s, ResultByteSize = %s,
                        LeaseTokenHash = NULL, LeaseExpiresAtUtc = NULL,
                        NextAttemptAtUtc = NULL, CompletedAtUtc = %s,
                        UpdatedAtUtc = %s
                    WHERE Id = %s AND LeaseTokenHash = %s
                    """,
                    (
                        bucket,
                        object_key,
                        checksum_sha256.lower(),
                        byte_size,
                        now,
                        now,
                        claim.internal_id,
                        self._lease_hash(claim.lease_owner),
                    ),
                )
                if cursor.rowcount != 1:
                    raise BulkImportDatabaseError(
                        "BulkImportLeaseLost", "The bulk import lease is no longer active."
                    )
                # The immutable result object now contains every per-row
                # outcome. Keep compact ManifestImportFailure rows for audit,
                # but release the large stage/work set in the same terminal
                # transaction so repeated full syncs reuse InnoDB pages.
                cursor.execute(
                    "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                    (claim.internal_id,),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def fail(
        self,
        claim: ManifestImportClaim,
        *,
        failure_class: str,
        code: str,
        message: str,
        retryable: bool,
    ) -> bool:
        now = self._clock()
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT AttemptCount, MaxAttempts
                    FROM ManifestImport
                    WHERE Id = %s AND LeaseTokenHash = %s
                    FOR UPDATE
                    """,
                    (claim.internal_id, self._lease_hash(claim.lease_owner)),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.rollback()
                    return False
                should_retry = retryable and int(_value(row, "AttemptCount")) < int(
                    _value(row, "MaxAttempts")
                )
                attempt_count = int(_value(row, "AttemptCount") or 1)
                next_attempt = now + timedelta(
                    seconds=min(3600, 300 * (2 ** max(0, attempt_count - 1)))
                )
                cursor.execute(
                    """
                    UPDATE ManifestImport
                    SET Status = %s, ActiveMarker = IF(%s, 1, NULL),
                        NextAttemptAtUtc = IF(%s, %s, NULL),
                        LeaseTokenHash = NULL, LeaseExpiresAtUtc = NULL,
                        FailureClass = %s, FailureCode = %s, FailureMessage = %s,
                        CompletedAtUtc = IF(%s, NULL, %s), UpdatedAtUtc = %s
                    WHERE Id = %s
                    """,
                    (
                        "RetryDue" if should_retry else "FailedPermanent",
                        should_retry,
                        should_retry,
                        next_attempt,
                        failure_class[:32],
                        code[:64],
                        message[:1000],
                        should_retry,
                        now,
                        now,
                        claim.internal_id,
                    ),
                )
                if not should_retry:
                    cursor.execute(
                        "DELETE FROM ManifestImportEntry WHERE ManifestImportId = %s",
                        (claim.internal_id,),
                    )
            connection.commit()
            return should_retry
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _require_lease(
        self,
        cursor: Any,
        claim: ManifestImportClaim,
        *,
        allowed_phases: set[str] | None = None,
    ) -> Mapping[str, Any]:
        cursor.execute(
            """
            SELECT Id, UserId, MediaSourceId, Status, Phase, LeaseTokenHash,
                   LeaseExpiresAtUtc, SchemaVersion, ManifestKind,
                   DeletionDetectionReliable
            FROM ManifestImport
            WHERE Id = %s
            FOR UPDATE
            """,
            (claim.internal_id,),
        )
        row = cursor.fetchone()
        now = self._clock()
        if (
            row is None
            or int(_value(row, "UserId")) != claim.user_id
            or int(_value(row, "MediaSourceId")) != claim.source_internal_id
            or _value(row, "Status") != "Running"
            or _value(row, "LeaseTokenHash") != self._lease_hash(claim.lease_owner)
            or _value(row, "LeaseExpiresAtUtc") is None
            or _value(row, "LeaseExpiresAtUtc") <= now
        ):
            raise BulkImportDatabaseError(
                "BulkImportLeaseLost", "The bulk import lease is no longer active."
            )
        if _value(row, "SchemaVersion") != "ManifestNdjsonV1":
            raise BulkImportDatabaseError(
                "BulkImportSchemaUnsupported", "The bulk manifest schema is unsupported."
            )
        if _value(row, "ManifestKind") != "Full" or bool(
            _value(row, "DeletionDetectionReliable")
        ):
            raise BulkImportDatabaseError(
                "BulkImportSemanticsUnsupported",
                "This bulk importer accepts Full hash-upsert manifests without inferred deletions.",
            )
        if allowed_phases is not None and str(_value(row, "Phase")) not in allowed_phases:
            raise BulkImportDatabaseError(
                "BulkImportPhaseInvalid", "The bulk import is not in the expected phase."
            )
        return row
