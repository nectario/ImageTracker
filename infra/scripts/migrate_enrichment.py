"""Apply the narrowly scoped additive ImageTracker migrations 012 through 014.

The runner is deliberately narrow and crash-reconcilable. If MySQL commits DDL
before the SchemaMigration marker is written, a rerun verifies the complete
target shape and records the missing ledger row instead of repeating unsafe DDL.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

import boto3
import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ImageTracker import _split_sql_statements  # noqa: E402


MIGRATIONS = {
    "012": ROOT / "migrations" / "012_AddProviderCircuit.sql",
    "013": ROOT / "migrations" / "013_WidenLocationProviderFields.sql",
    "014": ROOT / "migrations" / "014_CreateManifestImportTables.sql",
}
REQUIRED_BASE = {"007", "008", "009", "010", "011"}
CA_PATH = ROOT / "services" / "data" / "certs" / "us-east-2-bundle.pem"


def _secret(region: str, parameter_name: str) -> dict[str, Any]:
    response = boto3.client("ssm", region_name=region).get_parameter(
        Name=parameter_name,
        WithDecryption=True,
    )
    try:
        value = json.loads(response["Parameter"]["Value"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("The ImageTracker database parameter is invalid") from exc
    if not isinstance(value, dict) or value.get("database") != "ImageTracker":
        raise RuntimeError("The database credential must be scoped to ImageTracker")
    return value


def _admin_environment() -> dict[str, Any] | None:
    values = {
        "host": os.environ.get("MYSQL_HOST"),
        "port": os.environ.get("MYSQL_PORT", "3306"),
        "user": os.environ.get("MYSQL_USERNAME")
        or os.environ.get("MYSQL_USERID"),
        "password": os.environ.get("MYSQL_PASSWORD"),
    }
    supplied = [bool(value) for value in values.values()]
    if not any(supplied):
        return None
    if not all(supplied):
        raise RuntimeError("The MYSQL_* administrative environment is incomplete")
    return {**values, "database": "ImageTracker", "tls": True}


def _connect(
    secret: dict[str, Any],
    *,
    local_infile: bool = False,
    timeout_seconds: int = 30,
) -> pymysql.Connection:
    user = secret.get("user") or secret.get("username")
    password = secret.get("password")
    host = secret.get("host")
    if not all(isinstance(value, str) and value for value in (user, password, host)):
        raise RuntimeError("The ImageTracker database parameter is incomplete")
    options: dict[str, Any] = {}
    if bool(secret.get("tls", True)):
        options["ssl"] = {"ca": str(CA_PATH), "check_hostname": True}
    return pymysql.connect(
        host=host,
        port=int(secret.get("port", 3306)),
        user=user,
        password=password,
        database="ImageTracker",
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
        read_timeout=timeout_seconds,
        write_timeout=timeout_seconds,
        cursorclass=DictCursor,
        local_infile=local_infile,
        **options,
    )


def _versions(connection: pymysql.Connection) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT `Version` FROM `SchemaMigration` ORDER BY `Version`"
        )
        return {str(row["Version"]) for row in cursor.fetchall()}


def _columns(connection: pymysql.Connection, table: str) -> dict[str, int | None]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'ImageTracker' AND TABLE_NAME = %s
            """,
            (table,),
        )
        return {
            str(row["COLUMN_NAME"]): (
                int(row["CHARACTER_MAXIMUM_LENGTH"])
                if row["CHARACTER_MAXIMUM_LENGTH"] is not None
                else None
            )
            for row in cursor.fetchall()
        }


def _indexes(connection: pymysql.Connection, table: str) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT INDEX_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = 'ImageTracker' AND TABLE_NAME = %s
            """,
            (table,),
        )
        return {str(row["INDEX_NAME"]) for row in cursor.fetchall()}


def _constraints(connection: pymysql.Connection, table: str) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = 'ImageTracker' AND TABLE_NAME = %s
            """,
            (table,),
        )
        return {str(row["CONSTRAINT_NAME"]) for row in cursor.fetchall()}


def _satisfied(connection: pymysql.Connection, version: str) -> bool:
    if version == "012":
        columns = _columns(connection, "ProviderUsageMonth")
        return {
            "CircuitState",
            "CircuitOpenedAtUtc",
            "CircuitFailureCode",
        }.issubset(columns)
    if version == "013":
        columns = _columns(connection, "MediaLocation")
        return (
            (columns.get("ProviderPlaceId") or 0) >= 500
            and (columns.get("PostalCode") or 0) >= 50
        )
    if version == "014":
        required_columns = {
            "ManifestImport": {
                "Id",
                "PublicId",
                "UserId",
                "MediaSourceId",
                "SnapshotId",
                "IdempotencyKey",
                "RequestSha256",
                "ActiveMarker",
                "ManifestKind",
                "PermissionState",
                "DeletionDetectionReliable",
                "ClientCursor",
                "SchemaVersion",
                "Status",
                "Phase",
                "InputS3Bucket",
                "InputS3ObjectKey",
                "InputS3VersionId",
                "InputChecksumSha256",
                "InputByteSize",
                "DeclaredEntryCount",
                "ValidatedEntryCount",
                "ProcessedEntryCount",
                "CreatedCount",
                "UpdatedCount",
                "DuplicateLinkedCount",
                "DeletedCount",
                "IgnoredDeletionCount",
                "UnchangedCount",
                "RejectedCount",
                "ResultS3Bucket",
                "ResultS3ObjectKey",
                "ResultChecksumSha256",
                "ResultByteSize",
                "AttemptCount",
                "MaxAttempts",
                "NextAttemptAtUtc",
                "LeaseTokenHash",
                "LeaseExpiresAtUtc",
                "FailureClass",
                "FailureCode",
                "FailureMessage",
                "UploadExpiresAtUtc",
                "QueuedAtUtc",
                "StartedAtUtc",
                "CompletedAtUtc",
                "CreatedAtUtc",
                "UpdatedAtUtc",
            },
            "ManifestImportEntry": {
                "StageId",
                "ManifestImportId",
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
                "Operation",
                "SourceItemId",
                "SourceRevision",
                "OriginalFileName",
                "LocalLocator",
                "ContentSha256",
                "MediaType",
                "MimeType",
                "ByteSize",
                "WidthPixels",
                "HeightPixels",
                "DurationMilliseconds",
                "CaptureDateTimeLocal",
                "CaptureDateTimeUtc",
                "TimeZone",
                "UtcOffsetMinutes",
                "Latitude",
                "Longitude",
                "AltitudeMeters",
                "AccuracyMeters",
                "CoordinateRevision",
                "ProvenanceJson",
                "LocationSource",
                "ValidationState",
                "ExistingOccurrenceId",
                "ExistingAssetId",
                "ResolvedAssetId",
                "Outcome",
                "ErrorCode",
                "ErrorMessage",
                "OccurrencePublicId",
                "MediaAssetPublicId",
                "DescriptionJobPublicId",
                "CreatedAtUtc",
                "UpdatedAtUtc",
            },
            "ManifestImportAssetWork": {
                "ManifestImportId",
                "ContentSha256",
                "CanonicalStageId",
                "CanonicalRowNumber",
                "ResolvedMediaAssetId",
                "ResolvedMediaAssetPublicId",
                "AssetWasPreexisting",
                "AssetCreated",
                "AssetChanged",
                "ErrorCode",
                "ErrorMessage",
                "CreatedAtUtc",
                "UpdatedAtUtc",
            },
            "ManifestImportFailure": {
                "Id",
                "PublicId",
                "UserId",
                "ManifestImportId",
                "RowNumber",
                "SourceItemId",
                "SourceRevision",
                "Operation",
                "ErrorCode",
                "ErrorMessage",
                "CreatedAtUtc",
            },
        }
        if any(
            not columns.issubset(_columns(connection, table))
            for table, columns in required_columns.items()
        ):
            return False
        required_indexes = {
            "ManifestImport": {
                "Ux_ManifestImport_PublicId",
                "Ux_ManifestImport_User_Idempotency",
                "Ux_ManifestImport_User_Source_Snapshot",
                "Ux_ManifestImport_User_Source_Active",
                "Ix_ManifestImport_Status_NextAttempt",
            },
            "ManifestImportEntry": {
                "Ux_ManifestImportEntry_Import_Row",
                "Ix_ManifestImportEntry_Import_SourceItem",
                "Ix_ManifestImportEntry_Import_Hash",
            },
            "ManifestImportAssetWork": {
                "PRIMARY",
                "Ix_ManifestImportAssetWork_Import_ResolvedAsset",
            },
            "ManifestImportFailure": {
                "Ux_ManifestImportFailure_Import_Row",
                "Ix_ManifestImportFailure_User_Import",
            },
            "MediaOccurrence": {
                "Ix_MediaOccurrence_User_Asset_DeletionState",
            },
        }
        if not all(
            names.issubset(_indexes(connection, table))
            for table, names in required_indexes.items()
        ):
            return False
        required_constraints = {
            "ManifestImport": {"Fk_ManifestImport_MediaSource"},
            "ManifestImportEntry": {"Fk_ManifestImportEntry_ManifestImport"},
            "ManifestImportAssetWork": {
                "Fk_ManifestImportAssetWork_CanonicalEntry"
            },
            "ManifestImportFailure": {"Fk_ManifestImportFailure_ManifestImport"},
        }
        return all(
            names.issubset(_constraints(connection, table))
            for table, names in required_constraints.items()
        )
    raise ValueError(f"Unsupported migration version: {version}")


def _migration_statements(version: str, migration_path: Path) -> list[str]:
    statements = _split_sql_statements(migration_path.read_text(encoding="utf-8"))
    if version in {"012", "013"}:
        if len(statements) != 1 or not statements[0].lstrip().upper().startswith("ALTER TABLE"):
            raise RuntimeError(
                f"Migration {version} must contain exactly one atomic ALTER"
            )
        return statements
    if version == "014":
        if len(statements) != 5:
            raise RuntimeError(
                "Migration 014 must contain four additive CREATE TABLE statements "
                "and one online index ALTER"
            )
        if any(
            not statement.lstrip().upper().startswith("CREATE TABLE IF NOT EXISTS")
            for statement in statements[:4]
        ) or not statements[4].lstrip().upper().startswith(
            "ALTER TABLE `MEDIAOCCURRENCE`"
        ):
            raise RuntimeError("Migration 014 contains an unsupported DDL statement")
        return statements
    raise ValueError(f"Unsupported migration version: {version}")


def _apply_migration(
    connection: pymysql.Connection, version: str, migration_path: Path
) -> None:
    statements = _migration_statements(version, migration_path)
    if version != "014":
        with connection.cursor() as cursor:
            cursor.execute(statements[0])
        return

    # CREATE TABLE IF NOT EXISTS makes every committed table step replay-safe.
    # MySQL has no portable ADD INDEX IF NOT EXISTS, so reconcile that final
    # online ALTER independently after a crash before attempting it again.
    with connection.cursor() as cursor:
        for statement in statements[:4]:
            cursor.execute(statement)
    if "Ix_MediaOccurrence_User_Asset_DeletionState" not in _indexes(
        connection, "MediaOccurrence"
    ):
        with connection.cursor() as cursor:
            cursor.execute(statements[4])


def _assert_idle(connection: pymysql.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM `UploadSession`
               WHERE `Status` IN ('Preparing','Uploading','Completing')) AS ActiveUploads,
              (SELECT COUNT(*) FROM `ProcessingJob`
               WHERE `Status` IN ('Queued','Running')) AS ActiveJobs
            """
        )
        row = cursor.fetchone()
    if int(row["ActiveUploads"] or 0) or int(row["ActiveJobs"] or 0):
        raise RuntimeError(
            "ImageTracker processing must be idle before schema migration"
        )


def _requires_idle(connection: pymysql.Connection, versions: set[str]) -> bool:
    """Only the legacy table-rewriting ALTERs require a quiet database."""

    return any(
        version not in versions and not _satisfied(connection, version)
        for version in ("012", "013")
    )


def _record(
    connection: pymysql.Connection, version: str, migration_path: Path
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO `SchemaMigration` (`Version`, `Name`, `AppliedAtUtc`)
            VALUES (%s, %s, %s)
            """,
            (
                version,
                migration_path.name,
                datetime.now(timezone.utc).replace(tzinfo=None),
            ),
        )
    connection.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--region",
        default=os.environ.get("IMAGETRACKER_AWS_REGION", "us-east-2"),
    )
    parser.add_argument(
        "--parameter",
        default=os.environ.get(
            "IMAGETRACKER_DB_SECRET_PARAMETER", "/imagetracker/prod/mysql"
        ),
    )
    args = parser.parse_args()

    connection = _connect(
        _admin_environment() or _secret(args.region, args.parameter)
    )
    actions: list[dict[str, str]] = []
    try:
        versions = _versions(connection)
        if not REQUIRED_BASE.issubset(versions):
            missing = sorted(REQUIRED_BASE - versions)
            raise RuntimeError(
                f"Required base migrations are missing: {', '.join(missing)}"
            )
        if _requires_idle(connection, versions):
            _assert_idle(connection)
        for version, path in MIGRATIONS.items():
            if version in versions:
                if not _satisfied(connection, version):
                    raise RuntimeError(
                        f"Migration {version} is recorded but its schema is incomplete"
                    )
                actions.append({"version": version, "action": "AlreadyApplied"})
                continue
            satisfied = _satisfied(connection, version)
            action = (
                "ReconcileLedger"
                if satisfied
                else "ApplyAdditiveSchema"
                if version == "014"
                else "ApplyAlter"
            )
            actions.append({"version": version, "action": action})
            if not args.apply:
                continue
            if not satisfied:
                _apply_migration(connection, version, path)
                if not _satisfied(connection, version):
                    raise RuntimeError(
                        f"Migration {version} did not produce its required schema"
                    )
            _record(connection, version, path)
            versions.add(version)
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "database": "ImageTracker",
                "mode": "apply" if args.apply else "dry-run",
                "actions": actions,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
