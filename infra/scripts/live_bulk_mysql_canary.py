"""Run a self-cleaning MySQL canary for the asynchronous bulk importer.

The command is read-only unless ``--apply`` is supplied.  It deliberately uses
the application credential from SSM, targets one synthetic UUID-prefixed Local
source, and never accepts a user source name or path.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sys
import tempfile
from typing import Any, Callable, Iterable, Mapping, Sequence
from uuid import UUID, uuid4, uuid5

import boto3
from botocore.exceptions import ClientError
import pymysql
from pymysql.cursors import DictCursor


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cli.imagetracker_cli.bulk import write_manifest_gzip  # noqa: E402
from services.bulk.manifest import (  # noqa: E402
    ManifestGuardrails,
    parse_manifest_gzip,
    write_result_gzip,
)
from services.bulk.repository import (  # noqa: E402
    ManifestImportClaim,
    MySqlManifestImportRepository,
)
from services.data.database import (  # noqa: E402
    DatabaseConnectionConfig,
    SsmParameterResolver,
    database_config_from_secret,
)


CANARY_NAMESPACE = UUID("c1638a72-77fc-4ba8-93f3-b95eec32aa1b")
CANARY_LABEL = "imagetracker-bulk-db-canary"
DEFAULT_PARAMETER = "/imagetracker/prod/mysql"
PARAMETER_PATTERN = re.compile(r"^/imagetracker/[a-z0-9][a-z0-9-]*/mysql$")
EXPECTED_SCHEMA_TABLES = {
    "ManifestImport",
    "ManifestImportEntry",
    "ManifestImportAssetWork",
    "ManifestImportFailure",
}


class CanaryError(RuntimeError):
    """A fail-closed canary precondition or acceptance check failed."""


class CanaryRunFailed(Exception):
    def __init__(self, report: Mapping[str, Any]) -> None:
        super().__init__("Bulk MySQL canary failed")
        self.report = dict(report)


@dataclass(frozen=True)
class CanaryTarget:
    run_id: UUID
    device_public_id: UUID
    source_public_id: UUID
    import_public_id: UUID
    snapshot_id: UUID
    prefix: str
    device_key: str
    source_key: str
    device_name: str
    source_name: str
    idempotency_key: str
    request_sha256: str
    asset_hashes: tuple[str, str, str]
    local_locator_prefix: str

    @classmethod
    def build(cls, run_id: UUID) -> "CanaryTarget":
        run_text = str(run_id)
        prefix = f"{run_text}:{CANARY_LABEL}"
        hashes = tuple(
            hashlib.sha256(f"{prefix}:asset:{name}".encode("utf-8")).hexdigest()
            for name in ("duplicate", "unique", "rejected")
        )
        target = cls(
            run_id=run_id,
            device_public_id=uuid5(CANARY_NAMESPACE, f"{run_text}:device"),
            source_public_id=uuid5(CANARY_NAMESPACE, f"{run_text}:source"),
            import_public_id=uuid5(CANARY_NAMESPACE, f"{run_text}:import"),
            snapshot_id=uuid5(CANARY_NAMESPACE, f"{run_text}:snapshot"),
            prefix=prefix,
            device_key=f"{prefix}:device",
            source_key=f"{prefix}:source",
            device_name=f"{run_text}: ImageTracker Bulk DB Canary Device",
            source_name=f"{run_text}: ImageTracker Bulk DB Canary Source",
            idempotency_key=f"{run_text}:bulk-db-canary-import",
            request_sha256=hashlib.sha256(
                f"{prefix}:request".encode("utf-8")
            ).hexdigest(),
            asset_hashes=hashes,  # type: ignore[arg-type]
            local_locator_prefix=f"/__imagetracker_bulk_db_canary__/{run_text}/",
        )
        _validate_target(target)
        return target


@dataclass(frozen=True)
class Preflight:
    account_id: int
    account_public_id: UUID
    migration_014_present: bool
    schema_ready: bool
    local_infile_enabled: bool
    active_import_count: int

    def as_json(self) -> dict[str, Any]:
        return {
            "activeAccountCount": 1,
            "migration014Present": self.migration_014_present,
            "schemaReady": self.schema_ready,
            "localInfileEnabled": self.local_infile_enabled,
            "activeManifestImports": self.active_import_count,
        }


ConnectionFactory = Callable[[], Any]


def _is_wsl() -> bool:
    if sys.platform != "linux":
        return False
    try:
        release = platform.release().casefold()
        version = Path("/proc/version").read_text(encoding="utf-8").casefold()
    except OSError:
        return False
    return "microsoft" in release or "microsoft" in version


def _require_wsl() -> None:
    if not _is_wsl():
        raise CanaryError(
            "Run the bulk database canary inside WSL Ubuntu so it uses the "
            "configured WSL AWS credentials."
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CanaryError(message)


def _safe_error(error: BaseException) -> str:
    if isinstance(error, CanaryError):
        return str(error)
    if isinstance(error, ClientError):
        details = error.response.get("Error", {})
        code = str(details.get("Code") or "Unknown")
        operation = getattr(error, "operation_name", "AWS operation")
        return f"{operation} failed with AWS error {code}"
    return f"{type(error).__name__}: the operation failed; secret-bearing details were suppressed"


def _validate_parameter_name(value: str) -> str:
    selected = value.strip()
    if not PARAMETER_PATTERN.fullmatch(selected):
        raise CanaryError(
            "The database parameter must be an ImageTracker app credential path "
            "ending in /mysql."
        )
    return selected


def _validate_target(target: CanaryTarget) -> None:
    run_text = str(target.run_id)
    _require(target.prefix == f"{run_text}:{CANARY_LABEL}", "Canary prefix is invalid")
    for value in (
        target.device_key,
        target.source_key,
        target.device_name,
        target.source_name,
        target.idempotency_key,
    ):
        _require("my photos" not in value.casefold(), "The canary may never target My Photos")
        _require(value.startswith(run_text), "Every synthetic identity must be UUID-prefixed")
    _require(
        target.device_public_id == uuid5(CANARY_NAMESPACE, f"{run_text}:device")
        and target.source_public_id == uuid5(CANARY_NAMESPACE, f"{run_text}:source")
        and target.import_public_id == uuid5(CANARY_NAMESPACE, f"{run_text}:import")
        and target.snapshot_id == uuid5(CANARY_NAMESPACE, f"{run_text}:snapshot"),
        "Synthetic public IDs are not the exact deterministic canary IDs",
    )
    _require(
        len(set(target.asset_hashes)) == 3
        and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in target.asset_hashes),
        "Canary content hashes are invalid",
    )
    _require(
        target.local_locator_prefix
        == f"/__imagetracker_bulk_db_canary__/{run_text}/",
        "Canary Local locator prefix is invalid",
    )


def _bundled_ca(region: str) -> Path:
    path = ROOT / "services" / "data" / "certs" / f"{region}-bundle.pem"
    if not path.is_file():
        raise CanaryError("No bundled Amazon RDS CA is available for this region")
    return path


def _connect_factory(config: DatabaseConnectionConfig, *, region: str) -> ConnectionFactory:
    url = config.url
    if url.database != "ImageTracker":
        raise CanaryError("The canary credential is not scoped to ImageTracker")
    ssl: dict[str, Any] | None = None
    if config.tls_enabled:
        ca = Path(config.ssl_ca) if config.ssl_ca else _bundled_ca(region)
        if not ca.is_file():
            raise CanaryError("The configured Amazon RDS CA file is unavailable")
        ssl = {"ca": str(ca), "check_hostname": True}

    def connect() -> Any:
        return pymysql.connect(
            host=str(url.host),
            port=int(url.port or 3306),
            user=str(url.username),
            password=str(url.password or ""),
            database="ImageTracker",
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=10,
            read_timeout=900,
            write_timeout=900,
            cursorclass=DictCursor,
            local_infile=True,
            ssl=ssl,
        )

    return connect


def _connection_factory_from_ssm(args: argparse.Namespace) -> ConnectionFactory:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    resolver = SsmParameterResolver(
        region_name=args.region,
        client=session.client("ssm", region_name=args.region),
    )
    raw = resolver.resolve(_validate_parameter_name(args.parameter))
    config = database_config_from_secret(raw, required_database="ImageTracker")
    return _connect_factory(config, region=args.region)


def _scalar(cursor: Any, sql: str, params: Sequence[Any] = ()) -> Any:
    cursor.execute(sql, tuple(params))
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    return row[0]


def _count(cursor: Any, sql: str, params: Sequence[Any] = ()) -> int:
    return int(_scalar(cursor, sql, params) or 0)


def _target_collision_count(cursor: Any, target: CanaryTarget, account_id: int) -> int:
    checks = (
        (
            "SELECT COUNT(*) AS Value FROM Device WHERE PublicId = %s "
            "OR (UserId = %s AND DeviceKey LIKE %s)",
            (str(target.device_public_id), account_id, f"{target.prefix}%"),
        ),
        (
            "SELECT COUNT(*) AS Value FROM MediaSource WHERE PublicId = %s "
            "OR (UserId = %s AND SourceKey LIKE %s)",
            (str(target.source_public_id), account_id, f"{target.prefix}%"),
        ),
        (
            "SELECT COUNT(*) AS Value FROM MediaAsset WHERE UserId = %s "
            "AND ContentSha256 IN (%s, %s, %s)",
            (account_id, *target.asset_hashes),
        ),
    )
    return sum(_count(cursor, sql, params) for sql, params in checks)


def _manifest_collision_count(cursor: Any, target: CanaryTarget, account_id: int) -> int:
    return _count(
        cursor,
        "SELECT COUNT(*) AS Value FROM ManifestImport WHERE UserId = %s AND ("
        "PublicId = %s OR SnapshotId = %s OR IdempotencyKey = %s)",
        (
            account_id,
            str(target.import_public_id),
            str(target.snapshot_id),
            target.idempotency_key,
        ),
    )


def _preflight(
    connection: Any,
    target: CanaryTarget,
    *,
    require_migration: bool,
) -> Preflight:
    _validate_target(target)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            cursor.execute("START TRANSACTION READ ONLY")
            database_name = _scalar(cursor, "SELECT DATABASE() AS Value")
            _require(database_name == "ImageTracker", "Canary connection escaped ImageTracker")
            local_infile = bool(
                int(_scalar(cursor, "SELECT @@local_infile AS Value") or 0)
            )
            cursor.execute(
                "SELECT Id, PublicId FROM UserAccount "
                "WHERE AccountStatus = 'Active' AND DeletedAtUtc IS NULL ORDER BY Id LIMIT 2"
            )
            accounts = list(cursor.fetchall())
            _require(
                len(accounts) == 1,
                "Bulk canary requires exactly one active ImageTracker account",
            )
            account_id = int(accounts[0]["Id"])
            account_public_id = UUID(str(accounts[0]["PublicId"]))
            migration_present = bool(
                _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM SchemaMigration WHERE Version = '014'",
                )
            )
            cursor.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = 'ImageTracker' "
                "AND TABLE_NAME IN (%s, %s, %s, %s)",
                tuple(sorted(EXPECTED_SCHEMA_TABLES)),
            )
            schema_tables = {str(row["TABLE_NAME"]) for row in cursor.fetchall()}
            schema_ready = schema_tables == EXPECTED_SCHEMA_TABLES
            active_import_count = (
                _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM ManifestImport WHERE ActiveMarker = 1",
                )
                if schema_ready
                else 0
            )
            _require(
                _target_collision_count(cursor, target, account_id) == 0,
                "Synthetic canary target collides with existing application rows",
            )
            if schema_ready:
                _require(
                    _manifest_collision_count(cursor, target, account_id) == 0,
                    "Synthetic canary import identity already exists",
                )
            if require_migration:
                _require(migration_present, "Migration 014 is not recorded")
                _require(schema_ready, "Migration 014 tables are incomplete")
                _require(local_infile, "MySQL local_infile is disabled")
                _require(
                    active_import_count == 0,
                    "Another manifest import is active; the canary will not race it",
                )
        connection.rollback()
        return Preflight(
            account_id=account_id,
            account_public_id=account_public_id,
            migration_014_present=migration_present,
            schema_ready=schema_ready,
            local_infile_enabled=local_infile,
            active_import_count=active_import_count,
        )
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _manifest_entries(target: CanaryTarget) -> list[dict[str, Any]]:
    duplicate_hash, unique_hash, rejected_hash = target.asset_hashes

    def entry(row: int, content_hash: str, byte_size: int) -> dict[str, Any]:
        file_name = f"bulk-canary-{row}.nef"
        return {
            "operation": "Upsert",
            "sourceItemId": f"{target.run_id}:row:{row}",
            "sourceRevision": hashlib.sha256(
                f"{target.prefix}:revision:{row}".encode("utf-8")
            ).hexdigest(),
            "fileName": file_name,
            "localLocator": f"{target.local_locator_prefix}{file_name}",
            "contentSha256": content_hash,
            "mediaType": "Photo",
            "mimeType": "image/x-nikon-nef",
            "byteSize": byte_size,
            "widthPixels": 6000,
            "heightPixels": 4000,
            "provenance": [],
        }

    values = [
        entry(1, duplicate_hash, 101),
        entry(2, duplicate_hash, 101),
        entry(3, unique_hash, 202),
        entry(4, rejected_hash, 0),
    ]
    _validate_manifest_entries(values, target)
    return values


def _validate_manifest_entries(
    entries: Sequence[Mapping[str, Any]], target: CanaryTarget
) -> None:
    _require(len(entries) == 4, "Canary manifest must contain exactly four rows")
    _require(
        all(str(item.get("fileName") or "").casefold().endswith(".nef") for item in entries),
        "Canary rows must all use the no-description NEF extension",
    )
    _require(
        all("location" not in item for item in entries),
        "Canary rows must not contain GPS metadata",
    )
    _require(
        [str(item.get("contentSha256")) for item in entries]
        == [
            target.asset_hashes[0],
            target.asset_hashes[0],
            target.asset_hashes[1],
            target.asset_hashes[2],
        ],
        "Canary manifest hash layout is invalid",
    )
    _require(int(entries[3].get("byteSize") or 0) == 0, "Fourth row must be rejected")


def _insert_control_rows(
    factory: ConnectionFactory,
    target: CanaryTarget,
    preflight: Preflight,
    *,
    input_sha256: str,
    input_bytes: int,
) -> tuple[int, int]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT Id, PublicId FROM UserAccount WHERE AccountStatus = 'Active' "
                "AND DeletedAtUtc IS NULL ORDER BY Id LIMIT 2 FOR UPDATE"
            )
            accounts = list(cursor.fetchall())
            _require(
                len(accounts) == 1
                and int(accounts[0]["Id"]) == preflight.account_id
                and str(accounts[0]["PublicId"]) == str(preflight.account_public_id),
                "Selected active account changed",
            )
            _require(
                _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM ManifestImport WHERE ActiveMarker = 1",
                )
                == 0,
                "Another manifest import became active; canary refused to race it",
            )
            _require(
                _target_collision_count(cursor, target, preflight.account_id) == 0,
                "Synthetic canary target is no longer empty",
            )
            _require(
                _manifest_collision_count(cursor, target, preflight.account_id) == 0,
                "Synthetic canary import identity is no longer empty",
            )
            cursor.execute(
                "INSERT INTO Device (PublicId, UserId, DeviceKey, DisplayName, Platform, "
                "AppVersion, CreatedAtUtc, UpdatedAtUtc) VALUES (%s, %s, %s, %s, "
                "'LinuxCLI', 'bulk-db-canary-v1', %s, %s)",
                (
                    str(target.device_public_id),
                    preflight.account_id,
                    target.device_key,
                    target.device_name,
                    now,
                    now,
                ),
            )
            device_id = int(cursor.lastrowid)
            cursor.execute(
                "INSERT INTO MediaSource (PublicId, UserId, DeviceId, SourceKey, "
                "DisplayName, SourceType, StorageMode, PermissionState, SourceStatus, "
                "CreatedAtUtc, UpdatedAtUtc) VALUES (%s, %s, %s, %s, %s, 'Folder', "
                "'Local', 'NotApplicable', 'Active', %s, %s)",
                (
                    str(target.source_public_id),
                    preflight.account_id,
                    device_id,
                    target.source_key,
                    target.source_name,
                    now,
                    now,
                ),
            )
            source_id = int(cursor.lastrowid)
            cursor.execute(
                "INSERT INTO ManifestImport (PublicId, UserId, MediaSourceId, SnapshotId, "
                "IdempotencyKey, RequestSha256, ActiveMarker, ManifestKind, PermissionState, "
                "DeletionDetectionReliable, ClientCursor, SchemaVersion, Status, Phase, "
                "InputS3Bucket, InputS3ObjectKey, InputChecksumSha256, InputByteSize, "
                "DeclaredEntryCount, AttemptCount, MaxAttempts, NextAttemptAtUtc, "
                "QueuedAtUtc, CreatedAtUtc, UpdatedAtUtc) VALUES (%s, %s, %s, %s, %s, "
                "%s, 1, 'Full', 'NotApplicable', 0, %s, 'ManifestNdjsonV1', 'Queued', "
                "'Queued', 'imagetracker-bulk-db-canary', %s, %s, %s, 4, 0, 2, %s, %s, %s, %s)",
                (
                    str(target.import_public_id),
                    preflight.account_id,
                    source_id,
                    str(target.snapshot_id),
                    target.idempotency_key,
                    target.request_sha256,
                    target.prefix,
                    f"manifests/canary/{target.run_id}/input.ndjson.gz",
                    input_sha256,
                    input_bytes,
                    now,
                    now,
                    now,
                    now,
                ),
            )
        connection.commit()
        return device_id, source_id
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _read_result(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        records = [json.loads(line) for line in handle]
    _require(len(records) == 5, "Result must contain one header and four rows")
    header, rows = records[0], records[1:]
    _require(header.get("recordType") == "Result", "Result header is invalid")
    _require(
        [row.get("rowNumber") for row in rows] == [1, 2, 3, 4],
        "Result row order is invalid",
    )
    return header, rows


def _validate_database_state(
    factory: ConnectionFactory,
    target: CanaryTarget,
    preflight: Preflight,
    *,
    source_id: int,
) -> dict[str, Any]:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT Id, Status, Phase, ProcessedEntryCount, CreatedCount, "
                "UpdatedCount, DuplicateLinkedCount, RejectedCount FROM ManifestImport "
                "WHERE UserId = %s AND PublicId = %s AND MediaSourceId = %s",
                (preflight.account_id, str(target.import_public_id), source_id),
            )
            import_row = cursor.fetchone()
            _require(import_row is not None, "Synthetic import row is missing")
            _require(
                import_row["Status"] == "CompletedWithErrors"
                and import_row["Phase"] == "Complete"
                and int(import_row["ProcessedEntryCount"]) == 4
                and int(import_row["CreatedCount"]) == 2
                and int(import_row["UpdatedCount"]) == 0
                and int(import_row["DuplicateLinkedCount"]) == 1
                and int(import_row["RejectedCount"]) == 1,
                "Synthetic import counters are incorrect",
            )
            import_id = int(import_row["Id"])
            cursor.execute(
                "SELECT Id, ContentSha256, StorageState, S3Bucket, OriginalS3ObjectKey "
                "FROM MediaAsset WHERE UserId = %s AND ContentSha256 IN (%s, %s, %s) "
                "ORDER BY ContentSha256",
                (preflight.account_id, *target.asset_hashes),
            )
            assets = list(cursor.fetchall())
            _require(len(assets) == 2, "Canary did not create exactly two assets")
            _require(
                {row["ContentSha256"] for row in assets}
                == {target.asset_hashes[0], target.asset_hashes[1]},
                "Canary created an unexpected asset hash",
            )
            _require(
                all(
                    row["StorageState"] == "LocalOnly"
                    and row["S3Bucket"] is None
                    and row["OriginalS3ObjectKey"] is None
                    for row in assets
                ),
                "Canary assets unexpectedly reference uploaded objects",
            )
            asset_ids = tuple(int(row["Id"]) for row in assets)
            cursor.execute(
                "SELECT Id, SourceItemId, LocalLocator FROM MediaOccurrence "
                "WHERE UserId = %s AND MediaSourceId = %s ORDER BY SourceItemId",
                (preflight.account_id, source_id),
            )
            occurrences = list(cursor.fetchall())
            _require(len(occurrences) == 3, "Canary did not create three occurrences")
            _require(
                all(
                    str(row["SourceItemId"]).startswith(f"{target.run_id}:row:")
                    and str(row["LocalLocator"]).startswith(target.local_locator_prefix)
                    for row in occurrences
                ),
                "Canary occurrence scope is invalid",
            )
            occurrence_ids = tuple(int(row["Id"]) for row in occurrences)
            failure_count = _count(
                cursor,
                "SELECT COUNT(*) AS Value FROM ManifestImportFailure "
                "WHERE UserId = %s AND ManifestImportId = %s",
                (preflight.account_id, import_id),
            )
            stage_count = _count(
                cursor,
                "SELECT COUNT(*) AS Value FROM ManifestImportEntry WHERE ManifestImportId = %s",
                (import_id,),
            )
            asset_placeholders = ",".join("%s" for _ in asset_ids)
            occurrence_placeholders = ",".join("%s" for _ in occurrence_ids)
            jobs = _count(
                cursor,
                f"SELECT COUNT(*) AS Value FROM ProcessingJob WHERE UserId = %s "
                f"AND (MediaSourceId = %s OR MediaAssetId IN ({asset_placeholders}))",
                (preflight.account_id, source_id, *asset_ids),
            )
            locations = _count(
                cursor,
                f"SELECT COUNT(*) AS Value FROM MediaLocation WHERE UserId = %s "
                f"AND MediaAssetId IN ({asset_placeholders})",
                (preflight.account_id, *asset_ids),
            )
            uploads = _count(
                cursor,
                f"SELECT COUNT(*) AS Value FROM UploadSession WHERE UserId = %s "
                f"AND (MediaAssetId IN ({asset_placeholders}) "
                f"OR MediaOccurrenceId IN ({occurrence_placeholders}))",
                (preflight.account_id, *asset_ids, *occurrence_ids),
            )
            _require(failure_count == 1, "Canary did not retain one rejected-row audit")
            _require(stage_count == 0, "Canary staging rows were not released")
            _require(jobs == 0, "NEF/no-GPS canary unexpectedly created jobs")
            _require(locations == 0, "No-GPS canary unexpectedly created locations")
            _require(uploads == 0, "Local canary unexpectedly created uploads")
        connection.rollback()
        return {
            "assets": len(assets),
            "occurrences": len(occurrences),
            "rejections": failure_count,
            "jobs": jobs,
            "locations": locations,
            "uploads": uploads,
            "stagingRows": stage_count,
        }
    finally:
        connection.close()


def _cleanup(
    factory: ConnectionFactory,
    target: CanaryTarget,
    *,
    expected_user_id: int,
) -> dict[str, int]:
    """Delete only the exact synthetic graph, or roll back without broadening scope."""

    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT Id, UserId, DeviceKey, DisplayName FROM Device "
                "WHERE PublicId = %s OR (UserId = %s AND DeviceKey LIKE %s) FOR UPDATE",
                (str(target.device_public_id), expected_user_id, f"{target.prefix}%"),
            )
            devices = list(cursor.fetchall())
            _require(len(devices) <= 1, "Cleanup found multiple synthetic devices")
            for row in devices:
                _require(
                    int(row["UserId"]) == expected_user_id
                    and row["DeviceKey"] == target.device_key
                    and row["DisplayName"] == target.device_name,
                    "Cleanup device scope is not exact",
                )
            device_ids = tuple(int(row["Id"]) for row in devices)

            cursor.execute(
                "SELECT Id, UserId, DeviceId, SourceKey, DisplayName FROM MediaSource "
                "WHERE PublicId = %s OR (UserId = %s AND SourceKey LIKE %s) FOR UPDATE",
                (str(target.source_public_id), expected_user_id, f"{target.prefix}%"),
            )
            sources = list(cursor.fetchall())
            _require(len(sources) <= 1, "Cleanup found multiple synthetic sources")
            for row in sources:
                _require(
                    int(row["UserId"]) == expected_user_id
                    and row["SourceKey"] == target.source_key
                    and row["DisplayName"] == target.source_name
                    and (not device_ids or int(row["DeviceId"]) in device_ids),
                    "Cleanup source scope is not exact",
                )
            source_ids = tuple(int(row["Id"]) for row in sources)

            cursor.execute(
                "SELECT Id, UserId, MediaSourceId, SnapshotId, IdempotencyKey "
                "FROM ManifestImport WHERE PublicId = %s OR (UserId = %s AND ("
                "SnapshotId = %s OR IdempotencyKey = %s)) FOR UPDATE",
                (
                    str(target.import_public_id),
                    expected_user_id,
                    str(target.snapshot_id),
                    target.idempotency_key,
                ),
            )
            imports = list(cursor.fetchall())
            _require(len(imports) <= 1, "Cleanup found multiple synthetic imports")
            for row in imports:
                _require(
                    int(row["UserId"]) == expected_user_id
                    and row["SnapshotId"] == str(target.snapshot_id)
                    and row["IdempotencyKey"] == target.idempotency_key
                    and (not source_ids or int(row["MediaSourceId"]) in source_ids),
                    "Cleanup import scope is not exact",
                )
            import_ids = tuple(int(row["Id"]) for row in imports)

            cursor.execute(
                "SELECT Id, ContentSha256 FROM MediaAsset WHERE UserId = %s "
                "AND ContentSha256 IN (%s, %s, %s) FOR UPDATE",
                (expected_user_id, *target.asset_hashes),
            )
            assets = list(cursor.fetchall())
            _require(
                all(row["ContentSha256"] in target.asset_hashes for row in assets),
                "Cleanup asset scope is not exact",
            )
            asset_ids = tuple(int(row["Id"]) for row in assets)

            occurrence_clauses: list[str] = []
            occurrence_params: list[Any] = [expected_user_id]
            if source_ids:
                occurrence_clauses.append(
                    f"MediaSourceId IN ({','.join('%s' for _ in source_ids)})"
                )
                occurrence_params.extend(source_ids)
            if asset_ids:
                occurrence_clauses.append(
                    f"MediaAssetId IN ({','.join('%s' for _ in asset_ids)})"
                )
                occurrence_params.extend(asset_ids)
            occurrences: list[Mapping[str, Any]] = []
            if occurrence_clauses:
                cursor.execute(
                    "SELECT Id, UserId, MediaSourceId, SourceItemId, LocalLocator "
                    "FROM MediaOccurrence WHERE UserId = %s AND ("
                    + " OR ".join(occurrence_clauses)
                    + ") FOR UPDATE",
                    tuple(occurrence_params),
                )
                occurrences = list(cursor.fetchall())
            for row in occurrences:
                _require(
                    str(row["SourceItemId"]).startswith(f"{target.run_id}:row:")
                    and str(row["LocalLocator"]).startswith(target.local_locator_prefix)
                    and (not source_ids or int(row["MediaSourceId"]) in source_ids),
                    "Cleanup occurrence scope is not exact",
                )
            occurrence_ids = tuple(int(row["Id"]) for row in occurrences)

            if asset_ids:
                external = _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM MediaOccurrence WHERE MediaAssetId IN "
                    f"({','.join('%s' for _ in asset_ids)}) AND (UserId <> %s"
                    + (
                        f" OR MediaSourceId NOT IN ({','.join('%s' for _ in source_ids)}))"
                        if source_ids
                        else ")"
                    ),
                    (*asset_ids, expected_user_id, *source_ids),
                )
                _require(external == 0, "Canary asset is referenced outside synthetic source")

            def delete_ids(table: str, column: str, ids: Sequence[int]) -> None:
                if ids:
                    cursor.execute(
                        f"DELETE FROM {table} WHERE {column} IN "
                        f"({','.join('%s' for _ in ids)})",
                        tuple(ids),
                    )

            if asset_ids or source_ids:
                job_clauses: list[str] = []
                job_params: list[Any] = [expected_user_id]
                if source_ids:
                    job_clauses.append(
                        f"MediaSourceId IN ({','.join('%s' for _ in source_ids)})"
                    )
                    job_params.extend(source_ids)
                if asset_ids:
                    job_clauses.append(
                        f"MediaAssetId IN ({','.join('%s' for _ in asset_ids)})"
                    )
                    job_params.extend(asset_ids)
                cursor.execute(
                    "DELETE FROM ProcessingJob WHERE UserId = %s AND ("
                    + " OR ".join(job_clauses)
                    + ")",
                    tuple(job_params),
                )
            if asset_ids or occurrence_ids:
                upload_clauses: list[str] = []
                upload_params: list[Any] = [expected_user_id]
                if asset_ids:
                    upload_clauses.append(
                        f"MediaAssetId IN ({','.join('%s' for _ in asset_ids)})"
                    )
                    upload_params.extend(asset_ids)
                if occurrence_ids:
                    upload_clauses.append(
                        f"MediaOccurrenceId IN ({','.join('%s' for _ in occurrence_ids)})"
                    )
                    upload_params.extend(occurrence_ids)
                cursor.execute(
                    "DELETE FROM UploadSession WHERE UserId = %s AND ("
                    + " OR ".join(upload_clauses)
                    + ")",
                    tuple(upload_params),
                )

            change_clauses: list[str] = []
            change_params: list[Any] = [expected_user_id]
            for column, ids in (
                ("DeviceId", device_ids),
                ("MediaSourceId", source_ids),
                ("MediaAssetId", asset_ids),
                ("MediaOccurrenceId", occurrence_ids),
            ):
                if ids:
                    change_clauses.append(
                        f"{column} IN ({','.join('%s' for _ in ids)})"
                    )
                    change_params.extend(ids)
            if change_clauses:
                cursor.execute(
                    "DELETE FROM MediaChange WHERE UserId = %s AND ("
                    + " OR ".join(change_clauses)
                    + ")",
                    tuple(change_params),
                )

            delete_ids("ManifestImport", "Id", import_ids)
            delete_ids("MediaOccurrence", "Id", occurrence_ids)
            delete_ids("MediaAsset", "Id", asset_ids)
            delete_ids("MediaSource", "Id", source_ids)
            delete_ids("Device", "Id", device_ids)

            remaining = {
                "devices": _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM Device WHERE PublicId = %s "
                    "OR (UserId = %s AND DeviceKey LIKE %s)",
                    (str(target.device_public_id), expected_user_id, f"{target.prefix}%"),
                ),
                "sources": _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM MediaSource WHERE PublicId = %s "
                    "OR (UserId = %s AND SourceKey LIKE %s)",
                    (str(target.source_public_id), expected_user_id, f"{target.prefix}%"),
                ),
                "imports": _manifest_collision_count(cursor, target, expected_user_id),
                "assets": _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM MediaAsset WHERE UserId = %s "
                    "AND ContentSha256 IN (%s, %s, %s)",
                    (expected_user_id, *target.asset_hashes),
                ),
            }
            remaining["occurrences"] = (
                _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM MediaOccurrence WHERE Id IN "
                    f"({','.join('%s' for _ in occurrence_ids)})",
                    occurrence_ids,
                )
                if occurrence_ids
                else 0
            )
            remaining["changes"] = (
                _count(
                    cursor,
                    "SELECT COUNT(*) AS Value FROM MediaChange WHERE UserId = %s AND ("
                    + " OR ".join(change_clauses)
                    + ")",
                    tuple(change_params),
                )
                if change_clauses
                else 0
            )
            remaining["jobs"] = (
                _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM ProcessingJob WHERE UserId = %s "
                    f"AND MediaAssetId IN ({','.join('%s' for _ in asset_ids)})",
                    (expected_user_id, *asset_ids),
                )
                if asset_ids
                else 0
            )
            remaining["locations"] = (
                _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM MediaLocation WHERE UserId = %s "
                    f"AND MediaAssetId IN ({','.join('%s' for _ in asset_ids)})",
                    (expected_user_id, *asset_ids),
                )
                if asset_ids
                else 0
            )
            remaining["uploads"] = (
                _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM UploadSession WHERE UserId = %s "
                    f"AND MediaAssetId IN ({','.join('%s' for _ in asset_ids)})",
                    (expected_user_id, *asset_ids),
                )
                if asset_ids
                else 0
            )
            remaining["failures"] = (
                _count(
                    cursor,
                    f"SELECT COUNT(*) AS Value FROM ManifestImportFailure "
                    f"WHERE ManifestImportId IN ({','.join('%s' for _ in import_ids)})",
                    import_ids,
                )
                if import_ids
                else 0
            )
            _require(
                all(value == 0 for value in remaining.values()),
                "Synthetic rows remain after cleanup; transaction was rolled back",
            )
        connection.commit()
        return remaining
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _exercise(
    factory: ConnectionFactory,
    target: CanaryTarget,
    preflight: Preflight,
) -> dict[str, Any]:
    entries = _manifest_entries(target)
    with tempfile.TemporaryDirectory(prefix="imagetracker-bulk-db-canary-") as temporary:
        directory = Path(temporary)
        input_path = directory / "manifest.ndjson.gz"
        csv_path = directory / "manifest.csv"
        artifact = write_manifest_gzip(
            input_path,
            source_id=target.source_public_id,
            snapshot_id=target.snapshot_id,
            entries=entries,
        )
        parsed = parse_manifest_gzip(
            artifact.path,
            csv_path,
            expected_sha256=artifact.compressed_sha256,
            expected_compressed_bytes=artifact.compressed_bytes,
            expected_entry_count=4,
            guardrails=ManifestGuardrails(max_entries=4),
        )
        _require(parsed.rejected_count == 1, "Canonical parser did not reject row four")
        _device_id, source_id = _insert_control_rows(
            factory,
            target,
            preflight,
            input_sha256=artifact.compressed_sha256,
            input_bytes=artifact.compressed_bytes,
        )
        repository = MySqlManifestImportRepository(factory)
        claim = repository.claim(
            import_id=target.import_public_id,
            lease_owner=f"{target.run_id}:claim",
            lease_seconds=1200,
        )
        _require(isinstance(claim, ManifestImportClaim), "Queued canary import was not claimed")
        repository.load_stage(claim, parsed)
        repository.set_phase(
            claim,
            phase="Merging",
            allowed_phases={"Staged", "Merging"},
        )
        merge = repository.merge(claim)
        _require(
            merge.processed == 4
            and merge.created == 2
            and merge.updated == 0
            and merge.duplicates_linked == 1
            and merge.unchanged == 0
            and merge.rejected == 1,
            "Set-based merge counters are incorrect",
        )
        repository.set_phase(
            claim,
            phase="WritingResult",
            allowed_phases={"Merged", "WritingResult"},
        )
        result_path = directory / "result.ndjson.gz"
        result_bytes, result_sha256, result_rows, _expanded = write_result_gzip(
            result_path,
            import_id=target.import_public_id,
            counts=merge.as_counts(),
            rows=repository.iter_results(claim),
        )
        _require(result_rows == 4, "Result writer did not emit four rows")
        header, rows = _read_result(result_path)
        _require(
            header.get("counts", {}).get("created") == 2
            and header.get("counts", {}).get("duplicatesLinked") == 1
            and header.get("counts", {}).get("rejected") == 1,
            "Result header counters are incorrect",
        )
        outcomes = [row.get("outcome") for row in rows]
        _require(
            outcomes.count("CreatedOccurrence") == 2
            and outcomes.count("DuplicateLinked") == 1
            and outcomes.count("Rejected") == 1,
            "Result row outcomes are incorrect",
        )
        repository.complete_result(
            claim,
            bucket="imagetracker-bulk-db-canary",
            object_key=f"manifests/canary/{target.run_id}/result.ndjson.gz",
            checksum_sha256=result_sha256,
            byte_size=result_bytes,
        )
        before_redelivery = _validate_database_state(
            factory,
            target,
            preflight,
            source_id=source_id,
        )
        redelivery = repository.claim(
            import_id=target.import_public_id,
            lease_owner=f"{target.run_id}:redelivery",
            lease_seconds=1200,
        )
        _require(redelivery is None, "Terminal import was claimed on redelivery")
        after_redelivery = _validate_database_state(
            factory,
            target,
            preflight,
            source_id=source_id,
        )
        _require(
            before_redelivery == after_redelivery,
            "Idempotent redelivery changed synthetic database rows",
        )
        return {
            "merge": merge.as_counts(),
            "database": after_redelivery,
            "resultRows": result_rows,
            "redeliveryIgnored": True,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the self-cleaning ImageTracker bulk MySQL canary."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create and clean the exact synthetic rows; default is read-only dry-run.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("IMAGETRACKER_AWS_REGION", "us-east-2"),
    )
    parser.add_argument("--profile", help="Optional AWS profile from WSL config.")
    parser.add_argument(
        "--parameter",
        default=os.environ.get(
            "IMAGETRACKER_DB_SECRET_PARAMETER", DEFAULT_PARAMETER
        ),
        help="ImageTracker application MySQL SSM parameter.",
    )
    return parser


def run(
    args: argparse.Namespace,
    *,
    connection_factory: ConnectionFactory | None = None,
    target: CanaryTarget | None = None,
) -> dict[str, Any]:
    _require_wsl()
    _validate_parameter_name(args.parameter)
    selected_target = target or CanaryTarget.build(uuid4())
    _validate_target(selected_target)
    factory = connection_factory or _connection_factory_from_ssm(args)
    preflight = _preflight(
        factory(),
        selected_target,
        require_migration=bool(args.apply),
    )
    report: dict[str, Any] = {
        "status": "passed",
        "mode": "apply" if args.apply else "dry-run",
        "runId": str(selected_target.run_id),
        "target": {
            "kind": "synthetic-local-source",
            "uuidPrefixed": True,
            "rows": 4,
            "gps": False,
            "extension": ".nef",
        },
        "preflight": preflight.as_json(),
    }
    if not args.apply:
        report["wouldApply"] = (
            preflight.migration_014_present
            and preflight.schema_ready
            and preflight.local_infile_enabled
            and preflight.active_import_count == 0
        )
        return report

    primary_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    try:
        report["acceptance"] = _exercise(factory, selected_target, preflight)
    except BaseException as error:
        primary_error = error
    finally:
        try:
            report["cleanup"] = _cleanup(
                factory,
                selected_target,
                expected_user_id=preflight.account_id,
            )
        except BaseException as error:
            cleanup_error = error

    if primary_error is not None or cleanup_error is not None:
        report["status"] = "failed"
        if primary_error is not None:
            report["error"] = _safe_error(primary_error)
        if cleanup_error is not None:
            report["cleanupError"] = _safe_error(cleanup_error)
        raise CanaryRunFailed(report)
    return report


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = run(args)
    except CanaryRunFailed as error:
        print(json.dumps(error.report, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1
    except BaseException as error:
        print(
            json.dumps(
                {"status": "failed", "error": _safe_error(error)},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
