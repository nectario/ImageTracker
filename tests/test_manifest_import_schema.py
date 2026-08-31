from __future__ import annotations

from pathlib import Path
import re

from ImageTracker import _split_sql_statements
from services.bulk.manifest import CANONICAL_CSV_COLUMNS
from services.data.models import (
    Base,
    ManifestImport,
    ManifestImportAssetWork,
    ManifestImportEntry,
    ManifestImportFailure,
    MediaOccurrence,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "014_CreateManifestImportTables.sql"


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_manifest_import_migration_is_additive_and_crash_replayable() -> None:
    statements = _split_sql_statements(_sql())

    assert len(statements) == 5
    assert [
        re.match(
            r"CREATE TABLE IF NOT EXISTS `([^`]+)`",
            statement,
            flags=re.IGNORECASE,
        ).group(1)
        for statement in statements[:4]
    ] == [
        "ManifestImport",
        "ManifestImportEntry",
        "ManifestImportAssetWork",
        "ManifestImportFailure",
    ]
    assert statements[4].startswith("ALTER TABLE `MediaOccurrence`")
    assert "ALGORITHM=INPLACE" in statements[4]
    assert "LOCK=NONE" in statements[4]


def test_manifest_import_header_has_durable_identity_lease_and_result_state() -> None:
    sql = _sql()

    assert "UNIQUE KEY `Ux_ManifestImport_User_Idempotency`" in sql
    assert "`RequestSha256` CHAR(64) CHARACTER SET ascii" in sql
    assert "UNIQUE KEY `Ux_ManifestImport_User_Source_Snapshot`" in sql
    assert "UNIQUE KEY `Ux_ManifestImport_User_Source_Active`" in sql
    assert "`Status` VARCHAR(32) NOT NULL DEFAULT 'AwaitingUpload'" in sql
    assert "`Phase` VARCHAR(32) NOT NULL DEFAULT 'Preparing'" in sql
    assert "`InputChecksumSha256` CHAR(64)" in sql
    assert "`DeclaredEntryCount` INT UNSIGNED NOT NULL" in sql
    assert "`ProcessedEntryCount` INT UNSIGNED NOT NULL DEFAULT 0" in sql
    assert "`ResultChecksumSha256` CHAR(64)" in sql
    assert "`LeaseTokenHash` CHAR(64)" in sql
    assert "`NextAttemptAtUtc` DATETIME(6) NULL" in sql
    assert "FOREIGN KEY (`UserId`, `MediaSourceId`)" in sql
    assert "REFERENCES `MediaSource` (`UserId`, `Id`) ON DELETE CASCADE" in sql


def test_stage_keeps_raw_evidence_and_separately_indexed_normalized_values() -> None:
    sql = _sql()
    stage = sql.split(
        "CREATE TABLE IF NOT EXISTS `ManifestImportEntry`", 1
    )[1].split(") ENGINE=InnoDB", 1)[0]

    for field in (
        "OperationRaw",
        "SourceItemIdRaw",
        "ContentSha256Raw",
        "ByteSizeRaw",
        "CaptureDateTimeLocalRaw",
        "LatitudeRaw",
    ):
        assert f"`{field}` TEXT" in stage
    assert "`SourceItemIdRaw` TEXT COLLATE utf8mb4_bin NOT NULL" in stage
    assert "`LocalLocatorRaw` LONGTEXT NULL" in stage
    assert "`ProvenanceJsonRaw` LONGTEXT NULL" in stage

    assert "`SourceItemId` VARCHAR(512) COLLATE utf8mb4_bin NULL" in stage
    assert "`ContentSha256` CHAR(64) CHARACTER SET ascii" in stage
    assert "`ByteSize` BIGINT UNSIGNED NULL" in stage
    assert "`CaptureDateTimeLocal` DATETIME(6) NULL" in stage
    assert "`Latitude` DECIMAL(9,6) NULL" in stage
    assert "`CoordinateRevision` CHAR(64) CHARACTER SET ascii" in stage
    assert "`ProvenanceJson` JSON NULL" in stage
    assert "`LocationSource` VARCHAR(32) NULL" in stage
    assert (
        "KEY `Ix_ManifestImportEntry_Import_SourceItem` "
        "(`ManifestImportId`, `SourceItemId`)"
    ) in stage
    assert (
        "KEY `Ix_ManifestImportEntry_Import_Hash` "
        "(`ManifestImportId`, `ContentSha256`)"
    ) in stage
    assert "UNIQUE KEY `Ux_ManifestImportEntry_Import_Row`" in stage
    assert "UNIQUE KEY `Ux_ManifestImportEntry_Import_SourceItem`" not in stage
    stage_columns = set(
        re.findall(r"^\s+`([^`]+)`\s+", stage, flags=re.MULTILINE)
    )
    assert set(CANONICAL_CSV_COLUMNS).issubset(stage_columns)


def test_asset_work_is_durable_and_failure_audit_is_compact() -> None:
    sql = _sql()

    assert "PRIMARY KEY (`ManifestImportId`, `ContentSha256`)" in sql
    assert "`CanonicalStageId` BIGINT UNSIGNED NOT NULL" in sql
    assert "`CanonicalRowNumber` INT UNSIGNED NOT NULL" in sql
    assert "`ResolvedMediaAssetId` BIGINT UNSIGNED NULL" in sql
    assert "`ResolvedMediaAssetPublicId` CHAR(36)" in sql
    assert "`AssetWasPreexisting` TINYINT(1) NOT NULL DEFAULT 0" in sql
    assert "`AssetCreated` TINYINT(1) NOT NULL DEFAULT 0" in sql
    assert "`AssetChanged` TINYINT(1) NOT NULL DEFAULT 0" in sql
    assert "CONSTRAINT `Fk_ManifestImportAssetWork_CanonicalEntry`" in sql
    assert "ON DELETE CASCADE" in sql

    assert "UNIQUE KEY `Ux_ManifestImportFailure_Import_Row`" in sql
    assert "CONSTRAINT `Fk_ManifestImportFailure_ManifestImport`" in sql
    assert "REFERENCES `ManifestImport` (`UserId`, `Id`) ON DELETE CASCADE" in sql


def test_sqlalchemy_models_match_manifest_import_tables_and_indexes() -> None:
    assert {
        "ManifestImport",
        "ManifestImportEntry",
        "ManifestImportAssetWork",
        "ManifestImportFailure",
    }.issubset(Base.metadata.tables)

    assert ManifestImport.__table__.c.Status.default.arg == "AwaitingUpload"
    assert ManifestImport.__table__.c.RequestSha256.nullable is False
    assert ManifestImportEntry.__table__.c.SourceItemId.nullable
    assert ManifestImportEntry.__table__.c.SourceItemIdRaw.type.length is None
    assert ManifestImportAssetWork.__table__.primary_key.columns.keys() == [
        "ManifestImportId",
        "ContentSha256",
    ]
    assert {
        index.name for index in ManifestImportFailure.__table__.indexes
    } == {"Ix_ManifestImportFailure_User_Import"}
    assert "Ix_MediaOccurrence_User_Asset_DeletionState" in {
        index.name for index in MediaOccurrence.__table__.indexes
    }
