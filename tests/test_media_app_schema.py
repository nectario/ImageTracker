from pathlib import Path
import re

from ImageTracker import _split_sql_statements


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "migrations"
    / "007_CreateMediaAppTables.sql"
)

EXPECTED_TABLES = {
    "UserAccount",
    "Device",
    "IdempotencyRecord",
    "MediaSource",
    "MediaAsset",
    "MediaOccurrence",
    "MediaLocation",
    "MediaDescription",
    "MediaTranscript",
    "MediaTranscriptSegment",
    "UploadSession",
    "ProcessingJob",
    "ProviderUsageMonth",
    "MediaChange",
    "LegacyImageAssetMap",
}


def _migration_sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def test_media_app_migration_is_additive_and_recoverable() -> None:
    sql = _migration_sql()
    statements = _split_sql_statements(sql)

    created_tables = {
        match.group(1)
        for statement in statements
        if (
            match := re.match(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`([^`]+)`",
                statement,
                flags=re.IGNORECASE,
            )
        )
    }

    assert created_tables == EXPECTED_TABLES
    assert len(statements) == len(EXPECTED_TABLES)
    assert all(
        re.match(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS",
            statement,
            flags=re.IGNORECASE,
        )
        for statement in statements
    )
    assert not re.search(r"\b(?:ALTER|DROP|TRUNCATE|UPDATE|DELETE)\s+`?ImageAsset`?", sql, re.IGNORECASE)


def test_media_app_migration_enforces_identity_and_ownership() -> None:
    sql = _migration_sql()

    assert sql.count("`PublicId` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL") == len(EXPECTED_TABLES)
    assert "UNIQUE KEY `Ux_MediaAsset_User_ContentSha256` (`UserId`, `ContentSha256`)" in sql
    assert "UNIQUE KEY `Ux_MediaOccurrence_Source_SourceItemId` (`MediaSourceId`, `SourceItemId`)" in sql
    assert "FOREIGN KEY (`UserId`, `MediaSourceId`) REFERENCES `MediaSource` (`UserId`, `Id`)" in sql
    assert "FOREIGN KEY (`UserId`, `MediaAssetId`) REFERENCES `MediaAsset` (`UserId`, `Id`)" in sql
    assert "`OriginalFileName` VARCHAR(512) NOT NULL" in sql


def test_media_app_migration_supports_sync_search_and_bounded_processing() -> None:
    sql = _migration_sql()

    assert "KEY `Ix_MediaChange_User_Cursor` (`UserId`, `Id`)" in sql
    assert "FULLTEXT KEY `Ix_MediaDescription_FullText` (`Description`)" in sql
    assert "FULLTEXT KEY `Ix_MediaTranscript_FullText` (`TranscriptText`)" in sql
    assert "FULLTEXT KEY `Ix_MediaTranscriptSegment_FullText` (`SegmentText`)" in sql
    assert "UNIQUE KEY `Ux_UploadSession_ActiveLease`" in sql
    assert "`HardLimitUnits` DECIMAL(20,6) UNSIGNED NOT NULL" in sql
    assert "`PurgeAfterUtc` DATETIME(6) NULL" in sql
    assert "UNIQUE KEY `Ux_IdempotencyRecord_User_Key` (`UserId`, `IdempotencyKey`)" in sql
    assert "`RequestSha256` CHAR(64)" in sql


def test_media_app_migration_uses_contract_defaults() -> None:
    sql = _migration_sql()

    assert "`StorageMode` VARCHAR(16) NOT NULL DEFAULT 'Local'" in sql
    assert "`PermissionState` VARCHAR(32) NOT NULL DEFAULT 'NotApplicable'" in sql
    assert "`SourceStatus` VARCHAR(16) NOT NULL DEFAULT 'Active'" in sql
    assert "`StorageState` VARCHAR(32) NOT NULL DEFAULT 'LocalOnly'" in sql
    assert "`Status` VARCHAR(32) NOT NULL DEFAULT 'Queued'" in sql

    for table_name in ("MediaDescription", "MediaTranscript"):
        table_sql = sql.split(f"CREATE TABLE IF NOT EXISTS `{table_name}`", 1)[1].split(
            ") ENGINE=InnoDB",
            1,
        )[0]
        assert "`Status` VARCHAR(32) NOT NULL DEFAULT 'Queued'" in table_sql
