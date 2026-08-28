from __future__ import annotations

from pathlib import Path

from ImageTracker import _split_sql_statements


ROOT = Path(__file__).resolve().parents[1]


def _migration(name: str) -> str:
    return (ROOT / "migrations" / name).read_text(encoding="utf-8")


def test_upload_session_stores_s3_transport_checksum_separately():
    sql = _migration("008_AddUploadSessionChecksumColumns.sql")

    assert len(_split_sql_statements(sql)) == 1
    assert "`S3ChecksumAlgorithm` VARCHAR(16) NOT NULL" in sql
    assert "`S3ChecksumType` VARCHAR(16) NOT NULL" in sql
    assert "`S3ChecksumValue` VARCHAR(255) NULL" in sql
    assert "ALGORITHM=INSTANT" in sql


def test_media_asset_tracks_client_hash_provenance_and_s3_checksums():
    sql = _migration("009_AddMediaAssetObjectChecksumColumns.sql")

    assert len(_split_sql_statements(sql)) == 1
    assert "`ContentHashSource` VARCHAR(32) NOT NULL DEFAULT 'ClientDeclared'" in sql
    assert "`ContentHashVerifiedAtUtc` DATETIME(6) NULL" in sql
    assert "`OriginalS3ChecksumValue` VARCHAR(255) NULL" in sql
    assert "`PreviewS3ChecksumValue` VARCHAR(255) NULL" in sql


def test_change_feed_persists_public_id_for_tombstones():
    sql = _migration("010_AddMediaChangeEntityPublicId.sql")

    assert len(_split_sql_statements(sql)) == 1
    assert "`EntityPublicId` CHAR(36)" in sql
    assert "`Ix_MediaChange_User_EntityPublicId` (`UserId`, `EntityPublicId`)" in sql
    assert "ALGORITHM=INPLACE" in sql
    assert "LOCK=NONE" in sql


def test_legacy_map_foreign_keys_include_user_ownership():
    sql = _migration("011_EnforceLegacyMapOwnership.sql")

    assert len(_split_sql_statements(sql)) == 1
    assert "FOREIGN KEY (`UserId`, `MediaAssetId`)" in sql
    assert "REFERENCES `MediaAsset` (`UserId`, `Id`)" in sql
    assert "FOREIGN KEY (`UserId`, `MediaOccurrenceId`)" in sql
    assert "REFERENCES `MediaOccurrence` (`UserId`, `Id`)" in sql
    assert "DROP INDEX `Fk_LegacyImageAssetMap_MediaAsset`" in sql
    assert "`Ix_LegacyImageAssetMap_User_MediaAsset` (`UserId`, `MediaAssetId`)" in sql
    assert "`Ix_LegacyImageAssetMap_User_MediaOccurrence` (`UserId`, `MediaOccurrenceId`)" in sql
    assert "ADD CONSTRAINT `Fk_LegacyImageAssetMap_User_MediaAsset`" in sql
    assert "ADD CONSTRAINT `Fk_LegacyImageAssetMap_User_MediaOccurrence`" in sql
    assert "ALGORITHM=COPY" in sql


def test_provider_circuit_is_an_atomic_additive_migration():
    sql = _migration("012_AddProviderCircuit.sql")
    statements = _split_sql_statements(sql)

    assert len(statements) == 1
    assert "`CircuitState` VARCHAR(16) NOT NULL DEFAULT 'Closed'" in sql
    assert "`CircuitOpenedAtUtc` DATETIME(6) NULL" in sql
    assert "`CircuitFailureCode` VARCHAR(64) NULL" in sql
    assert "ALGORITHM=INSTANT" in sql


def test_international_address_capacity_is_an_atomic_migration():
    sql = _migration("013_WidenLocationProviderFields.sql")

    assert len(_split_sql_statements(sql)) == 1
    assert "`ProviderPlaceId` VARCHAR(500) NULL" in sql
    assert "`PostalCode` VARCHAR(50) NULL" in sql
    # Crossing VARCHAR(255) can change the length-byte representation; COPY is
    # the portable MySQL algorithm, and this new table is empty before rollout.
    assert "ALGORITHM=COPY" in sql
