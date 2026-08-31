from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    """Return naive UTC, matching MySQL DATETIME semantics."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_public_id() -> str:
    return str(uuid4())


ID_TYPE = mysql.BIGINT(unsigned=True).with_variant(Integer, "sqlite")
UINT_BIGINT = mysql.BIGINT(unsigned=True).with_variant(BigInteger, "sqlite")
UINT_INT = mysql.INTEGER(unsigned=True).with_variant(Integer, "sqlite")
UINT_SMALLINT = mysql.SMALLINT(unsigned=True).with_variant(SmallInteger, "sqlite")
BOOL_INT = mysql.TINYINT(unsigned=True).with_variant(Integer, "sqlite")
LONG_TEXT = mysql.LONGTEXT().with_variant(Text(), "sqlite")
BINARY_SOURCE_ITEM = mysql.VARCHAR(512, collation="utf8mb4_bin").with_variant(
    String(512), "sqlite"
)
BINARY_TEXT = mysql.TEXT(collation="utf8mb4_bin").with_variant(Text(), "sqlite")


class Base(DeclarativeBase):
    pass


class UserAccount(Base):
    __tablename__ = "UserAccount"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_UserAccount_PublicId"),
        UniqueConstraint("CognitoSubject", name="Ux_UserAccount_CognitoSubject"),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    cognito_subject: Mapped[str] = mapped_column("CognitoSubject", String(128))
    email: Mapped[str | None] = mapped_column("Email", String(320))
    display_name: Mapped[str | None] = mapped_column("DisplayName", String(255))
    account_status: Mapped[str] = mapped_column("AccountStatus", String(32), default="Active")
    last_sign_in_at_utc: Mapped[datetime | None] = mapped_column("LastSignInAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )
    deleted_at_utc: Mapped[datetime | None] = mapped_column("DeletedAtUtc", DateTime)


class Device(Base):
    __tablename__ = "Device"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_Device_PublicId"),
        UniqueConstraint("UserId", "DeviceKey", name="Ux_Device_User_DeviceKey"),
        UniqueConstraint("UserId", "Id", name="Ux_Device_User_Id"),
        ForeignKeyConstraint(
            ["UserId"], ["UserAccount.Id"], name="Fk_Device_UserAccount", ondelete="CASCADE"
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    device_key: Mapped[str] = mapped_column("DeviceKey", String(128))
    display_name: Mapped[str] = mapped_column("DisplayName", String(255))
    platform: Mapped[str] = mapped_column("Platform", String(32))
    operating_system_version: Mapped[str | None] = mapped_column(
        "OperatingSystemVersion", String(64)
    )
    app_version: Mapped[str | None] = mapped_column("AppVersion", String(64))
    sync_cursor: Mapped[int] = mapped_column("SyncCursor", UINT_BIGINT, default=0)
    last_activity_at_utc: Mapped[datetime | None] = mapped_column("LastActivityAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )
    retired_at_utc: Mapped[datetime | None] = mapped_column("RetiredAtUtc", DateTime)


class IdempotencyRecord(Base):
    __tablename__ = "IdempotencyRecord"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_IdempotencyRecord_PublicId"),
        UniqueConstraint(
            "UserId", "IdempotencyKey", name="Ux_IdempotencyRecord_User_Key"
        ),
        ForeignKeyConstraint(
            ["UserId"],
            ["UserAccount.Id"],
            name="Fk_IdempotencyRecord_UserAccount",
            ondelete="CASCADE",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    idempotency_key: Mapped[str] = mapped_column("IdempotencyKey", String(128))
    http_method: Mapped[str] = mapped_column("HttpMethod", String(16))
    route_pattern: Mapped[str] = mapped_column("RoutePattern", String(255))
    request_sha256: Mapped[str] = mapped_column("RequestSha256", String(64))
    response_status_code: Mapped[int | None] = mapped_column(
        "ResponseStatusCode", UINT_SMALLINT
    )
    response_headers_json: Mapped[dict[str, Any] | None] = mapped_column(
        "ResponseHeadersJson", JSON
    )
    response_body_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(
        "ResponseBodyJson", JSON
    )
    expires_at_utc: Mapped[datetime] = mapped_column("ExpiresAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaSource(Base):
    __tablename__ = "MediaSource"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaSource_PublicId"),
        UniqueConstraint(
            "UserId", "DeviceId", "SourceKey", name="Ux_MediaSource_User_Device_SourceKey"
        ),
        UniqueConstraint("UserId", "Id", name="Ux_MediaSource_User_Id"),
        ForeignKeyConstraint(
            ["UserId", "DeviceId"],
            ["Device.UserId", "Device.Id"],
            name="Fk_MediaSource_Device",
            ondelete="CASCADE",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    device_id: Mapped[int] = mapped_column("DeviceId", UINT_BIGINT)
    source_key: Mapped[str] = mapped_column("SourceKey", String(128))
    display_name: Mapped[str] = mapped_column("DisplayName", String(255))
    source_type: Mapped[str] = mapped_column("SourceType", String(32))
    storage_mode: Mapped[str] = mapped_column("StorageMode", String(16), default="Local")
    permission_state: Mapped[str] = mapped_column(
        "PermissionState", String(32), default="NotApplicable"
    )
    source_status: Mapped[str] = mapped_column("SourceStatus", String(16), default="Active")
    sync_policy_json: Mapped[dict[str, Any] | None] = mapped_column("SyncPolicyJson", JSON)
    sync_cursor: Mapped[str | None] = mapped_column("SyncCursor", String(1024))
    last_manifest_at_utc: Mapped[datetime | None] = mapped_column("LastManifestAtUtc", DateTime)
    last_success_at_utc: Mapped[datetime | None] = mapped_column("LastSuccessAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )
    removed_at_utc: Mapped[datetime | None] = mapped_column("RemovedAtUtc", DateTime)


class ManifestImport(Base):
    __tablename__ = "ManifestImport"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_ManifestImport_PublicId"),
        UniqueConstraint(
            "UserId", "IdempotencyKey", name="Ux_ManifestImport_User_Idempotency"
        ),
        UniqueConstraint(
            "UserId",
            "MediaSourceId",
            "SnapshotId",
            name="Ux_ManifestImport_User_Source_Snapshot",
        ),
        UniqueConstraint(
            "UserId",
            "MediaSourceId",
            "ActiveMarker",
            name="Ux_ManifestImport_User_Source_Active",
        ),
        UniqueConstraint("UserId", "Id", name="Ux_ManifestImport_User_Id"),
        ForeignKeyConstraint(
            ["UserId", "MediaSourceId"],
            ["MediaSource.UserId", "MediaSource.Id"],
            name="Fk_ManifestImport_MediaSource",
            ondelete="CASCADE",
        ),
        Index(
            "Ix_ManifestImport_Status_NextAttempt",
            "Status",
            "NextAttemptAtUtc",
            "Id",
        ),
        Index(
            "Ix_ManifestImport_User_Source_Created",
            "UserId",
            "MediaSourceId",
            "CreatedAtUtc",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_source_id: Mapped[int] = mapped_column("MediaSourceId", UINT_BIGINT)
    snapshot_id: Mapped[str] = mapped_column("SnapshotId", String(36))
    idempotency_key: Mapped[str] = mapped_column("IdempotencyKey", String(128))
    request_sha256: Mapped[str] = mapped_column("RequestSha256", String(64))
    active_marker: Mapped[int | None] = mapped_column("ActiveMarker", BOOL_INT, default=1)
    manifest_kind: Mapped[str] = mapped_column("ManifestKind", String(16))
    permission_state: Mapped[str] = mapped_column("PermissionState", String(32))
    deletion_detection_reliable: Mapped[int] = mapped_column(
        "DeletionDetectionReliable", BOOL_INT
    )
    client_cursor: Mapped[str | None] = mapped_column("ClientCursor", String(1024))
    schema_version: Mapped[str] = mapped_column("SchemaVersion", String(32))
    status: Mapped[str] = mapped_column(
        "Status", String(32), default="AwaitingUpload"
    )
    phase: Mapped[str] = mapped_column("Phase", String(32), default="Preparing")
    input_s3_bucket: Mapped[str] = mapped_column("InputS3Bucket", String(63))
    input_s3_object_key: Mapped[str] = mapped_column("InputS3ObjectKey", String(1024))
    input_s3_version_id: Mapped[str | None] = mapped_column(
        "InputS3VersionId", String(1024)
    )
    input_checksum_sha256: Mapped[str] = mapped_column(
        "InputChecksumSha256", String(64)
    )
    input_byte_size: Mapped[int] = mapped_column("InputByteSize", UINT_BIGINT)
    declared_entry_count: Mapped[int] = mapped_column("DeclaredEntryCount", UINT_INT)
    validated_entry_count: Mapped[int] = mapped_column(
        "ValidatedEntryCount", UINT_INT, default=0
    )
    processed_entry_count: Mapped[int] = mapped_column(
        "ProcessedEntryCount", UINT_INT, default=0
    )
    created_count: Mapped[int] = mapped_column("CreatedCount", UINT_INT, default=0)
    updated_count: Mapped[int] = mapped_column("UpdatedCount", UINT_INT, default=0)
    duplicate_linked_count: Mapped[int] = mapped_column(
        "DuplicateLinkedCount", UINT_INT, default=0
    )
    deleted_count: Mapped[int] = mapped_column("DeletedCount", UINT_INT, default=0)
    ignored_deletion_count: Mapped[int] = mapped_column(
        "IgnoredDeletionCount", UINT_INT, default=0
    )
    unchanged_count: Mapped[int] = mapped_column("UnchangedCount", UINT_INT, default=0)
    rejected_count: Mapped[int] = mapped_column("RejectedCount", UINT_INT, default=0)
    result_s3_bucket: Mapped[str | None] = mapped_column("ResultS3Bucket", String(63))
    result_s3_object_key: Mapped[str | None] = mapped_column(
        "ResultS3ObjectKey", String(1024)
    )
    result_checksum_sha256: Mapped[str | None] = mapped_column(
        "ResultChecksumSha256", String(64)
    )
    result_byte_size: Mapped[int | None] = mapped_column("ResultByteSize", UINT_BIGINT)
    attempt_count: Mapped[int] = mapped_column("AttemptCount", UINT_INT, default=0)
    max_attempts: Mapped[int] = mapped_column("MaxAttempts", UINT_INT, default=5)
    next_attempt_at_utc: Mapped[datetime | None] = mapped_column(
        "NextAttemptAtUtc", DateTime
    )
    lease_token_hash: Mapped[str | None] = mapped_column("LeaseTokenHash", String(64))
    lease_expires_at_utc: Mapped[datetime | None] = mapped_column(
        "LeaseExpiresAtUtc", DateTime
    )
    failure_class: Mapped[str | None] = mapped_column("FailureClass", String(32))
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    failure_message: Mapped[str | None] = mapped_column("FailureMessage", Text)
    upload_expires_at_utc: Mapped[datetime | None] = mapped_column(
        "UploadExpiresAtUtc", DateTime
    )
    queued_at_utc: Mapped[datetime | None] = mapped_column("QueuedAtUtc", DateTime)
    started_at_utc: Mapped[datetime | None] = mapped_column("StartedAtUtc", DateTime)
    completed_at_utc: Mapped[datetime | None] = mapped_column("CompletedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class ManifestImportEntry(Base):
    __tablename__ = "ManifestImportEntry"
    __table_args__ = (
        UniqueConstraint(
            "ManifestImportId", "RowNumber", name="Ux_ManifestImportEntry_Import_Row"
        ),
        UniqueConstraint(
            "ManifestImportId", "StageId", name="Ux_ManifestImportEntry_Import_Stage"
        ),
        ForeignKeyConstraint(
            ["ManifestImportId"],
            ["ManifestImport.Id"],
            name="Fk_ManifestImportEntry_ManifestImport",
            ondelete="CASCADE",
        ),
        Index(
            "Ix_ManifestImportEntry_Import_SourceItem",
            "ManifestImportId",
            "SourceItemId",
        ),
        Index(
            "Ix_ManifestImportEntry_Import_Hash",
            "ManifestImportId",
            "ContentSha256",
        ),
        Index(
            "Ix_ManifestImportEntry_Import_ResolvedAsset",
            "ManifestImportId",
            "ResolvedAssetId",
        ),
    )

    stage_id: Mapped[int] = mapped_column(
        "StageId", ID_TYPE, primary_key=True, autoincrement=True
    )
    manifest_import_id: Mapped[int] = mapped_column("ManifestImportId", UINT_BIGINT)
    row_number: Mapped[int] = mapped_column("RowNumber", UINT_INT)
    operation_raw: Mapped[str] = mapped_column("OperationRaw", Text)
    source_item_id_raw: Mapped[str] = mapped_column("SourceItemIdRaw", BINARY_TEXT)
    source_revision_raw: Mapped[str | None] = mapped_column("SourceRevisionRaw", Text)
    original_file_name_raw: Mapped[str | None] = mapped_column("OriginalFileNameRaw", Text)
    local_locator_raw: Mapped[str | None] = mapped_column("LocalLocatorRaw", LONG_TEXT)
    content_sha256_raw: Mapped[str | None] = mapped_column("ContentSha256Raw", Text)
    media_type_raw: Mapped[str | None] = mapped_column("MediaTypeRaw", Text)
    mime_type_raw: Mapped[str | None] = mapped_column("MimeTypeRaw", Text)
    byte_size_raw: Mapped[str | None] = mapped_column("ByteSizeRaw", Text)
    width_pixels_raw: Mapped[str | None] = mapped_column("WidthPixelsRaw", Text)
    height_pixels_raw: Mapped[str | None] = mapped_column("HeightPixelsRaw", Text)
    duration_milliseconds_raw: Mapped[str | None] = mapped_column(
        "DurationMillisecondsRaw", Text
    )
    capture_datetime_local_raw: Mapped[str | None] = mapped_column(
        "CaptureDateTimeLocalRaw", Text
    )
    capture_datetime_utc_raw: Mapped[str | None] = mapped_column(
        "CaptureDateTimeUtcRaw", Text
    )
    time_zone_raw: Mapped[str | None] = mapped_column("TimeZoneRaw", Text)
    utc_offset_minutes_raw: Mapped[str | None] = mapped_column(
        "UtcOffsetMinutesRaw", Text
    )
    latitude_raw: Mapped[str | None] = mapped_column("LatitudeRaw", Text)
    longitude_raw: Mapped[str | None] = mapped_column("LongitudeRaw", Text)
    altitude_meters_raw: Mapped[str | None] = mapped_column("AltitudeMetersRaw", Text)
    accuracy_meters_raw: Mapped[str | None] = mapped_column("AccuracyMetersRaw", Text)
    provenance_json_raw: Mapped[str | None] = mapped_column("ProvenanceJsonRaw", LONG_TEXT)
    operation: Mapped[str | None] = mapped_column("Operation", String(16))
    source_item_id: Mapped[str | None] = mapped_column(
        "SourceItemId", BINARY_SOURCE_ITEM
    )
    source_revision: Mapped[str | None] = mapped_column("SourceRevision", String(255))
    original_file_name: Mapped[str | None] = mapped_column(
        "OriginalFileName", String(512)
    )
    local_locator: Mapped[str | None] = mapped_column("LocalLocator", Text)
    content_sha256: Mapped[str | None] = mapped_column("ContentSha256", String(64))
    media_type: Mapped[str | None] = mapped_column("MediaType", String(16))
    mime_type: Mapped[str | None] = mapped_column("MimeType", String(255))
    byte_size: Mapped[int | None] = mapped_column("ByteSize", UINT_BIGINT)
    width_pixels: Mapped[int | None] = mapped_column("WidthPixels", UINT_INT)
    height_pixels: Mapped[int | None] = mapped_column("HeightPixels", UINT_INT)
    duration_milliseconds: Mapped[int | None] = mapped_column(
        "DurationMilliseconds", UINT_BIGINT
    )
    capture_datetime_local: Mapped[datetime | None] = mapped_column(
        "CaptureDateTimeLocal", DateTime
    )
    capture_datetime_utc: Mapped[datetime | None] = mapped_column(
        "CaptureDateTimeUtc", DateTime
    )
    time_zone: Mapped[str | None] = mapped_column("TimeZone", String(64))
    utc_offset_minutes: Mapped[int | None] = mapped_column("UtcOffsetMinutes", SmallInteger)
    latitude: Mapped[Decimal | None] = mapped_column("Latitude", Numeric(9, 6))
    longitude: Mapped[Decimal | None] = mapped_column("Longitude", Numeric(10, 6))
    altitude_meters: Mapped[Decimal | None] = mapped_column(
        "AltitudeMeters", Numeric(10, 3)
    )
    accuracy_meters: Mapped[Decimal | None] = mapped_column(
        "AccuracyMeters", Numeric(10, 3)
    )
    coordinate_revision: Mapped[str | None] = mapped_column(
        "CoordinateRevision", String(64)
    )
    provenance_json: Mapped[list[dict[str, Any]] | None] = mapped_column(
        "ProvenanceJson", JSON
    )
    location_source: Mapped[str | None] = mapped_column("LocationSource", String(32))
    validation_state: Mapped[str] = mapped_column(
        "ValidationState", String(32), default="Pending"
    )
    existing_occurrence_id: Mapped[int | None] = mapped_column(
        "ExistingOccurrenceId", UINT_BIGINT
    )
    existing_asset_id: Mapped[int | None] = mapped_column("ExistingAssetId", UINT_BIGINT)
    resolved_asset_id: Mapped[int | None] = mapped_column("ResolvedAssetId", UINT_BIGINT)
    outcome: Mapped[str | None] = mapped_column("Outcome", String(32))
    error_code: Mapped[str | None] = mapped_column("ErrorCode", String(64))
    error_message: Mapped[str | None] = mapped_column("ErrorMessage", String(1000))
    occurrence_public_id: Mapped[str | None] = mapped_column(
        "OccurrencePublicId", String(36)
    )
    media_asset_public_id: Mapped[str | None] = mapped_column(
        "MediaAssetPublicId", String(36)
    )
    description_job_public_id: Mapped[str | None] = mapped_column(
        "DescriptionJobPublicId", String(36)
    )
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class ManifestImportAssetWork(Base):
    __tablename__ = "ManifestImportAssetWork"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ManifestImportId", "CanonicalStageId"],
            ["ManifestImportEntry.ManifestImportId", "ManifestImportEntry.StageId"],
            name="Fk_ManifestImportAssetWork_CanonicalEntry",
            ondelete="CASCADE",
        ),
        Index(
            "Ix_ManifestImportAssetWork_Import_ResolvedAsset",
            "ManifestImportId",
            "ResolvedMediaAssetId",
        ),
        Index(
            "Ix_ManifestImportAssetWork_CanonicalStage",
            "ManifestImportId",
            "CanonicalStageId",
        ),
    )

    manifest_import_id: Mapped[int] = mapped_column(
        "ManifestImportId", UINT_BIGINT, primary_key=True
    )
    content_sha256: Mapped[str] = mapped_column(
        "ContentSha256", String(64), primary_key=True
    )
    canonical_stage_id: Mapped[int] = mapped_column("CanonicalStageId", UINT_BIGINT)
    canonical_row_number: Mapped[int] = mapped_column("CanonicalRowNumber", UINT_INT)
    resolved_media_asset_id: Mapped[int | None] = mapped_column(
        "ResolvedMediaAssetId", UINT_BIGINT
    )
    resolved_media_asset_public_id: Mapped[str | None] = mapped_column(
        "ResolvedMediaAssetPublicId", String(36)
    )
    asset_was_preexisting: Mapped[int] = mapped_column(
        "AssetWasPreexisting", BOOL_INT, default=0
    )
    asset_created: Mapped[int] = mapped_column("AssetCreated", BOOL_INT, default=0)
    asset_changed: Mapped[int] = mapped_column("AssetChanged", BOOL_INT, default=0)
    error_code: Mapped[str | None] = mapped_column("ErrorCode", String(64))
    error_message: Mapped[str | None] = mapped_column("ErrorMessage", String(1000))
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class ManifestImportFailure(Base):
    __tablename__ = "ManifestImportFailure"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_ManifestImportFailure_PublicId"),
        UniqueConstraint(
            "ManifestImportId", "RowNumber", name="Ux_ManifestImportFailure_Import_Row"
        ),
        ForeignKeyConstraint(
            ["UserId", "ManifestImportId"],
            ["ManifestImport.UserId", "ManifestImport.Id"],
            name="Fk_ManifestImportFailure_ManifestImport",
            ondelete="CASCADE",
        ),
        Index(
            "Ix_ManifestImportFailure_User_Import", "UserId", "ManifestImportId"
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    manifest_import_id: Mapped[int] = mapped_column("ManifestImportId", UINT_BIGINT)
    row_number: Mapped[int] = mapped_column("RowNumber", UINT_INT)
    source_item_id: Mapped[str] = mapped_column("SourceItemId", BINARY_SOURCE_ITEM)
    source_revision: Mapped[str | None] = mapped_column("SourceRevision", String(255))
    operation: Mapped[str] = mapped_column("Operation", String(16))
    error_code: Mapped[str] = mapped_column("ErrorCode", String(64))
    error_message: Mapped[str | None] = mapped_column("ErrorMessage", String(1000))
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)


class MediaAsset(Base):
    __tablename__ = "MediaAsset"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaAsset_PublicId"),
        UniqueConstraint(
            "UserId", "ContentSha256", name="Ux_MediaAsset_User_ContentSha256"
        ),
        UniqueConstraint("UserId", "Id", name="Ux_MediaAsset_User_Id"),
        ForeignKeyConstraint(
            ["UserId"],
            ["UserAccount.Id"],
            name="Fk_MediaAsset_UserAccount",
            ondelete="CASCADE",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    content_sha256: Mapped[str] = mapped_column("ContentSha256", String(64))
    content_hash_source: Mapped[str] = mapped_column(
        "ContentHashSource", String(32), default="ClientDeclared"
    )
    content_hash_verified_at_utc: Mapped[datetime | None] = mapped_column(
        "ContentHashVerifiedAtUtc", DateTime
    )
    media_type: Mapped[str] = mapped_column("MediaType", String(16))
    mime_type: Mapped[str] = mapped_column("MimeType", String(255))
    byte_size: Mapped[int] = mapped_column("ByteSize", UINT_BIGINT)
    width_pixels: Mapped[int | None] = mapped_column("WidthPixels", UINT_INT)
    height_pixels: Mapped[int | None] = mapped_column("HeightPixels", UINT_INT)
    duration_milliseconds: Mapped[int | None] = mapped_column(
        "DurationMilliseconds", UINT_BIGINT
    )
    capture_datetime_local: Mapped[datetime | None] = mapped_column(
        "CaptureDateTimeLocal", DateTime
    )
    capture_datetime_utc: Mapped[datetime | None] = mapped_column(
        "CaptureDateTimeUtc", DateTime
    )
    time_zone: Mapped[str | None] = mapped_column("TimeZone", String(64))
    utc_offset_minutes: Mapped[int | None] = mapped_column("UtcOffsetMinutes", SmallInteger)
    capture_time_source: Mapped[str | None] = mapped_column("CaptureTimeSource", String(32))
    capture_time_confidence: Mapped[Decimal | None] = mapped_column(
        "CaptureTimeConfidence", Numeric(5, 4)
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("MetadataJson", JSON)
    metadata_version: Mapped[str | None] = mapped_column("MetadataVersion", String(64))
    category: Mapped[str | None] = mapped_column("Category", String(255))
    category_source: Mapped[str | None] = mapped_column("CategorySource", String(64))
    storage_state: Mapped[str] = mapped_column(
        "StorageState", String(32), default="LocalOnly"
    )
    s3_bucket: Mapped[str | None] = mapped_column("S3Bucket", String(63))
    original_s3_object_key: Mapped[str | None] = mapped_column(
        "OriginalS3ObjectKey", String(1024)
    )
    original_s3_version_id: Mapped[str | None] = mapped_column(
        "OriginalS3VersionId", String(1024)
    )
    original_s3_etag: Mapped[str | None] = mapped_column("OriginalS3ETag", String(255))
    original_s3_checksum_algorithm: Mapped[str | None] = mapped_column(
        "OriginalS3ChecksumAlgorithm", String(16)
    )
    original_s3_checksum_type: Mapped[str | None] = mapped_column(
        "OriginalS3ChecksumType", String(16)
    )
    original_s3_checksum_value: Mapped[str | None] = mapped_column(
        "OriginalS3ChecksumValue", String(255)
    )
    preview_s3_object_key: Mapped[str | None] = mapped_column(
        "PreviewS3ObjectKey", String(1024)
    )
    preview_s3_checksum_algorithm: Mapped[str | None] = mapped_column(
        "PreviewS3ChecksumAlgorithm", String(16)
    )
    preview_s3_checksum_type: Mapped[str | None] = mapped_column(
        "PreviewS3ChecksumType", String(16)
    )
    preview_s3_checksum_value: Mapped[str | None] = mapped_column(
        "PreviewS3ChecksumValue", String(255)
    )
    lifecycle_state: Mapped[str] = mapped_column(
        "LifecycleState", String(32), default="Active"
    )
    last_processed_at_utc: Mapped[datetime | None] = mapped_column(
        "LastProcessedAtUtc", DateTime
    )
    trashed_at_utc: Mapped[datetime | None] = mapped_column("TrashedAtUtc", DateTime)
    purge_after_utc: Mapped[datetime | None] = mapped_column("PurgeAfterUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaOccurrence(Base):
    __tablename__ = "MediaOccurrence"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaOccurrence_PublicId"),
        UniqueConstraint(
            "MediaSourceId", "SourceItemId", name="Ux_MediaOccurrence_Source_SourceItemId"
        ),
        UniqueConstraint("UserId", "Id", name="Ux_MediaOccurrence_User_Id"),
        ForeignKeyConstraint(
            ["UserId", "MediaSourceId"],
            ["MediaSource.UserId", "MediaSource.Id"],
            name="Fk_MediaOccurrence_MediaSource",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_MediaOccurrence_MediaAsset",
            ondelete="CASCADE",
        ),
        Index(
            "Ix_MediaOccurrence_User_Asset_DeletionState",
            "UserId",
            "MediaAssetId",
            "DeletionState",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_source_id: Mapped[int] = mapped_column("MediaSourceId", UINT_BIGINT)
    media_asset_id: Mapped[int | None] = mapped_column("MediaAssetId", UINT_BIGINT)
    source_item_id: Mapped[str] = mapped_column("SourceItemId", String(512))
    original_file_name: Mapped[str] = mapped_column("OriginalFileName", String(512))
    local_locator: Mapped[str | None] = mapped_column("LocalLocator", Text)
    source_revision: Mapped[str | None] = mapped_column("SourceRevision", String(255))
    observed_byte_size: Mapped[int | None] = mapped_column("ObservedByteSize", UINT_BIGINT)
    observed_modified_at_utc: Mapped[datetime | None] = mapped_column(
        "ObservedModifiedAtUtc", DateTime
    )
    hash_status: Mapped[str] = mapped_column("HashStatus", String(32), default="Pending")
    hash_failure_code: Mapped[str | None] = mapped_column("HashFailureCode", String(64))
    availability_state: Mapped[str] = mapped_column(
        "AvailabilityState", String(32), default="Available"
    )
    deletion_state: Mapped[str] = mapped_column("DeletionState", String(32), default="Active")
    first_seen_at_utc: Mapped[datetime] = mapped_column(
        "FirstSeenAtUtc", DateTime, default=utc_now
    )
    last_seen_at_utc: Mapped[datetime] = mapped_column(
        "LastSeenAtUtc", DateTime, default=utc_now
    )
    deleted_at_utc: Mapped[datetime | None] = mapped_column("DeletedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaLocation(Base):
    __tablename__ = "MediaLocation"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaLocation_PublicId"),
        UniqueConstraint("UserId", "MediaAssetId", name="Ux_MediaLocation_User_Asset"),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_MediaLocation_MediaAsset",
            ondelete="CASCADE",
        ),
        Index("Ix_MediaLocation_User_LatLon", "UserId", "Latitude", "Longitude"),
        Index("Ix_MediaLocation_User_City_State", "UserId", "City", "State"),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_asset_id: Mapped[int] = mapped_column("MediaAssetId", UINT_BIGINT)
    latitude: Mapped[Decimal | None] = mapped_column("Latitude", Numeric(9, 6))
    longitude: Mapped[Decimal | None] = mapped_column("Longitude", Numeric(10, 6))
    altitude_meters: Mapped[Decimal | None] = mapped_column("AltitudeMeters", Numeric(10, 3))
    accuracy_meters: Mapped[Decimal | None] = mapped_column("AccuracyMeters", Numeric(10, 3))
    location_display_name: Mapped[str | None] = mapped_column(
        "LocationDisplayName", String(512)
    )
    street_address: Mapped[str | None] = mapped_column("StreetAddress", String(512))
    original_street_number: Mapped[str | None] = mapped_column(
        "OriginalStreetNumber", String(32)
    )
    neighborhood: Mapped[str | None] = mapped_column("Neighborhood", String(255))
    city: Mapped[str | None] = mapped_column("City", String(255))
    county: Mapped[str | None] = mapped_column("County", String(255))
    state: Mapped[str | None] = mapped_column("State", String(255))
    postal_code: Mapped[str | None] = mapped_column("PostalCode", String(50))
    country: Mapped[str | None] = mapped_column("Country", String(255))
    country_code: Mapped[str | None] = mapped_column("CountryCode", String(8))
    location_source: Mapped[str | None] = mapped_column("LocationSource", String(32))
    provider: Mapped[str | None] = mapped_column("Provider", String(64))
    provider_place_id: Mapped[str | None] = mapped_column("ProviderPlaceId", String(500))
    normalization_rule_version: Mapped[str | None] = mapped_column(
        "NormalizationRuleVersion", String(64)
    )
    confidence: Mapped[Decimal | None] = mapped_column("Confidence", Numeric(5, 4))
    raw_provider_json: Mapped[dict[str, Any] | None] = mapped_column("RawProviderJson", JSON)
    provider_updated_at_utc: Mapped[datetime | None] = mapped_column(
        "ProviderUpdatedAtUtc", DateTime
    )
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaDescription(Base):
    __tablename__ = "MediaDescription"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaDescription_PublicId"),
        UniqueConstraint(
            "UserId",
            "MediaAssetId",
            "Provider",
            "Model",
            "PromptVersion",
            name="Ux_MediaDescription_Asset_Version",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_MediaDescription_MediaAsset",
            ondelete="CASCADE",
        ),
        Index("Ix_MediaDescription_User_Status", "UserId", "Status", "UpdatedAtUtc"),
        Index(
            "Ix_MediaDescription_User_Asset_Current",
            "UserId",
            "MediaAssetId",
            "IsCurrent",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_asset_id: Mapped[int] = mapped_column("MediaAssetId", UINT_BIGINT)
    description: Mapped[str | None] = mapped_column("Description", Text)
    language_code: Mapped[str | None] = mapped_column("LanguageCode", String(16))
    provider: Mapped[str] = mapped_column("Provider", String(64))
    model: Mapped[str] = mapped_column("Model", String(128))
    prompt_version: Mapped[str] = mapped_column("PromptVersion", String(64))
    status: Mapped[str] = mapped_column("Status", String(32), default="Queued")
    is_current: Mapped[int] = mapped_column("IsCurrent", BOOL_INT, default=0)
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    requested_at_utc: Mapped[datetime | None] = mapped_column("RequestedAtUtc", DateTime)
    completed_at_utc: Mapped[datetime | None] = mapped_column("CompletedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaTranscript(Base):
    __tablename__ = "MediaTranscript"

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_asset_id: Mapped[int] = mapped_column("MediaAssetId", UINT_BIGINT)
    provider: Mapped[str] = mapped_column("Provider", String(64))
    model: Mapped[str] = mapped_column("Model", String(128))
    provider_request_id: Mapped[str | None] = mapped_column("ProviderRequestId", String(255))
    language_code: Mapped[str | None] = mapped_column("LanguageCode", String(16))
    transcript_text: Mapped[str | None] = mapped_column("TranscriptText", Text)
    duration_milliseconds: Mapped[int | None] = mapped_column(
        "DurationMilliseconds", UINT_BIGINT
    )
    status: Mapped[str] = mapped_column("Status", String(32), default="Queued")
    is_current: Mapped[int] = mapped_column("IsCurrent", BOOL_INT, default=0)
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    requested_at_utc: Mapped[datetime | None] = mapped_column("RequestedAtUtc", DateTime)
    completed_at_utc: Mapped[datetime | None] = mapped_column("CompletedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaTranscriptSegment(Base):
    __tablename__ = "MediaTranscriptSegment"

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_transcript_id: Mapped[int] = mapped_column("MediaTranscriptId", UINT_BIGINT)
    sequence_number: Mapped[int] = mapped_column("SequenceNumber", UINT_INT)
    start_milliseconds: Mapped[int] = mapped_column("StartMilliseconds", UINT_BIGINT)
    end_milliseconds: Mapped[int] = mapped_column("EndMilliseconds", UINT_BIGINT)
    speaker_label: Mapped[str | None] = mapped_column("SpeakerLabel", String(128))
    segment_text: Mapped[str] = mapped_column("SegmentText", Text)
    confidence: Mapped[Decimal | None] = mapped_column("Confidence", Numeric(5, 4))
    word_timings_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(
        "WordTimingsJson", JSON
    )
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)


class UploadSession(Base):
    __tablename__ = "UploadSession"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_UploadSession_PublicId"),
        UniqueConstraint(
            "UserId", "IdempotencyKey", name="Ux_UploadSession_User_Idempotency"
        ),
        UniqueConstraint(
            "UserId",
            "MediaAssetId",
            "ObjectPurpose",
            "ActiveLeaseMarker",
            name="Ux_UploadSession_ActiveLease",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_UploadSession_MediaAsset",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaOccurrenceId"],
            ["MediaOccurrence.UserId", "MediaOccurrence.Id"],
            name="Fk_UploadSession_MediaOccurrence",
            ondelete="CASCADE",
        ),
        Index("Ix_UploadSession_User_Status", "UserId", "Status", "UpdatedAtUtc"),
        Index("Ix_UploadSession_Status_Expiry", "Status", "ExpiresAtUtc"),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_asset_id: Mapped[int] = mapped_column("MediaAssetId", UINT_BIGINT)
    media_occurrence_id: Mapped[int | None] = mapped_column(
        "MediaOccurrenceId", UINT_BIGINT
    )
    idempotency_key: Mapped[str] = mapped_column("IdempotencyKey", String(128))
    object_purpose: Mapped[str] = mapped_column("ObjectPurpose", String(32))
    upload_kind: Mapped[str] = mapped_column("UploadKind", String(16))
    status: Mapped[str] = mapped_column("Status", String(32), default="Preparing")
    active_lease_marker: Mapped[int | None] = mapped_column(
        "ActiveLeaseMarker", BOOL_INT, default=1
    )
    lease_token_hash: Mapped[str] = mapped_column("LeaseTokenHash", String(64))
    lease_owner: Mapped[str | None] = mapped_column("LeaseOwner", String(128))
    s3_bucket: Mapped[str] = mapped_column("S3Bucket", String(63))
    s3_object_key: Mapped[str] = mapped_column("S3ObjectKey", String(1024))
    s3_upload_id: Mapped[str | None] = mapped_column("S3UploadId", String(1024))
    checksum_sha256: Mapped[str] = mapped_column("ChecksumSha256", String(64))
    s3_checksum_algorithm: Mapped[str] = mapped_column(
        "S3ChecksumAlgorithm", String(16)
    )
    s3_checksum_type: Mapped[str] = mapped_column("S3ChecksumType", String(16))
    s3_checksum_value: Mapped[str | None] = mapped_column(
        "S3ChecksumValue", String(255)
    )
    expected_byte_size: Mapped[int] = mapped_column("ExpectedByteSize", UINT_BIGINT)
    uploaded_byte_size: Mapped[int] = mapped_column(
        "UploadedByteSize", UINT_BIGINT, default=0
    )
    part_size_bytes: Mapped[int | None] = mapped_column("PartSizeBytes", UINT_BIGINT)
    parts_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(
        "PartsJson", JSON
    )
    expires_at_utc: Mapped[datetime] = mapped_column("ExpiresAtUtc", DateTime)
    completed_at_utc: Mapped[datetime | None] = mapped_column("CompletedAtUtc", DateTime)
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class ProcessingJob(Base):
    __tablename__ = "ProcessingJob"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_ProcessingJob_PublicId"),
        UniqueConstraint(
            "UserId", "IdempotencyKey", name="Ux_ProcessingJob_User_Idempotency"
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_ProcessingJob_MediaAsset",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaSourceId"],
            ["MediaSource.UserId", "MediaSource.Id"],
            name="Fk_ProcessingJob_MediaSource",
            ondelete="RESTRICT",
        ),
        Index(
            "Ix_ProcessingJob_Status_NextAttempt",
            "Status",
            "NextAttemptAtUtc",
            "Id",
        ),
        Index("Ix_ProcessingJob_User_Status", "UserId", "Status", "CreatedAtUtc"),
        Index(
            "Ix_ProcessingJob_User_Asset_Type",
            "UserId",
            "MediaAssetId",
            "JobType",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    media_asset_id: Mapped[int] = mapped_column("MediaAssetId", UINT_BIGINT)
    media_source_id: Mapped[int | None] = mapped_column("MediaSourceId", UINT_BIGINT)
    idempotency_key: Mapped[str] = mapped_column("IdempotencyKey", String(128))
    job_type: Mapped[str] = mapped_column("JobType", String(32))
    status: Mapped[str] = mapped_column("Status", String(32), default="Queued")
    provider: Mapped[str | None] = mapped_column("Provider", String(64))
    attempt_count: Mapped[int] = mapped_column("AttemptCount", UINT_INT, default=0)
    max_attempts: Mapped[int] = mapped_column("MaxAttempts", UINT_INT, default=5)
    next_attempt_at_utc: Mapped[datetime | None] = mapped_column("NextAttemptAtUtc", DateTime)
    lease_token_hash: Mapped[str | None] = mapped_column("LeaseTokenHash", String(64))
    lease_expires_at_utc: Mapped[datetime | None] = mapped_column("LeaseExpiresAtUtc", DateTime)
    failure_class: Mapped[str | None] = mapped_column("FailureClass", String(32))
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    failure_message: Mapped[str | None] = mapped_column("FailureMessage", Text)
    request_json: Mapped[dict[str, Any] | None] = mapped_column("RequestJson", JSON)
    started_at_utc: Mapped[datetime | None] = mapped_column("StartedAtUtc", DateTime)
    completed_at_utc: Mapped[datetime | None] = mapped_column("CompletedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class ProviderUsageMonth(Base):
    __tablename__ = "ProviderUsageMonth"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_ProviderUsageMonth_PublicId"),
        UniqueConstraint(
            "UserId",
            "Provider",
            "UsageMonth",
            "UnitType",
            name="Ux_ProviderUsageMonth_User_Provider_Month_Unit",
        ),
        ForeignKeyConstraint(
            ["UserId"],
            ["UserAccount.Id"],
            name="Fk_ProviderUsageMonth_UserAccount",
            ondelete="CASCADE",
        ),
        Index("Ix_ProviderUsageMonth_User_Month", "UserId", "UsageMonth"),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    provider: Mapped[str] = mapped_column("Provider", String(64))
    usage_month: Mapped[date] = mapped_column("UsageMonth", Date)
    unit_type: Mapped[str] = mapped_column("UnitType", String(32))
    processed_units: Mapped[Decimal] = mapped_column(
        "ProcessedUnits", Numeric(20, 6), default=Decimal("0")
    )
    reserved_units: Mapped[Decimal] = mapped_column(
        "ReservedUnits", Numeric(20, 6), default=Decimal("0")
    )
    hard_limit_units: Mapped[Decimal] = mapped_column(
        "HardLimitUnits", Numeric(20, 6)
    )
    circuit_state: Mapped[str] = mapped_column(
        "CircuitState", String(16), default="Closed"
    )
    circuit_opened_at_utc: Mapped[datetime | None] = mapped_column(
        "CircuitOpenedAtUtc", DateTime
    )
    circuit_failure_code: Mapped[str | None] = mapped_column(
        "CircuitFailureCode", String(64)
    )
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


class MediaChange(Base):
    __tablename__ = "MediaChange"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_MediaChange_PublicId"),
        ForeignKeyConstraint(
            ["UserId"],
            ["UserAccount.Id"],
            name="Fk_MediaChange_UserAccount",
            ondelete="CASCADE",
        ),
        Index("Ix_MediaChange_User_Cursor", "UserId", "Id"),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    device_id: Mapped[int | None] = mapped_column("DeviceId", UINT_BIGINT)
    media_source_id: Mapped[int | None] = mapped_column("MediaSourceId", UINT_BIGINT)
    media_asset_id: Mapped[int | None] = mapped_column("MediaAssetId", UINT_BIGINT)
    media_occurrence_id: Mapped[int | None] = mapped_column("MediaOccurrenceId", UINT_BIGINT)
    entity_type: Mapped[str] = mapped_column("EntityType", String(32))
    entity_id: Mapped[int] = mapped_column("EntityId", UINT_BIGINT)
    entity_public_id: Mapped[str] = mapped_column("EntityPublicId", String(36))
    change_type: Mapped[str] = mapped_column("ChangeType", String(32))
    change_data_json: Mapped[dict[str, Any] | None] = mapped_column("ChangeDataJson", JSON)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)


class LegacyImageAssetMap(Base):
    __tablename__ = "LegacyImageAssetMap"
    __table_args__ = (
        UniqueConstraint("PublicId", name="Ux_LegacyImageAssetMap_PublicId"),
        UniqueConstraint("LegacyImageAssetId", name="Ux_LegacyImageAssetMap_LegacyId"),
        UniqueConstraint("MediaOccurrenceId", name="Ux_LegacyImageAssetMap_Occurrence"),
        ForeignKeyConstraint(
            ["UserId"],
            ["UserAccount.Id"],
            name="Fk_LegacyImageAssetMap_UserAccount",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaAssetId"],
            ["MediaAsset.UserId", "MediaAsset.Id"],
            name="Fk_LegacyImageAssetMap_User_MediaAsset",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["UserId", "MediaOccurrenceId"],
            ["MediaOccurrence.UserId", "MediaOccurrence.Id"],
            name="Fk_LegacyImageAssetMap_User_MediaOccurrence",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[int] = mapped_column("Id", ID_TYPE, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column("PublicId", String(36), default=new_public_id)
    user_id: Mapped[int] = mapped_column("UserId", UINT_BIGINT)
    legacy_image_asset_id: Mapped[int] = mapped_column("LegacyImageAssetId", BigInteger)
    media_asset_id: Mapped[int | None] = mapped_column("MediaAssetId", UINT_BIGINT)
    media_occurrence_id: Mapped[int | None] = mapped_column("MediaOccurrenceId", UINT_BIGINT)
    migration_status: Mapped[str] = mapped_column(
        "MigrationStatus", String(32), default="Pending"
    )
    failure_code: Mapped[str | None] = mapped_column("FailureCode", String(64))
    failure_message: Mapped[str | None] = mapped_column("FailureMessage", Text)
    temporal_review_required: Mapped[int] = mapped_column(
        "TemporalReviewRequired", BOOL_INT, default=0
    )
    last_attempt_at_utc: Mapped[datetime | None] = mapped_column("LastAttemptAtUtc", DateTime)
    migrated_at_utc: Mapped[datetime | None] = mapped_column("MigratedAtUtc", DateTime)
    created_at_utc: Mapped[datetime] = mapped_column("CreatedAtUtc", DateTime, default=utc_now)
    updated_at_utc: Mapped[datetime] = mapped_column(
        "UpdatedAtUtc", DateTime, default=utc_now, onupdate=utc_now
    )


__all__ = [
    "Base",
    "Device",
    "IdempotencyRecord",
    "LegacyImageAssetMap",
    "ManifestImport",
    "ManifestImportAssetWork",
    "ManifestImportEntry",
    "ManifestImportFailure",
    "MediaAsset",
    "MediaChange",
    "MediaDescription",
    "MediaLocation",
    "MediaOccurrence",
    "MediaSource",
    "MediaTranscript",
    "MediaTranscriptSegment",
    "ProcessingJob",
    "ProviderUsageMonth",
    "UploadSession",
    "UserAccount",
    "new_public_id",
    "utc_now",
]
