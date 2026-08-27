from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.common.enums import (
    EnrichmentStatus,
    MediaType,
    ProcessingJobStatus,
    SourcePlatform,
    StorageMode,
    StorageState,
    UserFacingState,
)


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class ApiModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="forbid",
        from_attributes=True,
        populate_by_name=True,
        use_enum_values=True,
    )


class StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class DeviceStatus(StringEnum):
    ACTIVE = "Active"
    SIGNED_OUT = "SignedOut"
    REMOVED = "Removed"


class SourceType(StringEnum):
    PHOTO_LIBRARY = "PhotoLibrary"
    FOLDER = "Folder"
    SINGLE_UPLOAD = "SingleUpload"
    LEGACY_IMPORT = "LegacyImport"


class PermissionState(StringEnum):
    FULL = "Full"
    LIMITED = "Limited"
    DENIED = "Denied"
    UNAVAILABLE = "Unavailable"
    NOT_APPLICABLE = "NotApplicable"


class SourceStatus(StringEnum):
    ACTIVE = "Active"
    PAUSED = "Paused"
    REMOVED = "Removed"


class NetworkPolicy(StringEnum):
    WIFI_ONLY = "WiFiOnly"
    WIFI_OR_CELLULAR = "WiFiOrCellular"


class ManifestKind(StringEnum):
    INCREMENTAL = "Incremental"
    FULL = "Full"


class ManifestOutcome(StringEnum):
    CREATED_OCCURRENCE = "CreatedOccurrence"
    UPDATED_OCCURRENCE = "UpdatedOccurrence"
    DUPLICATE_LINKED = "DuplicateLinked"
    DELETED_OCCURRENCE = "DeletedOccurrence"
    IGNORED_DELETION = "IgnoredDeletion"
    UNCHANGED = "Unchanged"
    REJECTED = "Rejected"


class ProvenanceSource(StringEnum):
    EXIF = "Exif"
    DEVICE = "Device"
    FILE_MTIME = "FileMtime"
    GOOGLE = "Google"
    MANUAL = "Manual"
    AI = "AI"
    LEGACY = "Legacy"
    UNKNOWN = "Unknown"


class MediaAvailability(StringEnum):
    LOCAL_ON_THIS_DEVICE = "LocalOnThisDevice"
    REMOTE = "Remote"
    UNAVAILABLE = "Unavailable"


class ChangeType(StringEnum):
    UPSERT = "Upsert"
    DELETE = "Delete"


class ChangeResourceType(StringEnum):
    MEDIA_ASSET = "MediaAsset"
    MEDIA_OCCURRENCE = "MediaOccurrence"
    MEDIA_SOURCE = "MediaSource"
    PROCESSING_JOB = "ProcessingJob"


class MatchField(StringEnum):
    FILE_NAME = "FileName"
    DESCRIPTION = "Description"
    TRANSCRIPT = "Transcript"
    CATEGORY = "Category"
    DATE = "Date"
    MEDIA_TYPE = "MediaType"
    LOCATION = "Location"


class ProcessingJobType(StringEnum):
    METADATA = "Metadata"
    GEOCODE = "Geocode"
    DESCRIPTION = "Description"
    TRANSCRIPTION = "Transcription"
    SEARCH_INDEX = "SearchIndex"
    CLEANUP = "Cleanup"
    LEGACY_MIGRATION = "LegacyMigration"


class FailureClass(StringEnum):
    TRANSIENT = "Transient"
    AUTHENTICATION = "Authentication"
    QUOTA = "Quota"
    UNSUPPORTED_FORMAT = "UnsupportedFormat"
    INVALID_MEDIA = "InvalidMedia"
    INTERNAL = "Internal"


class HealthStatus(StringEnum):
    OK = "Ok"
    DEGRADED = "Degraded"
    UNAVAILABLE = "Unavailable"


class AuditSeverity(StringEnum):
    INFO = "Info"
    WARNING = "Warning"
    ERROR = "Error"


class CurrentUser(ApiModel):
    user_id: UUID
    email: str | None
    display_name: str | None = Field(default=None, max_length=200)
    created_at_utc: datetime


class PageInfo(ApiModel):
    next_cursor: str | None = None
    has_more: bool


class DeviceRegistrationRequest(ApiModel):
    installation_id: UUID
    platform: SourcePlatform
    display_name: str = Field(min_length=1, max_length=200)
    app_version: str = Field(min_length=1, max_length=50)
    os_version: str = Field(min_length=1, max_length=64)


class Device(ApiModel):
    device_id: UUID
    installation_id: UUID
    platform: SourcePlatform
    display_name: str
    app_version: str
    os_version: str
    status: DeviceStatus
    registered_at_utc: datetime
    last_seen_at_utc: datetime


class DevicePage(ApiModel):
    items: list[Device]
    page: PageInfo


class SyncSettings(ApiModel):
    automatic_sync: bool
    network_policy: NetworkPolicy
    require_charging_for_historical_upload: bool


class MediaSourceCreateRequest(ApiModel):
    device_id: UUID
    source_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    )
    source_type: SourceType
    display_name: str = Field(min_length=1, max_length=200)
    storage_mode: StorageMode = StorageMode.LOCAL
    permission_state: PermissionState = PermissionState.NOT_APPLICABLE
    sync_settings: SyncSettings | None = None

    @model_validator(mode="after")
    def reject_explicit_null_sync_settings(self) -> "MediaSourceCreateRequest":
        if "sync_settings" in self.model_fields_set and self.sync_settings is None:
            raise ValueError("syncSettings cannot be null when supplied")
        return self


class MediaSourceUpdateRequest(ApiModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    storage_mode: StorageMode | None = None
    permission_state: PermissionState | None = None
    status: Literal["Active", "Paused"] | None = None
    sync_settings: SyncSettings | None = None

    @model_validator(mode="after")
    def require_at_least_one_property(self) -> "MediaSourceUpdateRequest":
        if not self.model_fields_set:
            raise ValueError("At least one source property is required")
        if any(getattr(self, field_name) is None for field_name in self.model_fields_set):
            raise ValueError("Updated source properties cannot be null")
        return self


class MediaSource(ApiModel):
    source_id: UUID
    device_id: UUID
    source_key: str = Field(max_length=128)
    source_type: SourceType
    display_name: str
    storage_mode: StorageMode
    permission_state: PermissionState
    status: SourceStatus
    sync_settings: SyncSettings
    last_manifest_at_utc: datetime | None = None
    created_at_utc: datetime
    updated_at_utc: datetime


class MediaSourcePage(ApiModel):
    items: list[MediaSource]
    page: PageInfo


class FieldProvenance(ApiModel):
    field: str = Field(min_length=1, max_length=100)
    source: ProvenanceSource
    confidence: float | None = Field(default=None, ge=0, le=1)
    processor_version: str | None = Field(default=None, max_length=100)
    observed_at_utc: datetime | None = None


class GeoPointInput(ApiModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    altitude_meters: float | None = None
    horizontal_accuracy_meters: float | None = Field(default=None, ge=0)


class ManifestUpsertEntry(ApiModel):
    operation: Literal["Upsert"]
    source_item_id: str = Field(min_length=1, max_length=512)
    source_revision: str = Field(min_length=1, max_length=255)
    file_name: str = Field(min_length=1, max_length=512)
    local_locator: str | None = Field(default=None, max_length=4096)
    content_sha256: str | None = Field(
        default=None,
        pattern=r"^[A-Fa-f0-9]{64}$",
    )
    media_type: MediaType
    mime_type: str = Field(min_length=1, max_length=255)
    byte_size: int = Field(ge=1)
    width_pixels: int | None = Field(default=None, ge=1)
    height_pixels: int | None = Field(default=None, ge=1)
    duration_ms: int | None = Field(default=None, ge=0)
    captured_at_local: str | None = None
    captured_at_utc: datetime | None = None
    time_zone_id: str | None = Field(default=None, max_length=64)
    utc_offset_minutes: int | None = Field(default=None, ge=-840, le=840)
    location: GeoPointInput | None = None
    provenance: list[FieldProvenance] = Field(default_factory=list)

    @field_validator("captured_at_local")
    @classmethod
    def validate_local_wall_clock(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("capturedAtLocal must be an ISO 8601 date-time") from exc
        if parsed.tzinfo is not None:
            raise ValueError("capturedAtLocal must not include a time-zone offset")
        return value

    @field_validator("captured_at_utc")
    @classmethod
    def require_utc_offset(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("capturedAtUtc must include Z or a time-zone offset")
        return value

    @model_validator(mode="after")
    def validate_capture_time_consistency(self) -> "ManifestUpsertEntry":
        if (
            self.captured_at_local is None
            or self.utc_offset_minutes is None
            or self.captured_at_utc is None
        ):
            return self

        local_value = datetime.fromisoformat(self.captured_at_local)
        expected_utc = (
            local_value - timedelta(minutes=self.utc_offset_minutes)
        ).replace(tzinfo=timezone.utc)
        actual_utc = self.captured_at_utc.astimezone(timezone.utc)
        if abs((actual_utc - expected_utc).total_seconds()) > 1:
            raise ValueError(
                "capturedAtLocal, utcOffsetMinutes, and capturedAtUtc are inconsistent"
            )
        return self


class ManifestDeletedEntry(ApiModel):
    operation: Literal["Deleted"]
    source_item_id: str = Field(min_length=1, max_length=512)
    source_revision: str = Field(min_length=1, max_length=255)


ManifestEntry = Annotated[
    ManifestUpsertEntry | ManifestDeletedEntry,
    Field(discriminator="operation"),
]


class ManifestRequest(ApiModel):
    snapshot_id: UUID | None = None
    kind: ManifestKind
    permission_state: PermissionState
    deletion_detection_reliable: bool
    client_cursor: str | None = Field(default=None, max_length=1024)
    entries: list[ManifestEntry] = Field(min_length=1, max_length=500)


class ManifestEntryResult(ApiModel):
    source_item_id: str
    outcome: ManifestOutcome
    occurrence_id: UUID | None = None
    media_asset_id: UUID | None = None
    upload_required: bool
    error_code: str | None = None
    error_message: str | None = None


class ManifestCounts(ApiModel):
    created: int = Field(ge=0)
    updated: int = Field(ge=0)
    duplicates_linked: int = Field(ge=0)
    deleted: int = Field(ge=0)
    ignored_deletions: int = Field(ge=0)
    unchanged: int = Field(ge=0)
    rejected: int = Field(ge=0)


class ManifestResponse(ApiModel):
    source_id: UUID
    accepted_at_utc: datetime
    source_cursor: str | None = None
    counts: ManifestCounts
    results: list[ManifestEntryResult]


class TemporalMetadata(ApiModel):
    captured_at_local: str | None = None
    captured_at_utc: datetime | None = None
    time_zone_id: str | None = None
    utc_offset_minutes: int | None = Field(default=None, ge=-840, le=840)
    source: ProvenanceSource | None = None
    confidence: float | None = Field(default=None, ge=0, le=1)


class LocationSummary(ApiModel):
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    display_name: str | None = None
    city: str | None = None
    state: str | None = None
    country_code: str | None = Field(default=None, min_length=2, max_length=2)


class MediaAssetSummary(ApiModel):
    media_asset_id: UUID
    content_sha256: str = Field(pattern=r"^[A-Fa-f0-9]{64}$")
    media_type: MediaType
    mime_type: str
    byte_size: int = Field(ge=0)
    display_file_name: str
    storage_mode: StorageMode
    storage_state: StorageState
    availability: MediaAvailability
    state: UserFacingState
    temporal: TemporalMetadata
    duration_ms: int | None = Field(default=None, ge=0)
    width_pixels: int | None = Field(default=None, ge=1)
    height_pixels: int | None = Field(default=None, ge=1)
    category: str | None = None
    location: LocationSummary | None = None
    description_excerpt: str | None = Field(default=None, max_length=500)
    preview_url: str | None = None
    preview_url_expires_at_utc: datetime | None = None
    is_trashed: bool
    purge_after_utc: datetime | None = None
    created_at_utc: datetime
    updated_at_utc: datetime


class MediaAssetPage(ApiModel):
    items: list[MediaAssetSummary]
    page: PageInfo


class MediaSearchHit(ApiModel):
    asset: MediaAssetSummary
    matched_field: MatchField
    highlight: str | None = Field(default=None, max_length=1000)
    transcript_segment_id: UUID | None = None
    seek_to_ms: int | None = Field(default=None, ge=0)


class MediaSearchPage(ApiModel):
    items: list[MediaSearchHit]
    page: PageInfo


class MediaOccurrence(ApiModel):
    occurrence_id: UUID
    source_id: UUID
    source_item_id: str
    source_revision: str
    exact_file_name: str
    local_locator: str | None = None
    first_seen_at_utc: datetime
    last_seen_at_utc: datetime
    is_deleted: bool
    deleted_at_utc: datetime | None = None


class MediaLocation(ApiModel):
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    altitude_meters: float | None = None
    horizontal_accuracy_meters: float | None = Field(default=None, ge=0)
    display_name: str | None = None
    street_address: str | None = None
    neighborhood: str | None = None
    city: str | None = None
    county: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None
    country_code: str | None = Field(default=None, min_length=2, max_length=2)
    provenance: list[FieldProvenance]


class MediaDescription(ApiModel):
    status: EnrichmentStatus
    text: str | None = None
    provider: str | None = None
    model: str | None = None
    prompt_version: str | None = None
    updated_at_utc: datetime | None = None


class TranscriptSegment(ApiModel):
    segment_id: UUID
    index: int = Field(ge=0)
    start_ms: int = Field(ge=0)
    end_ms: int = Field(ge=0)
    speaker: str | None = None
    text: str
    confidence: float | None = Field(default=None, ge=0, le=1)


class MediaTranscript(ApiModel):
    transcript_id: UUID
    status: EnrichmentStatus
    language_code: str | None = Field(default=None, max_length=20)
    provider: str | None = None
    model: str | None = None
    provider_request_id: str | None = None
    full_text: str | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    segments: list[TranscriptSegment]
    updated_at_utc: datetime | None = None


class RemoteMediaAccess(ApiModel):
    original_url: str
    preview_url: str | None = None
    expires_at_utc: datetime


class MediaAssetDetail(MediaAssetSummary):
    occurrences: list[MediaOccurrence]
    location_detail: MediaLocation | None = None
    description: MediaDescription | None = None
    transcript: MediaTranscript | None = None
    remote_access: RemoteMediaAccess | None = None
    provenance: list[FieldProvenance]


class ProcessingJob(ApiModel):
    job_id: UUID
    media_asset_id: UUID
    job_type: ProcessingJobType
    status: ProcessingJobStatus
    state: UserFacingState
    attempt_count: int = Field(ge=0)
    next_attempt_at_utc: datetime | None = None
    failure_class: FailureClass | None = None
    error_code: str | None = None
    user_message: str | None = None
    can_retry: bool = False
    created_at_utc: datetime
    started_at_utc: datetime | None = None
    completed_at_utc: datetime | None = None
    updated_at_utc: datetime


class ProcessingJobPage(ApiModel):
    items: list[ProcessingJob]
    page: PageInfo


class MediaChange(ApiModel):
    cursor: str
    change_type: ChangeType
    resource_type: ChangeResourceType
    resource_id: UUID
    occurred_at_utc: datetime
    media_asset: MediaAssetSummary | None = None
    source: MediaSource | None = None
    processing_job: ProcessingJob | None = None


class ChangePage(ApiModel):
    items: list[MediaChange]
    page: PageInfo


class DependencyHealth(ApiModel):
    name: str
    status: HealthStatus
    latency_ms: int | None = Field(default=None, ge=0)
    message: str | None = Field(default=None, max_length=500)


class AdminHealthResponse(ApiModel):
    service: str
    version: str
    status: HealthStatus
    time_utc: datetime
    dependencies: list[DependencyHealth]


class AuditCheck(ApiModel):
    code: str
    severity: AuditSeverity
    title: str
    count: int = Field(ge=0)
    details: str | None = Field(default=None, max_length=2000)


class AdminAuditResponse(ApiModel):
    run_at_utc: datetime
    read_only: Literal[True]
    status: HealthStatus
    checks: list[AuditCheck]


class FieldError(ApiModel):
    field: str
    code: str
    message: str


class ProblemDetails(ApiModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    code: str
    detail: str | None = None
    instance: str | None = None
    trace_id: str
    field_errors: list[FieldError] = Field(default_factory=list)
