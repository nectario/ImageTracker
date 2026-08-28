from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Generic, Literal, Protocol, TypeVar
from uuid import UUID


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None
    has_more: bool


@dataclass(frozen=True)
class MutationContext:
    request_id: UUID
    idempotency_key: str
    operation: str
    target: str
    request_hash: str


@dataclass(frozen=True)
class MutationResult(Generic[T]):
    value: T
    status_code: int
    replayed: bool = False


class JobDispatcher(Protocol):
    """Infrastructure boundary for publishing durable processing-job identities."""

    def dispatch(self, *, job_ids: tuple[UUID, ...], job_type: str) -> None: ...


@dataclass(frozen=True)
class TemporaryObjectUpload:
    """A checksum-bound direct PUT prepared by the object-store adapter."""

    bucket: str
    object_key: str
    url: str
    headers: dict[str, str]
    expires_at_utc: datetime


@dataclass(frozen=True)
class TemporaryObjectMetadata:
    """Normalized object metadata returned by a provider HEAD request."""

    byte_size: int
    content_type: str
    checksum_sha256_hex: str


class TemporaryObjectStore(Protocol):
    """Provider-neutral boundary for short-lived scene-preview objects.

    ``create_presigned_put`` must bind the exact base64 SHA-256 value, content
    type, and content length into the signed PUT. It returns the private bucket
    and key chosen by the adapter; the API never exposes either locator.
    """

    def create_presigned_put(
        self,
        *,
        user_id: UUID,
        media_asset_id: UUID,
        upload_session_id: UUID,
        checksum_sha256_base64: str,
        content_type: str,
        content_length: int,
        url_expires_at_utc: datetime,
        object_expires_at_utc: datetime,
    ) -> TemporaryObjectUpload: ...

    def head_object(
        self, *, bucket: str, object_key: str
    ) -> TemporaryObjectMetadata | None: ...

    def delete_object(self, *, bucket: str, object_key: str) -> None: ...


@dataclass(frozen=True)
class AccountIdentity:
    cognito_subject: str
    email: str | None = None
    display_name: str | None = None


@dataclass(frozen=True)
class UserRecord:
    user_id: UUID
    email: str | None
    display_name: str | None
    created_at_utc: datetime


@dataclass(frozen=True)
class DeviceRegistration:
    installation_id: UUID
    platform: str
    display_name: str
    app_version: str
    os_version: str


@dataclass(frozen=True)
class DeviceRecord:
    device_id: UUID
    installation_id: UUID
    platform: str
    display_name: str
    app_version: str
    os_version: str
    status: Literal["Active", "Removed"]
    registered_at_utc: datetime
    last_seen_at_utc: datetime


@dataclass(frozen=True)
class SyncSettings:
    automatic_sync: bool = True
    network_policy: str = "WiFiOnly"
    require_charging_for_historical_upload: bool = True

    def as_json(self) -> dict[str, object]:
        return {
            "automaticSync": self.automatic_sync,
            "networkPolicy": self.network_policy,
            "requireChargingForHistoricalUpload": self.require_charging_for_historical_upload,
        }

    @classmethod
    def from_json(cls, value: dict[str, object] | None) -> "SyncSettings":
        value = value or {}
        return cls(
            automatic_sync=bool(value.get("automaticSync", True)),
            network_policy=str(value.get("networkPolicy", "WiFiOnly")),
            require_charging_for_historical_upload=bool(
                value.get("requireChargingForHistoricalUpload", True)
            ),
        )


@dataclass(frozen=True)
class SourceCreate:
    device_id: UUID
    source_key: str
    source_type: str
    display_name: str
    storage_mode: str = "Local"
    permission_state: str = "NotApplicable"
    sync_settings: SyncSettings = field(default_factory=SyncSettings)


@dataclass(frozen=True)
class SourceUpdate:
    display_name: str | None = None
    storage_mode: str | None = None
    permission_state: str | None = None
    status: str | None = None
    sync_settings: SyncSettings | None = None


@dataclass(frozen=True)
class SourceRecord:
    source_id: UUID
    device_id: UUID
    source_key: str
    source_type: str
    display_name: str
    storage_mode: str
    permission_state: str
    status: str
    sync_settings: SyncSettings
    last_manifest_at_utc: datetime | None
    created_at_utc: datetime
    updated_at_utc: datetime


@dataclass(frozen=True)
class GeoPoint:
    latitude: Decimal
    longitude: Decimal
    altitude_meters: Decimal | None = None
    horizontal_accuracy_meters: Decimal | None = None


@dataclass(frozen=True)
class FieldProvenance:
    field: str
    source: str
    confidence: Decimal | None = None
    processor_version: str | None = None
    observed_at_utc: datetime | None = None

    def as_json(self) -> dict[str, object | None]:
        return {
            "field": self.field,
            "source": self.source,
            "confidence": float(self.confidence) if self.confidence is not None else None,
            "processorVersion": self.processor_version,
            "observedAtUtc": (
                self.observed_at_utc.isoformat() if self.observed_at_utc is not None else None
            ),
        }


@dataclass(frozen=True)
class ManifestUpsert:
    source_item_id: str
    source_revision: str
    file_name: str
    media_type: str
    mime_type: str
    byte_size: int
    local_locator: str | None = None
    content_sha256: str | None = None
    width_pixels: int | None = None
    height_pixels: int | None = None
    duration_ms: int | None = None
    captured_at_local: datetime | None = None
    captured_at_utc: datetime | None = None
    time_zone_id: str | None = None
    utc_offset_minutes: int | None = None
    location: GeoPoint | None = None
    provenance: tuple[FieldProvenance, ...] = ()
    operation: Literal["Upsert"] = "Upsert"


@dataclass(frozen=True)
class ManifestDelete:
    source_item_id: str
    source_revision: str
    operation: Literal["Deleted"] = "Deleted"


ManifestEntry = ManifestUpsert | ManifestDelete


@dataclass(frozen=True)
class ManifestCommand:
    kind: str
    permission_state: str
    deletion_detection_reliable: bool
    entries: tuple[ManifestEntry, ...]
    snapshot_id: UUID | None = None
    client_cursor: str | None = None


@dataclass(frozen=True)
class ManifestEntryResult:
    source_item_id: str
    outcome: str
    occurrence_id: UUID | None = None
    media_asset_id: UUID | None = None
    upload_required: bool = False
    description_job_id: UUID | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ManifestCounts:
    created: int = 0
    updated: int = 0
    duplicates_linked: int = 0
    deleted: int = 0
    ignored_deletions: int = 0
    unchanged: int = 0
    rejected: int = 0


@dataclass(frozen=True)
class ManifestResult:
    source_id: UUID
    accepted_at_utc: datetime
    source_cursor: str | None
    counts: ManifestCounts
    results: tuple[ManifestEntryResult, ...]


@dataclass(frozen=True)
class MediaQuery:
    cursor: str | None = None
    limit: int = 50
    source_id: UUID | None = None
    media_type: str | None = None
    storage_mode: str | None = None
    captured_after_utc: datetime | None = None
    captured_before_utc: datetime | None = None
    category: str | None = None
    has_location: bool | None = None
    trash_state: str = "Active"
    sort: str = "CapturedAtDesc"


@dataclass(frozen=True)
class MediaLocationRecord:
    latitude: Decimal | None
    longitude: Decimal | None
    altitude_meters: Decimal | None
    horizontal_accuracy_meters: Decimal | None
    display_name: str | None
    street_address: str | None
    neighborhood: str | None
    city: str | None
    county: str | None
    state: str | None
    postal_code: str | None
    country: str | None
    country_code: str | None
    provenance: tuple[FieldProvenance, ...] = ()
    original_street_number: str | None = None
    provider: str | None = None
    provider_place_id: str | None = None
    normalization_rule_version: str | None = None
    provider_updated_at_utc: datetime | None = None


@dataclass(frozen=True)
class MediaSummary:
    media_asset_id: UUID
    content_sha256: str
    media_type: str
    mime_type: str
    byte_size: int
    display_file_name: str
    storage_mode: str
    storage_state: str
    availability: str
    state: str
    captured_at_local: datetime | None
    captured_at_utc: datetime | None
    time_zone_id: str | None
    utc_offset_minutes: int | None
    capture_time_source: str | None
    capture_time_confidence: Decimal | None
    duration_ms: int | None
    width_pixels: int | None
    height_pixels: int | None
    category: str | None
    location: MediaLocationRecord | None
    description_excerpt: str | None
    is_trashed: bool
    purge_after_utc: datetime | None
    created_at_utc: datetime
    updated_at_utc: datetime


@dataclass(frozen=True)
class OccurrenceRecord:
    occurrence_id: UUID
    source_id: UUID
    source_item_id: str
    source_revision: str
    exact_file_name: str
    local_locator: str | None
    first_seen_at_utc: datetime
    last_seen_at_utc: datetime
    is_deleted: bool
    deleted_at_utc: datetime | None


@dataclass(frozen=True)
class DescriptionRecord:
    status: str
    text: str | None
    provider: str | None
    model: str | None
    prompt_version: str | None
    updated_at_utc: datetime | None


@dataclass(frozen=True)
class TranscriptSegmentRecord:
    segment_id: UUID
    index: int
    start_ms: int
    end_ms: int
    speaker: str | None
    text: str
    confidence: Decimal | None


@dataclass(frozen=True)
class TranscriptRecord:
    transcript_id: UUID
    status: str
    language_code: str | None
    provider: str | None
    model: str | None
    provider_request_id: str | None
    full_text: str | None
    duration_ms: int | None
    segments: tuple[TranscriptSegmentRecord, ...]
    updated_at_utc: datetime | None


@dataclass(frozen=True)
class MediaDetail:
    asset: MediaSummary
    occurrences: tuple[OccurrenceRecord, ...]
    location_detail: MediaLocationRecord | None
    description: DescriptionRecord | None
    transcript: TranscriptRecord | None
    provenance: tuple[FieldProvenance, ...]


@dataclass(frozen=True)
class MediaSearchQuery:
    text: str
    filters: MediaQuery = field(default_factory=MediaQuery)


@dataclass(frozen=True)
class MediaSearchHit:
    asset: MediaSummary
    matched_field: str
    highlight: str | None = None
    transcript_segment_id: UUID | None = None
    seek_to_ms: int | None = None


@dataclass(frozen=True)
class ChangeRecord:
    cursor: str
    change_type: str
    resource_type: str
    resource_id: UUID
    occurred_at_utc: datetime


@dataclass(frozen=True)
class JobQuery:
    cursor: str | None = None
    limit: int = 50
    status: str | None = None
    job_type: str | None = None
    media_asset_id: UUID | None = None


@dataclass(frozen=True)
class JobRecord:
    job_id: UUID
    media_asset_id: UUID
    job_type: str
    status: str
    state: str
    attempt_count: int
    next_attempt_at_utc: datetime | None
    failure_class: str | None
    error_code: str | None
    user_message: str | None
    can_retry: bool
    created_at_utc: datetime
    started_at_utc: datetime | None
    completed_at_utc: datetime | None
    updated_at_utc: datetime


@dataclass(frozen=True)
class UploadPlanCommand:
    source_id: UUID
    occurrence_id: UUID
    asset_content_sha256: str
    object_sha256: str
    file_name: str
    media_type: str
    object_mime_type: str
    object_byte_size: int
    purpose: str
    processing_job_id: UUID | None = None


@dataclass(frozen=True)
class SignedUploadRequestRecord:
    url: str
    method: Literal["PUT"]
    headers: dict[str, str]
    expires_at_utc: datetime


@dataclass(frozen=True)
class UploadPlanRecord:
    disposition: str
    strategy: str
    media_asset_id: UUID
    occurrence_id: UUID
    upload_session_id: UUID | None
    expires_at_utc: datetime | None
    deduplicated: bool
    retry_after_seconds: int | None = None
    single_part: SignedUploadRequestRecord | None = None


@dataclass(frozen=True)
class UploadSessionRecord:
    upload_session_id: UUID
    strategy: str
    status: str
    expected_byte_size: int
    uploaded_byte_size: int
    expires_at_utc: datetime | None


@dataclass(frozen=True)
class UploadCompleteCommand:
    object_sha256: str
    etag: str | None = None
    parts: tuple[object, ...] = ()


@dataclass(frozen=True)
class UploadCompleteRecord:
    media_asset_id: UUID
    storage_state: str
    processing_jobs: tuple[UUID, ...]
