from __future__ import annotations

from copy import deepcopy
import base64
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import logging
import re
from typing import Any, Callable, Mapping, Sequence, TypeVar
from urllib.parse import urlsplit
from uuid import UUID
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import Numeric, and_, case, cast, func, or_, select, update
from sqlalchemy.orm import Session, aliased, sessionmaker

from services.data.database import transaction_scope
from services.data.models import (
    Device,
    MediaAsset,
    MediaDescription,
    MediaLocation,
    MediaOccurrence,
    MediaSource,
    MediaTranscript,
    MediaTranscriptSegment,
    ProcessingJob,
    UploadSession,
    UserAccount,
    utc_now,
)
from services.domain.cursor import CursorCodec
from services.domain.errors import (
    ConflictError,
    ForbiddenError,
    InvalidCursorError,
    NotFoundError,
    RetryNotAllowedError,
)
from services.domain.models import (
    AccountIdentity,
    ChangeRecord,
    DescriptionRecord,
    DeviceRecord,
    DeviceRegistration,
    EnrichmentPreparation,
    EnrichmentPrepareCommand,
    FieldProvenance,
    JobDispatcher,
    JobQuery,
    JobRecord,
    ManifestCommand,
    ManifestCounts,
    ManifestDelete,
    ManifestEntryResult,
    ManifestResult,
    ManifestUpsert,
    MediaDetail,
    MediaLocationRecord,
    MediaQuery,
    MediaSearchHit,
    MediaSearchQuery,
    MediaSummary,
    MutationContext,
    MutationResult,
    OccurrenceRecord,
    Page,
    SceneDescriptionTaskRecord,
    SourceCreate,
    SourceRecord,
    SourceUpdate,
    SyncSettings,
    SignedUploadRequestRecord,
    TemporaryObjectStore,
    TranscriptRecord,
    TranscriptSegmentRecord,
    UploadCompleteCommand,
    UploadCompleteRecord,
    UploadPlanCommand,
    UploadPlanRecord,
    UploadSessionRecord,
    UserRecord,
)
from services.enrichment.models import (
    AMAZON_LOCATION_PROVIDER,
    GeocodeResolution,
    ProviderFailureClass,
    ReverseGeocodeResult,
)
from services.enrichment.normalization import (
    LocationNormalizationRuleset,
    LocationNormalizer,
)
from services.enrichment.openai_scene import (
    SCENE_DESCRIPTION_PROMPT_VERSION,
    SceneDescriptionResult,
    USD_QUANTUM,
    scene_description_cost_usd,
    scene_description_maximum_cost_usd,
    scene_description_usage_within_bounds,
)
from services.domain.repositories import (
    AccountRepository,
    AssetRepository,
    ChangeRepository,
    DeviceRepository,
    IdempotencyRepository,
    JobRepository,
    LocationRepository,
    OccurrenceRepository,
    ProviderUsageRepository,
    SourceRepository,
    UploadRepository,
)
from services.worker.contracts import (
    DescriptionCleanupDecision,
    DescriptionFailureOutcome,
    DescriptionJob,
    DescriptionJobFailure,
    GeocodeJob,
    GeocodeJobFailure,
)


T = TypeVar("T")
logger = logging.getLogger(__name__)
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
GEOCODE_PROVIDER = AMAZON_LOCATION_PROVIDER
DEFAULT_GEOCODE_REUSE_RADIUS_METERS = 5.0
GEOCODE_LEASE_SECONDS = 300
DESCRIPTION_LEASE_SECONDS = 900
DISPATCH_RECOVERY_DELAY = timedelta(minutes=5)
DESCRIPTION_PROVIDER = "OpenAI"
SCENE_PREVIEW_MIME_TYPE = "image/jpeg"
SCENE_PREVIEW_MAX_BYTES = 25 * 1024 * 1024
MAX_ENRICHMENT_PREPARE = 64
SCENE_PREVIEW_EXTENSIONS = (
    ".avif",
    ".bmp",
    ".gif",
    ".heic",
    ".heif",
    ".jpeg",
    ".jpg",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
)
RAW_PHOTO_EXTENSIONS = (".arw", ".cr2", ".cr3", ".dng", ".nef", ".rw2")
TEMPORARY_UPLOAD_LEASE = timedelta(days=1)
TEMPORARY_UPLOAD_URL_LIFETIME = timedelta(minutes=15)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _db_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _uuid(value: str) -> UUID:
    return UUID(value)


def _coordinate_revision(latitude: Decimal, longitude: Decimal) -> str:
    coordinate = f"{latitude:.6f},{longitude:.6f}"
    return hashlib.sha256(coordinate.encode("ascii")).hexdigest()


def _lease_hash(lease_owner: str) -> str:
    return hashlib.sha256(lease_owner.encode("utf-8")).hexdigest()


def _json_value(value: Any) -> Any:
    if is_dataclass(value):
        return {
            item.name: _json_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return _utc(value)
    return _utc(datetime.fromisoformat(value))


def _require_page_limit(limit: int) -> None:
    if not 1 <= limit <= 200:
        raise ConflictError("InvalidPageSize", "Page size must be between 1 and 200")


def _decode_single_id(
    codec: CursorCodec, cursor: str | None, *, kind: str
) -> int | None:
    position = codec.decode(cursor, kind=kind)
    if position is None:
        return None
    if len(position) != 1 or not isinstance(position[0], int) or position[0] < 0:
        raise InvalidCursorError()
    return position[0]


class Phase1DomainService:
    """Transaction-scoped, ownership-safe Phase 1 application service.

    Methods are async to fit FastAPI's service protocol. SQLAlchemy work is
    deliberately synchronous: each Lambda runtime has a one-connection pool,
    and every method performs one short transaction.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        cursor_codec: CursorCodec | None = None,
        clock: Callable[[], datetime] = utc_now,
        trash_retention_days: int = 30,
        idempotency_hours: int = 24,
        job_dispatcher: JobDispatcher | None = None,
        enrichment_processing_enabled: bool = True,
        temporary_object_store: TemporaryObjectStore | None = None,
        scene_description_model: str = "gpt-5.6-terra",
        scene_description_detail: str = "high",
        scene_description_service_tier: str = "flex",
        scene_description_max_words: int = 24,
        scene_description_monthly_call_limit: int = 100_000,
        scene_description_monthly_usd_limit: Decimal = Decimal("230.000000"),
        scene_description_reserved_usd_per_request: Decimal = Decimal("0.010000"),
        scene_description_input_usd_per_million: Decimal = Decimal("2.000000"),
        scene_description_cached_input_usd_per_million: Decimal = Decimal("0.200000"),
        scene_description_output_usd_per_million: Decimal = Decimal("12.000000"),
        geocode_reuse_radius_meters: float = DEFAULT_GEOCODE_REUSE_RADIUS_METERS,
        location_normalizer: LocationNormalizer | None = None,
    ) -> None:
        if not 0 <= geocode_reuse_radius_meters <= 100:
            raise ValueError("The geocode reuse radius must be between 0 and 100 meters")
        if not isinstance(enrichment_processing_enabled, bool):
            raise ValueError("The enrichment processing switch must be boolean")
        if scene_description_monthly_call_limit < 0:
            raise ValueError("The scene-description monthly limit cannot be negative")
        cost_values = {
            "monthly USD limit": Decimal(scene_description_monthly_usd_limit),
            "request USD reservation": Decimal(
                scene_description_reserved_usd_per_request
            ),
            "input rate": Decimal(scene_description_input_usd_per_million),
            "cached input rate": Decimal(
                scene_description_cached_input_usd_per_million
            ),
            "output rate": Decimal(scene_description_output_usd_per_million),
        }
        if any(not value.is_finite() or value < 0 for value in cost_values.values()):
            raise ValueError("Scene-description USD settings must be finite and non-negative")
        if cost_values["request USD reservation"] <= 0:
            raise ValueError("The scene-description USD reservation must be positive")
        if (
            cost_values["monthly USD limit"] > 0
            and cost_values["request USD reservation"]
            > cost_values["monthly USD limit"]
        ):
            raise ValueError("The scene-description USD reservation exceeds its limit")
        maximum_scene_cost = scene_description_maximum_cost_usd(
            input_usd_per_million=cost_values["input rate"],
            cached_input_usd_per_million=cost_values["cached input rate"],
            output_usd_per_million=cost_values["output rate"],
        )
        if cost_values["request USD reservation"] < maximum_scene_cost:
            raise ValueError(
                "The scene-description USD reservation is below the maximum request cost"
            )
        if not scene_description_model or len(scene_description_model) > 128:
            raise ValueError("The scene-description model identity is invalid")
        if scene_description_detail not in {"low", "high"}:
            raise ValueError("The scene-description detail is invalid")
        if scene_description_service_tier not in {"auto", "default", "flex"}:
            raise ValueError("The scene-description service tier is invalid")
        if not 8 <= scene_description_max_words <= 24:
            raise ValueError("The scene-description word limit is invalid")
        self._session_factory = session_factory
        self._cursor = cursor_codec or CursorCodec()
        self._clock = clock
        self._trash_retention = timedelta(days=trash_retention_days)
        self._idempotency_retention = timedelta(hours=idempotency_hours)
        self._job_dispatcher = job_dispatcher
        self._enrichment_processing_enabled = enrichment_processing_enabled
        self._temporary_object_store = temporary_object_store
        self._scene_description_model = scene_description_model
        self._scene_description_detail = scene_description_detail
        self._scene_description_service_tier = scene_description_service_tier
        self._scene_description_max_words = scene_description_max_words
        self._scene_description_monthly_call_limit = (
            scene_description_monthly_call_limit
        )
        self._scene_description_monthly_usd_limit = cost_values[
            "monthly USD limit"
        ].quantize(USD_QUANTUM)
        self._scene_description_reserved_usd_per_request = cost_values[
            "request USD reservation"
        ].quantize(USD_QUANTUM)
        self._scene_description_input_usd_per_million = cost_values[
            "input rate"
        ]
        self._scene_description_cached_input_usd_per_million = cost_values[
            "cached input rate"
        ]
        self._scene_description_output_usd_per_million = cost_values[
            "output rate"
        ]
        self._geocode_reuse_radius_meters = geocode_reuse_radius_meters
        self._location_normalizer = location_normalizer or LocationNormalizer(
            LocationNormalizationRuleset(rules=(), version="none")
        )

    def _now(self) -> datetime:
        return _db_datetime(self._clock()) or utc_now()

    def _dispatch_after_commit(
        self, *, job_ids: tuple[UUID, ...], job_type: str
    ) -> None:
        if not job_ids or self._job_dispatcher is None:
            return
        try:
            self._job_dispatcher.dispatch(job_ids=job_ids, job_type=job_type)
        except Exception as exc:
            # The DB job is already committed and the enabled recovery sweep
            # will republish it. Never leak an SDK message or queue URL.
            logger.warning(
                "Deferred processing dispatch jobType=%s count=%s errorType=%s",
                job_type,
                len(job_ids),
                type(exc).__name__,
            )

    def _account(self, session: Session, user_id: UUID | str) -> UserAccount:
        return AccountRepository(session).require_by_public_id(user_id)

    def _mutation(
        self,
        *,
        session: Session,
        account: UserAccount,
        context: MutationContext,
        action: Callable[[], tuple[T, int]],
        replay_decoder: Callable[[Mapping[str, Any]], T],
    ) -> MutationResult[T]:
        if (
            not context.idempotency_key
            or len(context.idempotency_key) > 128
            or not HEX_SHA256.fullmatch(context.request_hash.lower())
            or not context.operation
            or len(context.operation) > 16
            or not context.target
            or len(context.target) > 255
        ):
            raise ConflictError(
                "InvalidIdempotencyContext", "The mutation identity is invalid"
            )
        # One inexpensive per-user row lock serializes idempotency reservation
        # and exact-hash creation without adding an external lock service.
        session.execute(
            select(UserAccount.id)
            .where(UserAccount.id == account.id)
            .with_for_update()
        )
        now = self._now()
        record, writable = IdempotencyRepository(session).reserve(
            user_id=account.id,
            idempotency_key=context.idempotency_key,
            http_method=context.operation,
            route_pattern=context.target,
            request_sha256=context.request_hash.lower(),
            expires_at_utc=now + self._idempotency_retention,
            now=now,
        )
        if not writable:
            if record.response_status_code is None or not isinstance(
                record.response_body_json, dict
            ):
                raise ConflictError(
                    "RequestInProgress",
                    "A request with this idempotency key is already in progress",
                )
            payload = record.response_body_json.get("value")
            if not isinstance(payload, dict):
                payload = {"is_none": True}
            return MutationResult(
                value=replay_decoder(payload),
                status_code=record.response_status_code,
                replayed=True,
            )

        value, status_code = action()
        record.response_status_code = status_code
        record.response_headers_json = {
            "Idempotency-Replayed": "false",
            "X-Request-Id": str(context.request_id),
        }
        encoded = _json_value(value)
        record.response_body_json = {
            "value": encoded if isinstance(encoded, dict) else {"is_none": True}
        }
        record.updated_at_utc = now
        session.flush()
        return MutationResult(value=value, status_code=status_code, replayed=False)

    async def current_user(self, identity: AccountIdentity) -> UserRecord:
        if not identity.cognito_subject:
            raise ForbiddenError("InvalidIdentity", "The authenticated identity is incomplete")
        with transaction_scope(self._session_factory) as session:
            account, _ = AccountRepository(session).bootstrap(
                cognito_subject=identity.cognito_subject,
                email=identity.email,
                display_name=identity.display_name,
                now=self._now(),
            )
            return self._user_record(account)

    async def list_devices(
        self, user_id: UUID, cursor: str | None = None, limit: int = 50
    ) -> Page[DeviceRecord]:
        _require_page_limit(limit)
        after_id = _decode_single_id(self._cursor, cursor, kind="devices")
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            rows = DeviceRepository(session).list_after(
                user_id=account.id, after_id=after_id, limit=limit + 1
            )
            return self._simple_page(
                rows,
                limit=limit,
                kind="devices",
                convert=self._device_record,
            )

    async def register_device(
        self,
        user_id: UUID,
        command: DeviceRegistration,
        context: MutationContext,
    ) -> MutationResult[DeviceRecord]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[DeviceRecord, int]:
                device, created = DeviceRepository(session).register(
                    user_id=account.id,
                    device_key=str(command.installation_id),
                    display_name=command.display_name,
                    platform=command.platform,
                    app_version=command.app_version,
                    os_version=command.os_version,
                    now=self._now(),
                )
                return self._device_record(device), 201 if created else 200

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._device_from_json,
            )

    async def list_sources(
        self, user_id: UUID, cursor: str | None = None, limit: int = 50
    ) -> Page[SourceRecord]:
        _require_page_limit(limit)
        after_id = _decode_single_id(self._cursor, cursor, kind="sources")
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            repository = SourceRepository(session)
            rows = repository.list_after(
                user_id=account.id, after_id=after_id, limit=limit + 1
            )
            items = tuple(
                self._source_record(
                    source,
                    repository.device_for(user_id=account.id, source=source),
                )
                for source in rows[:limit]
            )
            return Page(
                items=items,
                has_more=len(rows) > limit,
                next_cursor=(
                    self._cursor.encode("sources", [rows[limit - 1].id])
                    if len(rows) > limit and items
                    else None
                ),
            )

    async def create_source(
        self,
        user_id: UUID,
        command: SourceCreate,
        context: MutationContext,
    ) -> MutationResult[SourceRecord]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[SourceRecord, int]:
                device = DeviceRepository(session).require(
                    user_id=account.id, device_public_id=command.device_id
                )
                source, disposition = SourceRepository(session).create(
                    user_id=account.id,
                    device=device,
                    source_key=command.source_key,
                    source_type=command.source_type,
                    display_name=command.display_name,
                    storage_mode=command.storage_mode,
                    permission_state=command.permission_state,
                    sync_policy_json=command.sync_settings.as_json(),
                    now=self._now(),
                )
                if disposition != "Existing":
                    ChangeRepository(session).add(
                        user_id=account.id,
                        device_id=device.id,
                        source_id=source.id,
                        entity_type="MediaSource",
                        entity_id=source.id,
                        entity_public_id=source.public_id,
                        change_type="Upsert",
                        now=self._now(),
                    )
                return (
                    self._source_record(source, device),
                    201 if disposition == "Created" else 200,
                )

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._source_from_json,
            )

    async def get_source(self, user_id: UUID, source_id: UUID) -> SourceRecord:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            repository = SourceRepository(session)
            source = repository.require(
                user_id=account.id, source_public_id=source_id
            )
            return self._source_record(
                source, repository.device_for(user_id=account.id, source=source)
            )

    async def update_source(
        self,
        user_id: UUID,
        source_id: UUID,
        command: SourceUpdate,
        context: MutationContext,
    ) -> MutationResult[SourceRecord]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[SourceRecord, int]:
                repository = SourceRepository(session)
                source = repository.require(
                    user_id=account.id, source_public_id=source_id
                )
                old_mode = source.storage_mode
                if command.display_name is not None:
                    source.display_name = command.display_name
                if command.storage_mode is not None:
                    source.storage_mode = command.storage_mode
                if command.permission_state is not None:
                    source.permission_state = command.permission_state
                if command.status is not None:
                    source.source_status = command.status
                if command.sync_settings is not None:
                    source.sync_policy_json = command.sync_settings.as_json()
                source.updated_at_utc = self._now()
                session.flush()
                if old_mode == "Local" and source.storage_mode == "Remote":
                    assets = session.scalars(
                        select(MediaAsset)
                        .join(
                            MediaOccurrence,
                            and_(
                                MediaOccurrence.user_id == MediaAsset.user_id,
                                MediaOccurrence.media_asset_id == MediaAsset.id,
                            ),
                        )
                        .where(
                            MediaAsset.user_id == account.id,
                            MediaOccurrence.media_source_id == source.id,
                            MediaOccurrence.deletion_state == "Active",
                            MediaAsset.storage_state == "LocalOnly",
                        )
                    ).all()
                    for asset in assets:
                        asset.storage_state = "UploadPending"
                        ChangeRepository(session).add(
                            user_id=account.id,
                            source_id=source.id,
                            asset_id=asset.id,
                            entity_type="MediaAsset",
                            entity_id=asset.id,
                            entity_public_id=asset.public_id,
                            change_type="Upsert",
                            now=self._now(),
                        )
                device = repository.device_for(user_id=account.id, source=source)
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=device.id,
                    source_id=source.id,
                    entity_type="MediaSource",
                    entity_id=source.id,
                    entity_public_id=source.public_id,
                    change_type="Upsert",
                    now=self._now(),
                )
                return self._source_record(source, device), 200

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._source_from_json,
            )

    async def remove_source(
        self,
        user_id: UUID,
        source_id: UUID,
        context: MutationContext,
    ) -> MutationResult[None]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[None, int]:
                source = SourceRepository(session).require(
                    user_id=account.id, source_public_id=source_id
                )
                now = self._now()
                occurrences = session.scalars(
                    select(MediaOccurrence).where(
                        MediaOccurrence.user_id == account.id,
                        MediaOccurrence.media_source_id == source.id,
                        MediaOccurrence.deletion_state == "Active",
                    )
                ).all()
                for occurrence in occurrences:
                    self._delete_occurrence(
                        session=session,
                        account=account,
                        source=source,
                        occurrence=occurrence,
                        now=now,
                    )
                source.source_status = "Removed"
                source.removed_at_utc = now
                source.updated_at_utc = now
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=source.device_id,
                    source_id=source.id,
                    entity_type="MediaSource",
                    entity_id=source.id,
                    entity_public_id=source.public_id,
                    change_type="Delete",
                    now=now,
                )
                return None, 204

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=lambda _: None,
            )

    async def submit_manifest(
        self,
        user_id: UUID,
        source_id: UUID,
        command: ManifestCommand,
        context: MutationContext,
    ) -> MutationResult[ManifestResult]:
        if not 1 <= len(command.entries) <= 500:
            raise ConflictError(
                "InvalidManifestSize", "A manifest must contain between 1 and 500 entries"
            )
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[ManifestResult, int]:
                source = SourceRepository(session).require(
                    user_id=account.id, source_public_id=source_id
                )
                now = self._now()
                results: list[ManifestEntryResult] = []
                counts = {
                    "created": 0,
                    "updated": 0,
                    "duplicates_linked": 0,
                    "deleted": 0,
                    "ignored_deletions": 0,
                    "unchanged": 0,
                    "rejected": 0,
                }
                deletion_allowed = command.deletion_detection_reliable and (
                    command.permission_state not in {"Limited", "Denied", "Unavailable"}
                )
                seen_item_ids: set[str] = set()
                source_item_ids = {entry.source_item_id for entry in command.entries}
                occurrence_index = OccurrenceRepository(session).by_source_items(
                    user_id=account.id,
                    source_id=source.id,
                    source_item_ids=source_item_ids,
                )
                content_hashes = {
                    entry.content_sha256.lower()
                    for entry in command.entries
                    if isinstance(entry, ManifestUpsert)
                    and isinstance(entry.content_sha256, str)
                    and HEX_SHA256.fullmatch(entry.content_sha256.lower())
                }
                asset_index = AssetRepository(session).by_hashes(
                    user_id=account.id,
                    sha256_values=content_hashes,
                )
                new_asset_hashes = self._prepare_manifest_assets(
                    session=session,
                    source=source,
                    entries=command.entries,
                    asset_index=asset_index,
                    now=now,
                )
                deferred_pending_occurrences: list[MediaOccurrence] = []
                deferred_hashed_occurrences: list[MediaOccurrence] = []
                for entry in command.entries:
                    if entry.source_item_id in seen_item_ids:
                        result = ManifestEntryResult(
                            source_item_id=entry.source_item_id,
                            outcome="Rejected",
                            upload_required=False,
                            error_code="DuplicateManifestEntry",
                            error_message="The source item appears more than once in this batch",
                        )
                    else:
                        seen_item_ids.add(entry.source_item_id)
                        try:
                            if isinstance(entry, ManifestDelete):
                                result = self._manifest_delete(
                                    session=session,
                                    account=account,
                                    source=source,
                                    entry=entry,
                                    deletion_allowed=deletion_allowed,
                                    now=now,
                                    occurrence_index=occurrence_index,
                                )
                            else:
                                result = self._manifest_upsert(
                                    session=session,
                                    account=account,
                                    source=source,
                                    entry=entry,
                                    now=now,
                                    occurrence_index=occurrence_index,
                                    asset_index=asset_index,
                                    new_asset_hashes=new_asset_hashes,
                                    deferred_pending_occurrences=(
                                        deferred_pending_occurrences
                                    ),
                                    deferred_hashed_occurrences=(
                                        deferred_hashed_occurrences
                                    ),
                                )
                        except (ConflictError, ValueError) as exc:
                            result = ManifestEntryResult(
                                source_item_id=entry.source_item_id,
                                outcome="Rejected",
                                upload_required=False,
                                error_code=getattr(exc, "code", "InvalidManifestEntry"),
                                error_message=getattr(exc, "detail", str(exc)),
                            )
                    count_key = {
                        "CreatedOccurrence": "created",
                        "UpdatedOccurrence": "updated",
                        "DuplicateLinked": "duplicates_linked",
                        "DeletedOccurrence": "deleted",
                        "IgnoredDeletion": "ignored_deletions",
                        "Unchanged": "unchanged",
                        "Rejected": "rejected",
                    }[result.outcome]
                    counts[count_key] += 1
                    results.append(result)

                # New occurrences are flushed as ORM batches, then their
                # ID-dependent change rows are queued. Metadata ingestion is
                # deliberately enrichment-free; explicit preparation owns
                # all ProcessingJob creation and dispatch.
                session.flush()
                changes = ChangeRepository(session)
                for occurrence in (
                    *deferred_pending_occurrences,
                    *deferred_hashed_occurrences,
                ):
                    changes.add(
                        user_id=account.id,
                        device_id=source.device_id,
                        source_id=source.id,
                        asset_id=occurrence.media_asset_id,
                        occurrence_id=occurrence.id,
                        entity_type="MediaOccurrence",
                        entity_id=occurrence.id,
                        entity_public_id=occurrence.public_id,
                        change_type="Upsert",
                        now=now,
                        flush=False,
                    )
                source.permission_state = command.permission_state
                source.sync_cursor = command.client_cursor
                source.last_manifest_at_utc = now
                source.last_success_at_utc = now
                source.updated_at_utc = now
                session.flush()
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=source.device_id,
                    source_id=source.id,
                    entity_type="MediaSource",
                    entity_id=source.id,
                    entity_public_id=source.public_id,
                    change_type="Upsert",
                    now=now,
                    flush=False,
                )
                session.flush()
                return (
                    ManifestResult(
                        source_id=_uuid(source.public_id),
                        accepted_at_utc=_utc(now) or datetime.now(timezone.utc),
                        source_cursor=source.sync_cursor,
                        counts=ManifestCounts(**counts),
                        results=tuple(results),
                    ),
                    200,
                )

            result = self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._manifest_from_json,
            )
        return result

    async def prepare_enrichment(
        self,
        user_id: UUID,
        requesting_device_id: UUID,
        source_id: UUID,
        command: EnrichmentPrepareCommand,
        context: MutationContext,
    ) -> MutationResult[EnrichmentPreparation]:
        """Explicitly prepare bounded enrichment without replaying metadata.

        Ordinary manifests never create ``ProcessingJob`` rows. This mutation
        is the sole user-facing opt-in boundary for geocoding and scene
        descriptions, and local locators are returned only to the source's
        registered device.
        """

        if not self._enrichment_processing_enabled:
            raise ConflictError(
                "EnrichmentProcessingPaused",
                "Enrichment processing is paused; metadata sync remains available",
            )
        selected_types = tuple(dict.fromkeys(command.types))
        if (
            not selected_types
            or len(selected_types) != len(command.types)
            or any(value not in {"Geocode", "Description"} for value in selected_types)
        ):
            raise ConflictError(
                "InvalidEnrichmentTypes",
                "Choose Geocode, Description, or both exactly once",
            )
        if (
            isinstance(command.limit, bool)
            or not 1 <= command.limit <= MAX_ENRICHMENT_PREPARE
        ):
            raise ConflictError(
                "InvalidEnrichmentLimit",
                f"Enrichment limit must be between 1 and {MAX_ENRICHMENT_PREPARE}",
            )

        geocode_job_ids: list[UUID] = []
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            device = DeviceRepository(session).require(
                user_id=account.id,
                device_public_id=requesting_device_id,
            )
            source = SourceRepository(session).require(
                user_id=account.id,
                source_public_id=source_id,
            )
            if source.device_id != device.id:
                raise ForbiddenError(
                    "SourceDeviceMismatch",
                    "Local enrichment must be requested by the source device",
                )
            if source.storage_mode != "Local":
                raise ConflictError(
                    "LocalEnrichmentRequired",
                    "This enrichment preparation flow currently supports Local sources",
                )

            def action() -> tuple[EnrichmentPreparation, int]:
                occurrence_ids = (
                    select(
                        MediaOccurrence.media_asset_id.label("MediaAssetId"),
                        func.min(MediaOccurrence.id).label("OccurrenceId"),
                    )
                    .where(
                        MediaOccurrence.user_id == account.id,
                        MediaOccurrence.media_source_id == source.id,
                        MediaOccurrence.media_asset_id.is_not(None),
                        MediaOccurrence.deletion_state == "Active",
                        MediaOccurrence.availability_state != "Unavailable",
                    )
                    .group_by(MediaOccurrence.media_asset_id)
                    .subquery()
                )
                current_description = (
                    select(MediaDescription.id)
                    .where(
                        MediaDescription.user_id == account.id,
                        MediaDescription.media_asset_id == MediaAsset.id,
                        MediaDescription.is_current == 1,
                        MediaDescription.status == "Succeeded",
                        func.length(func.trim(MediaDescription.description)) > 0,
                    )
                    .exists()
                )
                normalized_name = func.lower(MediaOccurrence.original_file_name)
                description_job = aliased(ProcessingJob)
                geocode_job = aliased(ProcessingJob)
                supported_photo = and_(
                    MediaAsset.media_type == "Photo",
                    MediaOccurrence.local_locator.is_not(None),
                    or_(
                        *(normalized_name.like(f"%{extension}") for extension in SCENE_PREVIEW_EXTENSIONS)
                    ),
                    ~current_description,
                )
                description_actionable = and_(
                    supported_photo,
                    or_(
                        description_job.id.is_(None),
                        description_job.status == "Preparing",
                    ),
                )
                unresolved_location = and_(
                    MediaLocation.latitude.is_not(None),
                    MediaLocation.longitude.is_not(None),
                    or_(
                        MediaLocation.provider.is_(None),
                        MediaLocation.provider_updated_at_utc.is_(None),
                    ),
                )
                geocode_actionable = and_(
                    unresolved_location,
                    or_(
                        geocode_job.id.is_(None),
                        geocode_job.status == "Succeeded",
                    ),
                )
                eligibility = []
                if "Geocode" in selected_types:
                    eligibility.append(geocode_actionable)
                if "Description" in selected_types:
                    eligibility.append(description_actionable)
                rows = session.execute(
                    select(
                        MediaAsset,
                        MediaOccurrence,
                        MediaLocation,
                        description_job,
                        geocode_job,
                    )
                    .join(
                        occurrence_ids,
                        occurrence_ids.c.MediaAssetId == MediaAsset.id,
                    )
                    .join(
                        MediaOccurrence,
                        MediaOccurrence.id == occurrence_ids.c.OccurrenceId,
                    )
                    .outerjoin(
                        MediaLocation,
                        and_(
                            MediaLocation.user_id == account.id,
                            MediaLocation.media_asset_id == MediaAsset.id,
                        ),
                    )
                    .outerjoin(
                        description_job,
                        and_(
                            description_job.user_id == account.id,
                            description_job.media_asset_id == MediaAsset.id,
                            description_job.job_type == "Description",
                        ),
                    )
                    .outerjoin(
                        geocode_job,
                        and_(
                            geocode_job.user_id == account.id,
                            geocode_job.media_asset_id == MediaAsset.id,
                            geocode_job.job_type == "Geocode",
                            cast(
                                geocode_job.request_json["latitude"].as_string(),
                                Numeric(9, 6),
                            )
                            == MediaLocation.latitude,
                            cast(
                                geocode_job.request_json["longitude"].as_string(),
                                Numeric(10, 6),
                            )
                            == MediaLocation.longitude,
                        ),
                    )
                    .where(
                        MediaAsset.user_id == account.id,
                        MediaAsset.lifecycle_state == "Active",
                        or_(*eligibility),
                    )
                    .order_by(MediaAsset.id, MediaOccurrence.id)
                    .limit(command.limit)
                ).all()

                now = self._now()
                new_jobs: list[ProcessingJob] = []
                changed_jobs: list[ProcessingJob] = []
                task_sources: list[
                    tuple[MediaAsset, MediaOccurrence, ProcessingJob]
                ] = []
                geocode_jobs: list[ProcessingJob] = []
                for (
                    asset,
                    occurrence,
                    location,
                    existing_description_job,
                    existing_geocode_job,
                ) in rows:
                    if (
                        "Geocode" in selected_types
                        and location is not None
                        and location.latitude is not None
                        and location.longitude is not None
                        and (
                            location.provider is None
                            or location.provider_updated_at_utc is None
                        )
                        and (
                            existing_geocode_job is None
                            or existing_geocode_job.status == "Succeeded"
                        )
                    ):
                        revision = _coordinate_revision(
                            location.latitude,
                            location.longitude,
                        )
                        request = {
                                "latitude": f"{location.latitude:.6f}",
                                "longitude": f"{location.longitude:.6f}",
                                "coordinateRevision": revision,
                                "locationPublicId": location.public_id,
                        }
                        selected_geocode_job = existing_geocode_job
                        if selected_geocode_job is None:
                            selected_geocode_job = ProcessingJob(
                                public_id=str(uuid4()),
                                user_id=account.id,
                                media_asset_id=asset.id,
                                media_source_id=source.id,
                                idempotency_key=(
                                    f"geocode:{asset.public_id}:{revision}"
                                ),
                                job_type="Geocode",
                                status="Queued",
                                provider=GEOCODE_PROVIDER,
                                attempt_count=0,
                                max_attempts=5,
                                next_attempt_at_utc=now,
                                request_json=request,
                                created_at_utc=now,
                                updated_at_utc=now,
                            )
                            new_jobs.append(selected_geocode_job)
                        else:
                            selected_geocode_job.status = "Queued"
                            selected_geocode_job.attempt_count = 0
                            selected_geocode_job.next_attempt_at_utc = now
                            selected_geocode_job.lease_token_hash = None
                            selected_geocode_job.lease_expires_at_utc = None
                            selected_geocode_job.failure_class = None
                            selected_geocode_job.failure_code = None
                            selected_geocode_job.failure_message = None
                            selected_geocode_job.started_at_utc = None
                            selected_geocode_job.completed_at_utc = None
                            selected_geocode_job.request_json = request
                            selected_geocode_job.updated_at_utc = now
                        changed_jobs.append(selected_geocode_job)
                        geocode_jobs.append(selected_geocode_job)

                    if (
                        "Description" not in selected_types
                        or occurrence.local_locator is None
                        or not self._scene_preview_supported(
                            asset=asset,
                            file_name=occurrence.original_file_name,
                        )
                    ):
                        continue
                    selected_description_job = existing_description_job
                    if selected_description_job is None:
                        selected_description_job = self._new_description_job(
                            account=account,
                            asset=asset,
                            source=source,
                            now=now,
                        )
                        new_jobs.append(selected_description_job)
                        changed_jobs.append(selected_description_job)
                    else:
                        configured_request = self._description_request(
                            asset=asset,
                            source=source,
                        )
                        if selected_description_job.request_json != configured_request:
                            selected_description_job.request_json = configured_request
                            selected_description_job.updated_at_utc = now
                            changed_jobs.append(selected_description_job)
                    if selected_description_job.status != "Preparing":
                        continue
                    task_sources.append(
                        (asset, occurrence, selected_description_job)
                    )

                session.add_all(new_jobs)
                session.flush()
                changes = ChangeRepository(session)
                for job in changed_jobs:
                    changes.add(
                        user_id=account.id,
                        source_id=source.id,
                        asset_id=job.media_asset_id,
                        entity_type="ProcessingJob",
                        entity_id=job.id,
                        entity_public_id=job.public_id,
                        change_type="Upsert",
                        now=now,
                        flush=False,
                    )
                tasks = tuple(
                        SceneDescriptionTaskRecord(
                            job_id=_uuid(job.public_id),
                            media_asset_id=_uuid(asset.public_id),
                            occurrence_id=_uuid(occurrence.public_id),
                            source_item_id=occurrence.source_item_id,
                            local_locator=str(occurrence.local_locator),
                            asset_content_sha256=asset.content_sha256.lower(),
                            file_name=occurrence.original_file_name,
                        )
                    for asset, occurrence, job in task_sources
                    )
                geocode_job_ids.extend(
                    _uuid(job.public_id) for job in geocode_jobs
                )
                session.flush()
                return (
                    EnrichmentPreparation(
                        source_id=_uuid(source.public_id),
                        geocode_jobs_queued=len(geocode_jobs),
                        description_jobs_prepared=len(tasks),
                        scene_description_tasks=tasks,
                    ),
                    200,
                )

            result = self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._enrichment_preparation_from_json,
            )
        self._dispatch_after_commit(
            job_ids=tuple(geocode_job_ids),
            job_type="Geocode",
        )
        return result

    def _manifest_upsert(
        self,
        *,
        session: Session,
        account: UserAccount,
        source: MediaSource,
        entry: ManifestUpsert,
        now: datetime,
        occurrence_index: dict[str, MediaOccurrence] | None = None,
        asset_index: dict[str, MediaAsset] | None = None,
        new_asset_hashes: set[str] | None = None,
        deferred_pending_occurrences: list[MediaOccurrence] | None = None,
        deferred_hashed_occurrences: list[MediaOccurrence] | None = None,
    ) -> ManifestEntryResult:
        if entry.byte_size <= 0:
            raise ConflictError("InvalidByteSize", "Media byte size must be positive")
        occurrence = (
            occurrence_index.get(entry.source_item_id)
            if occurrence_index is not None
            else OccurrenceRepository(session).by_source_item(
                user_id=account.id,
                source_id=source.id,
                source_item_id=entry.source_item_id,
            )
        )
        occurrence_created = occurrence is None
        old_asset_id = occurrence.media_asset_id if occurrence is not None else None
        was_deleted = occurrence is not None and occurrence.deletion_state != "Active"

        if (
            entry.content_sha256 is None
            and occurrence is None
            and deferred_pending_occurrences is not None
        ):
            occurrence = MediaOccurrence(
                public_id=str(uuid4()),
                user_id=account.id,
                media_source_id=source.id,
                source_item_id=entry.source_item_id,
                original_file_name=entry.file_name,
                local_locator=entry.local_locator,
                source_revision=entry.source_revision,
                observed_byte_size=entry.byte_size,
                hash_status="Pending",
                availability_state="Available",
                deletion_state="Active",
                first_seen_at_utc=now,
                last_seen_at_utc=now,
                created_at_utc=now,
                updated_at_utc=now,
            )
            session.add(occurrence)
            deferred_pending_occurrences.append(occurrence)
            if occurrence_index is not None:
                occurrence_index[entry.source_item_id] = occurrence
            return ManifestEntryResult(
                source_item_id=entry.source_item_id,
                outcome="CreatedOccurrence",
                occurrence_id=_uuid(occurrence.public_id),
                media_asset_id=None,
                upload_required=False,
                description_job_id=None,
            )

        if entry.content_sha256 is None:
            changed = occurrence_created or (
                occurrence is not None
                and (
                    occurrence.source_revision != entry.source_revision
                    or occurrence.original_file_name != entry.file_name
                    or occurrence.local_locator != entry.local_locator
                    or occurrence.observed_byte_size != entry.byte_size
                    or occurrence.deletion_state != "Active"
                )
            )
            if occurrence is None:
                occurrence = MediaOccurrence(
                    user_id=account.id,
                    media_source_id=source.id,
                    source_item_id=entry.source_item_id,
                    original_file_name=entry.file_name,
                    local_locator=entry.local_locator,
                    source_revision=entry.source_revision,
                    observed_byte_size=entry.byte_size,
                    hash_status="Pending",
                    availability_state="Available",
                    deletion_state="Active",
                    first_seen_at_utc=now,
                    last_seen_at_utc=now,
                    created_at_utc=now,
                    updated_at_utc=now,
                )
                session.add(occurrence)
            else:
                revision_changed = occurrence.source_revision != entry.source_revision
                occurrence.original_file_name = entry.file_name
                occurrence.local_locator = entry.local_locator
                occurrence.source_revision = entry.source_revision
                occurrence.observed_byte_size = entry.byte_size
                occurrence.availability_state = "Available"
                occurrence.deletion_state = "Active"
                occurrence.deleted_at_utc = None
                occurrence.last_seen_at_utc = now
                occurrence.updated_at_utc = now
                if revision_changed:
                    occurrence.media_asset_id = None
                    occurrence.hash_status = "Pending"
                    occurrence.hash_failure_code = None
            session.flush()
            if occurrence_index is not None:
                occurrence_index[entry.source_item_id] = occurrence
            if old_asset_id is not None and occurrence.media_asset_id is None:
                self._trash_if_unreferenced(
                    session=session,
                    account=account,
                    asset_id=old_asset_id,
                    excluding_occurrence_id=occurrence.id,
                    now=now,
                    device_id=source.device_id,
                    source_id=source.id,
                )
            linked_asset: MediaAsset | None = None
            if occurrence.media_asset_id is not None:
                linked_asset = session.scalar(
                    select(MediaAsset).where(
                        MediaAsset.user_id == account.id,
                        MediaAsset.id == occurrence.media_asset_id,
                    )
                )
                if linked_asset is not None and linked_asset.lifecycle_state == "Trashed":
                    self._restore_asset(linked_asset, source=source, now=now)
                    ChangeRepository(session).add(
                        user_id=account.id,
                        device_id=source.device_id,
                        source_id=source.id,
                        asset_id=linked_asset.id,
                        entity_type="MediaAsset",
                        entity_id=linked_asset.id,
                        entity_public_id=linked_asset.public_id,
                        change_type="Upsert",
                        now=now,
                    )
            if changed:
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=source.device_id,
                    source_id=source.id,
                    occurrence_id=occurrence.id,
                    entity_type="MediaOccurrence",
                    entity_id=occurrence.id,
                    entity_public_id=occurrence.public_id,
                    change_type="Upsert",
                    now=now,
                )
            return ManifestEntryResult(
                source_item_id=entry.source_item_id,
                outcome="CreatedOccurrence" if occurrence_created else (
                    "UpdatedOccurrence" if changed else "Unchanged"
                ),
                occurrence_id=_uuid(occurrence.public_id),
                media_asset_id=(
                    _uuid(linked_asset.public_id) if linked_asset is not None else None
                ),
                upload_required=(
                    linked_asset is not None
                    and source.storage_mode == "Remote"
                    and linked_asset.storage_state != "RemoteAvailable"
                ),
                description_job_id=None,
            )

        content_hash = entry.content_sha256.lower()
        if not HEX_SHA256.fullmatch(content_hash):
            raise ConflictError(
                "InvalidContentHash", "Content SHA-256 must contain 64 hexadecimal characters"
            )
        asset = (
            asset_index.get(content_hash)
            if asset_index is not None
            else AssetRepository(session).by_hash(
                user_id=account.id, sha256=content_hash
            )
        )
        asset_created = (
            new_asset_hashes is not None and content_hash in new_asset_hashes
        ) or asset is None
        if asset is not None and asset.byte_size != entry.byte_size:
            raise ConflictError(
                "ContentHashMetadataMismatch",
                "The content hash is already associated with a different byte size",
            )
        if asset is None:
            asset = self._new_manifest_asset(
                account_id=account.id,
                source=source,
                entry=entry,
                content_hash=content_hash,
                now=now,
            )
            session.add(asset)
            session.flush()
            if asset_index is not None:
                asset_index[content_hash] = asset
        if asset_created:
            if new_asset_hashes is not None:
                new_asset_hashes.discard(content_hash)
            ChangeRepository(session).add(
                user_id=account.id,
                device_id=source.device_id,
                source_id=source.id,
                asset_id=asset.id,
                entity_type="MediaAsset",
                entity_id=asset.id,
                entity_public_id=asset.public_id,
                change_type="Upsert",
                now=now,
                flush=deferred_hashed_occurrences is None,
            )
        else:
            asset_enriched = self._merge_asset_metadata(asset, entry=entry, now=now)
            if source.storage_mode == "Remote" and asset.storage_state == "LocalOnly":
                asset.storage_state = "UploadPending"
                asset.updated_at_utc = now
                asset_enriched = True
            if asset_enriched:
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=source.device_id,
                    source_id=source.id,
                    asset_id=asset.id,
                    entity_type="MediaAsset",
                    entity_id=asset.id,
                    entity_public_id=asset.public_id,
                    change_type="Upsert",
                    now=now,
                    flush=deferred_hashed_occurrences is None,
                )

        if asset.lifecycle_state == "Trashed":
            self._restore_asset(asset, source=source, now=now)
            ChangeRepository(session).add(
                user_id=account.id,
                device_id=source.device_id,
                source_id=source.id,
                asset_id=asset.id,
                entity_type="MediaAsset",
                entity_id=asset.id,
                entity_public_id=asset.public_id,
                change_type="Upsert",
                now=now,
                flush=deferred_hashed_occurrences is None,
            )

        existing_unchanged = (
            occurrence is not None
            and occurrence.media_asset_id == asset.id
            and occurrence.source_revision == entry.source_revision
            and occurrence.original_file_name == entry.file_name
            and occurrence.local_locator == entry.local_locator
            and occurrence.observed_byte_size == entry.byte_size
            and occurrence.deletion_state == "Active"
            and occurrence.hash_status == "Complete"
        )
        if occurrence is None:
            occurrence = MediaOccurrence(
                public_id=str(uuid4()),
                user_id=account.id,
                media_source_id=source.id,
                media_asset_id=asset.id,
                source_item_id=entry.source_item_id,
                original_file_name=entry.file_name,
                local_locator=entry.local_locator,
                source_revision=entry.source_revision,
                observed_byte_size=entry.byte_size,
                hash_status="Complete",
                availability_state="Available",
                deletion_state="Active",
                first_seen_at_utc=now,
                last_seen_at_utc=now,
                created_at_utc=now,
                updated_at_utc=now,
            )
            session.add(occurrence)
        else:
            occurrence.media_asset_id = asset.id
            occurrence.original_file_name = entry.file_name
            occurrence.local_locator = entry.local_locator
            occurrence.source_revision = entry.source_revision
            occurrence.observed_byte_size = entry.byte_size
            occurrence.hash_status = "Complete"
            occurrence.hash_failure_code = None
            occurrence.availability_state = "Available"
            occurrence.deletion_state = "Active"
            occurrence.deleted_at_utc = None
            occurrence.last_seen_at_utc = now
            occurrence.updated_at_utc = now
        if occurrence_created and deferred_hashed_occurrences is not None:
            deferred_hashed_occurrences.append(occurrence)
        elif old_asset_id is not None and old_asset_id != asset.id:
            # The orphan check below must observe the occurrence's new asset.
            session.flush()
        elif deferred_hashed_occurrences is None:
            session.flush()
        if occurrence_index is not None:
            occurrence_index[entry.source_item_id] = occurrence

        if old_asset_id is not None and old_asset_id != asset.id:
            self._trash_if_unreferenced(
                session=session,
                account=account,
                asset_id=old_asset_id,
                excluding_occurrence_id=occurrence.id,
                now=now,
                device_id=source.device_id,
                source_id=source.id,
            )
        if entry.location is not None:
            self._upsert_location(
                session=session,
                account=account,
                asset=asset,
                entry=entry,
                now=now,
            )
        if not existing_unchanged:
            if not occurrence_created or deferred_hashed_occurrences is None:
                ChangeRepository(session).add(
                    user_id=account.id,
                    device_id=source.device_id,
                    source_id=source.id,
                    asset_id=asset.id,
                    occurrence_id=occurrence.id,
                    entity_type="MediaOccurrence",
                    entity_id=occurrence.id,
                    entity_public_id=occurrence.public_id,
                    change_type="Upsert",
                    now=now,
                    flush=deferred_hashed_occurrences is None,
                )
        outcome = (
            "Unchanged"
            if existing_unchanged
            else "DuplicateLinked"
            if occurrence_created and not asset_created
            else "CreatedOccurrence"
            if occurrence_created
            else "UpdatedOccurrence"
        )
        return ManifestEntryResult(
            source_item_id=entry.source_item_id,
            outcome=outcome,
            occurrence_id=_uuid(occurrence.public_id),
            media_asset_id=_uuid(asset.public_id),
            upload_required=(
                source.storage_mode == "Remote"
                and asset.storage_state != "RemoteAvailable"
            ),
            description_job_id=None,
        )

    def _manifest_delete(
        self,
        *,
        session: Session,
        account: UserAccount,
        source: MediaSource,
        entry: ManifestDelete,
        deletion_allowed: bool,
        now: datetime,
        occurrence_index: dict[str, MediaOccurrence] | None = None,
    ) -> ManifestEntryResult:
        occurrence = (
            occurrence_index.get(entry.source_item_id)
            if occurrence_index is not None
            else OccurrenceRepository(session).by_source_item(
                user_id=account.id,
                source_id=source.id,
                source_item_id=entry.source_item_id,
            )
        )
        if not deletion_allowed or occurrence is None or occurrence.deletion_state != "Active":
            return ManifestEntryResult(
                source_item_id=entry.source_item_id,
                outcome="IgnoredDeletion",
                occurrence_id=(
                    _uuid(occurrence.public_id) if occurrence is not None else None
                ),
                media_asset_id=(
                    self._asset_public_id(session, account.id, occurrence.media_asset_id)
                    if occurrence is not None and occurrence.media_asset_id is not None
                    else None
                ),
                upload_required=False,
            )
        occurrence.source_revision = entry.source_revision
        asset_public_id = (
            self._asset_public_id(session, account.id, occurrence.media_asset_id)
            if occurrence.media_asset_id is not None
            else None
        )
        self._delete_occurrence(
            session=session,
            account=account,
            source=source,
            occurrence=occurrence,
            now=now,
        )
        return ManifestEntryResult(
            source_item_id=entry.source_item_id,
            outcome="DeletedOccurrence",
            occurrence_id=_uuid(occurrence.public_id),
            media_asset_id=asset_public_id,
            upload_required=False,
        )

    def _capture_provenance(
        self, entry: ManifestUpsert
    ) -> tuple[str | None, Decimal | None]:
        for provenance in entry.provenance:
            if provenance.field in {
                "capturedAtLocal",
                "capturedAtUtc",
                "CaptureDateTimeLocal",
                "CaptureDateTimeUtc",
            }:
                return provenance.source, provenance.confidence
        return ("Unknown" if entry.captured_at_local or entry.captured_at_utc else None, None)

    def _prepare_manifest_assets(
        self,
        *,
        session: Session,
        source: MediaSource,
        entries: Sequence[ManifestUpsert | ManifestDelete],
        asset_index: dict[str, MediaAsset],
        now: datetime,
    ) -> set[str]:
        """Create new hash-addressed assets with one batch-oriented flush."""

        prepared_hashes: set[str] = set()
        seen_item_ids: set[str] = set()
        for entry in entries:
            if entry.source_item_id in seen_item_ids:
                continue
            seen_item_ids.add(entry.source_item_id)
            if (
                not isinstance(entry, ManifestUpsert)
                or entry.byte_size <= 0
                or not isinstance(entry.content_sha256, str)
            ):
                continue
            content_hash = entry.content_sha256.lower()
            if not HEX_SHA256.fullmatch(content_hash) or content_hash in asset_index:
                continue
            asset = self._new_manifest_asset(
                account_id=source.user_id,
                source=source,
                entry=entry,
                content_hash=content_hash,
                now=now,
            )
            session.add(asset)
            asset_index[content_hash] = asset
            prepared_hashes.add(content_hash)
        if prepared_hashes:
            session.flush()
        return prepared_hashes

    def _new_manifest_asset(
        self,
        *,
        account_id: int,
        source: MediaSource,
        entry: ManifestUpsert,
        content_hash: str,
        now: datetime,
    ) -> MediaAsset:
        capture_source, capture_confidence = self._capture_provenance(entry)
        return MediaAsset(
            public_id=str(uuid4()),
            user_id=account_id,
            content_sha256=content_hash,
            content_hash_source="ClientDeclared",
            media_type=entry.media_type,
            mime_type=entry.mime_type,
            byte_size=entry.byte_size,
            width_pixels=entry.width_pixels,
            height_pixels=entry.height_pixels,
            duration_milliseconds=entry.duration_ms,
            capture_datetime_local=_db_datetime(entry.captured_at_local),
            capture_datetime_utc=_db_datetime(entry.captured_at_utc),
            time_zone=entry.time_zone_id,
            utc_offset_minutes=entry.utc_offset_minutes,
            capture_time_source=capture_source,
            capture_time_confidence=capture_confidence,
            metadata_json={
                "provenance": [item.as_json() for item in entry.provenance]
            },
            metadata_version="ManifestV1",
            storage_state=(
                "UploadPending" if source.storage_mode == "Remote" else "LocalOnly"
            ),
            lifecycle_state="Active",
            created_at_utc=now,
            updated_at_utc=now,
        )

    def _merge_asset_metadata(
        self, asset: MediaAsset, *, entry: ManifestUpsert, now: datetime
    ) -> bool:
        """Fill canonical gaps from another occurrence without replacing evidence."""

        changed = False
        for attribute, incoming in (
            ("width_pixels", entry.width_pixels),
            ("height_pixels", entry.height_pixels),
            ("duration_milliseconds", entry.duration_ms),
            ("capture_datetime_local", _db_datetime(entry.captured_at_local)),
            ("capture_datetime_utc", _db_datetime(entry.captured_at_utc)),
            ("time_zone", entry.time_zone_id),
            ("utc_offset_minutes", entry.utc_offset_minutes),
        ):
            if getattr(asset, attribute) is None and incoming is not None:
                setattr(asset, attribute, incoming)
                changed = True
        if asset.capture_time_source is None and (
            entry.captured_at_local is not None or entry.captured_at_utc is not None
        ):
            source, confidence = self._capture_provenance(entry)
            asset.capture_time_source = source
            asset.capture_time_confidence = confidence
            changed = True
        existing = asset.metadata_json if isinstance(asset.metadata_json, dict) else {}
        provenance = existing.get("provenance")
        if not isinstance(provenance, list):
            provenance = []
        known = {
            (str(item.get("field")), str(item.get("source")))
            for item in provenance
            if isinstance(item, dict)
        }
        for item in entry.provenance:
            if (item.field, item.source) not in known:
                provenance.append(item.as_json())
                known.add((item.field, item.source))
                changed = True
        if changed:
            asset.metadata_json = {**existing, "provenance": provenance}
            asset.metadata_version = "ManifestV1"
            asset.updated_at_utc = now
        return changed

    @staticmethod
    def _restore_asset(asset: MediaAsset, *, source: MediaSource, now: datetime) -> None:
        asset.lifecycle_state = "Active"
        asset.trashed_at_utc = None
        asset.purge_after_utc = None
        asset.storage_state = (
            "RemoteAvailable"
            if asset.original_s3_object_key
            else "UploadPending"
            if source.storage_mode == "Remote"
            else "LocalOnly"
        )
        asset.updated_at_utc = now

    def _upsert_location(
        self,
        *,
        session: Session,
        account: UserAccount,
        asset: MediaAsset,
        entry: ManifestUpsert,
        now: datetime,
    ) -> None:
        assert entry.location is not None
        latitude = entry.location.latitude
        longitude = entry.location.longitude
        if not latitude.is_finite() or not Decimal("-90") <= latitude <= Decimal("90"):
            raise ConflictError("InvalidLatitude", "Latitude must be between -90 and 90")
        if not longitude.is_finite() or not Decimal("-180") <= longitude <= Decimal("180"):
            raise ConflictError(
                "InvalidLongitude", "Longitude must be between -180 and 180"
            )

        repository = LocationRepository(session)
        location = repository.by_asset(user_id=account.id, asset_id=asset.id)
        reusable = repository.resolved_nearby(
            user_id=account.id,
            latitude=latitude,
            longitude=longitude,
            radius_meters=self._geocode_reuse_radius_meters,
        )
        if reusable is not None:
            reusable_result = self._reverse_geocode_result(reusable)
            if (
                reusable_result.resolution is not None
                and not self._location_normalizer.can_reuse(
                    reusable_result.resolution
                )
            ):
                reusable = None
        location_source = next(
            (
                item.source
                for item in entry.provenance
                if item.field.lower() in {"location", "gps", "latitude", "longitude"}
            ),
            "Unknown",
        )
        if location is None:
            location = MediaLocation(
                user_id=account.id,
                media_asset_id=asset.id,
                created_at_utc=now,
                updated_at_utc=now,
            )
            session.add(location)
        location.latitude = latitude
        location.longitude = longitude
        location.altitude_meters = entry.location.altitude_meters
        location.accuracy_meters = entry.location.horizontal_accuracy_meters
        location.location_source = location_source
        location.updated_at_utc = now
        if reusable is not None:
            normalized = self._location_normalizer.normalize_result(
                self._reverse_geocode_result(reusable)
            )
            self._apply_geocode_result(location, result=normalized, now=now)
            self._apply_geocode_timezone(
                session,
                user_id=account.id,
                asset_id=asset.id,
                result=normalized,
                now=now,
            )
        else:
            self._clear_resolved_location(location)
        session.flush()

    @staticmethod
    def _copy_resolved_location(source: MediaLocation, target: MediaLocation) -> None:
        for attribute in (
            "location_display_name",
            "street_address",
            "original_street_number",
            "neighborhood",
            "city",
            "county",
            "state",
            "postal_code",
            "country",
            "country_code",
            "provider",
            "provider_place_id",
            "normalization_rule_version",
            "confidence",
            "provider_updated_at_utc",
        ):
            setattr(target, attribute, getattr(source, attribute))
        target.raw_provider_json = deepcopy(source.raw_provider_json)

    @staticmethod
    def _clear_resolved_location(location: MediaLocation) -> None:
        for attribute in (
            "location_display_name",
            "street_address",
            "original_street_number",
            "neighborhood",
            "city",
            "county",
            "state",
            "postal_code",
            "country",
            "country_code",
            "provider",
            "provider_place_id",
            "normalization_rule_version",
            "confidence",
            "raw_provider_json",
            "provider_updated_at_utc",
        ):
            setattr(location, attribute, None)

    @staticmethod
    def _scene_preview_supported(*, asset: MediaAsset, file_name: str) -> bool:
        normalized_name = file_name.casefold()
        return (
            asset.media_type == "Photo"
            and not normalized_name.endswith(RAW_PHOTO_EXTENSIONS)
            and normalized_name.endswith(SCENE_PREVIEW_EXTENSIONS)
        )

    def _description_request(
        self,
        *,
        asset: MediaAsset,
        source: MediaSource,
    ) -> dict[str, object]:
        return {
            "assetRevision": asset.content_sha256.lower(),
            "sourceId": source.public_id,
            "model": self._scene_description_model,
            "promptVersion": SCENE_DESCRIPTION_PROMPT_VERSION,
            "detail": self._scene_description_detail,
            "serviceTier": self._scene_description_service_tier,
            "maxWords": self._scene_description_max_words,
            "monthlyCallLimit": self._scene_description_monthly_call_limit,
            "monthlyUsdLimit": str(self._scene_description_monthly_usd_limit),
            "reservedUsdPerRequest": str(
                self._scene_description_reserved_usd_per_request
            ),
            "inputUsdPerMillion": str(
                self._scene_description_input_usd_per_million
            ),
            "cachedInputUsdPerMillion": str(
                self._scene_description_cached_input_usd_per_million
            ),
            "outputUsdPerMillion": str(
                self._scene_description_output_usd_per_million
            ),
        }

    def _new_description_job(
        self,
        *,
        account: UserAccount,
        asset: MediaAsset,
        source: MediaSource,
        now: datetime,
    ) -> ProcessingJob:
        return ProcessingJob(
            public_id=str(uuid4()),
            user_id=account.id,
            media_asset_id=asset.id,
            media_source_id=source.id,
            idempotency_key=f"description:{asset.public_id}",
            job_type="Description",
            status="Preparing",
            provider=DESCRIPTION_PROVIDER,
            attempt_count=0,
            max_attempts=5,
            next_attempt_at_utc=None,
            request_json=self._description_request(asset=asset, source=source),
            created_at_utc=now,
            updated_at_utc=now,
        )

    def _require_temporary_object_store(self) -> TemporaryObjectStore:
        if self._temporary_object_store is None:
            raise ConflictError(
                "TemporaryProcessingUnavailable",
                "Temporary scene processing is not configured",
            )
        return self._temporary_object_store

    def _validate_temporary_upload_command(
        self, command: UploadPlanCommand, *, asset: MediaAsset
    ) -> None:
        asset_hash = command.asset_content_sha256.lower()
        object_hash = command.object_sha256.lower()
        if not HEX_SHA256.fullmatch(asset_hash) or asset_hash != asset.content_sha256.lower():
            raise ConflictError(
                "AssetHashMismatch", "The asset content hash does not match this photo"
            )
        if not HEX_SHA256.fullmatch(object_hash):
            raise ConflictError(
                "InvalidObjectHash",
                "Object SHA-256 must contain 64 hexadecimal characters",
            )
        if command.media_type != "Photo" or asset.media_type != "Photo":
            raise ConflictError(
                "ScenePreviewRequiresPhoto",
                "Scene descriptions currently accept photos only",
            )
        if self._normalized_mime(command.object_mime_type) != SCENE_PREVIEW_MIME_TYPE:
            raise ConflictError(
                "ScenePreviewRequiresJpeg",
                "The temporary scene preview must be a JPEG image",
            )
        if (
            command.object_byte_size <= 0
            or command.object_byte_size > SCENE_PREVIEW_MAX_BYTES
        ):
            raise ConflictError(
                "ScenePreviewSizeInvalid",
                "The temporary scene preview has an invalid byte size",
            )
        if not command.file_name or len(command.file_name) > 512:
            raise ConflictError("InvalidFileName", "The preview file name is invalid")
        if asset.lifecycle_state != "Active":
            raise ConflictError("MediaNotActive", "The media asset is not active")
        self._assert_local_asset_has_no_remote_locator(asset)

    @staticmethod
    def _validate_prepared_upload(
        prepared: Any,
        *,
        checksum_base64: str,
        byte_size: int,
        now: datetime,
        session_expires_at: datetime,
    ) -> None:
        if not prepared.bucket or len(prepared.bucket) > 63:
            raise ConflictError(
                "InvalidStagingTarget", "The temporary object target is invalid"
            )
        if not prepared.object_key or len(prepared.object_key) > 1024:
            raise ConflictError(
                "InvalidStagingTarget", "The temporary object target is invalid"
            )
        parsed = urlsplit(prepared.url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username is not None:
            raise ConflictError(
                "InvalidStagingTarget", "The temporary upload URL is invalid"
            )
        headers = {str(key).lower(): str(value) for key, value in prepared.headers.items()}
        if (
            headers.get("x-amz-checksum-sha256") != checksum_base64
            or headers.get("content-type", "").casefold() != SCENE_PREVIEW_MIME_TYPE
            or headers.get("content-length") != str(byte_size)
        ):
            raise ConflictError(
                "UnboundStagingUpload",
                "The temporary upload is not bound to its exact metadata",
            )
        expires = _db_datetime(prepared.expires_at_utc)
        if expires is None or expires <= now or expires > session_expires_at:
            raise ConflictError(
                "InvalidStagingExpiry", "The temporary upload expiry is invalid"
            )

    @staticmethod
    def _normalized_mime(value: str) -> str:
        return value.split(";", 1)[0].strip().casefold()

    @staticmethod
    def _assert_local_asset_has_no_remote_locator(asset: MediaAsset) -> None:
        remote_values = (
            asset.s3_bucket,
            asset.original_s3_object_key,
            asset.original_s3_version_id,
            asset.original_s3_etag,
            asset.original_s3_checksum_algorithm,
            asset.original_s3_checksum_type,
            asset.original_s3_checksum_value,
            asset.preview_s3_object_key,
            asset.preview_s3_checksum_algorithm,
            asset.preview_s3_checksum_type,
            asset.preview_s3_checksum_value,
        )
        if asset.storage_state != "LocalOnly" or any(
            value is not None for value in remote_values
        ):
            raise ConflictError(
                "LocalAssetRemoteLocatorUnexpected",
                "Local media cannot use a durable remote object locator",
            )

    @staticmethod
    def _require_temporary_single_part(upload: UploadSession) -> None:
        if (
            upload.object_purpose != "TemporaryProcessing"
            or upload.upload_kind != "SinglePart"
            or upload.s3_upload_id is not None
            or upload.part_size_bytes is not None
        ):
            raise ConflictError(
                "UploadSessionNotSupported",
                "Only single-part temporary scene uploads are available",
            )

    @staticmethod
    def _job_for_upload_session(
        *, session: Session, account: UserAccount, upload: UploadSession
    ) -> ProcessingJob:
        metadata = upload.parts_json if isinstance(upload.parts_json, dict) else {}
        job_public_id = metadata.get("processingJobId")
        if not isinstance(job_public_id, str):
            raise ConflictError(
                "UploadSessionInvalid", "The upload session has no processing job"
            )
        job = JobRepository(session).require(
            user_id=account.id, job_public_id=job_public_id
        )
        if job.job_type != "Description" or job.media_asset_id != upload.media_asset_id:
            raise ConflictError(
                "UploadSessionInvalid",
                "The upload session processing job does not match its photo",
            )
        return job

    def _reserve_description_provider_request(
        self,
        *,
        session: Session,
        account: UserAccount,
        job: ProcessingJob,
        now: datetime,
    ) -> bool:
        return self._reserve_description_usage(
            session=session,
            user_id=account.id,
            job=job,
            provider=DESCRIPTION_PROVIDER,
            monthly_request_limit=self._scene_description_monthly_call_limit,
            monthly_usd_limit=self._scene_description_monthly_usd_limit,
            reserved_usd=self._scene_description_reserved_usd_per_request,
            now=now,
        )

    def _reserve_description_usage(
        self,
        *,
        session: Session,
        user_id: int,
        job: ProcessingJob,
        provider: str,
        monthly_request_limit: int,
        monthly_usd_limit: Decimal,
        reserved_usd: Decimal,
        now: datetime,
    ) -> bool:
        """Atomically reserve both the provider request and conservative USD."""

        request = dict(job.request_json) if isinstance(job.request_json, dict) else {}
        existing_request = request.get("providerUsageReservation")
        existing_cost = request.get("providerCostReservation")
        usage_month = date(now.year, now.month, 1)
        try:
            existing_cost_units = Decimal(
                str(
                    existing_cost.get("units", "-1")
                    if isinstance(existing_cost, dict)
                    else "-1"
                )
            )
        except (ValueError, InvalidOperation):
            existing_cost_units = Decimal("-1")
        request_matches = (
            isinstance(existing_request, dict)
            and existing_request.get("state") == "Reserved"
            and existing_request.get("provider") == provider
            and existing_request.get("usageMonth") == usage_month.isoformat()
            and existing_request.get("unitType") == "Request"
            and existing_request.get("units") == "1"
        )
        cost_matches = (
            isinstance(existing_cost, dict)
            and existing_cost.get("state") == "Reserved"
            and existing_cost.get("provider") == provider
            and existing_cost.get("usageMonth") == usage_month.isoformat()
            and existing_cost.get("unitType") == "Usd"
            and existing_cost_units == reserved_usd
        )
        if request_matches and cost_matches:
            request_usage = ProviderUsageRepository(session).get(
                user_id=user_id,
                provider=provider,
                usage_month=usage_month,
                unit_type="Request",
                for_update=True,
            )
            cost_usage = ProviderUsageRepository(session).get(
                user_id=user_id,
                provider=provider,
                usage_month=usage_month,
                unit_type="Usd",
                for_update=True,
            )
            if request_usage is not None and cost_usage is not None:
                if request_usage.circuit_state == "Open":
                    request["quotaBlockReason"] = "CircuitOpen"
                    job.request_json = request
                    return False
                request.pop("quotaBlockReason", None)
                job.request_json = request
                return True
        if request_matches or cost_matches:
            self._release_description_provider_request(
                session=session, job=job, now=now
            )
            request = dict(job.request_json or {})

        request_limit = Decimal(monthly_request_limit)
        request_usage = ProviderUsageRepository(session).get_or_create(
            user_id=user_id,
            provider=provider,
            usage_month=usage_month,
            unit_type="Request",
            hard_limit_units=request_limit,
            now=now,
        )
        cost_usage = ProviderUsageRepository(session).get_or_create(
            user_id=user_id,
            provider=provider,
            usage_month=usage_month,
            unit_type="Usd",
            hard_limit_units=monthly_usd_limit,
            now=now,
        )
        request_usage.hard_limit_units = request_limit
        cost_usage.hard_limit_units = monthly_usd_limit
        if request_usage.circuit_state == "Open":
            request["quotaBlockReason"] = "CircuitOpen"
            job.request_json = request
            request_usage.updated_at_utc = now
            cost_usage.updated_at_utc = now
            session.flush()
            return False
        request.pop("quotaBlockReason", None)
        if (
            monthly_request_limit == 0
            or request_usage.processed_units
            + request_usage.reserved_units
            + Decimal("1")
            > request_limit
            or monthly_usd_limit == 0
            or cost_usage.processed_units + cost_usage.reserved_units + reserved_usd
            > monthly_usd_limit
        ):
            request_usage.updated_at_utc = now
            cost_usage.updated_at_utc = now
            session.flush()
            return False
        request_usage.reserved_units += Decimal("1")
        request_usage.updated_at_utc = now
        cost_usage.reserved_units += reserved_usd
        cost_usage.updated_at_utc = now
        request["providerUsageReservation"] = {
            "provider": provider,
            "usageMonth": usage_month.isoformat(),
            "unitType": "Request",
            "units": "1",
            "state": "Reserved",
        }
        request["providerCostReservation"] = {
            "provider": provider,
            "usageMonth": usage_month.isoformat(),
            "unitType": "Usd",
            "units": str(reserved_usd.quantize(USD_QUANTUM)),
            "state": "Reserved",
        }
        job.request_json = request
        job.updated_at_utc = now
        session.flush()
        return True

    @staticmethod
    def _release_description_provider_request(
        *, session: Session, job: ProcessingJob, now: datetime
    ) -> None:
        request = dict(job.request_json) if isinstance(job.request_json, dict) else {}
        changed = False
        for key, default_unit in (
            ("providerUsageReservation", "Request"),
            ("providerCostReservation", "Usd"),
        ):
            reservation = request.get(key)
            if (
                not isinstance(reservation, dict)
                or reservation.get("state") != "Reserved"
            ):
                continue
            try:
                usage_month = date.fromisoformat(str(reservation["usageMonth"]))
                units = Decimal(str(reservation.get("units", "1")))
            except (KeyError, ValueError, InvalidOperation):
                continue
            usage = ProviderUsageRepository(session).get(
                user_id=job.user_id,
                provider=str(reservation.get("provider") or DESCRIPTION_PROVIDER),
                usage_month=usage_month,
                unit_type=str(reservation.get("unitType") or default_unit),
                for_update=True,
            )
            if usage is not None:
                usage.reserved_units -= min(
                    usage.reserved_units, max(Decimal("0"), units)
                )
                usage.updated_at_utc = now
            released = dict(reservation)
            released["state"] = "Released"
            released["releasedAtUtc"] = (_utc(now) or now).isoformat()
            request[key] = released
            changed = True
        if not changed:
            return
        job.request_json = request
        job.updated_at_utc = now

    def _expire_temporary_uploads(
        self,
        *,
        session: Session,
        account: UserAccount,
        repository: UploadRepository,
        store: TemporaryObjectStore,
        now: datetime,
    ) -> None:
        for upload in repository.expired_active_for_user(
            user_id=account.id, now=now
        ):
            self._require_temporary_single_part(upload)
            store.delete_object(
                bucket=upload.s3_bucket, object_key=upload.s3_object_key
            )
            job = self._job_for_upload_session(
                session=session, account=account, upload=upload
            )
            if job.status == "Preparing":
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
            upload.status = "Expired"
            upload.active_lease_marker = None
            upload.failure_code = "UploadLeaseExpired"
            upload.updated_at_utc = now
        session.flush()

    @staticmethod
    def _first_of_next_month(now: datetime) -> datetime:
        year = now.year + 1 if now.month == 12 else now.year
        month = 1 if now.month == 12 else now.month + 1
        return datetime(year, month, 1)

    @staticmethod
    def _upload_session_record(upload: UploadSession) -> UploadSessionRecord:
        return UploadSessionRecord(
            upload_session_id=_uuid(upload.public_id),
            strategy="SinglePart",
            status=upload.status,
            expected_byte_size=upload.expected_byte_size,
            uploaded_byte_size=upload.uploaded_byte_size,
            expires_at_utc=_utc(upload.expires_at_utc),
        )

    def _delete_occurrence(
        self,
        *,
        session: Session,
        account: UserAccount,
        source: MediaSource,
        occurrence: MediaOccurrence,
        now: datetime,
    ) -> None:
        asset_id = occurrence.media_asset_id
        occurrence.deletion_state = "Deleted"
        occurrence.availability_state = "Unavailable"
        occurrence.deleted_at_utc = now
        occurrence.last_seen_at_utc = now
        occurrence.updated_at_utc = now
        ChangeRepository(session).add(
            user_id=account.id,
            device_id=source.device_id,
            source_id=source.id,
            asset_id=asset_id,
            occurrence_id=occurrence.id,
            entity_type="MediaOccurrence",
            entity_id=occurrence.id,
            entity_public_id=occurrence.public_id,
            change_type="Delete",
            now=now,
        )
        if asset_id is not None:
            self._trash_if_unreferenced(
                session=session,
                account=account,
                asset_id=asset_id,
                excluding_occurrence_id=occurrence.id,
                now=now,
                device_id=source.device_id,
                source_id=source.id,
            )

    def _trash_if_unreferenced(
        self,
        *,
        session: Session,
        account: UserAccount,
        asset_id: int,
        excluding_occurrence_id: int | None,
        now: datetime,
        device_id: int | None = None,
        source_id: int | None = None,
    ) -> None:
        if OccurrenceRepository(session).active_count_for_asset(
            user_id=account.id,
            asset_id=asset_id,
            excluding_id=excluding_occurrence_id,
        ):
            return
        asset = session.scalar(
            select(MediaAsset).where(
                MediaAsset.user_id == account.id,
                MediaAsset.id == asset_id,
            )
        )
        if asset is None or asset.lifecycle_state == "Trashed":
            return
        asset.lifecycle_state = "Trashed"
        asset.storage_state = "Trashed"
        asset.trashed_at_utc = now
        asset.purge_after_utc = now + self._trash_retention
        asset.updated_at_utc = now
        self._cancel_asset_processing(
            session=session,
            account=account,
            asset=asset,
            now=now,
        )
        ChangeRepository(session).add(
            user_id=account.id,
            device_id=device_id,
            source_id=source_id,
            asset_id=asset.id,
            entity_type="MediaAsset",
            entity_id=asset.id,
            entity_public_id=asset.public_id,
            change_type="Delete",
            now=now,
        )

    def _cancel_asset_processing(
        self,
        *,
        session: Session,
        account: UserAccount,
        asset: MediaAsset,
        now: datetime,
    ) -> None:
        jobs = list(
            session.scalars(
                select(ProcessingJob)
                .where(
                    ProcessingJob.user_id == account.id,
                    ProcessingJob.media_asset_id == asset.id,
                    ProcessingJob.status.in_(
                        ("Preparing", "Queued", "Running", "DeferredQuota")
                    ),
                )
                .with_for_update()
            )
        )
        for job in jobs:
            description_provider_called = (
                job.job_type == "Description"
                and self._description_provider_call_started(job)
            )
            self._settle_provider_reservation(
                session,
                job=job,
                now=now,
                consumed=(
                    description_provider_called
                    if job.job_type == "Description"
                    else job.status == "Running"
                ),
            )
            if job.job_type == "Description":
                self._settle_description_cost_reservation(
                    session,
                    job=job,
                    now=now,
                    provider_called=description_provider_called,
                )
            job.status = "Cancelled"
            job.next_attempt_at_utc = None
            job.lease_token_hash = None
            job.lease_expires_at_utc = None
            job.failure_class = "InvalidMedia"
            job.failure_code = "MediaTrashed"
            job.failure_message = "Processing stopped because the media was removed."
            job.completed_at_utc = now
            job.updated_at_utc = now
            self._add_job_change(session, job=job, now=now)

        uploads = list(
            session.scalars(
                select(UploadSession)
                .where(
                    UploadSession.user_id == account.id,
                    UploadSession.media_asset_id == asset.id,
                    UploadSession.object_purpose == "TemporaryProcessing",
                    UploadSession.status.in_(
                        ("Preparing", "Uploading", "Completing", "Completed")
                    ),
                )
                .with_for_update()
            )
        )
        for upload in uploads:
            if self._temporary_object_store is not None:
                try:
                    self._temporary_object_store.delete_object(
                        bucket=upload.s3_bucket,
                        object_key=upload.s3_object_key,
                    )
                except Exception as exc:
                    logger.warning(
                        "Deferred trashed preview cleanup uploadId=%s errorType=%s",
                        upload.public_id,
                        type(exc).__name__,
                    )
            upload.status = "Cancelled"
            upload.active_lease_marker = None
            upload.failure_code = "MediaTrashed"
            upload.updated_at_utc = now

    @staticmethod
    def _asset_public_id(
        session: Session, user_id: int, asset_id: int | None
    ) -> UUID | None:
        if asset_id is None:
            return None
        value = session.scalar(
            select(MediaAsset.public_id).where(
                MediaAsset.user_id == user_id,
                MediaAsset.id == asset_id,
            )
        )
        return _uuid(value) if value is not None else None

    async def list_changes(
        self,
        user_id: UUID,
        device_id: UUID,
        cursor: str | None = None,
        limit: int = 50,
    ) -> Page[ChangeRecord]:
        _require_page_limit(limit)
        after_id = _decode_single_id(self._cursor, cursor, kind="changes") or 0
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            device = DeviceRepository(session).require(
                user_id=account.id, device_public_id=device_id
            )
            rows = ChangeRepository(session).list_after(
                user_id=account.id,
                device_id=device.id,
                after_id=after_id,
                limit=limit + 1,
            )
            included = rows[:limit]
            items = tuple(
                ChangeRecord(
                    cursor=self._cursor.encode("changes", [change.id]),
                    change_type=change.change_type,
                    resource_type=change.entity_type,
                    resource_id=_uuid(change.entity_public_id),
                    occurred_at_utc=_utc(change.created_at_utc)
                    or datetime.now(timezone.utc),
                )
                for change in included
            )
            return Page(
                items=items,
                has_more=len(rows) > limit,
                next_cursor=(items[-1].cursor if len(rows) > limit and items else None),
            )

    async def list_media(
        self,
        user_id: UUID,
        device_id: UUID,
        query: MediaQuery,
    ) -> Page[MediaSummary]:
        _require_page_limit(query.limit)
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            device = DeviceRepository(session).require(
                user_id=account.id, device_public_id=device_id
            )
            source_internal_id = self._optional_source_id(
                session, account.id, query.source_id
            )
            statement = AssetRepository(session).query_media(
                user_id=account.id,
                device_id=device.id,
                source_internal_id=source_internal_id,
                media_type=query.media_type,
                storage_mode=query.storage_mode,
                captured_after_utc=_db_datetime(query.captured_after_utc),
                captured_before_utc=_db_datetime(query.captured_before_utc),
                category=query.category,
                has_location=query.has_location,
                trash_state=query.trash_state,
            )
            assets, next_cursor, has_more = self._media_page(
                session,
                statement=statement,
                cursor=query.cursor,
                limit=query.limit,
                sort=query.sort,
                kind="media",
            )
            items = tuple(
                self._media_summary(session, account.id, device.id, asset)
                for asset in assets
            )
            return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    async def search_media(
        self,
        user_id: UUID,
        device_id: UUID,
        query: MediaSearchQuery,
    ) -> Page[MediaSearchHit]:
        text = query.text.strip()
        if not text:
            raise ConflictError("InvalidSearch", "Search text cannot be empty")
        filters = query.filters
        _require_page_limit(filters.limit)
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            device = DeviceRepository(session).require(
                user_id=account.id, device_public_id=device_id
            )
            source_internal_id = self._optional_source_id(
                session, account.id, filters.source_id
            )
            repository = AssetRepository(session)
            statement = repository.query_media(
                user_id=account.id,
                device_id=device.id,
                source_internal_id=source_internal_id,
                media_type=filters.media_type,
                storage_mode=filters.storage_mode,
                captured_after_utc=_db_datetime(filters.captured_after_utc),
                captured_before_utc=_db_datetime(filters.captured_before_utc),
                category=filters.category,
                has_location=filters.has_location,
                trash_state=filters.trash_state,
                search_text=text,
            )
            assets, next_cursor, has_more = self._media_page(
                session,
                statement=statement,
                cursor=filters.cursor,
                limit=filters.limit,
                sort="UpdatedAtDesc",
                kind="search",
            )
            items = tuple(
                self._search_hit(
                    session=session,
                    user_id=account.id,
                    device_id=device.id,
                    asset=asset,
                    search_text=text,
                )
                for asset in assets
            )
            return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    async def get_media_asset(
        self, user_id: UUID, device_id: UUID, asset_id: UUID
    ) -> MediaDetail:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            device = DeviceRepository(session).require(
                user_id=account.id, device_public_id=device_id
            )
            repository = AssetRepository(session)
            asset = repository.require(
                user_id=account.id, asset_public_id=asset_id
            )
            visible = session.scalar(
                select(
                    repository.visible_expression(
                        user_id=account.id,
                        device_id=device.id,
                        include_trashed_occurrences=True,
                    )
                ).where(MediaAsset.id == asset.id, MediaAsset.user_id == account.id)
            )
            if not visible:
                raise NotFoundError("MediaNotFound", "The media asset was not found")
            summary = self._media_summary(session, account.id, device.id, asset)
            occurrences: list[OccurrenceRecord] = []
            for occurrence, source in session.execute(
                select(MediaOccurrence, MediaSource)
                .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
                .where(
                    MediaOccurrence.user_id == account.id,
                    MediaOccurrence.media_asset_id == asset.id,
                    MediaSource.user_id == account.id,
                )
                .order_by(MediaOccurrence.id.desc())
            ):
                occurrences.append(
                    self._occurrence_record(
                        occurrence,
                        source,
                        reveal_locator=source.device_id == device.id,
                    )
                )
            location = repository.location(user_id=account.id, asset_id=asset.id)
            description = repository.current_description(
                user_id=account.id, asset_id=asset.id
            )
            transcript = repository.current_transcript(
                user_id=account.id, asset_id=asset.id
            )
            return MediaDetail(
                asset=summary,
                occurrences=tuple(occurrences),
                location_detail=self._location_record(location),
                description=self._description_record(description),
                transcript=self._transcript_record(session, account.id, transcript),
                provenance=self._provenance(asset.metadata_json),
            )

    async def create_upload_plan(
        self,
        user_id: UUID,
        command: UploadPlanCommand,
        context: MutationContext,
    ) -> MutationResult[UploadPlanRecord]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[UploadPlanRecord, int]:
                store = self._require_temporary_object_store()
                if command.purpose != "TemporaryProcessing":
                    raise ConflictError(
                        "UploadPurposeNotSupported",
                        "Only temporary scene-processing uploads are available",
                    )
                if command.processing_job_id is None:
                    raise ConflictError(
                        "ProcessingJobRequired",
                        "A scene-description processing job is required",
                    )
                source = SourceRepository(session).require(
                    user_id=account.id, source_public_id=command.source_id
                )
                if source.storage_mode != "Local":
                    raise ConflictError(
                        "TemporaryUploadRequiresLocalSource",
                        "This staging flow is available only for Local sources",
                    )
                occurrence = OccurrenceRepository(session).require(
                    user_id=account.id,
                    occurrence_public_id=command.occurrence_id,
                    source_id=source.id,
                )
                if (
                    occurrence.media_asset_id is None
                    or occurrence.deletion_state != "Active"
                    or occurrence.availability_state == "Unavailable"
                ):
                    raise ConflictError(
                        "OccurrenceUnavailable",
                        "The local media occurrence is not available",
                    )
                asset = session.scalar(
                    select(MediaAsset).where(
                        MediaAsset.user_id == account.id,
                        MediaAsset.id == occurrence.media_asset_id,
                    )
                )
                if asset is None:
                    raise NotFoundError("MediaNotFound", "The media asset was not found")
                self._validate_temporary_upload_command(command, asset=asset)
                job = JobRepository(session).require(
                    user_id=account.id, job_public_id=command.processing_job_id
                )
                if job.media_asset_id != asset.id or job.job_type != "Description":
                    raise ConflictError(
                        "ProcessingJobMismatch",
                        "The processing job does not match this photo",
                    )
                now = self._now()
                if (
                    job.status == "DeferredQuota"
                    and job.next_attempt_at_utc is not None
                    and job.next_attempt_at_utc <= now
                ):
                    self._release_description_provider_request(
                        session=session, job=job, now=now
                    )
                    job.status = "Preparing"
                    job.attempt_count = 0
                    job.next_attempt_at_utc = None
                    job.failure_class = None
                    job.failure_code = None
                    job.failure_message = None
                    job.completed_at_utc = None
                    job.updated_at_utc = now
                if job.status != "Preparing":
                    raise ConflictError(
                        "ProcessingJobNotPreparing",
                        "The scene-description job is not waiting for a preview",
                    )
                current = AssetRepository(session).current_description(
                    user_id=account.id, asset_id=asset.id
                )
                if (
                    current is not None
                    and current.status == "Succeeded"
                    and current.is_current == 1
                    and bool(current.description and current.description.strip())
                ):
                    raise ConflictError(
                        "DescriptionAlreadyAvailable",
                        "This photo already has a current scene description",
                    )

                repository = UploadRepository(session)
                self._expire_temporary_uploads(
                    session=session,
                    account=account,
                    repository=repository,
                    store=store,
                    now=now,
                )
                active = repository.active_for_asset(
                    user_id=account.id,
                    asset_id=asset.id,
                    object_purpose="TemporaryProcessing",
                    for_update=True,
                )
                if active is not None:
                    if active.lease_owner != job.public_id:
                        raise ConflictError(
                            "UploadLeaseHeld",
                            "Another temporary-processing lease is active for this photo",
                        )
                    return (
                        UploadPlanRecord(
                            disposition="LeaseHeld",
                            strategy="SinglePart",
                            media_asset_id=_uuid(asset.public_id),
                            occurrence_id=_uuid(occurrence.public_id),
                            upload_session_id=_uuid(active.public_id),
                            expires_at_utc=_utc(active.expires_at_utc),
                            deduplicated=(
                                OccurrenceRepository(session).active_count_for_asset(
                                    user_id=account.id, asset_id=asset.id
                                )
                                > 1
                            ),
                            retry_after_seconds=max(
                                1,
                                min(
                                    900,
                                    int((active.expires_at_utc - now).total_seconds()),
                                ),
                            ),
                        ),
                        200,
                    )

                if not self._reserve_description_provider_request(
                    session=session,
                    account=account,
                    job=job,
                    now=now,
                ):
                    retry_at = self._first_of_next_month(now)
                    request_json = (
                        job.request_json
                        if isinstance(job.request_json, dict)
                        else {}
                    )
                    circuit_open = (
                        request_json.get("quotaBlockReason") == "CircuitOpen"
                    )
                    job.status = "DeferredQuota"
                    job.next_attempt_at_utc = retry_at
                    job.failure_class = "Quota"
                    job.failure_code = (
                        "ProviderCircuitOpen"
                        if circuit_open
                        else "MonthlySceneDescriptionLimitReached"
                    )
                    job.failure_message = (
                        "Scene description is paused after a provider credential or quota failure."
                        if circuit_open
                        else "Scene description is waiting for the monthly provider quota."
                    )
                    job.lease_token_hash = None
                    job.lease_expires_at_utc = None
                    job.completed_at_utc = None
                    job.updated_at_utc = now
                    ChangeRepository(session).add(
                        user_id=account.id,
                        source_id=job.media_source_id,
                        asset_id=job.media_asset_id,
                        entity_type="ProcessingJob",
                        entity_id=job.id,
                        entity_public_id=job.public_id,
                        change_type="Upsert",
                        now=now,
                    )
                    session.flush()
                    return (
                        UploadPlanRecord(
                            disposition="Deferred",
                            strategy="None",
                            media_asset_id=_uuid(asset.public_id),
                            occurrence_id=_uuid(occurrence.public_id),
                            upload_session_id=None,
                            expires_at_utc=None,
                            deduplicated=(
                                OccurrenceRepository(session).active_count_for_asset(
                                    user_id=account.id, asset_id=asset.id
                                )
                                > 1
                            ),
                            retry_after_seconds=max(
                                1, int((retry_at - now).total_seconds())
                            ),
                        ),
                        200,
                    )

                upload_session_id = uuid4()
                object_expires_at = now + TEMPORARY_UPLOAD_LEASE
                url_expires_at = now + TEMPORARY_UPLOAD_URL_LIFETIME
                checksum_base64 = base64.b64encode(
                    bytes.fromhex(command.object_sha256.lower())
                ).decode("ascii")
                prepared = store.create_presigned_put(
                    user_id=_uuid(account.public_id),
                    media_asset_id=_uuid(asset.public_id),
                    upload_session_id=upload_session_id,
                    checksum_sha256_base64=checksum_base64,
                    content_type=SCENE_PREVIEW_MIME_TYPE,
                    content_length=command.object_byte_size,
                    url_expires_at_utc=_utc(url_expires_at) or url_expires_at,
                    object_expires_at_utc=_utc(object_expires_at) or object_expires_at,
                )
                self._validate_prepared_upload(
                    prepared,
                    checksum_base64=checksum_base64,
                    byte_size=command.object_byte_size,
                    now=now,
                    session_expires_at=object_expires_at,
                )
                upload = UploadSession(
                    public_id=str(upload_session_id),
                    user_id=account.id,
                    media_asset_id=asset.id,
                    media_occurrence_id=occurrence.id,
                    idempotency_key=context.idempotency_key,
                    object_purpose="TemporaryProcessing",
                    upload_kind="SinglePart",
                    status="Uploading",
                    active_lease_marker=1,
                    lease_token_hash=hashlib.sha256(
                        f"{upload_session_id}:{context.request_id}".encode("ascii")
                    ).hexdigest(),
                    lease_owner=job.public_id,
                    s3_bucket=prepared.bucket,
                    s3_object_key=prepared.object_key,
                    checksum_sha256=command.object_sha256.lower(),
                    s3_checksum_algorithm="SHA256",
                    s3_checksum_type="FULL_OBJECT",
                    s3_checksum_value=checksum_base64,
                    expected_byte_size=command.object_byte_size,
                    uploaded_byte_size=0,
                    parts_json={
                        "contentType": SCENE_PREVIEW_MIME_TYPE,
                        "processingJobId": job.public_id,
                        "sourceId": source.public_id,
                        "occurrenceId": occurrence.public_id,
                        "fileName": command.file_name,
                    },
                    expires_at_utc=object_expires_at,
                    created_at_utc=now,
                    updated_at_utc=now,
                )
                session.add(upload)
                session.flush()
                return (
                    UploadPlanRecord(
                        disposition="UploadRequired",
                        strategy="SinglePart",
                        media_asset_id=_uuid(asset.public_id),
                        occurrence_id=_uuid(occurrence.public_id),
                        upload_session_id=upload_session_id,
                        expires_at_utc=_utc(object_expires_at),
                        deduplicated=(
                            OccurrenceRepository(session).active_count_for_asset(
                                user_id=account.id, asset_id=asset.id
                            )
                            > 1
                        ),
                        single_part=SignedUploadRequestRecord(
                            url=prepared.url,
                            method="PUT",
                            headers=dict(prepared.headers),
                            expires_at_utc=_utc(prepared.expires_at_utc)
                            or prepared.expires_at_utc,
                        ),
                    ),
                    200,
                )

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._upload_plan_from_json,
            )

    async def get_upload_session(
        self, user_id: UUID, upload_session_id: UUID
    ) -> UploadSessionRecord:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            upload = UploadRepository(session).require(
                user_id=account.id,
                upload_public_id=upload_session_id,
                for_update=True,
            )
            self._require_temporary_single_part(upload)
            now = self._now()
            if (
                upload.active_lease_marker == 1
                and upload.status in {"Preparing", "Uploading", "Completing"}
                and upload.expires_at_utc <= now
            ):
                store = self._require_temporary_object_store()
                store.delete_object(
                    bucket=upload.s3_bucket, object_key=upload.s3_object_key
                )
                job = self._job_for_upload_session(
                    session=session, account=account, upload=upload
                )
                if job.status == "Preparing":
                    self._release_description_provider_request(
                        session=session, job=job, now=now
                    )
                upload.status = "Expired"
                upload.active_lease_marker = None
                upload.failure_code = "UploadLeaseExpired"
                upload.updated_at_utc = now
                session.flush()
            return self._upload_session_record(upload)

    async def complete_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        command: UploadCompleteCommand,
        context: MutationContext,
    ) -> MutationResult[UploadCompleteRecord]:
        dispatch_job_ids: list[UUID] = []
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[UploadCompleteRecord, int]:
                store = self._require_temporary_object_store()
                upload = UploadRepository(session).require(
                    user_id=account.id,
                    upload_public_id=upload_session_id,
                    for_update=True,
                )
                self._require_temporary_single_part(upload)
                if command.parts:
                    raise ConflictError(
                        "MultipartNotSupported",
                        "Temporary scene previews must use a single PUT",
                    )
                if not HEX_SHA256.fullmatch(command.object_sha256.lower()):
                    raise ConflictError(
                        "InvalidObjectHash",
                        "Object SHA-256 must contain 64 hexadecimal characters",
                    )
                if command.object_sha256.lower() != upload.checksum_sha256.lower():
                    raise ConflictError(
                        "ObjectHashMismatch",
                        "The completed object does not match the planned preview",
                    )
                asset = session.scalar(
                    select(MediaAsset).where(
                        MediaAsset.user_id == account.id,
                        MediaAsset.id == upload.media_asset_id,
                    )
                )
                if asset is None:
                    raise NotFoundError("MediaNotFound", "The media asset was not found")
                job = self._job_for_upload_session(
                    session=session, account=account, upload=upload
                )
                if upload.status == "Completed":
                    return (
                        UploadCompleteRecord(
                            media_asset_id=_uuid(asset.public_id),
                            storage_state=asset.storage_state,
                            processing_jobs=(_uuid(job.public_id),),
                        ),
                        200,
                    )
                if upload.status != "Uploading" or upload.active_lease_marker != 1:
                    raise ConflictError(
                        "UploadNotCompletable",
                        "The upload session cannot be completed",
                    )
                now = self._now()
                if upload.expires_at_utc <= now:
                    raise ConflictError(
                        "UploadLeaseExpired", "The temporary upload lease has expired"
                    )
                if job.status != "Preparing":
                    raise ConflictError(
                        "ProcessingJobNotPreparing",
                        "The scene-description job is not waiting for a preview",
                    )
                metadata = store.head_object(
                    bucket=upload.s3_bucket, object_key=upload.s3_object_key
                )
                if metadata is None:
                    raise ConflictError(
                        "UploadedObjectNotFound",
                        "The temporary preview has not been uploaded",
                    )
                if metadata.byte_size != upload.expected_byte_size:
                    raise ConflictError(
                        "UploadedObjectSizeMismatch",
                        "The uploaded preview size does not match the plan",
                    )
                if self._normalized_mime(metadata.content_type) != SCENE_PREVIEW_MIME_TYPE:
                    raise ConflictError(
                        "UploadedObjectTypeMismatch",
                        "The uploaded preview is not a JPEG image",
                    )
                if metadata.checksum_sha256_hex.lower() != upload.checksum_sha256.lower():
                    raise ConflictError(
                        "UploadedObjectChecksumMismatch",
                        "The uploaded preview checksum does not match the plan",
                    )
                self._assert_local_asset_has_no_remote_locator(asset)

                upload.status = "Completed"
                upload.active_lease_marker = None
                upload.uploaded_byte_size = metadata.byte_size
                upload.completed_at_utc = now
                upload.failure_code = None
                upload.updated_at_utc = now
                request_json = (
                    dict(job.request_json) if isinstance(job.request_json, dict) else {}
                )
                parts = upload.parts_json if isinstance(upload.parts_json, dict) else {}
                request_json.update(
                    {
                        "assetRevision": asset.content_sha256.lower(),
                        "stagingBucket": upload.s3_bucket,
                        "stagingObjectKey": upload.s3_object_key,
                        "previewSha256": upload.checksum_sha256.lower(),
                        "previewChecksumSha256": upload.s3_checksum_value,
                        "previewByteSize": upload.expected_byte_size,
                        "previewMimeType": SCENE_PREVIEW_MIME_TYPE,
                        "uploadSessionId": upload.public_id,
                        "sourceId": parts.get("sourceId"),
                        "occurrenceId": parts.get("occurrenceId"),
                    }
                )
                job.request_json = request_json
                job.status = "Queued"
                job.next_attempt_at_utc = now
                job.lease_token_hash = None
                job.lease_expires_at_utc = None
                job.failure_class = None
                job.failure_code = None
                job.failure_message = None
                job.started_at_utc = None
                job.completed_at_utc = None
                job.updated_at_utc = now
                ChangeRepository(session).add(
                    user_id=account.id,
                    source_id=job.media_source_id,
                    asset_id=asset.id,
                    entity_type="ProcessingJob",
                    entity_id=job.id,
                    entity_public_id=job.public_id,
                    change_type="Upsert",
                    now=now,
                )
                session.flush()
                dispatch_job_ids.append(_uuid(job.public_id))
                return (
                    UploadCompleteRecord(
                        media_asset_id=_uuid(asset.public_id),
                        storage_state="LocalOnly",
                        processing_jobs=(_uuid(job.public_id),),
                    ),
                    200,
                )

            result = self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._upload_complete_from_json,
            )
        self._dispatch_after_commit(
            job_ids=tuple(dispatch_job_ids), job_type="Description"
        )
        return result

    async def cancel_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        context: MutationContext,
    ) -> MutationResult[None]:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[None, int]:
                store = self._require_temporary_object_store()
                upload = UploadRepository(session).require(
                    user_id=account.id,
                    upload_public_id=upload_session_id,
                    for_update=True,
                )
                self._require_temporary_single_part(upload)
                if upload.status == "Completed":
                    raise ConflictError(
                        "UploadAlreadyCompleted",
                        "A completed scene preview cannot be cancelled",
                    )
                if upload.status == "Cancelled":
                    return None, 204
                job = self._job_for_upload_session(
                    session=session, account=account, upload=upload
                )
                if job.status != "Preparing":
                    raise ConflictError(
                        "ProcessingJobAlreadyQueued",
                        "The scene-description job has already been queued",
                    )
                store.delete_object(
                    bucket=upload.s3_bucket, object_key=upload.s3_object_key
                )
                now = self._now()
                upload.status = "Cancelled"
                upload.active_lease_marker = None
                upload.failure_code = "CancelledByClient"
                upload.updated_at_utc = now
                request_json = (
                    dict(job.request_json) if isinstance(job.request_json, dict) else {}
                )
                for key in (
                    "stagingBucket",
                    "stagingObjectKey",
                    "previewSha256",
                    "previewChecksumSha256",
                    "previewByteSize",
                    "previewMimeType",
                    "uploadSessionId",
                    "occurrenceId",
                ):
                    request_json.pop(key, None)
                job.request_json = request_json
                job.status = "Preparing"
                job.next_attempt_at_utc = None
                job.updated_at_utc = now
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                session.flush()
                return None, 204

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=lambda _: None,
            )

    async def list_jobs(self, user_id: UUID, query: JobQuery) -> Page[JobRecord]:
        _require_page_limit(query.limit)
        after_id = _decode_single_id(self._cursor, query.cursor, kind="jobs")
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            asset_internal_id: int | None = None
            if query.media_asset_id is not None:
                asset_internal_id = AssetRepository(session).require(
                    user_id=account.id,
                    asset_public_id=query.media_asset_id,
                ).id
            rows = JobRepository(session).list_after(
                user_id=account.id,
                after_id=after_id,
                limit=query.limit + 1,
                status=query.status,
                job_type=query.job_type,
                media_asset_id=asset_internal_id,
            )
            included = rows[: query.limit]
            items = tuple(self._job_record(session, account.id, row) for row in included)
            return Page(
                items=items,
                has_more=len(rows) > query.limit,
                next_cursor=(
                    self._cursor.encode("jobs", [included[-1].id])
                    if len(rows) > query.limit and included
                    else None
                ),
            )

    async def get_job(self, user_id: UUID, job_id: UUID) -> JobRecord:
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)
            job = JobRepository(session).require(
                user_id=account.id, job_public_id=job_id
            )
            return self._job_record(session, account.id, job)

    async def retry_job(
        self,
        user_id: UUID,
        job_id: UUID,
        context: MutationContext,
    ) -> MutationResult[JobRecord]:
        dispatch_jobs: list[tuple[UUID, str]] = []
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[JobRecord, int]:
                job = JobRepository(session).require(
                    user_id=account.id, job_public_id=job_id
                )
                retryable_cancel = (
                    job.job_type == "Description"
                    and job.status == "Cancelled"
                    and job.failure_code
                    in {
                        "UnsupportedPhoto",
                        "SourceChanged",
                        "SourceUnavailable",
                        "UserSkipped",
                    }
                )
                if job.status not in {"Failed", "DeferredQuota"} and not retryable_cancel:
                    raise RetryNotAllowedError(
                        "JobRetryNotAllowed",
                        "Only failed, quota-deferred, or explicitly skipped scene jobs can be retried",
                    )
                if job.status == "Failed" and job.attempt_count >= job.max_attempts:
                    raise RetryNotAllowedError(
                        "JobAttemptsExhausted",
                        "This job has exhausted its configured retry attempts",
                    )
                now = self._now()
                description_requires_restage = job.job_type == "Description"
                circuit_recovery = self._provider_circuit_is_open(
                    session, job=job, now=now
                )
                if job.status == "DeferredQuota":
                    job.attempt_count = 0
                if circuit_recovery:
                    self._close_provider_circuit(
                        session, job=job, now=now
                    )
                    dispatch_jobs.extend(
                        self._promote_circuit_deferred_jobs(
                            session,
                            job=job,
                            now=now,
                        )
                    )
                if description_requires_restage:
                    self._release_description_provider_request(
                        session=session, job=job, now=now
                    )
                    self._clear_description_staging_request(job)
                job.status = "Preparing" if description_requires_restage else "Queued"
                job.next_attempt_at_utc = None if description_requires_restage else now
                job.lease_token_hash = None
                job.lease_expires_at_utc = None
                job.failure_class = None
                job.failure_code = None
                job.failure_message = None
                job.started_at_utc = None
                job.completed_at_utc = None
                job.updated_at_utc = now
                ChangeRepository(session).add(
                    user_id=account.id,
                    source_id=job.media_source_id,
                    asset_id=job.media_asset_id,
                    entity_type="ProcessingJob",
                    entity_id=job.id,
                    entity_public_id=job.public_id,
                    change_type="Upsert",
                    now=now,
                )
                if not description_requires_restage:
                    dispatch_jobs.append((_uuid(job.public_id), job.job_type))
                return self._job_record(session, account.id, job), 202

            result = self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._job_from_json,
            )
        for dispatched_job_id, dispatched_job_type in dispatch_jobs:
            self._dispatch_after_commit(
                job_ids=(dispatched_job_id,), job_type=dispatched_job_type
            )
        return result

    async def cancel_job(
        self,
        user_id: UUID,
        job_id: UUID,
        reason: str,
        context: MutationContext,
    ) -> MutationResult[JobRecord]:
        messages = {
            "UnsupportedPhoto": "Scene description is unavailable for this photo format.",
            "SourceUnavailable": "Scene description was skipped because the source photo is unavailable.",
            "SourceChanged": "Scene description was superseded because the source photo changed.",
            "UserSkipped": "Scene description was skipped on this device.",
        }
        if reason not in messages:
            raise ValueError("The processing cancellation reason is invalid")
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[JobRecord, int]:
                job = session.scalar(
                    select(ProcessingJob)
                    .where(
                        ProcessingJob.user_id == account.id,
                        ProcessingJob.public_id == str(job_id),
                    )
                    .with_for_update()
                )
                if job is None:
                    raise NotFoundError(
                        "JobNotFound", "The processing job was not found"
                    )
                if job.job_type != "Description":
                    raise ConflictError(
                        "JobCancellationNotAllowed",
                        "Only scene-description preparation can be skipped",
                    )
                if job.status == "Succeeded":
                    raise ConflictError(
                        "JobCancellationNotAllowed",
                        "A completed scene description cannot be skipped",
                    )
                if job.status == "Cancelled":
                    return self._job_record(session, account.id, job), 200
                now = self._now()
                provider_called = self._description_provider_call_started(job)
                self._settle_provider_reservation(
                    session,
                    job=job,
                    now=now,
                    consumed=provider_called,
                )
                self._settle_description_cost_reservation(
                    session,
                    job=job,
                    now=now,
                    provider_called=provider_called,
                )
                uploads = list(
                    session.scalars(
                        select(UploadSession)
                        .where(
                            UploadSession.user_id == account.id,
                            UploadSession.media_asset_id == job.media_asset_id,
                            UploadSession.object_purpose == "TemporaryProcessing",
                            UploadSession.status.in_(
                                (
                                    "Preparing",
                                    "Uploading",
                                    "Completing",
                                    "Completed",
                                )
                            ),
                        )
                        .with_for_update()
                    )
                )
                for upload in uploads:
                    if self._temporary_object_store is not None:
                        try:
                            self._temporary_object_store.delete_object(
                                bucket=upload.s3_bucket,
                                object_key=upload.s3_object_key,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Deferred cancelled preview cleanup uploadId=%s errorType=%s",
                                upload.public_id,
                                type(exc).__name__,
                            )
                    upload.status = "Cancelled"
                    upload.active_lease_marker = None
                    upload.failure_code = reason
                    upload.updated_at_utc = now
                job.status = "Cancelled"
                job.next_attempt_at_utc = None
                job.lease_token_hash = None
                job.lease_expires_at_utc = None
                job.failure_class = "InvalidMedia"
                job.failure_code = reason
                job.failure_message = messages[reason]
                job.completed_at_utc = now
                job.updated_at_utc = now
                self._add_job_change(session, job=job, now=now)
                session.flush()
                return self._job_record(session, account.id, job), 200

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._job_from_json,
            )

    def redispatch_due_jobs(self, *, limit: int = 100) -> int:
        """Recover durable jobs independently of their original SQS delivery."""

        if isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("The due-job sweep limit must be from 1 through 500")
        if self._job_dispatcher is None:
            return 0
        now = self._now()
        stale_before = now - DISPATCH_RECOVERY_DELAY
        dispatch: dict[str, list[UUID]] = {"Geocode": [], "Description": []}
        with transaction_scope(self._session_factory) as session:
            jobs = list(
                session.scalars(
                    select(ProcessingJob)
                    .where(
                        ProcessingJob.job_type.in_(("Geocode", "Description")),
                        or_(
                            and_(
                                ProcessingJob.status == "Queued",
                                ProcessingJob.next_attempt_at_utc.is_not(None),
                                ProcessingJob.next_attempt_at_utc <= now,
                                ProcessingJob.updated_at_utc <= stale_before,
                            ),
                            and_(
                                ProcessingJob.status == "Running",
                                ProcessingJob.lease_expires_at_utc.is_not(None),
                                ProcessingJob.lease_expires_at_utc <= now,
                            ),
                            and_(
                                ProcessingJob.status == "DeferredQuota",
                                ProcessingJob.next_attempt_at_utc.is_not(None),
                                ProcessingJob.next_attempt_at_utc <= now,
                            ),
                        ),
                    )
                    .order_by(
                        ProcessingJob.next_attempt_at_utc.asc(),
                        ProcessingJob.id.asc(),
                    )
                    .limit(limit)
                    .with_for_update()
                )
            )
            for job in jobs:
                original_status = job.status
                if original_status == "Running":
                    # The provider may already have accepted this attempt.
                    description_provider_called = (
                        job.job_type == "Description"
                        and self._description_provider_call_started(job)
                    )
                    self._settle_provider_reservation(
                        session,
                        job=job,
                        now=now,
                        consumed=(
                            description_provider_called
                            if job.job_type == "Description"
                            else True
                        ),
                    )
                    if job.job_type == "Description":
                        self._settle_description_cost_reservation(
                            session,
                            job=job,
                            now=now,
                            provider_called=description_provider_called,
                        )
                elif original_status == "DeferredQuota":
                    if job.job_type == "Description":
                        self._release_description_provider_request(
                            session=session, job=job, now=now
                        )
                    else:
                        self._settle_provider_reservation(
                            session, job=job, now=now, consumed=False
                        )
                    job.attempt_count = 0

                if (
                    original_status != "DeferredQuota"
                    and job.attempt_count >= job.max_attempts
                ):
                    job.status = "Failed"
                    job.next_attempt_at_utc = None
                    job.lease_token_hash = None
                    job.lease_expires_at_utc = None
                    job.failure_class = "Internal"
                    job.failure_code = "AttemptsExhausted"
                    job.failure_message = "Processing exhausted its retry attempts."
                    job.completed_at_utc = now
                    job.updated_at_utc = now
                    self._add_job_change(session, job=job, now=now)
                    continue

                job.lease_token_hash = None
                job.lease_expires_at_utc = None
                job.failure_class = None
                job.failure_code = None
                job.failure_message = None
                job.completed_at_utc = None
                job.updated_at_utc = now
                if (
                    original_status == "DeferredQuota"
                    and job.job_type == "Description"
                ):
                    self._clear_description_staging_request(job)
                    job.status = "Preparing"
                    job.next_attempt_at_utc = None
                else:
                    job.status = "Queued"
                    job.next_attempt_at_utc = now
                    dispatch[job.job_type].append(_uuid(job.public_id))
                self._add_job_change(session, job=job, now=now)
            session.flush()

        for job_type in ("Geocode", "Description"):
            job_ids = dispatch[job_type]
            if job_ids:
                self._job_dispatcher.dispatch(
                    job_ids=tuple(job_ids), job_type=job_type
                )
        return sum(len(job_ids) for job_ids in dispatch.values())

    @staticmethod
    def _clear_description_staging_request(job: ProcessingJob) -> None:
        request = dict(job.request_json) if isinstance(job.request_json, dict) else {}
        for key in (
            "stagingBucket",
            "stagingObjectKey",
            "previewSha256",
            "previewChecksumSha256",
            "previewByteSize",
            "previewMimeType",
            "uploadSessionId",
            "occurrenceId",
        ):
            request.pop(key, None)
        job.request_json = request

    def _promote_circuit_deferred_jobs(
        self,
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
    ) -> list[tuple[UUID, str]]:
        if not job.provider:
            return []
        promoted_dispatch: list[tuple[UUID, str]] = []
        rows = list(
            session.scalars(
                select(ProcessingJob)
                .where(
                    ProcessingJob.user_id == job.user_id,
                    ProcessingJob.provider == job.provider,
                    ProcessingJob.status == "DeferredQuota",
                    ProcessingJob.failure_code == "ProviderCircuitOpen",
                    ProcessingJob.id != job.id,
                )
                .order_by(ProcessingJob.id.asc())
                .limit(100)
                .with_for_update()
            )
        )
        for deferred in rows:
            deferred.attempt_count = 0
            deferred.lease_token_hash = None
            deferred.lease_expires_at_utc = None
            deferred.failure_class = None
            deferred.failure_code = None
            deferred.failure_message = None
            deferred.completed_at_utc = None
            deferred.updated_at_utc = now
            request = (
                dict(deferred.request_json)
                if isinstance(deferred.request_json, dict)
                else {}
            )
            request.pop("quotaBlockReason", None)
            deferred.request_json = request
            if deferred.job_type == "Description":
                self._clear_description_staging_request(deferred)
                deferred.status = "Preparing"
                deferred.next_attempt_at_utc = None
            else:
                deferred.status = "Queued"
                deferred.next_attempt_at_utc = now
                promoted_dispatch.append(
                    (_uuid(deferred.public_id), deferred.job_type)
                )
            self._add_job_change(session, job=deferred, now=now)
        session.execute(
            update(ProcessingJob)
            .where(
                ProcessingJob.user_id == job.user_id,
                ProcessingJob.provider == job.provider,
                ProcessingJob.status == "DeferredQuota",
                ProcessingJob.failure_code == "ProviderCircuitOpen",
                ProcessingJob.id != job.id,
            )
            .values(next_attempt_at_utc=now, updated_at_utc=now)
        )
        return promoted_dispatch

    def claim_description_job(
        self, *, job_id: UUID, message_id: str
    ) -> DescriptionJob | None:
        if not message_id or len(message_id) > 1024:
            return None
        with transaction_scope(self._session_factory) as session:
            job = JobRepository(session).by_public_id(
                job_public_id=job_id, for_update=True
            )
            if job is None or job.job_type != "Description":
                return None
            now = self._now()
            request = job.request_json if isinstance(job.request_json, dict) else {}
            try:
                asset_revision = str(request["assetRevision"]).lower()
                staging_bucket = str(request["stagingBucket"])
                staging_object_key = str(request["stagingObjectKey"])
                preview_sha256 = str(request["previewSha256"]).lower()
                preview_byte_size = int(request["previewByteSize"])
                preview_mime_type = str(request["previewMimeType"])
                upload_session_id = UUID(str(request["uploadSessionId"]))
                model = str(request["model"])
                prompt_version = str(request["promptVersion"])
                detail = str(request["detail"])
                service_tier = str(request["serviceTier"])
                max_words = int(request["maxWords"])
                monthly_call_limit = int(request["monthlyCallLimit"])
                monthly_usd_limit = Decimal(
                    str(
                        request.get(
                            "monthlyUsdLimit",
                            self._scene_description_monthly_usd_limit,
                        )
                    )
                )
                reserved_usd_per_request = Decimal(
                    str(
                        request.get(
                            "reservedUsdPerRequest",
                            self._scene_description_reserved_usd_per_request,
                        )
                    )
                )
                input_usd_per_million = Decimal(
                    str(
                        request.get(
                            "inputUsdPerMillion",
                            self._scene_description_input_usd_per_million,
                        )
                    )
                )
                cached_input_usd_per_million = Decimal(
                    str(
                        request.get(
                            "cachedInputUsdPerMillion",
                            self._scene_description_cached_input_usd_per_million,
                        )
                    )
                )
                output_usd_per_million = Decimal(
                    str(
                        request.get(
                            "outputUsdPerMillion",
                            self._scene_description_output_usd_per_million,
                        )
                    )
                )
            except (KeyError, TypeError, ValueError, InvalidOperation):
                self._fail_invalid_description_job(job, now=now)
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                return None
            if (
                not HEX_SHA256.fullmatch(asset_revision)
                or not HEX_SHA256.fullmatch(preview_sha256)
                or not staging_bucket
                or len(staging_bucket) > 63
                or not staging_object_key
                or len(staging_object_key) > 1024
                or preview_byte_size <= 0
                or preview_byte_size > SCENE_PREVIEW_MAX_BYTES
                or self._normalized_mime(preview_mime_type)
                != SCENE_PREVIEW_MIME_TYPE
                or not model
                or len(model) > 128
                or not prompt_version
                or len(prompt_version) > 64
                or detail not in {"low", "high"}
                or service_tier not in {"auto", "default", "flex"}
                or not 8 <= max_words <= 24
                or monthly_call_limit < 0
                or not monthly_usd_limit.is_finite()
                or monthly_usd_limit < 0
                or not reserved_usd_per_request.is_finite()
                or reserved_usd_per_request <= 0
                or (
                    monthly_usd_limit > 0
                    and reserved_usd_per_request > monthly_usd_limit
                )
                or any(
                    not rate.is_finite() or rate < 0
                    for rate in (
                        input_usd_per_million,
                        cached_input_usd_per_million,
                        output_usd_per_million,
                    )
                )
            ):
                self._fail_invalid_description_job(job, now=now)
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                return None
            cost_configuration = {
                "monthlyUsdLimit": str(monthly_usd_limit.quantize(USD_QUANTUM)),
                "reservedUsdPerRequest": str(
                    reserved_usd_per_request.quantize(USD_QUANTUM)
                ),
                "inputUsdPerMillion": str(input_usd_per_million),
                "cachedInputUsdPerMillion": str(cached_input_usd_per_million),
                "outputUsdPerMillion": str(output_usd_per_million),
            }
            if any(request.get(key) != value for key, value in cost_configuration.items()):
                request = {**request, **cost_configuration}
                job.request_json = request
                job.updated_at_utc = now
            asset = session.scalar(
                select(MediaAsset).where(
                    MediaAsset.user_id == job.user_id,
                    MediaAsset.id == job.media_asset_id,
                )
            )
            try:
                upload = UploadRepository(session).require(
                    user_id=job.user_id, upload_public_id=upload_session_id
                )
            except NotFoundError:
                self._fail_stale_description_job(job, now=now)
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                return None
            if (
                asset is None
                or asset.lifecycle_state != "Active"
                or asset.media_type != "Photo"
                or asset.content_sha256.lower() != asset_revision
                or asset.storage_state != "LocalOnly"
                or asset.s3_bucket is not None
                or upload.media_asset_id != job.media_asset_id
                or upload.status != "Completed"
                or upload.object_purpose != "TemporaryProcessing"
                or upload.upload_kind != "SinglePart"
                or upload.s3_bucket != staging_bucket
                or upload.s3_object_key != staging_object_key
                or upload.checksum_sha256.lower() != preview_sha256
                or upload.expected_byte_size != preview_byte_size
            ):
                self._fail_stale_description_job(job, now=now)
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                return None
            current = AssetRepository(session).current_description(
                user_id=job.user_id, asset_id=job.media_asset_id
            )
            if (
                current is not None
                and current.status == "Succeeded"
                and current.is_current == 1
                and bool(current.description and current.description.strip())
            ):
                self._release_description_provider_request(
                    session=session, job=job, now=now
                )
                self._finish_description_job(job, status="Succeeded", now=now)
                return None

            requested_lease_hash = _lease_hash(message_id)
            same_active_lease = (
                job.status == "Running"
                and job.lease_token_hash == requested_lease_hash
                and job.lease_expires_at_utc is not None
                and job.lease_expires_at_utc > now
            )
            if not same_active_lease:
                expired_running_lease = (
                    job.status == "Running"
                    and (
                        job.lease_expires_at_utc is None
                        or job.lease_expires_at_utc <= now
                    )
                )
                if expired_running_lease and self._description_provider_call_started(job):
                    self._settle_description_cost_reservation(
                        session,
                        job=job,
                        now=now,
                        provider_called=True,
                    )
                if job.status != "Queued" and not expired_running_lease:
                    return None
                if job.next_attempt_at_utc is not None and job.next_attempt_at_utc > now:
                    return None
                if job.attempt_count >= job.max_attempts:
                    if self._description_provider_call_started(job):
                        self._settle_description_cost_reservation(
                            session,
                            job=job,
                            now=now,
                            provider_called=True,
                        )
                    else:
                        self._release_description_provider_request(
                            session=session, job=job, now=now
                        )
                    job.status = "Failed"
                    job.failure_class = "Internal"
                    job.failure_code = "AttemptsExhausted"
                    job.failure_message = (
                        "Scene description exhausted its retry attempts."
                    )
                    job.completed_at_utc = now
                    job.updated_at_utc = now
                    return None
                job.status = "Running"
                job.attempt_count += 1
                job.lease_token_hash = requested_lease_hash
                job.lease_expires_at_utc = now + timedelta(
                    seconds=DESCRIPTION_LEASE_SECONDS
                )
                job.started_at_utc = job.started_at_utc or now
                job.completed_at_utc = None
                job.failure_class = None
                job.failure_code = None
                job.failure_message = None
                job.updated_at_utc = now
                session.flush()
            return DescriptionJob(
                job_id=_uuid(job.public_id),
                user_id=job.user_id,
                media_asset_id=job.media_asset_id,
                asset_revision=asset_revision,
                staging_bucket=staging_bucket,
                staging_object_key=staging_object_key,
                preview_sha256=preview_sha256,
                preview_byte_size=preview_byte_size,
                preview_mime_type=SCENE_PREVIEW_MIME_TYPE,
                model=model,
                prompt_version=prompt_version,
                detail=detail,
                service_tier=service_tier,
                max_words=max_words,
                monthly_call_limit=monthly_call_limit,
                monthly_usd_limit=monthly_usd_limit.quantize(USD_QUANTUM),
                reserved_usd_per_request=reserved_usd_per_request.quantize(
                    USD_QUANTUM
                ),
                input_usd_per_million=input_usd_per_million,
                cached_input_usd_per_million=cached_input_usd_per_million,
                output_usd_per_million=output_usd_per_million,
                lease_owner=message_id,
                attempt_count=job.attempt_count,
                max_attempts=job.max_attempts,
            )

    def reserve_description_provider_call(
        self,
        *,
        job: DescriptionJob,
        provider: str,
        monthly_limit: int,
    ) -> bool:
        if not provider or len(provider) > 64 or monthly_limit < 0:
            raise ValueError("The scene-description provider limit is invalid")
        with transaction_scope(self._session_factory) as session:
            session.execute(
                select(UserAccount.id)
                .where(UserAccount.id == job.user_id)
                .with_for_update()
            )
            claimed = self._claimed_description_job(session, claim=job)
            if claimed is None:
                return False
            stored_job, _, _ = claimed
            if stored_job.provider != provider:
                raise ValueError("The provider does not match the claimed job")
            now = self._now()
            return self._reserve_description_usage(
                session=session,
                user_id=stored_job.user_id,
                job=stored_job,
                provider=provider,
                monthly_request_limit=monthly_limit,
                monthly_usd_limit=job.monthly_usd_limit,
                reserved_usd=job.reserved_usd_per_request,
                now=now,
            )

    def consume_description_provider_call(
        self,
        *,
        job: DescriptionJob,
        provider: str,
    ) -> bool:
        """Conservatively charge a scene request before opening the network call."""

        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_description_job(session, claim=job)
            if claimed is None:
                return False
            stored_job, _, _ = claimed
            if stored_job.provider != provider:
                raise ValueError("The provider does not match the claimed job")
            request = stored_job.request_json if isinstance(stored_job.request_json, dict) else {}
            reservation = request.get("providerUsageReservation")
            if (
                not isinstance(reservation, dict)
                or reservation.get("state") != "Reserved"
                or reservation.get("provider") != provider
            ):
                return False
            now = self._now()
            consumption = {
                "provider": provider,
                "usageMonth": str(reservation.get("usageMonth")),
                "unitType": "Request",
                "units": str(reservation.get("units", "1")),
                "state": "Consumed",
                "consumedAtUtc": (_utc(now) or now).isoformat(),
            }
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=True
            )
            request = dict(stored_job.request_json or {})
            request["providerRequestConsumption"] = consumption
            stored_job.request_json = request
            stored_job.updated_at_utc = now
            session.flush()
            return True

    def complete_description(
        self, *, job: DescriptionJob, result: SceneDescriptionResult
    ) -> DescriptionCleanupDecision:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_description_job(session, claim=job)
            if claimed is None:
                return DescriptionCleanupDecision.RETAIN
            stored_job, asset, _ = claimed
            request = dict(stored_job.request_json or {})
            if (
                result.provider != stored_job.provider
                or result.model != job.model
                or result.prompt_version != job.prompt_version
                or not result.description.strip()
                or not scene_description_usage_within_bounds(result.usage)
            ):
                now = self._now()
                self._settle_provider_reservation(
                    session, job=stored_job, now=now, consumed=True
                )
                self._settle_description_cost_reservation(
                    session,
                    job=stored_job,
                    now=now,
                    provider_called=True,
                    usage=None,
                )
                stored_job.status = "Failed"
                stored_job.next_attempt_at_utc = None
                stored_job.lease_token_hash = None
                stored_job.lease_expires_at_utc = None
                stored_job.failure_class = "Internal"
                stored_job.failure_code = "InvalidSceneDescriptionResult"
                stored_job.failure_message = (
                    "The scene-description provider returned an invalid result."
                )
                stored_job.completed_at_utc = now
                stored_job.updated_at_utc = now
                self._add_job_change(session, job=stored_job, now=now)
                return DescriptionCleanupDecision.DELETE
            now = self._now()
            for current in session.scalars(
                select(MediaDescription).where(
                    MediaDescription.user_id == stored_job.user_id,
                    MediaDescription.media_asset_id == stored_job.media_asset_id,
                    MediaDescription.is_current == 1,
                )
            ):
                current.is_current = 0
                current.updated_at_utc = now
            description = MediaDescription(
                user_id=stored_job.user_id,
                media_asset_id=stored_job.media_asset_id,
                description=result.description,
                language_code="en",
                provider=result.provider,
                model=result.model,
                prompt_version=result.prompt_version,
                status="Succeeded",
                is_current=1,
                requested_at_utc=stored_job.created_at_utc,
                completed_at_utc=now,
                created_at_utc=now,
                updated_at_utc=now,
            )
            session.add(description)
            session.flush()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=True
            )
            self._settle_description_cost_reservation(
                session,
                job=stored_job,
                now=now,
                provider_called=True,
                usage=result.usage,
            )
            self._finish_description_job(stored_job, status="Succeeded", now=now)
            asset.last_processed_at_utc = now
            asset.updated_at_utc = now
            ChangeRepository(session).add(
                user_id=stored_job.user_id,
                source_id=stored_job.media_source_id,
                asset_id=stored_job.media_asset_id,
                entity_type="MediaDescription",
                entity_id=description.id,
                entity_public_id=description.public_id,
                change_type="Upsert",
                now=now,
            )
            self._add_job_change(session, job=stored_job, now=now)
            return DescriptionCleanupDecision.DELETE

    def fail_description(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool = True,
    ) -> DescriptionFailureOutcome:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_description_job(session, claim=job)
            if claimed is None:
                return DescriptionFailureOutcome(
                    retry_requested=False,
                    cleanup=DescriptionCleanupDecision.RETAIN,
                )
            stored_job, _, _ = claimed
            now = self._now()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=provider_called
            )
            self._settle_description_cost_reservation(
                session,
                job=stored_job,
                now=now,
                provider_called=provider_called,
                usage=None,
            )
            if provider_called and failure.failure_class in {
                ProviderFailureClass.AUTHENTICATION,
                ProviderFailureClass.QUOTA,
            }:
                self._open_provider_circuit(
                    session, job=stored_job, now=now, failure_code=failure.code
                )
            should_retry = (
                failure.retryable and stored_job.attempt_count < stored_job.max_attempts
            )
            stored_job.status = "Queued" if should_retry else "Failed"
            stored_job.next_attempt_at_utc = now if should_retry else None
            stored_job.failure_class = str(failure.failure_class.value)[:32]
            stored_job.failure_code = failure.code[:64]
            stored_job.failure_message = failure.user_message
            stored_job.lease_token_hash = None
            stored_job.lease_expires_at_utc = None
            stored_job.completed_at_utc = None if should_retry else now
            stored_job.updated_at_utc = now
            self._add_job_change(session, job=stored_job, now=now)
            return DescriptionFailureOutcome(
                retry_requested=should_retry,
                cleanup=(
                    DescriptionCleanupDecision.RETAIN
                    if should_retry
                    else DescriptionCleanupDecision.DELETE
                ),
            )

    def defer_description_quota(
        self,
        *,
        job: DescriptionJob,
        failure: DescriptionJobFailure,
        provider_called: bool = False,
    ) -> DescriptionCleanupDecision:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_description_job(session, claim=job)
            if claimed is None:
                return DescriptionCleanupDecision.RETAIN
            stored_job, _, _ = claimed
            now = self._now()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=provider_called
            )
            self._settle_description_cost_reservation(
                session,
                job=stored_job,
                now=now,
                provider_called=provider_called,
                usage=None,
            )
            if provider_called:
                self._open_provider_circuit(
                    session, job=stored_job, now=now, failure_code=failure.code
                )
            request = (
                stored_job.request_json
                if isinstance(stored_job.request_json, dict)
                else {}
            )
            circuit_open = (
                not provider_called
                and request.get("quotaBlockReason") == "CircuitOpen"
            )
            stored_job.status = "DeferredQuota"
            stored_job.next_attempt_at_utc = self._first_of_next_month(now)
            stored_job.failure_class = str(failure.failure_class.value)[:32]
            stored_job.failure_code = (
                "ProviderCircuitOpen" if circuit_open else failure.code[:64]
            )
            stored_job.failure_message = (
                "Scene description is paused after a provider credential or quota failure."
                if circuit_open
                else failure.user_message
            )
            stored_job.lease_token_hash = None
            stored_job.lease_expires_at_utc = None
            stored_job.completed_at_utc = None
            stored_job.updated_at_utc = now
            self._add_job_change(session, job=stored_job, now=now)
            return DescriptionCleanupDecision.DELETE

    def _claimed_description_job(
        self, session: Session, *, claim: DescriptionJob
    ) -> tuple[ProcessingJob, MediaAsset, UploadSession] | None:
        now = self._now()
        stored_job = session.scalar(
            select(ProcessingJob)
            .where(
                ProcessingJob.public_id == str(claim.job_id),
                ProcessingJob.user_id == claim.user_id,
                ProcessingJob.media_asset_id == claim.media_asset_id,
                ProcessingJob.job_type == "Description",
            )
            .with_for_update()
        )
        if (
            stored_job is None
            or stored_job.status != "Running"
            or stored_job.lease_token_hash != _lease_hash(claim.lease_owner)
            or stored_job.lease_expires_at_utc is None
            or stored_job.lease_expires_at_utc <= now
        ):
            return None
        request = stored_job.request_json if isinstance(stored_job.request_json, dict) else {}
        try:
            upload_session_id = str(UUID(str(request["uploadSessionId"])))
            request_values = (
                str(request["assetRevision"]).lower(),
                str(request["stagingBucket"]),
                str(request["stagingObjectKey"]),
                str(request["previewSha256"]).lower(),
                int(request["previewByteSize"]),
                self._normalized_mime(str(request["previewMimeType"])),
                str(request["model"]),
                str(request["promptVersion"]),
                str(request["detail"]),
                str(request["serviceTier"]),
                int(request["maxWords"]),
                int(request["monthlyCallLimit"]),
                Decimal(
                    str(
                        request.get(
                            "monthlyUsdLimit",
                            self._scene_description_monthly_usd_limit,
                        )
                    )
                ).quantize(USD_QUANTUM),
                Decimal(
                    str(
                        request.get(
                            "reservedUsdPerRequest",
                            self._scene_description_reserved_usd_per_request,
                        )
                    )
                ).quantize(USD_QUANTUM),
                Decimal(
                    str(
                        request.get(
                            "inputUsdPerMillion",
                            self._scene_description_input_usd_per_million,
                        )
                    )
                ),
                Decimal(
                    str(
                        request.get(
                            "cachedInputUsdPerMillion",
                            self._scene_description_cached_input_usd_per_million,
                        )
                    )
                ),
                Decimal(
                    str(
                        request.get(
                            "outputUsdPerMillion",
                            self._scene_description_output_usd_per_million,
                        )
                    )
                ),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None
        claim_values = (
            claim.asset_revision,
            claim.staging_bucket,
            claim.staging_object_key,
            claim.preview_sha256,
            claim.preview_byte_size,
            self._normalized_mime(claim.preview_mime_type),
            claim.model,
            claim.prompt_version,
            claim.detail,
            claim.service_tier,
            claim.max_words,
            claim.monthly_call_limit,
            claim.monthly_usd_limit,
            claim.reserved_usd_per_request,
            claim.input_usd_per_million,
            claim.cached_input_usd_per_million,
            claim.output_usd_per_million,
        )
        if request_values != claim_values:
            return None
        asset = session.scalar(
            select(MediaAsset).where(
                MediaAsset.user_id == stored_job.user_id,
                MediaAsset.id == stored_job.media_asset_id,
                MediaAsset.content_sha256 == claim.asset_revision,
                MediaAsset.lifecycle_state == "Active",
                MediaAsset.storage_state == "LocalOnly",
            )
        )
        upload = session.scalar(
            select(UploadSession).where(
                UploadSession.user_id == stored_job.user_id,
                UploadSession.public_id == upload_session_id,
                UploadSession.media_asset_id == stored_job.media_asset_id,
                UploadSession.object_purpose == "TemporaryProcessing",
                UploadSession.upload_kind == "SinglePart",
                UploadSession.status == "Completed",
            )
        )
        if (
            asset is None
            or asset.s3_bucket is not None
            or asset.original_s3_object_key is not None
            or asset.preview_s3_object_key is not None
            or upload is None
            or upload.s3_bucket != claim.staging_bucket
            or upload.s3_object_key != claim.staging_object_key
            or upload.checksum_sha256.lower() != claim.preview_sha256
            or upload.expected_byte_size != claim.preview_byte_size
        ):
            return None
        return stored_job, asset, upload

    @staticmethod
    def _finish_description_job(
        job: ProcessingJob, *, status: str, now: datetime
    ) -> None:
        job.status = status
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = None
        job.failure_code = None
        job.failure_message = None
        job.completed_at_utc = now
        job.updated_at_utc = now

    @staticmethod
    def _fail_invalid_description_job(
        job: ProcessingJob, *, now: datetime
    ) -> None:
        job.status = "Failed"
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = "Internal"
        job.failure_code = "InvalidJobPayload"
        job.failure_message = "Scene description could not read its job payload."
        job.completed_at_utc = now
        job.updated_at_utc = now

    @staticmethod
    def _fail_stale_description_job(
        job: ProcessingJob, *, now: datetime
    ) -> None:
        job.status = "Cancelled"
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = "InvalidMedia"
        job.failure_code = "StaleScenePreview"
        job.failure_message = "Scene description was superseded by newer media data."
        job.completed_at_utc = now
        job.updated_at_utc = now

    def claim_geocode_job(
        self, *, job_id: UUID, message_id: str
    ) -> GeocodeJob | None:
        """Atomically lease a queued geocode job to one SQS delivery.

        A missing job is an expected orphan-message outcome because dispatch occurs
        just before the creating transaction commits. Duplicate or stale messages
        are likewise acknowledged by returning ``None``.
        """

        if not message_id or len(message_id) > 1024:
            return None
        with transaction_scope(self._session_factory) as session:
            repository = JobRepository(session)
            job = repository.by_public_id(job_public_id=job_id, for_update=True)
            if job is None or job.job_type != "Geocode":
                return None
            now = self._now()
            request = job.request_json if isinstance(job.request_json, dict) else {}
            try:
                latitude = Decimal(str(request["latitude"]))
                longitude = Decimal(str(request["longitude"]))
                revision = str(request["coordinateRevision"])
            except (KeyError, TypeError, ValueError):
                self._fail_invalid_geocode_job(job, now=now)
                return None
            if (
                not latitude.is_finite()
                or not longitude.is_finite()
                or not Decimal("-90") <= latitude <= Decimal("90")
                or not Decimal("-180") <= longitude <= Decimal("180")
                or revision != _coordinate_revision(latitude, longitude)
            ):
                self._fail_invalid_geocode_job(job, now=now)
                return None
            location = LocationRepository(session).by_asset(
                user_id=job.user_id, asset_id=job.media_asset_id
            )
            asset_active = session.scalar(
                select(MediaAsset.lifecycle_state).where(
                    MediaAsset.user_id == job.user_id,
                    MediaAsset.id == job.media_asset_id,
                )
            )
            if (
                asset_active != "Active"
                or
                location is None
                or location.latitude is None
                or location.longitude is None
                or _coordinate_revision(location.latitude, location.longitude) != revision
            ):
                self._settle_provider_reservation(
                    session, job=job, now=now, consumed=True
                )
                self._cancel_stale_geocode_job(job, now=now)
                return None

            requested_lease_hash = _lease_hash(message_id)
            same_active_lease = (
                job.status == "Running"
                and job.lease_token_hash == requested_lease_hash
                and job.lease_expires_at_utc is not None
                and job.lease_expires_at_utc > now
            )
            if not same_active_lease:
                expired_running_lease = (
                    job.status == "Running"
                    and (
                        job.lease_expires_at_utc is None
                        or job.lease_expires_at_utc <= now
                    )
                )
                if job.status != "Queued" and not expired_running_lease:
                    return None
                if job.next_attempt_at_utc is not None and job.next_attempt_at_utc > now:
                    return None
                if job.attempt_count >= job.max_attempts:
                    self._settle_provider_reservation(
                        session, job=job, now=now, consumed=True
                    )
                    job.status = "Failed"
                    job.failure_class = "Internal"
                    job.failure_code = "AttemptsExhausted"
                    job.failure_message = "Location enrichment exhausted its retry attempts."
                    job.completed_at_utc = now
                    job.updated_at_utc = now
                    return None
                job.status = "Running"
                job.attempt_count += 1
                job.lease_token_hash = requested_lease_hash
                job.lease_expires_at_utc = now + timedelta(
                    seconds=GEOCODE_LEASE_SECONDS
                )
                job.started_at_utc = job.started_at_utc or now
                job.completed_at_utc = None
                job.failure_class = None
                job.failure_code = None
                job.failure_message = None
                job.updated_at_utc = now
                session.flush()
            return GeocodeJob(
                job_id=_uuid(job.public_id),
                user_id=job.user_id,
                media_asset_id=job.media_asset_id,
                latitude=float(latitude),
                longitude=float(longitude),
                attempt_count=job.attempt_count,
                max_attempts=job.max_attempts,
                coordinate_revision=revision,
                lease_owner=message_id,
            )

    def find_reusable_location(
        self, *, job: GeocodeJob, radius_meters: float
    ) -> ReverseGeocodeResult | None:
        if radius_meters < 0 or radius_meters > 100:
            raise ValueError("The geocode reuse radius must be between 0 and 100 meters")
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return None
            stored_job, current_location = claimed
            reusable = LocationRepository(session).resolved_nearby(
                user_id=stored_job.user_id,
                latitude=current_location.latitude,
                longitude=current_location.longitude,
                radius_meters=radius_meters,
                exclude_asset_id=stored_job.media_asset_id,
            )
            if reusable is None:
                return None
            reusable_result = self._reverse_geocode_result(reusable)
            if (
                reusable_result.resolution is not None
                and not self._location_normalizer.can_reuse(
                    reusable_result.resolution
                )
            ):
                return None
            request = dict(stored_job.request_json or {})
            request["reuseLocationPublicId"] = reusable.public_id
            stored_job.request_json = request
            stored_job.updated_at_utc = self._now()
            session.flush()
            return reusable_result

    def reserve_provider_call(
        self,
        *,
        job: GeocodeJob,
        provider: str,
        monthly_limit: int,
    ) -> bool:
        if not provider or len(provider) > 64:
            raise ValueError("The geocode provider identity is invalid")
        if monthly_limit < 0:
            raise ValueError("The monthly provider limit cannot be negative")
        with transaction_scope(self._session_factory) as session:
            # Serialize creation of the per-user/month counter without requiring
            # an external lock or allowing two first calls to overrun the cap.
            session.execute(
                select(UserAccount.id)
                .where(UserAccount.id == job.user_id)
                .with_for_update()
            )
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return False
            stored_job, _ = claimed
            if stored_job.provider != provider:
                raise ValueError("The provider does not match the claimed job")
            request = dict(stored_job.request_json or {})
            existing_reservation = request.get("providerUsageReservation")
            now = self._now()
            usage_month = date(now.year, now.month, 1)
            if (
                isinstance(existing_reservation, dict)
                and existing_reservation.get("state") == "Reserved"
            ):
                current_reservation = (
                    existing_reservation.get("provider") == provider
                    and existing_reservation.get("usageMonth")
                    == usage_month.isoformat()
                    and existing_reservation.get("unitType") == "Request"
                    and existing_reservation.get("units") == "1"
                )
                if current_reservation:
                    usage = ProviderUsageRepository(session).get(
                        user_id=stored_job.user_id,
                        provider=provider,
                        usage_month=usage_month,
                        unit_type="Request",
                        for_update=True,
                    )
                    if usage is not None and usage.circuit_state == "Open":
                        request["quotaBlockReason"] = "CircuitOpen"
                        stored_job.request_json = request
                        return False
                    request.pop("quotaBlockReason", None)
                    stored_job.request_json = request
                    return usage is not None
                self._settle_provider_reservation(
                    session, job=stored_job, now=now, consumed=False
                )
                request = dict(stored_job.request_json or {})
            limit = Decimal(monthly_limit)
            usage = ProviderUsageRepository(session).get_or_create(
                user_id=stored_job.user_id,
                provider=provider,
                usage_month=usage_month,
                unit_type="Request",
                hard_limit_units=limit,
                now=now,
            )
            usage.hard_limit_units = limit
            used_or_reserved = usage.processed_units + usage.reserved_units
            if usage.circuit_state == "Open":
                request["quotaBlockReason"] = "CircuitOpen"
                stored_job.request_json = request
                usage.updated_at_utc = now
                session.flush()
                return False
            request.pop("quotaBlockReason", None)
            if (
                monthly_limit == 0
                or used_or_reserved + Decimal("1") > limit
            ):
                usage.updated_at_utc = now
                session.flush()
                return False
            usage.reserved_units += Decimal("1")
            usage.updated_at_utc = now
            request["providerUsageReservation"] = {
                "provider": provider,
                "usageMonth": usage_month.isoformat(),
                "unitType": "Request",
                "units": "1",
                "state": "Reserved",
            }
            stored_job.request_json = request
            stored_job.updated_at_utc = now
            session.flush()
            return True

    def consume_provider_call(
        self,
        *,
        job: GeocodeJob,
        provider: str,
    ) -> bool:
        """Conservatively charge a geocode request before opening the network call."""

        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return False
            stored_job, _ = claimed
            if stored_job.provider != provider:
                raise ValueError("The provider does not match the claimed job")
            request = stored_job.request_json if isinstance(stored_job.request_json, dict) else {}
            reservation = request.get("providerUsageReservation")
            if (
                not isinstance(reservation, dict)
                or reservation.get("state") != "Reserved"
                or reservation.get("provider") != provider
            ):
                return False
            now = self._now()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=True
            )
            stored_job.updated_at_utc = now
            session.flush()
            return True

    def complete_geocode(
        self,
        *,
        job: GeocodeJob,
        result: ReverseGeocodeResult,
        reused: bool,
    ) -> None:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return
            stored_job, location = claimed
            now = self._now()
            self._apply_geocode_result(location, result=result, now=now)
            self._apply_geocode_timezone(
                session,
                user_id=stored_job.user_id,
                asset_id=stored_job.media_asset_id,
                result=result,
                now=now,
            )
            location.updated_at_utc = now
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=not reused
            )
            self._finish_geocode_job(stored_job, status="Succeeded", now=now)
            session.flush()
            self._add_geocode_changes(session, job=stored_job, location=location, now=now)

    def fail_geocode(
        self, *, job: GeocodeJob, failure: GeocodeJobFailure
    ) -> bool:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return False
            stored_job, _ = claimed
            now = self._now()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=True
            )
            if failure.failure_class in {
                ProviderFailureClass.AUTHENTICATION,
                ProviderFailureClass.QUOTA,
            }:
                self._open_provider_circuit(
                    session, job=stored_job, now=now, failure_code=failure.code
                )
            should_retry = failure.retryable and stored_job.attempt_count < stored_job.max_attempts
            stored_job.status = "Queued" if should_retry else "Failed"
            stored_job.next_attempt_at_utc = now if should_retry else None
            stored_job.failure_class = str(failure.failure_class.value)[:32]
            stored_job.failure_code = failure.code[:64]
            stored_job.failure_message = failure.user_message
            stored_job.lease_token_hash = None
            stored_job.lease_expires_at_utc = None
            stored_job.completed_at_utc = None if should_retry else now
            stored_job.updated_at_utc = now
            session.flush()
            self._add_job_change(session, job=stored_job, now=now)
            return should_retry

    def defer_geocode_quota(
        self,
        *,
        job: GeocodeJob,
        failure: GeocodeJobFailure,
        provider_called: bool,
    ) -> None:
        with transaction_scope(self._session_factory) as session:
            claimed = self._claimed_geocode_job(session, claim=job)
            if claimed is None:
                return
            stored_job, _ = claimed
            now = self._now()
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=provider_called
            )
            if provider_called:
                self._open_provider_circuit(
                    session, job=stored_job, now=now, failure_code=failure.code
                )
            request = (
                stored_job.request_json
                if isinstance(stored_job.request_json, dict)
                else {}
            )
            circuit_open = (
                not provider_called
                and request.get("quotaBlockReason") == "CircuitOpen"
            )
            next_year = now.year + 1 if now.month == 12 else now.year
            next_month = 1 if now.month == 12 else now.month + 1
            stored_job.status = "DeferredQuota"
            stored_job.next_attempt_at_utc = datetime(next_year, next_month, 1)
            stored_job.failure_class = str(failure.failure_class.value)[:32]
            stored_job.failure_code = (
                "ProviderCircuitOpen" if circuit_open else failure.code[:64]
            )
            stored_job.failure_message = (
                "Location enrichment is paused after a provider credential or quota failure."
                if circuit_open
                else failure.user_message
            )
            stored_job.lease_token_hash = None
            stored_job.lease_expires_at_utc = None
            stored_job.completed_at_utc = None
            stored_job.updated_at_utc = now
            session.flush()
            self._add_job_change(session, job=stored_job, now=now)

    def _claimed_geocode_job(
        self, session: Session, *, claim: GeocodeJob
    ) -> tuple[ProcessingJob, MediaLocation] | None:
        now = self._now()
        stored_job = session.scalar(
            select(ProcessingJob)
            .where(
                ProcessingJob.public_id == str(claim.job_id),
                ProcessingJob.user_id == claim.user_id,
                ProcessingJob.media_asset_id == claim.media_asset_id,
                ProcessingJob.job_type == "Geocode",
            )
            .with_for_update()
        )
        if (
            stored_job is None
            or stored_job.status != "Running"
            or stored_job.lease_token_hash != _lease_hash(claim.lease_owner)
            or stored_job.lease_expires_at_utc is None
            or stored_job.lease_expires_at_utc <= now
        ):
            return None
        request = stored_job.request_json if isinstance(stored_job.request_json, dict) else {}
        if request.get("coordinateRevision") != claim.coordinate_revision:
            return None
        location = LocationRepository(session).by_asset(
            user_id=stored_job.user_id, asset_id=stored_job.media_asset_id
        )
        asset_active = session.scalar(
            select(MediaAsset.lifecycle_state).where(
                MediaAsset.user_id == stored_job.user_id,
                MediaAsset.id == stored_job.media_asset_id,
            )
        )
        if (
            asset_active != "Active"
            or
            location is None
            or location.latitude is None
            or location.longitude is None
            or _coordinate_revision(location.latitude, location.longitude)
            != claim.coordinate_revision
        ):
            self._settle_provider_reservation(
                session, job=stored_job, now=now, consumed=True
            )
            self._cancel_stale_geocode_job(stored_job, now=now)
            session.flush()
            self._add_job_change(session, job=stored_job, now=now)
            return None
        return stored_job, location

    @staticmethod
    def _apply_geocode_result(
        location: MediaLocation, *, result: ReverseGeocodeResult, now: datetime
    ) -> None:
        resolution = result.resolution
        location.provider = result.provider
        location.provider_updated_at_utc = (
            _db_datetime(result.provider_updated_at_utc) or now
        )
        location.raw_provider_json = deepcopy(dict(result.raw_provider_json))
        if resolution is None:
            for attribute in (
                "location_display_name",
                "street_address",
                "original_street_number",
                "neighborhood",
                "city",
                "county",
                "state",
                "postal_code",
                "country",
                "country_code",
                "provider_place_id",
                "normalization_rule_version",
                "confidence",
            ):
                setattr(location, attribute, None)
            return
        location.location_display_name = resolution.location_display_name
        location.street_address = resolution.street_address
        location.original_street_number = resolution.original_street_number
        location.neighborhood = resolution.neighborhood
        location.city = resolution.city
        location.county = resolution.county
        location.state = resolution.state
        location.postal_code = resolution.postal_code
        location.country = resolution.country
        location.country_code = resolution.country_code
        location.provider = resolution.provider
        location.provider_place_id = resolution.provider_place_id
        location.normalization_rule_version = resolution.normalization_rule_version
        # Address-match certainty is not the same thing as EXIF GPS certainty.
        # Keep this unset until the API exposes a dedicated address-confidence field.
        location.confidence = None

    @staticmethod
    def _reverse_geocode_result(location: MediaLocation) -> ReverseGeocodeResult:
        raw = deepcopy(location.raw_provider_json) or {}
        original = (
            raw.get("OriginalAddress")
            if isinstance(raw.get("OriginalAddress"), dict)
            else None
        )

        def original_text(key: str, fallback: str | None) -> str | None:
            if original is None:
                return fallback
            value = original.get(key)
            return str(value) if isinstance(value, str) and value else fallback

        provider_status = str(
            raw.get("ProviderStatus") or raw.get("status") or "OK"
        )
        has_address = any(
            (
                original_text("DisplayName", location.location_display_name),
                original_text("StreetAddress", location.street_address),
                original_text("City", location.city),
                original_text("State", location.state),
                original_text("Country", location.country),
            )
        )
        resolution = None
        if has_address:
            resolution = GeocodeResolution(
                location_display_name=original_text(
                    "DisplayName", location.location_display_name
                )
                or "",
                street_address=original_text(
                    "StreetAddress", location.street_address
                ),
                original_street_number=original_text(
                    "StreetNumber", location.original_street_number
                ),
                neighborhood=original_text(
                    "Neighborhood", location.neighborhood
                ),
                city=original_text("City", location.city),
                county=original_text("County", location.county),
                state=original_text("State", location.state),
                postal_code=original_text("PostalCode", location.postal_code),
                country=original_text("Country", location.country),
                country_code=original_text(
                    "CountryCode", location.country_code
                ),
                provider=location.provider or GEOCODE_PROVIDER,
                provider_place_id=location.provider_place_id,
                raw_provider_json=deepcopy(raw),
                normalization_rule_version=(
                    None
                    if original is not None
                    else location.normalization_rule_version
                ),
                time_zone_id=(
                    str((raw.get("TimeZone") or {}).get("Name"))
                    if isinstance(raw.get("TimeZone"), dict)
                    and (raw.get("TimeZone") or {}).get("Name")
                    else None
                ),
            )
        return ReverseGeocodeResult(
            provider=location.provider or GEOCODE_PROVIDER,
            provider_status=provider_status,
            resolution=resolution,
            raw_provider_json=raw,
            provider_updated_at_utc=_utc(location.provider_updated_at_utc),
        )

    @staticmethod
    def _apply_geocode_timezone(
        session: Session,
        *,
        user_id: int,
        asset_id: int,
        result: ReverseGeocodeResult,
        now: datetime,
    ) -> None:
        resolution = result.resolution
        if resolution is None or not resolution.time_zone_id:
            return
        asset = session.scalar(
            select(MediaAsset).where(
                MediaAsset.user_id == user_id,
                MediaAsset.id == asset_id,
            )
        )
        if asset is None:
            return
        changed = False
        if not asset.time_zone:
            asset.time_zone = resolution.time_zone_id
            changed = True
        if asset.capture_datetime_local is not None:
            try:
                aware_local = asset.capture_datetime_local.replace(
                    tzinfo=ZoneInfo(asset.time_zone or resolution.time_zone_id)
                )
            except (ValueError, ZoneInfoNotFoundError):
                aware_local = None
            if aware_local is not None:
                offset = aware_local.utcoffset()
                if asset.utc_offset_minutes is None and offset is not None:
                    asset.utc_offset_minutes = int(offset.total_seconds() // 60)
                    changed = True
                if asset.capture_datetime_utc is None:
                    asset.capture_datetime_utc = aware_local.astimezone(
                        timezone.utc
                    ).replace(tzinfo=None)
                    changed = True
        if changed:
            asset.updated_at_utc = now

    def _settle_provider_reservation(
        self,
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
        consumed: bool,
    ) -> None:
        request = dict(job.request_json or {})
        reservation = request.get("providerUsageReservation")
        if not isinstance(reservation, dict) or reservation.get("state") != "Reserved":
            return
        try:
            usage_month = date.fromisoformat(str(reservation["usageMonth"]))
            provider = str(reservation["provider"])
            unit_type = str(reservation["unitType"])
            units = Decimal(str(reservation.get("units", "1")))
        except (KeyError, ValueError):
            request.pop("providerUsageReservation", None)
            job.request_json = request
            return
        usage = ProviderUsageRepository(session).get(
            user_id=job.user_id,
            provider=provider,
            usage_month=usage_month,
            unit_type=unit_type,
            for_update=True,
        )
        if usage is not None:
            released = min(usage.reserved_units, units)
            usage.reserved_units -= released
            if consumed:
                usage.processed_units += released
            usage.updated_at_utc = now
        request.pop("providerUsageReservation", None)
        job.request_json = request

    def _settle_description_cost_reservation(
        self,
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
        provider_called: bool,
        usage: Mapping[str, Any] | None = None,
    ) -> Decimal:
        """Release reserved USD and charge actual or conservative provider cost."""

        request = dict(job.request_json) if isinstance(job.request_json, dict) else {}
        reservation = request.get("providerCostReservation")
        if not isinstance(reservation, dict) or reservation.get("state") != "Reserved":
            return Decimal("0.000000")
        try:
            usage_month = date.fromisoformat(str(reservation["usageMonth"]))
            reserved = Decimal(str(reservation.get("units", "0"))).quantize(
                USD_QUANTUM
            )
            provider = str(reservation.get("provider") or DESCRIPTION_PROVIDER)
            input_rate = Decimal(
                str(
                    request.get(
                        "inputUsdPerMillion",
                        self._scene_description_input_usd_per_million,
                    )
                )
            )
            cached_rate = Decimal(
                str(
                    request.get(
                        "cachedInputUsdPerMillion",
                        self._scene_description_cached_input_usd_per_million,
                    )
                )
            )
            output_rate = Decimal(
                str(
                    request.get(
                        "outputUsdPerMillion",
                        self._scene_description_output_usd_per_million,
                    )
                )
            )
        except (KeyError, ValueError, InvalidOperation):
            return Decimal("0.000000")
        calculation = (
            scene_description_cost_usd(
                usage,
                input_usd_per_million=input_rate,
                cached_input_usd_per_million=cached_rate,
                output_usd_per_million=output_rate,
            )
            if provider_called
            else None
        )
        if not provider_called:
            charged = Decimal("0.000000")
            basis = "ReleasedNoProviderCall"
            safe_usage = None
        elif calculation is None:
            charged = reserved
            basis = "ConservativeReservation"
            safe_usage = None
        else:
            charged, safe_usage = calculation
            if charged > reserved:
                charged = reserved
                basis = "ConservativeReservation"
            else:
                basis = "ActualUsage"
        if not provider_called:
            consumption = request.get("providerRequestConsumption")
            if (
                isinstance(consumption, dict)
                and consumption.get("state") == "Consumed"
            ):
                try:
                    consumed_month = date.fromisoformat(
                        str(consumption["usageMonth"])
                    )
                    consumed_units = Decimal(
                        str(consumption.get("units", "1"))
                    )
                except (KeyError, ValueError, InvalidOperation):
                    consumed_month = usage_month
                    consumed_units = Decimal("0")
                request_usage = ProviderUsageRepository(session).get(
                    user_id=job.user_id,
                    provider=str(consumption.get("provider") or provider),
                    usage_month=consumed_month,
                    unit_type="Request",
                    for_update=True,
                )
                if request_usage is not None:
                    request_usage.processed_units -= min(
                        request_usage.processed_units,
                        max(Decimal("0"), consumed_units),
                    )
                    request_usage.updated_at_utc = now
                reversed_consumption = dict(consumption)
                reversed_consumption["state"] = "Reversed"
                reversed_consumption["reversedAtUtc"] = (
                    _utc(now) or now
                ).isoformat()
                request["providerRequestConsumption"] = reversed_consumption
        cost_usage = ProviderUsageRepository(session).get(
            user_id=job.user_id,
            provider=provider,
            usage_month=usage_month,
            unit_type="Usd",
            for_update=True,
        )
        if cost_usage is not None:
            cost_usage.reserved_units -= min(
                cost_usage.reserved_units, max(Decimal("0"), reserved)
            )
            cost_usage.processed_units += charged
            cost_usage.updated_at_utc = now
        settled = dict(reservation)
        settled["state"] = "Consumed" if provider_called else "Released"
        settled["settledAtUtc"] = (_utc(now) or now).isoformat()
        settled["chargedUsd"] = str(charged.quantize(USD_QUANTUM))
        request["providerCostReservation"] = settled
        request["providerCost"] = {
            "currency": "USD",
            "amount": str(charged.quantize(USD_QUANTUM)),
            "basis": basis,
            "ratesPerMillion": {
                "input": str(input_rate),
                "cachedInput": str(cached_rate),
                "output": str(output_rate),
            },
        }
        if safe_usage is not None:
            request["providerUsage"] = deepcopy(safe_usage)
        job.request_json = request
        job.updated_at_utc = now
        return charged

    @staticmethod
    def _description_provider_call_started(job: ProcessingJob) -> bool:
        request = job.request_json if isinstance(job.request_json, dict) else {}
        cost = request.get("providerCostReservation")
        provider_request = request.get("providerUsageReservation")
        return (
            isinstance(cost, dict)
            and cost.get("state") == "Reserved"
            and not (
                isinstance(provider_request, dict)
                and provider_request.get("state") == "Reserved"
            )
        )

    @staticmethod
    def _open_provider_circuit(
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
        failure_code: str,
    ) -> None:
        if not job.provider:
            return
        usage = ProviderUsageRepository(session).get(
            user_id=job.user_id,
            provider=job.provider,
            usage_month=date(now.year, now.month, 1),
            unit_type="Request",
            for_update=True,
        )
        if usage is None:
            return
        usage.circuit_state = "Open"
        usage.circuit_opened_at_utc = now
        usage.circuit_failure_code = failure_code[:64]
        usage.updated_at_utc = now

    @staticmethod
    def _provider_circuit_is_open(
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
    ) -> bool:
        if not job.provider:
            return False
        usage = ProviderUsageRepository(session).get(
            user_id=job.user_id,
            provider=job.provider,
            usage_month=date(now.year, now.month, 1),
            unit_type="Request",
            for_update=True,
        )
        return usage is not None and usage.circuit_state == "Open"

    @staticmethod
    def _close_provider_circuit(
        session: Session,
        *,
        job: ProcessingJob,
        now: datetime,
    ) -> None:
        if not job.provider:
            return
        usage = ProviderUsageRepository(session).get(
            user_id=job.user_id,
            provider=job.provider,
            usage_month=date(now.year, now.month, 1),
            unit_type="Request",
            for_update=True,
        )
        if usage is None:
            return
        usage.circuit_state = "Closed"
        usage.circuit_opened_at_utc = None
        usage.circuit_failure_code = None
        usage.updated_at_utc = now

    @staticmethod
    def _finish_geocode_job(
        job: ProcessingJob, *, status: str, now: datetime
    ) -> None:
        job.status = status
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = None
        job.failure_code = None
        job.failure_message = None
        job.completed_at_utc = now
        job.updated_at_utc = now

    @staticmethod
    def _cancel_stale_geocode_job(job: ProcessingJob, *, now: datetime) -> None:
        job.status = "Cancelled"
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = "InvalidMedia"
        job.failure_code = "StaleCoordinates"
        job.failure_message = "Location enrichment was superseded by newer GPS data."
        job.completed_at_utc = now
        job.updated_at_utc = now

    @staticmethod
    def _fail_invalid_geocode_job(job: ProcessingJob, *, now: datetime) -> None:
        job.status = "Failed"
        job.next_attempt_at_utc = None
        job.lease_token_hash = None
        job.lease_expires_at_utc = None
        job.failure_class = "Internal"
        job.failure_code = "InvalidJobPayload"
        job.failure_message = "Location enrichment could not read its job payload."
        job.completed_at_utc = now
        job.updated_at_utc = now

    @staticmethod
    def _add_job_change(
        session: Session, *, job: ProcessingJob, now: datetime
    ) -> None:
        ChangeRepository(session).add(
            user_id=job.user_id,
            source_id=job.media_source_id,
            asset_id=job.media_asset_id,
            entity_type="ProcessingJob",
            entity_id=job.id,
            entity_public_id=job.public_id,
            change_type="Upsert",
            now=now,
        )

    def _add_geocode_changes(
        self,
        session: Session,
        *,
        job: ProcessingJob,
        location: MediaLocation,
        now: datetime,
    ) -> None:
        ChangeRepository(session).add(
            user_id=job.user_id,
            source_id=job.media_source_id,
            asset_id=job.media_asset_id,
            entity_type="MediaLocation",
            entity_id=location.id,
            entity_public_id=location.public_id,
            change_type="Upsert",
            now=now,
        )
        self._add_job_change(session, job=job, now=now)

    def _optional_source_id(
        self, session: Session, user_id: int, source_public_id: UUID | None
    ) -> int | None:
        if source_public_id is None:
            return None
        return SourceRepository(session).require(
            user_id=user_id, source_public_id=source_public_id
        ).id

    def _media_page(
        self,
        session: Session,
        *,
        statement: Any,
        cursor: str | None,
        limit: int,
        sort: str,
        kind: str,
    ) -> tuple[list[MediaAsset], str | None, bool]:
        if sort == "UpdatedAtDesc":
            key = MediaAsset.updated_at_utc
            descending = True
        elif sort in {"CapturedAtDesc", "CapturedAtAsc"}:
            key = func.coalesce(
                MediaAsset.capture_datetime_utc,
                MediaAsset.capture_datetime_local,
                MediaAsset.created_at_utc,
            )
            descending = sort == "CapturedAtDesc"
        else:
            raise ConflictError("InvalidMediaSort", "The media sort is invalid")
        position = self._cursor.decode(cursor, kind=kind)
        if position is not None:
            if len(position) != 2 or not isinstance(position[0], str) or not isinstance(
                position[1], int
            ):
                raise InvalidCursorError()
            try:
                date_position = _db_datetime(datetime.fromisoformat(position[0]))
            except ValueError as exc:
                raise InvalidCursorError() from exc
            if descending:
                statement = statement.where(
                    or_(
                        key < date_position,
                        and_(key == date_position, MediaAsset.id < position[1]),
                    )
                )
            else:
                statement = statement.where(
                    or_(
                        key > date_position,
                        and_(key == date_position, MediaAsset.id > position[1]),
                    )
                )
        ordering = (
            (key.desc(), MediaAsset.id.desc())
            if descending
            else (key.asc(), MediaAsset.id.asc())
        )
        rows = list(session.scalars(statement.order_by(*ordering).limit(limit + 1)))
        included = rows[:limit]
        next_cursor = None
        if len(rows) > limit and included:
            last = included[-1]
            last_value = (
                last.updated_at_utc
                if sort == "UpdatedAtDesc"
                else last.capture_datetime_utc
                or last.capture_datetime_local
                or last.created_at_utc
            )
            next_cursor = self._cursor.encode(
                kind,
                [(_utc(last_value) or datetime.now(timezone.utc)).isoformat(), last.id],
            )
        return included, next_cursor, len(rows) > limit

    def _media_summary(
        self,
        session: Session,
        user_id: int,
        device_id: int,
        asset: MediaAsset,
    ) -> MediaSummary:
        repository = AssetRepository(session)
        selected = repository.preferred_occurrence(
            user_id=user_id, device_id=device_id, asset_id=asset.id
        )
        if selected is None:
            raise NotFoundError("MediaNotFound", "The media asset is not available")
        occurrence, selected_source = selected
        has_remote = bool(
            session.scalar(
                select(
                    func.count(MediaOccurrence.id)
                )
                .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
                .where(
                    MediaOccurrence.user_id == user_id,
                    MediaOccurrence.media_asset_id == asset.id,
                    MediaOccurrence.deletion_state == "Active",
                    MediaSource.user_id == user_id,
                    MediaSource.source_status != "Removed",
                    MediaSource.storage_mode == "Remote",
                )
            )
            or asset.original_s3_object_key
        )
        local_here = bool(
            session.scalar(
                select(func.count(MediaOccurrence.id))
                .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
                .where(
                    MediaOccurrence.user_id == user_id,
                    MediaOccurrence.media_asset_id == asset.id,
                    MediaOccurrence.deletion_state == "Active",
                    MediaOccurrence.availability_state == "Available",
                    MediaSource.user_id == user_id,
                    MediaSource.device_id == device_id,
                    MediaSource.source_status != "Removed",
                )
            )
        )
        availability = (
            "Remote"
            if asset.storage_state == "RemoteAvailable"
            else "LocalOnThisDevice"
            if local_here
            else "Unavailable"
        )
        location = repository.location(user_id=user_id, asset_id=asset.id)
        description = repository.current_description(user_id=user_id, asset_id=asset.id)
        excerpt = None
        if description is not None and description.description:
            excerpt = description.description[:500]
        return MediaSummary(
            media_asset_id=_uuid(asset.public_id),
            content_sha256=asset.content_sha256,
            media_type=asset.media_type,
            mime_type=asset.mime_type,
            byte_size=asset.byte_size,
            display_file_name=occurrence.original_file_name,
            storage_mode="Remote" if has_remote else selected_source.storage_mode,
            storage_state=asset.storage_state,
            availability=availability,
            state=self._user_facing_state(session, user_id, asset),
            captured_at_local=asset.capture_datetime_local,
            captured_at_utc=_utc(asset.capture_datetime_utc),
            time_zone_id=asset.time_zone,
            utc_offset_minutes=asset.utc_offset_minutes,
            capture_time_source=asset.capture_time_source,
            capture_time_confidence=asset.capture_time_confidence,
            duration_ms=asset.duration_milliseconds,
            width_pixels=asset.width_pixels,
            height_pixels=asset.height_pixels,
            category=asset.category,
            location=self._location_record(location),
            description_excerpt=excerpt,
            is_trashed=asset.lifecycle_state == "Trashed",
            purge_after_utc=_utc(asset.purge_after_utc),
            created_at_utc=_utc(asset.created_at_utc) or datetime.now(timezone.utc),
            updated_at_utc=_utc(asset.updated_at_utc) or datetime.now(timezone.utc),
        )

    def _search_hit(
        self,
        *,
        session: Session,
        user_id: int,
        device_id: int,
        asset: MediaAsset,
        search_text: str,
    ) -> MediaSearchHit:
        repository = AssetRepository(session)
        summary = self._media_summary(session, user_id, device_id, asset)
        needle = search_text.casefold()
        matched_field = "FileName"
        highlight: str | None = None
        segment_id: UUID | None = None
        seek_to_ms: int | None = None
        if needle in summary.display_file_name.casefold():
            highlight = summary.display_file_name
        else:
            description = repository.current_description(
                user_id=user_id, asset_id=asset.id
            )
            transcript = repository.current_transcript(
                user_id=user_id, asset_id=asset.id
            )
            location = repository.location(user_id=user_id, asset_id=asset.id)
            if (
                description is not None
                and description.description
                and needle in description.description.casefold()
            ):
                matched_field = "Description"
                highlight = description.description[:1000]
            elif (
                transcript is not None
                and transcript.transcript_text
                and needle in transcript.transcript_text.casefold()
            ):
                matched_field = "Transcript"
                segment = repository.first_matching_segment(
                    user_id=user_id,
                    transcript_id=transcript.id,
                    search_text=search_text,
                )
                if segment is not None:
                    segment_id = _uuid(segment.public_id)
                    seek_to_ms = segment.start_milliseconds
                    highlight = segment.segment_text[:1000]
                else:
                    highlight = transcript.transcript_text[:1000]
            elif asset.category and needle in asset.category.casefold():
                matched_field = "Category"
                highlight = asset.category
            elif needle in asset.media_type.casefold():
                matched_field = "MediaType"
                highlight = asset.media_type
            elif self._location_contains(location, needle):
                matched_field = "Location"
                highlight = self._location_highlight(location, needle)
            else:
                matched_field = "Date"
                highlight = str(
                    asset.capture_datetime_local
                    or asset.capture_datetime_utc
                    or asset.created_at_utc
                )
        return MediaSearchHit(
            asset=summary,
            matched_field=matched_field,
            highlight=highlight,
            transcript_segment_id=segment_id,
            seek_to_ms=seek_to_ms,
        )

    @staticmethod
    def _location_contains(location: MediaLocation | None, needle: str) -> bool:
        if location is None:
            return False
        return any(
            value is not None and needle in value.casefold()
            for value in (
                location.location_display_name,
                location.street_address,
                location.neighborhood,
                location.city,
                location.county,
                location.state,
                location.postal_code,
                location.country,
                location.country_code,
            )
        )

    @staticmethod
    def _location_highlight(
        location: MediaLocation | None, needle: str
    ) -> str | None:
        if location is None:
            return None
        values = (
            location.street_address,
            location.neighborhood,
            location.city,
            location.county,
            location.state,
            location.postal_code,
            location.country,
            location.country_code,
            location.location_display_name,
        )
        return next(
            (
                value
                for value in values
                if value is not None and needle in value.casefold()
            ),
            location.street_address or location.location_display_name,
        )

    @staticmethod
    def _user_facing_state(
        session: Session, user_id: int, asset: MediaAsset
    ) -> str:
        if asset.storage_state in {"UploadPending"}:
            return "Preparing"
        if asset.storage_state == "Uploading":
            return "Uploading"
        statuses = set(
            session.scalars(
                select(ProcessingJob.status).where(
                    ProcessingJob.user_id == user_id,
                    ProcessingJob.media_asset_id == asset.id,
                )
            )
        )
        if "DeferredQuota" in statuses:
            return "WaitingForMonthlyQuota"
        if "Failed" in statuses:
            return "NeedsAttention"
        if "Preparing" in statuses:
            return "Preparing"
        if statuses.intersection({"Queued", "Running"}):
            return "Processing"
        return "Ready"

    @staticmethod
    def _provenance(metadata: dict[str, Any] | None) -> tuple[FieldProvenance, ...]:
        if not metadata or not isinstance(metadata.get("provenance"), list):
            return ()
        values: list[FieldProvenance] = []
        for item in metadata["provenance"]:
            if not isinstance(item, dict) or not item.get("field") or not item.get("source"):
                continue
            confidence = item.get("confidence")
            values.append(
                FieldProvenance(
                    field=str(item["field"]),
                    source=str(item["source"]),
                    confidence=(Decimal(str(confidence)) if confidence is not None else None),
                    processor_version=(
                        str(item["processorVersion"])
                        if item.get("processorVersion") is not None
                        else None
                    ),
                    observed_at_utc=_parse_datetime(item.get("observedAtUtc")),
                )
            )
        return tuple(values)

    def _location_record(
        self, location: MediaLocation | None
    ) -> MediaLocationRecord | None:
        if location is None:
            return None
        provenance = ()
        if location.location_source:
            provenance = (
                FieldProvenance(
                    field="location",
                    source=location.location_source,
                    confidence=location.confidence,
                    processor_version=location.normalization_rule_version,
                    observed_at_utc=_utc(location.provider_updated_at_utc),
                ),
            )
        return MediaLocationRecord(
            latitude=location.latitude,
            longitude=location.longitude,
            altitude_meters=location.altitude_meters,
            horizontal_accuracy_meters=location.accuracy_meters,
            display_name=location.location_display_name,
            street_address=location.street_address,
            neighborhood=location.neighborhood,
            city=location.city,
            county=location.county,
            state=location.state,
            postal_code=location.postal_code,
            country=location.country,
            country_code=location.country_code,
            provenance=provenance,
            original_street_number=location.original_street_number,
            provider=location.provider,
            provider_place_id=location.provider_place_id,
            normalization_rule_version=location.normalization_rule_version,
            provider_updated_at_utc=_utc(location.provider_updated_at_utc),
        )

    @staticmethod
    def _description_record(
        description: MediaDescription | None,
    ) -> DescriptionRecord | None:
        if description is None:
            return None
        return DescriptionRecord(
            status=description.status,
            text=description.description,
            provider=description.provider,
            model=description.model,
            prompt_version=description.prompt_version,
            updated_at_utc=_utc(description.updated_at_utc),
        )

    @staticmethod
    def _transcript_record(
        session: Session,
        user_id: int,
        transcript: MediaTranscript | None,
    ) -> TranscriptRecord | None:
        if transcript is None:
            return None
        segments = session.scalars(
            select(MediaTranscriptSegment)
            .where(
                MediaTranscriptSegment.user_id == user_id,
                MediaTranscriptSegment.media_transcript_id == transcript.id,
            )
            .order_by(MediaTranscriptSegment.sequence_number)
        ).all()
        return TranscriptRecord(
            transcript_id=_uuid(transcript.public_id),
            status=transcript.status,
            language_code=transcript.language_code,
            provider=transcript.provider,
            model=transcript.model,
            provider_request_id=transcript.provider_request_id,
            full_text=transcript.transcript_text,
            duration_ms=transcript.duration_milliseconds,
            segments=tuple(
                TranscriptSegmentRecord(
                    segment_id=_uuid(segment.public_id),
                    index=segment.sequence_number,
                    start_ms=segment.start_milliseconds,
                    end_ms=segment.end_milliseconds,
                    speaker=segment.speaker_label,
                    text=segment.segment_text,
                    confidence=segment.confidence,
                )
                for segment in segments
            ),
            updated_at_utc=_utc(transcript.updated_at_utc),
        )

    @staticmethod
    def _occurrence_record(
        occurrence: MediaOccurrence,
        source: MediaSource,
        *,
        reveal_locator: bool,
    ) -> OccurrenceRecord:
        return OccurrenceRecord(
            occurrence_id=_uuid(occurrence.public_id),
            source_id=_uuid(source.public_id),
            source_item_id=occurrence.source_item_id,
            source_revision=occurrence.source_revision or "",
            exact_file_name=occurrence.original_file_name,
            local_locator=occurrence.local_locator if reveal_locator else None,
            first_seen_at_utc=_utc(occurrence.first_seen_at_utc)
            or datetime.now(timezone.utc),
            last_seen_at_utc=_utc(occurrence.last_seen_at_utc)
            or datetime.now(timezone.utc),
            is_deleted=occurrence.deletion_state != "Active",
            deleted_at_utc=_utc(occurrence.deleted_at_utc),
        )

    def _job_record(
        self, session: Session, user_id: int, job: ProcessingJob
    ) -> JobRecord:
        asset_public_id = session.scalar(
            select(MediaAsset.public_id).where(
                MediaAsset.user_id == user_id,
                MediaAsset.id == job.media_asset_id,
            )
        )
        if asset_public_id is None:
            raise NotFoundError("MediaNotFound", "The job media asset was not found")
        return JobRecord(
            job_id=_uuid(job.public_id),
            media_asset_id=_uuid(asset_public_id),
            job_type=job.job_type,
            status=job.status,
            state={
                "Preparing": "Preparing",
                "Queued": "Processing",
                "Running": "Processing",
                "Succeeded": "Ready",
                "DeferredQuota": "WaitingForMonthlyQuota",
                "Failed": "NeedsAttention",
                "Cancelled": "NeedsAttention",
            }.get(job.status, "NeedsAttention"),
            attempt_count=job.attempt_count,
            next_attempt_at_utc=_utc(job.next_attempt_at_utc),
            failure_class=job.failure_class,
            error_code=job.failure_code,
            user_message=job.failure_message,
            can_retry=(
                job.status == "DeferredQuota"
                or (
                    job.job_type == "Description"
                    and job.status == "Cancelled"
                    and job.failure_code
                    in {
                        "UnsupportedPhoto",
                        "SourceChanged",
                        "SourceUnavailable",
                        "UserSkipped",
                    }
                )
                or (job.status == "Failed" and job.attempt_count < job.max_attempts)
            ),
            created_at_utc=_utc(job.created_at_utc) or datetime.now(timezone.utc),
            started_at_utc=_utc(job.started_at_utc),
            completed_at_utc=_utc(job.completed_at_utc),
            updated_at_utc=_utc(job.updated_at_utc) or datetime.now(timezone.utc),
        )

    @staticmethod
    def _user_record(account: UserAccount) -> UserRecord:
        return UserRecord(
            user_id=_uuid(account.public_id),
            email=account.email,
            display_name=account.display_name,
            created_at_utc=_utc(account.created_at_utc) or datetime.now(timezone.utc),
        )

    @staticmethod
    def _device_record(device: Device) -> DeviceRecord:
        registered = _utc(device.created_at_utc) or datetime.now(timezone.utc)
        return DeviceRecord(
            device_id=_uuid(device.public_id),
            installation_id=_uuid(device.device_key),
            platform=device.platform,
            display_name=device.display_name,
            app_version=device.app_version or "",
            os_version=device.operating_system_version or "",
            status="Removed" if device.retired_at_utc is not None else "Active",
            registered_at_utc=registered,
            last_seen_at_utc=_utc(device.last_activity_at_utc) or registered,
        )

    @staticmethod
    def _source_record(source: MediaSource, device: Device) -> SourceRecord:
        return SourceRecord(
            source_id=_uuid(source.public_id),
            device_id=_uuid(device.public_id),
            source_key=source.source_key,
            source_type=source.source_type,
            display_name=source.display_name,
            storage_mode=source.storage_mode,
            permission_state=source.permission_state,
            status=source.source_status,
            sync_settings=SyncSettings.from_json(source.sync_policy_json),
            last_manifest_at_utc=_utc(source.last_manifest_at_utc),
            created_at_utc=_utc(source.created_at_utc) or datetime.now(timezone.utc),
            updated_at_utc=_utc(source.updated_at_utc) or datetime.now(timezone.utc),
        )

    def _simple_page(
        self,
        rows: Sequence[Any],
        *,
        limit: int,
        kind: str,
        convert: Callable[[Any], T],
    ) -> Page[T]:
        included = rows[:limit]
        return Page(
            items=tuple(convert(item) for item in included),
            has_more=len(rows) > limit,
            next_cursor=(
                self._cursor.encode(kind, [included[-1].id])
                if len(rows) > limit and included
                else None
            ),
        )

    @staticmethod
    def _device_from_json(value: Mapping[str, Any]) -> DeviceRecord:
        return DeviceRecord(
            device_id=UUID(str(value["device_id"])),
            installation_id=UUID(str(value["installation_id"])),
            platform=str(value["platform"]),
            display_name=str(value["display_name"]),
            app_version=str(value["app_version"]),
            os_version=str(value["os_version"]),
            status="Removed" if value["status"] == "Removed" else "Active",
            registered_at_utc=_parse_datetime(value["registered_at_utc"])
            or datetime.now(timezone.utc),
            last_seen_at_utc=_parse_datetime(value["last_seen_at_utc"])
            or datetime.now(timezone.utc),
        )

    @staticmethod
    def _source_from_json(value: Mapping[str, Any]) -> SourceRecord:
        settings = value.get("sync_settings")
        if not isinstance(settings, dict):
            settings = {}
        return SourceRecord(
            source_id=UUID(str(value["source_id"])),
            device_id=UUID(str(value["device_id"])),
            source_key=str(value["source_key"]),
            source_type=str(value["source_type"]),
            display_name=str(value["display_name"]),
            storage_mode=str(value["storage_mode"]),
            permission_state=str(value["permission_state"]),
            status=str(value["status"]),
            sync_settings=SyncSettings(
                automatic_sync=bool(settings.get("automatic_sync", True)),
                network_policy=str(settings.get("network_policy", "WiFiOnly")),
                require_charging_for_historical_upload=bool(
                    settings.get("require_charging_for_historical_upload", True)
                ),
            ),
            last_manifest_at_utc=_parse_datetime(value.get("last_manifest_at_utc")),
            created_at_utc=_parse_datetime(value["created_at_utc"])
            or datetime.now(timezone.utc),
            updated_at_utc=_parse_datetime(value["updated_at_utc"])
            or datetime.now(timezone.utc),
        )

    @staticmethod
    def _manifest_from_json(value: Mapping[str, Any]) -> ManifestResult:
        count_values = value.get("counts")
        if not isinstance(count_values, dict):
            count_values = {}
        result_values = value.get("results")
        if not isinstance(result_values, list):
            result_values = []
        results: list[ManifestEntryResult] = []
        for item in result_values:
            if not isinstance(item, dict):
                continue
            occurrence_id = item.get("occurrence_id")
            asset_id = item.get("media_asset_id")
            description_job_id = item.get("description_job_id")
            results.append(
                ManifestEntryResult(
                    source_item_id=str(item["source_item_id"]),
                    outcome=str(item["outcome"]),
                    occurrence_id=(UUID(str(occurrence_id)) if occurrence_id else None),
                    media_asset_id=(UUID(str(asset_id)) if asset_id else None),
                    upload_required=bool(item.get("upload_required", False)),
                    description_job_id=(
                        UUID(str(description_job_id)) if description_job_id else None
                    ),
                    error_code=(
                        str(item["error_code"])
                        if item.get("error_code") is not None
                        else None
                    ),
                    error_message=(
                        str(item["error_message"])
                        if item.get("error_message") is not None
                        else None
                    ),
                )
            )
        return ManifestResult(
            source_id=UUID(str(value["source_id"])),
            accepted_at_utc=_parse_datetime(value["accepted_at_utc"])
            or datetime.now(timezone.utc),
            source_cursor=(
                str(value["source_cursor"])
                if value.get("source_cursor") is not None
                else None
            ),
            counts=ManifestCounts(
                created=int(count_values.get("created", 0)),
                updated=int(count_values.get("updated", 0)),
                duplicates_linked=int(count_values.get("duplicates_linked", 0)),
                deleted=int(count_values.get("deleted", 0)),
                ignored_deletions=int(count_values.get("ignored_deletions", 0)),
                unchanged=int(count_values.get("unchanged", 0)),
                rejected=int(count_values.get("rejected", 0)),
            ),
            results=tuple(results),
        )

    @staticmethod
    def _enrichment_preparation_from_json(
        value: Mapping[str, Any],
    ) -> EnrichmentPreparation:
        task_values = value.get("scene_description_tasks")
        if not isinstance(task_values, list):
            task_values = []
        tasks = tuple(
            SceneDescriptionTaskRecord(
                job_id=UUID(str(item["job_id"])),
                media_asset_id=UUID(str(item["media_asset_id"])),
                occurrence_id=UUID(str(item["occurrence_id"])),
                source_item_id=str(item["source_item_id"]),
                local_locator=str(item["local_locator"]),
                asset_content_sha256=str(item["asset_content_sha256"]),
                file_name=str(item["file_name"]),
            )
            for item in task_values
            if isinstance(item, Mapping)
        )
        return EnrichmentPreparation(
            source_id=UUID(str(value["source_id"])),
            geocode_jobs_queued=int(value.get("geocode_jobs_queued", 0)),
            description_jobs_prepared=int(
                value.get("description_jobs_prepared", 0)
            ),
            scene_description_tasks=tasks,
        )

    @staticmethod
    def _upload_plan_from_json(value: Mapping[str, Any]) -> UploadPlanRecord:
        single_value = value.get("single_part")
        single_part = None
        if isinstance(single_value, dict):
            headers = single_value.get("headers")
            single_part = SignedUploadRequestRecord(
                url=str(single_value["url"]),
                method="PUT",
                headers=(
                    {str(key): str(item) for key, item in headers.items()}
                    if isinstance(headers, dict)
                    else {}
                ),
                expires_at_utc=_parse_datetime(single_value["expires_at_utc"])
                or datetime.now(timezone.utc),
            )
        session_id = value.get("upload_session_id")
        return UploadPlanRecord(
            disposition=str(value["disposition"]),
            strategy=str(value["strategy"]),
            media_asset_id=UUID(str(value["media_asset_id"])),
            occurrence_id=UUID(str(value["occurrence_id"])),
            upload_session_id=(UUID(str(session_id)) if session_id else None),
            expires_at_utc=_parse_datetime(value.get("expires_at_utc")),
            deduplicated=bool(value.get("deduplicated", False)),
            retry_after_seconds=(
                int(value["retry_after_seconds"])
                if value.get("retry_after_seconds") is not None
                else None
            ),
            single_part=single_part,
        )

    @staticmethod
    def _upload_complete_from_json(
        value: Mapping[str, Any]
    ) -> UploadCompleteRecord:
        jobs = value.get("processing_jobs")
        if not isinstance(jobs, list):
            jobs = []
        return UploadCompleteRecord(
            media_asset_id=UUID(str(value["media_asset_id"])),
            storage_state=str(value["storage_state"]),
            processing_jobs=tuple(UUID(str(item)) for item in jobs),
        )

    @staticmethod
    def _job_from_json(value: Mapping[str, Any]) -> JobRecord:
        return JobRecord(
            job_id=UUID(str(value["job_id"])),
            media_asset_id=UUID(str(value["media_asset_id"])),
            job_type=str(value["job_type"]),
            status=str(value["status"]),
            state=str(value["state"]),
            attempt_count=int(value["attempt_count"]),
            next_attempt_at_utc=_parse_datetime(value.get("next_attempt_at_utc")),
            failure_class=(
                str(value["failure_class"])
                if value.get("failure_class") is not None
                else None
            ),
            error_code=(
                str(value["error_code"]) if value.get("error_code") is not None else None
            ),
            user_message=(
                str(value["user_message"])
                if value.get("user_message") is not None
                else None
            ),
            can_retry=bool(value.get("can_retry", False)),
            created_at_utc=_parse_datetime(value["created_at_utc"])
            or datetime.now(timezone.utc),
            started_at_utc=_parse_datetime(value.get("started_at_utc")),
            completed_at_utc=_parse_datetime(value.get("completed_at_utc")),
            updated_at_utc=_parse_datetime(value["updated_at_utc"])
            or datetime.now(timezone.utc),
        )
