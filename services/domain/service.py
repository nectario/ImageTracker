from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from typing import Any, Callable, Mapping, Sequence, TypeVar
from uuid import UUID

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

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
    FieldProvenance,
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
    SourceCreate,
    SourceRecord,
    SourceUpdate,
    SyncSettings,
    TranscriptRecord,
    TranscriptSegmentRecord,
    UserRecord,
)
from services.domain.repositories import (
    AccountRepository,
    AssetRepository,
    ChangeRepository,
    DeviceRepository,
    IdempotencyRepository,
    JobRepository,
    OccurrenceRepository,
    SourceRepository,
)


T = TypeVar("T")
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
    ) -> None:
        self._session_factory = session_factory
        self._cursor = cursor_codec or CursorCodec()
        self._clock = clock
        self._trash_retention = timedelta(days=trash_retention_days)
        self._idempotency_retention = timedelta(hours=idempotency_hours)

    def _now(self) -> datetime:
        return _db_datetime(self._clock()) or utc_now()

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
                                )
                            else:
                                result = self._manifest_upsert(
                                    session=session,
                                    account=account,
                                    source=source,
                                    entry=entry,
                                    now=now,
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
                )
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

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._manifest_from_json,
            )

    def _manifest_upsert(
        self,
        *,
        session: Session,
        account: UserAccount,
        source: MediaSource,
        entry: ManifestUpsert,
        now: datetime,
    ) -> ManifestEntryResult:
        if entry.byte_size <= 0:
            raise ConflictError("InvalidByteSize", "Media byte size must be positive")
        occurrence_repository = OccurrenceRepository(session)
        occurrence = occurrence_repository.by_source_item(
            user_id=account.id,
            source_id=source.id,
            source_item_id=entry.source_item_id,
        )
        occurrence_created = occurrence is None
        old_asset_id = occurrence.media_asset_id if occurrence is not None else None
        was_deleted = occurrence is not None and occurrence.deletion_state != "Active"

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
            )

        content_hash = entry.content_sha256.lower()
        if not HEX_SHA256.fullmatch(content_hash):
            raise ConflictError(
                "InvalidContentHash", "Content SHA-256 must contain 64 hexadecimal characters"
            )
        asset_repository = AssetRepository(session)
        asset = asset_repository.by_hash(user_id=account.id, sha256=content_hash)
        asset_created = asset is None
        if asset is not None and asset.byte_size != entry.byte_size:
            raise ConflictError(
                "ContentHashMetadataMismatch",
                "The content hash is already associated with a different byte size",
            )
        if asset is None:
            capture_source, capture_confidence = self._capture_provenance(entry)
            asset = MediaAsset(
                user_id=account.id,
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
            session.add(asset)
            session.flush()
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
        session.flush()

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
    ) -> ManifestEntryResult:
        occurrence = OccurrenceRepository(session).by_source_item(
            user_id=account.id,
            source_id=source.id,
            source_item_id=entry.source_item_id,
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
        location = session.scalar(
            select(MediaLocation).where(
                MediaLocation.user_id == account.id,
                MediaLocation.media_asset_id == asset.id,
            )
        )
        source = next(
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
        location.latitude = entry.location.latitude
        location.longitude = entry.location.longitude
        location.altitude_meters = entry.location.altitude_meters
        location.accuracy_meters = entry.location.horizontal_accuracy_meters
        location.location_source = source
        location.updated_at_utc = now
        session.flush()

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
        with transaction_scope(self._session_factory) as session:
            account = self._account(session, user_id)

            def action() -> tuple[JobRecord, int]:
                job = JobRepository(session).require(
                    user_id=account.id, job_public_id=job_id
                )
                if job.status != "Failed":
                    raise RetryNotAllowedError(
                        "JobRetryNotAllowed", "Only failed jobs can be manually retried"
                    )
                if job.attempt_count >= job.max_attempts:
                    raise RetryNotAllowedError(
                        "JobAttemptsExhausted",
                        "This job has exhausted its configured retry attempts",
                    )
                now = self._now()
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
                    asset_id=job.media_asset_id,
                    entity_type="ProcessingJob",
                    entity_id=job.id,
                    entity_public_id=job.public_id,
                    change_type="Upsert",
                    now=now,
                )
                return self._job_record(session, account.id, job), 202

            return self._mutation(
                session=session,
                account=account,
                context=context,
                action=action,
                replay_decoder=self._job_from_json,
            )

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
                highlight = (
                    location.location_display_name
                    or location.street_address
                    or location.city
                    if location is not None
                    else None
                )
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
            can_retry=(job.status == "Failed" and job.attempt_count < job.max_attempts),
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
            results.append(
                ManifestEntryResult(
                    source_item_id=str(item["source_item_id"]),
                    outcome=str(item["outcome"]),
                    occurrence_id=(UUID(str(occurrence_id)) if occurrence_id else None),
                    media_asset_id=(UUID(str(asset_id)) if asset_id else None),
                    upload_required=bool(item.get("upload_required", False)),
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
