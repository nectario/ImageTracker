from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
import math
from typing import Any
from uuid import UUID

from sqlalchemy import String, and_, case, cast, exists, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from services.data.models import (
    Device,
    IdempotencyRecord,
    MediaAsset,
    MediaChange,
    MediaDescription,
    MediaLocation,
    MediaOccurrence,
    MediaSource,
    MediaTranscript,
    MediaTranscriptSegment,
    ProcessingJob,
    ProviderUsageMonth,
    UploadSession,
    UserAccount,
)
from services.domain.errors import ConflictError, NotFoundError


def public_id(value: UUID | str) -> str:
    return str(value)


def _haversine_meters(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    earth_radius_meters = 6_371_008.8
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = math.radians(latitude_b - latitude_a)
    delta_lambda = math.radians(longitude_b - longitude_a)
    value = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi_a)
        * math.cos(phi_b)
        * math.sin(delta_lambda / 2.0) ** 2
    )
    return earth_radius_meters * 2.0 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1.0 - value))
    )


class AccountRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def by_cognito_subject(
        self, subject: str, *, for_update: bool = False
    ) -> UserAccount | None:
        statement = select(UserAccount).where(
            UserAccount.cognito_subject == subject
        )
        if for_update:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def require_by_public_id(self, user_public_id: UUID | str) -> UserAccount:
        account = self.session.scalar(
            select(UserAccount).where(
                UserAccount.public_id == public_id(user_public_id),
                UserAccount.account_status == "Active",
                UserAccount.deleted_at_utc.is_(None),
            )
        )
        if account is None:
            raise NotFoundError("UserNotFound", "The ImageTracker account was not found")
        return account

    def bootstrap(
        self,
        *,
        cognito_subject: str,
        email: str | None,
        display_name: str | None,
        now: datetime,
    ) -> tuple[UserAccount, bool]:
        account = self.by_cognito_subject(cognito_subject)
        created = False
        if account is None:
            candidate = UserAccount(
                cognito_subject=cognito_subject,
                email=email,
                display_name=display_name,
                account_status="Active",
                last_sign_in_at_utc=now,
                created_at_utc=now,
                updated_at_utc=now,
            )
            try:
                # The candidate is added inside the savepoint because
                # begin_nested() flushes pre-existing pending state first.
                # A concurrent winner therefore rolls back only this insert,
                # never the outer request transaction.
                with self.session.begin_nested():
                    self.session.add(candidate)
                    self.session.flush()
                account = candidate
                created = True
            except IntegrityError:
                # A locking read sees the just-committed winner even under
                # MySQL's default REPEATABLE READ transaction isolation.
                account = self.by_cognito_subject(
                    cognito_subject, for_update=True
                )
                if account is None:
                    raise

        if account.deleted_at_utc is not None or account.account_status != "Active":
            raise ConflictError(
                "AccountInactive", "The ImageTracker account is not active"
            )
        changed = False
        if email is not None and email != account.email:
            account.email = email
            changed = True
        if display_name is not None and display_name != account.display_name:
            account.display_name = display_name
            changed = True
        if (
            account.last_sign_in_at_utc is None
            or now - account.last_sign_in_at_utc >= timedelta(hours=24)
        ):
            account.last_sign_in_at_utc = now
            changed = True
        if changed:
            account.updated_at_utc = now
        self.session.flush()
        return account, created


class DeviceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def require(
        self,
        *,
        user_id: int,
        device_public_id: UUID | str,
        include_retired: bool = False,
    ) -> Device:
        conditions = [
            Device.user_id == user_id,
            Device.public_id == public_id(device_public_id),
        ]
        if not include_retired:
            conditions.append(Device.retired_at_utc.is_(None))
        device = self.session.scalar(select(Device).where(*conditions))
        if device is None:
            raise NotFoundError("DeviceNotFound", "The device was not found")
        return device

    def register(
        self,
        *,
        user_id: int,
        device_key: str,
        display_name: str,
        platform: str,
        app_version: str,
        os_version: str,
        now: datetime,
    ) -> tuple[Device, bool]:
        device = self.session.scalar(
            select(Device).where(
                Device.user_id == user_id,
                Device.device_key == device_key,
            )
        )
        created = device is None
        if device is None:
            device = Device(
                user_id=user_id,
                device_key=device_key,
                display_name=display_name,
                platform=platform,
                app_version=app_version,
                operating_system_version=os_version,
                last_activity_at_utc=now,
                created_at_utc=now,
                updated_at_utc=now,
            )
            self.session.add(device)
        else:
            device.display_name = display_name
            device.platform = platform
            device.app_version = app_version
            device.operating_system_version = os_version
            device.last_activity_at_utc = now
            device.retired_at_utc = None
            device.updated_at_utc = now
        self.session.flush()
        return device, created

    def list_after(
        self, *, user_id: int, after_id: int | None, limit: int
    ) -> list[Device]:
        statement = select(Device).where(Device.user_id == user_id)
        if after_id is not None:
            statement = statement.where(Device.id < after_id)
        return list(
            self.session.scalars(statement.order_by(Device.id.desc()).limit(limit))
        )


class SourceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def require(
        self,
        *,
        user_id: int,
        source_public_id: UUID | str,
        include_removed: bool = False,
    ) -> tuple[MediaSource, str]:
        conditions = [
            MediaSource.user_id == user_id,
            MediaSource.public_id == public_id(source_public_id),
        ]
        if not include_removed:
            conditions.append(MediaSource.source_status != "Removed")
        source = self.session.scalar(select(MediaSource).where(*conditions))
        if source is None:
            raise NotFoundError("SourceNotFound", "The media source was not found")
        return source

    def create(
        self,
        *,
        user_id: int,
        device: Device,
        source_key: str,
        source_type: str,
        display_name: str,
        storage_mode: str,
        permission_state: str,
        sync_policy_json: dict[str, object],
        now: datetime,
    ) -> MediaSource:
        existing = self.session.scalar(
            select(MediaSource).where(
                MediaSource.user_id == user_id,
                MediaSource.device_id == device.id,
                MediaSource.source_key == source_key,
            )
        )
        if existing is not None:
            requested_policy = sync_policy_json or {
                "automaticSync": True,
                "networkPolicy": "WiFiOnly",
                "requireChargingForHistoricalUpload": True,
            }
            existing_policy = existing.sync_policy_json or {
                "automaticSync": True,
                "networkPolicy": "WiFiOnly",
                "requireChargingForHistoricalUpload": True,
            }
            compatible = (
                existing.source_type == source_type
                and existing.display_name == display_name
                and existing.storage_mode == storage_mode
                and existing.permission_state == permission_state
                and existing_policy == requested_policy
            )
            if existing.source_status == "Removed":
                existing.source_type = source_type
                existing.display_name = display_name
                existing.storage_mode = storage_mode
                existing.permission_state = permission_state
                existing.sync_policy_json = requested_policy
                existing.source_status = "Active"
                existing.removed_at_utc = None
                existing.updated_at_utc = now
                self.session.flush()
                return existing, "Reactivated"
            if existing.source_status == "Active" and compatible:
                return existing, "Existing"
            raise ConflictError(
                "SourceAlreadyExists",
                "An incompatible source with this key is already registered on the device",
            )
        source = MediaSource(
            user_id=user_id,
            device_id=device.id,
            source_key=source_key,
            source_type=source_type,
            display_name=display_name,
            storage_mode=storage_mode,
            permission_state=permission_state,
            source_status="Active",
            sync_policy_json=sync_policy_json,
            created_at_utc=now,
            updated_at_utc=now,
        )
        self.session.add(source)
        self.session.flush()
        return source, "Created"

    def list_after(
        self, *, user_id: int, after_id: int | None, limit: int
    ) -> list[MediaSource]:
        statement = select(MediaSource).where(MediaSource.user_id == user_id)
        if after_id is not None:
            statement = statement.where(MediaSource.id < after_id)
        return list(
            self.session.scalars(statement.order_by(MediaSource.id.desc()).limit(limit))
        )

    def device_for(self, *, user_id: int, source: MediaSource) -> Device:
        device = self.session.scalar(
            select(Device).where(
                Device.user_id == user_id,
                Device.id == source.device_id,
            )
        )
        if device is None:
            raise NotFoundError("DeviceNotFound", "The source device was not found")
        return device


class OccurrenceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def by_source_item(
        self, *, user_id: int, source_id: int, source_item_id: str
    ) -> MediaOccurrence | None:
        return self.session.scalar(
            select(MediaOccurrence).where(
                MediaOccurrence.user_id == user_id,
                MediaOccurrence.media_source_id == source_id,
                MediaOccurrence.source_item_id == source_item_id,
            )
        )

    def by_source_items(
        self,
        *,
        user_id: int,
        source_id: int,
        source_item_ids: set[str],
    ) -> dict[str, MediaOccurrence]:
        """Prefetch a manifest batch without one SELECT per entry."""

        if not source_item_ids:
            return {}
        rows = self.session.scalars(
            select(MediaOccurrence).where(
                MediaOccurrence.user_id == user_id,
                MediaOccurrence.media_source_id == source_id,
                MediaOccurrence.source_item_id.in_(source_item_ids),
            )
        )
        return {row.source_item_id: row for row in rows}

    def require(
        self,
        *,
        user_id: int,
        occurrence_public_id: UUID | str,
        source_id: int | None = None,
    ) -> MediaOccurrence:
        conditions = [
            MediaOccurrence.user_id == user_id,
            MediaOccurrence.public_id == public_id(occurrence_public_id),
        ]
        if source_id is not None:
            conditions.append(MediaOccurrence.media_source_id == source_id)
        occurrence = self.session.scalar(select(MediaOccurrence).where(*conditions))
        if occurrence is None:
            raise NotFoundError(
                "OccurrenceNotFound", "The media occurrence was not found"
            )
        return occurrence

    def active_count_for_asset(
        self, *, user_id: int, asset_id: int, excluding_id: int | None = None
    ) -> int:
        statement = select(func.count()).select_from(MediaOccurrence).where(
            MediaOccurrence.user_id == user_id,
            MediaOccurrence.media_asset_id == asset_id,
            MediaOccurrence.deletion_state == "Active",
        )
        if excluding_id is not None:
            statement = statement.where(MediaOccurrence.id != excluding_id)
        return int(self.session.scalar(statement) or 0)

    def for_asset(self, *, user_id: int, asset_id: int) -> list[MediaOccurrence]:
        return list(
            self.session.scalars(
                select(MediaOccurrence)
                .where(
                    MediaOccurrence.user_id == user_id,
                    MediaOccurrence.media_asset_id == asset_id,
                )
                .order_by(MediaOccurrence.id.desc())
            )
        )


class AssetRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def by_hash(self, *, user_id: int, sha256: str) -> MediaAsset | None:
        return self.session.scalar(
            select(MediaAsset).where(
                MediaAsset.user_id == user_id,
                MediaAsset.content_sha256 == sha256,
            )
        )

    def by_hashes(
        self,
        *,
        user_id: int,
        sha256_values: set[str],
    ) -> dict[str, MediaAsset]:
        """Prefetch exact-duplicate candidates for an entire manifest batch."""

        if not sha256_values:
            return {}
        rows = self.session.scalars(
            select(MediaAsset).where(
                MediaAsset.user_id == user_id,
                MediaAsset.content_sha256.in_(sha256_values),
            )
        )
        return {row.content_sha256: row for row in rows}

    def require(self, *, user_id: int, asset_public_id: UUID | str) -> MediaAsset:
        asset = self.session.scalar(
            select(MediaAsset).where(
                MediaAsset.user_id == user_id,
                MediaAsset.public_id == public_id(asset_public_id),
            )
        )
        if asset is None:
            raise NotFoundError("MediaNotFound", "The media asset was not found")
        return asset

    @staticmethod
    def visible_expression(
        *, user_id: int, device_id: int, include_trashed_occurrences: bool = False
    ) -> Any:
        occurrence_state = and_(
            MediaOccurrence.deletion_state == "Active",
            MediaOccurrence.availability_state != "Unavailable",
        )
        source_state = MediaSource.source_status != "Removed"
        if include_trashed_occurrences:
            occurrence_state = or_(
                occurrence_state,
                and_(
                    MediaAsset.lifecycle_state == "Trashed",
                    MediaOccurrence.deletion_state == "Deleted",
                ),
            )
            source_state = or_(
                source_state,
                MediaAsset.lifecycle_state == "Trashed",
            )
        return exists(
            select(MediaOccurrence.id)
            .join(
                MediaSource,
                and_(
                    MediaSource.user_id == MediaOccurrence.user_id,
                    MediaSource.id == MediaOccurrence.media_source_id,
                ),
            )
            .where(
                MediaOccurrence.user_id == user_id,
                MediaOccurrence.media_asset_id == MediaAsset.id,
                source_state,
                occurrence_state,
                or_(
                    MediaSource.storage_mode == "Remote",
                    MediaSource.device_id == device_id,
                ),
            )
        )

    def is_visible(self, *, user_id: int, device_id: int, asset_id: int) -> bool:
        return bool(
            self.session.scalar(
                select(exists().where(
                    MediaAsset.id == asset_id,
                    MediaAsset.user_id == user_id,
                    self.visible_expression(user_id=user_id, device_id=device_id),
                ))
            )
        )

    def query_media(
        self,
        *,
        user_id: int,
        device_id: int,
        source_internal_id: int | None,
        media_type: str | None,
        storage_mode: str | None,
        captured_after_utc: datetime | None,
        captured_before_utc: datetime | None,
        category: str | None,
        has_location: bool | None,
        trash_state: str,
        search_text: str | None = None,
    ) -> Any:
        include_trashed = trash_state in {"Trashed", "All"}
        statement = select(MediaAsset).where(
            MediaAsset.user_id == user_id,
            self.visible_expression(
                user_id=user_id,
                device_id=device_id,
                include_trashed_occurrences=include_trashed,
            ),
        )
        occurrence_filter = exists(
            select(MediaOccurrence.id)
            .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
            .where(
                MediaOccurrence.user_id == user_id,
                MediaOccurrence.media_asset_id == MediaAsset.id,
            )
        )
        if source_internal_id is not None:
            occurrence_filter = exists(
                select(MediaOccurrence.id).where(
                    MediaOccurrence.user_id == user_id,
                    MediaOccurrence.media_asset_id == MediaAsset.id,
                    MediaOccurrence.media_source_id == source_internal_id,
                )
            )
            statement = statement.where(occurrence_filter)
        if storage_mode is not None:
            statement = statement.where(
                exists(
                    select(MediaOccurrence.id)
                    .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
                    .where(
                        MediaOccurrence.user_id == user_id,
                        MediaOccurrence.media_asset_id == MediaAsset.id,
                        MediaSource.storage_mode == storage_mode,
                    )
                )
            )
        if media_type is not None:
            statement = statement.where(MediaAsset.media_type == media_type)
        if captured_after_utc is not None:
            statement = statement.where(
                MediaAsset.capture_datetime_utc >= captured_after_utc
            )
        if captured_before_utc is not None:
            statement = statement.where(
                MediaAsset.capture_datetime_utc < captured_before_utc
            )
        if category is not None:
            statement = statement.where(MediaAsset.category == category)
        location_exists = exists(
            select(MediaLocation.id).where(
                MediaLocation.user_id == user_id,
                MediaLocation.media_asset_id == MediaAsset.id,
                MediaLocation.latitude.is_not(None),
                MediaLocation.longitude.is_not(None),
            )
        )
        if has_location is True:
            statement = statement.where(location_exists)
        elif has_location is False:
            statement = statement.where(~location_exists)
        if trash_state == "Active":
            statement = statement.where(MediaAsset.lifecycle_state == "Active")
        elif trash_state == "Trashed":
            statement = statement.where(MediaAsset.lifecycle_state == "Trashed")
        elif trash_state != "All":
            raise ConflictError("InvalidTrashState", "The trash filter is invalid")
        if search_text:
            pattern = f"%{search_text.lower()}%"
            file_match = exists(
                select(MediaOccurrence.id).where(
                    MediaOccurrence.user_id == user_id,
                    MediaOccurrence.media_asset_id == MediaAsset.id,
                    func.lower(MediaOccurrence.original_file_name).like(pattern),
                )
            )
            description_match = exists(
                select(MediaDescription.id).where(
                    MediaDescription.user_id == user_id,
                    MediaDescription.media_asset_id == MediaAsset.id,
                    MediaDescription.is_current == 1,
                    func.lower(MediaDescription.description).like(pattern),
                )
            )
            transcript_match = exists(
                select(MediaTranscript.id).where(
                    MediaTranscript.user_id == user_id,
                    MediaTranscript.media_asset_id == MediaAsset.id,
                    MediaTranscript.is_current == 1,
                    func.lower(MediaTranscript.transcript_text).like(pattern),
                )
            )
            location_match = exists(
                select(MediaLocation.id).where(
                    MediaLocation.user_id == user_id,
                    MediaLocation.media_asset_id == MediaAsset.id,
                    or_(
                        func.lower(MediaLocation.location_display_name).like(pattern),
                        func.lower(MediaLocation.street_address).like(pattern),
                        func.lower(MediaLocation.neighborhood).like(pattern),
                        func.lower(MediaLocation.city).like(pattern),
                        func.lower(MediaLocation.county).like(pattern),
                        func.lower(MediaLocation.state).like(pattern),
                        func.lower(MediaLocation.postal_code).like(pattern),
                        func.lower(MediaLocation.country).like(pattern),
                        func.lower(MediaLocation.country_code).like(pattern),
                    ),
                )
            )
            statement = statement.where(
                or_(
                    file_match,
                    description_match,
                    transcript_match,
                    func.lower(MediaAsset.category).like(pattern),
                    func.lower(MediaAsset.media_type).like(pattern),
                    func.lower(cast(MediaAsset.capture_datetime_local, String)).like(pattern),
                    func.lower(cast(MediaAsset.capture_datetime_utc, String)).like(pattern),
                    location_match,
                )
            )
        return statement

    def preferred_occurrence(
        self, *, user_id: int, device_id: int, asset_id: int
    ) -> tuple[MediaOccurrence, MediaSource] | None:
        return self.session.execute(
            select(MediaOccurrence, MediaSource)
            .join(MediaSource, MediaSource.id == MediaOccurrence.media_source_id)
            .join(
                MediaAsset,
                and_(
                    MediaAsset.user_id == MediaOccurrence.user_id,
                    MediaAsset.id == MediaOccurrence.media_asset_id,
                ),
            )
            .where(
                MediaOccurrence.user_id == user_id,
                MediaOccurrence.media_asset_id == asset_id,
                MediaSource.user_id == user_id,
                or_(
                    MediaSource.source_status != "Removed",
                    MediaAsset.lifecycle_state == "Trashed",
                ),
                or_(
                    and_(
                        MediaOccurrence.deletion_state == "Active",
                        MediaOccurrence.availability_state != "Unavailable",
                    ),
                    and_(
                        MediaAsset.lifecycle_state == "Trashed",
                        MediaOccurrence.deletion_state == "Deleted",
                    ),
                ),
                or_(MediaSource.storage_mode == "Remote", MediaSource.device_id == device_id),
            )
            .order_by(
                case((MediaSource.device_id == device_id, 1), else_=0).desc(),
                case((MediaOccurrence.deletion_state == "Active", 1), else_=0).desc(),
                MediaOccurrence.id.desc(),
            )
            .limit(1)
        ).first()

    def location(self, *, user_id: int, asset_id: int) -> MediaLocation | None:
        return self.session.scalar(
            select(MediaLocation).where(
                MediaLocation.user_id == user_id,
                MediaLocation.media_asset_id == asset_id,
            )
        )

    def current_description(
        self, *, user_id: int, asset_id: int
    ) -> MediaDescription | None:
        return self.session.scalar(
            select(MediaDescription)
            .where(
                MediaDescription.user_id == user_id,
                MediaDescription.media_asset_id == asset_id,
                MediaDescription.is_current == 1,
            )
            .order_by(MediaDescription.id.desc())
            .limit(1)
        )

    def current_descriptions(
        self, *, user_id: int, asset_ids: set[int]
    ) -> dict[int, MediaDescription]:
        """Prefetch current descriptions for a manifest batch."""

        if not asset_ids:
            return {}
        rows = self.session.scalars(
            select(MediaDescription)
            .where(
                MediaDescription.user_id == user_id,
                MediaDescription.media_asset_id.in_(asset_ids),
                MediaDescription.is_current == 1,
            )
            .order_by(MediaDescription.id.desc())
        )
        descriptions: dict[int, MediaDescription] = {}
        for row in rows:
            descriptions.setdefault(row.media_asset_id, row)
        return descriptions

    def current_transcript(
        self, *, user_id: int, asset_id: int
    ) -> MediaTranscript | None:
        return self.session.scalar(
            select(MediaTranscript)
            .where(
                MediaTranscript.user_id == user_id,
                MediaTranscript.media_asset_id == asset_id,
                MediaTranscript.is_current == 1,
            )
            .order_by(MediaTranscript.id.desc())
            .limit(1)
        )

    def first_matching_segment(
        self, *, user_id: int, transcript_id: int, search_text: str
    ) -> MediaTranscriptSegment | None:
        return self.session.scalar(
            select(MediaTranscriptSegment)
            .where(
                MediaTranscriptSegment.user_id == user_id,
                MediaTranscriptSegment.media_transcript_id == transcript_id,
                func.lower(MediaTranscriptSegment.segment_text).like(
                    f"%{search_text.lower()}%"
                ),
            )
            .order_by(MediaTranscriptSegment.sequence_number)
            .limit(1)
        )


class ChangeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(
        self,
        *,
        user_id: int,
        entity_type: str,
        entity_id: int,
        entity_public_id: str,
        change_type: str,
        now: datetime,
        device_id: int | None = None,
        source_id: int | None = None,
        asset_id: int | None = None,
        occurrence_id: int | None = None,
        data: dict[str, Any] | None = None,
        flush: bool = True,
    ) -> MediaChange:
        change = MediaChange(
            user_id=user_id,
            device_id=device_id,
            media_source_id=source_id,
            media_asset_id=asset_id,
            media_occurrence_id=occurrence_id,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_public_id=entity_public_id,
            change_type=change_type,
            change_data_json=data,
            created_at_utc=now,
        )
        self.session.add(change)
        if flush:
            self.session.flush()
        return change

    def list_after(
        self,
        *,
        user_id: int,
        device_id: int,
        after_id: int,
        limit: int,
    ) -> list[MediaChange]:
        visible_asset = AssetRepository.visible_expression(
            user_id=user_id, device_id=device_id
        )
        statement = (
            select(MediaChange)
            .outerjoin(
                MediaAsset,
                and_(
                    MediaAsset.user_id == MediaChange.user_id,
                    MediaAsset.id == MediaChange.media_asset_id,
                ),
            )
            .where(
                MediaChange.user_id == user_id,
                MediaChange.id > after_id,
                or_(
                    MediaChange.media_asset_id.is_(None),
                    MediaChange.device_id == device_id,
                    and_(MediaAsset.id.is_not(None), visible_asset),
                ),
            )
            .order_by(MediaChange.id.asc())
            .limit(limit)
        )
        return list(self.session.scalars(statement))


class LocationRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def by_asset(self, *, user_id: int, asset_id: int) -> MediaLocation | None:
        return self.session.scalar(
            select(MediaLocation).where(
                MediaLocation.user_id == user_id,
                MediaLocation.media_asset_id == asset_id,
            )
        )

    def resolved_nearby(
        self,
        *,
        user_id: int,
        latitude: Decimal,
        longitude: Decimal,
        radius_meters: float,
        exclude_asset_id: int | None = None,
    ) -> MediaLocation | None:
        """Return the nearest provider-resolved location inside a small radius.

        The indexed bounding box keeps this portable across SQLite tests and MySQL;
        the final Haversine check prevents corner points from escaping the radius.
        """

        latitude_float = float(latitude)
        longitude_float = float(longitude)
        latitude_delta = radius_meters / 111_320.0
        cosine = abs(math.cos(math.radians(latitude_float)))
        longitude_delta = (
            180.0 if cosine < 1e-8 else min(180.0, radius_meters / (111_320.0 * cosine))
        )
        conditions = [
            MediaLocation.user_id == user_id,
            MediaLocation.latitude.between(
                latitude_float - latitude_delta, latitude_float + latitude_delta
            ),
            MediaLocation.provider.is_not(None),
            MediaLocation.provider_updated_at_utc.is_not(None),
        ]
        if longitude_delta < 180.0:
            longitude_low = longitude_float - longitude_delta
            longitude_high = longitude_float + longitude_delta
            if longitude_low < -180.0:
                conditions.append(
                    or_(
                        MediaLocation.longitude >= longitude_low + 360.0,
                        MediaLocation.longitude <= longitude_high,
                    )
                )
            elif longitude_high > 180.0:
                conditions.append(
                    or_(
                        MediaLocation.longitude >= longitude_low,
                        MediaLocation.longitude <= longitude_high - 360.0,
                    )
                )
            else:
                conditions.append(
                    MediaLocation.longitude.between(longitude_low, longitude_high)
                )
        if exclude_asset_id is not None:
            conditions.append(MediaLocation.media_asset_id != exclude_asset_id)
        candidates = self.session.scalars(
            select(MediaLocation)
            .where(*conditions)
            .order_by(MediaLocation.provider_updated_at_utc.desc(), MediaLocation.id.desc())
            .limit(256)
        )
        nearest: tuple[float, MediaLocation] | None = None
        for candidate in candidates:
            if candidate.latitude is None or candidate.longitude is None:
                continue
            distance = _haversine_meters(
                latitude_float,
                longitude_float,
                float(candidate.latitude),
                float(candidate.longitude),
            )
            if distance <= radius_meters and (nearest is None or distance < nearest[0]):
                nearest = (distance, candidate)
        return nearest[1] if nearest is not None else None


class JobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def require(self, *, user_id: int, job_public_id: UUID | str) -> ProcessingJob:
        job = self.session.scalar(
            select(ProcessingJob).where(
                ProcessingJob.user_id == user_id,
                ProcessingJob.public_id == public_id(job_public_id),
            )
        )
        if job is None:
            raise NotFoundError("JobNotFound", "The processing job was not found")
        return job

    def by_public_id(
        self, *, job_public_id: UUID | str, for_update: bool = False
    ) -> ProcessingJob | None:
        statement = select(ProcessingJob).where(
            ProcessingJob.public_id == public_id(job_public_id)
        )
        if for_update:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def by_idempotency_key(
        self, *, user_id: int, idempotency_key: str
    ) -> ProcessingJob | None:
        return self.session.scalar(
            select(ProcessingJob).where(
                ProcessingJob.user_id == user_id,
                ProcessingJob.idempotency_key == idempotency_key,
            )
        )

    def by_idempotency_keys(
        self, *, user_id: int, idempotency_keys: set[str]
    ) -> dict[str, ProcessingJob]:
        """Prefetch jobs used by every entry in one manifest batch."""

        if not idempotency_keys:
            return {}
        rows = self.session.scalars(
            select(ProcessingJob).where(
                ProcessingJob.user_id == user_id,
                ProcessingJob.idempotency_key.in_(idempotency_keys),
            )
        )
        return {row.idempotency_key: row for row in rows}

    def list_after(
        self,
        *,
        user_id: int,
        after_id: int | None,
        limit: int,
        status: str | None,
        job_type: str | None,
        media_asset_id: int | None,
    ) -> list[ProcessingJob]:
        statement = select(ProcessingJob).where(ProcessingJob.user_id == user_id)
        if after_id is not None:
            statement = statement.where(ProcessingJob.id < after_id)
        if status is not None:
            statement = statement.where(ProcessingJob.status == status)
        if job_type is not None:
            statement = statement.where(ProcessingJob.job_type == job_type)
        if media_asset_id is not None:
            statement = statement.where(ProcessingJob.media_asset_id == media_asset_id)
        return list(
            self.session.scalars(
                statement.order_by(ProcessingJob.id.desc()).limit(limit)
            )
        )


class UploadRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def require(
        self,
        *,
        user_id: int,
        upload_public_id: UUID | str,
        for_update: bool = False,
    ) -> UploadSession:
        statement = select(UploadSession).where(
            UploadSession.user_id == user_id,
            UploadSession.public_id == public_id(upload_public_id),
        )
        if for_update:
            statement = statement.with_for_update()
        upload = self.session.scalar(statement)
        if upload is None:
            raise NotFoundError(
                "UploadSessionNotFound", "The upload session was not found"
            )
        return upload

    def active_for_asset(
        self,
        *,
        user_id: int,
        asset_id: int,
        object_purpose: str,
        for_update: bool = False,
    ) -> UploadSession | None:
        statement = select(UploadSession).where(
            UploadSession.user_id == user_id,
            UploadSession.media_asset_id == asset_id,
            UploadSession.object_purpose == object_purpose,
            UploadSession.active_lease_marker == 1,
        )
        if for_update:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def expired_active_for_user(
        self, *, user_id: int, now: datetime, limit: int = 100
    ) -> list[UploadSession]:
        return list(
            self.session.scalars(
                select(UploadSession)
                .where(
                    UploadSession.user_id == user_id,
                    UploadSession.object_purpose == "TemporaryProcessing",
                    UploadSession.active_lease_marker == 1,
                    UploadSession.expires_at_utc <= now,
                )
                .order_by(UploadSession.id.asc())
                .limit(limit)
                .with_for_update()
            )
        )


class ProviderUsageRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(
        self,
        *,
        user_id: int,
        provider: str,
        usage_month: date,
        unit_type: str,
        for_update: bool = False,
    ) -> ProviderUsageMonth | None:
        statement = select(ProviderUsageMonth).where(
            ProviderUsageMonth.user_id == user_id,
            ProviderUsageMonth.provider == provider,
            ProviderUsageMonth.usage_month == usage_month,
            ProviderUsageMonth.unit_type == unit_type,
        )
        if for_update:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def get_or_create(
        self,
        *,
        user_id: int,
        provider: str,
        usage_month: date,
        unit_type: str,
        hard_limit_units: Decimal,
        now: datetime,
    ) -> ProviderUsageMonth:
        usage = self.get(
            user_id=user_id,
            provider=provider,
            usage_month=usage_month,
            unit_type=unit_type,
            for_update=True,
        )
        if usage is None:
            usage = ProviderUsageMonth(
                user_id=user_id,
                provider=provider,
                usage_month=usage_month,
                unit_type=unit_type,
                processed_units=Decimal("0"),
                reserved_units=Decimal("0"),
                hard_limit_units=hard_limit_units,
                created_at_utc=now,
                updated_at_utc=now,
            )
            self.session.add(usage)
            self.session.flush()
        return usage


class IdempotencyRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(
        self, *, user_id: int, idempotency_key: str, now: datetime
    ) -> IdempotencyRecord | None:
        return self.session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.user_id == user_id,
                IdempotencyRecord.idempotency_key == idempotency_key,
                IdempotencyRecord.expires_at_utc > now,
            )
        )

    def reserve(
        self,
        *,
        user_id: int,
        idempotency_key: str,
        http_method: str,
        route_pattern: str,
        request_sha256: str,
        expires_at_utc: datetime,
        now: datetime,
    ) -> tuple[IdempotencyRecord, bool]:
        existing = self.session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.user_id == user_id,
                IdempotencyRecord.idempotency_key == idempotency_key,
            )
        )
        if existing is not None:
            if existing.expires_at_utc <= now:
                existing.http_method = http_method
                existing.route_pattern = route_pattern
                existing.request_sha256 = request_sha256
                existing.response_status_code = None
                existing.response_headers_json = None
                existing.response_body_json = None
                existing.expires_at_utc = expires_at_utc
                existing.updated_at_utc = now
                self.session.flush()
                return existing, True
            if (
                existing.http_method != http_method
                or existing.route_pattern != route_pattern
                or existing.request_sha256 != request_sha256
            ):
                raise ConflictError(
                    "IdempotencyKeyReused",
                    "The idempotency key was already used for a different request",
                )
            return existing, False
        record = IdempotencyRecord(
            user_id=user_id,
            idempotency_key=idempotency_key,
            http_method=http_method,
            route_pattern=route_pattern,
            request_sha256=request_sha256,
            expires_at_utc=expires_at_utc,
            created_at_utc=now,
            updated_at_utc=now,
        )
        self.session.add(record)
        self.session.flush()
        return record, True
