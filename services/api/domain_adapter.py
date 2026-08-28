from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import re
from typing import Awaitable, Callable, TypeVar
from uuid import UUID

from sqlalchemy.exc import OperationalError, TimeoutError as SqlAlchemyTimeoutError

from services.api import models as api
from services.api.service import (
    AuthIdentity,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    JobQuery,
    MediaQuery,
    MediaSearchQuery,
    MutationContext,
    MutationResult,
    NotFoundError,
    ServiceUnavailableError,
)
from services.domain import errors as domain_errors
from services.domain import models as domain
from services.domain.service import Phase1DomainService


T = TypeVar("T")
R = TypeVar("R")


def _error_code(value: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).upper()


async def _domain_call(awaitable: Awaitable[T]) -> T:
    try:
        return await awaitable
    except domain_errors.InvalidCursorError as exc:
        raise BadRequestError(exc.detail, code=_error_code(exc.code)) from exc
    except domain_errors.NotFoundError as exc:
        raise NotFoundError(exc.detail, code=_error_code(exc.code)) from exc
    except domain_errors.ForbiddenError as exc:
        raise ForbiddenError(exc.detail, code=_error_code(exc.code)) from exc
    except domain_errors.ConflictError as exc:
        raise ConflictError(exc.detail, code=_error_code(exc.code)) from exc
    except domain_errors.DomainError as exc:
        raise ConflictError(exc.detail, code=_error_code(exc.code)) from exc
    except (OperationalError, SqlAlchemyTimeoutError) as exc:
        raise ServiceUnavailableError(
            "The ImageTracker database is temporarily unavailable",
            code="DATABASE_UNAVAILABLE",
        ) from exc


def _domain_mutation(value: MutationContext) -> domain.MutationContext:
    return domain.MutationContext(
        request_id=value.request_id,
        idempotency_key=value.idempotency_key,
        operation=value.operation,
        target=value.target,
        request_hash=value.request_hash,
    )


def _mutation_result(
    value: domain.MutationResult[T], convert: Callable[[T], R]
) -> MutationResult[R]:
    return MutationResult(
        value=convert(value.value),
        status_code=value.status_code,
        replayed=value.replayed,
    )


def _page(value: domain.Page[T], convert: Callable[[T], R]) -> tuple[list[R], api.PageInfo]:
    return (
        [convert(item) for item in value.items],
        api.PageInfo(next_cursor=value.next_cursor, has_more=value.has_more),
    )


def _sync_settings(value: domain.SyncSettings) -> api.SyncSettings:
    return api.SyncSettings(
        automatic_sync=value.automatic_sync,
        network_policy=value.network_policy,
        require_charging_for_historical_upload=value.require_charging_for_historical_upload,
    )


def _domain_sync_settings(value: api.SyncSettings | None) -> domain.SyncSettings:
    if value is None:
        return domain.SyncSettings()
    return domain.SyncSettings(
        automatic_sync=value.automatic_sync,
        network_policy=str(value.network_policy),
        require_charging_for_historical_upload=value.require_charging_for_historical_upload,
    )


def _device(value: domain.DeviceRecord) -> api.Device:
    return api.Device.model_validate(value)


def _source(value: domain.SourceRecord) -> api.MediaSource:
    return api.MediaSource(
        source_id=value.source_id,
        device_id=value.device_id,
        source_key=value.source_key,
        source_type=value.source_type,
        display_name=value.display_name,
        storage_mode=value.storage_mode,
        permission_state=value.permission_state,
        status=value.status,
        sync_settings=_sync_settings(value.sync_settings),
        last_manifest_at_utc=value.last_manifest_at_utc,
        created_at_utc=value.created_at_utc,
        updated_at_utc=value.updated_at_utc,
    )


def _provenance(value: domain.FieldProvenance) -> api.FieldProvenance:
    return api.FieldProvenance.model_validate(value)


def _location_summary(
    value: domain.MediaLocationRecord | None,
) -> api.LocationSummary | None:
    if value is None:
        return None
    return api.LocationSummary(
        latitude=value.latitude,
        longitude=value.longitude,
        display_name=value.display_name,
        street_address=value.street_address,
        neighborhood=value.neighborhood,
        city=value.city,
        county=value.county,
        state=value.state,
        postal_code=value.postal_code,
        country=value.country,
        country_code=value.country_code,
        provider=value.provider,
    )


def _location(value: domain.MediaLocationRecord | None) -> api.MediaLocation | None:
    if value is None:
        return None
    return api.MediaLocation(
        latitude=value.latitude,
        longitude=value.longitude,
        altitude_meters=value.altitude_meters,
        horizontal_accuracy_meters=value.horizontal_accuracy_meters,
        display_name=value.display_name,
        street_address=value.street_address,
        original_street_number=value.original_street_number,
        neighborhood=value.neighborhood,
        city=value.city,
        county=value.county,
        state=value.state,
        postal_code=value.postal_code,
        country=value.country,
        country_code=value.country_code,
        provider=value.provider,
        provider_place_id=value.provider_place_id,
        normalization_rule_version=value.normalization_rule_version,
        provider_updated_at_utc=value.provider_updated_at_utc,
        provenance=[_provenance(item) for item in value.provenance],
    )


def _local_wall_clock(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(tzinfo=None).isoformat()


def _media_summary(value: domain.MediaSummary) -> api.MediaAssetSummary:
    return api.MediaAssetSummary(
        media_asset_id=value.media_asset_id,
        content_sha256=value.content_sha256,
        media_type=value.media_type,
        mime_type=value.mime_type,
        byte_size=value.byte_size,
        display_file_name=value.display_file_name,
        storage_mode=value.storage_mode,
        storage_state=value.storage_state,
        availability=value.availability,
        state=value.state,
        temporal=api.TemporalMetadata(
            captured_at_local=_local_wall_clock(value.captured_at_local),
            captured_at_utc=value.captured_at_utc,
            time_zone_id=value.time_zone_id,
            utc_offset_minutes=value.utc_offset_minutes,
            source=value.capture_time_source,
            confidence=value.capture_time_confidence,
        ),
        duration_ms=value.duration_ms,
        width_pixels=value.width_pixels,
        height_pixels=value.height_pixels,
        category=value.category,
        location=_location_summary(value.location),
        description_excerpt=value.description_excerpt,
        preview_url=None,
        preview_url_expires_at_utc=None,
        is_trashed=value.is_trashed,
        purge_after_utc=value.purge_after_utc,
        created_at_utc=value.created_at_utc,
        updated_at_utc=value.updated_at_utc,
    )


def _occurrence(value: domain.OccurrenceRecord) -> api.MediaOccurrence:
    return api.MediaOccurrence.model_validate(value)


def _description(value: domain.DescriptionRecord | None) -> api.MediaDescription | None:
    return api.MediaDescription.model_validate(value) if value is not None else None


def _transcript(value: domain.TranscriptRecord | None) -> api.MediaTranscript | None:
    return api.MediaTranscript.model_validate(value) if value is not None else None


def _media_detail(value: domain.MediaDetail) -> api.MediaAssetDetail:
    summary = _media_summary(value.asset)
    return api.MediaAssetDetail(
        **summary.model_dump(),
        occurrences=[_occurrence(item) for item in value.occurrences],
        location_detail=_location(value.location_detail),
        description=_description(value.description),
        transcript=_transcript(value.transcript),
        remote_access=None,
        provenance=[_provenance(item) for item in value.provenance],
    )


def _search_hit(value: domain.MediaSearchHit) -> api.MediaSearchHit:
    return api.MediaSearchHit(
        asset=_media_summary(value.asset),
        matched_field=value.matched_field,
        highlight=value.highlight,
        transcript_segment_id=value.transcript_segment_id,
        seek_to_ms=value.seek_to_ms,
    )


def _job(value: domain.JobRecord) -> api.ProcessingJob:
    return api.ProcessingJob.model_validate(value)


def _manifest(value: domain.ManifestResult) -> api.ManifestResponse:
    return api.ManifestResponse.model_validate(value)


def _upload_plan(value: domain.UploadPlanRecord) -> api.UploadPlan:
    return api.UploadPlan.model_validate(value)


def _upload_session(value: domain.UploadSessionRecord) -> api.UploadSessionStatus:
    return api.UploadSessionStatus(
        upload_session_id=value.upload_session_id,
        strategy=value.strategy,
        status=value.status,
        expected_byte_size=value.expected_byte_size,
        uploaded_byte_size=value.uploaded_byte_size,
        uploaded_parts=[],
        expires_at_utc=value.expires_at_utc,
    )


def _upload_complete(value: domain.UploadCompleteRecord) -> api.UploadCompleteResponse:
    return api.UploadCompleteResponse.model_validate(value)


def _domain_provenance(value: api.FieldProvenance) -> domain.FieldProvenance:
    return domain.FieldProvenance(
        field=value.field,
        source=str(value.source),
        confidence=(Decimal(str(value.confidence)) if value.confidence is not None else None),
        processor_version=value.processor_version,
        observed_at_utc=value.observed_at_utc,
    )


def _manifest_command(value: api.ManifestRequest) -> domain.ManifestCommand:
    entries: list[domain.ManifestEntry] = []
    for item in value.entries:
        if isinstance(item, api.ManifestDeletedEntry):
            entries.append(
                domain.ManifestDelete(
                    source_item_id=item.source_item_id,
                    source_revision=item.source_revision,
                )
            )
            continue
        location = item.location
        entries.append(
            domain.ManifestUpsert(
                source_item_id=item.source_item_id,
                source_revision=item.source_revision,
                file_name=item.file_name,
                media_type=str(item.media_type),
                mime_type=item.mime_type,
                byte_size=item.byte_size,
                local_locator=item.local_locator,
                content_sha256=item.content_sha256,
                width_pixels=item.width_pixels,
                height_pixels=item.height_pixels,
                duration_ms=item.duration_ms,
                captured_at_local=(
                    datetime.fromisoformat(item.captured_at_local)
                    if item.captured_at_local is not None
                    else None
                ),
                captured_at_utc=item.captured_at_utc,
                time_zone_id=item.time_zone_id,
                utc_offset_minutes=item.utc_offset_minutes,
                location=(
                    domain.GeoPoint(
                        latitude=Decimal(str(location.latitude)),
                        longitude=Decimal(str(location.longitude)),
                        altitude_meters=(
                            Decimal(str(location.altitude_meters))
                            if location.altitude_meters is not None
                            else None
                        ),
                        horizontal_accuracy_meters=(
                            Decimal(str(location.horizontal_accuracy_meters))
                            if location.horizontal_accuracy_meters is not None
                            else None
                        ),
                    )
                    if location is not None
                    else None
                ),
                provenance=tuple(_domain_provenance(entry) for entry in item.provenance),
            )
        )
    return domain.ManifestCommand(
        snapshot_id=value.snapshot_id,
        kind=str(value.kind),
        permission_state=str(value.permission_state),
        deletion_detection_reliable=value.deletion_detection_reliable,
        client_cursor=value.client_cursor,
        entries=tuple(entries),
    )


class DomainServiceAdapter:
    """Translate the public API contract to the persistence-agnostic domain facade."""

    def __init__(self, service: Phase1DomainService) -> None:
        self._service = service

    async def current_user(self, identity: AuthIdentity) -> api.CurrentUser:
        value = await _domain_call(
            self._service.current_user(
                domain.AccountIdentity(
                    cognito_subject=identity.subject,
                    email=identity.email,
                    display_name=identity.display_name,
                )
            )
        )
        return api.CurrentUser.model_validate(value)

    async def list_devices(
        self, user_id: UUID, cursor: str | None, limit: int
    ) -> api.DevicePage:
        value = await _domain_call(self._service.list_devices(user_id, cursor, limit))
        items, page = _page(value, _device)
        return api.DevicePage(items=items, page=page)

    async def register_device(
        self,
        user_id: UUID,
        payload: api.DeviceRegistrationRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.Device]:
        value = await _domain_call(
            self._service.register_device(
                user_id,
                domain.DeviceRegistration(
                    installation_id=payload.installation_id,
                    platform=str(payload.platform),
                    display_name=payload.display_name,
                    app_version=payload.app_version,
                    os_version=payload.os_version,
                ),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _device)

    async def list_sources(
        self, user_id: UUID, cursor: str | None, limit: int
    ) -> api.MediaSourcePage:
        value = await _domain_call(self._service.list_sources(user_id, cursor, limit))
        items, page = _page(value, _source)
        return api.MediaSourcePage(items=items, page=page)

    async def create_source(
        self,
        user_id: UUID,
        payload: api.MediaSourceCreateRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.MediaSource]:
        value = await _domain_call(
            self._service.create_source(
                user_id,
                domain.SourceCreate(
                    device_id=payload.device_id,
                    source_key=payload.source_key,
                    source_type=str(payload.source_type),
                    display_name=payload.display_name,
                    storage_mode=str(payload.storage_mode),
                    permission_state=str(payload.permission_state),
                    sync_settings=_domain_sync_settings(payload.sync_settings),
                ),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _source)

    async def get_source(self, user_id: UUID, source_id: UUID) -> api.MediaSource:
        return _source(
            await _domain_call(self._service.get_source(user_id, source_id))
        )

    async def update_source(
        self,
        user_id: UUID,
        source_id: UUID,
        payload: api.MediaSourceUpdateRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.MediaSource]:
        sync_settings = (
            _domain_sync_settings(payload.sync_settings)
            if payload.sync_settings is not None
            else None
        )
        value = await _domain_call(
            self._service.update_source(
                user_id,
                source_id,
                domain.SourceUpdate(
                    display_name=payload.display_name,
                    storage_mode=(
                        str(payload.storage_mode)
                        if payload.storage_mode is not None
                        else None
                    ),
                    permission_state=(
                        str(payload.permission_state)
                        if payload.permission_state is not None
                        else None
                    ),
                    status=payload.status,
                    sync_settings=sync_settings,
                ),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _source)

    async def remove_source(
        self, user_id: UUID, source_id: UUID, mutation: MutationContext
    ) -> MutationResult[None]:
        value = await _domain_call(
            self._service.remove_source(user_id, source_id, _domain_mutation(mutation))
        )
        return _mutation_result(value, lambda _: None)

    async def submit_manifest(
        self,
        user_id: UUID,
        source_id: UUID,
        payload: api.ManifestRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.ManifestResponse]:
        value = await _domain_call(
            self._service.submit_manifest(
                user_id,
                source_id,
                _manifest_command(payload),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _manifest)

    async def create_upload_plan(
        self,
        user_id: UUID,
        payload: api.UploadPlanRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.UploadPlan]:
        value = await _domain_call(
            self._service.create_upload_plan(
                user_id,
                domain.UploadPlanCommand(
                    source_id=payload.source_id,
                    occurrence_id=payload.occurrence_id,
                    asset_content_sha256=payload.asset_content_sha256,
                    object_sha256=payload.object_sha256,
                    file_name=payload.file_name,
                    media_type=str(payload.media_type),
                    object_mime_type=payload.object_mime_type,
                    object_byte_size=payload.object_byte_size,
                    purpose=str(payload.purpose),
                    processing_job_id=payload.processing_job_id,
                ),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _upload_plan)

    async def get_upload_session(
        self, user_id: UUID, upload_session_id: UUID
    ) -> api.UploadSessionStatus:
        return _upload_session(
            await _domain_call(
                self._service.get_upload_session(user_id, upload_session_id)
            )
        )

    async def complete_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        payload: api.UploadCompleteRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.UploadCompleteResponse]:
        value = await _domain_call(
            self._service.complete_upload(
                user_id,
                upload_session_id,
                domain.UploadCompleteCommand(
                    object_sha256=payload.object_sha256,
                    etag=payload.etag,
                    parts=tuple(payload.parts),
                ),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _upload_complete)

    async def cancel_upload(
        self,
        user_id: UUID,
        upload_session_id: UUID,
        payload: api.UploadCancelRequest,
        mutation: MutationContext,
    ) -> MutationResult[None]:
        del payload
        value = await _domain_call(
            self._service.cancel_upload(
                user_id, upload_session_id, _domain_mutation(mutation)
            )
        )
        return _mutation_result(value, lambda _: None)

    async def list_changes(
        self,
        user_id: UUID,
        requesting_device_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> api.ChangePage:
        value = await _domain_call(
            self._service.list_changes(user_id, requesting_device_id, cursor, limit)
        )
        items, page = _page(
            value,
            lambda item: api.MediaChange(
                cursor=item.cursor,
                change_type=item.change_type,
                resource_type=item.resource_type,
                resource_id=item.resource_id,
                occurred_at_utc=item.occurred_at_utc,
            ),
        )
        return api.ChangePage(items=items, page=page)

    @staticmethod
    def _media_query(value: MediaQuery) -> domain.MediaQuery:
        return domain.MediaQuery(
            cursor=value.cursor,
            limit=value.limit,
            source_id=value.source_id,
            media_type=(str(value.media_type) if value.media_type is not None else None),
            storage_mode=(
                str(value.storage_mode) if value.storage_mode is not None else None
            ),
            captured_after_utc=value.captured_after_utc,
            captured_before_utc=value.captured_before_utc,
            category=value.category,
            has_location=value.has_location,
            trash_state=value.trash_state,
            sort=value.sort,
        )

    async def list_media(
        self, user_id: UUID, requesting_device_id: UUID, query: MediaQuery
    ) -> api.MediaAssetPage:
        value = await _domain_call(
            self._service.list_media(
                user_id, requesting_device_id, self._media_query(query)
            )
        )
        items, page = _page(value, _media_summary)
        return api.MediaAssetPage(items=items, page=page)

    async def search_media(
        self, user_id: UUID, requesting_device_id: UUID, query: MediaSearchQuery
    ) -> api.MediaSearchPage:
        value = await _domain_call(
            self._service.search_media(
                user_id,
                requesting_device_id,
                domain.MediaSearchQuery(
                    text=query.text,
                    filters=self._media_query(query),
                ),
            )
        )
        items, page = _page(value, _search_hit)
        return api.MediaSearchPage(items=items, page=page)

    async def get_media_asset(
        self, user_id: UUID, requesting_device_id: UUID, media_asset_id: UUID
    ) -> api.MediaAssetDetail:
        return _media_detail(
            await _domain_call(
                self._service.get_media_asset(
                    user_id, requesting_device_id, media_asset_id
                )
            )
        )

    async def list_jobs(
        self, user_id: UUID, query: JobQuery
    ) -> api.ProcessingJobPage:
        value = await _domain_call(
            self._service.list_jobs(
                user_id,
                domain.JobQuery(
                    cursor=query.cursor,
                    limit=query.limit,
                    status=(str(query.status) if query.status is not None else None),
                    job_type=(
                        str(query.job_type) if query.job_type is not None else None
                    ),
                    media_asset_id=query.media_asset_id,
                ),
            )
        )
        items, page = _page(value, _job)
        return api.ProcessingJobPage(items=items, page=page)

    async def get_job(self, user_id: UUID, job_id: UUID) -> api.ProcessingJob:
        return _job(await _domain_call(self._service.get_job(user_id, job_id)))

    async def retry_job(
        self, user_id: UUID, job_id: UUID, mutation: MutationContext
    ) -> MutationResult[api.ProcessingJob]:
        value = await _domain_call(
            self._service.retry_job(user_id, job_id, _domain_mutation(mutation))
        )
        return _mutation_result(value, _job)

    async def cancel_job(
        self,
        user_id: UUID,
        job_id: UUID,
        payload: api.JobCancelRequest,
        mutation: MutationContext,
    ) -> MutationResult[api.ProcessingJob]:
        value = await _domain_call(
            self._service.cancel_job(
                user_id,
                job_id,
                str(payload.reason),
                _domain_mutation(mutation),
            )
        )
        return _mutation_result(value, _job)
