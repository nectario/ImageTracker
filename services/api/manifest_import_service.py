from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from services.api.manifest_store import S3ManifestObjectStore
from services.api.models import (
    ManifestCounts,
    ManifestImport as ManifestImportResponse,
    ManifestImportCreateRequest,
    ManifestImportResultDownload,
    SignedUploadRequest,
)
from services.api.service import (
    BadRequestError,
    ConflictError,
    MutationContext,
    NotFoundError,
    ServiceUnavailableError,
)
from services.data.database import transaction_scope
from services.data.models import (
    ManifestImport as ManifestImportRow,
    MediaSource,
    UserAccount,
)


TERMINAL_IMPORT_STATUSES = {
    "Succeeded",
    "CompletedWithErrors",
    "FailedPermanent",
    "Cancelled",
    "Expired",
}


class ManifestImportDispatcher(Protocol):
    def dispatch(self, import_id: UUID) -> None: ...


class ManifestImportService(Protocol):
    def create(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        payload: ManifestImportCreateRequest,
        mutation: MutationContext,
    ) -> tuple[ManifestImportResponse, bool]: ...

    def refresh_upload(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse: ...

    def complete(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse: ...

    def get(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse: ...

    def result(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResultDownload: ...


class UnavailableManifestImportService:
    def __getattr__(self, _name: str):
        def unavailable(**_kwargs: Any):
            raise ServiceUnavailableError(
                "Bulk manifest import is not configured.",
                code="MANIFEST_IMPORT_UNAVAILABLE",
            )

        return unavailable


class SqlAlchemyManifestImportService:
    """Authenticated, idempotent control plane for asynchronous imports."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        object_store: S3ManifestObjectStore,
        dispatcher: ManifestImportDispatcher,
        clock: Any | None = None,
        upload_ttl_minutes: int = 15,
    ) -> None:
        if not 5 <= upload_ttl_minutes <= 60:
            raise ValueError("Manifest upload TTL must be between 5 and 60 minutes")
        self._session_factory = session_factory
        self._store = object_store
        self._dispatcher = dispatcher
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._upload_ttl = timedelta(minutes=upload_ttl_minutes)

    def create(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        payload: ManifestImportCreateRequest,
        mutation: MutationContext,
    ) -> tuple[ManifestImportResponse, bool]:
        now = self._now()
        upload_expires = now + self._upload_ttl
        replayed = False
        try:
            with transaction_scope(self._session_factory) as session:
                account = self._account(session, user_id)
                session.execute(
                    select(UserAccount.id)
                    .where(UserAccount.id == account.id)
                    .with_for_update()
                )
                existing = session.scalar(
                    select(ManifestImportRow).where(
                        ManifestImportRow.user_id == account.id,
                        ManifestImportRow.idempotency_key
                        == mutation.idempotency_key,
                    )
                )
                if existing is not None:
                    if existing.request_sha256 != mutation.request_hash:
                        raise ConflictError(
                            "The idempotency key was already used for another import.",
                            code="IDEMPOTENCY_KEY_REUSED",
                        )
                    row = existing
                    replayed = True
                else:
                    source = self._source(
                        session,
                        account_id=account.id,
                        source_id=source_id,
                        for_update=True,
                    )
                    self._require_bulk_source(source)
                    active = session.scalar(
                        select(ManifestImportRow).where(
                            ManifestImportRow.user_id == account.id,
                            ManifestImportRow.media_source_id == source.id,
                            ManifestImportRow.active_marker == 1,
                        ).with_for_update()
                    )
                    if active is not None:
                        if (
                            active.status == "AwaitingUpload"
                            and active.upload_expires_at_utc is not None
                            and active.upload_expires_at_utc <= now
                        ):
                            active.status = "Expired"
                            active.phase = "Complete"
                            active.active_marker = None
                            active.completed_at_utc = now
                            active.updated_at_utc = now
                            session.flush()
                        else:
                            raise ConflictError(
                                "This source already has an active manifest import.",
                                code="MANIFEST_IMPORT_ACTIVE",
                            )
                    public_id = uuid4()
                    upload = self._store.create_input_upload(
                        user_id=user_id,
                        source_id=source_id,
                        import_id=public_id,
                        checksum_sha256_hex=payload.checksum_sha256,
                        content_length=payload.byte_size,
                        expires_at_utc=self._aware(upload_expires),
                    )
                    row = ManifestImportRow(
                        public_id=str(public_id),
                        user_id=account.id,
                        media_source_id=source.id,
                        snapshot_id=str(payload.snapshot_id),
                        idempotency_key=mutation.idempotency_key,
                        request_sha256=mutation.request_hash,
                        active_marker=1,
                        manifest_kind="Full",
                        permission_state=str(payload.permission_state),
                        deletion_detection_reliable=0,
                        client_cursor=payload.client_cursor,
                        schema_version=payload.schema_version,
                        status="AwaitingUpload",
                        phase="WaitingForUpload",
                        input_s3_bucket=upload.bucket,
                        input_s3_object_key=upload.object_key,
                        input_checksum_sha256=payload.checksum_sha256.lower(),
                        input_byte_size=payload.byte_size,
                        declared_entry_count=payload.entry_count,
                        upload_expires_at_utc=upload_expires,
                        created_at_utc=now,
                        updated_at_utc=now,
                    )
                    session.add(row)
                    session.flush()
                if replayed and row.status == "AwaitingUpload":
                    row.upload_expires_at_utc = upload_expires
                    row.updated_at_utc = now
        except IntegrityError as exc:
            raise ConflictError(
                "This source already has this manifest import.",
                code="MANIFEST_IMPORT_CONFLICT",
            ) from exc
        return (
            self._response(
                row,
                user_public_id=user_id,
                source_public_id=source_id,
                include_upload=row.status == "AwaitingUpload",
            ),
            replayed,
        )

    def refresh_upload(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse:
        now = self._now()
        with transaction_scope(self._session_factory) as session:
            row = self._import(
                session,
                user_id=user_id,
                source_id=source_id,
                import_id=import_id,
                for_update=True,
            )
            if row.status != "AwaitingUpload":
                raise ConflictError(
                    "Only an import awaiting upload can receive another URL.",
                    code="MANIFEST_IMPORT_NOT_AWAITING_UPLOAD",
                )
            row.upload_expires_at_utc = now + self._upload_ttl
            row.updated_at_utc = now
        return self._response(
            row,
            user_public_id=user_id,
            source_public_id=source_id,
            include_upload=True,
        )

    def complete(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse:
        with transaction_scope(self._session_factory) as session:
            row = self._import(
                session,
                user_id=user_id,
                source_id=source_id,
                import_id=import_id,
            )
            if row.status in {"Queued", "Running", "RetryDue"} | TERMINAL_IMPORT_STATUSES:
                return self._response(
                    row,
                    user_public_id=user_id,
                    source_public_id=source_id,
                )
            if row.status != "AwaitingUpload":
                raise ConflictError(
                    "The manifest import cannot be completed in its current state.",
                    code="MANIFEST_IMPORT_STATE_CONFLICT",
                )
            bucket = row.input_s3_bucket
            key = row.input_s3_object_key
            expected_bytes = row.input_byte_size
            expected_sha = row.input_checksum_sha256
        try:
            metadata = self._store.head_object(bucket=bucket, object_key=key)
        except Exception as exc:
            raise ServiceUnavailableError(
                "Manifest storage is temporarily unavailable.",
                code="MANIFEST_STORAGE_UNAVAILABLE",
            ) from exc
        if metadata is None:
            raise ConflictError(
                "Upload the manifest before completing the import.",
                code="MANIFEST_OBJECT_MISSING",
            )
        if (
            metadata.byte_size != expected_bytes
            or metadata.checksum_sha256_hex.lower() != expected_sha.lower()
            or metadata.content_type != "application/x-ndjson"
            or metadata.content_encoding.casefold() != "gzip"
        ):
            raise BadRequestError(
                "The uploaded manifest does not match its declaration.",
                code="MANIFEST_OBJECT_MISMATCH",
            )
        now = self._now()
        queued = False
        with transaction_scope(self._session_factory) as session:
            row = self._import(
                session,
                user_id=user_id,
                source_id=source_id,
                import_id=import_id,
                for_update=True,
            )
            if row.status == "AwaitingUpload":
                row.status = "Queued"
                row.phase = "Queued"
                row.input_s3_version_id = metadata.version_id
                row.queued_at_utc = now
                row.next_attempt_at_utc = now
                row.updated_at_utc = now
                queued = True
        if queued:
            try:
                self._dispatcher.dispatch(import_id)
            except Exception:
                # Queued is durable; the five-minute recovery schedule republishes it.
                pass
        return self._response(
            row,
            user_public_id=user_id,
            source_public_id=source_id,
        )

    def get(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResponse:
        with transaction_scope(self._session_factory) as session:
            row = self._import(
                session,
                user_id=user_id,
                source_id=source_id,
                import_id=import_id,
            )
            return self._response(
                row,
                user_public_id=user_id,
                source_public_id=source_id,
            )

    def result(
        self, *, user_id: UUID, source_id: UUID, import_id: UUID
    ) -> ManifestImportResultDownload:
        with transaction_scope(self._session_factory) as session:
            row = self._import(
                session,
                user_id=user_id,
                source_id=source_id,
                import_id=import_id,
            )
            if row.status not in {"Succeeded", "CompletedWithErrors"}:
                raise ConflictError(
                    "The manifest result is not available yet.",
                    code="MANIFEST_RESULT_NOT_READY",
                )
            if not all(
                (
                    row.result_s3_bucket,
                    row.result_s3_object_key,
                    row.result_checksum_sha256,
                    row.result_byte_size,
                )
            ):
                raise ServiceUnavailableError(
                    "The completed manifest result is unavailable.",
                    code="MANIFEST_RESULT_UNAVAILABLE",
                )
            bucket = str(row.result_s3_bucket)
            key = str(row.result_s3_object_key)
            checksum = str(row.result_checksum_sha256)
            byte_size = int(row.result_byte_size or 0)
        expires = self._aware(self._now() + timedelta(minutes=15))
        download = self._store.create_result_download(
            bucket=bucket,
            object_key=key,
            expires_at_utc=expires,
        )
        return ManifestImportResultDownload(
            url=download.url,
            expires_at_utc=download.expires_at_utc,
            checksum_sha256=checksum,
            byte_size=byte_size,
        )

    def _response(
        self,
        row: ManifestImportRow,
        *,
        user_public_id: UUID,
        source_public_id: UUID,
        include_upload: bool = False,
    ) -> ManifestImportResponse:
        upload: SignedUploadRequest | None = None
        if include_upload:
            expires = row.upload_expires_at_utc or (self._now() + self._upload_ttl)
            signed = self._store.create_input_upload(
                user_id=user_public_id,
                source_id=source_public_id,
                import_id=UUID(row.public_id),
                checksum_sha256_hex=row.input_checksum_sha256,
                content_length=row.input_byte_size,
                expires_at_utc=self._aware(expires),
            )
            upload = SignedUploadRequest(
                url=signed.url,
                method="PUT",
                headers=signed.headers,
                expires_at_utc=signed.expires_at_utc,
            )
        return ManifestImportResponse(
            import_id=UUID(row.public_id),
            source_id=source_public_id,
            snapshot_id=UUID(row.snapshot_id),
            schema_version="ManifestNdjsonV1",
            status=row.status,
            phase=row.phase,
            entry_count=row.declared_entry_count,
            validated_entry_count=row.validated_entry_count,
            processed_entry_count=row.processed_entry_count,
            counts=ManifestCounts(
                created=row.created_count,
                updated=row.updated_count,
                duplicates_linked=row.duplicate_linked_count,
                deleted=row.deleted_count,
                ignored_deletions=row.ignored_deletion_count,
                unchanged=row.unchanged_count,
                rejected=row.rejected_count,
            ),
            upload=upload,
            upload_expires_at_utc=(
                self._aware(row.upload_expires_at_utc)
                if row.upload_expires_at_utc
                else None
            ),
            next_attempt_at_utc=(
                self._aware(row.next_attempt_at_utc)
                if row.next_attempt_at_utc
                else None
            ),
            failure_code=row.failure_code,
            failure_message=row.failure_message,
            result_available=row.status in {"Succeeded", "CompletedWithErrors"}
            and bool(row.result_s3_object_key),
            created_at_utc=self._aware(row.created_at_utc),
            updated_at_utc=self._aware(row.updated_at_utc),
            completed_at_utc=(
                self._aware(row.completed_at_utc) if row.completed_at_utc else None
            ),
        )

    def _account(self, session: Session, public_id: UUID) -> UserAccount:
        row = session.scalar(
            select(UserAccount).where(
                UserAccount.public_id == str(public_id),
                UserAccount.account_status == "Active",
                UserAccount.deleted_at_utc.is_(None),
            )
        )
        if row is None:
            raise NotFoundError("The ImageTracker account was not found.")
        return row

    def _source(
        self,
        session: Session,
        *,
        account_id: int,
        source_id: UUID,
        for_update: bool = False,
    ) -> MediaSource:
        statement = select(MediaSource).where(
            MediaSource.user_id == account_id,
            MediaSource.public_id == str(source_id),
        )
        if for_update:
            statement = statement.with_for_update()
        row = session.scalar(statement)
        if row is None:
            raise NotFoundError("The media source was not found.")
        return row

    def _import(
        self,
        session: Session,
        *,
        user_id: UUID,
        source_id: UUID,
        import_id: UUID,
        for_update: bool = False,
    ) -> ManifestImportRow:
        account = self._account(session, user_id)
        source = self._source(
            session,
            account_id=account.id,
            source_id=source_id,
        )
        statement = select(ManifestImportRow).where(
            ManifestImportRow.user_id == account.id,
            ManifestImportRow.media_source_id == source.id,
            ManifestImportRow.public_id == str(import_id),
        )
        if for_update:
            statement = statement.with_for_update()
        row = session.scalar(statement)
        if row is None:
            raise NotFoundError("The manifest import was not found.")
        return row

    @staticmethod
    def _require_bulk_source(source: MediaSource) -> None:
        if source.storage_mode != "Local" or source.source_status != "Active":
            raise ConflictError(
                "Bulk manifest import requires an active Local source.",
                code="BULK_SOURCE_UNAVAILABLE",
            )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    @staticmethod
    def _aware(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
