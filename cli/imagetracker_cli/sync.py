from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .api_client import ApiClient, ApiError, ApiProblem
from .media import MediaScanner, stream_sha256
from .scene_preview import (
    SCENE_PREVIEW_CAPABILITY_VERSION,
    ScenePreview,
    ScenePreviewError,
    prepare_scene_preview,
)
from .state import DescriptionOutboxItem, LocalState, SourceBinding


MANIFEST_BATCH_SIZE = 500


@dataclass
class SyncSummary:
    source_id: str
    root_path: str
    dry_run: bool
    scanned: int = 0
    hashed: int = 0
    cached: int = 0
    unchanged: int = 0
    upserts: int = 0
    deletions: int = 0
    failed: int = 0
    batches_sent: int = 0
    resumed_batches: int = 0
    duplicates_linked: int = 0
    upload_required: int = 0
    deletion_detection_reliable: bool = False
    queued_batches: int = 0
    quarantined_batches: int = 0
    quarantined_entries: int = 0
    rejected_entries: int = 0
    descriptions_staged: int = 0
    description_pending: int = 0
    description_deferred: int = 0
    description_quarantined: int = 0
    descriptions_recovered: int = 0
    force_rehash: bool = False
    scan_workers: int = 1
    scan_seconds: float = 0.0
    scan_files_per_second: float = 0.0
    hash_pending: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class SyncEngine:
    def __init__(
        self,
        api: ApiClient,
        state: LocalState,
        scanner: MediaScanner | None = None,
        *,
        progress: Callable[[str], None] | None = None,
        preview_factory: Callable[[str | Path], ScenePreview] = prepare_scene_preview,
        hash_file: Callable[[Path], str] = stream_sha256,
    ):
        self.api = api
        self.state = state
        self.scanner = scanner or MediaScanner(state)
        self.progress = progress or (lambda _message: None)
        self.preview_factory = preview_factory
        self.hash_file = hash_file

    def sync(
        self,
        binding: SourceBinding,
        *,
        dry_run: bool = False,
        force_rehash: bool = False,
        scan_workers: int | None = None,
        fast_add: bool = False,
    ) -> SyncSummary:
        summary = SyncSummary(
            source_id=binding.source_id,
            root_path=binding.root_path,
            dry_run=dry_run,
            force_rehash=force_rehash,
        )
        if binding.storage_mode != "Local":
            raise ValueError(
                "This Phase 1 sync command handles Local sources only. "
                "Remote upload will be enabled with the resumable upload phase."
            )

        if not dry_run:
            self._flush_description_outbox(binding, summary)

        pending = self.state.pending_batches(binding.source_id)
        summary.resumed_batches = len(pending)
        if pending and not dry_run:
            self.progress(f"Resuming {len(pending)} saved manifest batch(es)")
            self._flush_pending(binding, summary)
        elif pending:
            summary.queued_batches += len(pending)

        root = Path(binding.root_path)
        self.progress(f"Scanning {root}")
        scan = self.scanner.scan(
            binding.source_id,
            root,
            force_rehash=force_rehash,
            fast_add=fast_add,
            workers=scan_workers,
            progress=self.progress,
        )
        known = self.state.known_occurrences(binding.source_id)
        quarantined = self.state.quarantined_revisions(binding.source_id)
        changed_entries: list[dict[str, Any]] = []
        for entry in scan.entries:
            existing = known.get(str(entry["sourceItemId"]))
            if existing and existing[0] == entry["sourceRevision"]:
                summary.unchanged += 1
            elif quarantined.get(str(entry["sourceItemId"])) == entry["sourceRevision"]:
                summary.failed += 1
                summary.quarantined_entries += 1
            else:
                changed_entries.append(entry)

        deleted_entries: list[dict[str, Any]] = []
        if scan.complete_read:
            for item_id, (revision, _path) in known.items():
                if item_id not in scan.seen_source_item_ids:
                    if quarantined.get(item_id) == revision:
                        summary.failed += 1
                        summary.quarantined_entries += 1
                        continue
                    deleted_entries.append(
                        {
                            "operation": "Deleted",
                            "sourceItemId": item_id,
                            "sourceRevision": revision,
                        }
                    )

        summary.scanned = scan.scanned
        summary.hashed = scan.hashed
        summary.cached = scan.cached
        summary.upserts = len(changed_entries)
        summary.deletions = len(deleted_entries)
        summary.failed += scan.failed
        summary.deletion_detection_reliable = scan.complete_read
        summary.scan_workers = scan.worker_count
        summary.scan_seconds = scan.elapsed_seconds
        summary.scan_files_per_second = scan.files_per_second
        summary.hash_pending = scan.pending_hash

        entries = [*changed_entries, *deleted_entries]
        if not entries:
            self.progress("Library is already in sync")
            if not dry_run:
                self._flush_description_outbox(binding, summary)
                self._add_description_attention_counts(binding, summary)
            return summary

        batches = self._manifest_payloads(
            scan_id=None if dry_run else "pending",
            entries=entries,
            complete_read=scan.complete_read,
        )
        if dry_run:
            summary.queued_batches += len(batches)
            return summary

        scan_id = self.state.begin_scan(binding.source_id, root)
        batches = self._manifest_payloads(
            scan_id=scan_id,
            entries=entries,
            complete_read=scan.complete_read,
        )
        self.state.queue_batches(binding.source_id, scan_id, batches)
        self.state.finish_scan(
            scan_id,
            complete_read=scan.complete_read,
            summary={**scan.summary(), "upserts": len(changed_entries), "deletions": len(deleted_entries)},
        )
        summary.queued_batches = len(batches)
        self._flush_pending(binding, summary)
        self._flush_description_outbox(binding, summary)
        self._add_description_attention_counts(binding, summary)
        return summary

    def _flush_pending(self, binding: SourceBinding, summary: SyncSummary) -> None:
        for batch in self.state.pending_batches(binding.source_id):
            try:
                response = self.api.submit_manifest(
                    binding.source_id,
                    batch.payload,
                    key=batch.idempotency_key,
                )
            except ApiError as exc:
                if not self._is_permanent_request_error(exc):
                    raise
                resolution = self.state.quarantine_request_error(
                    batch,
                    status=exc.problem.status,
                    code=exc.problem.code,
                    title=exc.problem.title,
                    detail=exc.problem.detail,
                    request_id=exc.problem.request_id,
                )
                summary.quarantined_batches += 1
                summary.quarantined_entries += resolution.failed_entries
                summary.rejected_entries += resolution.failed_entries
                summary.failed += resolution.failed_entries
                self.progress(
                    f"Quarantined permanently rejected manifest batch {batch.sequence + 1}"
                )
                continue
            results = response.get("results") or []
            upload_required = sum(1 for item in results if item.get("uploadRequired"))
            resolution = self.state.acknowledge_batch(batch, response)
            if resolution.state == "Failed":
                summary.quarantined_batches += 1
                summary.quarantined_entries += resolution.failed_entries
                summary.rejected_entries += resolution.failed_entries
                summary.failed += resolution.failed_entries
                self.progress(
                    f"Quarantined manifest batch {batch.sequence + 1} "
                    f"({resolution.failed_entries} entr{'y' if resolution.failed_entries == 1 else 'ies'})"
                )
            summary.duplicates_linked += int(
                (response.get("counts") or {}).get("duplicatesLinked") or 0
            )
            summary.upload_required += upload_required
            summary.batches_sent += 1
            if resolution.state == "Sent":
                self.progress(f"Accepted manifest batch {batch.sequence + 1}")
            if upload_required:
                raise ApiError(
                    ApiProblem(
                        409,
                        "Local source requested an upload",
                        "The service requested object upload for a Local source; the batch was quarantined.",
                    )
                )

    def _flush_description_outbox(
        self,
        binding: SourceBinding,
        summary: SyncSummary,
    ) -> None:
        self._reconcile_sent_descriptions(binding)
        self._recover_supported_description_skips(binding, summary)
        tasks = self.state.due_description_tasks(binding.source_id)
        if tasks:
            self.progress(f"Preparing {len(tasks)} scene preview(s)")
        for task in tasks:
            try:
                outcome = self._stage_description(task)
            except ScenePreviewError as exc:
                if self._skip_description(
                    task,
                    reason="UnsupportedPhoto",
                    code=exc.code,
                    message=str(exc),
                ):
                    self.progress(
                        f"Scene description skipped for unsupported {task.file_name}"
                    )
                else:
                    self.progress(
                        f"Scene preview for {task.file_name} will retry later"
                    )
                continue
            except OSError:
                self.state.defer_description(
                    task.job_id,
                    retry_after_seconds=60,
                    code="source_photo_unavailable",
                    message="The source photo is no longer readable.",
                )
                self.progress(f"Scene preview for {task.file_name} will retry later")
                continue
            except ApiError as exc:
                code = (exc.problem.code or "SCENE_STAGING_FAILED").strip().upper()
                if code in {
                    "PROCESSINGJOBNOTPREPARING",
                    "PROCESSING_JOB_NOT_PREPARING",
                }:
                    outcome = self._reconcile_not_preparing(task)
                    if outcome == "Sent":
                        summary.descriptions_staged += 1
                    elif outcome == "Failed":
                        self.progress(
                            f"Scene preview for {task.file_name} needs a server retry"
                        )
                    else:
                        self.progress(
                            f"Scene preview for {task.file_name} will retry later"
                        )
                    continue
                if code in {
                    "DESCRIPTIONALREADYAVAILABLE",
                    "DESCRIPTION_ALREADY_AVAILABLE",
                    "PROCESSINGJOBALREADYQUEUED",
                    "PROCESSING_JOB_ALREADY_QUEUED",
                    "UPLOADALREADYCOMPLETED",
                    "UPLOAD_ALREADY_COMPLETED",
                }:
                    self.state.mark_description_sent(task.job_id)
                    summary.descriptions_staged += 1
                    continue
                if self._is_permanent_request_error(exc):
                    self.state.quarantine_description(
                        task.job_id,
                        code=code,
                        message="The scene preview request was permanently rejected.",
                    )
                    self.progress(f"Scene preview for {task.file_name} needs attention")
                else:
                    self.state.defer_description(
                        task.job_id,
                        retry_after_seconds=30,
                        code=code,
                        message="Scene preview staging is temporarily unavailable.",
                    )
                    summary.failed += 1
                    self.progress(f"Scene preview for {task.file_name} will retry later")
                continue

            if outcome == "Sent":
                summary.descriptions_staged += 1
                self.progress(f"Staged scene preview for {task.file_name}")
            elif outcome == "QuotaDeferred":
                self.progress(f"Scene preview for {task.file_name} is waiting for quota")
                break
            elif outcome == "Pending":
                self.progress(f"Scene preview for {task.file_name} will retry later")

    def _recover_supported_description_skips(
        self,
        binding: SourceBinding,
        summary: SyncSummary,
    ) -> None:
        """Retry previously rejected MPO/JPG files once after decoder upgrades."""

        setting_key = "scene-preview-capability-version"
        if (
            self.state.get_setting(setting_key)
            == SCENE_PREVIEW_CAPABILITY_VERSION
        ):
            return
        skipped = self.state.list_description_outbox(
            state="Skipped",
            limit=1_000,
        )
        retry_deferred = False
        recovered = 0
        for task in skipped:
            if task.source_id != binding.source_id:
                continue
            error_code = str((task.error or {}).get("code") or "")
            if error_code != ScenePreviewError.code:
                continue
            path = Path(task.local_path)
            if path.suffix.casefold() not in {".jpg", ".jpeg", ".mpo"}:
                continue
            try:
                preview = self.preview_factory(path)
            except (OSError, ScenePreviewError):
                continue
            if (
                preview.source_sha256_hex.casefold()
                != task.asset_content_sha256.casefold()
            ):
                continue
            try:
                job = self.api.retry_job(
                    task.job_id,
                    key=(
                        f"scene-retry-capability:{task.job_id}:"
                        f"{SCENE_PREVIEW_CAPABILITY_VERSION}"
                    ),
                )
            except ApiError:
                retry_deferred = True
                continue
            if str(job.get("status") or "") != "Preparing":
                retry_deferred = True
                continue
            self.state.retry_description(task.job_id)
            recovered += 1
        if recovered:
            summary.descriptions_recovered += recovered
            self.progress(
                f"Recovered {recovered} scene description(s) after "
                "adding MPO camera-photo support"
            )
        if not retry_deferred and len(skipped) < 1_000:
            self.state.set_setting(
                setting_key,
                SCENE_PREVIEW_CAPABILITY_VERSION,
            )

    def _reconcile_not_preparing(self, task: DescriptionOutboxItem) -> str:
        try:
            job = self.api.get_job(task.job_id)
        except ApiError:
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=30,
                code="DESCRIPTION_STATUS_UNAVAILABLE",
                message="Scene-description status is temporarily unavailable.",
            )
            return "Pending"
        status = str(job.get("status") or "")
        if status in {"Succeeded", "Queued", "Running"}:
            self.state.mark_description_sent(task.job_id)
            return "Sent"
        if status == "DeferredQuota":
            retry_seconds = 3600
            next_attempt = self._parse_utc(
                str(job.get("nextAttemptAtUtc") or "")
            )
            if next_attempt is not None:
                retry_seconds = max(
                    30,
                    int(
                        (
                            next_attempt - datetime.now(timezone.utc)
                        ).total_seconds()
                    ),
                )
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=retry_seconds,
                code="MONTHLY_DESCRIPTION_QUOTA",
                message="Scene description is waiting for monthly quota.",
            )
            return "Pending"
        if status in {"Failed", "Cancelled"}:
            self.state.quarantine_description(
                task.job_id,
                code=str(job.get("errorCode") or "DESCRIPTION_JOB_FAILED"),
                message=(
                    "The server job needs attention. Run 'imagetracker jobs retry "
                    f"{task.job_id}' after fixing the issue."
                ),
            )
            return "Failed"
        self.state.defer_description(
            task.job_id,
            retry_after_seconds=30,
            code="DESCRIPTION_NOT_READY",
            message="Scene description is not ready for another preview yet.",
        )
        return "Pending"

    def _reconcile_sent_descriptions(self, binding: SourceBinding) -> None:
        for task in self.state.due_sent_description_tasks(
            binding.source_id, limit=100
        ):
            try:
                job = self.api.get_job(task.job_id)
            except ApiError as exc:
                if exc.problem.status == 404:
                    self.state.fail_description_from_server(
                        task.job_id,
                        code=exc.problem.code or "DESCRIPTION_JOB_NOT_FOUND",
                        message="The scene-description job is no longer available.",
                    )
                else:
                    self.state.schedule_sent_description_check(
                        task.job_id, retry_after_seconds=300
                    )
                continue
            status = str(job.get("status") or "")
            if status == "Succeeded":
                self.state.confirm_description_complete(task.job_id)
            elif status == "Preparing":
                self.state.retry_description(task.job_id, allow_sent=True)
            elif status in {"Failed", "Cancelled"}:
                self.state.fail_description_from_server(
                    task.job_id,
                    code=str(job.get("errorCode") or "DESCRIPTION_FAILED"),
                    message=str(
                        job.get("userMessage")
                        or "Scene description needs attention."
                    ),
                )
            else:
                retry_seconds = 300
                next_attempt = self._parse_utc(
                    str(job.get("nextAttemptAtUtc") or "")
                )
                if next_attempt is not None:
                    retry_seconds = max(
                        30,
                        int(
                            (
                                next_attempt - datetime.now(timezone.utc)
                            ).total_seconds()
                        ),
                    )
                self.state.schedule_sent_description_check(
                    task.job_id, retry_after_seconds=retry_seconds
                )

    def _stage_description(self, task: DescriptionOutboxItem) -> str:
        path = Path(task.local_path)
        stat_before = path.stat()
        preview = self.preview_factory(path)
        if (
            preview.source_sha256_hex.lower()
            != task.asset_content_sha256.lower()
        ):
            return (
                "Skipped"
                if self._skip_description(
                    task,
                    reason="SourceChanged",
                    code="source_photo_changed",
                    message="The source photo changed after its metadata was accepted.",
                )
                else "Pending"
            )
        stat_after = path.stat()
        if (
            stat_after.st_size != stat_before.st_size
            or stat_after.st_mtime_ns != stat_before.st_mtime_ns
        ):
            return (
                "Skipped"
                if self._skip_description(
                    task,
                    reason="SourceChanged",
                    code="source_photo_changed",
                    message="The source photo changed while its preview was being prepared.",
                )
                else "Pending"
            )
        upload_request = {
            "sourceId": task.source_id,
            "occurrenceId": task.occurrence_id,
            "assetContentSha256": task.asset_content_sha256,
            "objectSha256": preview.sha256_hex,
            "fileName": task.file_name,
            "mediaType": "Photo",
            "objectMimeType": preview.content_type,
            "objectByteSize": preview.byte_size,
            "purpose": "TemporaryProcessing",
            "processingJobId": task.job_id,
        }
        replacement = 0
        while True:
            plan = self.api.create_upload_plan(
                upload_request,
                key=(
                    f"scene-plan:{task.job_id}:{preview.sha256_hex[:24]}:"
                    f"{task.attempt_count}:{replacement}"
                ),
            )
            self._validate_plan_identity(plan, task)
            disposition = str(plan.get("disposition") or "")
            if disposition == "Deferred":
                self.state.defer_all_descriptions_for_quota(
                    task.job_id,
                    retry_after_seconds=int(plan.get("retryAfterSeconds") or 3600),
                )
                return "QuotaDeferred"
            if disposition == "AlreadyStored":
                self.state.mark_description_sent(task.job_id)
                return "Sent"
            if disposition != "LeaseHeld":
                break

            upload_session_id = str(plan.get("uploadSessionId") or "")
            if not upload_session_id:
                self.state.defer_description(
                    task.job_id,
                    retry_after_seconds=int(plan.get("retryAfterSeconds") or 60),
                    code="TEMPORARY_UPLOAD_LEASE_HELD",
                    message="A prior scene preview upload is still being resolved.",
                )
                return "Pending"
            session = self.api.get_upload_session(upload_session_id)
            session_status = str(session.get("status") or "")
            if session_status == "Completed":
                self.state.mark_description_sent(task.job_id)
                return "Sent"
            if session_status == "Uploading":
                try:
                    self._complete_preview_upload(
                        task=task,
                        upload_session_id=upload_session_id,
                        preview_sha256=preview.sha256_hex,
                    )
                except ApiError as exc:
                    if not self._is_uploaded_object_missing(exc):
                        raise
                    self.api.cancel_upload(
                        upload_session_id,
                        reason="Temporary preview was not present; replace the stale lease.",
                        key=f"scene-cancel:{task.job_id}:{upload_session_id}",
                    )
                    replacement += 1
                    if replacement <= 1:
                        continue
                else:
                    self.state.mark_description_sent(task.job_id)
                    return "Sent"
            elif session_status in {"Cancelled", "Expired", "Failed"}:
                replacement += 1
                if replacement <= 1:
                    continue
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=int(plan.get("retryAfterSeconds") or 60),
                code="TEMPORARY_UPLOAD_LEASE_HELD",
                message="A prior scene preview upload is still being resolved.",
            )
            return "Pending"
        if disposition != "UploadRequired":
            self.state.quarantine_description(
                task.job_id,
                code="unexpected_upload_disposition",
                message="The service did not authorize temporary preview staging.",
            )
            return "Failed"
        if str(plan.get("strategy") or "") != "SinglePart":
            self.state.quarantine_description(
                task.job_id,
                code="unsupported_upload_strategy",
                message="The temporary preview requires an unsupported upload strategy.",
            )
            return "Failed"
        upload_session_id = str(plan.get("uploadSessionId") or "")
        signed = plan.get("singlePart")
        if not upload_session_id or not isinstance(signed, Mapping):
            self.state.quarantine_description(
                task.job_id,
                code="invalid_upload_plan",
                message="The temporary preview upload plan was incomplete.",
            )
            return "Failed"
        url = str(signed.get("url") or "")
        method = str(signed.get("method") or "")
        raw_headers = signed.get("headers")
        if not url.startswith("https://") or method != "PUT" or not isinstance(raw_headers, Mapping):
            self.state.quarantine_description(
                task.job_id,
                code="invalid_upload_plan",
                message="The temporary preview upload plan was invalid.",
            )
            return "Failed"
        headers = {str(key): str(value) for key, value in raw_headers.items()}
        signed_expiry = self._parse_utc(str(signed.get("expiresAtUtc") or ""))
        if signed_expiry is not None and signed_expiry <= datetime.now(timezone.utc):
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=30,
                code="TEMPORARY_UPLOAD_URL_EXPIRED",
                message="The temporary scene preview upload URL expired.",
            )
            return "Pending"
        try:
            self.api.put_signed_upload(url, preview.content, headers=headers)
        except ApiError as exc:
            code = (exc.problem.code or "").strip().upper()
            if code not in {
                "TEMPORARY_UPLOAD_NETWORK_ERROR",
                "TEMPORARY_UPLOAD_REJECTED",
            }:
                raise
            try:
                self.api.cancel_upload(
                    upload_session_id,
                    reason="Replace a temporary preview whose signed upload did not complete.",
                    key=f"scene-cancel:{task.job_id}:{upload_session_id}",
                )
            except ApiError:
                raise exc
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=30,
                code=code,
                message="The temporary scene preview upload will be retried with a fresh URL.",
            )
            return "Pending"
        self._complete_preview_upload(
            task=task,
            upload_session_id=upload_session_id,
            preview_sha256=preview.sha256_hex,
        )
        self.state.mark_description_sent(task.job_id)
        return "Sent"

    def _skip_description(
        self,
        task: DescriptionOutboxItem,
        *,
        reason: str,
        code: str,
        message: str,
    ) -> bool:
        try:
            self.api.cancel_job(
                task.job_id,
                reason=reason,
                key=f"scene-cancel-job:{task.job_id}:{reason}",
            )
        except ApiError:
            self.state.defer_description(
                task.job_id,
                retry_after_seconds=60,
                code="DESCRIPTION_SKIP_PENDING",
                message="Scene-description cleanup will retry.",
            )
            return False
        self.state.mark_description_skipped(
            task.job_id, code=code, message=message
        )
        return True

    def _complete_preview_upload(
        self,
        *,
        task: DescriptionOutboxItem,
        upload_session_id: str,
        preview_sha256: str,
    ) -> None:
        self.api.complete_upload(
            upload_session_id,
            {"objectSha256": preview_sha256, "parts": []},
            key=f"scene-complete:{task.job_id}:{upload_session_id}",
        )

    @staticmethod
    def _is_uploaded_object_missing(error: ApiError) -> bool:
        normalized = "".join(
            character
            for character in (error.problem.code or "").upper()
            if character.isalnum()
        )
        return normalized == "UPLOADEDOBJECTNOTFOUND"

    @staticmethod
    def _parse_utc(value: str) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _validate_plan_identity(
        plan: Mapping[str, Any], task: DescriptionOutboxItem
    ) -> None:
        if (
            str(plan.get("mediaAssetId") or "") != task.media_asset_id
            or str(plan.get("occurrenceId") or "") != task.occurrence_id
        ):
            raise ApiError(
                ApiProblem(
                    422,
                    "Invalid upload plan",
                    "The temporary upload plan did not match the accepted photo.",
                    code="UPLOAD_PLAN_IDENTITY_MISMATCH",
                )
            )

    def _add_description_attention_counts(
        self, binding: SourceBinding, summary: SyncSummary
    ) -> None:
        counts = self.state.description_counts(binding.source_id)
        summary.description_pending = counts["Pending"]
        summary.description_deferred = counts["Deferred"]
        summary.description_quarantined = counts["Failed"]
        summary.failed += summary.description_quarantined

    @staticmethod
    def _is_permanent_request_error(error: ApiError) -> bool:
        if (error.problem.code or "").strip().upper() in {
            "TEMPORARY_UPLOAD_NETWORK_ERROR",
            "TEMPORARY_UPLOAD_REJECTED",
            "TEMPORARY_UPLOAD_URL_EXPIRED",
        }:
            return False
        status = error.problem.status
        if status in {400, 403, 404, 422}:
            return True
        if status != 409:
            return False
        code = (error.problem.code or "").strip().upper()
        return not code.endswith("REQUEST_IN_PROGRESS")

    @staticmethod
    def _manifest_payloads(
        *,
        scan_id: str | None,
        entries: Sequence[Mapping[str, Any]],
        complete_read: bool,
    ) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for offset in range(0, len(entries), MANIFEST_BATCH_SIZE):
            batch_entries = [dict(item) for item in entries[offset : offset + MANIFEST_BATCH_SIZE]]
            payload: dict[str, Any] = {
                "kind": "Full",
                "permissionState": "NotApplicable",
                "deletionDetectionReliable": complete_read,
                "entries": batch_entries,
            }
            if scan_id and scan_id != "pending":
                payload["snapshotId"] = scan_id
            payloads.append(payload)
        return payloads
