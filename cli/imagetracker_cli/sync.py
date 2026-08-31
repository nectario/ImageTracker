from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Sequence
import uuid

from .api_client import ApiClient, ApiError, ApiProblem
from .bulk import (
    MAX_COMPRESSED_BYTES,
    MANIFEST_SCHEMA_VERSION,
    BulkArtifactError,
    iter_result_entries,
    read_result_header,
    write_manifest_gzip,
)
from .media import MediaScanner, stream_sha256
from .scene_preview import (
    SCENE_PREVIEW_CAPABILITY_VERSION,
    ScenePreview,
    ScenePreviewError,
    prepare_scene_preview,
)
from .state import (
    BulkManifestOutboxItem,
    DescriptionOutboxItem,
    LocalState,
    OutboxBatch,
    SourceBinding,
)


MANIFEST_BATCH_SIZE = 100
DEFAULT_ENRICHMENT_LIMIT = 100
BULK_AUTO_BATCH_THRESHOLD = 10
BULK_AUTO_ROW_THRESHOLD = 1_000
BULK_RESULT_APPLY_PAGE_SIZE = 500
BULK_POLL_MAX_ATTEMPTS = 120


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
    bulk_state: str | None = None
    bulk_phase: str | None = None
    bulk_processed: int = 0
    bulk_total: int = 0
    bulk_captured_batches: int = 0
    bulk_completed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EnrichmentSummary:
    source_id: str
    root_path: str
    limit: int
    descriptions_staged: int = 0
    descriptions_recovered: int = 0
    description_pending: int = 0
    description_deferred: int = 0
    description_quarantined: int = 0
    failed: int = 0

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
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.api = api
        self.state = state
        self.scanner = scanner or MediaScanner(state)
        self.progress = progress or (lambda _message: None)
        self.preview_factory = preview_factory
        self.hash_file = hash_file
        self.sleep = sleep

    def sync(
        self,
        binding: SourceBinding,
        *,
        dry_run: bool = False,
        force_rehash: bool = False,
        scan_workers: int | None = None,
        fast_add: bool = False,
        with_enrichment: bool = False,
        enrichment_limit: int = DEFAULT_ENRICHMENT_LIMIT,
        transport: str = "auto",
        bulk_max_rows: int | None = None,
    ) -> SyncSummary:
        self._validate_enrichment_limit(enrichment_limit)
        if transport not in {"auto", "bulk", "batch"}:
            raise ValueError("Sync transport must be auto, bulk, or batch")
        if bulk_max_rows is not None and (
            isinstance(bulk_max_rows, bool)
            or not isinstance(bulk_max_rows, int)
            or not 100 <= bulk_max_rows <= 250_000
        ):
            raise ValueError("Bulk row limit must be between 100 and 250000")
        if transport == "batch" and bulk_max_rows is not None:
            raise ValueError("--bulk-max-rows cannot be used with batch transport")
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

        pending = self.state.pending_batches(binding.source_id)
        summary.resumed_batches = len(pending)
        if pending and not dry_run:
            self.progress(f"Resuming {len(pending)} saved manifest batch(es)")
            if not self._deliver_pending(
                binding,
                summary,
                transport=transport,
                bulk_max_rows=bulk_max_rows,
            ):
                self._add_description_attention_counts(
                    binding,
                    summary,
                    count_as_failed=with_enrichment,
                )
                return summary
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
                if with_enrichment:
                    self._flush_description_outbox(
                        binding,
                        summary,
                        limit=enrichment_limit,
                    )
                self._add_description_attention_counts(
                    binding,
                    summary,
                    count_as_failed=with_enrichment,
                )
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
        if not self._deliver_pending(
            binding,
            summary,
            transport=transport,
            bulk_max_rows=bulk_max_rows,
        ):
            self._add_description_attention_counts(
                binding,
                summary,
                count_as_failed=with_enrichment,
            )
            return summary
        if with_enrichment:
            self._flush_description_outbox(
                binding,
                summary,
                limit=enrichment_limit,
            )
        self._add_description_attention_counts(
            binding,
            summary,
            count_as_failed=with_enrichment,
        )
        return summary

    def enrich(
        self,
        binding: SourceBinding,
        *,
        limit: int,
    ) -> EnrichmentSummary:
        """Stage bounded, already-queued scene previews without synchronizing."""

        if binding.storage_mode != "Local":
            raise ValueError(
                "Scene-preview staging currently supports Local sources only."
            )
        self._validate_enrichment_limit(limit)
        summary = EnrichmentSummary(
            source_id=binding.source_id,
            root_path=binding.root_path,
            limit=limit,
        )
        self._flush_description_outbox(binding, summary, limit=limit)
        self._add_description_attention_counts(
            binding,
            summary,
            count_as_failed=True,
        )
        return summary

    def _deliver_pending(
        self,
        binding: SourceBinding,
        summary: SyncSummary,
        *,
        transport: str,
        bulk_max_rows: int | None,
    ) -> bool:
        active = self.state.active_bulk_manifest(binding.source_id)
        if active is not None:
            if transport == "batch":
                escape = self._escape_active_bulk_to_batch(
                    binding,
                    active,
                    summary,
                )
                if escape == "Fallback":
                    return self._flush_pending(binding, summary)
                if escape == "Paused":
                    return False
                active = self._require_local_bulk(active.bulk_id)
            outcome = self._resume_bulk_manifest(binding, active, summary)
            if outcome == "Paused":
                return False
            if outcome == "Complete":
                return self._bulk_segment_finished(binding, summary)
            self.progress(
                "Bulk metadata import could not complete cleanly · "
                "falling back to timeout-safe batches"
            )
            return self._flush_pending(binding, summary)

        batches = self.state.pending_batches(binding.source_id)
        if not batches:
            summary.queued_batches = 0
            return True
        if transport == "batch":
            return self._flush_pending(binding, summary)

        selected_batches = self._bounded_bulk_batches(
            batches,
            maximum_rows=bulk_max_rows,
        )
        entries = [
            dict(entry)
            for batch in selected_batches
            for entry in (batch.payload.get("entries") or [])
        ]
        threshold_met = (
            len(selected_batches) >= BULK_AUTO_BATCH_THRESHOLD
            or len(entries) >= BULK_AUTO_ROW_THRESHOLD
        )
        if transport == "auto" and not threshold_met:
            return self._flush_pending(binding, summary)
        if not self._bulk_entries_eligible(selected_batches, entries):
            self.progress(
                "Bulk metadata requires hash-enriched additions only · "
                "using timeout-safe batches for deletions or pending hashes"
            )
            return self._flush_pending(binding, summary)

        snapshot_id = str(uuid.uuid4())
        artifact_path = (
            self.state.path.parent
            / "bulk-manifests"
            / f"{snapshot_id}.ndjson.gz"
        )
        try:
            artifact = write_manifest_gzip(
                artifact_path,
                source_id=binding.source_id,
                snapshot_id=snapshot_id,
                entries=entries,
                permission_state=str(
                    selected_batches[0].payload.get("permissionState")
                    or "NotApplicable"
                ),
                client_cursor=(
                    str(selected_batches[0].payload["clientCursor"])
                    if selected_batches[0].payload.get("clientCursor") is not None
                    else None
                ),
            )
            active = self.state.queue_bulk_manifest(
                binding.source_id,
                snapshot_id,
                artifact_path=artifact.path,
                artifact_sha256=artifact.compressed_sha256,
                artifact_bytes=artifact.compressed_bytes,
                entry_count=artifact.entry_count,
                superseded_batch_ids=tuple(
                    batch.batch_id for batch in selected_batches
                ),
            )
        except (BulkArtifactError, OSError, ValueError) as exc:
            self.progress(
                "Bulk metadata preparation was not usable · "
                f"using timeout-safe batches ({type(exc).__name__})"
            )
            return self._flush_pending(binding, summary)

        self.progress(
            "Bulk metadata · Prepared one manifest · "
            f"{active.entry_count:,} rows from "
            f"{len(active.superseded_batch_ids):,} saved batches"
        )
        outcome = self._resume_bulk_manifest(binding, active, summary)
        if outcome == "Paused":
            return False
        if outcome == "Complete":
            return self._bulk_segment_finished(binding, summary)
        self.progress(
            "Bulk metadata import could not complete cleanly · "
            "falling back to timeout-safe batches"
        )
        return self._flush_pending(binding, summary)

    @staticmethod
    def _bounded_bulk_batches(
        batches: Sequence[OutboxBatch],
        *,
        maximum_rows: int | None,
    ) -> list[OutboxBatch]:
        if maximum_rows is None:
            return list(batches)
        selected: list[OutboxBatch] = []
        rows = 0
        for batch in batches:
            batch_rows = len(batch.payload.get("entries") or [])
            if selected and rows + batch_rows > maximum_rows:
                break
            if not selected and batch_rows > maximum_rows:
                selected.append(batch)
                break
            selected.append(batch)
            rows += batch_rows
        return selected

    def _bulk_segment_finished(
        self,
        binding: SourceBinding,
        summary: SyncSummary,
    ) -> bool:
        remaining = len(self.state.pending_batches(binding.source_id))
        summary.queued_batches = remaining
        if remaining:
            self.progress(
                "Bulk metadata segment complete · "
                f"{remaining} saved batch(es) remain; rerun sync to continue"
            )
            return False
        return True

    def _escape_active_bulk_to_batch(
        self,
        binding: SourceBinding,
        item: BulkManifestOutboxItem,
        summary: SyncSummary,
    ) -> str:
        if item.server_import_id is None:
            self.state.fail_bulk_manifest(
                item.bulk_id,
                state="Cancelled",
                code="BULK_REPLACED_BY_BATCH",
                message="The prepared bulk import was replaced by batch transport.",
            )
            self.progress(
                "Bulk metadata was only prepared locally · switching to batch transport"
            )
            return "Fallback"
        try:
            fresh = self.api.get_manifest_import(
                binding.source_id,
                str(item.server_import_id),
            )
            status = str(fresh.get("status") or item.server_status or "")
            self.state.update_bulk_manifest_status(
                item.bulk_id,
                status=status,
                phase=str(fresh.get("phase") or item.server_phase or status),
                processed_entries=int(
                    fresh.get("processedEntryCount") or item.processed_entries
                ),
            )
            item = self._require_local_bulk(item.bulk_id)
            authoritative_payload: Mapping[str, Any] = fresh
        except ApiError as exc:
            if exc.problem.status == 401:
                raise
            if self._bulk_error_is_transient(exc):
                self._set_bulk_summary(summary, item)
                summary.queued_batches = len(
                    self.state.pending_batches(binding.source_id)
                )
                self.progress(
                    "Cannot safely switch to batch while the submitted bulk "
                    "status is unavailable · rerun sync"
                )
                return "Paused"
            self.state.fail_bulk_manifest(
                item.bulk_id,
                state="Cancelled",
                server_status=item.server_status,
                code=exc.problem.code or "BULK_STATUS_REJECTED",
                message=exc.problem.detail,
            )
            return "Fallback"

        if item.server_status == "AwaitingUpload":
            self.state.fail_bulk_manifest(
                item.bulk_id,
                state="Cancelled",
                server_status="AwaitingUpload",
                code="BULK_REPLACED_BY_BATCH",
                message="The unsubmitted bulk upload was replaced by batch transport.",
            )
            self.progress(
                "Bulk upload had not been submitted · switching to batch transport"
            )
            return "Fallback"
        if item.server_status in {"Queued", "Running", "RetryDue"}:
            self._set_bulk_summary(summary, item)
            summary.queued_batches = len(self.state.pending_batches(binding.source_id))
            self.progress(
                "Submitted bulk metadata is still running · batch transport "
                "will not race it; rerun sync after it finishes"
            )
            return "Paused"
        return "Resume"

    @staticmethod
    def _bulk_entries_eligible(
        batches: Sequence[OutboxBatch],
        entries: Sequence[Mapping[str, Any]],
    ) -> bool:
        if not entries:
            return False
        permission_states = {
            str(batch.payload.get("permissionState") or "NotApplicable")
            for batch in batches
        }
        client_cursors = {
            (
                str(batch.payload.get("clientCursor"))
                if batch.payload.get("clientCursor") is not None
                else None
            )
            for batch in batches
        }
        if (
            any(str(batch.payload.get("kind") or "") != "Full" for batch in batches)
            or len(permission_states) != 1
            or len(client_cursors) != 1
        ):
            return False
        for entry in entries:
            content_hash = entry.get("contentSha256")
            if (
                entry.get("operation") != "Upsert"
                or not isinstance(content_hash, str)
                or len(content_hash) != 64
            ):
                return False
            try:
                int(content_hash, 16)
            except ValueError:
                return False
        return True

    def _resume_bulk_manifest(
        self,
        binding: SourceBinding,
        item: BulkManifestOutboxItem,
        summary: SyncSummary,
    ) -> str:
        self._set_bulk_summary(summary, item)
        create_response: Mapping[str, Any] | None = None
        try:
            if item.server_import_id is None:
                self.progress(
                    "Bulk metadata · Creating import · "
                    f"{item.entry_count:,} rows"
                )
                captured_batches = self._captured_bulk_batches(item)
                first_payload = captured_batches[0].payload
                create_response = self.api.create_manifest_import(
                    binding.source_id,
                    {
                        "snapshotId": item.snapshot_id,
                        "kind": "Full",
                        "permissionState": str(
                            first_payload.get("permissionState") or "NotApplicable"
                        ),
                        "deletionDetectionReliable": False,
                        "clientCursor": first_payload.get("clientCursor"),
                        "schemaVersion": MANIFEST_SCHEMA_VERSION,
                        "checksumSha256": item.artifact_sha256,
                        "byteSize": item.artifact_bytes,
                        "entryCount": item.entry_count,
                    },
                    key=item.idempotency_key,
                )
                server_import_id = str(create_response.get("importId") or "")
                if not server_import_id:
                    raise ValueError("Bulk import response did not include importId")
                status = str(create_response.get("status") or "AwaitingUpload")
                phase = str(create_response.get("phase") or "WaitingForUpload")
                self.state.set_bulk_manifest_server_import(
                    item.bulk_id,
                    server_import_id=server_import_id,
                    status=status,
                    phase=phase,
                )
                item = self._require_local_bulk(item.bulk_id)

            # Once the server ID exists, its current state is authoritative.
            # Refreshing here reconciles lost complete responses and prevents
            # a cached RetryDue state from becoming a local deadlock.
            fresh = self.api.get_manifest_import(
                binding.source_id,
                str(item.server_import_id),
            )
            self.state.update_bulk_manifest_status(
                item.bulk_id,
                status=str(fresh.get("status") or item.server_status or "Queued"),
                phase=str(fresh.get("phase") or item.server_phase or "Preparing"),
                processed_entries=int(
                    fresh.get("processedEntryCount") or item.processed_entries
                ),
            )
            item = self._require_local_bulk(item.bulk_id)
            authoritative_payload: Mapping[str, Any] = fresh

            if item.server_status == "AwaitingUpload":
                plan_response = create_response
                if self._signed_transfer(plan_response) is None:
                    try:
                        plan_response = self.api.refresh_manifest_import_upload(
                            binding.source_id,
                            str(item.server_import_id),
                            key=f"{item.idempotency_key}:upload",
                        )
                    except ApiError as exc:
                        if "NOT_AWAITING_UPLOAD" not in str(
                            exc.problem.code or ""
                        ).upper():
                            raise
                        reconciled = self.api.get_manifest_import(
                            binding.source_id,
                            str(item.server_import_id),
                        )
                        self.state.update_bulk_manifest_status(
                            item.bulk_id,
                            status=str(reconciled.get("status") or "Queued"),
                            phase=str(reconciled.get("phase") or "Preparing"),
                            processed_entries=int(
                                reconciled.get("processedEntryCount") or 0
                            ),
                        )
                        item = self._require_local_bulk(item.bulk_id)
                        authoritative_payload = reconciled
                        plan_response = None
                if item.server_status != "AwaitingUpload":
                    plan_response = None
                else:
                    transfer = self._signed_transfer(plan_response)
                    if transfer is None:
                        raise ValueError(
                            "Bulk upload plan did not include a signed request"
                        )
                    self.progress(
                        "Bulk metadata · Uploading one manifest · "
                        f"{item.artifact_bytes / (1024 * 1024):,.1f} MiB"
                    )
                    self.api.put_signed_file(
                        str(transfer["url"]),
                        Path(item.artifact_path),
                        headers=dict(transfer.get("headers") or {}),
                    )
                    completed = self.api.complete_manifest_import(
                        binding.source_id,
                        str(item.server_import_id),
                        key=f"{item.idempotency_key}:complete",
                    )
                    status = str((completed or {}).get("status") or "Queued")
                    phase = str((completed or {}).get("phase") or "Queued")
                    self.state.update_bulk_manifest_status(
                        item.bulk_id,
                        status=status,
                        phase=phase,
                        processed_entries=int(
                            (completed or {}).get("processedEntryCount") or 0
                        ),
                    )
                    item = self._require_local_bulk(item.bulk_id)
                    authoritative_payload = completed or {
                        "status": status,
                        "phase": phase,
                        "processedEntryCount": item.processed_entries,
                        "entryCount": item.entry_count,
                        "counts": {},
                    }

            payload: Mapping[str, Any] = authoritative_payload
            last_progress: tuple[str, str, int] | None = None
            for attempt in range(BULK_POLL_MAX_ATTEMPTS):
                status = str(payload.get("status") or item.server_status or "")
                if status in {
                    "Succeeded",
                    "CompletedWithErrors",
                    "FailedPermanent",
                    "Cancelled",
                    "Expired",
                }:
                    break
                if status == "RetryDue":
                    self._set_bulk_summary(summary, self._require_local_bulk(item.bulk_id))
                    self.progress(
                        "Bulk metadata · Waiting for an automatic retry · "
                        f"{summary.bulk_processed:,}/{summary.bulk_total:,} rows"
                    )
                    return "Paused"
                payload = self.api.get_manifest_import(
                    binding.source_id,
                    str(item.server_import_id),
                )
                status = str(payload.get("status") or "")
                phase = str(payload.get("phase") or status or "Processing")
                processed = int(payload.get("processedEntryCount") or 0)
                self.state.update_bulk_manifest_status(
                    item.bulk_id,
                    status=status,
                    phase=phase,
                    processed_entries=processed,
                )
                marker = (status, phase, processed)
                if marker != last_progress:
                    percent = (processed / item.entry_count * 100) if item.entry_count else 0
                    self.progress(
                        f"Bulk metadata · {phase} · {processed:,}/{item.entry_count:,} "
                        f"rows · {percent:.1f}%"
                    )
                    last_progress = marker
                if status not in {
                    "Succeeded",
                    "CompletedWithErrors",
                    "FailedPermanent",
                    "Cancelled",
                    "Expired",
                }:
                    self.sleep(min(1.0 + attempt // 10, 5.0))
            else:
                self._set_bulk_summary(summary, self._require_local_bulk(item.bulk_id))
                self.progress(
                    "Bulk metadata continues on the server · rerun sync to resume watching"
                )
                return "Paused"

            status = str(payload.get("status") or "")
            if status != "Succeeded" or self._count(payload, "rejected") > 0:
                return self._close_bulk_for_fallback(item, status, payload)
            return self._apply_bulk_result(binding, item.bulk_id, payload, summary)
        except ApiError as exc:
            if exc.problem.status == 401:
                raise
            if self._bulk_error_is_transient(exc):
                current = self._require_local_bulk(item.bulk_id)
                self._set_bulk_summary(summary, current)
                summary.queued_batches = len(
                    self.state.pending_batches(binding.source_id)
                )
                self.progress(
                    "Bulk metadata paused because the service is temporarily "
                    "unavailable · saved work will resume"
                )
                return "Paused"
            self.state.fail_bulk_manifest(
                item.bulk_id,
                state="FailedPermanent",
                server_status=item.server_status,
                code=exc.problem.code or "BULK_IMPORT_REJECTED",
                message=exc.problem.detail,
            )
            return "Fallback"
        except (BulkArtifactError, OSError, ValueError) as exc:
            self.state.fail_bulk_manifest(
                item.bulk_id,
                state="FailedPermanent",
                server_status=item.server_status,
                code=getattr(exc, "code", "BULK_RESULT_INVALID"),
                message=str(exc),
            )
            return "Fallback"

    @staticmethod
    def _signed_transfer(
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if not payload:
            return None
        nested = payload.get("upload")
        candidate = nested if isinstance(nested, Mapping) else payload
        if candidate.get("url") and str(candidate.get("method") or "PUT") == "PUT":
            return candidate
        return None

    def _apply_bulk_result(
        self,
        binding: SourceBinding,
        bulk_id: str,
        status_payload: Mapping[str, Any],
        summary: SyncSummary,
    ) -> str:
        item = self._require_local_bulk(bulk_id)
        result_path = Path(item.result_path) if item.result_path else None
        if result_path is None or not result_path.is_file():
            result_plan = self.api.get_manifest_import_result(
                binding.source_id,
                str(item.server_import_id),
            )
            result_url = str(result_plan.get("url") or "")
            checksum = str(result_plan.get("checksumSha256") or "")
            result_bytes = int(result_plan.get("byteSize") or 0)
            if not result_url or not checksum or result_bytes <= 0:
                raise ValueError("Bulk result download plan is incomplete")
            result_path = (
                self.state.path.parent
                / "bulk-results"
                / f"{item.server_import_id}.ndjson.gz"
            )
            self.progress("Bulk metadata · Downloading verified result")
            self.api.get_signed_file(
                result_url,
                result_path,
                expected_bytes=result_bytes,
                max_bytes=MAX_COMPRESSED_BYTES,
                headers=dict(result_plan.get("headers") or {}),
            )
            header = read_result_header(
                result_path,
                expected_sha256=checksum,
                expected_compressed_bytes=result_bytes,
                expected_import_id=str(item.server_import_id),
            )
            if header.entry_count != item.entry_count:
                raise ValueError("Bulk result row count does not match the manifest")
            if self._count({"counts": header.counts}, "rejected") > 0:
                raise ValueError("Bulk result contains rejected rows")
            self.state.record_bulk_manifest_result(
                bulk_id,
                result_path=result_path,
                result_sha256=checksum,
                result_bytes=result_bytes,
            )
            item = self._require_local_bulk(bulk_id)
        else:
            if not item.result_sha256 or not item.result_bytes:
                raise ValueError("Saved bulk result metadata is incomplete")
            header = read_result_header(
                result_path,
                expected_sha256=item.result_sha256,
                expected_compressed_bytes=item.result_bytes,
                expected_import_id=str(item.server_import_id),
            )
            if header.entry_count != item.entry_count:
                raise ValueError("Saved bulk result row count is inconsistent")

        entries = self._captured_bulk_entries(item)
        if len(entries) != item.entry_count:
            raise ValueError("Captured bulk manifest rows are incomplete")
        page: list[tuple[int, Mapping[str, Any], Mapping[str, Any]]] = []
        for result in iter_result_entries(
            header,
            after_row_number=item.result_applied_through,
        ):
            page.append(
                (
                    result.row_number,
                    entries[result.row_number - 1],
                    result.as_api_result(),
                )
            )
            if len(page) >= BULK_RESULT_APPLY_PAGE_SIZE:
                applied = self.state.apply_bulk_result_rows(bulk_id, tuple(page))
                self.progress(
                    f"Bulk metadata · Applying result · {applied:,}/{item.entry_count:,} rows"
                )
                page.clear()
        if page:
            applied = self.state.apply_bulk_result_rows(bulk_id, tuple(page))
            self.progress(
                f"Bulk metadata · Applying result · {applied:,}/{item.entry_count:,} rows"
            )
        current = self._require_local_bulk(bulk_id)
        if current.result_applied_through != current.entry_count:
            raise ValueError("Bulk result application stopped before the final row")
        self.state.complete_bulk_manifest(bulk_id)
        summary.bulk_completed = True
        summary.bulk_state = "Applied"
        summary.bulk_phase = "Complete"
        summary.bulk_processed = item.entry_count
        summary.bulk_total = item.entry_count
        summary.bulk_captured_batches = len(item.superseded_batch_ids)
        summary.duplicates_linked += self._count(
            {"counts": header.counts},
            "duplicatesLinked",
        )
        summary.queued_batches = len(self.state.pending_batches(binding.source_id))
        self.progress(
            f"Bulk metadata · Complete · {item.entry_count:,} rows committed"
        )
        return "Complete"

    def _captured_bulk_entries(
        self,
        item: BulkManifestOutboxItem,
    ) -> list[dict[str, Any]]:
        batches = self._captured_bulk_batches(item)
        return [
            dict(entry)
            for batch in batches
            for entry in (batch.payload.get("entries") or [])
        ]

    def _captured_bulk_batches(
        self,
        item: BulkManifestOutboxItem,
    ) -> list[OutboxBatch]:
        pending = {
            batch.batch_id: batch
            for batch in self.state.pending_batches(item.source_id)
        }
        if any(batch_id not in pending for batch_id in item.superseded_batch_ids):
            raise ValueError("A captured manifest batch is no longer pending")
        return [pending[batch_id] for batch_id in item.superseded_batch_ids]

    def _close_bulk_for_fallback(
        self,
        item: BulkManifestOutboxItem,
        status: str,
        payload: Mapping[str, Any],
    ) -> str:
        self.state.fail_bulk_manifest(
            item.bulk_id,
            state="FailedPermanent",
            server_status=status or item.server_status,
            code=str(payload.get("failureCode") or status or "BULK_IMPORT_FAILED"),
            message=str(
                payload.get("failureMessage")
                or "The bulk import did not complete without rejected rows."
            ),
        )
        return "Fallback"

    @staticmethod
    def _bulk_error_is_transient(error: ApiError) -> bool:
        code = str(error.problem.code or "").upper()
        if code.startswith("BULK_MANIFEST_UPLOAD_") or code.startswith(
            "BULK_RESULT_DOWNLOAD_"
        ):
            return True
        if error.problem.status in {0, 408, 429} or error.problem.status >= 500:
            return True
        return error.problem.status == 409 and code.endswith("REQUEST_IN_PROGRESS")

    @staticmethod
    def _count(payload: Mapping[str, Any], key: str) -> int:
        counts = payload.get("counts")
        if not isinstance(counts, Mapping):
            return 0
        snake = {
            "duplicatesLinked": "duplicates_linked",
            "ignoredDeletions": "ignored_deletions",
        }.get(key, key)
        return int(counts.get(key) or counts.get(snake) or 0)

    def _require_local_bulk(self, bulk_id: str) -> BulkManifestOutboxItem:
        item = self.state.bulk_manifest(bulk_id)
        if item is None:
            raise ValueError("The local bulk outbox item disappeared")
        return item

    @staticmethod
    def _set_bulk_summary(
        summary: SyncSummary,
        item: BulkManifestOutboxItem,
    ) -> None:
        summary.bulk_state = item.server_status or item.state
        summary.bulk_phase = item.server_phase
        summary.bulk_processed = item.processed_entries
        summary.bulk_total = item.entry_count
        summary.bulk_captured_batches = len(item.superseded_batch_ids)

    def _flush_pending(
        self,
        binding: SourceBinding,
        summary: SyncSummary,
    ) -> bool:
        split_batches, replacement_batches = (
            self.state.split_oversized_pending_batches(
                binding.source_id,
                Path(binding.root_path),
                max_entries=MANIFEST_BATCH_SIZE,
            )
        )
        if split_batches:
            self.progress(
                f"Replaced {split_batches} oversized manifest batch(es) with "
                f"{replacement_batches} timeout-safe batch(es)"
            )
        for batch in self.state.pending_batches(binding.source_id):
            try:
                response = self.api.submit_manifest(
                    binding.source_id,
                    batch.payload,
                    key=batch.idempotency_key,
                )
            except ApiError as exc:
                if not self._is_permanent_request_error(exc):
                    if exc.problem.status == 401:
                        raise
                    remaining = len(
                        self.state.pending_batches(binding.source_id)
                    )
                    summary.queued_batches = remaining
                    self.progress(
                        "Manifest delivery paused because the service is "
                        f"temporarily unavailable · {remaining} batch(es) "
                        "remain safely queued"
                    )
                    return False
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
        summary.queued_batches = len(
            self.state.pending_batches(binding.source_id)
        )
        return True

    def _flush_description_outbox(
        self,
        binding: SourceBinding,
        summary: SyncSummary | EnrichmentSummary,
        *,
        limit: int,
    ) -> None:
        self._reconcile_sent_descriptions(binding)
        self._recover_supported_description_skips(binding, summary)
        tasks = self.state.due_description_tasks(
            binding.source_id,
            limit=limit,
        )
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
        summary: SyncSummary | EnrichmentSummary,
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
        self,
        binding: SourceBinding,
        summary: SyncSummary | EnrichmentSummary,
        *,
        count_as_failed: bool,
    ) -> None:
        counts = self.state.description_counts(binding.source_id)
        summary.description_pending = counts["Pending"]
        summary.description_deferred = counts["Deferred"]
        summary.description_quarantined = counts["Failed"]
        if count_as_failed:
            summary.failed += summary.description_quarantined

    @staticmethod
    def _validate_enrichment_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("Scene-preview limit must be an integer")
        if not 1 <= limit <= 1_000:
            raise ValueError("Scene-preview limit must be between 1 and 1000")

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
