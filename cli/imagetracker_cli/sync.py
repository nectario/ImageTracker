from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .api_client import ApiClient, ApiError, ApiProblem
from .media import MediaScanner
from .state import LocalState, SourceBinding


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
    force_rehash: bool = False

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
    ):
        self.api = api
        self.state = state
        self.scanner = scanner or MediaScanner(state)
        self.progress = progress or (lambda _message: None)

    def sync(
        self,
        binding: SourceBinding,
        *,
        dry_run: bool = False,
        force_rehash: bool = False,
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

        pending = self.state.pending_batches(binding.source_id)
        summary.resumed_batches = len(pending)
        if pending and not dry_run:
            self.progress(f"Resuming {len(pending)} saved manifest batch(es)")
            self._flush_pending(binding, summary)
        elif pending:
            summary.queued_batches += len(pending)

        root = Path(binding.root_path)
        self.progress(f"Scanning {root}")
        scan = self.scanner.scan(binding.source_id, root, force_rehash=force_rehash)
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

        entries = [*changed_entries, *deleted_entries]
        if not entries:
            self.progress("Library is already in sync")
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

    @staticmethod
    def _is_permanent_request_error(error: ApiError) -> bool:
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
