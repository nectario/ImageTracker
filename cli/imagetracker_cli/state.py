from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .config import ensure_private_directory


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalized_path(path: Path | str) -> str:
    return os.path.normcase(str(Path(path).expanduser().resolve(strict=False)))


@dataclass(frozen=True)
class SourceBinding:
    source_id: str
    source_key: str
    root_path: str
    display_name: str
    storage_mode: str


@dataclass(frozen=True)
class CachedFile:
    sha256: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class OutboxBatch:
    batch_id: str
    source_id: str
    scan_id: str
    sequence: int
    idempotency_key: str
    payload: Mapping[str, Any]
    state: str = "Pending"
    failure: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class BatchResolution:
    accepted_entries: int
    failed_entries: int
    state: str
    failures: tuple[Mapping[str, Any], ...]


class LocalState:
    def __init__(self, path: Path):
        self.path = path
        ensure_private_directory(path.parent)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS LocalSetting (
                    SettingKey TEXT PRIMARY KEY,
                    SettingValue TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS SourceBinding (
                    SourceId TEXT PRIMARY KEY,
                    SourceKey TEXT NOT NULL UNIQUE,
                    RootPath TEXT NOT NULL UNIQUE,
                    DisplayName TEXT NOT NULL,
                    StorageMode TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS FileCache (
                    SourceId TEXT NOT NULL,
                    PathKey TEXT NOT NULL,
                    FilePath TEXT NOT NULL,
                    ByteSize INTEGER NOT NULL,
                    ModifiedNs INTEGER NOT NULL,
                    ContentSha256 TEXT NOT NULL,
                    MetadataJson TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL,
                    PRIMARY KEY (SourceId, PathKey)
                );
                CREATE TABLE IF NOT EXISTS KnownOccurrence (
                    SourceId TEXT NOT NULL,
                    SourceItemId TEXT NOT NULL,
                    SourceRevision TEXT NOT NULL,
                    RelativePath TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL,
                    PRIMARY KEY (SourceId, SourceItemId)
                );
                CREATE TABLE IF NOT EXISTS ScanRun (
                    ScanId TEXT PRIMARY KEY,
                    SourceId TEXT NOT NULL,
                    RootPath TEXT NOT NULL,
                    Status TEXT NOT NULL,
                    CompleteRead INTEGER NOT NULL DEFAULT 0,
                    SummaryJson TEXT,
                    StartedAtUtc TEXT NOT NULL,
                    CompletedAtUtc TEXT
                );
                CREATE TABLE IF NOT EXISTS ManifestOutbox (
                    BatchId TEXT PRIMARY KEY,
                    SourceId TEXT NOT NULL,
                    ScanId TEXT NOT NULL,
                    SequenceNumber INTEGER NOT NULL,
                    IdempotencyKey TEXT NOT NULL UNIQUE,
                    PayloadJson TEXT NOT NULL,
                    State TEXT NOT NULL DEFAULT 'Pending',
                    CreatedAtUtc TEXT NOT NULL,
                    SentAtUtc TEXT,
                    FailureJson TEXT,
                    FailedAtUtc TEXT,
                    DiscardedAtUtc TEXT,
                    UNIQUE (ScanId, SequenceNumber)
                );
                CREATE INDEX IF NOT EXISTS IX_ManifestOutbox_Pending
                    ON ManifestOutbox (SourceId, State, SequenceNumber);
                CREATE TABLE IF NOT EXISTS LegacyMigrationCheckpoint (
                    MigrationName TEXT PRIMARY KEY,
                    LastLegacyId INTEGER NOT NULL DEFAULT 0,
                    ProcessedCount INTEGER NOT NULL DEFAULT 0,
                    UpdatedAtUtc TEXT NOT NULL
                );
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(ManifestOutbox)").fetchall()
            }
            for column, declaration in (
                ("FailureJson", "TEXT"),
                ("FailedAtUtc", "TEXT"),
                ("DiscardedAtUtc", "TEXT"),
            ):
                if column not in columns:
                    connection.execute(
                        f"ALTER TABLE ManifestOutbox ADD COLUMN {column} {declaration}"
                    )
        if os.name != "nt":
            self.path.chmod(0o600)

    def get_setting(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT SettingValue FROM LocalSetting WHERE SettingKey = ?", (key,)
            ).fetchone()
        return str(row["SettingValue"]) if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO LocalSetting (SettingKey, SettingValue) VALUES (?, ?)
                ON CONFLICT (SettingKey) DO UPDATE SET SettingValue = excluded.SettingValue
                """,
                (key, value),
            )

    def installation_id(self) -> str:
        current = self.get_setting("installation-id")
        if current:
            return current
        value = str(uuid.uuid4())
        self.set_setting("installation-id", value)
        return value

    def source_key_for_root(self, root: Path) -> str:
        key = f"source-key:{normalized_path(root)}"
        current = self.get_setting(key)
        if current:
            return current
        value = uuid.uuid4().hex
        self.set_setting(key, value)
        return value

    def source_lifecycle_generation(self, root: Path | str) -> int:
        key = f"source-lifecycle:{normalized_path(root)}"
        current = self.get_setting(key)
        if current is None:
            return 0
        try:
            generation = int(current)
        except ValueError as exc:
            raise ValueError(f"Invalid local source lifecycle generation for {root}") from exc
        if generation < 0:
            raise ValueError(f"Invalid local source lifecycle generation for {root}")
        return generation

    def bind_source(self, source: Mapping[str, Any], root: Path) -> SourceBinding:
        binding = SourceBinding(
            source_id=str(source["sourceId"]),
            source_key=str(source["sourceKey"]),
            root_path=normalized_path(root),
            display_name=str(source["displayName"]),
            storage_mode=str(source["storageMode"]),
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO SourceBinding
                    (SourceId, SourceKey, RootPath, DisplayName, StorageMode, UpdatedAtUtc)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (SourceId) DO UPDATE SET
                    SourceKey = excluded.SourceKey,
                    RootPath = excluded.RootPath,
                    DisplayName = excluded.DisplayName,
                    StorageMode = excluded.StorageMode,
                    UpdatedAtUtc = excluded.UpdatedAtUtc
                """,
                (
                    binding.source_id,
                    binding.source_key,
                    binding.root_path,
                    binding.display_name,
                    binding.storage_mode,
                    utc_now_text(),
                ),
            )
        return binding

    def update_binding_mode(self, source_id: str, storage_mode: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE SourceBinding SET StorageMode = ?, UpdatedAtUtc = ? WHERE SourceId = ?",
                (storage_mode, utc_now_text(), source_id),
            )

    def remove_binding(self, source_id: str) -> int:
        with self._connect() as connection:
            binding = connection.execute(
                "SELECT RootPath FROM SourceBinding WHERE SourceId = ?", (source_id,)
            ).fetchone()
            if not binding:
                raise ValueError(f"Local source binding {source_id!r} was not found")
            root_path = str(binding["RootPath"])
            lifecycle_key = f"source-lifecycle:{root_path}"
            generation_row = connection.execute(
                "SELECT SettingValue FROM LocalSetting WHERE SettingKey = ?",
                (lifecycle_key,),
            ).fetchone()
            try:
                generation = int(generation_row["SettingValue"]) if generation_row else 0
            except ValueError as exc:
                raise ValueError(f"Invalid local source lifecycle generation for {root_path}") from exc
            if generation < 0:
                raise ValueError(f"Invalid local source lifecycle generation for {root_path}")
            next_generation = generation + 1
            # A server-side source removal deletes its occurrences. Keeping the
            # local accepted inventory would make a later reactivation falsely
            # look unchanged and prevent those occurrences from being restored.
            connection.execute("DELETE FROM KnownOccurrence WHERE SourceId = ?", (source_id,))
            connection.execute("DELETE FROM ManifestOutbox WHERE SourceId = ?", (source_id,))
            connection.execute("DELETE FROM ScanRun WHERE SourceId = ?", (source_id,))
            connection.execute("DELETE FROM SourceBinding WHERE SourceId = ?", (source_id,))
            connection.execute(
                """
                INSERT INTO LocalSetting (SettingKey, SettingValue) VALUES (?, ?)
                ON CONFLICT (SettingKey) DO UPDATE SET SettingValue = excluded.SettingValue
                """,
                (lifecycle_key, str(next_generation)),
            )
        return next_generation

    def list_bindings(self) -> list[SourceBinding]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT SourceId, SourceKey, RootPath, DisplayName, StorageMode FROM SourceBinding ORDER BY DisplayName"
            ).fetchall()
        return [SourceBinding(*map(str, row)) for row in rows]

    def resolve_binding(self, selector: str | None) -> SourceBinding:
        bindings = self.list_bindings()
        if selector is None:
            if len(bindings) == 1:
                return bindings[0]
            if not bindings:
                raise ValueError("No local sources are configured. Run 'imagetracker source add PATH'.")
            raise ValueError("More than one source exists; specify a source ID, name, or path.")
        normalized_selector = normalized_path(selector)
        matches = [
            item
            for item in bindings
            if item.source_id == selector
            or item.source_key == selector
            or item.display_name.casefold() == selector.casefold()
            or item.root_path == normalized_selector
        ]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise ValueError(f"No local source matches {selector!r}")
        raise ValueError(f"Source selector {selector!r} is ambiguous")

    def cached_file(self, source_id: str, path: Path, size: int, modified_ns: int) -> CachedFile | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT ContentSha256, MetadataJson FROM FileCache
                WHERE SourceId = ? AND PathKey = ? AND ByteSize = ? AND ModifiedNs = ?
                """,
                (source_id, normalized_path(path), size, modified_ns),
            ).fetchone()
        if not row:
            return None
        return CachedFile(sha256=str(row["ContentSha256"]), metadata=json.loads(row["MetadataJson"]))

    def cache_file(
        self,
        source_id: str,
        path: Path,
        *,
        size: int,
        modified_ns: int,
        sha256: str,
        metadata: Mapping[str, Any],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO FileCache
                    (SourceId, PathKey, FilePath, ByteSize, ModifiedNs, ContentSha256, MetadataJson, UpdatedAtUtc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (SourceId, PathKey) DO UPDATE SET
                    FilePath = excluded.FilePath,
                    ByteSize = excluded.ByteSize,
                    ModifiedNs = excluded.ModifiedNs,
                    ContentSha256 = excluded.ContentSha256,
                    MetadataJson = excluded.MetadataJson,
                    UpdatedAtUtc = excluded.UpdatedAtUtc
                """,
                (
                    source_id,
                    normalized_path(path),
                    str(path),
                    size,
                    modified_ns,
                    sha256,
                    json.dumps(metadata, separators=(",", ":"), sort_keys=True),
                    utc_now_text(),
                ),
            )

    def known_occurrences(self, source_id: str) -> dict[str, tuple[str, str]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT SourceItemId, SourceRevision, RelativePath FROM KnownOccurrence WHERE SourceId = ?",
                (source_id,),
            ).fetchall()
        return {
            str(row["SourceItemId"]): (str(row["SourceRevision"]), str(row["RelativePath"]))
            for row in rows
        }

    def begin_scan(self, source_id: str, root: Path) -> str:
        scan_id = str(uuid.uuid4())
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ScanRun (ScanId, SourceId, RootPath, Status, StartedAtUtc)
                VALUES (?, ?, ?, 'Scanning', ?)
                """,
                (scan_id, source_id, normalized_path(root), utc_now_text()),
            )
        return scan_id

    def finish_scan(
        self,
        scan_id: str,
        *,
        complete_read: bool,
        summary: Mapping[str, Any],
        status: str = "Queued",
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ScanRun SET Status = ?, CompleteRead = ?, SummaryJson = ?, CompletedAtUtc = ?
                WHERE ScanId = ?
                """,
                (status, int(complete_read), json.dumps(summary, sort_keys=True), utc_now_text(), scan_id),
            )

    def queue_batches(
        self,
        source_id: str,
        scan_id: str,
        payloads: Sequence[Mapping[str, Any]],
    ) -> None:
        with self._connect() as connection:
            for sequence, payload in enumerate(payloads):
                batch_id = str(uuid.uuid4())
                connection.execute(
                    """
                    INSERT INTO ManifestOutbox
                        (BatchId, SourceId, ScanId, SequenceNumber, IdempotencyKey, PayloadJson, CreatedAtUtc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        source_id,
                        scan_id,
                        sequence,
                        f"manifest:{scan_id}:{sequence}",
                        json.dumps(payload, separators=(",", ":"), sort_keys=True),
                        utc_now_text(),
                    ),
                )

    def pending_batches(self, source_id: str) -> list[OutboxBatch]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT BatchId, SourceId, ScanId, SequenceNumber, IdempotencyKey,
                       PayloadJson, State, FailureJson
                FROM ManifestOutbox
                WHERE SourceId = ? AND State = 'Pending'
                ORDER BY CreatedAtUtc, SequenceNumber
                """,
                (source_id,),
            ).fetchall()
        return [
            OutboxBatch(
                batch_id=str(row["BatchId"]),
                source_id=str(row["SourceId"]),
                scan_id=str(row["ScanId"]),
                sequence=int(row["SequenceNumber"]),
                idempotency_key=str(row["IdempotencyKey"]),
                payload=json.loads(row["PayloadJson"]),
                state=str(row["State"]),
                failure=json.loads(row["FailureJson"]) if row["FailureJson"] else None,
            )
            for row in rows
        ]

    def acknowledge_batch(
        self,
        batch: OutboxBatch,
        response: Mapping[str, Any] | None = None,
    ) -> BatchResolution:
        if response is None:
            response = {
                "counts": {"rejected": 0},
                "results": [
                    {
                        "sourceItemId": entry["sourceItemId"],
                        "outcome": (
                            "DeletedOccurrence"
                            if entry.get("operation") == "Deleted"
                            else "CreatedOccurrence"
                        ),
                        "uploadRequired": False,
                    }
                    for entry in batch.payload.get("entries", [])
                ],
            }
        entries = batch.payload.get("entries") or []
        results = {
            str(item.get("sourceItemId")): item
            for item in response.get("results", [])
            if item.get("sourceItemId")
        }
        accepted: list[Mapping[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for entry in entries:
            item_id = str(entry.get("sourceItemId") or "")
            result = results.get(item_id)
            outcome = str((result or {}).get("outcome") or "")
            reason: str | None = None
            if result is None:
                reason = "MissingResult"
            elif result.get("uploadRequired"):
                reason = "UnexpectedLocalUpload"
            elif outcome == "Rejected":
                reason = "Rejected"
            elif entry.get("operation") == "Deleted":
                reliable_ignored_deletion = (
                    outcome == "IgnoredDeletion"
                    and bool(batch.payload.get("deletionDetectionReliable"))
                )
                if outcome != "DeletedOccurrence" and not reliable_ignored_deletion:
                    reason = outcome or "DeletionNotApplied"
            elif entry.get("operation") == "Upsert" and outcome not in {
                "CreatedOccurrence",
                "UpdatedOccurrence",
                "DuplicateLinked",
                "Unchanged",
            }:
                reason = outcome or "UpsertNotApplied"
            if reason:
                failures.append(
                    {
                        "sourceItemId": item_id,
                        "sourceRevision": str(entry.get("sourceRevision") or ""),
                        "operation": str(entry.get("operation") or ""),
                        "fileName": entry.get("fileName"),
                        "reason": reason,
                        "outcome": outcome or None,
                        "errorCode": (result or {}).get("errorCode"),
                        "errorMessage": (result or {}).get("errorMessage"),
                    }
                )
            else:
                accepted.append(entry)

        reported_rejected = int((response.get("counts") or {}).get("rejected") or 0)
        if reported_rejected > len(failures):
            failures.append(
                {
                    "sourceItemId": None,
                    "sourceRevision": None,
                    "operation": None,
                    "fileName": None,
                    "reason": "RejectedCountMismatch",
                    "outcome": "Rejected",
                    "errorCode": "MANIFEST_RESULT_MISMATCH",
                    "errorMessage": (
                        f"Service reported {reported_rejected} rejected entries but returned "
                        f"{len(failures)} structured failures."
                    ),
                }
            )

        failure_payload = {
            "reason": "ManifestEntryFailure",
            "entries": failures,
            "responseCounts": dict(response.get("counts") or {}),
        }
        final_state = "Failed" if failures else "Sent"
        with self._connect() as connection:
            for entry in accepted:
                if entry.get("operation") == "Upsert":
                    connection.execute(
                        """
                        INSERT INTO KnownOccurrence
                            (SourceId, SourceItemId, SourceRevision, RelativePath, UpdatedAtUtc)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (SourceId, SourceItemId) DO UPDATE SET
                            SourceRevision = excluded.SourceRevision,
                            RelativePath = excluded.RelativePath,
                            UpdatedAtUtc = excluded.UpdatedAtUtc
                        """,
                        (
                            batch.source_id,
                            entry["sourceItemId"],
                            entry["sourceRevision"],
                            entry.get("localLocator") or entry["fileName"],
                            utc_now_text(),
                        ),
                    )
                elif entry.get("operation") == "Deleted":
                    connection.execute(
                        "DELETE FROM KnownOccurrence WHERE SourceId = ? AND SourceItemId = ?",
                        (batch.source_id, entry["sourceItemId"]),
                    )
            now = utc_now_text()
            if failures:
                connection.execute(
                    """
                    UPDATE ManifestOutbox
                    SET State = 'Failed', FailureJson = ?, FailedAtUtc = ?
                    WHERE BatchId = ?
                    """,
                    (json.dumps(failure_payload, sort_keys=True), now, batch.batch_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE ManifestOutbox
                    SET State = 'Sent', SentAtUtc = ?, FailureJson = NULL, FailedAtUtc = NULL
                    WHERE BatchId = ?
                    """,
                    (now, batch.batch_id),
                )
            remaining = connection.execute(
                "SELECT COUNT(*) FROM ManifestOutbox WHERE ScanId = ? AND State = 'Pending'",
                (batch.scan_id,),
            ).fetchone()[0]
            if remaining == 0:
                failed = connection.execute(
                    "SELECT COUNT(*) FROM ManifestOutbox WHERE ScanId = ? AND State = 'Failed'",
                    (batch.scan_id,),
                ).fetchone()[0]
                status = "NeedsAttention" if failed else "Complete"
                connection.execute("UPDATE ScanRun SET Status = ? WHERE ScanId = ?", (status, batch.scan_id))
        return BatchResolution(
            accepted_entries=len(accepted),
            failed_entries=len(failures),
            state=final_state,
            failures=tuple(failures),
        )

    def quarantine_request_error(
        self,
        batch: OutboxBatch,
        *,
        status: int,
        code: str | None,
        title: str,
        detail: str,
        request_id: str | None,
    ) -> BatchResolution:
        safe_code = self._safe_error_text(code, 128)
        safe_title = self._safe_error_text(title, 200) or "Request rejected"
        safe_detail = self._safe_error_text(detail, 1000) or safe_title
        safe_request_id = self._safe_error_text(request_id, 128)
        failures = [
            {
                "sourceItemId": str(entry.get("sourceItemId") or ""),
                "sourceRevision": str(entry.get("sourceRevision") or ""),
                "operation": str(entry.get("operation") or ""),
                "fileName": entry.get("fileName"),
                "reason": "RequestRejected",
                "outcome": None,
                "errorCode": safe_code,
                "errorMessage": safe_detail,
            }
            for entry in batch.payload.get("entries", [])
        ]
        failure_payload = {
            "reason": "RequestRejected",
            "requestError": {
                "status": status,
                "code": safe_code,
                "title": safe_title,
                "detail": safe_detail,
                "requestId": safe_request_id,
            },
            "entries": failures,
        }
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ManifestOutbox
                SET State = 'Failed', FailureJson = ?, FailedAtUtc = ?
                WHERE BatchId = ? AND State = 'Pending'
                """,
                (json.dumps(failure_payload, sort_keys=True), utc_now_text(), batch.batch_id),
            )
            remaining = connection.execute(
                "SELECT COUNT(*) FROM ManifestOutbox WHERE ScanId = ? AND State = 'Pending'",
                (batch.scan_id,),
            ).fetchone()[0]
            if remaining == 0:
                connection.execute(
                    "UPDATE ScanRun SET Status = 'NeedsAttention' WHERE ScanId = ?",
                    (batch.scan_id,),
                )
        return BatchResolution(
            accepted_entries=0,
            failed_entries=len(failures),
            state="Failed",
            failures=tuple(failures),
        )

    @staticmethod
    def _safe_error_text(value: Any, limit: int) -> str | None:
        if value is None:
            return None
        text = " ".join(str(value).split())
        return text[:limit] if text else None

    def quarantined_revisions(self, source_id: str) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT FailureJson FROM ManifestOutbox
                WHERE SourceId = ? AND State = 'Failed' AND FailureJson IS NOT NULL
                """,
                (source_id,),
            ).fetchall()
        blocked: dict[str, str] = {}
        for row in rows:
            failure = json.loads(row["FailureJson"])
            for entry in failure.get("entries", []):
                item_id = entry.get("sourceItemId")
                revision = entry.get("sourceRevision")
                if item_id and revision:
                    blocked[str(item_id)] = str(revision)
        return blocked

    def list_outbox(self, *, state: str | None = None, limit: int = 100) -> list[OutboxBatch]:
        if not 1 <= limit <= 1000:
            raise ValueError("Outbox limit must be between 1 and 1000")
        query = """
            SELECT BatchId, SourceId, ScanId, SequenceNumber, IdempotencyKey,
                   PayloadJson, State, FailureJson
            FROM ManifestOutbox
        """
        parameters: list[Any] = []
        if state and state != "All":
            query += " WHERE State = ?"
            parameters.append(state)
        query += " ORDER BY CreatedAtUtc DESC, SequenceNumber DESC LIMIT ?"
        parameters.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [
            OutboxBatch(
                batch_id=str(row["BatchId"]),
                source_id=str(row["SourceId"]),
                scan_id=str(row["ScanId"]),
                sequence=int(row["SequenceNumber"]),
                idempotency_key=str(row["IdempotencyKey"]),
                payload=json.loads(row["PayloadJson"]),
                state=str(row["State"]),
                failure=json.loads(row["FailureJson"]) if row["FailureJson"] else None,
            )
            for row in rows
        ]

    def discard_outbox(self, batch_id: str) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT State, ScanId FROM ManifestOutbox WHERE BatchId = ?", (batch_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Outbox batch {batch_id!r} was not found")
            if row["State"] != "Failed":
                raise ValueError("Only a Failed outbox batch can be discarded")
            connection.execute(
                """
                UPDATE ManifestOutbox SET State = 'Discarded', DiscardedAtUtc = ?
                WHERE BatchId = ?
                """,
                (utc_now_text(), batch_id),
            )
            remaining_failed = connection.execute(
                "SELECT COUNT(*) FROM ManifestOutbox WHERE ScanId = ? AND State = 'Failed'",
                (row["ScanId"],),
            ).fetchone()[0]
            if remaining_failed == 0:
                connection.execute(
                    "UPDATE ScanRun SET Status = 'CompleteWithDiscarded' WHERE ScanId = ?",
                    (row["ScanId"],),
                )

    def pending_count(self) -> int:
        with self._connect() as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM ManifestOutbox WHERE State = 'Pending'"
                ).fetchone()[0]
            )

    def failed_count(self) -> int:
        with self._connect() as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM ManifestOutbox WHERE State = 'Failed'"
                ).fetchone()[0]
            )

    def recent_scans(self, limit: int = 10) -> list[Mapping[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ScanId, SourceId, Status, CompleteRead, SummaryJson, StartedAtUtc, CompletedAtUtc
                FROM ScanRun ORDER BY StartedAtUtc DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def legacy_checkpoint(self, migration_name: str = "ImageAsset-v1") -> tuple[int, int]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT LastLegacyId, ProcessedCount FROM LegacyMigrationCheckpoint
                WHERE MigrationName = ?
                """,
                (migration_name,),
            ).fetchone()
        return (int(row["LastLegacyId"]), int(row["ProcessedCount"])) if row else (0, 0)

    def save_legacy_checkpoint(
        self,
        last_legacy_id: int,
        processed_count: int,
        migration_name: str = "ImageAsset-v1",
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO LegacyMigrationCheckpoint
                    (MigrationName, LastLegacyId, ProcessedCount, UpdatedAtUtc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (MigrationName) DO UPDATE SET
                    LastLegacyId = excluded.LastLegacyId,
                    ProcessedCount = excluded.ProcessedCount,
                    UpdatedAtUtc = excluded.UpdatedAtUtc
                """,
                (migration_name, last_legacy_id, processed_count, utc_now_text()),
            )
