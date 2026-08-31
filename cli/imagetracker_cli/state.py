from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    byte_size: int | None = None
    modified_ns: int | None = None


@dataclass(frozen=True)
class FileCacheUpdate:
    path_key: str
    file_path: str
    byte_size: int
    modified_ns: int
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
class BulkManifestOutboxItem:
    bulk_id: str
    source_id: str
    snapshot_id: str
    idempotency_key: str
    artifact_path: str
    artifact_sha256: str
    artifact_bytes: int
    entry_count: int
    state: str
    server_import_id: str | None = None
    server_status: str | None = None
    server_phase: str | None = None
    processed_entries: int = 0
    result_path: str | None = None
    result_sha256: str | None = None
    result_bytes: int | None = None
    result_applied_through: int = 0
    superseded_batch_ids: tuple[str, ...] = ()
    failure: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class BatchResolution:
    accepted_entries: int
    failed_entries: int
    state: str
    failures: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class DescriptionOutboxItem:
    job_id: str
    source_id: str
    occurrence_id: str
    media_asset_id: str
    source_item_id: str
    local_path: str
    asset_content_sha256: str
    file_name: str
    state: str
    attempt_count: int
    next_attempt_at_utc: str | None = None
    error: Mapping[str, Any] | None = None


class LocalState:
    def __init__(self, path: Path):
        self.path = path
        ensure_private_directory(path.parent)
        self._initialize()

    @contextmanager
    def source_sync_lock(self, source_id: str) -> Iterator[None]:
        """Prevent two CLI processes from synchronizing one source at once."""

        lock_path = self.path.parent / f"sync-{source_id}.lock"
        with lock_path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt

                if lock_path.stat().st_size == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError:
                    raise ValueError(
                        "A sync is already running for this source."
                    ) from None
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                return

            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError(
                    "A sync is already running for this source."
                ) from None
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

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
                CREATE TABLE IF NOT EXISTS BulkManifestOutbox (
                    BulkId TEXT PRIMARY KEY,
                    SourceId TEXT NOT NULL,
                    SnapshotId TEXT NOT NULL,
                    IdempotencyKey TEXT NOT NULL UNIQUE,
                    ArtifactPath TEXT NOT NULL,
                    ArtifactSha256 TEXT NOT NULL,
                    ArtifactBytes INTEGER NOT NULL,
                    EntryCount INTEGER NOT NULL,
                    State TEXT NOT NULL DEFAULT 'Prepared',
                    ServerImportId TEXT,
                    ServerStatus TEXT,
                    ServerPhase TEXT,
                    ProcessedEntries INTEGER NOT NULL DEFAULT 0,
                    ResultPath TEXT,
                    ResultSha256 TEXT,
                    ResultBytes INTEGER,
                    ResultAppliedThrough INTEGER NOT NULL DEFAULT 0,
                    SupersededBatchIdsJson TEXT NOT NULL DEFAULT '[]',
                    FailureJson TEXT,
                    CreatedAtUtc TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL,
                    CompletedAtUtc TEXT,
                    UNIQUE (SourceId, SnapshotId)
                );
                CREATE INDEX IF NOT EXISTS IX_BulkManifestOutbox_Source_State
                    ON BulkManifestOutbox (SourceId, State, CreatedAtUtc);
                CREATE TABLE IF NOT EXISTS DescriptionOutbox (
                    JobId TEXT PRIMARY KEY,
                    SourceId TEXT NOT NULL,
                    OccurrenceId TEXT NOT NULL,
                    MediaAssetId TEXT NOT NULL,
                    SourceItemId TEXT NOT NULL,
                    LocalPath TEXT NOT NULL,
                    AssetContentSha256 TEXT NOT NULL,
                    FileName TEXT NOT NULL,
                    State TEXT NOT NULL DEFAULT 'Pending',
                    AttemptCount INTEGER NOT NULL DEFAULT 0,
                    NextAttemptAtUtc TEXT,
                    ErrorJson TEXT,
                    CreatedAtUtc TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL,
                    SentAtUtc TEXT,
                    FailedAtUtc TEXT
                );
                CREATE INDEX IF NOT EXISTS IX_DescriptionOutbox_Due
                    ON DescriptionOutbox (SourceId, State, NextAttemptAtUtc, CreatedAtUtc);
                CREATE TABLE IF NOT EXISTS DescriptionCandidate (
                    JobId TEXT NOT NULL,
                    OccurrenceId TEXT NOT NULL,
                    MediaAssetId TEXT NOT NULL,
                    SourceId TEXT NOT NULL,
                    SourceItemId TEXT NOT NULL,
                    LocalPath TEXT NOT NULL,
                    AssetContentSha256 TEXT NOT NULL,
                    FileName TEXT NOT NULL,
                    UpdatedAtUtc TEXT NOT NULL,
                    PRIMARY KEY (JobId, OccurrenceId)
                );
                CREATE INDEX IF NOT EXISTS IX_DescriptionCandidate_Source
                    ON DescriptionCandidate (SourceId, SourceItemId, JobId);
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
            bulk_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(BulkManifestOutbox)"
                ).fetchall()
            }
            for column, declaration in (
                ("ServerPhase", "TEXT"),
                ("ProcessedEntries", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if column not in bulk_columns:
                    connection.execute(
                        f"ALTER TABLE BulkManifestOutbox ADD COLUMN {column} {declaration}"
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
            connection.execute("DELETE FROM BulkManifestOutbox WHERE SourceId = ?", (source_id,))
            affected_jobs = [
                str(row["JobId"])
                for row in connection.execute(
                    "SELECT DISTINCT JobId FROM DescriptionCandidate WHERE SourceId = ?",
                    (source_id,),
                ).fetchall()
            ]
            connection.execute(
                "DELETE FROM DescriptionCandidate WHERE SourceId = ?", (source_id,)
            )
            for job_id in affected_jobs:
                self._reselect_description_candidate(connection, job_id)
            connection.execute(
                """
                DELETE FROM DescriptionOutbox
                WHERE SourceId = ?
                  AND JobId NOT IN (SELECT JobId FROM DescriptionCandidate)
                """,
                (source_id,),
            )
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
                SELECT ByteSize, ModifiedNs, ContentSha256, MetadataJson FROM FileCache
                WHERE SourceId = ? AND PathKey = ? AND ByteSize = ? AND ModifiedNs = ?
                """,
                (source_id, normalized_path(path), size, modified_ns),
            ).fetchone()
        if not row:
            return None
        return CachedFile(
            sha256=str(row["ContentSha256"]),
            metadata=json.loads(row["MetadataJson"]),
            byte_size=int(row["ByteSize"]),
            modified_ns=int(row["ModifiedNs"]),
        )

    def cached_files(self, source_id: str) -> dict[str, CachedFile]:
        """Load one source's hash cache in a single SQLite round trip."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT PathKey, ByteSize, ModifiedNs, ContentSha256, MetadataJson
                FROM FileCache
                WHERE SourceId = ?
                """,
                (source_id,),
            ).fetchall()
        cached: dict[str, CachedFile] = {}
        for row in rows:
            try:
                metadata = json.loads(row["MetadataJson"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(metadata, dict):
                continue
            cached[str(row["PathKey"])] = CachedFile(
                sha256=str(row["ContentSha256"]),
                metadata=metadata,
                byte_size=int(row["ByteSize"]),
                modified_ns=int(row["ModifiedNs"]),
            )
        return cached

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
        self.cache_files(
            source_id,
            (
                FileCacheUpdate(
                    path_key=normalized_path(path),
                    file_path=str(path),
                    byte_size=size,
                    modified_ns=modified_ns,
                    sha256=sha256,
                    metadata=metadata,
                ),
            ),
        )

    def cache_files(
        self,
        source_id: str,
        updates: Sequence[FileCacheUpdate],
    ) -> None:
        """Persist many scan results with one transaction and one prepared SQL."""

        if not updates:
            return
        updated_at = utc_now_text()
        values = [
            (
                source_id,
                update.path_key,
                update.file_path,
                update.byte_size,
                update.modified_ns,
                update.sha256,
                json.dumps(update.metadata, separators=(",", ":"), sort_keys=True),
                updated_at,
            )
            for update in updates
        ]
        with self._connect() as connection:
            # FileCache is reproducible local acceleration state. NORMAL keeps
            # WAL durability while avoiding an fsync for every cache batch.
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executemany(
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
                values,
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

    def record_known_occurrences(
        self,
        source_id: str,
        entries: Sequence[Mapping[str, Any]],
    ) -> None:
        """Record an acknowledged one-file MySQL import in one SQLite batch."""

        values = [
            (
                source_id,
                str(entry["sourceItemId"]),
                str(entry["sourceRevision"]),
                str(entry.get("localLocator") or entry["fileName"]),
                utc_now_text(),
            )
            for entry in entries
            if entry.get("sourceItemId")
            and entry.get("sourceRevision")
            and entry.get("fileName")
        ]
        if not values:
            return
        with self._connect() as connection:
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executemany(
                """
                INSERT INTO KnownOccurrence
                    (SourceId, SourceItemId, SourceRevision, RelativePath, UpdatedAtUtc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (SourceId, SourceItemId) DO UPDATE SET
                    SourceRevision = excluded.SourceRevision,
                    RelativePath = excluded.RelativePath,
                    UpdatedAtUtc = excluded.UpdatedAtUtc
                """,
                values,
            )

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

    def split_oversized_pending_batches(
        self,
        source_id: str,
        root: Path,
        *,
        max_entries: int,
    ) -> tuple[int, int]:
        """Replace oversized pending requests with durable smaller requests."""

        if not 1 <= max_entries <= 500:
            raise ValueError("Manifest split size must be between 1 and 500")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT BatchId, ScanId, PayloadJson
                FROM ManifestOutbox
                WHERE SourceId = ? AND State = 'Pending'
                ORDER BY CreatedAtUtc, SequenceNumber
                """,
                (source_id,),
            ).fetchall()
            oversized: list[tuple[str, str, dict[str, Any]]] = []
            for row in rows:
                payload = json.loads(row["PayloadJson"])
                if len(payload.get("entries") or []) > max_entries:
                    oversized.append(
                        (
                            str(row["BatchId"]),
                            str(row["ScanId"]),
                            payload,
                        )
                    )
            if not oversized:
                return 0, 0

            replacement_scan_id = str(uuid.uuid4())
            now = utc_now_text()
            connection.execute(
                """
                INSERT INTO ScanRun
                    (ScanId, SourceId, RootPath, Status, CompleteRead,
                     SummaryJson, StartedAtUtc, CompletedAtUtc)
                VALUES (?, ?, ?, 'Queued', 1, ?, ?, ?)
                """,
                (
                    replacement_scan_id,
                    source_id,
                    normalized_path(root),
                    json.dumps(
                        {
                            "reason": "SplitOversizedManifestRequests",
                            "originalBatches": len(oversized),
                            "maxEntries": max_entries,
                        },
                        sort_keys=True,
                    ),
                    now,
                    now,
                ),
            )

            sequence = 0
            replacement_values: list[tuple[Any, ...]] = []
            for _batch_id, _scan_id, payload in oversized:
                entries = list(payload.get("entries") or [])
                for start in range(0, len(entries), max_entries):
                    replacement = dict(payload)
                    replacement["snapshotId"] = replacement_scan_id
                    replacement["entries"] = entries[start : start + max_entries]
                    replacement_values.append(
                        (
                            str(uuid.uuid4()),
                            source_id,
                            replacement_scan_id,
                            sequence,
                            f"manifest:{replacement_scan_id}:{sequence}",
                            json.dumps(
                                replacement,
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
                            now,
                        )
                    )
                    sequence += 1
            connection.executemany(
                """
                INSERT INTO ManifestOutbox
                    (BatchId, SourceId, ScanId, SequenceNumber, IdempotencyKey,
                     PayloadJson, CreatedAtUtc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                replacement_values,
            )

            split_note = json.dumps(
                {
                    "reason": "SplitAfterApiTimeout",
                    "replacementScanId": replacement_scan_id,
                    "maxEntries": max_entries,
                },
                sort_keys=True,
            )
            connection.executemany(
                """
                UPDATE ManifestOutbox
                SET State = 'Discarded', FailureJson = ?, DiscardedAtUtc = ?
                WHERE BatchId = ? AND State = 'Pending'
                """,
                (
                    (split_note, now, batch_id)
                    for batch_id, _scan_id, _payload in oversized
                ),
            )
            original_scan_ids = {
                scan_id for _batch_id, scan_id, _payload in oversized
            }
            connection.executemany(
                """
                UPDATE ScanRun
                SET Status = 'SplitForApiTimeout'
                WHERE ScanId = ?
                """,
                ((scan_id,) for scan_id in original_scan_ids),
            )
        return len(oversized), sequence

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
        accepted: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
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
                accepted.append((entry, result or {}))

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
            for entry, result in accepted:
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
                    self._queue_description_in_transaction(
                        connection,
                        source_id=batch.source_id,
                        entry=entry,
                        result=result,
                    )
                elif entry.get("operation") == "Deleted":
                    affected_jobs = [
                        str(row["JobId"])
                        for row in connection.execute(
                            """
                            SELECT JobId FROM DescriptionCandidate
                            WHERE SourceId = ? AND SourceItemId = ?
                            """,
                            (batch.source_id, entry["sourceItemId"]),
                        ).fetchall()
                    ]
                    connection.execute(
                        "DELETE FROM KnownOccurrence WHERE SourceId = ? AND SourceItemId = ?",
                        (batch.source_id, entry["sourceItemId"]),
                    )
                    connection.execute(
                        """
                        DELETE FROM DescriptionCandidate
                        WHERE SourceId = ? AND SourceItemId = ?
                        """,
                        (batch.source_id, entry["sourceItemId"]),
                    )
                    for job_id in affected_jobs:
                        self._reselect_description_candidate(connection, job_id)
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

    @staticmethod
    def _queue_description_in_transaction(
        connection: sqlite3.Connection,
        *,
        source_id: str,
        entry: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> None:
        """Persist an accepted photo's staging work in the manifest transaction."""

        job_id = str(result.get("descriptionJobId") or "").strip()
        if not job_id or str(entry.get("mediaType") or "") != "Photo":
            return
        required = {
            "occurrence_id": str(result.get("occurrenceId") or "").strip(),
            "media_asset_id": str(result.get("mediaAssetId") or "").strip(),
            "source_item_id": str(entry.get("sourceItemId") or "").strip(),
            "local_path": str(entry.get("localLocator") or "").strip(),
            "asset_content_sha256": str(entry.get("contentSha256") or "").strip().lower(),
            "file_name": str(entry.get("fileName") or "").strip(),
        }
        # An incomplete server result must not make an otherwise accepted
        # manifest transaction fail. The service contract supplies all fields.
        if any(not value for value in required.values()):
            return
        now = utc_now_text()
        replaced_jobs = [
            str(row["JobId"])
            for row in connection.execute(
                """
                SELECT DISTINCT JobId FROM DescriptionCandidate
                WHERE SourceId = ? AND SourceItemId = ? AND JobId != ?
                """,
                (source_id, required["source_item_id"], job_id),
            ).fetchall()
        ]
        connection.execute(
            """
            DELETE FROM DescriptionCandidate
            WHERE SourceId = ? AND SourceItemId = ? AND JobId != ?
            """,
            (source_id, required["source_item_id"], job_id),
        )
        for replaced_job_id in replaced_jobs:
            LocalState._reselect_description_candidate(
                connection, replaced_job_id
            )
        connection.execute(
            """
            INSERT INTO DescriptionCandidate
                (JobId, OccurrenceId, MediaAssetId, SourceId, SourceItemId,
                 LocalPath, AssetContentSha256, FileName, UpdatedAtUtc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (JobId, OccurrenceId) DO UPDATE SET
                MediaAssetId = excluded.MediaAssetId,
                SourceId = excluded.SourceId,
                SourceItemId = excluded.SourceItemId,
                LocalPath = excluded.LocalPath,
                AssetContentSha256 = excluded.AssetContentSha256,
                FileName = excluded.FileName,
                UpdatedAtUtc = excluded.UpdatedAtUtc
            """,
            (
                job_id,
                required["occurrence_id"],
                required["media_asset_id"],
                source_id,
                required["source_item_id"],
                required["local_path"],
                required["asset_content_sha256"],
                required["file_name"],
                now,
            ),
        )

        connection.execute(
            """
            INSERT INTO DescriptionOutbox
                (JobId, SourceId, OccurrenceId, MediaAssetId, SourceItemId,
                 LocalPath, AssetContentSha256, FileName, State, CreatedAtUtc, UpdatedAtUtc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?, ?)
            ON CONFLICT (JobId) DO UPDATE SET
                UpdatedAtUtc = excluded.UpdatedAtUtc
            WHERE DescriptionOutbox.State != 'Sent'
              AND DescriptionOutbox.OccurrenceId = excluded.OccurrenceId
            """,
            (
                job_id,
                source_id,
                required["occurrence_id"],
                required["media_asset_id"],
                required["source_item_id"],
                required["local_path"],
                required["asset_content_sha256"],
                required["file_name"],
                now,
                now,
            ),
        )

    def queue_scene_description_tasks(
        self,
        source_id: str,
        tasks: Sequence[Mapping[str, Any]],
    ) -> int:
        """Persist explicit server-prepared scene work in one local transaction."""

        queued = 0
        with self._connect() as connection:
            for task in tasks:
                if str(task.get("sourceId") or source_id) != source_id:
                    raise ValueError("Prepared scene task belongs to another source")
                entry = {
                    "sourceItemId": task.get("sourceItemId"),
                    "localLocator": task.get("localLocator"),
                    "contentSha256": task.get("assetContentSha256"),
                    "fileName": task.get("fileName"),
                    "mediaType": "Photo",
                }
                result = {
                    "descriptionJobId": task.get("jobId"),
                    "occurrenceId": task.get("occurrenceId"),
                    "mediaAssetId": task.get("mediaAssetId"),
                }
                existing = connection.execute(
                    """
                    SELECT OccurrenceId, MediaAssetId, SourceItemId, LocalPath,
                           AssetContentSha256, FileName, State
                    FROM DescriptionOutbox WHERE JobId = ?
                    """,
                    (str(task.get("jobId") or ""),),
                ).fetchone()
                self._queue_description_in_transaction(
                    connection,
                    source_id=source_id,
                    entry=entry,
                    result=result,
                )
                if existing is None:
                    queued += 1
                    continue
                identity = (
                    str(task.get("occurrenceId") or ""),
                    str(task.get("mediaAssetId") or ""),
                    str(task.get("sourceItemId") or ""),
                    str(task.get("localLocator") or ""),
                    str(task.get("assetContentSha256") or "").lower(),
                    str(task.get("fileName") or ""),
                )
                previous_identity = (
                    str(existing["OccurrenceId"]),
                    str(existing["MediaAssetId"]),
                    str(existing["SourceItemId"]),
                    str(existing["LocalPath"]),
                    str(existing["AssetContentSha256"]).lower(),
                    str(existing["FileName"]),
                )
                reset = (
                    str(existing["State"]) != "Sent"
                    and (
                        identity != previous_identity
                        or str(existing["State"]) in {"Deferred", "Failed", "Skipped"}
                    )
                )
                connection.execute(
                    """
                    UPDATE DescriptionOutbox
                    SET SourceId = ?, OccurrenceId = ?, MediaAssetId = ?,
                        SourceItemId = ?, LocalPath = ?, AssetContentSha256 = ?,
                        FileName = ?,
                        State = CASE WHEN ? THEN 'Pending' ELSE State END,
                        NextAttemptAtUtc = CASE WHEN ? THEN NULL ELSE NextAttemptAtUtc END,
                        ErrorJson = CASE WHEN ? THEN NULL ELSE ErrorJson END,
                        FailedAtUtc = CASE WHEN ? THEN NULL ELSE FailedAtUtc END,
                        UpdatedAtUtc = ?
                    WHERE JobId = ?
                    """,
                    (
                        source_id,
                        *identity,
                        int(reset),
                        int(reset),
                        int(reset),
                        int(reset),
                        utc_now_text(),
                        str(task.get("jobId") or ""),
                    ),
                )
                if reset:
                    queued += 1
        return queued

    @staticmethod
    def _reselect_description_candidate(
        connection: sqlite3.Connection, job_id: str
    ) -> None:
        candidate = connection.execute(
            """
            SELECT SourceId, OccurrenceId, MediaAssetId, SourceItemId, LocalPath,
                   AssetContentSha256, FileName
            FROM DescriptionCandidate
            WHERE JobId = ?
            ORDER BY UpdatedAtUtc DESC, OccurrenceId
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if candidate is None:
            connection.execute(
                "DELETE FROM DescriptionOutbox WHERE JobId = ?", (job_id,)
            )
            return
        connection.execute(
            """
            UPDATE DescriptionOutbox
            SET SourceId = ?, OccurrenceId = ?, MediaAssetId = ?,
                SourceItemId = ?, LocalPath = ?, AssetContentSha256 = ?,
                FileName = ?, UpdatedAtUtc = ?
            WHERE JobId = ?
            """,
            (
                candidate["SourceId"],
                candidate["OccurrenceId"],
                candidate["MediaAssetId"],
                candidate["SourceItemId"],
                candidate["LocalPath"],
                candidate["AssetContentSha256"],
                candidate["FileName"],
                utc_now_text(),
                job_id,
            ),
        )

    def due_description_tasks(
        self,
        source_id: str,
        *,
        now_utc: datetime | None = None,
        limit: int,
    ) -> list[DescriptionOutboxItem]:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("Description outbox limit must be an integer")
        if not 1 <= limit <= 1000:
            raise ValueError("Description outbox limit must be between 1 and 1000")
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        now_text = now.isoformat().replace("+00:00", "Z")
        return self._description_items(
            """
            WHERE SourceId = ? AND State IN ('Pending', 'Deferred')
              AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= ?)
            ORDER BY CreatedAtUtc, JobId LIMIT ?
            """,
            (source_id, now_text, limit),
        )

    def due_sent_description_tasks(
        self,
        source_id: str,
        *,
        now_utc: datetime | None = None,
        limit: int = 100,
    ) -> list[DescriptionOutboxItem]:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        now_text = now.isoformat().replace("+00:00", "Z")
        return self._description_items(
            """
            WHERE SourceId = ? AND State = 'Sent'
              AND NextAttemptAtUtc IS NOT NULL AND NextAttemptAtUtc <= ?
            ORDER BY NextAttemptAtUtc, CreatedAtUtc, JobId LIMIT ?
            """,
            (source_id, now_text, limit),
        )

    def list_description_outbox(
        self,
        *,
        state: str | None = None,
        source_id: str | None = None,
        limit: int = 100,
    ) -> list[DescriptionOutboxItem]:
        if not 1 <= limit <= 1000:
            raise ValueError("Description outbox limit must be between 1 and 1000")
        clauses: list[str] = []
        parameters: list[Any] = []
        if state and state != "All":
            clauses.append("State = ?")
            parameters.append(state)
        if source_id:
            clauses.append("SourceId = ?")
            parameters.append(source_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit)
        return self._description_items(
            f"{where} ORDER BY UpdatedAtUtc DESC, JobId LIMIT ?",
            parameters,
        )

    def description_task(self, job_id: str) -> DescriptionOutboxItem | None:
        items = self._description_items("WHERE JobId = ? LIMIT 1", (job_id,))
        return items[0] if items else None

    def _description_items(
        self, suffix: str, parameters: Sequence[Any]
    ) -> list[DescriptionOutboxItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT JobId, SourceId, OccurrenceId, MediaAssetId, SourceItemId,
                       LocalPath, AssetContentSha256, FileName, State, AttemptCount,
                       NextAttemptAtUtc, ErrorJson
                FROM DescriptionOutbox
                """
                + suffix,
                parameters,
            ).fetchall()
        return [
            DescriptionOutboxItem(
                job_id=str(row["JobId"]),
                source_id=str(row["SourceId"]),
                occurrence_id=str(row["OccurrenceId"]),
                media_asset_id=str(row["MediaAssetId"]),
                source_item_id=str(row["SourceItemId"]),
                local_path=str(row["LocalPath"]),
                asset_content_sha256=str(row["AssetContentSha256"]),
                file_name=str(row["FileName"]),
                state=str(row["State"]),
                attempt_count=int(row["AttemptCount"]),
                next_attempt_at_utc=(
                    str(row["NextAttemptAtUtc"]) if row["NextAttemptAtUtc"] else None
                ),
                error=json.loads(row["ErrorJson"]) if row["ErrorJson"] else None,
            )
            for row in rows
        ]

    def mark_description_sent(self, job_id: str) -> None:
        now_value = datetime.now(timezone.utc)
        now = now_value.isoformat().replace("+00:00", "Z")
        next_check = (now_value + timedelta(minutes=5)).isoformat().replace(
            "+00:00", "Z"
        )
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Sent', AttemptCount = AttemptCount + 1,
                    NextAttemptAtUtc = ?, ErrorJson = NULL,
                    SentAtUtc = ?, FailedAtUtc = NULL, UpdatedAtUtc = ?
                WHERE JobId = ? AND State != 'Sent'
                """,
                (next_check, now, now, job_id),
            )

    def schedule_sent_description_check(
        self,
        job_id: str,
        *,
        retry_after_seconds: int,
    ) -> None:
        seconds = max(30, min(int(retry_after_seconds), 31 * 24 * 60 * 60))
        now = datetime.now(timezone.utc)
        next_check = (now + timedelta(seconds=seconds)).isoformat().replace(
            "+00:00", "Z"
        )
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET NextAttemptAtUtc = ?, UpdatedAtUtc = ?
                WHERE JobId = ? AND State = 'Sent'
                """,
                (
                    next_check,
                    now.isoformat().replace("+00:00", "Z"),
                    job_id,
                ),
            )

    def confirm_description_complete(self, job_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET NextAttemptAtUtc = NULL, ErrorJson = NULL, UpdatedAtUtc = ?
                WHERE JobId = ? AND State = 'Sent'
                """,
                (utc_now_text(), job_id),
            )
            connection.execute(
                "DELETE FROM DescriptionCandidate WHERE JobId = ?", (job_id,)
            )

    def fail_description_from_server(
        self, job_id: str, *, code: str, message: str
    ) -> None:
        error = {
            "code": self._safe_error_text(code, 128) or "DESCRIPTION_FAILED",
            "message": self._safe_error_text(message, 500)
            or "Scene description needs attention.",
            "retryable": False,
        }
        now = utc_now_text()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Failed', NextAttemptAtUtc = NULL, ErrorJson = ?,
                    FailedAtUtc = ?, UpdatedAtUtc = ?
                WHERE JobId = ?
                """,
                (json.dumps(error, sort_keys=True), now, now, job_id),
            )

    def mark_description_skipped(
        self, job_id: str, *, code: str, message: str
    ) -> None:
        note = {
            "code": self._safe_error_text(code, 128) or "DESCRIPTION_SKIPPED",
            "message": self._safe_error_text(message, 500)
            or "Scene description was skipped.",
            "retryable": False,
        }
        now = utc_now_text()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Skipped', NextAttemptAtUtc = NULL, ErrorJson = ?,
                    FailedAtUtc = NULL, UpdatedAtUtc = ?
                WHERE JobId = ?
                """,
                (json.dumps(note, sort_keys=True), now, job_id),
            )
            connection.execute(
                "DELETE FROM DescriptionCandidate WHERE JobId = ?", (job_id,)
            )

    def defer_description(
        self,
        job_id: str,
        *,
        retry_after_seconds: int,
        code: str,
        message: str,
    ) -> None:
        seconds = max(1, min(int(retry_after_seconds), 31 * 24 * 60 * 60))
        next_attempt = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        error = {
            "code": self._safe_error_text(code, 128) or "STAGING_DEFERRED",
            "message": self._safe_error_text(message, 500) or "Scene preview staging was deferred.",
            "retryable": True,
        }
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = ?, AttemptCount = AttemptCount + 1,
                    NextAttemptAtUtc = ?, ErrorJson = ?, FailedAtUtc = NULL,
                    UpdatedAtUtc = ?
                WHERE JobId = ? AND State != 'Sent'
                """,
                (
                    "Pending",
                    next_attempt.isoformat().replace("+00:00", "Z"),
                    json.dumps(error, sort_keys=True),
                    utc_now_text(),
                    job_id,
                ),
            )

    def defer_all_descriptions_for_quota(
        self,
        attempted_job_id: str,
        *,
        retry_after_seconds: int,
    ) -> int:
        """Apply one account-wide quota decision without more provider-plan calls."""

        seconds = max(1, min(int(retry_after_seconds), 31 * 24 * 60 * 60))
        now = datetime.now(timezone.utc)
        now_text = now.isoformat().replace("+00:00", "Z")
        next_attempt_text = (now + timedelta(seconds=seconds)).isoformat().replace(
            "+00:00", "Z"
        )
        error = json.dumps(
            {
                "code": "MONTHLY_DESCRIPTION_QUOTA",
                "message": "Scene description is waiting for monthly quota.",
                "retryable": True,
            },
            sort_keys=True,
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Deferred',
                    AttemptCount = AttemptCount + CASE WHEN JobId = ? THEN 1 ELSE 0 END,
                    NextAttemptAtUtc = ?, ErrorJson = ?, FailedAtUtc = NULL,
                    UpdatedAtUtc = ?
                WHERE State IN ('Pending', 'Deferred')
                  AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= ?)
                """,
                (
                    attempted_job_id,
                    next_attempt_text,
                    error,
                    now_text,
                    now_text,
                ),
            )
        return max(0, int(cursor.rowcount))

    def quarantine_description(self, job_id: str, *, code: str, message: str) -> None:
        error = {
            "code": self._safe_error_text(code, 128) or "STAGING_FAILED",
            "message": self._safe_error_text(message, 500) or "Scene preview staging failed.",
            "retryable": False,
        }
        now = utc_now_text()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Failed', AttemptCount = AttemptCount + 1,
                    NextAttemptAtUtc = NULL, ErrorJson = ?, FailedAtUtc = ?, UpdatedAtUtc = ?
                WHERE JobId = ? AND State != 'Sent'
                """,
                (json.dumps(error, sort_keys=True), now, now, job_id),
            )

    def retry_description(self, job_id: str, *, allow_sent: bool = False) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT State FROM DescriptionOutbox WHERE JobId = ?", (job_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Description task {job_id!r} was not found")
            allowed_states = {"Failed", "Deferred", "Pending", "Skipped"}
            if allow_sent:
                allowed_states.add("Sent")
            if row["State"] not in allowed_states:
                raise ValueError("Only an unsent description task can be retried")
            connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Pending', NextAttemptAtUtc = NULL, ErrorJson = NULL,
                    FailedAtUtc = NULL, UpdatedAtUtc = ? WHERE JobId = ?
                """,
                (utc_now_text(), job_id),
            )

    def retry_all_deferred_descriptions(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE DescriptionOutbox
                SET State = 'Pending', NextAttemptAtUtc = NULL, ErrorJson = NULL,
                    FailedAtUtc = NULL, UpdatedAtUtc = ?
                WHERE State = 'Deferred'
                """,
                (utc_now_text(),),
            )
        return max(0, int(cursor.rowcount))

    def description_counts(self, source_id: str | None = None) -> dict[str, int]:
        query = "SELECT State, COUNT(*) AS Total FROM DescriptionOutbox"
        parameters: tuple[Any, ...] = ()
        if source_id:
            query += " WHERE SourceId = ?"
            parameters = (source_id,)
        query += " GROUP BY State"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        counts = {
            "Pending": 0,
            "Deferred": 0,
            "Failed": 0,
            "Sent": 0,
            "Skipped": 0,
        }
        counts.update({str(row["State"]): int(row["Total"]) for row in rows})
        return counts

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

    def queue_bulk_manifest(
        self,
        source_id: str,
        snapshot_id: str,
        *,
        artifact_path: Path,
        artifact_sha256: str,
        artifact_bytes: int,
        entry_count: int,
        superseded_batch_ids: Sequence[str] = (),
    ) -> BulkManifestOutboxItem:
        """Persist a prepared artifact before any network request is made."""

        try:
            normalized_sha256 = artifact_sha256.strip().lower()
            if len(normalized_sha256) != 64:
                raise ValueError
            int(normalized_sha256, 16)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("Bulk artifact SHA-256 is invalid") from exc
        if isinstance(artifact_bytes, bool) or not isinstance(artifact_bytes, int):
            raise ValueError("Bulk artifact byte count must be an integer")
        if isinstance(entry_count, bool) or not isinstance(entry_count, int):
            raise ValueError("Bulk artifact entry count must be an integer")
        if artifact_bytes <= 0 or entry_count <= 0:
            raise ValueError("Bulk artifact byte and entry counts must be positive")
        selected_path = artifact_path.expanduser().resolve(strict=True)
        if not selected_path.is_file() or selected_path.stat().st_size != artifact_bytes:
            raise ValueError("Bulk artifact file size does not match its declaration")
        batch_ids = tuple(dict.fromkeys(str(value) for value in superseded_batch_ids))
        now = utc_now_text()
        with self._connect() as connection:
            source = connection.execute(
                "SELECT 1 FROM SourceBinding WHERE SourceId = ?",
                (source_id,),
            ).fetchone()
            if source is None:
                raise ValueError(f"Local source binding {source_id!r} was not found")
            active = connection.execute(
                """
                SELECT SnapshotId FROM BulkManifestOutbox
                WHERE SourceId = ?
                  AND State NOT IN (
                    'Applied', 'FailedPermanent', 'Cancelled', 'Expired'
                  )
                ORDER BY CreatedAtUtc DESC LIMIT 1
                """,
                (source_id,),
            ).fetchone()
            if active is not None and str(active["SnapshotId"]) != snapshot_id:
                raise ValueError("A bulk manifest import is already active for this source")
            existing = connection.execute(
                """
                SELECT * FROM BulkManifestOutbox
                WHERE SourceId = ? AND SnapshotId = ?
                """,
                (source_id, snapshot_id),
            ).fetchone()
            if existing is not None:
                item = self._bulk_manifest_item(existing)
                if (
                    item.artifact_sha256 != normalized_sha256
                    or item.artifact_bytes != artifact_bytes
                    or item.entry_count != entry_count
                    or item.superseded_batch_ids != batch_ids
                ):
                    raise ValueError(
                        "The bulk snapshot is already associated with different content"
                    )
                return item

            if batch_ids:
                placeholders = ",".join("?" for _ in batch_ids)
                rows = connection.execute(
                    f"""
                    SELECT BatchId, PayloadJson FROM ManifestOutbox
                    WHERE SourceId = ? AND State = 'Pending'
                      AND BatchId IN ({placeholders})
                    """,
                    (source_id, *batch_ids),
                ).fetchall()
                found = {str(row["BatchId"]) for row in rows}
                if found != set(batch_ids):
                    raise ValueError(
                        "Bulk supersede members must be pending batches for this source"
                    )
                represented_entries = 0
                for row in rows:
                    payload = json.loads(str(row["PayloadJson"]))
                    entries = payload.get("entries") or []
                    for entry in entries:
                        content_hash = entry.get("contentSha256")
                        if (
                            entry.get("operation") != "Upsert"
                            or not isinstance(content_hash, str)
                            or len(content_hash) != 64
                        ):
                            raise ValueError(
                                "Bulk supersede batches must contain only hash-enriched upserts"
                            )
                        try:
                            int(content_hash, 16)
                        except ValueError as exc:
                            raise ValueError(
                                "Bulk supersede batches contain an invalid content hash"
                            ) from exc
                    represented_entries += len(entries)
                if represented_entries != entry_count:
                    raise ValueError(
                        "Bulk artifact entry count must match its captured batches"
                    )

            bulk_id = str(uuid.uuid4())
            idempotency_key = f"manifest-import:{source_id}:{snapshot_id}"
            connection.execute(
                """
                INSERT INTO BulkManifestOutbox (
                    BulkId, SourceId, SnapshotId, IdempotencyKey,
                    ArtifactPath, ArtifactSha256, ArtifactBytes, EntryCount,
                    State, SupersededBatchIdsJson, CreatedAtUtc, UpdatedAtUtc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Prepared', ?, ?, ?)
                """,
                (
                    bulk_id,
                    source_id,
                    snapshot_id,
                    idempotency_key,
                    normalized_path(selected_path),
                    normalized_sha256,
                    artifact_bytes,
                    entry_count,
                    json.dumps(batch_ids, separators=(",", ":")),
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM BulkManifestOutbox WHERE BulkId = ?",
                (bulk_id,),
            ).fetchone()
            assert row is not None
            return self._bulk_manifest_item(row)

    def bulk_manifest(self, bulk_id: str) -> BulkManifestOutboxItem | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM BulkManifestOutbox WHERE BulkId = ?",
                (bulk_id,),
            ).fetchone()
        return self._bulk_manifest_item(row) if row is not None else None

    def active_bulk_manifest(
        self, source_id: str
    ) -> BulkManifestOutboxItem | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM BulkManifestOutbox
                WHERE SourceId = ?
                  AND State NOT IN (
                    'Applied', 'FailedPermanent', 'Cancelled', 'Expired'
                  )
                ORDER BY CreatedAtUtc DESC LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        return self._bulk_manifest_item(row) if row is not None else None

    def active_bulk_manifests(self) -> list[BulkManifestOutboxItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM BulkManifestOutbox
                WHERE State NOT IN (
                    'Applied', 'FailedPermanent', 'Cancelled', 'Expired'
                )
                ORDER BY CreatedAtUtc
                """
            ).fetchall()
        return [self._bulk_manifest_item(row) for row in rows]

    def list_bulk_manifests(
        self,
        source_id: str | None = None,
        *,
        limit: int = 100,
    ) -> list[BulkManifestOutboxItem]:
        if not 1 <= limit <= 1000:
            raise ValueError("Bulk outbox limit must be between 1 and 1000")
        query = "SELECT * FROM BulkManifestOutbox"
        parameters: list[Any] = []
        if source_id is not None:
            query += " WHERE SourceId = ?"
            parameters.append(source_id)
        query += " ORDER BY CreatedAtUtc DESC LIMIT ?"
        parameters.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._bulk_manifest_item(row) for row in rows]

    def set_bulk_manifest_server_import(
        self,
        bulk_id: str,
        *,
        server_import_id: str,
        status: str = "AwaitingUpload",
        phase: str | None = None,
    ) -> None:
        now = utc_now_text()
        with self._connect() as connection:
            row = self._require_bulk_manifest(connection, bulk_id)
            current = str(row["ServerImportId"] or "")
            if current and current != server_import_id:
                raise ValueError("Bulk outbox item is already bound to another import")
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET ServerImportId = ?, ServerStatus = ?, ServerPhase = ?,
                    State = ?, UpdatedAtUtc = ?
                WHERE BulkId = ?
                """,
                (server_import_id, status, phase, status, now, bulk_id),
            )

    def update_bulk_manifest_status(
        self,
        bulk_id: str,
        *,
        status: str,
        phase: str | None = None,
        processed_entries: int | None = None,
        failure: Mapping[str, Any] | None = None,
    ) -> None:
        if not status or len(status) > 64:
            raise ValueError("Bulk import status is invalid")
        if phase is not None and (not phase or len(phase) > 64):
            raise ValueError("Bulk import phase is invalid")
        if processed_entries is not None and (
            isinstance(processed_entries, bool)
            or not isinstance(processed_entries, int)
            or processed_entries < 0
        ):
            raise ValueError("Bulk processed entry count is invalid")
        safe_failure = self._safe_bulk_failure(failure)
        with self._connect() as connection:
            bulk = self._require_bulk_manifest(connection, bulk_id)
            if (
                processed_entries is not None
                and processed_entries > int(bulk["EntryCount"])
            ):
                raise ValueError("Bulk processed entry count exceeds the manifest")
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET State = ?, ServerStatus = ?,
                    ServerPhase = COALESCE(?, ServerPhase),
                    ProcessedEntries = COALESCE(?, ProcessedEntries),
                    FailureJson = ?, UpdatedAtUtc = ?
                WHERE BulkId = ?
                """,
                (
                    status,
                    status,
                    phase,
                    processed_entries,
                    json.dumps(safe_failure, sort_keys=True) if safe_failure else None,
                    utc_now_text(),
                    bulk_id,
                ),
            )

    def record_bulk_manifest_result(
        self,
        bulk_id: str,
        *,
        result_path: Path,
        result_sha256: str,
        result_bytes: int,
    ) -> None:
        selected = result_path.expanduser().resolve(strict=True)
        if not selected.is_file() or selected.stat().st_size != result_bytes:
            raise ValueError("Bulk result file size does not match its declaration")
        normalized_sha256 = result_sha256.strip().lower()
        try:
            if len(normalized_sha256) != 64:
                raise ValueError
            int(normalized_sha256, 16)
        except (TypeError, ValueError) as exc:
            raise ValueError("Bulk result SHA-256 is invalid") from exc
        with self._connect() as connection:
            bulk = self._require_bulk_manifest(connection, bulk_id)
            if not bulk["ServerImportId"] or str(bulk["ServerStatus"] or "") not in {
                "Succeeded",
                "CompletedWithErrors",
            }:
                raise ValueError("Bulk result is not available for this import")
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET ResultPath = ?, ResultSha256 = ?, ResultBytes = ?,
                    State = 'ResultReady', UpdatedAtUtc = ?
                WHERE BulkId = ?
                """,
                (
                    normalized_path(selected),
                    normalized_sha256,
                    result_bytes,
                    utc_now_text(),
                    bulk_id,
                ),
            )

    def apply_bulk_result_rows(
        self,
        bulk_id: str,
        rows: Sequence[tuple[int, Mapping[str, Any], Mapping[str, Any]]],
    ) -> int:
        """Apply one contiguous result page and its resume cursor atomically."""

        if not rows:
            raise ValueError("Bulk result page cannot be empty")
        with self._connect() as connection:
            bulk = self._require_bulk_manifest(connection, bulk_id)
            if str(bulk["State"]) not in {"ResultReady", "ApplyingResult"}:
                raise ValueError("Bulk result has not been downloaded and verified")
            current = int(bulk["ResultAppliedThrough"])
            entry_count = int(bulk["EntryCount"])
            expected = current + 1
            for row_number, entry, result in rows:
                if row_number != expected or row_number > entry_count:
                    raise ValueError(
                        "Bulk result rows must be contiguous with the saved cursor"
                    )
                expected += 1
                item_id = str(entry.get("sourceItemId") or "")
                outcome = str(result.get("outcome") or "")
                if outcome not in {
                    "CreatedOccurrence",
                    "UpdatedOccurrence",
                    "DuplicateLinked",
                    "Unchanged",
                    "Rejected",
                }:
                    raise ValueError("Bulk result contains an unsupported outcome")
                if result.get("uploadRequired"):
                    raise ValueError("A Local bulk result unexpectedly requires upload")
                result_item_id = str(result.get("sourceItemId") or "")
                if outcome == "Rejected":
                    if result_item_id != item_id and not result_item_id.startswith(
                        "__invalid_row__:"
                    ):
                        raise ValueError(
                            "Bulk rejected result identity is not a safe placeholder"
                        )
                    continue
                if not item_id or item_id != result_item_id:
                    raise ValueError("Bulk result identity does not match its manifest row")
                if entry.get("operation") != "Upsert":
                    raise ValueError("Bulk result accepted a non-Upsert manifest row")
                required = (
                    entry.get("sourceRevision"),
                    entry.get("fileName"),
                )
                if any(not isinstance(value, str) or not value for value in required):
                    raise ValueError("Bulk manifest row is missing occurrence identity")
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
                        str(bulk["SourceId"]),
                        item_id,
                        str(entry["sourceRevision"]),
                        str(entry.get("localLocator") or entry["fileName"]),
                        utc_now_text(),
                    ),
                )
                self._queue_description_in_transaction(
                    connection,
                    source_id=str(bulk["SourceId"]),
                    entry=entry,
                    result=result,
                )
            applied_through = rows[-1][0]
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET ResultAppliedThrough = ?, State = 'ApplyingResult',
                    UpdatedAtUtc = ?
                WHERE BulkId = ?
                """,
                (applied_through, utc_now_text(), bulk_id),
            )
        return applied_through

    def complete_bulk_manifest(self, bulk_id: str) -> None:
        """Finalize a fully applied result and supersede only captured batches."""

        now = utc_now_text()
        with self._connect() as connection:
            bulk = self._require_bulk_manifest(connection, bulk_id)
            if str(bulk["ServerStatus"] or "") != "Succeeded":
                raise ValueError(
                    "Only a rejection-free bulk result can supersede manifest batches"
                )
            if int(bulk["ResultAppliedThrough"]) != int(bulk["EntryCount"]):
                raise ValueError("Bulk result has not been fully applied")
            batch_ids = tuple(json.loads(str(bulk["SupersededBatchIdsJson"])))
            scan_ids: set[str] = set()
            if batch_ids:
                placeholders = ",".join("?" for _ in batch_ids)
                member_rows = connection.execute(
                    f"""
                    SELECT BatchId, ScanId, State FROM ManifestOutbox
                    WHERE SourceId = ? AND BatchId IN ({placeholders})
                    """,
                    (str(bulk["SourceId"]), *batch_ids),
                ).fetchall()
                if {str(row["BatchId"]) for row in member_rows} != set(batch_ids):
                    raise ValueError("A captured manifest batch no longer exists")
                scan_ids = {str(row["ScanId"]) for row in member_rows}
                note = json.dumps(
                    {
                        "reason": "SupersededByBulkManifestImport",
                        "bulkId": bulk_id,
                        "serverImportId": bulk["ServerImportId"],
                    },
                    sort_keys=True,
                )
                connection.executemany(
                    """
                    UPDATE ManifestOutbox
                    SET State = 'Discarded', FailureJson = ?, DiscardedAtUtc = ?
                    WHERE BatchId = ? AND State = 'Pending'
                    """,
                    ((note, now, batch_id) for batch_id in batch_ids),
                )
            for scan_id in scan_ids:
                remaining = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM ManifestOutbox
                        WHERE ScanId = ? AND State = 'Pending'
                        """,
                        (scan_id,),
                    ).fetchone()[0]
                )
                if remaining == 0:
                    connection.execute(
                        """
                        UPDATE ScanRun SET Status = 'SupersededByBulkManifestImport'
                        WHERE ScanId = ?
                        """,
                        (scan_id,),
                    )
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET State = 'Applied', UpdatedAtUtc = ?, CompletedAtUtc = ?,
                    FailureJson = NULL
                WHERE BulkId = ?
                """,
                (now, now, bulk_id),
            )

    def fail_bulk_manifest(
        self,
        bulk_id: str,
        *,
        state: str,
        code: str,
        message: str,
        server_status: str | None = None,
    ) -> None:
        if state not in {"FailedPermanent", "Cancelled", "Expired"}:
            raise ValueError("Bulk terminal failure state is invalid")
        failure = self._safe_bulk_failure({"code": code, "message": message})
        now = utc_now_text()
        with self._connect() as connection:
            self._require_bulk_manifest(connection, bulk_id)
            connection.execute(
                """
                UPDATE BulkManifestOutbox
                SET State = ?, ServerStatus = ?, FailureJson = ?,
                    UpdatedAtUtc = ?, CompletedAtUtc = ?
                WHERE BulkId = ?
                """,
                (
                    state,
                    server_status or state,
                    json.dumps(failure, sort_keys=True),
                    now,
                    now,
                    bulk_id,
                ),
            )

    @staticmethod
    def _require_bulk_manifest(
        connection: sqlite3.Connection,
        bulk_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM BulkManifestOutbox WHERE BulkId = ?",
            (bulk_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Bulk outbox item {bulk_id!r} was not found")
        return row

    @staticmethod
    def _bulk_manifest_item(row: sqlite3.Row) -> BulkManifestOutboxItem:
        batch_ids = json.loads(str(row["SupersededBatchIdsJson"] or "[]"))
        failure = json.loads(str(row["FailureJson"])) if row["FailureJson"] else None
        return BulkManifestOutboxItem(
            bulk_id=str(row["BulkId"]),
            source_id=str(row["SourceId"]),
            snapshot_id=str(row["SnapshotId"]),
            idempotency_key=str(row["IdempotencyKey"]),
            artifact_path=str(row["ArtifactPath"]),
            artifact_sha256=str(row["ArtifactSha256"]),
            artifact_bytes=int(row["ArtifactBytes"]),
            entry_count=int(row["EntryCount"]),
            state=str(row["State"]),
            server_import_id=(
                str(row["ServerImportId"]) if row["ServerImportId"] else None
            ),
            server_status=(
                str(row["ServerStatus"]) if row["ServerStatus"] else None
            ),
            server_phase=(
                str(row["ServerPhase"]) if row["ServerPhase"] else None
            ),
            processed_entries=int(row["ProcessedEntries"]),
            result_path=str(row["ResultPath"]) if row["ResultPath"] else None,
            result_sha256=(
                str(row["ResultSha256"]) if row["ResultSha256"] else None
            ),
            result_bytes=(int(row["ResultBytes"]) if row["ResultBytes"] else None),
            result_applied_through=int(row["ResultAppliedThrough"]),
            superseded_batch_ids=tuple(str(value) for value in batch_ids),
            failure=failure,
        )

    @classmethod
    def _safe_bulk_failure(
        cls,
        failure: Mapping[str, Any] | None,
    ) -> dict[str, str] | None:
        if not failure:
            return None
        code = cls._safe_error_text(failure.get("code"), 128) or "BULK_IMPORT_FAILED"
        message = cls._safe_error_text(failure.get("message"), 500) or (
            "The bulk manifest import failed."
        )
        return {"code": code, "message": message}

    def pending_count(self) -> int:
        with self._connect() as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM ManifestOutbox WHERE State = 'Pending'"
                ).fetchone()[0]
            )

    def suspend_pending_batches(self, source_id: str) -> tuple[str, ...]:
        """Atomically supersede one source's pending API batches."""

        now = utc_now_text()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT BatchId, ScanId
                FROM ManifestOutbox
                WHERE SourceId = ? AND State = 'Pending'
                ORDER BY CreatedAtUtc, SequenceNumber
                """,
                (source_id,),
            ).fetchall()
            batch_ids = tuple(str(row["BatchId"]) for row in rows)
            scan_ids = {str(row["ScanId"]) for row in rows}
            if batch_ids:
                connection.executemany(
                    """
                    UPDATE ManifestOutbox
                    SET State = 'Discarded',
                        FailureJson = ?,
                        DiscardedAtUtc = ?
                    WHERE BatchId = ? AND State = 'Pending'
                    """,
                    (
                        (
                            json.dumps(
                                {"reason": "SupersededByOneFileMySqlImport"},
                                sort_keys=True,
                            ),
                            now,
                            batch_id,
                        )
                        for batch_id in batch_ids
                    ),
                )
                connection.executemany(
                    """
                    UPDATE ScanRun
                    SET Status = 'SupersededByOneFileImport'
                    WHERE ScanId = ?
                    """,
                    ((scan_id,) for scan_id in scan_ids),
                )
        return batch_ids

    def restore_suspended_batches(self, batch_ids: Sequence[str]) -> None:
        """Restore a superseded batch set when the MySQL transaction fails."""

        if not batch_ids:
            return
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT ScanId
                FROM ManifestOutbox
                WHERE BatchId IN ({",".join("?" for _ in batch_ids)})
                  AND State = 'Discarded'
                """,
                tuple(batch_ids),
            ).fetchall()
            connection.executemany(
                """
                UPDATE ManifestOutbox
                SET State = 'Pending',
                    FailureJson = NULL,
                    DiscardedAtUtc = NULL
                WHERE BatchId = ? AND State = 'Discarded'
                """,
                ((batch_id,) for batch_id in batch_ids),
            )
            connection.executemany(
                "UPDATE ScanRun SET Status = 'Queued' WHERE ScanId = ?",
                ((str(row["ScanId"]),) for row in rows),
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
