#!/usr/bin/env python
"""Load one complete Local source manifest into MySQL from one CSV file.

This is an explicit WSL/admin fast path. It does not expose MySQL credentials
to consumer clients, does not read media contents, and inserts only missing
pending-hash occurrences. A later normal sync performs SHA-256 deduplication
and metadata enrichment.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence
from uuid import uuid4

from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cli.imagetracker_cli.auth import TokenStore  # noqa: E402
from cli.imagetracker_cli.config import ConfigStore  # noqa: E402
from cli.imagetracker_cli.media import MediaScanner, stream_sha256  # noqa: E402
from cli.imagetracker_cli.state import LocalState, SourceBinding  # noqa: E402
from infra.scripts.migrate_enrichment import _connect, _secret  # noqa: E402


CSV_COLUMNS = (
    "OccurrencePublicId",
    "ChangePublicId",
    "SourceItemId",
    "SourceRevision",
    "OriginalFileName",
    "LocalLocator",
    "ObservedByteSize",
)
TEMPORARY_TABLE = "TempImageTrackerOneFileManifest"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Source ID, name, or local path")
    parser.add_argument("--apply", action="store_true", help="Load and commit the file")
    parser.add_argument(
        "--replace-pending-outbox",
        action="store_true",
        help=(
            "Supersede this source's stopped 500-row pending batches. They are "
            "restored automatically if the MySQL transaction fails."
        ),
    )
    parser.add_argument("--workers", type=int, help="Parallel filesystem metadata workers")
    parser.add_argument("--region", default=os.environ.get("IMAGETRACKER_AWS_REGION", "us-east-2"))
    parser.add_argument(
        "--db-parameter",
        default=os.environ.get(
            "IMAGETRACKER_DB_SECRET_PARAMETER", "/imagetracker/prod/mysql"
        ),
    )
    parser.add_argument(
        "--admin-env-file",
        type=Path,
        help=(
            "Ignored env file containing MYSQL_HOST, MYSQL_PORT, "
            "MYSQL_USERNAME, and MYSQL_PASSWORD for the one-file admin load."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable output")
    return parser


def _runtime_state(source_selector: str) -> tuple[LocalState, SourceBinding]:
    store = ConfigStore()
    tokens = TokenStore(store.fallback_token_path).load()
    if tokens is None or not tokens.local_subject:
        raise RuntimeError("Sign in with ImageTracker before running a one-file import")
    state = LocalState(store.state_path_for_subject(tokens.local_subject))
    binding = state.resolve_binding(source_selector)
    if binding.storage_mode != "Local":
        raise RuntimeError("The one-file importer accepts only Local sources")
    return state, binding


def _source_outbox(
    state: LocalState, source_id: str
) -> tuple[list[Any], list[Any]]:
    pending = state.pending_batches(source_id)
    failed = [
        batch
        for batch in state.list_outbox(state="Failed", limit=1_000)
        if batch.source_id == source_id
    ]
    return pending, failed


def _output_path() -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = ROOT / "build" / "mysql-imports" / f"{run_id}-{uuid4().hex[:8]}"
    directory.mkdir(parents=True, exist_ok=False)
    return directory / "manifest.csv"


def admin_secret_from_env_file(path: Path) -> dict[str, Any]:
    """Load the existing admin credential without logging any value."""

    selected = path.expanduser().resolve(strict=True)
    if not selected.is_file():
        raise RuntimeError("The MySQL admin env file was not found")
    values = dotenv_values(selected)
    required = {
        "host": values.get("MYSQL_HOST"),
        "port": values.get("MYSQL_PORT", "3306"),
        "user": (
            values.get("MYSQL_USERNAME")
            or values.get("MYSQL_USERID")
            or values.get("MYSQL_USER")
            or "admin"
        ),
        "password": values.get("MYSQL_PASSWORD"),
    }
    if not all(isinstance(value, str) and value for value in required.values()):
        raise RuntimeError("The MySQL admin env file is incomplete")
    return {
        **required,
        "database": "ImageTracker",
        "tls": True,
    }


def write_manifest_csv(
    path: Path,
    entries: Sequence[Mapping[str, Any]],
) -> int:
    """Write exactly one CSV with MySQL-compatible escaping."""

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(
            handle,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
            escapechar="\\",
            doublequote=False,
        )
        writer.writerow(CSV_COLUMNS)
        for entry in entries:
            writer.writerow(
                (
                    str(uuid4()),
                    str(uuid4()),
                    str(entry["sourceItemId"]),
                    str(entry["sourceRevision"]),
                    str(entry["fileName"]),
                    str(entry.get("localLocator") or ""),
                    int(entry["byteSize"]),
                )
            )
    if os.name != "nt":
        path.chmod(0o600)
    return len(entries)


def _create_temporary_table(cursor: Any) -> None:
    cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {TEMPORARY_TABLE}")
    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE {TEMPORARY_TABLE} (
            OccurrencePublicId CHAR(36) NOT NULL,
            ChangePublicId CHAR(36) NOT NULL,
            SourceItemId VARCHAR(512) NOT NULL,
            SourceRevision VARCHAR(255) NOT NULL,
            OriginalFileName VARCHAR(512) NOT NULL,
            LocalLocator TEXT NULL,
            ObservedByteSize BIGINT UNSIGNED NOT NULL,
            PRIMARY KEY (SourceItemId),
            UNIQUE KEY Ux_TempOccurrencePublicId (OccurrencePublicId),
            UNIQUE KEY Ux_TempChangePublicId (ChangePublicId)
        ) ENGINE=InnoDB
        """
    )


def _load_csv(cursor: Any, path: Path) -> int:
    cursor.execute(
        f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {TEMPORARY_TABLE}
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '\\\\'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (
            OccurrencePublicId,
            ChangePublicId,
            SourceItemId,
            SourceRevision,
            OriginalFileName,
            LocalLocator,
            ObservedByteSize
        )
        """,
        (str(path),),
    )
    cursor.execute(f"SELECT COUNT(*) AS RowCount FROM {TEMPORARY_TABLE}")
    return int(cursor.fetchone()["RowCount"])


def load_one_file(
    *,
    path: Path,
    source_public_id: str,
    region: str,
    db_parameter: str,
    database_secret: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Load, set-merge, and commit the complete file once."""

    connection = _connect(
        database_secret or _secret(region, db_parameter),
        local_infile=True,
        timeout_seconds=600,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT Id, UserId, DeviceId, PublicId
                FROM MediaSource
                WHERE PublicId = %s AND SourceStatus = 'Active'
                FOR UPDATE
                """,
                (source_public_id,),
            )
            source = cursor.fetchone()
            if source is None:
                raise RuntimeError("The active ImageTracker source was not found in MySQL")
            source_id = int(source["Id"])
            user_id = int(source["UserId"])
            device_id = int(source["DeviceId"])

            _create_temporary_table(cursor)
            loaded = _load_csv(cursor, path)

            cursor.execute(
                f"""
                INSERT INTO MediaOccurrence (
                    PublicId,
                    UserId,
                    MediaSourceId,
                    MediaAssetId,
                    SourceItemId,
                    OriginalFileName,
                    LocalLocator,
                    SourceRevision,
                    ObservedByteSize,
                    HashStatus,
                    AvailabilityState,
                    DeletionState,
                    FirstSeenAtUtc,
                    LastSeenAtUtc,
                    CreatedAtUtc,
                    UpdatedAtUtc
                )
                SELECT
                    Stage.OccurrencePublicId,
                    %s,
                    %s,
                    NULL,
                    Stage.SourceItemId,
                    Stage.OriginalFileName,
                    NULLIF(Stage.LocalLocator, ''),
                    Stage.SourceRevision,
                    Stage.ObservedByteSize,
                    'Pending',
                    'Available',
                    'Active',
                    UTC_TIMESTAMP(6),
                    UTC_TIMESTAMP(6),
                    UTC_TIMESTAMP(6),
                    UTC_TIMESTAMP(6)
                FROM {TEMPORARY_TABLE} AS Stage
                LEFT JOIN MediaOccurrence AS Existing
                  ON Existing.UserId = %s
                 AND Existing.MediaSourceId = %s
                 AND Existing.SourceItemId = Stage.SourceItemId
                WHERE Existing.Id IS NULL
                """,
                (user_id, source_id, user_id, source_id),
            )
            inserted = int(cursor.rowcount)

            cursor.execute(
                f"""
                INSERT INTO MediaChange (
                    PublicId,
                    UserId,
                    DeviceId,
                    MediaSourceId,
                    MediaOccurrenceId,
                    EntityType,
                    EntityId,
                    EntityPublicId,
                    ChangeType,
                    CreatedAtUtc
                )
                SELECT
                    Stage.ChangePublicId,
                    %s,
                    %s,
                    %s,
                    Occurrence.Id,
                    'MediaOccurrence',
                    Occurrence.Id,
                    Occurrence.PublicId,
                    'Upsert',
                    UTC_TIMESTAMP(6)
                FROM {TEMPORARY_TABLE} AS Stage
                JOIN MediaOccurrence AS Occurrence
                  ON Occurrence.UserId = %s
                 AND Occurrence.MediaSourceId = %s
                 AND Occurrence.PublicId = Stage.OccurrencePublicId
                """,
                (user_id, device_id, source_id, user_id, source_id),
            )
            change_rows = int(cursor.rowcount)
            if change_rows != inserted:
                raise RuntimeError("The one-file change-feed count did not match inserts")

            cursor.execute(
                """
                UPDATE MediaSource
                SET LastManifestAtUtc = UTC_TIMESTAMP(6),
                    LastSuccessAtUtc = UTC_TIMESTAMP(6),
                    UpdatedAtUtc = UTC_TIMESTAMP(6)
                WHERE Id = %s AND UserId = %s
                """,
                (source_id, user_id),
            )
            if inserted:
                cursor.execute(
                    """
                    INSERT INTO MediaChange (
                        PublicId,
                        UserId,
                        DeviceId,
                        MediaSourceId,
                        EntityType,
                        EntityId,
                        EntityPublicId,
                        ChangeType,
                        CreatedAtUtc
                    )
                    VALUES (%s, %s, %s, %s, 'MediaSource', %s, %s, 'Upsert',
                            UTC_TIMESTAMP(6))
                    """,
                    (
                        str(uuid4()),
                        user_id,
                        device_id,
                        source_id,
                        source_id,
                        str(source["PublicId"]),
                    ),
                )
        connection.commit()
        return {
            "loaded": loaded,
            "inserted": inserted,
            "alreadyPresent": loaded - inserted,
            "changeRows": change_rows,
        }
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def run(args: argparse.Namespace) -> dict[str, Any]:
    state, binding = _runtime_state(args.source)
    pending, failed = _source_outbox(state, binding.source_id)
    if args.apply and failed:
        raise RuntimeError(
            "Resolve this source's failed manifest batches before a one-file import."
        )
    if args.apply and pending and not args.replace_pending_outbox:
        raise RuntimeError(
            "This stopped source has pending 500-row batches. Rerun with "
            "--replace-pending-outbox to supersede them with the one-file import."
        )

    suspended: tuple[str, ...] = ()
    database_committed = False
    if args.apply and pending:
        suspended = state.suspend_pending_batches(binding.source_id)
    try:
        scanner = MediaScanner(state, workers=args.workers)
        scan = scanner.scan(
            binding.source_id,
            Path(binding.root_path),
            force_rehash=True,
            fast_add=True,
            workers=args.workers,
            progress=(lambda message: None) if args.json else print,
        )
        if not scan.complete_read or scan.failed:
            raise RuntimeError(
                "The source scan was incomplete; no MySQL file was loaded"
            )

        output = _output_path()
        rows = write_manifest_csv(output, scan.entries)
        payload: dict[str, Any] = {
            "sourceId": binding.source_id,
            "rows": rows,
            "file": str(output),
            "fileBytes": output.stat().st_size,
            "fileSha256": stream_sha256(output),
            "applied": False,
            "supersededBatches": len(suspended),
            "scanSeconds": round(scan.elapsed_seconds, 3),
            "scanFilesPerSecond": round(scan.files_per_second, 1),
        }
        if not args.apply:
            return payload

        database = load_one_file(
            path=output,
            source_public_id=binding.source_id,
            region=args.region,
            db_parameter=args.db_parameter,
            database_secret=(
                admin_secret_from_env_file(args.admin_env_file)
                if args.admin_env_file is not None
                else None
            ),
        )
        database_committed = True
        state.record_known_occurrences(binding.source_id, scan.entries)
        payload["applied"] = True
        payload["database"] = database
        return payload
    except BaseException:
        if suspended and not database_committed:
            state.restore_suspended_batches(suspended)
        raise


def main() -> int:
    args = _parser().parse_args()
    try:
        payload = run(args)
    except Exception as exc:
        if args.json:
            print(
                json.dumps(
                    {"status": "failed", "error": type(exc).__name__},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    else:
        print(f"One manifest file: {payload['file']}")
        print(f"Rows: {payload['rows']:,}")
        print(f"Size: {payload['fileBytes'] / (1024 * 1024):,.2f} MiB")
        print(f"SHA-256: {payload['fileSha256']}")
        if payload["applied"]:
            database = payload["database"]
            print(
                "MySQL commit: "
                f"{database['inserted']:,} inserted · "
                f"{database['alreadyPresent']:,} already present"
            )
        else:
            print("Dry run only. Add --apply to load and commit this one file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
