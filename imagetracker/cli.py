from __future__ import annotations

import argparse
import sys
from pathlib import Path

from imagetracker.config import load_settings
from imagetracker.db import Database, DbError, parse_mysql_config, utc_now
from imagetracker.local_photo_sync import LocalPhotoSyncService, parse_cutoff_date
from imagetracker.migrations import MigrationRunner
from imagetracker.onedrive_auth import AuthRequiredError, OneDriveAuthService
from imagetracker.photo_sync import PhotoSyncService, build_default_captioner
from imagetracker.repositories import TokenCacheRepository


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="imagetracker")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("photos:auth", help="Authenticate with OneDrive via device code flow")
    subparsers.add_parser("photos:sync", help="Run incremental OneDrive photo sync")
    local_sync_parser = subparsers.add_parser(
        "photos:sync-local",
        help="Run local directory photo sync with cutoff date",
    )
    local_sync_parser.add_argument(
        "--directory",
        required=True,
        help="Directory containing local photos to process",
    )
    local_sync_parser.add_argument(
        "--cutoff-date",
        required=True,
        help="Cutoff date (YYYY-MM-DD or ISO datetime). Only files >= cutoff are processed.",
    )
    local_sync_parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess files even if already processed before",
    )
    return parser


def _build_database() -> Database:
    settings = load_settings()
    mysql_config = parse_mysql_config(settings)
    return Database(mysql_config)


def _build_migration_runner() -> MigrationRunner:
    root = Path(__file__).resolve().parent.parent
    return MigrationRunner(root / "migrations")


def run_photos_auth() -> int:
    settings = load_settings()

    if not settings.onedrive_client_id:
        print("ONEDRIVE_CLIENT_ID is required", file=sys.stderr)
        return 2

    try:
        database = _build_database()
    except DbError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    migration_runner = _build_migration_runner()
    auth_service = OneDriveAuthService(settings)
    token_repo = TokenCacheRepository()

    with database.connection() as conn:
        migration_runner.apply_all(conn)

    with database.connection() as conn:
        existing_cache_row = token_repo.get(conn)

    cache_json = auth_service.run_device_code_auth(
        existing_cache_row["CacheJson"] if existing_cache_row else None
    )

    with database.connection() as conn:
        token_repo.set(conn, cache_json, utc_now())

    print("OneDrive token cache stored in OneDriveTokenCache.")
    return 0


def run_photos_sync() -> int:
    settings = load_settings()

    if not settings.onedrive_client_id:
        print("ONEDRIVE_CLIENT_ID is required", file=sys.stderr)
        return 2

    try:
        database = _build_database()
    except DbError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    migration_runner = _build_migration_runner()
    auth_service = OneDriveAuthService(settings)
    captioner = build_default_captioner(settings)

    service = PhotoSyncService(
        settings=settings,
        database=database,
        migration_runner=migration_runner,
        auth_service=auth_service,
        captioner=captioner,
    )

    try:
        result = service.run_sync()
    except AuthRequiredError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(
        "Sync complete: "
        f"processed={result.processed_count}, "
        f"upserted={result.upserted_count}, "
        f"deleted={result.deleted_count}, "
        f"captioned={result.captioned_count}"
    )
    return 0


def run_photos_sync_local(directory: str, cutoff_date: str, force: bool) -> int:
    settings = load_settings()

    try:
        database = _build_database()
    except DbError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        cutoff_utc = parse_cutoff_date(cutoff_date)
    except ValueError as exc:
        print(f"Invalid --cutoff-date: {exc}", file=sys.stderr)
        return 2

    migration_runner = _build_migration_runner()
    captioner = build_default_captioner(settings)

    service = LocalPhotoSyncService(
        settings=settings,
        database=database,
        migration_runner=migration_runner,
        captioner=captioner,
    )

    result = service.run_sync(
        directory=Path(directory),
        cutoff_utc=cutoff_utc,
        force=force,
    )

    print(
        "Local sync complete: "
        f"scanned={result.scanned_count}, "
        f"eligible={result.eligible_count}, "
        f"skipped={result.skipped_count}, "
        f"upserted={result.upserted_count}, "
        f"captioned={result.captioned_count}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "photos:auth":
        return run_photos_auth()
    if args.command == "photos:sync":
        return run_photos_sync()
    if args.command == "photos:sync-local":
        return run_photos_sync_local(
            directory=args.directory,
            cutoff_date=args.cutoff_date,
            force=args.force,
        )

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
