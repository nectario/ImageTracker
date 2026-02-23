from __future__ import annotations

import hashlib
import json
import mimetypes
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Callable, Optional

from imagetracker.captioner import CaptionResult, OpenAIVisionCaptioner
from imagetracker.config import Settings
from imagetracker.db import Database, utc_now
from imagetracker.gps_extractor import ExifGpsExtractor, GpsCoordinates, GpsExtractor
from imagetracker.migrations import MigrationRunner
from imagetracker.repositories import ImageAssetRepository, ImageUpsertPayload


@dataclass
class LocalPhotoSyncResult:
    scanned_count: int
    eligible_count: int
    skipped_count: int
    upserted_count: int
    captioned_count: int


def parse_cutoff_date(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("cutoff date is required")

    # Accept YYYY-MM-DD and full ISO datetimes.
    if len(text) == 10:
        local_date = date.fromisoformat(text)
        local_datetime = datetime.combine(local_date, time.min)
        local_tz = datetime.now().astimezone().tzinfo
        aware_local = local_datetime.replace(tzinfo=local_tz)
        return aware_local.astimezone(timezone.utc).replace(tzinfo=None)

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        parsed = parsed.replace(tzinfo=local_tz)

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_from_timestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(tzinfo=None)


def _drive_item_id_for_path(path: Path) -> str:
    # Stable ID for local files so repeat runs can skip already-processed items.
    normalized = str(path.resolve()).lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _is_supported_photo(path: Path) -> bool:
    return path.suffix.lower() in {
        ".jpg",
        ".jpeg",
        ".heic",
        ".heif",
        ".png",
        ".webp",
        ".tif",
        ".tiff",
    }


class LocalPhotoSyncService:
    SOURCE = "LocalFile"

    def __init__(
        self,
        settings: Settings,
        database: Database,
        migration_runner: MigrationRunner,
        image_repo: Optional[ImageAssetRepository] = None,
        captioner: Optional[OpenAIVisionCaptioner] = None,
        gps_extractor: Optional[GpsExtractor] = None,
        now_fn: Callable[[], datetime] = utc_now,
    ):
        self._settings = settings
        self._database = database
        self._migration_runner = migration_runner
        self._image_repo = image_repo or ImageAssetRepository()
        self._captioner = captioner
        self._gps_extractor = gps_extractor or ExifGpsExtractor()
        self._now_fn = now_fn

    def run_sync(
        self,
        *,
        directory: Path,
        cutoff_utc: datetime,
        force: bool,
    ) -> LocalPhotoSyncResult:
        if not directory.exists() or not directory.is_dir():
            raise RuntimeError(f"Directory does not exist or is not a directory: {directory}")

        self._apply_migrations()

        scanned_count = 0
        eligible_count = 0
        skipped_count = 0
        upserted_count = 0
        captioned_count = 0

        now = self._now_fn()

        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            if not _is_supported_photo(path):
                continue

            scanned_count += 1
            stat = path.stat()
            file_modified_utc = _utc_from_timestamp(stat.st_mtime)

            if file_modified_utc < cutoff_utc:
                continue

            eligible_count += 1
            drive_item_id = _drive_item_id_for_path(path)

            with self._database.connection() as conn:
                existing = self._image_repo.get_by_source_and_drive_item(conn, self.SOURCE, drive_item_id)

            if existing and not force:
                skipped_count += 1
                continue

            try:
                content_bytes = path.read_bytes()
            except Exception as exc:
                print(f"Skipping unreadable file {path}: {exc}", file=sys.stderr)
                continue

            mime_type, _ = mimetypes.guess_type(str(path))
            extracted_gps = self._extract_gps(
                content_bytes=content_bytes,
                mime_type=mime_type,
                file_name=path.name,
            )

            latitude = extracted_gps.latitude if extracted_gps else (self._as_float(existing, "Latitude") if existing else None)
            longitude = (
                extracted_gps.longitude if extracted_gps else (self._as_float(existing, "Longitude") if existing else None)
            )
            altitude = (
                extracted_gps.altitude
                if extracted_gps and extracted_gps.altitude is not None
                else (self._as_float(existing, "Altitude") if existing else None)
            )

            payload = ImageUpsertPayload(
                source=self.SOURCE,
                drive_item_id=drive_item_id,
                file_name=path.name,
                taken_datetime_utc=file_modified_utc,
                latitude=latitude,
                longitude=longitude,
                altitude=altitude,
                raw_graph_json=json.dumps(
                    {
                        "LocalPath": str(path.resolve()),
                        "FileModifiedUtc": file_modified_utc.isoformat() + "Z",
                        "FileSizeBytes": stat.st_size,
                    },
                    ensure_ascii=False,
                ),
                inserted_at_utc=now,
                updated_at_utc=now,
            )

            with self._database.connection() as conn:
                self._image_repo.upsert(conn, payload)
            upserted_count += 1

            if not self._should_caption(existing, force=force):
                continue

            caption = self._generate_caption(content_bytes)
            if not caption:
                continue

            with self._database.connection() as conn:
                self._image_repo.update_caption(
                    conn,
                    source=self.SOURCE,
                    drive_item_id=drive_item_id,
                    description=caption.short_description,
                    description_model=caption.model,
                    updated_at_utc=now,
                )
            captioned_count += 1

        return LocalPhotoSyncResult(
            scanned_count=scanned_count,
            eligible_count=eligible_count,
            skipped_count=skipped_count,
            upserted_count=upserted_count,
            captioned_count=captioned_count,
        )

    def _extract_gps(
        self,
        *,
        content_bytes: bytes,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[GpsCoordinates]:
        if not self._gps_extractor:
            return None
        if hasattr(self._gps_extractor, "is_available") and not getattr(self._gps_extractor, "is_available"):
            return None

        try:
            return self._gps_extractor.extract(content_bytes, mime_type=mime_type, file_name=file_name)
        except Exception as exc:
            print(f"GPS extraction skipped for {file_name}: {exc}", file=sys.stderr)
            return None

    def _generate_caption(self, content_bytes: bytes) -> Optional[CaptionResult]:
        if not self._captioner:
            return None

        try:
            return self._captioner.generate_caption(content_bytes)
        except Exception as exc:
            print(f"Caption generation skipped: {exc}", file=sys.stderr)
            return None

    def _should_caption(self, existing: Optional[dict], *, force: bool) -> bool:
        if not self._captioner:
            return False
        if force:
            return True
        if existing is None:
            return True

        description = existing.get("Description")
        if not description:
            return True

        existing_model = existing.get("DescriptionModel")
        current_model = getattr(self._captioner, "model", None)
        if current_model and existing_model != current_model:
            return True

        if existing.get("DescriptionUpdatedAtUtc") is None:
            return True

        return False

    def _apply_migrations(self) -> None:
        with self._database.connection() as conn:
            self._migration_runner.apply_all(conn)

    @staticmethod
    def _as_float(existing: Optional[dict], key: str) -> Optional[float]:
        if not existing:
            return None
        value = existing.get(key)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


__all__ = [
    "LocalPhotoSyncResult",
    "LocalPhotoSyncService",
    "parse_cutoff_date",
]
