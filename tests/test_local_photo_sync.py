from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from ImageTracker import (
    CaptionResult,
    GpsCoordinates,
    LocalPhotoSyncService,
    Settings,
    _drive_item_id_for_path,
    parse_cutoff_date,
)


@dataclass
class DummyConnection:
    pass


class FakeDatabase:
    @contextmanager
    def connection(self):
        yield DummyConnection()


class FakeMigrationRunner:
    def __init__(self):
        self.called = 0

    def apply_all(self, conn):
        self.called += 1


class FakeImageRepository:
    def __init__(self):
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.upserts = []
        self.caption_updates = []

    def get_by_source_and_drive_item(self, conn, source: str, drive_item_id: str):
        return self.rows.get(drive_item_id)

    def upsert(self, conn, payload):
        row = self.rows.get(payload.drive_item_id, {}).copy()
        row.update(
            {
                "Source": payload.source,
                "DriveItemId": payload.drive_item_id,
                "FileName": payload.file_name,
                "Latitude": payload.latitude,
                "Longitude": payload.longitude,
                "Altitude": payload.altitude,
                "Description": row.get("Description"),
                "DescriptionModel": row.get("DescriptionModel"),
                "DescriptionUpdatedAtUtc": row.get("DescriptionUpdatedAtUtc"),
            }
        )
        self.rows[payload.drive_item_id] = row
        self.upserts.append(payload)

    def update_caption(
        self,
        conn,
        source: str,
        drive_item_id: str,
        description: str,
        description_model: str,
        updated_at_utc: datetime,
    ):
        row = self.rows.setdefault(drive_item_id, {})
        row["Description"] = description
        row["DescriptionModel"] = description_model
        row["DescriptionUpdatedAtUtc"] = updated_at_utc
        self.caption_updates.append((source, drive_item_id, description, description_model))


class FakeCaptioner:
    def __init__(self, model: str = "gpt-5.2"):
        self.model = model
        self.calls = 0

    def generate_caption(self, image_bytes: bytes):
        self.calls += 1
        return CaptionResult(short_description="A local photo description.", model=self.model)


class FakeGpsExtractor:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    def extract(self, content_bytes: bytes, *, mime_type: Optional[str], file_name: str):
        self.calls.append((content_bytes, mime_type, file_name))
        return self.result


class UnavailableGpsExtractor:
    is_available = False

    def extract(self, content_bytes: bytes, *, mime_type: Optional[str], file_name: str):
        raise AssertionError("Should not be called when extractor is unavailable")


def make_settings() -> Settings:
    return Settings(
        onedrive_tenant="consumers",
        onedrive_client_id="client-id",
        onedrive_scopes=["User.Read", "Files.Read.All"],
        onedrive_camera_upload_path="/Pictures/Camera Roll",
        onedrive_camera_upload_fallback_paths=[],
        mysql_dsn="mysql://user:password@localhost:3306/db",
        openai_api_key=None,
        openai_vision_model="gpt-5.2",
        photo_sync_initial_cutoff_days=14,
        photo_caption_max_words=18,
    )


def build_service(
    *,
    image_repo: Optional[FakeImageRepository] = None,
    captioner: Optional[FakeCaptioner] = None,
    gps_extractor=None,
):
    now = datetime(2026, 2, 23, 12, 0, 0)
    image_repo = image_repo or FakeImageRepository()

    service = LocalPhotoSyncService(
        settings=make_settings(),
        database=FakeDatabase(),
        migration_runner=FakeMigrationRunner(),
        image_repo=image_repo,
        captioner=captioner,
        gps_extractor=gps_extractor,
        now_fn=lambda: now,
    )

    return service, image_repo


def set_file_mtime(path: Path, dt_utc: datetime) -> None:
    ts = dt_utc.replace(tzinfo=timezone.utc).timestamp()
    path.touch(exist_ok=True)
    path.write_bytes(b"fake-image-bytes")
    import os

    os.utime(path, (ts, ts))


def test_parse_cutoff_date_accepts_date_only():
    cutoff = parse_cutoff_date("2026-02-01")
    assert isinstance(cutoff, datetime)


def test_skip_processed_files_without_force(tmp_path: Path):
    old_file = tmp_path / "IMG_0001.JPG"
    new_file = tmp_path / "IMG_0002.JPG"

    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(old_file, cutoff_utc - timedelta(days=2))
    set_file_mtime(new_file, cutoff_utc + timedelta(hours=1))

    image_repo = FakeImageRepository()
    existing_id = _drive_item_id_for_path(new_file)
    image_repo.rows[existing_id] = {
        "Source": "LocalFile",
        "DriveItemId": existing_id,
        "Description": "Already processed.",
    }

    service, _ = build_service(image_repo=image_repo, gps_extractor=UnavailableGpsExtractor())

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)

    assert result.scanned_count == 2
    assert result.eligible_count == 1
    assert result.skipped_count == 1
    assert result.upserted_count == 0


def test_force_reprocesses_existing_and_preserves_filename(tmp_path: Path):
    photo = tmp_path / "IMG_8677.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    image_repo = FakeImageRepository()
    drive_item_id = _drive_item_id_for_path(photo)
    image_repo.rows[drive_item_id] = {
        "Source": "LocalFile",
        "DriveItemId": drive_item_id,
        "Description": "Old caption.",
        "DescriptionModel": "old-model",
        "DescriptionUpdatedAtUtc": datetime(2026, 2, 20, 0, 0, 0),
        "Latitude": None,
        "Longitude": None,
        "Altitude": None,
    }

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=37.7749, longitude=-122.4194, altitude=10.0)
    )
    captioner = FakeCaptioner()

    service, image_repo = build_service(
        image_repo=image_repo,
        captioner=captioner,
        gps_extractor=gps_extractor,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=True)

    assert result.scanned_count == 1
    assert result.eligible_count == 1
    assert result.skipped_count == 0
    assert result.upserted_count == 1
    assert result.captioned_count == 1
    assert len(image_repo.upserts) == 1

    payload = image_repo.upserts[0]
    assert payload.file_name == "IMG_8677.JPG"
    assert payload.latitude == pytest.approx(37.7749)
    assert payload.longitude == pytest.approx(-122.4194)
    assert payload.altitude == pytest.approx(10.0)

    assert captioner.calls == 1
    assert len(gps_extractor.calls) == 1
