from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import pytest

from imagetracker.captioner import CaptionResult
from imagetracker.config import Settings
from imagetracker.graph_client import GraphApiError
from imagetracker.photo_sync import PhotoSyncService
from imagetracker.repositories import ImageAssetRepository


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


class FakeAuthService:
    def __init__(self):
        self.calls = 0

    def acquire_access_token(self, token_cache_json: Optional[str]):
        self.calls += 1
        return "access-token", '{"ok":true}'


class FakeTokenCacheRepository:
    def __init__(self):
        self.value = {"CacheJson": "{}"}
        self.set_calls = []

    def get(self, conn):
        return self.value

    def set(self, conn, cache_json: str, updated_at_utc: datetime):
        self.set_calls.append((cache_json, updated_at_utc))


class FakeSyncStateRepository:
    def __init__(self, state: Optional[Dict[str, Any]]):
        self.state = state
        self.upsert_calls = []

    def get(self, conn):
        return self.state

    def upsert(
        self,
        conn,
        *,
        folder_drive_item_id,
        folder_path,
        delta_link,
        last_run_at_utc,
        last_success_at_utc,
        last_error,
        updated_at_utc,
    ):
        self.state = {
            "Id": 1,
            "FolderDriveItemId": folder_drive_item_id,
            "FolderPath": folder_path,
            "DeltaLink": delta_link,
            "LastRunAtUtc": last_run_at_utc,
            "LastSuccessAtUtc": last_success_at_utc,
            "LastError": last_error,
            "UpdatedAtUtc": updated_at_utc,
        }
        self.upsert_calls.append(self.state.copy())


class FakeImageRepository:
    def __init__(self):
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.upserts = []
        self.deleted = []
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
                "ShortDescription": row.get("ShortDescription"),
                "ShortDescriptionModel": row.get("ShortDescriptionModel"),
                "ShortDescriptionUpdatedAtUtc": row.get("ShortDescriptionUpdatedAtUtc"),
            }
        )
        self.rows[payload.drive_item_id] = row
        self.upserts.append(payload)

    def mark_deleted(self, conn, source: str, drive_item_id: str, deleted_at_utc: datetime):
        self.deleted.append((source, drive_item_id, deleted_at_utc))

    def update_caption(
        self,
        conn,
        source: str,
        drive_item_id: str,
        short_description: str,
        short_description_model: str,
        updated_at_utc: datetime,
    ):
        row = self.rows.setdefault(drive_item_id, {})
        row["ShortDescription"] = short_description
        row["ShortDescriptionModel"] = short_description_model
        row["ShortDescriptionUpdatedAtUtc"] = updated_at_utc
        self.caption_updates.append((source, drive_item_id, short_description, short_description_model))


class FakeGraphClient:
    def __init__(self, pages: Dict[str, Dict[str, Any]], folder_paths: Optional[Dict[str, Dict[str, Any]]] = None):
        self.pages = pages
        self.folder_paths = folder_paths or {}
        self.requested_pages = []
        self.resolved_paths = []
        self.thumbnail_calls = []

    def get(self, path_or_url: str):
        self.requested_pages.append(path_or_url)
        return self.pages[path_or_url]

    def resolve_folder_by_path(self, folder_path: str):
        self.resolved_paths.append(folder_path)
        if folder_path not in self.folder_paths:
            raise GraphApiError("404")
        return self.folder_paths[folder_path]

    def get_thumbnails(self, drive_item_id: str):
        self.thumbnail_calls.append(drive_item_id)
        return {"value": [{"large": {"url": "https://thumb"}}]}

    def get_bytes(self, url: str):
        return b"image-bytes"


class FakeCaptioner:
    def __init__(self, model: str = "vision-model"):
        self.model = model
        self.calls = 0

    def generate_caption(self, image_bytes: bytes):
        self.calls += 1
        return CaptionResult(short_description="A short photo description.", model=self.model)


def make_settings() -> Settings:
    return Settings(
        onedrive_tenant="consumers",
        onedrive_client_id="client-id",
        onedrive_scopes=["User.Read", "Files.Read.All", "offline_access"],
        onedrive_camera_upload_path="/Pictures/Camera Roll",
        onedrive_camera_upload_fallback_paths=["/Pictures/CameraRoll", "/Pictures/Camera Uploads"],
        mysql_dsn="mysql://user:password@localhost:3306/db",
        openai_api_key=None,
        openai_vision_model="vision-model",
        photo_sync_initial_cutoff_days=14,
        photo_caption_max_words=18,
    )


def build_service(
    *,
    graph_client: FakeGraphClient,
    state: Optional[Dict[str, Any]],
    image_repo: Optional[FakeImageRepository] = None,
    captioner: Optional[FakeCaptioner] = None,
):
    now = datetime(2026, 2, 23, 12, 0, 0)
    image_repo = image_repo or FakeImageRepository()
    sync_state_repo = FakeSyncStateRepository(state)
    token_repo = FakeTokenCacheRepository()
    migration_runner = FakeMigrationRunner()

    service = PhotoSyncService(
        settings=make_settings(),
        database=FakeDatabase(),
        migration_runner=migration_runner,
        auth_service=FakeAuthService(),
        image_repo=image_repo,
        sync_state_repo=sync_state_repo,
        token_cache_repo=token_repo,
        graph_client_factory=lambda _token: graph_client,
        captioner=captioner,
        now_fn=lambda: now,
    )

    return service, sync_state_repo, image_repo, graph_client


def test_delta_pagination_updates_delta_only_after_full_run():
    pages = {
        "/me/drive/items/folder-1/delta": {
            "value": [
                {
                    "id": "item-1",
                    "name": "IMG_0001.JPG",
                    "photo": {"takenDateTime": "2026-02-22T12:00:00Z"},
                    "file": {"mimeType": "image/jpeg"},
                    "location": {"geoCoordinates": {"latitude": 40.7, "longitude": -73.9}},
                }
            ],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/next-page",
        },
        "https://graph.microsoft.com/v1.0/next-page": {
            "value": [],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta-token",
        },
    }
    service, state_repo, image_repo, graph_client = build_service(
        graph_client=FakeGraphClient(pages),
        state={
            "Id": 1,
            "FolderDriveItemId": "folder-1",
            "FolderPath": "/Pictures/Camera Roll",
            "DeltaLink": None,
            "LastRunAtUtc": None,
            "LastSuccessAtUtc": None,
            "LastError": None,
            "UpdatedAtUtc": None,
        },
    )

    result = service.run_sync()

    assert result.final_delta_link == "https://graph.microsoft.com/v1.0/delta-token"
    assert len(image_repo.upserts) == 1
    assert state_repo.upsert_calls[-1]["DeltaLink"] == "https://graph.microsoft.com/v1.0/delta-token"
    assert graph_client.requested_pages == [
        "/me/drive/items/folder-1/delta",
        "https://graph.microsoft.com/v1.0/next-page",
    ]


def test_deleted_item_marks_asset_deleted():
    pages = {
        "https://graph.microsoft.com/v1.0/start-delta": {
            "value": [{"id": "item-2", "deleted": {"state": "deleted"}}],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/new-delta",
        }
    }
    service, _, image_repo, _ = build_service(
        graph_client=FakeGraphClient(pages),
        state={
            "Id": 1,
            "FolderDriveItemId": "folder-1",
            "FolderPath": "/Pictures/Camera Roll",
            "DeltaLink": "https://graph.microsoft.com/v1.0/start-delta",
            "LastRunAtUtc": None,
            "LastSuccessAtUtc": None,
            "LastError": None,
            "UpdatedAtUtc": None,
        },
    )

    result = service.run_sync()

    assert result.deleted_count == 1
    assert len(image_repo.deleted) == 1
    assert image_repo.deleted[0][1] == "item-2"


def test_upsert_sql_uses_pascal_case_identifiers():
    sql = ImageAssetRepository.UPSERT_SQL
    assert "`ImageAsset`" in sql
    assert "`DriveItemId`" in sql
    assert "`ShortDescription`" not in sql  # Caption is updated separately.
    assert "drive_item_id" not in sql
    assert "short_description" not in sql


@pytest.mark.parametrize(
    "captioner_present,existing_row,expected_caption_calls",
    [
        (False, None, 0),
        (True, None, 1),
        (
            True,
            {
                "Source": "OneDrive",
                "DriveItemId": "item-3",
                "ShortDescription": "Already captioned.",
                "ShortDescriptionModel": "vision-model",
                "ShortDescriptionUpdatedAtUtc": datetime(2026, 2, 20, 0, 0, 0),
            },
            0,
        ),
        (
            True,
            {
                "Source": "OneDrive",
                "DriveItemId": "item-3",
                "ShortDescription": "Old model caption.",
                "ShortDescriptionModel": "old-model",
                "ShortDescriptionUpdatedAtUtc": datetime(2026, 2, 20, 0, 0, 0),
            },
            1,
        ),
    ],
)
def test_captioning_only_when_enabled_and_missing_or_stale(
    captioner_present,
    existing_row,
    expected_caption_calls,
):
    pages = {
        "https://graph.microsoft.com/v1.0/delta-start": {
            "value": [
                {
                    "id": "item-3",
                    "name": "IMG_0003.JPG",
                    "photo": {"takenDateTime": "2026-02-22T14:00:00Z"},
                    "file": {"mimeType": "image/jpeg"},
                }
            ],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta-new",
        }
    }

    image_repo = FakeImageRepository()
    if existing_row:
        image_repo.rows["item-3"] = existing_row.copy()

    captioner = FakeCaptioner() if captioner_present else None
    service, _, _, graph_client = build_service(
        graph_client=FakeGraphClient(pages),
        state={
            "Id": 1,
            "FolderDriveItemId": "folder-1",
            "FolderPath": "/Pictures/Camera Roll",
            "DeltaLink": "https://graph.microsoft.com/v1.0/delta-start",
            "LastRunAtUtc": None,
            "LastSuccessAtUtc": None,
            "LastError": None,
            "UpdatedAtUtc": None,
        },
        image_repo=image_repo,
        captioner=captioner,
    )

    service.run_sync()

    if captioner is None:
        assert expected_caption_calls == 0
    else:
        assert captioner.calls == expected_caption_calls

    assert len(graph_client.thumbnail_calls) == expected_caption_calls


def test_folder_discovery_uses_fallback_paths():
    pages = {
        "/me/drive/items/folder-from-fallback/delta": {
            "value": [],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/new-delta",
        }
    }
    graph_client = FakeGraphClient(
        pages,
        folder_paths={
            "/Pictures/Camera Uploads": {"id": "folder-from-fallback"},
        },
    )

    service, state_repo, _, _ = build_service(
        graph_client=graph_client,
        state=None,
    )

    service.run_sync()

    assert graph_client.resolved_paths == [
        "/Pictures/Camera Roll",
        "/Pictures/CameraRoll",
        "/Pictures/Camera Uploads",
    ]
    assert state_repo.upsert_calls[-1]["FolderDriveItemId"] == "folder-from-fallback"
    assert state_repo.upsert_calls[-1]["FolderPath"] == "/Pictures/Camera Uploads"
