from __future__ import annotations

import pytest

from imagetracker.config import Settings
from imagetracker.onedrive_auth import OneDriveAuthService


def make_settings(scopes: list[str]) -> Settings:
    return Settings(
        onedrive_tenant="consumers",
        onedrive_client_id="client-id",
        onedrive_scopes=scopes,
        onedrive_camera_upload_path="/Pictures/Camera Roll",
        onedrive_camera_upload_fallback_paths=[],
        mysql_dsn="",
        openai_api_key=None,
        openai_vision_model="gpt-5.2",
        photo_sync_initial_cutoff_days=14,
        photo_caption_max_words=18,
    )


def test_effective_scopes_filters_reserved_and_deduplicates():
    settings = make_settings(
        ["User.Read", "offline_access", "Files.Read.All", "openid", "user.read", "profile"]
    )
    service = OneDriveAuthService(settings)

    scopes = service._effective_scopes()

    assert scopes == ["User.Read", "Files.Read.All"]


def test_effective_scopes_requires_non_reserved_scope():
    settings = make_settings(["offline_access", "openid", "profile"])
    service = OneDriveAuthService(settings)

    with pytest.raises(RuntimeError):
        service._effective_scopes()
