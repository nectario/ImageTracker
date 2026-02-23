from __future__ import annotations

from imagetracker.config import Settings
from imagetracker.db import parse_mysql_config


def make_settings() -> Settings:
    return Settings(
        onedrive_tenant="consumers",
        onedrive_client_id="client-id",
        onedrive_scopes=["User.Read", "Files.Read.All", "offline_access"],
        onedrive_camera_upload_path="/Pictures/Camera Roll",
        onedrive_camera_upload_fallback_paths=[],
        mysql_dsn="",
        openai_api_key=None,
        openai_vision_model="gpt-4.1-mini",
        photo_sync_initial_cutoff_days=14,
        photo_caption_max_words=18,
    )


def test_parse_mysql_config_uses_project_scoped_env_first(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "generic-host")
    monkeypatch.setenv("MYSQL_PORT", "3306")
    monkeypatch.setenv("MYSQL_USER", "generic-user")
    monkeypatch.setenv("MYSQL_PASSWORD", "generic-password")
    monkeypatch.setenv("MYSQL_DATABASE", "generic-db")

    monkeypatch.setenv("IMAGETRACKER_MYSQL_HOST", "scoped-host")
    monkeypatch.setenv("IMAGETRACKER_MYSQL_PORT", "3310")
    monkeypatch.setenv("IMAGETRACKER_MYSQL_USER", "scoped-user")
    monkeypatch.setenv("IMAGETRACKER_MYSQL_PASSWORD", "scoped-password")
    monkeypatch.setenv("IMAGETRACKER_MYSQL_DATABASE", "scoped-db")

    cfg = parse_mysql_config(make_settings())

    assert cfg.host == "scoped-host"
    assert cfg.port == 3310
    assert cfg.user == "scoped-user"
    assert cfg.password == "scoped-password"
    assert cfg.database == "scoped-db"


def test_parse_mysql_config_supports_mysql_userid_and_mysql_database_imagetracker(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "my-host")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("MYSQL_USER", "generic-user")
    monkeypatch.setenv("MYSQL_DATABASE", "generic-db")
    monkeypatch.setenv("MYSQL_PASSWORD", "shared-password")

    monkeypatch.setenv("MYSQL_USERID", "imagetracker-user")
    monkeypatch.setenv("MYSQL_DATABASE_IMAGETRACKER", "imagetracker-db")

    cfg = parse_mysql_config(make_settings())

    assert cfg.host == "my-host"
    assert cfg.port == 3307
    assert cfg.user == "imagetracker-user"
    assert cfg.password == "shared-password"
    assert cfg.database == "imagetracker-db"
