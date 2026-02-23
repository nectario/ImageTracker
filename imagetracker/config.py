from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class Settings:
    onedrive_tenant: str
    onedrive_client_id: str
    onedrive_scopes: List[str]
    onedrive_camera_upload_path: str
    onedrive_camera_upload_fallback_paths: List[str]
    mysql_dsn: str
    openai_api_key: Optional[str]
    openai_vision_model: str
    photo_sync_initial_cutoff_days: int
    photo_caption_max_words: int


def _parse_dotenv_line(line: str) -> Optional[tuple[str, str]]:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()

    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1]

    return key, value


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(raw_line)
        if not parsed:
            continue
        key, value = parsed
        os.environ.setdefault(key, value)


def _split_space_list(value: str) -> List[str]:
    return [part for part in value.split() if part]


def _split_comma_list(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def load_settings() -> Settings:
    load_dotenv()

    scopes = _split_space_list(
        os.getenv("ONEDRIVE_SCOPES", "User.Read Files.Read.All offline_access")
    )

    primary_camera_path = os.getenv("ONEDRIVE_CAMERA_UPLOAD_PATH", "/Pictures/Camera Roll")
    fallback_paths = _split_comma_list(
        os.getenv(
            "ONEDRIVE_CAMERA_UPLOAD_FALLBACK_PATHS",
            "/Pictures/CameraRoll,/Pictures/Camera Uploads,/Pictures/OneDrive Camera Roll",
        )
    )
    fallback_paths = [path for path in fallback_paths if path != primary_camera_path]

    return Settings(
        onedrive_tenant=os.getenv("ONEDRIVE_TENANT", "consumers"),
        onedrive_client_id=os.getenv("ONEDRIVE_CLIENT_ID", ""),
        onedrive_scopes=scopes,
        onedrive_camera_upload_path=primary_camera_path,
        onedrive_camera_upload_fallback_paths=fallback_paths,
        mysql_dsn=os.getenv("MYSQL_DSN", ""),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_vision_model=os.getenv("OPENAI_VISION_MODEL", "gpt-4.1-mini"),
        photo_sync_initial_cutoff_days=int(os.getenv("PHOTO_SYNC_INITIAL_CUTOFF_DAYS", "14")),
        photo_caption_max_words=int(os.getenv("PHOTO_CAPTION_MAX_WORDS", "18")),
    )
