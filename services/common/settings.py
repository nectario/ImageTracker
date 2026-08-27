from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Non-secret service configuration.

    Provider and database credentials are intentionally represented by SSM
    parameter names. Lambda code resolves those parameters at runtime instead
    of loading a repository or deployment-bundled .env file.
    """

    model_config = SettingsConfigDict(
        env_prefix="IMAGETRACKER_",
        extra="ignore",
        case_sensitive=False,
    )

    service_name: str = "imagetracker-api"
    service_version: str = "0.2.0"
    stage: str = "local"
    aws_region: str = "us-east-2"
    mysql_database: str = "ImageTracker"
    media_bucket: str = ""
    db_secret_parameter: str = "/imagetracker/prod/mysql"
    openai_secret_parameter: str = "/imagetracker/prod/openai"
    google_secret_parameter: str = "/imagetracker/prod/google"
    elevenlabs_secret_parameter: str = "/imagetracker/prod/elevenlabs"
    api_url: str = ""
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

    @field_validator("mysql_database")
    @classmethod
    def require_imagetracker_database(cls, value: str) -> str:
        if value != "ImageTracker":
            raise ValueError("The app service may connect only to the ImageTracker database")
        return value


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()
