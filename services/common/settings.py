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
    service_version: str = "0.3.0"
    stage: str = "local"
    aws_region: str = "us-east-2"
    mysql_database: str = "ImageTracker"
    media_bucket: str = ""
    db_secret_parameter: str = "/imagetracker/prod/mysql"
    openai_secret_parameter: str = "/imagetracker/prod/openai"
    elevenlabs_secret_parameter: str = "/imagetracker/prod/elevenlabs"
    processing_queue_url: str = ""
    location_normalization_rules_path: str = "location_normalization_rules.json"
    geocode_reuse_radius_meters: float = Field(default=5.0, gt=0, le=100)
    geocode_monthly_call_limit: int = Field(default=1000, ge=0, le=10000)
    scene_description_model: str = "gpt-5.6-sol"
    scene_description_detail: str = Field(default="high", pattern=r"^(low|high)$")
    scene_description_service_tier: str = Field(
        default="flex", pattern=r"^(auto|default|flex)$"
    )
    scene_description_max_words: int = Field(default=24, ge=8, le=24)
    scene_description_monthly_call_limit: int = Field(
        default=1000, ge=0, le=100000
    )
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
