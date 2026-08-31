from __future__ import annotations

from decimal import Decimal
from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from services.enrichment.openai_scene import scene_description_maximum_cost_usd


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
    enrichment_processing_enabled: bool = False
    manifest_import_queue_url: str = ""
    manifest_import_max_entries: int = Field(default=250_000, ge=1, le=1_000_000)
    manifest_import_max_compressed_bytes: int = Field(
        default=268_435_456,
        ge=1_048_576,
        le=1_073_741_824,
    )
    manifest_import_max_uncompressed_bytes: int = Field(
        default=1_073_741_824,
        ge=1_048_576,
        le=2_147_483_648,
    )
    location_normalization_rules_path: str = "location_normalization_rules.json"
    geocode_reuse_radius_meters: float = Field(default=5.0, gt=0, le=100)
    geocode_monthly_call_limit: int = Field(default=1000, ge=0, le=10000)
    scene_description_model: str = "gpt-5.6-terra"
    scene_description_detail: str = Field(default="high", pattern=r"^(low|high)$")
    scene_description_service_tier: str = Field(
        default="flex", pattern=r"^(auto|default|flex)$"
    )
    scene_description_max_words: int = Field(default=24, ge=8, le=24)
    scene_description_monthly_call_limit: int = Field(
        default=100000, ge=0, le=100000
    )
    scene_description_monthly_usd_limit: Decimal = Field(
        default=Decimal("230.000000"), ge=0, le=Decimal("10000")
    )
    scene_description_reserved_usd_per_request: Decimal = Field(
        default=Decimal("0.010000"), gt=0, le=Decimal("100")
    )
    scene_description_input_usd_per_million: Decimal = Field(
        default=Decimal("2.000000"), ge=0, le=Decimal("1000")
    )
    scene_description_cached_input_usd_per_million: Decimal = Field(
        default=Decimal("0.200000"), ge=0, le=Decimal("1000")
    )
    scene_description_output_usd_per_million: Decimal = Field(
        default=Decimal("12.000000"), ge=0, le=Decimal("1000")
    )
    api_url: str = ""
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

    @field_validator("mysql_database")
    @classmethod
    def require_imagetracker_database(cls, value: str) -> str:
        if value != "ImageTracker":
            raise ValueError("The app service may connect only to the ImageTracker database")
        return value

    @model_validator(mode="after")
    def validate_scene_cost_guardrail(self) -> "AppSettings":
        if (
            self.scene_description_monthly_usd_limit > 0
            and self.scene_description_reserved_usd_per_request
            > self.scene_description_monthly_usd_limit
        ):
            raise ValueError(
                "The scene-description request reservation exceeds its monthly USD limit"
            )
        required_reservation = scene_description_maximum_cost_usd(
            input_usd_per_million=self.scene_description_input_usd_per_million,
            cached_input_usd_per_million=(
                self.scene_description_cached_input_usd_per_million
            ),
            output_usd_per_million=self.scene_description_output_usd_per_million,
        )
        if self.scene_description_reserved_usd_per_request < required_reservation:
            raise ValueError(
                "The scene-description USD reservation is below the maximum request cost"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()
