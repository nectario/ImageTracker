from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from datetime import datetime
from typing import Any, Mapping, Protocol


AMAZON_LOCATION_PROVIDER = "AmazonLocationPlacesV2"


class ProviderFailureClass(str, Enum):
    """Failure classes persisted by ``ProcessingJob``."""

    TRANSIENT = "Transient"
    AUTHENTICATION = "Authentication"
    QUOTA = "Quota"
    INVALID_MEDIA = "InvalidMedia"
    INTERNAL = "Internal"


@dataclass(frozen=True)
class GeocodeResolution:
    """A provider result ready to be copied to ``MediaLocation``."""

    location_display_name: str
    street_address: str | None
    original_street_number: str | None
    neighborhood: str | None
    city: str | None
    county: str | None
    state: str | None
    postal_code: str | None
    country: str | None
    country_code: str | None
    provider: str
    provider_place_id: str | None
    raw_provider_json: Mapping[str, Any]
    normalization_rule_version: str | None = None
    time_zone_id: str | None = None

    def with_normalization(
        self,
        *,
        street_address: str | None,
        original_street_number: str | None,
        rule_version: str | None,
    ) -> "GeocodeResolution":
        return replace(
            self,
            street_address=street_address,
            original_street_number=original_street_number,
            normalization_rule_version=rule_version,
        )


@dataclass(frozen=True)
class ReverseGeocodeResult:
    """One completed provider lookup, including a valid no-result response."""

    provider: str
    provider_status: str
    resolution: GeocodeResolution | None
    raw_provider_json: Mapping[str, Any]
    provider_updated_at_utc: datetime | None = None

    @property
    def has_result(self) -> bool:
        return self.resolution is not None

    def with_resolution(self, resolution: GeocodeResolution) -> "ReverseGeocodeResult":
        return replace(self, resolution=resolution)


@dataclass(frozen=True)
class ProviderFailure:
    failure_class: ProviderFailureClass
    code: str
    user_message: str
    retryable: bool


class GeocodeProviderError(RuntimeError):
    """A sanitized provider failure.

    The original URL, response body, and API key are deliberately never stored
    on this exception so it is safe for the worker to classify and persist.
    """

    def __init__(self, failure: ProviderFailure) -> None:
        super().__init__(failure.user_message)
        self.failure = failure


class ReverseGeocoder(Protocol):
    """Provider-neutral reverse-geocoding capability used by the worker."""

    provider: str

    def reverse_geocode(
        self, latitude: float, longitude: float
    ) -> ReverseGeocodeResult: ...
