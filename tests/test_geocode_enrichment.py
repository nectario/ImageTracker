from __future__ import annotations

from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError, EndpointConnectionError
import pytest

from services.enrichment.aws_location import AmazonLocationReverseGeocoder
from services.enrichment.models import (
    AMAZON_LOCATION_PROVIDER,
    GeocodeProviderError,
    GeocodeResolution,
    ProviderFailureClass,
)
from services.enrichment.normalization import (
    LocationNormalizer,
    load_location_normalization_rules,
)


def amazon_location_payload(*, street_number: str = "101") -> dict[str, Any]:
    label = f"{street_number} Prospect Ave, Bayonne, NJ 07002, United States"
    return {
        "PricingBucket": "Stored",
        "ResultItems": [
            {
                "PlaceId": "first-place",
                "PlaceType": "PointAddress",
                "Title": label,
                "Address": {
                    "Label": label,
                    "Country": {
                        "Code2": "US",
                        "Code3": "USA",
                        "Name": "United States",
                    },
                    "Region": {"Code": "NJ", "Name": "New Jersey"},
                    "SubRegion": {"Name": "Hudson County"},
                    "Locality": "Bayonne",
                    "District": "Constable Hook",
                    "PostalCode": "07002",
                    "Street": "Prospect Avenue",
                    "AddressNumber": street_number,
                },
                "Position": [-74.1143, 40.6684],
                "Distance": 0,
                "TimeZone": {
                    "Name": "America/New_York",
                    "Offset": "-04:00",
                    "OffsetSeconds": -14400,
                },
            },
            {
                "PlaceId": "second-place",
                "PlaceType": "Street",
                "Title": "This result must not win",
                "Address": {"Label": "This result must not win"},
            },
        ],
    }


class FakePlacesClient:
    def __init__(self, outcome: dict[str, Any] | Exception) -> None:
        self.outcome = outcome
        self.calls: list[dict[str, Any]] = []

    def reverse_geocode(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def test_amazon_location_uses_storage_mode_and_first_address_result() -> None:
    client = FakePlacesClient(amazon_location_payload())
    geocoder = AmazonLocationReverseGeocoder(client)

    result = geocoder.reverse_geocode(40.6684, -74.1143)

    assert result.provider == AMAZON_LOCATION_PROVIDER
    assert result.provider_status == "OK"
    assert result.resolution is not None
    location = result.resolution
    assert location.location_display_name == (
        "101 Prospect Ave, Bayonne, NJ 07002, United States"
    )
    assert location.street_address == "101 Prospect Avenue"
    assert location.original_street_number == "101"
    assert location.neighborhood == "Constable Hook"
    assert location.city == "Bayonne"
    assert location.county == "Hudson County"
    assert location.state == "New Jersey"
    assert location.postal_code == "07002"
    assert location.country == "United States"
    assert location.country_code == "US"
    assert location.provider_place_id == "first-place"
    assert location.time_zone_id == "America/New_York"
    assert result.raw_provider_json == {
        "PricingBucket": "Stored",
        "ProviderStatus": "OK",
        "PlaceId": "first-place",
        "PlaceType": "PointAddress",
        "TimeZone": {"Name": "America/New_York"},
        "DistanceMeters": 0,
        "Position": [-74.1143, 40.6684],
        "OriginalAddress": {
            "DisplayName": "101 Prospect Ave, Bayonne, NJ 07002, United States",
            "StreetAddress": "101 Prospect Avenue",
            "StreetNumber": "101",
            "Neighborhood": "Constable Hook",
            "City": "Bayonne",
            "County": "Hudson County",
            "State": "New Jersey",
            "PostalCode": "07002",
            "Country": "United States",
            "CountryCode": "US",
        },
    }
    assert client.calls == [
        {
            "QueryPosition": [-74.1143, 40.6684],
            "MaxResults": 1,
            "Filter": {
                "IncludePlaceTypes": [
                    "PointAddress",
                    "InterpolatedAddress",
                    "SecondaryAddress",
                    "Street",
                ]
            },
            "Language": "en",
            "AdditionalFeatures": ["TimeZone"],
            "IntendedUse": "Storage",
        }
    ]


def test_amazon_location_zero_results_is_a_completed_lookup() -> None:
    client = FakePlacesClient({"PricingBucket": "Stored", "ResultItems": []})

    result = AmazonLocationReverseGeocoder(client).reverse_geocode(0.0, 0.0)

    assert result.provider_status == "ZERO_RESULTS"
    assert result.resolution is None
    assert result.raw_provider_json == {
        "PricingBucket": "Stored",
        "ProviderStatus": "ZERO_RESULTS",
        "ResultCount": 0,
    }


@pytest.mark.parametrize(
    ("provider_code", "failure_class", "failure_code", "retryable"),
    [
        (
            "ThrottlingException",
            ProviderFailureClass.TRANSIENT,
            "AmazonLocationServiceUnavailable",
            True,
        ),
        (
            "AccessDeniedException",
            ProviderFailureClass.AUTHENTICATION,
            "AmazonLocationAuthenticationFailed",
            False,
        ),
        (
            "ServiceQuotaExceededException",
            ProviderFailureClass.QUOTA,
            "AmazonLocationQuotaExceeded",
            False,
        ),
        (
            "ValidationException",
            ProviderFailureClass.INTERNAL,
            "AmazonLocationInvalidRequest",
            False,
        ),
        (
            "UnknownProviderError",
            ProviderFailureClass.INTERNAL,
            "AmazonLocationRequestRejected",
            False,
        ),
    ],
)
def test_amazon_location_client_errors_are_safely_classified(
    provider_code: str,
    failure_class: ProviderFailureClass,
    failure_code: str,
    retryable: bool,
) -> None:
    sensitive_message = "credential-and-coordinate-must-not-leak"
    error = ClientError(
        {
            "Error": {"Code": provider_code, "Message": sensitive_message},
            "ResponseMetadata": {"HTTPStatusCode": 500},
        },
        "ReverseGeocode",
    )
    geocoder = AmazonLocationReverseGeocoder(FakePlacesClient(error))

    with pytest.raises(GeocodeProviderError) as raised:
        geocoder.reverse_geocode(1.0, 2.0)

    assert raised.value.failure.failure_class is failure_class
    assert raised.value.failure.code == failure_code
    assert raised.value.failure.retryable is retryable
    assert sensitive_message not in str(raised.value)


def test_amazon_location_transport_error_is_retryable_and_sanitized() -> None:
    secret_url = "https://secret-provider.invalid/path?key=sensitive"
    geocoder = AmazonLocationReverseGeocoder(
        FakePlacesClient(EndpointConnectionError(endpoint_url=secret_url))
    )

    with pytest.raises(GeocodeProviderError) as raised:
        geocoder.reverse_geocode(1.0, 2.0)

    assert raised.value.failure.failure_class is ProviderFailureClass.TRANSIENT
    assert raised.value.failure.retryable is True
    assert secret_url not in str(raised.value)


def test_invalid_coordinates_never_call_amazon_location() -> None:
    client = FakePlacesClient(amazon_location_payload())
    geocoder = AmazonLocationReverseGeocoder(client)

    with pytest.raises(GeocodeProviderError) as raised:
        geocoder.reverse_geocode(91.0, 2.0)

    assert raised.value.failure.failure_class is ProviderFailureClass.INVALID_MEDIA
    assert raised.value.failure.retryable is False
    assert client.calls == []


def test_invalid_amazon_location_response_is_retryable() -> None:
    geocoder = AmazonLocationReverseGeocoder(
        FakePlacesClient({"PricingBucket": "Stored", "ResultItems": [{}]})
    )

    with pytest.raises(GeocodeProviderError) as raised:
        geocoder.reverse_geocode(1.0, 2.0)

    assert raised.value.failure.code == "AmazonLocationInvalidResponse"
    assert raised.value.failure.retryable is True


def test_normalization_uses_current_rules_and_preserves_raw_street_number() -> None:
    rules = load_location_normalization_rules(
        Path(__file__).resolve().parents[1] / "location_normalization_rules.json"
    )
    raw = {
        "PricingBucket": "Stored",
        "PlaceId": "first-place",
        "PlaceType": "PointAddress",
    }
    location = GeocodeResolution(
        location_display_name=(
            "101 Prospect Ave, Bayonne, NJ 07002, United States"
        ),
        street_address="101 Prospect Avenue",
        original_street_number="101",
        neighborhood=None,
        city="Bayonne",
        county="Hudson County",
        state="New Jersey",
        postal_code="07002",
        country="United States",
        country_code="US",
        provider=AMAZON_LOCATION_PROVIDER,
        provider_place_id="first-place",
        raw_provider_json=raw,
    )

    normalized = LocationNormalizer(rules).normalize(location)

    assert normalized.street_address == "99 Prospect Avenue"
    assert normalized.original_street_number == "101"
    assert normalized.raw_provider_json is raw
    assert normalized.normalization_rule_version is not None
    assert normalized.normalization_rule_version.startswith(
        "BayonneProspectCanonical99@sha256:"
    )
    assert len(normalized.normalization_rule_version) <= 64
