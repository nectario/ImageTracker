from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    ParamValidationError,
    PartialCredentialsError,
)

from services.enrichment.models import (
    AMAZON_LOCATION_PROVIDER,
    GeocodeProviderError,
    GeocodeResolution,
    ProviderFailure,
    ProviderFailureClass,
    ReverseGeocodeResult,
)


def _failure(
    failure_class: ProviderFailureClass,
    code: str,
    message: str,
    *,
    retryable: bool,
) -> GeocodeProviderError:
    return GeocodeProviderError(
        ProviderFailure(
            failure_class=failure_class,
            code=code,
            user_message=message,
            retryable=retryable,
        )
    )


class AmazonLocationReverseGeocoder:
    """Resolve the closest address with Amazon Location Places V2.

    ``IntendedUse=Storage`` is deliberate: ImageTracker persists normalized
    address fields in MySQL for search and intelligence-layer queries. The
    provider is authenticated by the Lambda execution role, so no geocoding
    API key is copied into application configuration.
    """

    provider = AMAZON_LOCATION_PROVIDER

    def __init__(self, client: Any) -> None:
        if client is None:
            raise ValueError("An Amazon Location Places client is required")
        self._client = client

    def reverse_geocode(
        self, latitude: float, longitude: float
    ) -> ReverseGeocodeResult:
        self._validate_coordinates(latitude, longitude)
        try:
            response = self._client.reverse_geocode(
                QueryPosition=[float(longitude), float(latitude)],
                MaxResults=1,
                Filter={
                    "IncludePlaceTypes": [
                        "PointAddress",
                        "InterpolatedAddress",
                        "SecondaryAddress",
                        "Street",
                    ]
                },
                Language="en",
                AdditionalFeatures=["TimeZone"],
                IntendedUse="Storage",
            )
        except (NoCredentialsError, PartialCredentialsError):
            raise _failure(
                ProviderFailureClass.AUTHENTICATION,
                "AmazonLocationAuthenticationFailed",
                "Location enrichment is unavailable because AWS credentials could not be resolved.",
                retryable=False,
            ) from None
        except ParamValidationError:
            raise _failure(
                ProviderFailureClass.INTERNAL,
                "AmazonLocationInvalidRequest",
                "Location enrichment could not submit a valid provider request.",
                retryable=False,
            ) from None
        except ClientError as exc:
            raise self._client_failure(exc) from None
        except BotoCoreError:
            raise _failure(
                ProviderFailureClass.TRANSIENT,
                "AmazonLocationTransportError",
                "The location service is temporarily unavailable.",
                retryable=True,
            ) from None

        if not isinstance(response, Mapping):
            raise self._invalid_response()
        results = response.get("ResultItems")
        if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
            raise self._invalid_response()
        pricing_bucket = self._text(response.get("PricingBucket"), 64)
        if not results:
            raw = {
                "PricingBucket": pricing_bucket,
                "ProviderStatus": "ZERO_RESULTS",
                "ResultCount": 0,
            }
            return ReverseGeocodeResult(
                provider=self.provider,
                provider_status="ZERO_RESULTS",
                resolution=None,
                raw_provider_json=raw,
            )

        best = results[0]
        if not isinstance(best, Mapping):
            raise self._invalid_response()
        address_value = best.get("Address")
        if not isinstance(address_value, Mapping):
            raise self._invalid_response()
        address = address_value
        country = self._mapping(address.get("Country"))
        region = self._mapping(address.get("Region"))
        subregion = self._mapping(address.get("SubRegion"))

        street_number = self._text(address.get("AddressNumber"), 32)
        street = self._text(address.get("Street"), 480)
        label = self._text(address.get("Label"), 512)
        street_address = self._join(street_number, street, limit=512) or label
        city = self._text(address.get("Locality"), 255)
        state = self._text(region.get("Name"), 255) or self._text(
            region.get("Code"), 255
        )
        country_name = self._text(country.get("Name"), 255)
        neighborhood = self._text(address.get("District"), 255) or self._text(
            address.get("SubDistrict"), 255
        )
        county = self._text(subregion.get("Name"), 255)
        postal_code = self._text(address.get("PostalCode"), 50)
        country_code = self._text(country.get("Code2"), 8)
        title = self._text(best.get("Title"), 512)
        display_name = (
            title
            or label
            or self._join(city, state, country_name, separator=", ", limit=512)
            or f"{latitude:.6f},{longitude:.6f}"
        )
        place_id = self._text(best.get("PlaceId"), 500)
        raw = self._safe_provider_metadata(
            best=best,
            pricing_bucket=pricing_bucket,
            place_id=place_id,
        )
        raw = {
            **raw,
            "OriginalAddress": {
                key: value
                for key, value in {
                    "DisplayName": display_name,
                    "StreetAddress": street_address,
                    "StreetNumber": street_number,
                    "Neighborhood": neighborhood,
                    "City": city,
                    "County": county,
                    "State": state,
                    "PostalCode": postal_code,
                    "Country": country_name,
                    "CountryCode": country_code,
                }.items()
                if value is not None
            },
        }
        resolution = GeocodeResolution(
            location_display_name=display_name,
            street_address=street_address,
            original_street_number=street_number,
            neighborhood=neighborhood,
            city=city,
            county=county,
            state=state,
            postal_code=postal_code,
            country=country_name,
            country_code=country_code,
            provider=self.provider,
            provider_place_id=place_id,
            raw_provider_json=raw,
            time_zone_id=self._text(
                self._mapping(best.get("TimeZone")).get("Name"), 64
            ),
        )
        return ReverseGeocodeResult(
            provider=self.provider,
            provider_status="OK",
            resolution=resolution,
            raw_provider_json=raw,
        )

    @staticmethod
    def _validate_coordinates(latitude: float, longitude: float) -> None:
        if (
            isinstance(latitude, bool)
            or isinstance(longitude, bool)
            or not math.isfinite(latitude)
            or not math.isfinite(longitude)
            or not -90.0 <= latitude <= 90.0
            or not -180.0 <= longitude <= 180.0
        ):
            raise _failure(
                ProviderFailureClass.INVALID_MEDIA,
                "InvalidGpsCoordinates",
                "The media item contains invalid GPS coordinates.",
                retryable=False,
            )

    @staticmethod
    def _mapping(value: Any) -> Mapping[str, Any]:
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _text(value: Any, limit: int) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = " ".join(value.split()).strip()
        return cleaned[:limit] if cleaned else None

    @classmethod
    def _join(
        cls,
        *values: str | None,
        separator: str = " ",
        limit: int,
    ) -> str | None:
        selected = [value for value in values if value]
        return separator.join(selected)[:limit] if selected else None

    @classmethod
    def _safe_provider_metadata(
        cls,
        *,
        best: Mapping[str, Any],
        pricing_bucket: str | None,
        place_id: str | None,
    ) -> Mapping[str, Any]:
        raw: dict[str, Any] = {
            "PricingBucket": pricing_bucket,
            "ProviderStatus": "OK",
            "PlaceId": place_id,
            "PlaceType": cls._text(best.get("PlaceType"), 64),
        }
        time_zone = cls._mapping(best.get("TimeZone"))
        time_zone_name = cls._text(time_zone.get("Name"), 64)
        if time_zone_name:
            raw["TimeZone"] = {"Name": time_zone_name}
        distance = best.get("Distance")
        if isinstance(distance, (int, float)) and not isinstance(distance, bool):
            raw["DistanceMeters"] = distance
        position = best.get("Position")
        if (
            isinstance(position, Sequence)
            and not isinstance(position, (str, bytes))
            and len(position) == 2
            and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in position
            )
        ):
            raw["Position"] = [float(position[0]), float(position[1])]
        return {key: value for key, value in raw.items() if value is not None}

    @staticmethod
    def _client_failure(exc: ClientError) -> GeocodeProviderError:
        code = str((exc.response.get("Error") or {}).get("Code") or "")
        if code in {
            "AccessDeniedException",
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "InvalidSignatureException",
            "MissingAuthenticationTokenException",
            "UnrecognizedClientException",
        }:
            return _failure(
                ProviderFailureClass.AUTHENTICATION,
                "AmazonLocationAuthenticationFailed",
                "Location enrichment is unavailable because AWS rejected its credentials.",
                retryable=False,
            )
        if code in {"ServiceQuotaExceededException"}:
            return _failure(
                ProviderFailureClass.QUOTA,
                "AmazonLocationQuotaExceeded",
                "Location enrichment is waiting for provider quota.",
                retryable=False,
            )
        if code in {
            "ThrottlingException",
            "TooManyRequestsException",
            "InternalServerException",
            "ServiceUnavailableException",
            "RequestTimeout",
            "RequestTimeoutException",
        }:
            return _failure(
                ProviderFailureClass.TRANSIENT,
                "AmazonLocationServiceUnavailable",
                "The location service is temporarily unavailable.",
                retryable=True,
            )
        if code in {"ValidationException"}:
            return _failure(
                ProviderFailureClass.INTERNAL,
                "AmazonLocationInvalidRequest",
                "Location enrichment could not submit a valid provider request.",
                retryable=False,
            )
        return _failure(
            ProviderFailureClass.INTERNAL,
            "AmazonLocationRequestRejected",
            "Location enrichment could not be completed.",
            retryable=False,
        )

    @staticmethod
    def _invalid_response() -> GeocodeProviderError:
        return _failure(
            ProviderFailureClass.TRANSIENT,
            "AmazonLocationInvalidResponse",
            "The location service returned an invalid response.",
            retryable=True,
        )
