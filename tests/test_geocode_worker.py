from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from uuid import UUID

from services.enrichment.models import (
    AMAZON_LOCATION_PROVIDER,
    GeocodeProviderError,
    GeocodeResolution,
    ProviderFailure,
    ProviderFailureClass,
    ReverseGeocodeResult,
)
from services.enrichment.normalization import (
    LocationNormalizationRuleset,
    LocationNormalizer,
)
from services.worker.contracts import (
    GeocodeJob,
    GeocodeJobFailure,
    MessageDisposition,
)
from services.worker.handler import handler
from services.worker.processor import GeocodeMessageProcessor, InvalidGeocodeMessage


JOB_ID = UUID("00000000-0000-0000-0000-000000000123")


def resolved_result(street_number: str = "101") -> ReverseGeocodeResult:
    raw = {"status": "OK", "results": [{"place_id": "place-1"}]}
    return ReverseGeocodeResult(
        provider=AMAZON_LOCATION_PROVIDER,
        provider_status="OK",
        resolution=GeocodeResolution(
            location_display_name="Bayonne, New Jersey, United States",
            street_address=f"{street_number} Prospect Avenue",
            original_street_number=street_number,
            neighborhood=None,
            city="Bayonne",
            county="Hudson County",
            state="New Jersey",
            postal_code="07002",
            country="United States",
            country_code="US",
            provider=AMAZON_LOCATION_PROVIDER,
            provider_place_id="place-1",
            raw_provider_json=raw,
        ),
        raw_provider_json=raw,
    )


@dataclass
class FakeGeocoder:
    outcome: ReverseGeocodeResult | GeocodeProviderError
    provider: str = AMAZON_LOCATION_PROVIDER

    def __post_init__(self) -> None:
        self.calls: list[tuple[float, float]] = []

    def reverse_geocode(self, latitude: float, longitude: float):
        self.calls.append((latitude, longitude))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class FakeRepository:
    def __init__(self) -> None:
        self.job = GeocodeJob(
            job_id=JOB_ID,
            user_id=10,
            media_asset_id=20,
            latitude=40.6684,
            longitude=-74.1143,
            coordinate_revision="coordinate-revision",
            lease_owner="message-1",
            attempt_count=1,
            max_attempts=5,
        )
        self.reusable: ReverseGeocodeResult | None = None
        self.reservation_allowed = True
        self.retry_failure = False
        self.calls: list[tuple[str, Any]] = []

    def claim_geocode_job(self, *, job_id: UUID, message_id: str):
        self.calls.append(("claim", (job_id, message_id)))
        return self.job

    def find_reusable_location(self, *, job: GeocodeJob, radius_meters: float):
        self.calls.append(("reuse", (job, radius_meters)))
        return self.reusable

    def reserve_provider_call(
        self, *, job: GeocodeJob, provider: str, monthly_limit: int
    ):
        self.calls.append(("reserve", (job, provider, monthly_limit)))
        return self.reservation_allowed

    def consume_provider_call(self, *, job: GeocodeJob, provider: str):
        self.calls.append(("consume", (job, provider)))
        return True

    def complete_geocode(
        self, *, job: GeocodeJob, result: ReverseGeocodeResult, reused: bool
    ):
        self.calls.append(("complete", (job, result, reused)))

    def fail_geocode(self, *, job: GeocodeJob, failure: GeocodeJobFailure):
        self.calls.append(("fail", (job, failure)))
        return self.retry_failure

    def defer_geocode_quota(
        self,
        *,
        job: GeocodeJob,
        failure: GeocodeJobFailure,
        provider_called: bool,
    ):
        self.calls.append(("defer", (job, failure, provider_called)))


def processor(repository: FakeRepository, geocoder: FakeGeocoder):
    return GeocodeMessageProcessor(
        repository=repository,
        geocoder=geocoder,  # type: ignore[arg-type]
        normalizer=LocationNormalizer(
            LocationNormalizationRuleset(rules=(), version="none")
        ),
        reuse_radius_meters=15.0,
        monthly_call_limit=1000,
    )


def body() -> str:
    return json.dumps({"jobId": str(JOB_ID), "jobType": "Geocode"})


def test_reusable_location_skips_quota_reservation_and_provider() -> None:
    repository = FakeRepository()
    repository.reusable = resolved_result()
    geocoder = FakeGeocoder(resolved_result())

    disposition = processor(repository, geocoder).process_message(
        message_id="message-1", body=body()
    )

    assert disposition is MessageDisposition.ACK
    assert geocoder.calls == []
    assert [name for name, _ in repository.calls] == ["claim", "reuse", "complete"]
    assert repository.calls[-1][1][2] is True


def test_monthly_limit_defers_without_calling_provider() -> None:
    repository = FakeRepository()
    repository.reservation_allowed = False
    geocoder = FakeGeocoder(resolved_result())

    disposition = processor(repository, geocoder).process_message(
        message_id="message-1", body=body()
    )

    assert disposition is MessageDisposition.ACK
    assert geocoder.calls == []
    assert [name for name, _ in repository.calls] == ["claim", "reuse", "reserve", "defer"]
    failure = repository.calls[-1][1][1]
    assert failure.failure_class is ProviderFailureClass.QUOTA
    assert failure.code == "MonthlyGeocodeLimitReached"


def test_provider_success_completes_job_and_zero_results_also_succeeds() -> None:
    for provider_result in (
        resolved_result(),
        ReverseGeocodeResult(
            provider=AMAZON_LOCATION_PROVIDER,
            provider_status="ZERO_RESULTS",
            resolution=None,
            raw_provider_json={"status": "ZERO_RESULTS", "results": []},
        ),
    ):
        repository = FakeRepository()
        geocoder = FakeGeocoder(provider_result)

        disposition = processor(repository, geocoder).process_message(
            message_id="message-1", body=body()
        )

        assert disposition is MessageDisposition.ACK
        assert geocoder.calls == [(40.6684, -74.1143)]
        assert [name for name, _ in repository.calls] == [
            "claim",
            "reuse",
            "reserve",
            "consume",
            "complete",
        ]
        assert repository.calls[-1][1][1].provider_status == provider_result.provider_status
        assert repository.calls[-1][1][2] is False


def provider_error(
    failure_class: ProviderFailureClass, *, retryable: bool
) -> GeocodeProviderError:
    return GeocodeProviderError(
        ProviderFailure(
            failure_class=failure_class,
            code=f"{failure_class.value}Code",
            user_message="Safe user message",
            retryable=retryable,
        )
    )


def test_transient_provider_failure_requests_partial_batch_retry() -> None:
    repository = FakeRepository()
    repository.retry_failure = True
    geocoder = FakeGeocoder(
        provider_error(ProviderFailureClass.TRANSIENT, retryable=True)
    )

    disposition = processor(repository, geocoder).process_message(
        message_id="message-1", body=body()
    )

    assert disposition is MessageDisposition.RETRY
    assert repository.calls[-1][0] == "fail"


def test_authentication_failure_is_terminal_and_quota_failure_is_deferred() -> None:
    authentication_repo = FakeRepository()
    authentication_repo.retry_failure = True
    authentication = processor(
        authentication_repo,
        FakeGeocoder(
            provider_error(ProviderFailureClass.AUTHENTICATION, retryable=False)
        ),
    ).process_message(message_id="auth", body=body())
    assert authentication is MessageDisposition.ACK
    assert authentication_repo.calls[-1][0] == "fail"

    quota_repo = FakeRepository()
    quota = processor(
        quota_repo,
        FakeGeocoder(provider_error(ProviderFailureClass.QUOTA, retryable=False)),
    ).process_message(message_id="quota", body=body())
    assert quota is MessageDisposition.ACK
    assert quota_repo.calls[-1][0] == "defer"


def test_stale_duplicate_job_is_acknowledged_without_more_work() -> None:
    repository = FakeRepository()
    repository.job = None  # type: ignore[assignment]
    geocoder = FakeGeocoder(resolved_result())

    disposition = processor(repository, geocoder).process_message(
        message_id="duplicate", body=body()
    )

    assert disposition is MessageDisposition.ACK
    assert [name for name, _ in repository.calls] == ["claim"]
    assert geocoder.calls == []


class BatchProcessor:
    def process_message(self, *, message_id: str, body: Any) -> MessageDisposition:
        if body == "malformed":
            raise InvalidGeocodeMessage("bad")
        if body == "crash-with-secret-in-message":
            raise RuntimeError("secret-key-must-not-be-logged")
        return MessageDisposition.RETRY if body == "retry" else MessageDisposition.ACK


def test_lambda_handler_reports_only_retryable_batch_items(caplog) -> None:
    event = {
        "Records": [
            {"messageId": "ack", "body": "ack"},
            {"messageId": "retry", "body": "retry"},
            {"messageId": "bad", "body": "malformed"},
            {"messageId": "crash", "body": "crash-with-secret-in-message"},
        ]
    }

    result = handler(event, None, processor=BatchProcessor())  # type: ignore[arg-type]

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "retry"},
            {"itemIdentifier": "crash"},
        ]
    }
    assert "secret-key-must-not-be-logged" not in caplog.text


def test_lambda_handler_accepts_an_empty_batch_without_composition() -> None:
    assert handler({"Records": []}, None) == {"batchItemFailures": []}


def test_invalid_worker_message_is_permanent() -> None:
    repository = FakeRepository()
    geocoder = FakeGeocoder(resolved_result())
    worker = processor(repository, geocoder)

    for invalid in (
        "not-json",
        json.dumps([]),
        json.dumps({"jobId": str(JOB_ID), "jobType": "Description"}),
        json.dumps({"jobId": "not-a-uuid", "jobType": "Geocode"}),
    ):
        try:
            worker.process_message(message_id="invalid", body=invalid)
        except InvalidGeocodeMessage:
            pass
        else:  # pragma: no cover - assertion helper
            raise AssertionError(f"Expected InvalidGeocodeMessage for {invalid!r}")
    assert repository.calls == []
