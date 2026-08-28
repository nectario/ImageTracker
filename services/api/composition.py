from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Any

import boto3

from services.api.domain_adapter import DomainServiceAdapter
from services.api.job_dispatcher import SqsJobDispatcher
from services.api.temporary_store import S3TemporaryObjectStore
from services.api.service import Phase1Service, ServiceUnavailableError, UnavailablePhase1Service
from services.common.settings import AppSettings
from services.enrichment.normalization import (
    LocationNormalizer,
    load_location_normalization_rules,
)


def _normalization_path(settings: AppSettings) -> Path:
    selected = Path(settings.location_normalization_rules_path)
    if selected.is_absolute():
        return selected
    return Path(__file__).resolve().parents[2] / selected


class LazyConfiguredPhase1Service:
    """Resolve SSM and create the tiny DB pool only when an API request needs it."""

    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._lock = Lock()
        self._adapter: DomainServiceAdapter | None = None
        self._runtime: Any | None = None

    def _get(self) -> DomainServiceAdapter:
        if self._adapter is not None:
            return self._adapter
        with self._lock:
            if self._adapter is None:
                try:
                    from services.data.database import build_database_runtime
                    from services.domain.service import Phase1DomainService

                    self._runtime = build_database_runtime(self._settings)
                    dispatcher = (
                        SqsJobDispatcher(
                            client=boto3.client(
                                "sqs", region_name=self._settings.aws_region
                            ),
                            queue_url=self._settings.processing_queue_url,
                        )
                        if self._settings.processing_queue_url
                        else None
                    )
                    temporary_store = (
                        S3TemporaryObjectStore(
                            client=boto3.client(
                                "s3", region_name=self._settings.aws_region
                            ),
                            bucket=self._settings.media_bucket,
                        )
                        if self._settings.media_bucket
                        else None
                    )
                    location_normalizer = LocationNormalizer(
                        load_location_normalization_rules(
                            _normalization_path(self._settings)
                        )
                    )
                    self._adapter = DomainServiceAdapter(
                        Phase1DomainService(
                            self._runtime.session_factory,
                            job_dispatcher=dispatcher,
                            temporary_object_store=temporary_store,
                            scene_description_monthly_call_limit=(
                                self._settings.scene_description_monthly_call_limit
                            ),
                            scene_description_model=(
                                self._settings.scene_description_model
                            ),
                            scene_description_detail=(
                                self._settings.scene_description_detail
                            ),
                            scene_description_service_tier=(
                                self._settings.scene_description_service_tier
                            ),
                            scene_description_max_words=(
                                self._settings.scene_description_max_words
                            ),
                            geocode_reuse_radius_meters=(
                                self._settings.geocode_reuse_radius_meters
                            ),
                            location_normalizer=location_normalizer,
                        )
                    )
                except Exception as exc:
                    raise ServiceUnavailableError(
                        "The ImageTracker data service is temporarily unavailable",
                        code="DATA_SERVICE_UNAVAILABLE",
                    ) from exc
        adapter = self._adapter
        if adapter is None:  # pragma: no cover - guarded by the lock above
            raise ServiceUnavailableError(
                "The ImageTracker data service is temporarily unavailable",
                code="DATA_SERVICE_UNAVAILABLE",
            )
        return adapter

    def __getattr__(self, name: str) -> Any:
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            adapter = self._get()
            return await getattr(adapter, name)(*args, **kwargs)

        return invoke


def build_default_phase1_service(settings: AppSettings) -> Phase1Service:
    if settings.stage.casefold() in {"local", "test"}:
        return UnavailablePhase1Service()  # type: ignore[return-value]
    return LazyConfiguredPhase1Service(settings)  # type: ignore[return-value]
