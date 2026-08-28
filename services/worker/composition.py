from __future__ import annotations

from pathlib import Path

import boto3

from services.api.job_dispatcher import SqsJobDispatcher
from services.common.settings import AppSettings, get_settings
from services.data.database import build_database_runtime
from services.domain.service import Phase1DomainService
from services.enrichment.aws_location import AmazonLocationReverseGeocoder
from services.enrichment.normalization import (
    LocationNormalizer,
    load_location_normalization_rules,
)
from services.enrichment.openai_scene import OpenAISceneDescriptionProvider
from services.enrichment.openai_secrets import OpenAIApiKeyResolver
from services.worker.processor import (
    DescriptionMessageProcessor,
    DueJobMessageProcessor,
    GeocodeMessageProcessor,
    LazyEnrichmentMessageRouter,
)
from services.worker.staging import S3ScenePreviewStore


def _normalization_path(settings: AppSettings) -> Path:
    selected = Path(settings.location_normalization_rules_path)
    if selected.is_absolute():
        return selected
    repository_root = Path(__file__).resolve().parents[2]
    return repository_root / selected


def build_default_processor(
    settings: AppSettings | None = None,
) -> LazyEnrichmentMessageRouter:
    """Compose the Lambda worker without resolving secrets at module import."""

    selected_settings = settings or get_settings()
    runtime = build_database_runtime(selected_settings)
    ruleset = load_location_normalization_rules(
        _normalization_path(selected_settings)
    )
    location_normalizer = LocationNormalizer(ruleset)
    dispatcher = SqsJobDispatcher(
        client=boto3.client("sqs", region_name=selected_settings.aws_region),
        queue_url=selected_settings.processing_queue_url,
    )
    repository = Phase1DomainService(
        runtime.session_factory,
        job_dispatcher=dispatcher,
        geocode_reuse_radius_meters=(
            selected_settings.geocode_reuse_radius_meters
        ),
        scene_description_model=selected_settings.scene_description_model,
        scene_description_detail=selected_settings.scene_description_detail,
        scene_description_service_tier=(
            selected_settings.scene_description_service_tier
        ),
        scene_description_max_words=selected_settings.scene_description_max_words,
        scene_description_monthly_call_limit=(
            selected_settings.scene_description_monthly_call_limit
        ),
        location_normalizer=location_normalizer,
    )

    def geocode_factory() -> GeocodeMessageProcessor:
        return GeocodeMessageProcessor(
            repository=repository,
            geocoder=AmazonLocationReverseGeocoder(
                boto3.client(
                    "geo-places", region_name=selected_settings.aws_region
                )
            ),
            normalizer=location_normalizer,
            reuse_radius_meters=selected_settings.geocode_reuse_radius_meters,
            monthly_call_limit=selected_settings.geocode_monthly_call_limit,
        )

    def description_factory() -> DescriptionMessageProcessor:
        openai_resolver = OpenAIApiKeyResolver(
            region_name=selected_settings.aws_region,
            stage=selected_settings.stage,
            parameter_name=selected_settings.openai_secret_parameter,
        )
        openai_api_key = openai_resolver.resolve()
        scene_provider = OpenAISceneDescriptionProvider(
            openai_api_key,
            model=selected_settings.scene_description_model,
            detail=selected_settings.scene_description_detail,
            service_tier=selected_settings.scene_description_service_tier,
            max_words=selected_settings.scene_description_max_words,
            api_key_loader=openai_resolver.resolve,
            api_key_invalidator=openai_resolver.clear,
        )
        return DescriptionMessageProcessor(
            repository=repository,
            provider=scene_provider,
            preview_store=S3ScenePreviewStore(
                boto3.client("s3", region_name=selected_settings.aws_region),
                allowed_bucket=selected_settings.media_bucket,
            ),
            monthly_call_limit=(
                selected_settings.scene_description_monthly_call_limit
            ),
        )

    return LazyEnrichmentMessageRouter(
        geocode_factory=geocode_factory,
        description_factory=description_factory,
        due_jobs_factory=lambda: DueJobMessageProcessor(repository=repository),
    )
