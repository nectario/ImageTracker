from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID

import boto3
import pymysql
from pymysql.cursors import DictCursor

from services.api.job_dispatcher import SqsJobDispatcher
from services.bulk.manifest import ManifestGuardrails
from services.bulk.processor import (
    BulkManifestProcessor,
    BulkMessageDisposition,
    BulkProcessorSettings,
    S3ManifestObjectStore,
)
from services.bulk.repository import MergeSettings, MySqlManifestImportRepository
from services.common.settings import AppSettings, get_settings
from services.data.database import (
    DatabaseConnectionConfig,
    database_config_from_secret,
    default_ssm_resolver,
)


class InvalidBulkMessage(ValueError):
    pass


class SqsManifestImportDispatcher:
    def __init__(self, *, client: Any, queue_url: str) -> None:
        if not queue_url:
            raise ValueError("The manifest import queue URL is required")
        self._client = client
        self._queue_url = queue_url

    def dispatch(self, import_ids: tuple[UUID, ...]) -> None:
        for offset in range(0, len(import_ids), 10):
            batch = import_ids[offset : offset + 10]
            response = self._client.send_message_batch(
                QueueUrl=self._queue_url,
                Entries=[
                    {
                        "Id": str(index),
                        "MessageBody": json.dumps(
                            {"jobType": "BulkManifest", "importId": str(import_id)},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    }
                    for index, import_id in enumerate(batch)
                ],
            )
            failed = response.get("Failed") or []
            if failed:
                raise RuntimeError("SQS rejected one or more manifest imports")


class BulkMessageRouter:
    def __init__(
        self,
        *,
        processor: BulkManifestProcessor,
        repository: MySqlManifestImportRepository,
        import_dispatcher: SqsManifestImportDispatcher,
    ) -> None:
        self._processor = processor
        self._repository = repository
        self._import_dispatcher = import_dispatcher

    def process_message(
        self, *, message_id: str, body: str | Mapping[str, Any]
    ) -> BulkMessageDisposition:
        payload = self._payload(body)
        job_type = payload.get("jobType")
        if job_type == "BulkManifest":
            try:
                import_id = UUID(str(payload.get("importId")))
            except (TypeError, ValueError, AttributeError) as exc:
                raise InvalidBulkMessage("BulkManifest importId is invalid") from exc
            return self._processor.process(import_id=import_id, message_id=message_id)
        if job_type == "RetryManifestImports":
            try:
                due = self._repository.due_import_ids(limit=100)
                self._import_dispatcher.dispatch(due)
            except Exception:
                return BulkMessageDisposition.RETRY
            return BulkMessageDisposition.ACK
        raise InvalidBulkMessage("The bulk worker message type is invalid")

    @staticmethod
    def _payload(body: str | Mapping[str, Any]) -> Mapping[str, Any]:
        if isinstance(body, str):
            try:
                value = json.loads(body)
            except json.JSONDecodeError as exc:
                raise InvalidBulkMessage("The bulk worker body is invalid JSON") from exc
        else:
            value = body
        if not isinstance(value, Mapping):
            raise InvalidBulkMessage("The bulk worker body must be an object")
        return value


def _database_config(settings: AppSettings) -> DatabaseConnectionConfig:
    raw_secret = default_ssm_resolver(settings.aws_region).resolve(
        settings.db_secret_parameter
    )
    config = database_config_from_secret(
        raw_secret, required_database=settings.mysql_database
    )
    if config.tls_enabled and config.ssl_ca is None:
        bundled = (
            Path(__file__).resolve().parents[1]
            / "data"
            / "certs"
            / f"{settings.aws_region}-bundle.pem"
        )
        if not bundled.is_file():
            raise ValueError("No bundled Amazon RDS CA is available for this region")
        config = replace(config, ssl_ca=str(bundled))
    return config


def _connection_factory(settings: AppSettings):
    config = _database_config(settings)
    url = config.url

    def connect():
        ssl: dict[str, Any] | None = None
        if config.tls_enabled:
            ssl = {"check_hostname": True}
            if config.ssl_ca:
                ssl["ca"] = config.ssl_ca
        return pymysql.connect(
            host=str(url.host),
            port=int(url.port or 3306),
            user=str(url.username),
            password=str(url.password or ""),
            database="ImageTracker",
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=10,
            read_timeout=900,
            write_timeout=900,
            cursorclass=DictCursor,
            local_infile=True,
            ssl=ssl,
        )

    return connect


def build_default_processor(
    settings: AppSettings | None = None,
) -> BulkMessageRouter:
    selected = settings or get_settings()
    if not selected.media_bucket or not selected.manifest_import_queue_url:
        raise ValueError("Bulk manifest storage and queue configuration are required")
    repository = MySqlManifestImportRepository(_connection_factory(selected))
    sqs = boto3.client("sqs", region_name=selected.aws_region)
    processing_dispatcher = SqsJobDispatcher(
        client=sqs,
        queue_url=selected.processing_queue_url,
    )
    processor = BulkManifestProcessor(
        repository=repository,
        object_store=S3ManifestObjectStore(
            boto3.client("s3", region_name=selected.aws_region)
        ),
        settings=BulkProcessorSettings(
            result_bucket=selected.media_bucket,
            guardrails=ManifestGuardrails(
                max_compressed_bytes=selected.manifest_import_max_compressed_bytes,
                max_uncompressed_bytes=selected.manifest_import_max_uncompressed_bytes,
                max_entries=selected.manifest_import_max_entries,
            ),
            merge=MergeSettings(
                description_model=selected.scene_description_model,
                description_detail=selected.scene_description_detail,
                description_service_tier=selected.scene_description_service_tier,
                description_max_words=selected.scene_description_max_words,
                description_monthly_call_limit=(
                    selected.scene_description_monthly_call_limit
                ),
                description_monthly_usd_limit=(
                    selected.scene_description_monthly_usd_limit
                ),
                description_reserved_usd_per_request=(
                    selected.scene_description_reserved_usd_per_request
                ),
                description_input_usd_per_million=(
                    selected.scene_description_input_usd_per_million
                ),
                description_cached_input_usd_per_million=(
                    selected.scene_description_cached_input_usd_per_million
                ),
                description_output_usd_per_million=(
                    selected.scene_description_output_usd_per_million
                ),
            ),
        ),
        job_dispatcher=processing_dispatcher,
    )
    import_dispatcher = SqsManifestImportDispatcher(
        client=sqs, queue_url=selected.manifest_import_queue_url
    )
    return BulkMessageRouter(
        processor=processor,
        repository=repository,
        import_dispatcher=import_dispatcher,
    )
