from __future__ import annotations

import json
from asyncio import run
from datetime import datetime
from pathlib import Path
import subprocess
import sys
import zipfile

import httpx
import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from cli.imagetracker_cli.app import app as cli_app
from services.api.app import create_app
from services.common.enums import (
    EnrichmentStatus,
    MediaType,
    ProcessingJobStatus,
    SourcePlatform,
    StorageMode,
    StorageState,
    UserFacingState,
)
from services.common.settings import AppSettings, get_settings


def test_health_endpoint_is_stable_and_does_not_require_secrets():
    app = create_app(AppSettings(stage="test"))

    async def request_health() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(
                "/v1/health",
                headers={"x-request-id": "d27598e0-2607-45a7-a6c0-f12bb44a2cf0"},
            )

    response = run(request_health())

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "d27598e0-2607-45a7-a6c0-f12bb44a2cf0"
    payload = response.json()
    assert payload["status"] == "Ok"
    assert payload["service"] == "imagetracker-api"
    assert payload["version"] == "0.3.0"
    assert set(payload) == {"status", "service", "version", "timeUtc"}
    assert datetime.fromisoformat(payload["timeUtc"].replace("Z", "+00:00")).tzinfo is not None


def test_service_refuses_another_deeptrading_database():
    with pytest.raises(ValidationError, match="only to the ImageTracker database"):
        AppSettings(mysql_database="DeepTradingAI")


def test_public_state_values_match_the_approved_contract():
    assert StorageMode.LOCAL.value == "Local"
    assert StorageMode.REMOTE.value == "Remote"
    assert EnrichmentStatus.DEFERRED_QUOTA.value == "DeferredQuota"
    assert UserFacingState.WAITING_FOR_MONTHLY_QUOTA.value == "WaitingForMonthlyQuota"
    assert {item.value for item in SourcePlatform} == {
        "iOS",
        "Android",
        "Windows",
        "LinuxCLI",
        "WindowsCLI",
    }


def test_all_shared_enums_match_openapi():
    contract_path = Path(__file__).resolve().parents[1] / "contracts" / "v1" / "openapi.json"
    schemas = json.loads(contract_path.read_text(encoding="utf-8"))["components"]["schemas"]
    shared_enums = {
        "StorageMode": StorageMode,
        "MediaType": MediaType,
        "Platform": SourcePlatform,
        "StorageState": StorageState,
        "UserFacingState": UserFacingState,
        "EnrichmentStatus": EnrichmentStatus,
        "ProcessingJobStatus": ProcessingJobStatus,
    }

    for schema_name, enum_type in shared_enums.items():
        assert {item.value for item in enum_type} == set(schemas[schema_name]["enum"])


def test_cli_doctor_json_is_non_secret(monkeypatch):
    monkeypatch.setenv("IMAGETRACKER_STAGE", "test")
    monkeypatch.setenv("IMAGETRACKER_API_URL", "https://example.invalid")
    monkeypatch.setenv("MYSQL_PASSWORD", "must-not-appear")
    get_settings.cache_clear()

    result = CliRunner().invoke(cli_app, ["doctor", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["database_scope"] == "ImageTracker"
    assert payload["api_configured"] is True
    assert "must-not-appear" not in result.stdout
    get_settings.cache_clear()


def test_lambda_packaging_keeps_runtime_files_at_archive_root():
    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "infra" / "package.json").read_text(encoding="utf-8"))
    assert "--package .build/.serverless" in package["scripts"]["package"]
    deploy_script = package["scripts"]["deploy"]
    assert deploy_script == "python scripts/release.py"
    release_helper = (root / "infra" / "scripts" / "release.py").read_text(
        encoding="utf-8"
    )
    assert '"package"' in release_helper
    assert '"scripts/validate_foundation.py"' in release_helper
    assert '"scripts/deploy_packaged.py"' in release_helper
    assert "parameter_args" in release_helper
    assert 'parser.add_argument("--budget-email")' in release_helper
    assert '"create-notification"' in release_helper
    assert '"create-subscriber"' in release_helper
    assert '"budgetEmail"' not in release_helper

    deploy_helper = (root / "infra" / "scripts" / "deploy_packaged.py").read_text(
        encoding="utf-8"
    )
    assert "str(CONFIG_PATH.resolve())" in deploy_helper
    assert "str(PACKAGE_ROOT.resolve())" in deploy_helper

    validator = (root / "infra" / "scripts" / "validate_foundation.py").read_text(
        encoding="utf-8"
    )
    assert '"services/api/handler.py"' in validator
    assert '"services/api/job_dispatcher.py"' in validator
    assert '"services/api/temporary_store.py"' in validator
    assert '"services/worker/handler.py"' in validator
    assert '"services/worker/composition.py"' in validator
    assert '"services/worker/staging.py"' in validator
    assert '"services/bulk/handler.py"' in validator
    assert '"services/bulk/composition.py"' in validator
    assert '"services/bulk/manifest.py"' in validator
    assert '"services/bulk/processor.py"' in validator
    assert '"services/bulk/repository.py"' in validator
    assert '"services/enrichment/aws_location.py"' in validator
    assert '"services/enrichment/models.py"' in validator
    assert '"services/enrichment/normalization.py"' in validator
    assert '"services/enrichment/openai_scene.py"' in validator
    assert '"services/enrichment/openai_secrets.py"' in validator
    assert '"services/data/certs/us-east-2-bundle.pem"' in validator
    assert '"location_normalization_rules.json"' in validator
    assert 'name.startswith(".build/")' in validator

    stage_script = (root / "infra" / "scripts" / "stage_service.py").read_text(
        encoding="utf-8"
    )
    assert 'REPOSITORY_ROOT / "location_normalization_rules.json"' in stage_script
    assert "WORKER_SOURCE_ROOT / \"handler.py\"" in stage_script
    assert "BULK_SOURCE_ROOT / \"handler.py\"" in stage_script


def test_worker_infrastructure_is_bounded_and_schedules_stay_disabled():
    root = Path(__file__).resolve().parents[1]
    serverless = (root / "infra" / "serverless.yml").read_text(encoding="utf-8")

    assert "handler: services/worker/handler.handler" in serverless
    assert "memorySize: 384" in serverless
    assert "timeout: 120" in serverless
    assert "VisibilityTimeout: 900" in serverless
    assert "reservedConcurrency: 1" in serverless
    assert "batchSize: 1" in serverless
    assert "functionResponseType: ReportBatchItemFailures" in serverless
    assert "- sqs:ReceiveMessage" in serverless
    assert "- sqs:DeleteMessage" in serverless
    assert "- sqs:ChangeMessageVisibility" in serverless
    assert "- sqs:GetQueueAttributes" in serverless
    assert "- geo-places:ReverseGeocode" in serverless
    assert "::provider/default" in serverless
    assert "IMAGETRACKER_GEOCODE_REUSE_RADIUS_METERS: '5'" in serverless
    assert "IMAGETRACKER_GEOCODE_MONTHLY_CALL_LIMIT: '1000'" in serverless
    assert "IMAGETRACKER_SCENE_DESCRIPTION_MODEL: gpt-5.6-terra" in serverless
    assert "IMAGETRACKER_SCENE_DESCRIPTION_SERVICE_TIER: flex" in serverless
    assert "IMAGETRACKER_SCENE_DESCRIPTION_MONTHLY_CALL_LIMIT: '1000'" in serverless
    assert serverless.count("State: ${self:custom.maintenanceSchedulesState}") == 3
    assert "retryScheduleState: ${param:retryScheduleState, 'ENABLED'}" in serverless
    assert "State: ${self:custom.retryScheduleState}" in serverless
    assert "maintenanceSchedulesState: ${param:maintenanceSchedulesState, 'DISABLED'}" in serverless
    assert "maxReceiveCount: 8" in serverless
    assert "handler: services/bulk/handler.handler" in serverless
    assert "name: ${self:service}-${sls:stage}-manifest-import-worker" in serverless
    assert "memorySize: 1024" in serverless
    assert "timeout: 900" in serverless
    assert "ephemeralStorageSize: 2048" in serverless
    assert "VisibilityTimeout: 1800" in serverless
    assert "maxReceiveCount: 5" in serverless
    assert "IMAGETRACKER_MANIFEST_IMPORT_QUEUE_URL:" in serverless
    assert "Prefix: manifests/input/" in serverless
    assert "ManifestImportRetrySchedule:" in serverless
    assert "RetryManifestImports" in serverless


def test_python_wheel_includes_the_rds_trust_bundle(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(tmp_path),
            str(root),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(tmp_path.glob("imagetracker-*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as archive:
        assert "services/data/certs/us-east-2-bundle.pem" in archive.namelist()
