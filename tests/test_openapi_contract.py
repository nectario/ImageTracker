from __future__ import annotations

import json
from pathlib import Path

from openapi_spec_validator import validate


CONTRACT_PATH = Path(__file__).resolve().parents[1] / "contracts" / "v1" / "openapi.json"


def test_openapi_v1_is_valid_openapi_3_document():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    validate(document)


def test_health_inherits_cognito_authentication():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    operation = document["paths"]["/v1/health"]["get"]

    assert document["security"] == [{"CognitoBearer": []}]
    assert "security" not in operation
    assert "401" in operation["responses"]


def test_multipart_upload_contract_can_resume_without_reuploading():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert "get" in document["paths"]["/v1/uploads/{uploadSessionId}"]
    signing = document["paths"]["/v1/uploads/{uploadSessionId}/parts"]["post"]
    assert signing["operationId"] == "signUploadParts"
    request = document["components"]["schemas"]["UploadPartSigningRequest"]
    assert request["properties"]["parts"]["maxItems"] == 100
    assert "parts" not in document["components"]["schemas"]["MultipartUploadPlan"]["properties"]


def test_local_temporary_processing_has_an_authorized_staging_path():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]

    assert "TemporaryProcessing" in schemas["UploadPurpose"]["enum"]
    assert "processingJobId" in schemas["UploadPlanRequest"]["properties"]
    assert "one-day staging upload" in document["paths"]["/v1/uploads/plan"]["post"]["description"]


def test_upload_object_checksum_is_distinct_from_asset_identity():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]
    plan = schemas["UploadPlanRequest"]
    complete = schemas["UploadCompleteRequest"]

    assert {"assetContentSha256", "objectSha256"}.issubset(plan["required"])
    assert {"objectMimeType", "objectByteSize"}.issubset(plan["required"])
    assert "contentSha256" not in plan["properties"]
    assert complete["required"] == ["objectSha256"]


def test_multipart_transport_integrity_requires_per_part_checksums():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]

    assert "headers" in schemas["SignedUploadPart"]["required"]
    assert schemas["MultipartUploadPlan"]["properties"]["checksumAlgorithm"]["enum"] == [
        "SHA256"
    ]
    assert schemas["MultipartUploadPlan"]["properties"]["checksumType"]["enum"] == [
        "COMPOSITE"
    ]
    completed_part = schemas["CompletedUploadPart"]
    assert "checksumSha256" in completed_part["required"]
    assert completed_part["properties"]["checksumSha256"]["maxLength"] == 44
    signing_part = schemas["UploadPartChecksum"]
    assert set(signing_part["required"]) == {"partNumber", "checksumSha256"}


def test_media_source_has_a_stable_non_path_source_key():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]

    assert "sourceKey" in schemas["MediaSourceCreateRequest"]["required"]
    assert schemas["MediaSourceCreateRequest"]["properties"]["sourceKey"]["maxLength"] == 128
    assert "sourceKey" in schemas["MediaSource"]["required"]


def test_source_create_documents_idempotent_return_and_reactivation() -> None:
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    operation = document["paths"]["/v1/sources"]["post"]

    assert {"200", "201"}.issubset(operation["responses"])
    assert "reactivates the same source identity" in operation["description"]


def test_implemented_authenticated_phase1_operations_document_service_outages():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    implemented_operations = {
        "/v1/health": ("get",),
        "/v1/me": ("get",),
        "/v1/devices": ("get", "post"),
        "/v1/sources": ("get", "post"),
        "/v1/sources/{sourceId}": ("get", "patch", "delete"),
        "/v1/sources/{sourceId}/manifest": ("post",),
        "/v1/changes": ("get",),
        "/v1/media": ("get",),
        "/v1/media/search": ("get",),
        "/v1/media/{mediaAssetId}": ("get",),
        "/v1/jobs": ("get",),
        "/v1/jobs/{jobId}": ("get",),
        "/v1/jobs/{jobId}/retry": ("post",),
        "/v1/admin/health": ("get",),
        "/v1/admin/audit": ("get",),
    }

    for path, methods in implemented_operations.items():
        for method in methods:
            assert document["paths"][path][method]["responses"]["503"] == {
                "$ref": "#/components/responses/ServiceUnavailable"
            }


def test_manifest_variants_preserve_filename_and_match_live_column_limits():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]
    upsert = schemas["ManifestUpsertEntry"]
    deleted = schemas["ManifestDeletedEntry"]

    assert {"fileName", "mediaType", "mimeType", "byteSize"}.issubset(upsert["required"])
    assert upsert["properties"]["operation"]["enum"] == ["Upsert"]
    assert deleted["properties"]["operation"]["enum"] == ["Deleted"]
    assert set(deleted["required"]) == {"operation", "sourceItemId", "sourceRevision"}
    assert upsert["properties"]["sourceItemId"]["maxLength"] == 512
    assert upsert["properties"]["sourceRevision"]["maxLength"] == 255
    assert upsert["properties"]["fileName"]["maxLength"] == 512
    assert upsert["properties"]["timeZoneId"]["maxLength"] == 64


def test_location_contract_exposes_normalized_address_and_provider_provenance():
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    schemas = document["components"]["schemas"]
    detail_fields = set(schemas["MediaLocation"]["properties"])
    assert {
        "streetAddress",
        "originalStreetNumber",
        "neighborhood",
        "city",
        "county",
        "state",
        "postalCode",
        "country",
        "countryCode",
        "provider",
        "providerPlaceId",
        "normalizationRuleVersion",
        "providerUpdatedAtUtc",
    } <= detail_fields
    summary_fields = set(schemas["LocationSummary"]["properties"])
    assert {
        "displayName",
        "streetAddress",
        "neighborhood",
        "city",
        "county",
        "state",
        "postalCode",
        "country",
        "countryCode",
        "provider",
    } <= summary_fields
