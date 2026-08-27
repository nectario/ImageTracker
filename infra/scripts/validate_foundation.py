"""Perform credential-free structural checks on the ImageTracker foundation."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
import sys
import zipfile


INFRA_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = INFRA_ROOT / "serverless.yml"

REQUIRED_MARKERS = {
    "Python 3.12 runtime": "runtime: python3.12",
    "us-east-2 region": "region: us-east-2",
    "shared API handler": "handler: services/api/handler.handler",
    "bounded API concurrency": "reservedConcurrency: 4",
    "Phase 1 API proxy": "path: /v1/{proxy+}",
    "device context CORS header": "- X-ImageTracker-Device-Id",
    "Cognito user pool": "Type: AWS::Cognito::UserPool",
    "case-insensitive email sign-in": "CaseSensitive: false",
    "Cognito JWT authorizer": "type: jwt",
    "private media bucket": "PublicAccessBlockConfiguration:",
    "SSE-S3": "SSEAlgorithm: AES256",
    "multipart cleanup": "AbortIncompleteMultipartUpload:",
    "one-day staging cleanup": "ExpirationInDays: 1",
    "30-day trash cleanup": "ExpirationInDays: 30",
    "processing queue": "Type: AWS::SQS::Queue",
    "dead-letter policy": "RedrivePolicy:",
    "maintenance schedules": "Type: AWS::Events::Rule",
    "disabled schedule default": "maintenanceSchedulesState: ${param:maintenanceSchedulesState, 'DISABLED'}",
    "SSM parameter prefix": "IMAGETRACKER_CONFIG_PARAMETER_PREFIX:",
    "incremental budget": "Type: AWS::Budgets::Budget",
    "resource tags": "Application: ImageTracker",
}

FORBIDDEN_MARKERS = {
    "NAT gateway": "AWS::EC2::NatGateway",
    "RDS Proxy": "AWS::RDS::DBProxy",
    "ECS service": "AWS::ECS::Service",
    "load balancer": "AWS::ElasticLoadBalancingV2::LoadBalancer",
    "persistent SageMaker endpoint": "AWS::SageMaker::Endpoint",
}

FORBIDDEN_RESOURCE_TYPES = {
    "AWS::EC2::NatGateway",
    "AWS::RDS::DBProxy",
    "AWS::ECS::Service",
    "AWS::ElasticLoadBalancingV2::LoadBalancer",
    "AWS::SageMaker::Endpoint",
}

EXPECTED_RESOURCE_TYPES = {
    "AWS::ApiGatewayV2::Api",
    "AWS::Budgets::Budget",
    "AWS::Cognito::UserPool",
    "AWS::Cognito::UserPoolClient",
    "AWS::Events::Rule",
    "AWS::Lambda::Function",
    "AWS::S3::Bucket",
    "AWS::SQS::Queue",
}


def _validate_packaged_template(path: Path) -> list[str]:
    template = json.loads(path.read_text(encoding="utf-8"))
    resources = template.get("Resources", {})
    resource_types = {resource.get("Type") for resource in resources.values()}
    failures: list[str] = []

    missing_types = EXPECTED_RESOURCE_TYPES - resource_types
    if missing_types:
        failures.append(f"packaged template is missing resource types: {sorted(missing_types)}")

    forbidden_types = FORBIDDEN_RESOURCE_TYPES & resource_types
    if forbidden_types:
        failures.append(f"packaged template contains forbidden resource types: {sorted(forbidden_types)}")

    lambda_resource = resources.get("ApiLambdaFunction", {}).get("Properties", {})
    if lambda_resource.get("Runtime") != "python3.12":
        failures.append("packaged Lambda runtime is not python3.12")
    if lambda_resource.get("Handler") != "services/api/handler.handler":
        failures.append("packaged Lambda handler does not point to services/api")
    if lambda_resource.get("ReservedConcurrentExecutions") != 4:
        failures.append("packaged Lambda concurrency is not bounded at 4")
    if lambda_resource.get("MemorySize") != 512:
        failures.append("packaged Lambda memory is not 512 MB")
    if lambda_resource.get("Timeout") != 28:
        failures.append("packaged Lambda timeout is not 28 seconds")

    user_pool = resources.get("ImageTrackerUserPoolV2", {}).get("Properties", {})
    if user_pool.get("UsernameConfiguration", {}).get("CaseSensitive") is not False:
        failures.append("Cognito email usernames must be case-insensitive")

    for logical_id in (
        "RetrySchedule",
        "ReconciliationSchedule",
        "QuotaResetSchedule",
        "TrashPurgeSchedule",
    ):
        state = resources.get(logical_id, {}).get("Properties", {}).get("State")
        if state != "DISABLED":
            failures.append(f"{logical_id} must package as DISABLED until a worker is enabled")

    rendered = json.dumps(template)
    for secret_marker in ("sk-", "AKIA", "mysql://", "mysql+pymysql://"):
        if secret_marker in rendered:
            failures.append(f"packaged template appears to contain a secret value: {secret_marker}")

    for output_name, output in template.get("Outputs", {}).items():
        value = output.get("Value")
        if isinstance(value, dict) and len(value) != 1:
            failures.append(
                f"{output_name} has multiple intrinsic functions in one output value"
            )

    return failures


def _validate_lambda_archive(template_path: Path) -> list[str]:
    archive_path = template_path.parent / "image-tracker.zip"
    if not archive_path.is_file():
        return [f"packaged Lambda archive does not exist: {archive_path}"]

    required_entries = {
        "services/api/handler.py",
        "services/api/domain_adapter.py",
        "services/data/database.py",
        "services/domain/service.py",
        "services/data/certs/us-east-2-bundle.pem",
    }
    failures: list[str] = []
    try:
        with zipfile.ZipFile(archive_path) as archive:
            names = set(archive.namelist())
    except (OSError, zipfile.BadZipFile) as exc:
        return [f"packaged Lambda archive is invalid: {exc}"]

    missing = required_entries - names
    if missing:
        failures.append(
            f"packaged Lambda archive is missing runtime entries: {sorted(missing)}"
        )
    if any(name.startswith(".build/") for name in names):
        failures.append(
            "packaged Lambda archive incorrectly nests runtime files under .build/"
        )
    if any(
        name == ".env"
        or name.startswith(".git/")
        or "/.git/" in name
        or name.endswith("/.env")
        for name in names
    ):
        failures.append("packaged Lambda archive contains repository secrets or metadata")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", default="prod", help="Stage name to validate.")
    parser.add_argument(
        "--packaged-template",
        type=Path,
        help="Optional generated CloudFormation JSON to validate after `serverless package`.",
    )
    args = parser.parse_args()

    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?", args.stage):
        print(
            "Stage must be 1-32 lowercase letters, numbers, or hyphens and cannot end in a hyphen.",
            file=sys.stderr,
        )
        return 2

    text = CONFIG_PATH.read_text(encoding="utf-8")
    failures: list[str] = []

    for description, marker in REQUIRED_MARKERS.items():
        if marker not in text:
            failures.append(f"missing {description}: {marker}")

    for description, marker in FORBIDDEN_MARKERS.items():
        if marker in text:
            failures.append(f"forbidden {description}: {marker}")

    if "vpc:" in text:
        failures.append("Lambda VPC attachment is intentionally excluded from this stack")

    if args.packaged_template:
        if not args.packaged_template.is_file():
            failures.append(f"packaged template does not exist: {args.packaged_template}")
        else:
            failures.extend(_validate_packaged_template(args.packaged_template))
            failures.extend(_validate_lambda_archive(args.packaged_template))

    if failures:
        for failure in failures:
            print(f"ERROR: {failure}", file=sys.stderr)
        return 1

    print(f"Foundation structure is valid for stage '{args.stage}'.")
    if args.packaged_template:
        print(f"Packaged CloudFormation is valid: {args.packaged_template}")
    else:
        print("Run Serverless packaging in WSL for full schema and CloudFormation validation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
