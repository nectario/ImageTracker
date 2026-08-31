"""Build, validate, and deploy one freshly rendered ImageTracker package."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


INFRA_ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = INFRA_ROOT / ".build"
CONFIG_PATH = BUILD_ROOT / "serverless.yml"
PACKAGE_ROOT = BUILD_ROOT / ".serverless"
TEMPLATE_PATH = PACKAGE_ROOT / "cloudformation-template-update-stack.json"
ALLOWED_PARAMETERS = {
    "allowedOrigin",
    "manifestImportRetryScheduleState",
    "maintenanceSchedulesState",
    "monthlyBudgetUsd",
    "retryScheduleState",
}


def _serverless() -> str:
    discovered = shutil.which("serverless")
    if discovered:
        return discovered
    candidate = INFRA_ROOT / "node_modules" / ".bin" / (
        "serverless.cmd" if sys.platform == "win32" else "serverless"
    )
    if candidate.is_file():
        return str(candidate)
    raise FileNotFoundError("Serverless CLI is not installed; run npm ci in infra")


def _parameter(value: str) -> str:
    key, separator, selected = value.partition("=")
    if not separator or key not in ALLOWED_PARAMETERS or not selected:
        raise argparse.ArgumentTypeError(
            "--param must be one supported non-secret name=value pair"
        )
    return value


def _aws_json(arguments: list[str]) -> object:
    result = subprocess.run(
        ["aws", *arguments, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout or "null")


def _ensure_budget_alert(*, stage: str, email: str) -> None:
    account_id = str(
        _aws_json(["sts", "get-caller-identity", "--query", "Account"])
    )
    budget_name = f"image-tracker-{stage}-incremental-monthly"
    notification = {
        "NotificationType": "ACTUAL",
        "ComparisonOperator": "GREATER_THAN",
        "Threshold": 80,
        "ThresholdType": "PERCENTAGE",
    }
    subscriber = {"SubscriptionType": "EMAIL", "Address": email}
    response = _aws_json(
        [
            "budgets",
            "describe-notifications-for-budget",
            "--account-id",
            account_id,
            "--budget-name",
            budget_name,
            "--region",
            "us-east-1",
        ]
    )
    notifications = response.get("Notifications", []) if isinstance(response, dict) else []
    matching = next(
        (
            item
            for item in notifications
            if item.get("NotificationType") == "ACTUAL"
            and item.get("ComparisonOperator") == "GREATER_THAN"
            and float(item.get("Threshold", -1)) == 80.0
        ),
        None,
    )
    if matching is None:
        subprocess.run(
            [
                "aws",
                "budgets",
                "create-notification",
                "--account-id",
                account_id,
                "--budget-name",
                budget_name,
                "--notification",
                json.dumps(notification, separators=(",", ":")),
                "--subscribers",
                json.dumps([subscriber], separators=(",", ":")),
                "--region",
                "us-east-1",
            ],
            check=True,
        )
        return
    subscribers = _aws_json(
        [
            "budgets",
            "describe-subscribers-for-notification",
            "--account-id",
            account_id,
            "--budget-name",
            budget_name,
            "--notification",
            json.dumps(notification, separators=(",", ":")),
            "--region",
            "us-east-1",
        ]
    )
    values = subscribers.get("Subscribers", []) if isinstance(subscribers, dict) else []
    if subscriber not in values:
        subprocess.run(
            [
                "aws",
                "budgets",
                "create-subscriber",
                "--account-id",
                account_id,
                "--budget-name",
                budget_name,
                "--notification",
                json.dumps(notification, separators=(",", ":")),
                "--subscriber",
                json.dumps(subscriber, separators=(",", ":")),
                "--region",
                "us-east-1",
            ],
            check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", default="prod")
    parser.add_argument("--param", action="append", default=[], type=_parameter)
    parser.add_argument("--budget-email")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?", args.stage):
        raise ValueError("Stage must be a 1-32 character lowercase stage name")
    if args.budget_email and not re.fullmatch(
        r"[^@\s]+@[^@\s]+\.[^@\s]+", args.budget_email
    ):
        raise ValueError("Budget email must be a valid email address")
    parameter_args = [
        item for value in args.param for item in ("--param", value)
    ]

    subprocess.run(
        [sys.executable, "scripts/stage_service.py", "--install-dependencies"],
        cwd=INFRA_ROOT,
        check=True,
    )
    subprocess.run(
        [
            _serverless(),
            "package",
            "--config",
            str(CONFIG_PATH.resolve()),
            "--stage",
            args.stage,
            "--package",
            str(PACKAGE_ROOT.resolve()),
            *parameter_args,
        ],
        cwd=INFRA_ROOT,
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "scripts/validate_foundation.py",
            "--stage",
            args.stage,
            "--packaged-template",
            str(TEMPLATE_PATH.resolve()),
        ],
        cwd=INFRA_ROOT,
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "scripts/deploy_packaged.py",
            "--stage",
            args.stage,
            *parameter_args,
        ],
        cwd=INFRA_ROOT,
        check=True,
    )
    if args.budget_email:
        _ensure_budget_alert(stage=args.stage, email=args.budget_email)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
