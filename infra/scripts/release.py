"""Build, validate, and deploy one freshly rendered ImageTracker package."""

from __future__ import annotations

import argparse
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
    "budgetEmail",
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", default="prod")
    parser.add_argument("--param", action="append", default=[], type=_parameter)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?", args.stage):
        raise ValueError("Stage must be a 1-32 character lowercase stage name")
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
