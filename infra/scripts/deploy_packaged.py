"""Deploy the validated staged Serverless package using absolute paths."""

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


def _serverless_executable() -> str:
    discovered = shutil.which("serverless")
    if discovered:
        return discovered
    candidate = INFRA_ROOT / "node_modules" / ".bin" / (
        "serverless.cmd" if sys.platform == "win32" else "serverless"
    )
    if candidate.is_file():
        return str(candidate)
    raise FileNotFoundError("Serverless CLI is not installed; run npm ci in infra")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", default="prod")
    args, serverless_args = parser.parse_known_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?", args.stage):
        raise ValueError("Stage must be a 1-32 character lowercase stage name")
    if BUILD_ROOT.resolve().parent != INFRA_ROOT.resolve():
        raise RuntimeError(f"Unsafe build root: {BUILD_ROOT}")
    for required in (
        CONFIG_PATH,
        PACKAGE_ROOT / "serverless-state.json",
        PACKAGE_ROOT / "image-tracker.zip",
    ):
        if not required.is_file():
            raise FileNotFoundError(f"Validated package artifact is missing: {required}")

    command = [
        _serverless_executable(),
        "deploy",
        "--config",
        str(CONFIG_PATH.resolve()),
        "--stage",
        args.stage,
        "--package",
        str(PACKAGE_ROOT.resolve()),
        *serverless_args,
    ]
    subprocess.run(command, cwd=INFRA_ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
