"""Stage the shared ImageTracker services beside the Serverless configuration.

Serverless Framework resolves handler paths relative to the service directory.
The application code remains owned by ``services``; this script creates an
ignored, disposable build tree rather than maintaining an infrastructure copy.
"""

from __future__ import annotations

import argparse
import ast
from pathlib import Path
import shutil
import subprocess
import sys
import time


INFRA_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = INFRA_ROOT.parent
SERVICES_ROOT = REPOSITORY_ROOT / "services"
API_SOURCE_ROOT = SERVICES_ROOT / "api"
WORKER_SOURCE_ROOT = SERVICES_ROOT / "worker"
NORMALIZATION_RULES_PATH = REPOSITORY_ROOT / "location_normalization_rules.json"
BUILD_ROOT = INFRA_ROOT / ".build"


def _validate_paths() -> None:
    if BUILD_ROOT.resolve().parent != INFRA_ROOT.resolve() or BUILD_ROOT.name != ".build":
        raise RuntimeError(f"Unsafe build directory: {BUILD_ROOT}")

    handler_paths = (
        API_SOURCE_ROOT / "handler.py",
        WORKER_SOURCE_ROOT / "handler.py",
    )
    for handler_path in handler_paths:
        if not handler_path.is_file():
            raise FileNotFoundError(
                f"Shared Lambda handler not found at {handler_path}. "
                "Create the services/api and services/worker handlers before packaging."
            )

        tree = ast.parse(handler_path.read_text(encoding="utf-8"), filename=str(handler_path))
        exports_handler = any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "handler"
            for node in tree.body
        )
        if not exports_handler:
            raise ValueError(f"{handler_path} must export handler(event, context)")

    if not NORMALIZATION_RULES_PATH.is_file():
        raise FileNotFoundError(
            f"Location normalization rules not found at {NORMALIZATION_RULES_PATH}."
        )

    requirements_path = API_SOURCE_ROOT / "requirements.txt"
    if not requirements_path.is_file():
        raise FileNotFoundError(
            f"Runtime requirements not found at {requirements_path}. "
            "An empty requirements.txt is valid when the handler uses only the standard library."
        )


def _replace_build_tree() -> None:
    if BUILD_ROOT.exists():
        # Windows antivirus/indexing can briefly retain handles while WSL is
        # replacing the generated dependency tree. Retrying the same tightly
        # validated build path keeps packaging deterministic without touching
        # repository-owned source files.
        for attempt in range(6):
            try:
                shutil.rmtree(BUILD_ROOT)
                break
            except FileNotFoundError:
                break
            except OSError:
                if attempt == 5:
                    raise
                time.sleep(0.25 * (attempt + 1))

    staged_source = BUILD_ROOT / "services"
    staged_source.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        SERVICES_ROOT,
        staged_source,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"),
    )
    shutil.copy2(INFRA_ROOT / "serverless.yml", BUILD_ROOT / "serverless.yml")
    shutil.copy2(
        NORMALIZATION_RULES_PATH,
        BUILD_ROOT / NORMALIZATION_RULES_PATH.name,
    )


def _install_runtime_dependencies() -> None:
    requirements_path = API_SOURCE_ROOT / "requirements.txt"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--requirement",
            str(requirements_path),
            "--target",
            str(BUILD_ROOT),
            "--upgrade",
            "--platform",
            "manylinux2014_x86_64",
            "--implementation",
            "cp",
            "--python-version",
            "3.12",
            "--abi",
            "cp312",
            "--only-binary",
            ":all:",
        ],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--install-dependencies",
        action="store_true",
        help="Install services/api/requirements.txt into the staged Lambda package.",
    )
    args = parser.parse_args()

    _validate_paths()
    _replace_build_tree()
    if args.install_dependencies:
        _install_runtime_dependencies()

    print(f"Staged Serverless service at {BUILD_ROOT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
