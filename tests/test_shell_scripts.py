from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
EXPECTED_SCRIPTS = {
    "_common.sh",
    "api-smoke.sh",
    "aws-smoke.sh",
    "cli.sh",
    "db-smoke.sh",
    "package-infra.sh",
    "play.sh",
    "setup.sh",
    "test.sh",
}


def test_shell_scripts_parse_as_bash():
    actual = {path.name for path in SCRIPTS.glob("*.sh")}
    assert actual == EXPECTED_SCRIPTS

    for path in sorted(SCRIPTS.glob("*.sh")):
        subprocess.run(["bash", "-n", str(path)], check=True)


def test_playground_has_no_deploy_or_importer_execution_path():
    executable_text = "\n".join(
        path.read_text(encoding="utf-8") for path in SCRIPTS.glob("*.sh")
    )

    assert "serverless deploy" not in executable_text
    assert "npm run deploy" not in executable_text
    assert "ImageTracker.py" not in executable_text
    assert "tag_location.py" not in executable_text


def test_playground_help_is_available_without_environment_setup():
    result = subprocess.run(
        ["bash", str(SCRIPTS / "play.sh"), "help"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "ImageTracker Phase 0 playground" in result.stdout
    assert "No command in this toolkit deploys AWS resources" in result.stdout
