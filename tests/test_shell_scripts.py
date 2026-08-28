from __future__ import annotations

import os
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
    "store-openai-key.sh",
    "migrate-db.sh",
    "test.sh",
}


def test_shell_scripts_parse_as_bash():
    actual = {path.name for path in SCRIPTS.glob("*.sh")}
    assert actual == EXPECTED_SCRIPTS

    for path in sorted(SCRIPTS.glob("*.sh")):
        subprocess.run(["bash", "-n", str(path)], check=True)


def test_playground_has_no_deploy_migration_or_importer_execution_path():
    executable_text = (SCRIPTS / "play.sh").read_text(encoding="utf-8")

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

    assert "ImageTracker developer playground" in result.stdout
    assert "No `play.sh` command" in result.stdout
    assert "excluded from this playground" in result.stdout


def test_migration_wrapper_is_explicit_and_never_runs_legacy_importer():
    text = (SCRIPTS / "migrate-db.sh").read_text(encoding="utf-8")
    assert "migrate_enrichment.py" in text
    assert '"$@"' in text
    assert "ImageTracker.py" not in text


def test_store_openai_key_updates_env_without_printing_secret(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("EXISTING=value\nOPENAI_API_KEY=old\n", encoding="utf-8")
    secret = "sk-test-must-not-appear"
    result = subprocess.run(
        ["bash", str(SCRIPTS / "store-openai-key.sh")],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "OPENAI_API_KEY": secret,
            "IMAGETRACKER_ENV_FILE": str(env_file),
        },
    )

    assert secret not in result.stdout
    assert secret not in result.stderr
    assert env_file.read_text(encoding="utf-8") == (
        f"EXISTING=value\nOPENAI_API_KEY={secret}\n"
    )
