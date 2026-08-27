from __future__ import annotations

import json
import os
import stat
import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol


CONFIG_ENV = "IMAGETRACKER_CONFIG_DIR"
DEFAULT_STACK_NAME = "image-tracker-prod"
REQUIRED_STACK_OUTPUTS = {
    "ImageTrackerHttpApiUrl": "api_url",
    "CognitoUserPoolId": "cognito_user_pool_id",
    "CognitoUserPoolClientId": "cognito_client_id",
}


class StackClient(Protocol):
    def describe_stacks(self, **kwargs: Any) -> Mapping[str, Any]: ...


def default_config_dir() -> Path:
    override = os.environ.get(CONFIG_ENV)
    if override:
        return Path(override).expanduser()
    if os.name == "nt" and os.environ.get("APPDATA"):
        return Path(os.environ["APPDATA"]) / "ImageTracker"
    xdg_home = os.environ.get("XDG_CONFIG_HOME")
    return (Path(xdg_home) if xdg_home else Path.home() / ".config") / "imagetracker"


def ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        path.chmod(stat.S_IRWXU)


def write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    ensure_private_directory(path.parent)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if os.name != "nt":
        temporary.chmod(stat.S_IRUSR | stat.S_IWUSR)
    temporary.replace(path)
    if os.name != "nt":
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)


@dataclass(frozen=True)
class CliConfig:
    api_url: str = ""
    aws_region: str = "us-east-2"
    cognito_user_pool_id: str = ""
    cognito_client_id: str = ""
    stack_name: str = DEFAULT_STACK_NAME
    aws_profile: str | None = None

    @property
    def is_configured(self) -> bool:
        return bool(self.api_url and self.cognito_user_pool_id and self.cognito_client_id)

    def with_environment(self) -> "CliConfig":
        values = asdict(self)
        overrides = {
            "api_url": os.environ.get("IMAGETRACKER_API_URL"),
            "aws_region": os.environ.get("IMAGETRACKER_AWS_REGION"),
            "cognito_user_pool_id": os.environ.get("IMAGETRACKER_COGNITO_USER_POOL_ID"),
            "cognito_client_id": os.environ.get("IMAGETRACKER_COGNITO_CLIENT_ID"),
            "stack_name": os.environ.get("IMAGETRACKER_STACK_NAME"),
            "aws_profile": os.environ.get("AWS_PROFILE"),
        }
        for key, value in overrides.items():
            if value:
                values[key] = value
        values["api_url"] = str(values["api_url"]).rstrip("/")
        return CliConfig(**values)


class ConfigStore:
    def __init__(self, directory: Path | None = None):
        self.directory = directory or default_config_dir()
        self.path = self.directory / "config.json"

    @property
    def state_path(self) -> Path:
        """Legacy unscoped path retained only for explicit migration tooling."""

        return self.directory / "state.sqlite3"

    def state_path_for_subject(self, subject: str) -> Path:
        if not subject.strip():
            raise ValueError("A Cognito subject is required for account-local state")
        digest = hashlib.sha256(f"imagetracker-state-v1:{subject}".encode("utf-8")).hexdigest()
        return self.directory / "accounts" / digest / "state.sqlite3"

    @property
    def legacy_preview_state_path(self) -> Path:
        return self.directory / "legacy-preview.sqlite3"

    @property
    def fallback_token_path(self) -> Path:
        return self.directory / "credentials.json"

    def load(self) -> CliConfig:
        if not self.path.exists():
            return CliConfig().with_environment()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read ImageTracker configuration at {self.path}: {exc}") from exc
        allowed = {field for field in CliConfig.__dataclass_fields__}
        values = {key: value for key, value in raw.items() if key in allowed}
        return CliConfig(**values).with_environment()

    def save(self, config: CliConfig) -> None:
        write_private_json(self.path, {key: value for key, value in asdict(config).items() if value is not None})


def config_from_stack(
    client: StackClient,
    *,
    stack_name: str,
    region: str,
    profile: str | None = None,
) -> CliConfig:
    response = client.describe_stacks(StackName=stack_name)
    stacks = response.get("Stacks") or []
    if len(stacks) != 1:
        raise ValueError(f"CloudFormation stack {stack_name!r} was not found")
    outputs = {
        item.get("OutputKey"): item.get("OutputValue")
        for item in stacks[0].get("Outputs", [])
        if item.get("OutputKey") and item.get("OutputValue")
    }
    missing = [name for name in REQUIRED_STACK_OUTPUTS if name not in outputs]
    if missing:
        raise ValueError(f"Stack {stack_name!r} is missing outputs: {', '.join(sorted(missing))}")
    values = {field: str(outputs[output]) for output, field in REQUIRED_STACK_OUTPUTS.items()}
    values["api_url"] = values["api_url"].rstrip("/")
    return CliConfig(
        **values,
        aws_region=region,
        stack_name=stack_name,
        aws_profile=profile,
    )
