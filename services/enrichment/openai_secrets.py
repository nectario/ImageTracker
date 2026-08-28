from __future__ import annotations

import json
import os
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

import boto3


class OpenAISecretConfigurationError(ValueError):
    """A credential-resolution error that never retains the secret value."""


def parse_openai_api_key(raw_secret: str) -> str:
    """Read supported SSM/env secret shapes without exposing their contents."""

    if not isinstance(raw_secret, str) or not raw_secret.strip():
        raise OpenAISecretConfigurationError("The OpenAI credential is empty")

    candidate = raw_secret.strip()
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        if candidate.startswith(("{", "[", '"')):
            raise OpenAISecretConfigurationError(
                "The OpenAI credential contains invalid JSON"
            ) from None
        return candidate

    if isinstance(parsed, str) and parsed.strip():
        return parsed.strip()
    if not isinstance(parsed, Mapping):
        raise OpenAISecretConfigurationError(
            "The OpenAI credential has an unsupported format"
        )
    for name in ("api_key", "key", "OPENAI_API_KEY"):
        value = parsed.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise OpenAISecretConfigurationError(
        "The OpenAI credential does not contain an API key"
    )


class OpenAIApiKeyResolver:
    """Resolve OpenAI credentials from env, cwd ``.env``, then encrypted SSM.

    The resolved value is cached in memory. It is never logged, formatted into
    an exception, or written to disk by this class.
    """

    def __init__(
        self,
        *,
        region_name: str,
        stage: str,
        parameter_name: str | None = None,
        client: Any | None = None,
        environment: Mapping[str, str] | None = None,
        dotenv_path: str | Path | None = Path(".env"),
    ) -> None:
        self._region_name = region_name
        self._parameter_name = parameter_name or f"/imagetracker/{stage}/openai"
        self._client = client
        self._environment = environment if environment is not None else os.environ
        self._dotenv_path = Path(dotenv_path) if dotenv_path is not None else None
        self._api_key: str | None = None
        self._lock = RLock()

    def resolve(self) -> str:
        with self._lock:
            if self._api_key is not None:
                return self._api_key

            environment_value = self._environment.get("OPENAI_API_KEY")
            if isinstance(environment_value, str) and environment_value.strip():
                self._api_key = parse_openai_api_key(environment_value)
                return self._api_key

            dotenv_value = self._dotenv_api_key()
            if dotenv_value is not None:
                self._api_key = parse_openai_api_key(dotenv_value)
                return self._api_key

            client = self._client
            if client is None:
                client = boto3.client("ssm", region_name=self._region_name)
                self._client = client
            try:
                response = client.get_parameter(
                    Name=self._parameter_name,
                    WithDecryption=True,
                )
                raw_secret = response["Parameter"]["Value"]
            except (KeyError, TypeError):
                raise OpenAISecretConfigurationError(
                    "The OpenAI credential parameter has no value"
                ) from None
            except Exception:
                # SDK exception messages can include request details. Keep the
                # application-facing error deliberately small and sanitized.
                raise OpenAISecretConfigurationError(
                    "The OpenAI credential could not be resolved"
                ) from None

            self._api_key = parse_openai_api_key(raw_secret)
            return self._api_key

    def _dotenv_api_key(self) -> str | None:
        path = self._dotenv_path
        if path is None or not path.is_file():
            return None
        try:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        except (OSError, UnicodeDecodeError):
            return None

        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            name, separator, raw_value = line.partition("=")
            if not separator or name.strip() != "OPENAI_API_KEY":
                continue
            value = raw_value.strip()
            if value.startswith(('"', "'")):
                quote = value[0]
                closing_index = value.find(quote, 1)
                value = value[1:closing_index] if closing_index >= 1 else value[1:]
            elif " #" in value:
                value = value.split(" #", 1)[0].rstrip()
            return value or None
        return None

    def clear(self) -> None:
        with self._lock:
            self._api_key = None
