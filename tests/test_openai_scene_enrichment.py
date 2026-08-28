from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

import pytest

import services.enrichment.openai_scene as openai_scene_module
from services.enrichment.models import ProviderFailureClass
from services.enrichment.openai_scene import (
    DEFAULT_SCENE_DESCRIPTION_MODEL,
    OPENAI_RESPONSES_URL,
    OPENAI_SCENE_PROVIDER,
    SCENE_DESCRIPTION_PROMPT_VERSION,
    JsonHttpResponse,
    OpenAISceneDescriptionProvider,
    SceneDescriptionProviderError,
    UrllibJsonTransport,
)
from services.enrichment.openai_secrets import (
    OpenAIApiKeyResolver,
    OpenAISecretConfigurationError,
    parse_openai_api_key,
)


class FakeTransport:
    def __init__(self, response: JsonHttpResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        return self.response


def test_scene_provider_uses_responses_api_https_preview_and_safe_metadata() -> None:
    transport = FakeTransport(
        JsonHttpResponse(
            status_code=200,
            payload={
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "A child rides a red bicycle beside a sunny lakeside path",
                            }
                        ],
                    }
                ],
                "usage": {
                    "input_tokens": 812,
                    "output_tokens": 14,
                    "total_tokens": 826,
                    "input_tokens_details": {"cached_tokens": 64, "secret": "omit"},
                    "output_tokens_details": {"reasoning_tokens": 3},
                    "provider_debug": "omit",
                },
            },
        )
    )
    provider = OpenAISceneDescriptionProvider("never-display", transport=transport)
    preview_url = "https://preview.example.test/private/object.jpg?sig=short-lived"

    result = provider.describe(preview_url)

    assert result.description == "A child rides a red bicycle beside a sunny lakeside path."
    assert result.provider == OPENAI_SCENE_PROVIDER
    assert result.model == DEFAULT_SCENE_DESCRIPTION_MODEL
    assert result.prompt_version == SCENE_DESCRIPTION_PROMPT_VERSION
    assert result.usage == {
        "input_tokens": 812,
        "output_tokens": 14,
        "total_tokens": 826,
        "cached_input_tokens": 64,
        "reasoning_output_tokens": 3,
    }

    [call] = transport.calls
    assert call["url"] == OPENAI_RESPONSES_URL
    assert call["timeout_seconds"] == 60.0
    assert call["headers"]["Authorization"] == "Bearer never-display"
    payload = call["payload"]
    assert payload["model"] == "gpt-5.6-sol"
    assert payload["service_tier"] == "flex"
    assert payload["reasoning"] == {"effort": "none"}
    assert payload["store"] is False
    content = payload["input"][0]["content"]
    assert content[1] == {
        "type": "input_image",
        "image_url": preview_url,
        "detail": "high",
    }
    prompt = content[0]["text"]
    assert "no more than 24 words" in prompt
    assert "Never identify people" in prompt
    assert "infer sensitive attributes" in prompt
    assert "invent an address" in prompt
    assert "data:image" not in json.dumps(payload)


def test_scene_provider_accepts_output_text_convenience_field() -> None:
    provider = OpenAISceneDescriptionProvider(
        "key",
        model="later-more-accurate-model",
        transport=FakeTransport(
            JsonHttpResponse(status_code=200, payload={"output_text": "Snow on a city street."})
        ),
    )

    result = provider.describe("https://example.test/preview.jpg?expires=1")

    assert result.description == "Snow on a city street."
    assert result.model == "later-more-accurate-model"
    assert result.usage is None


@pytest.mark.parametrize(
    "preview_url",
    [
        "http://example.test/preview.jpg",
        "data:image/jpeg;base64,private-original-bytes",
        "file:///private/photo.jpg",
        "https://user:password@example.test/preview.jpg",
        "https://example.test/preview.jpg#fragment",
        "",
    ],
)
def test_scene_provider_requires_short_lived_https_preview(preview_url: str) -> None:
    transport = FakeTransport(JsonHttpResponse(status_code=200, payload={}))
    provider = OpenAISceneDescriptionProvider("secret", transport=transport)

    with pytest.raises(ValueError, match="short-lived HTTPS URL"):
        provider.describe(preview_url)

    assert transport.calls == []


@pytest.mark.parametrize(
    ("status_code", "provider_error_code", "failure_class", "code", "retryable"),
    [
        (401, None, ProviderFailureClass.AUTHENTICATION, "OpenAIAuthenticationFailed", False),
        (403, None, ProviderFailureClass.AUTHENTICATION, "OpenAIAuthenticationFailed", False),
        (429, None, ProviderFailureClass.TRANSIENT, "OpenAIRateLimited", True),
        (
            429,
            "insufficient_quota",
            ProviderFailureClass.QUOTA,
            "OpenAIQuotaDeferred",
            False,
        ),
        (408, None, ProviderFailureClass.TRANSIENT, "OpenAIServiceUnavailable", True),
        (500, None, ProviderFailureClass.TRANSIENT, "OpenAIServiceUnavailable", True),
        (503, None, ProviderFailureClass.TRANSIENT, "OpenAIServiceUnavailable", True),
        (400, None, ProviderFailureClass.INTERNAL, "OpenAIRequestRejected", False),
    ],
)
def test_scene_provider_classifies_http_failures_without_leaking_provider_body(
    status_code: int,
    provider_error_code: str | None,
    failure_class: ProviderFailureClass,
    code: str,
    retryable: bool,
) -> None:
    leaked = "provider-body-with-secret"
    provider = OpenAISceneDescriptionProvider(
        "api-key-must-not-leak",
        transport=FakeTransport(
            JsonHttpResponse(
                status_code=status_code,
                payload={"error": leaked},
                error_code=provider_error_code,
            )
        ),
    )

    with pytest.raises(SceneDescriptionProviderError) as raised:
        provider.describe("https://example.test/preview.jpg?sig=redacted")

    assert raised.value.failure.failure_class is failure_class
    assert raised.value.failure.code == code
    assert raised.value.failure.retryable is retryable
    assert leaked not in str(raised.value)
    assert "api-key-must-not-leak" not in str(raised.value)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"output": "not-a-list"},
        {"output_text": ""},
    ],
)
def test_invalid_scene_response_is_terminal(
    payload: Mapping[str, Any] | None,
) -> None:
    provider = OpenAISceneDescriptionProvider(
        "secret",
        transport=FakeTransport(JsonHttpResponse(status_code=200, payload=payload)),
    )

    with pytest.raises(SceneDescriptionProviderError) as raised:
        provider.describe("https://example.test/preview.jpg?sig=redacted")

    assert raised.value.failure.failure_class is ProviderFailureClass.INTERNAL
    assert raised.value.failure.code == "OpenAIInvalidResponse"
    assert raised.value.failure.retryable is False


def test_overlong_scene_response_is_clipped_to_preserve_paid_result() -> None:
    words = [f"word{index}" for index in range(1, 28)]
    provider = OpenAISceneDescriptionProvider(
        "secret",
        transport=FakeTransport(
            JsonHttpResponse(status_code=200, payload={"output_text": " ".join(words)})
        ),
    )

    result = provider.describe("https://example.test/preview.jpg?sig=redacted")

    assert result.description == " ".join(words[:24]) + "."
    assert len(result.description.rstrip(".").split()) == 24


def test_multiple_sentence_scene_response_is_terminal() -> None:
    provider = OpenAISceneDescriptionProvider(
        "secret",
        transport=FakeTransport(
            JsonHttpResponse(
                status_code=200,
                payload={"output_text": "A dog runs through snow. A cabin is behind it."},
            )
        ),
    )

    with pytest.raises(SceneDescriptionProviderError) as raised:
        provider.describe("https://example.test/preview.jpg?sig=redacted")

    assert raised.value.failure.code == "OpenAIInvalidResponse"
    assert raised.value.failure.retryable is False


def test_network_failure_is_transient_and_sanitized(monkeypatch) -> None:
    secret = "api-key-must-not-leak"

    def fail_urlopen(*args: Any, **kwargs: Any) -> None:
        raise OSError(f"network failed while handling {secret}")

    monkeypatch.setattr(openai_scene_module, "urlopen", fail_urlopen)
    provider = OpenAISceneDescriptionProvider(
        secret,
        transport=UrllibJsonTransport(),
    )

    with pytest.raises(SceneDescriptionProviderError) as raised:
        provider.describe("https://example.test/preview.jpg?sig=redacted")

    assert raised.value.failure.failure_class is ProviderFailureClass.TRANSIENT
    assert raised.value.failure.code == "OpenAITransportError"
    assert raised.value.failure.retryable is True
    assert secret not in str(raised.value)


def test_authentication_failure_invalidates_key_for_warm_worker_retry() -> None:
    class RotatingTransport:
        def __init__(self) -> None:
            self.authorizations: list[str] = []

        def post_json(self, _url, *, headers, payload, timeout_seconds):
            del payload, timeout_seconds
            self.authorizations.append(headers["Authorization"])
            if len(self.authorizations) == 1:
                return JsonHttpResponse(status_code=401, payload=None)
            return JsonHttpResponse(
                status_code=200,
                payload={"output_text": "A lighthouse stands above a rocky coast."},
            )

    transport = RotatingTransport()
    invalidations: list[bool] = []
    provider = OpenAISceneDescriptionProvider(
        "old-key",
        transport=transport,
        api_key_loader=lambda: "new-key",
        api_key_invalidator=lambda: invalidations.append(True),
    )

    with pytest.raises(SceneDescriptionProviderError):
        provider.describe("https://example.test/preview.jpg?sig=one")
    result = provider.describe("https://example.test/preview.jpg?sig=two")

    assert invalidations == [True]
    assert transport.authorizations == ["Bearer old-key", "Bearer new-key"]
    assert result.description.startswith("A lighthouse")


@pytest.mark.parametrize(
    "raw",
    [
        "plain-key",
        '"json-string-key"',
        '{"api_key":"api-key-value"}',
        '{"key":"key-value"}',
        '{"OPENAI_API_KEY":"environment-shape"}',
    ],
)
def test_openai_secret_shapes(raw: str) -> None:
    expected = {
        "plain-key": "plain-key",
        '"json-string-key"': "json-string-key",
        '{"api_key":"api-key-value"}': "api-key-value",
        '{"key":"key-value"}': "key-value",
        '{"OPENAI_API_KEY":"environment-shape"}': "environment-shape",
    }[raw]
    assert parse_openai_api_key(raw) == expected


class FakeSsm:
    def __init__(self, value: str = '{"api_key":"ssm-key"}') -> None:
        self.value = value
        self.calls: list[dict[str, Any]] = []

    def get_parameter(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"Parameter": {"Value": self.value}}


def test_openai_key_resolver_precedence_is_environment_then_dotenv_then_ssm(
    tmp_path: Path,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("OPENAI_API_KEY=dotenv-key\n", encoding="utf-8")
    ssm = FakeSsm()

    environment_resolver = OpenAIApiKeyResolver(
        region_name="us-east-2",
        stage="dev",
        client=ssm,
        environment={"OPENAI_API_KEY": "environment-key"},
        dotenv_path=dotenv,
    )
    dotenv_resolver = OpenAIApiKeyResolver(
        region_name="us-east-2",
        stage="dev",
        client=ssm,
        environment={},
        dotenv_path=dotenv,
    )
    ssm_resolver = OpenAIApiKeyResolver(
        region_name="us-east-2",
        stage="dev",
        client=ssm,
        environment={},
        dotenv_path=None,
    )

    assert environment_resolver.resolve() == "environment-key"
    assert dotenv_resolver.resolve() == "dotenv-key"
    assert ssm_resolver.resolve() == "ssm-key"
    assert ssm_resolver.resolve() == "ssm-key"
    assert ssm.calls == [
        {"Name": "/imagetracker/dev/openai", "WithDecryption": True}
    ]


def test_invalid_openai_secret_never_appears_in_error_or_logs(caplog) -> None:
    secret = '{"wrong":"very-sensitive-value"}'

    with caplog.at_level(logging.DEBUG), pytest.raises(
        OpenAISecretConfigurationError
    ) as raised:
        parse_openai_api_key(secret)

    assert secret not in str(raised.value)
    assert "very-sensitive-value" not in str(raised.value)
    assert "very-sensitive-value" not in caplog.text
