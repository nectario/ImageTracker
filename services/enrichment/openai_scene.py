from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import json
import re
import socket
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from services.enrichment.models import (
    ProviderFailure,
    ProviderFailureClass,
)


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_SCENE_PROVIDER = "OpenAI"
SCENE_DESCRIPTION_PROMPT_VERSION = "scene-search-v1"
DEFAULT_SCENE_DESCRIPTION_MODEL = "gpt-5.6-terra"
USD_QUANTUM = Decimal("0.000001")
MAX_SAFE_USAGE_TOKENS = 10_000_000
SCENE_HIGH_DETAIL_1024_MAX_IMAGE_TOKENS = 2_048
SCENE_PROMPT_MAX_INPUT_TOKENS = 512
SCENE_MAX_BILLABLE_INPUT_TOKENS = (
    SCENE_HIGH_DETAIL_1024_MAX_IMAGE_TOKENS + SCENE_PROMPT_MAX_INPUT_TOKENS
)
SCENE_MAX_OUTPUT_TOKENS = 100


@dataclass(frozen=True)
class SceneDescriptionResult:
    """A search-oriented description and safe provider metadata."""

    description: str
    provider: str
    model: str
    prompt_version: str
    usage: Mapping[str, int] | None = None


def sanitize_scene_description_usage(value: Any) -> dict[str, int] | None:
    """Return only bounded billing fields from an untrusted provider payload."""

    if not isinstance(value, Mapping):
        return None
    safe: dict[str, int] = {}

    def add(target: str, candidate: Any) -> None:
        if (
            isinstance(candidate, int)
            and not isinstance(candidate, bool)
            and 0 <= candidate <= MAX_SAFE_USAGE_TOKENS
        ):
            safe[target] = candidate

    add("input_tokens", value.get("input_tokens"))
    add("output_tokens", value.get("output_tokens"))
    add("total_tokens", value.get("total_tokens"))
    input_details = value.get("input_tokens_details")
    if isinstance(input_details, Mapping):
        add("cached_input_tokens", input_details.get("cached_tokens"))
    else:
        add("cached_input_tokens", value.get("cached_input_tokens"))
    output_details = value.get("output_tokens_details")
    if isinstance(output_details, Mapping):
        add("reasoning_output_tokens", output_details.get("reasoning_tokens"))
    else:
        add("reasoning_output_tokens", value.get("reasoning_output_tokens"))
    return safe or None


def scene_description_cost_usd(
    usage: Mapping[str, Any] | None,
    *,
    input_usd_per_million: Decimal,
    cached_input_usd_per_million: Decimal,
    output_usd_per_million: Decimal,
) -> tuple[Decimal, dict[str, int]] | None:
    """Calculate a conservative six-decimal USD charge from Responses usage.

    OpenAI reports cached tokens inside ``input_tokens``. Cached input is
    therefore subtracted before applying the ordinary input rate, then billed
    once at the cached rate. Output tokens already include reasoning tokens.
    """

    safe = sanitize_scene_description_usage(usage)
    if safe is None or "input_tokens" not in safe or "output_tokens" not in safe:
        return None
    input_tokens = safe["input_tokens"]
    cached_tokens = safe.get("cached_input_tokens", 0)
    if (
        cached_tokens > input_tokens
        or input_tokens > SCENE_MAX_BILLABLE_INPUT_TOKENS
        or safe["output_tokens"] > SCENE_MAX_OUTPUT_TOKENS
    ):
        return None
    rates = (
        input_usd_per_million,
        cached_input_usd_per_million,
        output_usd_per_million,
    )
    if any(not rate.is_finite() or rate < 0 for rate in rates):
        raise ValueError("Scene-description rates must be finite and non-negative")
    million = Decimal("1000000")
    amount = (
        Decimal(input_tokens - cached_tokens) * input_usd_per_million
        + Decimal(cached_tokens) * cached_input_usd_per_million
        + Decimal(safe["output_tokens"]) * output_usd_per_million
    ) / million
    return amount.quantize(USD_QUANTUM, rounding=ROUND_CEILING), safe


def scene_description_usage_within_bounds(
    usage: Mapping[str, Any] | None,
) -> bool:
    safe = sanitize_scene_description_usage(usage)
    if safe is None:
        return True
    input_tokens = safe.get("input_tokens")
    output_tokens = safe.get("output_tokens")
    cached_tokens = safe.get("cached_input_tokens", 0)
    if input_tokens is None or output_tokens is None:
        return True
    return (
        cached_tokens <= input_tokens <= SCENE_MAX_BILLABLE_INPUT_TOKENS
        and output_tokens <= SCENE_MAX_OUTPUT_TOKENS
    )


def scene_description_maximum_cost_usd(
    *,
    input_usd_per_million: Decimal,
    cached_input_usd_per_million: Decimal,
    output_usd_per_million: Decimal,
) -> Decimal:
    """Bound one 1024px/high request including prompt and 100 output tokens."""

    input_rate = max(input_usd_per_million, cached_input_usd_per_million)
    return (
        (
            Decimal(SCENE_MAX_BILLABLE_INPUT_TOKENS) * input_rate
            + Decimal(SCENE_MAX_OUTPUT_TOKENS) * output_usd_per_million
        )
        / Decimal("1000000")
    ).quantize(USD_QUANTUM, rounding=ROUND_CEILING)


class SceneDescriptionProviderError(RuntimeError):
    """A sanitized provider failure safe to persist or display."""

    def __init__(self, failure: ProviderFailure, *, provider_called: bool = True) -> None:
        super().__init__(failure.user_message)
        self.failure = failure
        self.provider_called = provider_called


@dataclass(frozen=True)
class JsonHttpResponse:
    status_code: int
    payload: Mapping[str, Any] | None
    error_code: str | None = None
    retry_after_seconds: int | None = None


class JsonHttpTransport(Protocol):
    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> JsonHttpResponse: ...


def _failure(
    failure_class: ProviderFailureClass,
    code: str,
    message: str,
    *,
    retryable: bool,
    provider_called: bool = True,
) -> SceneDescriptionProviderError:
    return SceneDescriptionProviderError(
        ProviderFailure(
            failure_class=failure_class,
            code=code,
            user_message=message,
            retryable=retryable,
        ),
        provider_called=provider_called,
    )


class UrllibJsonTransport:
    """Small Responses API transport that discards provider error bodies."""

    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        request = Request(
            url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers=dict(headers),
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                status_code = int(response.status)
                body = response.read()
        except HTTPError as exc:
            # The body can contain provider diagnostics or request material and
            # is never returned or attached to an exception. Only the bounded
            # machine error code and Retry-After value are retained for safe
            # rate-limit versus billing-quota classification.
            error_code = None
            try:
                error_body = exc.read(16_384)
                decoded_error = json.loads(error_body.decode("utf-8"))
                error = decoded_error.get("error") if isinstance(decoded_error, Mapping) else None
                if isinstance(error, Mapping):
                    candidate = error.get("code") or error.get("type")
                    if isinstance(candidate, str) and re.fullmatch(
                        r"[A-Za-z0-9_.-]{1,64}", candidate
                    ):
                        error_code = candidate
            except (UnicodeDecodeError, json.JSONDecodeError, OSError):
                error_code = None
            retry_after = None
            candidate_retry = exc.headers.get("Retry-After") if exc.headers else None
            if isinstance(candidate_retry, str) and candidate_retry.isdigit():
                retry_after = min(3600, int(candidate_retry))
            return JsonHttpResponse(
                status_code=int(exc.code),
                payload=None,
                error_code=error_code,
                retry_after_seconds=retry_after,
            )
        except (URLError, TimeoutError, socket.timeout, OSError):
            raise _failure(
                ProviderFailureClass.TRANSIENT,
                "OpenAITransportError",
                "Scene description is temporarily unavailable.",
                retryable=True,
            ) from None

        if not body:
            return JsonHttpResponse(status_code=status_code, payload=None)
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return JsonHttpResponse(status_code=status_code, payload=None)
        return JsonHttpResponse(
            status_code=status_code,
            payload=decoded if isinstance(decoded, Mapping) else None,
        )


class OpenAISceneDescriptionProvider:
    """Generate a concise scene description from a short-lived HTTPS preview."""

    provider = OPENAI_SCENE_PROVIDER
    prompt_version = SCENE_DESCRIPTION_PROMPT_VERSION

    def __init__(
        self,
        api_key: str,
        *,
        model: str = DEFAULT_SCENE_DESCRIPTION_MODEL,
        detail: str = "high",
        service_tier: str = "flex",
        max_words: int = 24,
        transport: JsonHttpTransport | None = None,
        timeout_seconds: float = 60.0,
        api_key_loader: Callable[[], str] | None = None,
        api_key_invalidator: Callable[[], None] | None = None,
    ) -> None:
        cleaned_key = api_key.strip()
        cleaned_model = model.strip()
        if not cleaned_key:
            raise ValueError("An OpenAI API key is required")
        if not cleaned_model:
            raise ValueError("An OpenAI model is required")
        if detail not in {"low", "high"}:
            raise ValueError("detail must be 'low' or 'high'")
        if service_tier not in {"auto", "default", "flex"}:
            raise ValueError("service_tier must be 'auto', 'default', or 'flex'")
        if isinstance(max_words, bool) or not isinstance(max_words, int):
            raise ValueError("max_words must be an integer from 1 through 24")
        if not 1 <= max_words <= 24:
            raise ValueError("max_words must be an integer from 1 through 24")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._api_key = cleaned_key
        self._model = cleaned_model
        self._detail = detail
        self._service_tier = service_tier
        self._max_words = max_words
        self._transport = transport or UrllibJsonTransport()
        self._timeout_seconds = timeout_seconds
        self._api_key_loader = api_key_loader
        self._api_key_invalidator = api_key_invalidator

    @property
    def model(self) -> str:
        return self._model

    @property
    def detail(self) -> str:
        return self._detail

    @property
    def service_tier(self) -> str:
        return self._service_tier

    @property
    def max_words(self) -> int:
        return self._max_words

    def describe(self, preview_url: str) -> SceneDescriptionResult:
        self._validate_preview_url(preview_url)
        api_key = self._current_api_key()
        response = self._transport.post_json(
            OPENAI_RESPONSES_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            payload={
                "model": self._model,
                "service_tier": self._service_tier,
                "reasoning": {"effort": "none"},
                "store": False,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": self._prompt()},
                            {
                                "type": "input_image",
                                "image_url": preview_url,
                                "detail": self._detail,
                            },
                        ],
                    }
                ],
                "max_output_tokens": SCENE_MAX_OUTPUT_TOKENS,
            },
            timeout_seconds=self._timeout_seconds,
        )
        try:
            self._raise_for_http_status(response)
        except SceneDescriptionProviderError as exc:
            if exc.failure.failure_class is ProviderFailureClass.AUTHENTICATION:
                if self._api_key_invalidator is not None:
                    self._api_key_invalidator()
                self._api_key = ""
            raise
        if response.payload is None:
            raise self._invalid_response()

        description = self._extract_output_text(response.payload)
        normalized = self._normalize_description(description)
        if normalized is None:
            raise self._invalid_response()

        return SceneDescriptionResult(
            description=normalized,
            provider=self.provider,
            model=self._model,
            prompt_version=self.prompt_version,
            usage=sanitize_scene_description_usage(response.payload.get("usage")),
        )

    def _current_api_key(self) -> str:
        if self._api_key:
            return self._api_key
        if self._api_key_loader is None:
            raise _failure(
                ProviderFailureClass.AUTHENTICATION,
                "OpenAICredentialUnavailable",
                "Scene description is unavailable because its provider credential could not be loaded.",
                retryable=False,
                provider_called=False,
            )
        try:
            loaded = self._api_key_loader().strip()
        except Exception:
            loaded = ""
        if not loaded:
            raise _failure(
                ProviderFailureClass.AUTHENTICATION,
                "OpenAICredentialUnavailable",
                "Scene description is unavailable because its provider credential could not be loaded.",
                retryable=False,
                provider_called=False,
            )
        self._api_key = loaded
        return loaded

    def _prompt(self) -> str:
        return (
            "Write exactly one natural sentence of no more than "
            f"{self._max_words} words, optimized for photo search. Mention the "
            "setting, activity, distinctive objects, and clearly visible text only "
            "when important. Describe only visible evidence. Never identify people, "
            "infer sensitive attributes, or invent an address. Never reproduce contact "
            "names, phone numbers, email addresses, or account identifiers. Return only "
            "the sentence."
        )

    @staticmethod
    def _validate_preview_url(preview_url: str) -> None:
        if not isinstance(preview_url, str):
            raise ValueError("preview_url must be a short-lived HTTPS URL")
        parsed = urlsplit(preview_url)
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or bool(parsed.fragment)
        ):
            raise ValueError("preview_url must be a short-lived HTTPS URL")

    @staticmethod
    def _extract_output_text(payload: Mapping[str, Any]) -> str | None:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct

        outputs = payload.get("output")
        if not isinstance(outputs, Sequence) or isinstance(outputs, (str, bytes)):
            return None
        for output in outputs:
            if not isinstance(output, Mapping):
                continue
            contents = output.get("content")
            if not isinstance(contents, Sequence) or isinstance(contents, (str, bytes)):
                continue
            for content in contents:
                if not isinstance(content, Mapping):
                    continue
                text = content.get("text")
                if isinstance(text, str) and text.strip():
                    return text
        return None

    def _normalize_description(self, value: str | None) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = re.sub(r"\s+", " ", value).strip()
        if not cleaned:
            return None
        if re.search(r"[.!?][\"')\]]*\s+\S", cleaned):
            return None
        words = cleaned.split()
        if len(words) > self._max_words:
            cleaned = " ".join(words[: self._max_words]).rstrip(".!?")
        if cleaned[-1] not in ".!?":
            cleaned += "."
        return cleaned

    @staticmethod
    def _sanitize_usage(value: Any) -> Mapping[str, int] | None:
        return sanitize_scene_description_usage(value)

    @staticmethod
    def _raise_for_http_status(response: JsonHttpResponse) -> None:
        status_code = response.status_code
        if 200 <= status_code < 300:
            return
        if status_code in {401, 403}:
            raise _failure(
                ProviderFailureClass.AUTHENTICATION,
                "OpenAIAuthenticationFailed",
                "Scene description is unavailable because its provider credential was rejected.",
                retryable=False,
            )
        if status_code == 429:
            if (response.error_code or "").casefold() not in {
                "insufficient_quota",
                "billing_hard_limit_reached",
            }:
                raise _failure(
                    ProviderFailureClass.TRANSIENT,
                    "OpenAIRateLimited",
                    "Scene description is temporarily rate limited and will retry.",
                    retryable=True,
                )
            raise _failure(
                ProviderFailureClass.QUOTA,
                "OpenAIQuotaDeferred",
                "Scene description is waiting for provider quota.",
                retryable=False,
            )
        if status_code == 408 or status_code >= 500:
            raise _failure(
                ProviderFailureClass.TRANSIENT,
                "OpenAIServiceUnavailable",
                "Scene description is temporarily unavailable.",
                retryable=True,
            )
        raise _failure(
            ProviderFailureClass.INTERNAL,
            "OpenAIRequestRejected",
            "Scene description could not be completed.",
            retryable=False,
        )

    @staticmethod
    def _invalid_response() -> SceneDescriptionProviderError:
        return _failure(
            ProviderFailureClass.INTERNAL,
            "OpenAIInvalidResponse",
            "The scene description provider returned an invalid response.",
            retryable=False,
        )
