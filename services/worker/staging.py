from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    ParamValidationError,
    PartialCredentialsError,
)

from services.enrichment.models import ProviderFailureClass


@dataclass(frozen=True)
class ScenePreviewStoreFailure:
    failure_class: ProviderFailureClass
    code: str
    user_message: str
    retryable: bool


class ScenePreviewStoreError(RuntimeError):
    """A sanitized S3 staging error safe to persist and classify."""

    def __init__(self, failure: ScenePreviewStoreFailure) -> None:
        super().__init__(failure.user_message)
        self.failure = failure


def _error(
    failure_class: ProviderFailureClass,
    code: str,
    message: str,
    *,
    retryable: bool,
) -> ScenePreviewStoreError:
    return ScenePreviewStoreError(
        ScenePreviewStoreFailure(
            failure_class=failure_class,
            code=code,
            user_message=message,
            retryable=retryable,
        )
    )


class S3ScenePreviewStore:
    """Create short-lived read URLs and remove disposable scene previews."""

    def __init__(self, client: Any, *, allowed_bucket: str) -> None:
        if not isinstance(allowed_bucket, str) or not allowed_bucket.strip():
            raise ValueError("A staging bucket is required")
        self._client = client
        self._allowed_bucket = allowed_bucket.strip()

    def create_presigned_get_url(
        self,
        *,
        bucket: str,
        object_key: str,
        expires_seconds: int,
    ) -> str:
        if bucket != self._allowed_bucket or not object_key:
            raise _error(
                ProviderFailureClass.INTERNAL,
                "InvalidScenePreviewReference",
                "The staged scene preview reference is invalid.",
                retryable=False,
            )
        if isinstance(expires_seconds, bool) or not 1 <= expires_seconds <= 900:
            raise ValueError("expires_seconds must be from 1 through 900")
        try:
            url = self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": object_key},
                ExpiresIn=expires_seconds,
                HttpMethod="GET",
            )
        except (NoCredentialsError, PartialCredentialsError):
            raise _error(
                ProviderFailureClass.AUTHENTICATION,
                "ScenePreviewCredentialUnavailable",
                "Scene description is unavailable because preview access could not be authorized.",
                retryable=False,
            ) from None
        except ParamValidationError:
            raise _error(
                ProviderFailureClass.INTERNAL,
                "InvalidScenePreviewReference",
                "The staged scene preview reference is invalid.",
                retryable=False,
            ) from None
        except (ClientError, BotoCoreError):
            raise _error(
                ProviderFailureClass.TRANSIENT,
                "ScenePreviewTemporarilyUnavailable",
                "The staged scene preview is temporarily unavailable.",
                retryable=True,
            ) from None
        except Exception:
            # Never propagate SDK messages: they can contain object keys or URLs.
            raise _error(
                ProviderFailureClass.INTERNAL,
                "ScenePreviewAccessFailed",
                "The staged scene preview could not be accessed.",
                retryable=False,
            ) from None

        if not isinstance(url, str) or not _is_safe_https_url(url):
            raise _error(
                ProviderFailureClass.INTERNAL,
                "InvalidScenePreviewUrl",
                "The staged scene preview could not be accessed securely.",
                retryable=False,
            )
        return url

    def delete_object(self, *, bucket: str, object_key: str) -> None:
        if bucket != self._allowed_bucket or not object_key:
            raise _error(
                ProviderFailureClass.INTERNAL,
                "InvalidScenePreviewReference",
                "The staged scene preview reference is invalid.",
                retryable=False,
            )
        try:
            self._client.delete_object(Bucket=bucket, Key=object_key)
        except Exception:
            # Cleanup is best-effort after a durable job transition. Surface a
            # sanitized exception so the processor can rely on bucket lifecycle
            # cleanup without retrying a completed paid provider call.
            raise _error(
                ProviderFailureClass.TRANSIENT,
                "ScenePreviewCleanupDeferred",
                "The staged scene preview will be cleaned up later.",
                retryable=True,
            ) from None


def _is_safe_https_url(url: str) -> bool:
    parsed = urlsplit(url)
    return (
        parsed.scheme.lower() == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
    )


__all__ = [
    "S3ScenePreviewStore",
    "ScenePreviewStoreError",
    "ScenePreviewStoreFailure",
]
