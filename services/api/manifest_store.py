from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from botocore.exceptions import ClientError


MANIFEST_CONTENT_TYPE = "application/x-ndjson"
MANIFEST_CONTENT_ENCODING = "gzip"


@dataclass(frozen=True)
class ManifestObjectUpload:
    bucket: str
    object_key: str
    url: str
    headers: dict[str, str]
    expires_at_utc: datetime


@dataclass(frozen=True)
class ManifestObjectMetadata:
    byte_size: int
    checksum_sha256_hex: str
    content_type: str
    content_encoding: str
    version_id: str | None


@dataclass(frozen=True)
class ManifestObjectDownload:
    url: str
    expires_at_utc: datetime


class S3ManifestObjectStore:
    """Checksum-bound private manifest artifacts in the retained media bucket."""

    def __init__(self, *, client: Any, bucket: str) -> None:
        if not bucket:
            raise ValueError("The ImageTracker media bucket is required")
        self._client = client
        self._bucket = bucket

    def create_input_upload(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        import_id: UUID,
        checksum_sha256_hex: str,
        content_length: int,
        expires_at_utc: datetime,
    ) -> ManifestObjectUpload:
        checksum_base64 = self._checksum_base64(checksum_sha256_hex)
        if content_length <= 0:
            raise ValueError("Manifest content length must be positive")
        object_key = (
            f"manifests/input/{user_id}/{source_id}/{import_id}.ndjson.gz"
        )
        expires = self._as_utc(expires_at_utc)
        params = {
            "Bucket": self._bucket,
            "Key": object_key,
            "ContentType": MANIFEST_CONTENT_TYPE,
            "ContentEncoding": MANIFEST_CONTENT_ENCODING,
            "ContentLength": content_length,
            "ChecksumSHA256": checksum_base64,
            "Metadata": {"imagetracker-schema-version": "ManifestNdjsonV1"},
        }
        url = self._client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=self._expiry_seconds(expires),
            HttpMethod="PUT",
        )
        return ManifestObjectUpload(
            bucket=self._bucket,
            object_key=object_key,
            url=url,
            headers={
                "Content-Type": MANIFEST_CONTENT_TYPE,
                "Content-Encoding": MANIFEST_CONTENT_ENCODING,
                "Content-Length": str(content_length),
                "x-amz-checksum-sha256": checksum_base64,
                "x-amz-meta-imagetracker-schema-version": "ManifestNdjsonV1",
            },
            expires_at_utc=expires,
        )

    def head_object(
        self, *, bucket: str, object_key: str
    ) -> ManifestObjectMetadata | None:
        self._require_owned_object(bucket, object_key)
        try:
            response = self._client.head_object(
                Bucket=bucket,
                Key=object_key,
                ChecksumMode="ENABLED",
            )
        except ClientError as exc:
            status = (exc.response.get("ResponseMetadata") or {}).get(
                "HTTPStatusCode"
            )
            code = str((exc.response.get("Error") or {}).get("Code") or "")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        checksum = self._checksum_hex(str(response.get("ChecksumSHA256") or ""))
        return ManifestObjectMetadata(
            byte_size=int(response.get("ContentLength") or 0),
            checksum_sha256_hex=checksum,
            content_type=str(response.get("ContentType") or ""),
            content_encoding=str(response.get("ContentEncoding") or ""),
            version_id=(
                str(response["VersionId"])
                if response.get("VersionId") is not None
                else None
            ),
        )

    def create_result_download(
        self,
        *,
        bucket: str,
        object_key: str,
        expires_at_utc: datetime,
    ) -> ManifestObjectDownload:
        self._require_owned_object(bucket, object_key)
        expires = self._as_utc(expires_at_utc)
        url = self._client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": object_key,
                "ResponseContentType": MANIFEST_CONTENT_TYPE,
                "ResponseContentEncoding": MANIFEST_CONTENT_ENCODING,
            },
            ExpiresIn=self._expiry_seconds(expires),
            HttpMethod="GET",
        )
        return ManifestObjectDownload(url=url, expires_at_utc=expires)

    def delete_object(self, *, bucket: str, object_key: str) -> None:
        self._require_owned_object(bucket, object_key)
        self._client.delete_object(Bucket=bucket, Key=object_key)

    def _require_owned_object(self, bucket: str, object_key: str) -> None:
        if bucket != self._bucket or not object_key.startswith("manifests/"):
            raise ValueError("Manifest object is outside ImageTracker ownership")

    @staticmethod
    def _checksum_base64(value: str) -> str:
        try:
            raw = bytes.fromhex(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("Manifest checksum must be hexadecimal SHA-256") from exc
        if len(raw) != 32:
            raise ValueError("Manifest checksum must be SHA-256")
        return base64.b64encode(raw).decode("ascii")

    @staticmethod
    def _checksum_hex(value: str) -> str:
        try:
            raw = base64.b64decode(value, validate=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("Stored manifest checksum is invalid") from exc
        if len(raw) != 32:
            raise ValueError("Stored manifest checksum must be SHA-256")
        return raw.hex()

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _expiry_seconds(cls, value: datetime) -> int:
        seconds = int(
            (cls._as_utc(value) - datetime.now(timezone.utc)).total_seconds()
        )
        return max(60, min(900, seconds))
