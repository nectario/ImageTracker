from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from botocore.exceptions import ClientError

from services.domain.models import TemporaryObjectMetadata, TemporaryObjectUpload


class S3TemporaryObjectStore:
    """Private checksum-bound staging objects under the one-day lifecycle prefix."""

    def __init__(self, *, client: Any, bucket: str) -> None:
        if not bucket:
            raise ValueError("The ImageTracker media bucket is required")
        self._client = client
        self._bucket = bucket

    def create_presigned_put(
        self,
        *,
        user_id: UUID,
        media_asset_id: UUID,
        upload_session_id: UUID,
        checksum_sha256_base64: str,
        content_type: str,
        content_length: int,
        url_expires_at_utc: datetime,
        object_expires_at_utc: datetime,
    ) -> TemporaryObjectUpload:
        if content_type != "image/jpeg" or content_length <= 0:
            raise ValueError("Only non-empty JPEG scene previews can be staged")
        self._decode_checksum(checksum_sha256_base64)
        object_key = (
            f"staging/{user_id}/scene/{media_asset_id}/{upload_session_id}.jpg"
        )
        expires_metadata = self._utc_text(object_expires_at_utc)
        seconds = self._expiry_seconds(url_expires_at_utc)
        params = {
            "Bucket": self._bucket,
            "Key": object_key,
            "ContentType": content_type,
            "ContentLength": content_length,
            "ChecksumSHA256": checksum_sha256_base64,
            "Metadata": {"imagetracker-expires-at": expires_metadata},
        }
        url = self._client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=seconds,
            HttpMethod="PUT",
        )
        return TemporaryObjectUpload(
            bucket=self._bucket,
            object_key=object_key,
            url=url,
            headers={
                "Content-Type": content_type,
                "Content-Length": str(content_length),
                "x-amz-checksum-sha256": checksum_sha256_base64,
                "x-amz-meta-imagetracker-expires-at": expires_metadata,
            },
            expires_at_utc=self._as_utc(url_expires_at_utc),
        )

    def head_object(
        self, *, bucket: str, object_key: str
    ) -> TemporaryObjectMetadata | None:
        self._require_owned_bucket(bucket)
        try:
            response = self._client.head_object(
                Bucket=bucket,
                Key=object_key,
                ChecksumMode="ENABLED",
            )
        except ClientError as exc:
            status = (exc.response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
            code = str((exc.response.get("Error") or {}).get("Code") or "")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        checksum_base64 = str(response.get("ChecksumSHA256") or "")
        checksum_hex = self._decode_checksum(checksum_base64).hex()
        return TemporaryObjectMetadata(
            byte_size=int(response.get("ContentLength") or 0),
            content_type=str(response.get("ContentType") or ""),
            checksum_sha256_hex=checksum_hex,
        )

    def create_presigned_get(
        self, *, bucket: str, object_key: str, expires_seconds: int = 900
    ) -> str:
        self._require_owned_bucket(bucket)
        if not 60 <= expires_seconds <= 3600:
            raise ValueError("Preview GET expiry must be between 60 and 3600 seconds")
        return self._client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": object_key,
                "ResponseContentType": "image/jpeg",
            },
            ExpiresIn=expires_seconds,
            HttpMethod="GET",
        )

    def delete_object(self, *, bucket: str, object_key: str) -> None:
        self._require_owned_bucket(bucket)
        self._client.delete_object(Bucket=bucket, Key=object_key)

    def _require_owned_bucket(self, bucket: str) -> None:
        if bucket != self._bucket:
            raise ValueError("Temporary object is outside the ImageTracker bucket")

    @staticmethod
    def _decode_checksum(value: str) -> bytes:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError("Preview checksum is invalid") from exc
        if len(decoded) != 32:
            raise ValueError("Preview checksum must be SHA-256")
        return decoded

    @classmethod
    def _expiry_seconds(cls, value: datetime) -> int:
        delta = int((cls._as_utc(value) - datetime.now(timezone.utc)).total_seconds())
        return max(60, min(900, delta))

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _utc_text(cls, value: datetime) -> str:
        return cls._as_utc(value).isoformat().replace("+00:00", "Z")
