from __future__ import annotations

import os
from pathlib import Path
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Protocol

import httpx

from .auth import TokenSet


class SessionProvider(Protocol):
    def current_tokens(self, *, refresh_if_needed: bool = True) -> TokenSet | None: ...
    def refresh(self, tokens: TokenSet) -> TokenSet: ...


@dataclass(frozen=True)
class ApiProblem:
    status: int
    title: str
    detail: str
    code: str | None = None
    request_id: str | None = None


class ApiError(RuntimeError):
    def __init__(self, problem: ApiProblem):
        self.problem = problem
        message = problem.detail or problem.title or f"ImageTracker API returned HTTP {problem.status}"
        if problem.request_id:
            message = f"{message} (request {problem.request_id})"
        super().__init__(message)


class AuthenticationRequired(ApiError):
    pass


class _FileByteStream(httpx.SyncByteStream):
    def __init__(self, path: Path, *, chunk_size: int = 1024 * 1024) -> None:
        self.path = path
        self.chunk_size = chunk_size

    def __iter__(self) -> Iterator[bytes]:
        with self.path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(self.chunk_size), b""):
                yield chunk


class ApiClient:
    def __init__(
        self,
        base_url: str,
        session_provider: SessionProvider,
        *,
        http_client: httpx.Client | None = None,
        timeout_seconds: float = 30.0,
    ):
        if not base_url:
            raise ValueError("ImageTracker API URL is not configured")
        self.base_url = base_url.rstrip("/")
        self.sessions = session_provider
        self.http = http_client or httpx.Client(timeout=timeout_seconds)
        self._owns_client = http_client is None

    def close(self) -> None:
        if self._owns_client:
            self.http.close()

    def __enter__(self) -> "ApiClient":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        retry_auth: bool = True,
    ) -> Any:
        tokens = self.sessions.current_tokens()
        if not tokens:
            raise AuthenticationRequired(ApiProblem(401, "Sign-in required", "Run 'imagetracker auth login' first."))
        request_headers = {"Authorization": f"Bearer {tokens.access_token}"}
        request_headers.update(headers or {})
        try:
            response = self.http.request(
                method,
                f"{self.base_url}{path}",
                json=json,
                params=params,
                headers=request_headers,
            )
        except httpx.HTTPError as exc:
            raise ApiError(ApiProblem(0, "Network error", str(exc))) from exc
        if response.status_code == 401 and retry_auth and tokens.refresh_token:
            refreshed = self.sessions.refresh(tokens)
            request_headers["Authorization"] = f"Bearer {refreshed.access_token}"
            try:
                response = self.http.request(
                    method,
                    f"{self.base_url}{path}",
                    json=json,
                    params=params,
                    headers=request_headers,
                )
            except httpx.HTTPError as exc:
                raise ApiError(ApiProblem(0, "Network error", str(exc))) from exc
        if response.status_code >= 400:
            raise self._error_from_response(response)
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(
                ApiProblem(
                    response.status_code,
                    "Invalid API response",
                    "The service returned a response that was not JSON.",
                    request_id=response.headers.get("x-request-id"),
                )
            ) from exc

    @staticmethod
    def _error_from_response(response: httpx.Response) -> ApiError:
        payload: Mapping[str, Any]
        try:
            candidate = response.json()
            payload = candidate if isinstance(candidate, Mapping) else {}
        except ValueError:
            payload = {}
        problem = ApiProblem(
            status=response.status_code,
            title=str(payload.get("title") or payload.get("error") or response.reason_phrase),
            detail=str(payload.get("detail") or payload.get("message") or response.reason_phrase),
            code=str(payload.get("code")) if payload.get("code") is not None else None,
            request_id=response.headers.get("x-request-id") or (
                str(payload.get("requestId")) if payload.get("requestId") else None
            ),
        )
        return AuthenticationRequired(problem) if response.status_code == 401 else ApiError(problem)

    @staticmethod
    def idempotency_key(prefix: str) -> str:
        return f"{prefix}:{uuid.uuid4()}"

    def me(self) -> Mapping[str, Any]:
        return self.request("GET", "/v1/me")

    def register_device(self, payload: Mapping[str, Any], *, key: str | None = None) -> Mapping[str, Any]:
        return self.request(
            "POST",
            "/v1/devices",
            json=payload,
            headers={"Idempotency-Key": key or self.idempotency_key("device")},
        )

    def list_sources(self) -> list[Mapping[str, Any]]:
        return list(self._all_pages("/v1/sources"))

    def get_source(self, source_id: str) -> Mapping[str, Any]:
        return self.request("GET", f"/v1/sources/{source_id}")

    def create_source(self, payload: Mapping[str, Any], *, key: str | None = None) -> Mapping[str, Any]:
        return self.request(
            "POST",
            "/v1/sources",
            json=payload,
            headers={"Idempotency-Key": key or self.idempotency_key("source")},
        )

    def update_source(
        self,
        source_id: str,
        payload: Mapping[str, Any],
        *,
        key: str | None = None,
    ) -> Mapping[str, Any]:
        return self.request(
            "PATCH",
            f"/v1/sources/{source_id}",
            json=payload,
            headers={"Idempotency-Key": key or self.idempotency_key("source-update")},
        )

    def remove_source(self, source_id: str, *, key: str | None = None) -> None:
        self.request(
            "DELETE",
            f"/v1/sources/{source_id}",
            headers={"Idempotency-Key": key or self.idempotency_key("source-remove")},
        )

    def submit_manifest(
        self,
        source_id: str,
        payload: Mapping[str, Any],
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/sources/{source_id}/manifest",
            json=payload,
            headers={"Idempotency-Key": key},
        )

    def create_manifest_import(
        self,
        source_id: str,
        payload: Mapping[str, Any],
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/sources/{source_id}/manifest-imports",
            json=payload,
            headers={"Idempotency-Key": key},
        )

    def refresh_manifest_import_upload(
        self,
        source_id: str,
        import_id: str,
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/sources/{source_id}/manifest-imports/{import_id}/upload-url",
            headers={"Idempotency-Key": key},
        )

    def complete_manifest_import(
        self,
        source_id: str,
        import_id: str,
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/sources/{source_id}/manifest-imports/{import_id}/complete",
            headers={"Idempotency-Key": key},
        )

    def get_manifest_import(
        self,
        source_id: str,
        import_id: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "GET",
            f"/v1/sources/{source_id}/manifest-imports/{import_id}",
        )

    def get_manifest_import_result(
        self,
        source_id: str,
        import_id: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "GET",
            f"/v1/sources/{source_id}/manifest-imports/{import_id}/result",
        )

    def put_signed_file(
        self,
        url: str,
        path: Path,
        *,
        headers: Mapping[str, str],
    ) -> str | None:
        """Stream an artifact to a signed URL without Cognito authorization."""

        selected = path.expanduser().resolve(strict=True)
        if not selected.is_file():
            raise ValueError("Signed upload path is not a file")
        request_headers = dict(headers)
        declared_length = request_headers.get("Content-Length")
        if declared_length is not None:
            try:
                if int(declared_length) != selected.stat().st_size:
                    raise ValueError
            except ValueError as exc:
                raise ValueError(
                    "Signed upload Content-Length does not match the artifact"
                ) from exc
        else:
            request_headers["Content-Length"] = str(selected.stat().st_size)
        try:
            response = self.http.request(
                "PUT",
                url,
                content=_FileByteStream(selected),
                headers=request_headers,
            )
        except httpx.HTTPError as exc:
            raise ApiError(
                ApiProblem(
                    0,
                    "Manifest upload unavailable",
                    "The bulk manifest could not be uploaded. It will be retried.",
                    code="BULK_MANIFEST_UPLOAD_NETWORK_ERROR",
                )
            ) from exc
        if response.status_code >= 400:
            raise ApiError(
                ApiProblem(
                    response.status_code,
                    "Manifest upload rejected",
                    "Object storage rejected the bulk manifest upload.",
                    code="BULK_MANIFEST_UPLOAD_REJECTED",
                )
            )
        return response.headers.get("etag")

    def get_signed_file(
        self,
        url: str,
        destination: Path,
        *,
        expected_bytes: int,
        max_bytes: int,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, str]:
        """Atomically stream a signed result download without Cognito headers."""

        if (
            isinstance(expected_bytes, bool)
            or not isinstance(expected_bytes, int)
            or isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or not 0 < expected_bytes <= max_bytes
        ):
            raise ValueError("Signed download byte limits are invalid")
        selected = destination.expanduser().resolve(strict=False)
        selected.parent.mkdir(parents=True, exist_ok=True)
        partial = selected.with_name(f".{selected.name}.{os.getpid()}.part")
        try:
            with self.http.stream(
                "GET",
                url,
                headers=dict(headers or {}),
            ) as response:
                if response.status_code >= 400:
                    raise ApiError(
                        ApiProblem(
                            response.status_code,
                            "Manifest result download rejected",
                            "Object storage rejected the bulk result download.",
                            code="BULK_RESULT_DOWNLOAD_REJECTED",
                        )
                    )
                declared_length = response.headers.get("Content-Length")
                if declared_length is not None:
                    try:
                        parsed_length = int(declared_length)
                    except ValueError as exc:
                        raise ApiError(
                            ApiProblem(
                                0,
                                "Manifest result size invalid",
                                "Object storage returned an invalid result size.",
                                code="BULK_RESULT_DOWNLOAD_SIZE_MISMATCH",
                            )
                        ) from exc
                    if parsed_length != expected_bytes or parsed_length > max_bytes:
                        raise ApiError(
                            ApiProblem(
                                0,
                                "Manifest result size mismatch",
                                "The bulk result size did not match its declaration.",
                                code="BULK_RESULT_DOWNLOAD_SIZE_MISMATCH",
                            )
                        )
                downloaded = 0
                with partial.open("wb") as handle:
                    # S3 may label the stored gzip object with Content-Encoding.
                    # Raw iteration preserves the exact signed bytes and checksum.
                    for chunk in response.iter_raw(1024 * 1024):
                        downloaded += len(chunk)
                        if downloaded > expected_bytes or downloaded > max_bytes:
                            raise ApiError(
                                ApiProblem(
                                    0,
                                    "Manifest result size mismatch",
                                    "The bulk result exceeded its declared size.",
                                    code="BULK_RESULT_DOWNLOAD_SIZE_MISMATCH",
                                )
                            )
                        handle.write(chunk)
                if downloaded != expected_bytes:
                    raise ApiError(
                        ApiProblem(
                            0,
                            "Manifest result size mismatch",
                            "The bulk result was incomplete.",
                            code="BULK_RESULT_DOWNLOAD_SIZE_MISMATCH",
                        )
                    )
                response_headers = dict(response.headers)
            os.replace(partial, selected)
            if os.name != "nt":
                selected.chmod(0o600)
            return response_headers
        except ApiError:
            partial.unlink(missing_ok=True)
            raise
        except httpx.HTTPError as exc:
            partial.unlink(missing_ok=True)
            raise ApiError(
                ApiProblem(
                    0,
                    "Manifest result unavailable",
                    "The bulk result could not be downloaded. It will be retried.",
                    code="BULK_RESULT_DOWNLOAD_NETWORK_ERROR",
                )
            ) from exc
        except OSError:
            partial.unlink(missing_ok=True)
            raise

    def create_upload_plan(
        self,
        payload: Mapping[str, Any],
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            "/v1/uploads/plan",
            json=payload,
            headers={"Idempotency-Key": key},
        )

    def put_signed_upload(
        self,
        url: str,
        content: bytes,
        *,
        headers: Mapping[str, str],
    ) -> str | None:
        """PUT only generated preview bytes using the plan's signed headers.

        This deliberately bypasses ``request`` so Cognito authorization is
        never attached to the private object-store URL.
        """

        try:
            response = self.http.request(
                "PUT",
                url,
                content=content,
                headers=dict(headers),
            )
        except httpx.HTTPError as exc:
            raise ApiError(
                ApiProblem(
                    0,
                    "Preview upload unavailable",
                    "The temporary scene preview could not be uploaded. It will be retried.",
                    code="TEMPORARY_UPLOAD_NETWORK_ERROR",
                )
            ) from exc
        if response.status_code >= 400:
            # Never include the signed URL or provider response body; both are
            # unnecessary for the user and may contain temporary credentials.
            raise ApiError(
                ApiProblem(
                    response.status_code,
                    "Preview upload rejected",
                    "Object storage rejected the temporary scene preview.",
                    code="TEMPORARY_UPLOAD_REJECTED",
                )
            )
        return response.headers.get("etag")

    def get_upload_session(self, upload_session_id: str) -> Mapping[str, Any]:
        return self.request("GET", f"/v1/uploads/{upload_session_id}")

    def complete_upload(
        self,
        upload_session_id: str,
        payload: Mapping[str, Any],
        *,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/uploads/{upload_session_id}/complete",
            json=payload,
            headers={"Idempotency-Key": key},
        )

    def cancel_upload(
        self,
        upload_session_id: str,
        *,
        reason: str,
        key: str,
    ) -> None:
        self.request(
            "POST",
            f"/v1/uploads/{upload_session_id}/cancel",
            json={"reason": reason},
            headers={"Idempotency-Key": key},
        )

    def list_jobs(
        self,
        *,
        limit: int = 50,
        status: str | None = None,
    ) -> list[Mapping[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        page = self.request("GET", "/v1/jobs", params=params)
        return [item for item in page.get("items", []) if isinstance(item, Mapping)]

    def get_job(self, job_id: str) -> Mapping[str, Any]:
        return self.request("GET", f"/v1/jobs/{job_id}")

    def retry_job(self, job_id: str, *, key: str | None = None) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/jobs/{job_id}/retry",
            headers={"Idempotency-Key": key or self.idempotency_key("job-retry")},
        )

    def cancel_job(
        self,
        job_id: str,
        *,
        reason: str,
        key: str,
    ) -> Mapping[str, Any]:
        return self.request(
            "POST",
            f"/v1/jobs/{job_id}/cancel",
            json={"reason": reason},
            headers={"Idempotency-Key": key},
        )

    def list_media(
        self,
        device_id: str,
        *,
        limit: int = 50,
        source_id: str | None = None,
        media_type: str | None = None,
    ) -> list[Mapping[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if source_id:
            params["sourceId"] = source_id
        if media_type:
            params["mediaType"] = media_type
        page = self.request(
            "GET",
            "/v1/media",
            params=params,
            headers={"X-ImageTracker-Device-Id": device_id},
        )
        return [item for item in page.get("items", []) if isinstance(item, Mapping)]

    def get_media(self, media_asset_id: str, device_id: str) -> Mapping[str, Any]:
        return self.request(
            "GET",
            f"/v1/media/{media_asset_id}",
            headers={"X-ImageTracker-Device-Id": device_id},
        )

    def search_media(
        self,
        query: str,
        device_id: str,
        *,
        limit: int = 50,
    ) -> list[Mapping[str, Any]]:
        page = self.request(
            "GET",
            "/v1/media/search",
            params={"q": query, "limit": limit},
            headers={"X-ImageTracker-Device-Id": device_id},
        )
        return [item for item in page.get("items", []) if isinstance(item, Mapping)]

    def admin_audit(self) -> Mapping[str, Any]:
        return self.request("GET", "/v1/admin/audit")

    def _all_pages(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        max_pages: int = 1000,
    ) -> Iterable[Mapping[str, Any]]:
        query = dict(params or {})
        for _ in range(max_pages):
            page = self.request("GET", path, params=query)
            for item in page.get("items", []):
                if isinstance(item, Mapping):
                    yield item
            next_cursor = (page.get("page") or {}).get("nextCursor")
            if not next_cursor:
                return
            query["cursor"] = next_cursor
        raise ApiError(ApiProblem(0, "Pagination limit reached", f"Too many pages returned by {path}"))
