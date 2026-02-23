from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import quote

import requests


class GraphApiError(RuntimeError):
    pass


class GraphClient:
    def __init__(self, access_token: str, session: Optional[requests.Session] = None):
        self._access_token = access_token
        self._session = session or requests.Session()
        self._base_url = "https://graph.microsoft.com/v1.0"

    def get(self, path_or_url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self._base_url}{path_or_url}"
        response = self._session.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {self._access_token}"},
            timeout=30,
        )
        if response.status_code >= 400:
            raise GraphApiError(f"Graph request failed ({response.status_code}): {response.text}")
        return response.json()

    def get_bytes(self, url: str) -> bytes:
        response = self._session.get(url, timeout=30)
        if response.status_code >= 400:
            raise GraphApiError(f"Thumbnail request failed ({response.status_code}): {response.text}")
        return response.content

    def resolve_folder_by_path(self, folder_path: str) -> Dict[str, Any]:
        normalized = folder_path if folder_path.startswith("/") else f"/{folder_path}"
        encoded_path = quote(normalized)
        return self.get(f"/me/drive/root:{encoded_path}")

    def get_folder_delta(self, folder_drive_item_id: str) -> Dict[str, Any]:
        return self.get(f"/me/drive/items/{folder_drive_item_id}/delta")

    def get_thumbnails(self, drive_item_id: str) -> Dict[str, Any]:
        return self.get(f"/me/drive/items/{drive_item_id}/thumbnails")
