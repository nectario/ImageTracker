from __future__ import annotations

import json
from typing import Optional, Tuple

from imagetracker.config import Settings


class AuthRequiredError(RuntimeError):
    pass


class OneDriveAuthService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._reserved_scopes = {"offline_access", "openid", "profile"}

    def _effective_scopes(self) -> list[str]:
        scopes: list[str] = []
        seen: set[str] = set()
        for scope in self._settings.onedrive_scopes:
            normalized = scope.strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in self._reserved_scopes:
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            scopes.append(normalized)
        if not scopes:
            raise RuntimeError("ONEDRIVE_SCOPES must include at least one non-reserved scope.")
        return scopes

    def _build_app(self, token_cache_json: Optional[str]):
        try:
            import msal
        except ImportError as exc:
            raise RuntimeError("msal is required. Install dependencies first.") from exc

        cache = msal.SerializableTokenCache()
        if token_cache_json:
            cache.deserialize(token_cache_json)

        app = msal.PublicClientApplication(
            client_id=self._settings.onedrive_client_id,
            authority=f"https://login.microsoftonline.com/{self._settings.onedrive_tenant}",
            token_cache=cache,
        )
        return app, cache

    def run_device_code_auth(self, token_cache_json: Optional[str]) -> str:
        if not self._settings.onedrive_client_id:
            raise RuntimeError("ONEDRIVE_CLIENT_ID is required")

        app, cache = self._build_app(token_cache_json)
        scopes = self._effective_scopes()

        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise RuntimeError(f"Device code flow failed to start: {json.dumps(flow)}")

        message = flow.get("message")
        if message:
            print(message)

        result = app.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            error = result.get("error_description") or str(result)
            raise RuntimeError(f"OneDrive auth failed: {error}")

        return cache.serialize()

    def acquire_access_token(self, token_cache_json: Optional[str]) -> Tuple[str, str]:
        if not self._settings.onedrive_client_id:
            raise RuntimeError("ONEDRIVE_CLIENT_ID is required")
        if not token_cache_json:
            raise AuthRequiredError("Run imagetracker photos:auth")

        app, cache = self._build_app(token_cache_json)
        scopes = self._effective_scopes()
        accounts = app.get_accounts()

        if not accounts:
            raise AuthRequiredError("Run imagetracker photos:auth")

        result = None
        for account in accounts:
            result = app.acquire_token_silent(scopes, account=account)
            if result and "access_token" in result:
                return result["access_token"], cache.serialize()

        if result and result.get("error") in {"interaction_required", "invalid_grant"}:
            raise AuthRequiredError("Run imagetracker photos:auth")

        raise AuthRequiredError("Run imagetracker photos:auth")
