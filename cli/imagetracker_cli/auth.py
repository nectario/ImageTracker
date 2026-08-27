from __future__ import annotations

import json
import os
import stat
import base64
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

from .config import write_private_json


TOKEN_SERVICE = "NektronAI.ImageTracker"
TOKEN_ACCOUNT = "default"


class CognitoClient(Protocol):
    def sign_up(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def confirm_sign_up(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def initiate_auth(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def get_user(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def global_sign_out(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class TokenSet:
    access_token: str
    id_token: str
    refresh_token: str
    expires_at_utc: str
    email: str | None = None
    subject: str | None = None

    @property
    def is_expired(self) -> bool:
        try:
            expires = datetime.fromisoformat(self.expires_at_utc.replace("Z", "+00:00"))
        except ValueError:
            return True
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return expires <= datetime.now(timezone.utc) + timedelta(seconds=30)

    @property
    def local_subject(self) -> str | None:
        """Return the Cognito subject for local namespacing only.

        API authorization still relies exclusively on Cognito/API Gateway token
        verification. This unverified claim is never sent as proof of identity.
        """

        return self.subject or id_token_subject(self.id_token)


def id_token_subject(id_token: str) -> str | None:
    parts = id_token.split(".")
    if len(parts) != 3:
        return None
    try:
        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded.encode("ascii")))
    except (ValueError, UnicodeError, json.JSONDecodeError):
        return None
    subject = payload.get("sub") if isinstance(payload, Mapping) else None
    return subject.strip() if isinstance(subject, str) and subject.strip() else None


class TokenBackend(Protocol):
    name: str
    def load(self) -> TokenSet | None: ...
    def save(self, tokens: TokenSet) -> None: ...
    def delete(self) -> None: ...


class FileTokenBackend:
    name = "private-file"

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> TokenSet | None:
        if not self.path.exists():
            return None
        if os.name != "nt":
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode & (stat.S_IRWXG | stat.S_IRWXO):
                self.path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return TokenSet(**raw)
        except (OSError, json.JSONDecodeError, TypeError, KeyError) as exc:
            raise ValueError(f"Cannot read saved ImageTracker session: {exc}") from exc

    def save(self, tokens: TokenSet) -> None:
        write_private_json(self.path, asdict(tokens))

    def delete(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class KeyringTokenBackend:
    name = "os-keyring"

    def __init__(self, keyring_module: Any):
        self._keyring = keyring_module

    def load(self) -> TokenSet | None:
        raw = self._keyring.get_password(TOKEN_SERVICE, TOKEN_ACCOUNT)
        if not raw:
            return None
        return TokenSet(**json.loads(raw))

    def save(self, tokens: TokenSet) -> None:
        self._keyring.set_password(TOKEN_SERVICE, TOKEN_ACCOUNT, json.dumps(asdict(tokens)))

    def delete(self) -> None:
        try:
            self._keyring.delete_password(TOKEN_SERVICE, TOKEN_ACCOUNT)
        except Exception:
            pass


class TokenStore:
    """Use the OS credential vault when available, with a private-file fallback.

    A typical WSL environment has no usable Secret Service session. The fallback
    is therefore deliberate and always created with user-only permissions.
    """

    def __init__(self, fallback_path: Path, keyring_module: Any | None = None):
        self._fallback = FileTokenBackend(fallback_path)
        self._keyring = self._discover_keyring(keyring_module)

    @staticmethod
    def _discover_keyring(keyring_module: Any | None) -> KeyringTokenBackend | None:
        module = keyring_module
        if module is None:
            try:
                import keyring as module  # type: ignore[no-redef]
            except Exception:
                return None
        try:
            backend = module.get_keyring()
            priority = getattr(backend, "priority", 0)
            if priority is None or float(priority) <= 0:
                return None
            return KeyringTokenBackend(module)
        except Exception:
            return None

    @property
    def backend_name(self) -> str:
        return self._keyring.name if self._keyring else self._fallback.name

    def load(self) -> TokenSet | None:
        if self._keyring:
            try:
                tokens = self._keyring.load()
                if tokens:
                    return tokens
            except Exception:
                self._keyring = None
        return self._fallback.load()

    def save(self, tokens: TokenSet) -> None:
        if self._keyring:
            try:
                self._keyring.save(tokens)
                self._fallback.delete()
                return
            except Exception:
                self._keyring = None
        self._fallback.save(tokens)

    def delete(self) -> None:
        if self._keyring:
            self._keyring.delete()
        self._fallback.delete()


class CognitoAuth:
    def __init__(self, client: CognitoClient, client_id: str, token_store: TokenStore):
        self.client = client
        self.client_id = client_id
        self.token_store = token_store

    def signup(self, email: str, password: str) -> Mapping[str, Any]:
        return self.client.sign_up(
            ClientId=self.client_id,
            Username=email.strip().lower(),
            Password=password,
            UserAttributes=[{"Name": "email", "Value": email.strip().lower()}],
        )

    def confirm(self, email: str, code: str) -> None:
        self.client.confirm_sign_up(
            ClientId=self.client_id,
            Username=email.strip().lower(),
            ConfirmationCode=code.strip(),
        )

    def login(self, email: str, password: str) -> TokenSet:
        response = self.client.initiate_auth(
            ClientId=self.client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": email.strip().lower(), "PASSWORD": password},
        )
        tokens = self._tokens_from_result(response.get("AuthenticationResult") or {}, email=email)
        self.token_store.save(tokens)
        return tokens

    def refresh(self, tokens: TokenSet) -> TokenSet:
        response = self.client.initiate_auth(
            ClientId=self.client_id,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": tokens.refresh_token},
        )
        refreshed = self._tokens_from_result(
            response.get("AuthenticationResult") or {},
            email=tokens.email,
            fallback_refresh=tokens.refresh_token,
            fallback_subject=tokens.local_subject,
        )
        self.token_store.save(refreshed)
        return refreshed

    def current_tokens(self, *, refresh_if_needed: bool = True) -> TokenSet | None:
        tokens = self.token_store.load()
        if tokens and tokens.is_expired and refresh_if_needed:
            return self.refresh(tokens)
        return tokens

    def status(self) -> Mapping[str, Any] | None:
        tokens = self.current_tokens()
        if not tokens:
            return None
        return self.client.get_user(AccessToken=tokens.access_token)

    def logout(self) -> None:
        tokens = self.token_store.load()
        try:
            if tokens and not tokens.is_expired:
                try:
                    self.client.global_sign_out(AccessToken=tokens.access_token)
                except Exception:
                    # Local sign-out must remain reliable when offline or after
                    # server-side token revocation. The saved session is removed.
                    pass
        finally:
            self.token_store.delete()

    @staticmethod
    def _tokens_from_result(
        result: Mapping[str, Any],
        *,
        email: str | None,
        fallback_refresh: str = "",
        fallback_subject: str | None = None,
    ) -> TokenSet:
        access_token = str(result.get("AccessToken") or "")
        id_token = str(result.get("IdToken") or "")
        refresh_token = str(result.get("RefreshToken") or fallback_refresh)
        if not access_token or not id_token or not refresh_token:
            raise ValueError("Cognito did not return a complete session")
        expires_in = int(result.get("ExpiresIn") or 3600)
        expires = datetime.now(timezone.utc) + timedelta(seconds=max(1, expires_in))
        subject = id_token_subject(id_token) or fallback_subject
        if fallback_subject and subject != fallback_subject:
            raise ValueError("Cognito refreshed a session for a different account")
        return TokenSet(
            access_token=access_token,
            id_token=id_token,
            refresh_token=refresh_token,
            expires_at_utc=expires.isoformat().replace("+00:00", "Z"),
            email=email,
            subject=subject,
        )
