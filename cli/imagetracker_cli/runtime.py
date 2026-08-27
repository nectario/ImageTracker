from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3

from .api_client import ApiClient
from .auth import CognitoAuth, TokenStore
from .config import CliConfig, ConfigStore
from .state import LocalState


@dataclass
class Runtime:
    config_store: ConfigStore
    config: CliConfig
    local_state: LocalState | None
    token_store: TokenStore
    auth: CognitoAuth
    api: ApiClient

    @property
    def state(self) -> LocalState:
        if self.local_state is None:
            raise ValueError(
                "No account-scoped local state is available. Sign in again before managing sources."
            )
        return self.local_state


def boto3_session(*, region: str, profile: str | None = None) -> boto3.Session:
    return boto3.Session(profile_name=profile, region_name=region)


def cloudformation_client(*, region: str, profile: str | None = None) -> Any:
    return boto3_session(region=region, profile=profile).client("cloudformation")


def build_runtime(config_store: ConfigStore | None = None) -> Runtime:
    store = config_store or ConfigStore()
    config = store.load()
    if not config.is_configured:
        raise ValueError("ImageTracker is not configured. Run 'imagetracker configure' first.")
    token_store = TokenStore(store.fallback_token_path)
    session = boto3_session(region=config.aws_region, profile=config.aws_profile)
    auth = CognitoAuth(session.client("cognito-idp"), config.cognito_client_id, token_store)
    api = ApiClient(config.api_url, auth)
    saved_tokens = token_store.load()
    subject = saved_tokens.local_subject if saved_tokens else None
    state = LocalState(store.state_path_for_subject(subject)) if subject else None
    return Runtime(store, config, state, token_store, auth, api)
