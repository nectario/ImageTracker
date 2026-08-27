from __future__ import annotations

from threading import Lock
from typing import Any

from services.api.domain_adapter import DomainServiceAdapter
from services.api.service import Phase1Service, ServiceUnavailableError, UnavailablePhase1Service
from services.common.settings import AppSettings


class LazyConfiguredPhase1Service:
    """Resolve SSM and create the tiny DB pool only when an API request needs it."""

    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._lock = Lock()
        self._adapter: DomainServiceAdapter | None = None
        self._runtime: Any | None = None

    def _get(self) -> DomainServiceAdapter:
        if self._adapter is not None:
            return self._adapter
        with self._lock:
            if self._adapter is None:
                try:
                    from services.data.database import build_database_runtime
                    from services.domain.service import Phase1DomainService

                    self._runtime = build_database_runtime(self._settings)
                    self._adapter = DomainServiceAdapter(
                        Phase1DomainService(self._runtime.session_factory)
                    )
                except Exception as exc:
                    raise ServiceUnavailableError(
                        "The ImageTracker data service is temporarily unavailable",
                        code="DATA_SERVICE_UNAVAILABLE",
                    ) from exc
        adapter = self._adapter
        if adapter is None:  # pragma: no cover - guarded by the lock above
            raise ServiceUnavailableError(
                "The ImageTracker data service is temporarily unavailable",
                code="DATA_SERVICE_UNAVAILABLE",
            )
        return adapter

    def __getattr__(self, name: str) -> Any:
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            adapter = self._get()
            return await getattr(adapter, name)(*args, **kwargs)

        return invoke


def build_default_phase1_service(settings: AppSettings) -> Phase1Service:
    if settings.stage.casefold() in {"local", "test"}:
        return UnavailablePhase1Service()  # type: ignore[return-value]
    return LazyConfiguredPhase1Service(settings)  # type: ignore[return-value]
