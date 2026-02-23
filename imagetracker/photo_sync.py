from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from imagetracker.captioner import CaptionResult, OpenAIVisionCaptioner
from imagetracker.config import Settings
from imagetracker.db import Database, utc_now
from imagetracker.graph_client import GraphApiError, GraphClient
from imagetracker.migrations import MigrationRunner
from imagetracker.onedrive_auth import AuthRequiredError, OneDriveAuthService
from imagetracker.repositories import (
    ImageAssetRepository,
    ImageUpsertPayload,
    SyncStateRepository,
    TokenCacheRepository,
)


@dataclass
class SyncResult:
    processed_count: int
    upserted_count: int
    deleted_count: int
    captioned_count: int
    final_delta_link: str


def parse_graph_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_image_item(item: Dict[str, Any]) -> bool:
    if item.get("photo") is not None:
        return True

    mime_type = item.get("file", {}).get("mimeType")
    if isinstance(mime_type, str) and mime_type.startswith("image/"):
        return True

    return False


def _pick_thumbnail_url(thumbnails: Dict[str, Any]) -> Optional[str]:
    for group in thumbnails.get("value", []):
        for size in ("large", "medium", "small"):
            candidate = group.get(size, {})
            if candidate and candidate.get("url"):
                return candidate["url"]
    return None


class PhotoSyncService:
    SOURCE = "OneDrive"

    def __init__(
        self,
        settings: Settings,
        database: Database,
        migration_runner: MigrationRunner,
        auth_service: OneDriveAuthService,
        image_repo: Optional[ImageAssetRepository] = None,
        sync_state_repo: Optional[SyncStateRepository] = None,
        token_cache_repo: Optional[TokenCacheRepository] = None,
        graph_client_factory: Optional[Callable[[str], GraphClient]] = None,
        captioner: Optional[OpenAIVisionCaptioner] = None,
        now_fn: Callable[[], datetime] = utc_now,
    ):
        self._settings = settings
        self._database = database
        self._migration_runner = migration_runner
        self._auth_service = auth_service
        self._image_repo = image_repo or ImageAssetRepository()
        self._sync_state_repo = sync_state_repo or SyncStateRepository()
        self._token_cache_repo = token_cache_repo or TokenCacheRepository()
        self._graph_client_factory = graph_client_factory or (lambda token: GraphClient(token))
        self._captioner = captioner
        self._now_fn = now_fn

    def run_sync(self) -> SyncResult:
        self._apply_migrations()

        with self._database.connection() as conn:
            token_cache_row = self._token_cache_repo.get(conn)

        access_token, refreshed_cache = self._auth_service.acquire_access_token(
            token_cache_row["CacheJson"] if token_cache_row else None
        )

        now = self._now_fn()
        with self._database.connection() as conn:
            self._token_cache_repo.set(conn, refreshed_cache, now)

        graph_client = self._graph_client_factory(access_token)

        with self._database.connection() as conn:
            state = self._sync_state_repo.get(conn)

        folder_drive_item_id, folder_path = self._resolve_folder(state, graph_client)
        previous_delta_link = state["DeltaLink"] if state else None
        previous_last_success = state["LastSuccessAtUtc"] if state else None

        initial_run = previous_delta_link is None
        next_or_delta_link = previous_delta_link or f"/me/drive/items/{folder_drive_item_id}/delta"

        processed_count = 0
        upserted_count = 0
        deleted_count = 0
        captioned_count = 0
        final_delta_link: Optional[str] = None

        try:
            while next_or_delta_link:
                page = graph_client.get(next_or_delta_link)
                items = page.get("value", [])

                for item in items:
                    processed_count += 1
                    outcome = self._process_item(
                        graph_client=graph_client,
                        item=item,
                        initial_run=initial_run,
                        now=now,
                    )
                    if outcome == "upserted":
                        upserted_count += 1
                    elif outcome == "deleted":
                        deleted_count += 1
                    elif outcome == "captioned":
                        upserted_count += 1
                        captioned_count += 1

                next_link = page.get("@odata.nextLink")
                if next_link:
                    next_or_delta_link = next_link
                    continue

                final_delta_link = page.get("@odata.deltaLink")
                next_or_delta_link = None

            if not final_delta_link:
                raise RuntimeError("Delta sync completed without @odata.deltaLink")

            with self._database.connection() as conn:
                self._sync_state_repo.upsert(
                    conn,
                    folder_drive_item_id=folder_drive_item_id,
                    folder_path=folder_path,
                    delta_link=final_delta_link,
                    last_run_at_utc=now,
                    last_success_at_utc=now,
                    last_error=None,
                    updated_at_utc=now,
                )
        except Exception as exc:
            with self._database.connection() as conn:
                self._sync_state_repo.upsert(
                    conn,
                    folder_drive_item_id=folder_drive_item_id,
                    folder_path=folder_path,
                    delta_link=previous_delta_link,
                    last_run_at_utc=now,
                    last_success_at_utc=previous_last_success,
                    last_error=str(exc)[:64000],
                    updated_at_utc=now,
                )
            raise

        return SyncResult(
            processed_count=processed_count,
            upserted_count=upserted_count,
            deleted_count=deleted_count,
            captioned_count=captioned_count,
            final_delta_link=final_delta_link,
        )

    def _process_item(
        self,
        graph_client: GraphClient,
        item: Dict[str, Any],
        initial_run: bool,
        now: datetime,
    ) -> Optional[str]:
        if item.get("folder") is not None:
            return None

        drive_item_id = item.get("id")
        if not drive_item_id:
            return None

        if item.get("deleted") is not None:
            with self._database.connection() as conn:
                self._image_repo.mark_deleted(conn, self.SOURCE, drive_item_id, now)
            return "deleted"

        if not _is_image_item(item):
            return None

        taken_datetime = parse_graph_datetime(item.get("photo", {}).get("takenDateTime"))
        if initial_run and not self._within_initial_cutoff(taken_datetime, now):
            return None

        geo_coordinates = item.get("location", {}).get("geoCoordinates", {})
        payload = ImageUpsertPayload(
            source=self.SOURCE,
            drive_item_id=drive_item_id,
            file_name=item.get("name", ""),
            taken_datetime_utc=taken_datetime,
            latitude=_as_float(geo_coordinates.get("latitude")),
            longitude=_as_float(geo_coordinates.get("longitude")),
            altitude=_as_float(geo_coordinates.get("altitude")),
            raw_graph_json=json.dumps(item, ensure_ascii=False),
            inserted_at_utc=now,
            updated_at_utc=now,
        )

        with self._database.connection() as conn:
            existing = self._image_repo.get_by_source_and_drive_item(conn, self.SOURCE, drive_item_id)
            self._image_repo.upsert(conn, payload)

        if not self._should_caption(existing):
            return "upserted"

        caption = self._generate_caption(graph_client, drive_item_id)
        if not caption:
            return "upserted"

        with self._database.connection() as conn:
            self._image_repo.update_caption(
                conn,
                source=self.SOURCE,
                drive_item_id=drive_item_id,
                short_description=caption.short_description,
                short_description_model=caption.model,
                updated_at_utc=now,
            )

        return "captioned"

    def _generate_caption(
        self,
        graph_client: GraphClient,
        drive_item_id: str,
    ) -> Optional[CaptionResult]:
        if not self._captioner:
            return None

        try:
            thumbnails = graph_client.get_thumbnails(drive_item_id)
            thumbnail_url = _pick_thumbnail_url(thumbnails)
            if not thumbnail_url:
                return None

            image_bytes = graph_client.get_bytes(thumbnail_url)
            return self._captioner.generate_caption(image_bytes)
        except Exception as exc:
            print(f"Caption generation skipped for {drive_item_id}: {exc}", file=sys.stderr)
            return None

    def _should_caption(self, existing: Optional[Dict[str, Any]]) -> bool:
        if not self._captioner:
            return False

        if existing is None:
            return True

        short_description = existing.get("ShortDescription")
        if not short_description:
            return True

        existing_model = existing.get("ShortDescriptionModel")
        current_model = getattr(self._captioner, "model", None)
        if current_model and existing_model != current_model:
            return True

        if existing.get("ShortDescriptionUpdatedAtUtc") is None:
            return True

        return False

    def _within_initial_cutoff(self, taken_datetime: Optional[datetime], now: datetime) -> bool:
        if not taken_datetime:
            return False
        cutoff_start = now - timedelta(days=self._settings.photo_sync_initial_cutoff_days)
        return taken_datetime >= cutoff_start

    def _resolve_folder(
        self,
        state: Optional[Dict[str, Any]],
        graph_client: GraphClient,
    ) -> tuple[str, str]:
        if state and state.get("FolderDriveItemId") and state.get("FolderPath"):
            return state["FolderDriveItemId"], state["FolderPath"]

        candidate_paths = [
            self._settings.onedrive_camera_upload_path,
            *self._settings.onedrive_camera_upload_fallback_paths,
        ]

        last_error: Optional[Exception] = None
        for path in candidate_paths:
            try:
                folder = graph_client.resolve_folder_by_path(path)
            except GraphApiError as exc:
                last_error = exc
                continue

            folder_id = folder.get("id")
            if folder_id:
                return folder_id, path

        if last_error:
            raise RuntimeError(
                f"Could not resolve camera upload folder using configured paths: {candidate_paths}"
            ) from last_error

        raise RuntimeError(f"Could not resolve camera upload folder using configured paths: {candidate_paths}")

    def _apply_migrations(self) -> None:
        with self._database.connection() as conn:
            self._migration_runner.apply_all(conn)


def build_default_captioner(settings: Settings) -> Optional[OpenAIVisionCaptioner]:
    if not settings.openai_api_key:
        return None

    return OpenAIVisionCaptioner(
        api_key=settings.openai_api_key,
        model=settings.openai_vision_model,
        max_words=settings.photo_caption_max_words,
    )


__all__ = [
    "AuthRequiredError",
    "PhotoSyncService",
    "SyncResult",
    "build_default_captioner",
    "parse_graph_datetime",
]
