from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from pymysql.connections import Connection


@dataclass
class ImageUpsertPayload:
    source: str
    drive_item_id: str
    file_name: str
    taken_datetime_utc: Optional[datetime]
    latitude: Optional[float]
    longitude: Optional[float]
    altitude: Optional[float]
    raw_graph_json: str
    inserted_at_utc: datetime
    updated_at_utc: datetime


class ImageAssetRepository:
    UPSERT_SQL = """
    INSERT INTO `ImageAsset` (
        `Source`,
        `DriveItemId`,
        `FileName`,
        `TakenDateTimeUtc`,
        `Latitude`,
        `Longitude`,
        `Altitude`,
        `RawGraphJson`,
        `InsertedAtUtc`,
        `UpdatedAtUtc`
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSON), %s, %s)
    ON DUPLICATE KEY UPDATE
        `FileName` = VALUES(`FileName`),
        `TakenDateTimeUtc` = VALUES(`TakenDateTimeUtc`),
        `Latitude` = VALUES(`Latitude`),
        `Longitude` = VALUES(`Longitude`),
        `Altitude` = VALUES(`Altitude`),
        `RawGraphJson` = VALUES(`RawGraphJson`),
        `IsDeleted` = 0,
        `DeletedAtUtc` = NULL,
        `UpdatedAtUtc` = VALUES(`UpdatedAtUtc`)
    """.strip()

    def get_by_source_and_drive_item(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
    ) -> Optional[Dict[str, Any]]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    `Id`,
                    `Source`,
                    `DriveItemId`,
                    `ShortDescription`,
                    `ShortDescriptionModel`,
                    `ShortDescriptionUpdatedAtUtc`
                FROM `ImageAsset`
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (source, drive_item_id),
            )
            return cursor.fetchone()

    def upsert(self, conn: Connection, payload: ImageUpsertPayload) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                self.UPSERT_SQL,
                (
                    payload.source,
                    payload.drive_item_id,
                    payload.file_name,
                    payload.taken_datetime_utc,
                    payload.latitude,
                    payload.longitude,
                    payload.altitude,
                    payload.raw_graph_json,
                    payload.inserted_at_utc,
                    payload.updated_at_utc,
                ),
            )

    def mark_deleted(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
        deleted_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `ImageAsset`
                SET
                    `IsDeleted` = 1,
                    `DeletedAtUtc` = %s,
                    `UpdatedAtUtc` = %s
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (deleted_at_utc, deleted_at_utc, source, drive_item_id),
            )

    def update_caption(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
        short_description: str,
        short_description_model: str,
        updated_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `ImageAsset`
                SET
                    `ShortDescription` = %s,
                    `ShortDescriptionModel` = %s,
                    `ShortDescriptionUpdatedAtUtc` = %s,
                    `UpdatedAtUtc` = %s
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (
                    short_description,
                    short_description_model,
                    updated_at_utc,
                    updated_at_utc,
                    source,
                    drive_item_id,
                ),
            )


class SyncStateRepository:
    def get(self, conn: Connection) -> Optional[Dict[str, Any]]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    `Id`,
                    `FolderDriveItemId`,
                    `FolderPath`,
                    `DeltaLink`,
                    `LastRunAtUtc`,
                    `LastSuccessAtUtc`,
                    `LastError`,
                    `UpdatedAtUtc`
                FROM `OneDriveSyncState`
                WHERE `Id` = 1
                """
            )
            return cursor.fetchone()

    def upsert(
        self,
        conn: Connection,
        *,
        folder_drive_item_id: str,
        folder_path: str,
        delta_link: Optional[str],
        last_run_at_utc: Optional[datetime],
        last_success_at_utc: Optional[datetime],
        last_error: Optional[str],
        updated_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO `OneDriveSyncState` (
                    `Id`,
                    `FolderDriveItemId`,
                    `FolderPath`,
                    `DeltaLink`,
                    `LastRunAtUtc`,
                    `LastSuccessAtUtc`,
                    `LastError`,
                    `UpdatedAtUtc`
                )
                VALUES (1, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    `FolderDriveItemId` = VALUES(`FolderDriveItemId`),
                    `FolderPath` = VALUES(`FolderPath`),
                    `DeltaLink` = VALUES(`DeltaLink`),
                    `LastRunAtUtc` = VALUES(`LastRunAtUtc`),
                    `LastSuccessAtUtc` = VALUES(`LastSuccessAtUtc`),
                    `LastError` = VALUES(`LastError`),
                    `UpdatedAtUtc` = VALUES(`UpdatedAtUtc`)
                """,
                (
                    folder_drive_item_id,
                    folder_path,
                    delta_link,
                    last_run_at_utc,
                    last_success_at_utc,
                    last_error,
                    updated_at_utc,
                ),
            )


class TokenCacheRepository:
    def get(self, conn: Connection) -> Optional[Dict[str, Any]]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `Id`, `CacheJson`, `UpdatedAtUtc`
                FROM `OneDriveTokenCache`
                WHERE `Id` = 1
                """
            )
            return cursor.fetchone()

    def set(self, conn: Connection, cache_json: str, updated_at_utc: datetime) -> None:
        # Validate cache is JSON to avoid storing corrupt values.
        json.loads(cache_json)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO `OneDriveTokenCache` (`Id`, `CacheJson`, `UpdatedAtUtc`)
                VALUES (1, %s, %s)
                ON DUPLICATE KEY UPDATE
                    `CacheJson` = VALUES(`CacheJson`),
                    `UpdatedAtUtc` = VALUES(`UpdatedAtUtc`)
                """,
                (cache_json, updated_at_utc),
            )
