from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import parse_qs, unquote, urlparse

import pymysql


class ParameterClient(Protocol):
    def get_parameter(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class LegacyDbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

    def validated(self) -> "LegacyDbConfig":
        if self.database != "ImageTracker":
            raise ValueError(
                f"Legacy administration is restricted to the ImageTracker database, not {self.database!r}."
            )
        if not self.host or not self.user:
            raise ValueError("Legacy database host and user are required")
        return self


def _config_from_dsn(dsn: str) -> LegacyDbConfig:
    parsed = urlparse(dsn)
    if parsed.scheme.lower() not in {"mysql", "mysql+pymysql"}:
        raise ValueError("The legacy admin DSN must use mysql:// or mysql+pymysql://")
    query = parse_qs(parsed.query)
    return LegacyDbConfig(
        host=parsed.hostname or "",
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset=query.get("charset", ["utf8mb4"])[0],
    ).validated()


def _config_from_mapping(payload: Mapping[str, Any]) -> LegacyDbConfig:
    return LegacyDbConfig(
        host=str(payload.get("host") or ""),
        port=int(payload.get("port") or 3306),
        user=str(payload.get("user") or payload.get("username") or ""),
        password=str(payload.get("password") or ""),
        database=str(payload.get("database") or ""),
        charset=str(payload.get("charset") or "utf8mb4"),
    ).validated()


def load_legacy_db_config(
    *,
    parameter_client: ParameterClient | None = None,
    environ: Mapping[str, str] | None = None,
) -> LegacyDbConfig:
    env = environ or os.environ
    admin_dsn = env.get("IMAGETRACKER_ADMIN_MYSQL_DSN")
    if admin_dsn:
        return _config_from_dsn(admin_dsn)

    parameter_name = env.get("IMAGETRACKER_DB_SECRET_PARAMETER")
    if parameter_name:
        if parameter_client is None:
            raise ValueError("An SSM client is required to resolve the legacy database parameter")
        response = parameter_client.get_parameter(Name=parameter_name, WithDecryption=True)
        try:
            payload = json.loads(str(response["Parameter"]["Value"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("The ImageTracker database parameter is not valid JSON") from exc
        return _config_from_mapping(payload)

    generic_dsn = env.get("MYSQL_DSN")
    if generic_dsn:
        return _config_from_dsn(generic_dsn)

    database = env.get("MYSQL_DATABASE_IMAGETRACKER") or env.get("MYSQL_DATABASE") or ""
    if database:
        return _config_from_mapping(
            {
                "host": env.get("MYSQL_HOST", "127.0.0.1"),
                "port": env.get("MYSQL_PORT", "3306"),
                "user": env.get("MYSQL_USERID") or env.get("MYSQL_USER") or "",
                "password": env.get("MYSQL_PASSWORD", ""),
                "database": database,
            }
        )
    raise ValueError(
        "Legacy DB access is not configured. Set IMAGETRACKER_DB_SECRET_PARAMETER, "
        "IMAGETRACKER_ADMIN_MYSQL_DSN, or MYSQL_* variables scoped to ImageTracker."
    )


def mysql_connection_factory(config: LegacyDbConfig) -> Callable[[], Any]:
    def connect() -> Any:
        return pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            charset=config.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )

    return connect


class LegacyInspector:
    """Aggregate-only legacy checks in an explicitly read-only transaction."""

    def __init__(self, connection_factory: Callable[[], Any]):
        self.connection_factory = connection_factory

    def audit(self) -> dict[str, Any]:
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION TRANSACTION READ ONLY")
                cursor.execute("START TRANSACTION READ ONLY")
                counts = self._counts(cursor)
                cursor.execute(
                    "SELECT `Version` FROM `SchemaMigration` WHERE `Version` >= '007' ORDER BY `Version`"
                )
                versions = [str(row["Version"]) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT `MigrationStatus` AS `Status`, COUNT(*) AS `Count`
                    FROM `LegacyImageAssetMap`
                    GROUP BY `MigrationStatus`
                    ORDER BY `MigrationStatus`
                    """
                )
                mapping = {str(row["Status"]): int(row["Count"]) for row in cursor.fetchall()}
                cursor.execute(
                    """
                    SELECT COUNT(*) AS `Count`
                    FROM `ImageAsset`
                    WHERE `DateTime` IS NOT NULL
                      AND `UtcOffsetMinutes` IS NOT NULL
                      AND `DateTimeUtc` IS NOT NULL
                      AND ABS(TIMESTAMPDIFF(
                          SECOND,
                          DATE_SUB(`DateTime`, INTERVAL `UtcOffsetMinutes` MINUTE),
                          `DateTimeUtc`
                      )) > 1
                    """
                )
                inconsistent_temporal = int(cursor.fetchone()["Count"])
            connection.rollback()
        finally:
            connection.close()
        checks = [
            {
                "code": "DATABASE_SCOPE",
                "status": "Ok",
                "detail": "Read-only connection is scoped to ImageTracker.",
            },
            {
                "code": "LEGACY_TEMPORAL_CONSISTENCY",
                "status": "Ok" if inconsistent_temporal == 0 else "Review",
                "count": inconsistent_temporal,
                "detail": "Legacy rows whose local time, offset, and stored UTC disagree.",
            },
        ]
        return {
            "readOnly": True,
            "database": "ImageTracker",
            "counts": counts,
            "migrationVersions": versions,
            "mappingStatus": mapping,
            "checks": checks,
        }

    def migration_preview(
        self,
        *,
        checkpoint_legacy_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        if checkpoint_legacy_id < 0:
            raise ValueError("Legacy checkpoint ID cannot be negative")
        if not 1 <= limit <= 1000:
            raise ValueError("Legacy preview limit must be between 1 and 1000")
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION TRANSACTION READ ONLY")
                cursor.execute("START TRANSACTION READ ONLY")
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS `BatchRows`,
                        COALESCE(MIN(i.`Id`), 0) AS `FirstLegacyId`,
                        COALESCE(MAX(i.`Id`), 0) AS `LastLegacyId`,
                        SUM(CASE WHEN m.`Id` IS NOT NULL THEN 1 ELSE 0 END) AS `AlreadyMapped`,
                        SUM(CASE WHEN m.`Id` IS NULL THEN 1 ELSE 0 END) AS `Unmapped`,
                        SUM(CASE WHEN
                            i.`RawGraphJson` IS NULL
                            OR JSON_UNQUOTE(JSON_EXTRACT(i.`RawGraphJson`, '$.LocalPath')) IS NULL
                            THEN 1 ELSE 0 END) AS `MissingLocalPath`
                    FROM (
                        SELECT `Id`, `RawGraphJson`
                        FROM `ImageAsset`
                        WHERE `Id` > %s
                        ORDER BY `Id`
                        LIMIT %s
                    ) i
                    LEFT JOIN `LegacyImageAssetMap` m ON m.`LegacyImageAssetId` = i.`Id`
                    """,
                    (checkpoint_legacy_id, limit),
                )
                row = cursor.fetchone()
                last_legacy_id = int(row["LastLegacyId"] or 0)
                cursor.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM `ImageAsset` WHERE `Id` > %s LIMIT 1
                    ) AS `HasMore`
                    """,
                    (last_legacy_id or checkpoint_legacy_id,),
                )
                has_more = bool(cursor.fetchone()["HasMore"])
            connection.rollback()
        finally:
            connection.close()
        batch_rows = int(row["BatchRows"] or 0)
        next_checkpoint = last_legacy_id if batch_rows else checkpoint_legacy_id
        return {
            "dryRun": True,
            "checkpointLegacyId": checkpoint_legacy_id,
            "batchLimit": limit,
            "batchRows": batch_rows,
            "firstLegacyId": int(row["FirstLegacyId"] or 0),
            "lastLegacyId": last_legacy_id,
            "nextCheckpointLegacyId": next_checkpoint,
            "hasMore": has_more,
            "alreadyMapped": int(row["AlreadyMapped"] or 0),
            "unmapped": int(row["Unmapped"] or 0),
            "missingLocalPath": int(row["MissingLocalPath"] or 0),
            "writesPerformed": 0,
        }

    @staticmethod
    def _counts(cursor: Any) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table in (
            "ImageAsset",
            "MediaAsset",
            "MediaOccurrence",
            "LegacyImageAssetMap",
            "ProcessingJob",
        ):
            cursor.execute(f"SELECT COUNT(*) AS `Count` FROM `{table}`")
            counts[table] = int(cursor.fetchone()["Count"])
        return counts
