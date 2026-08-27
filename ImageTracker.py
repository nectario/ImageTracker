from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import math
import mimetypes
import os
import re
import sys
import time as time_module
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Protocol
from urllib.parse import parse_qs, urlparse

import pymysql
import requests
from pymysql.connections import Connection


@dataclass(frozen=True)
class Settings:
    onedrive_tenant: str
    onedrive_client_id: str
    onedrive_scopes: List[str]
    onedrive_camera_upload_path: str
    onedrive_camera_upload_fallback_paths: List[str]
    mysql_dsn: str
    openai_api_key: Optional[str]
    google_maps_api_key: Optional[str]
    location_normalization_rules_path: str
    openai_vision_model: str
    photo_sync_initial_cutoff_days: int
    photo_caption_max_words: int


@dataclass(frozen=True)
class MysqlConnectionConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


@dataclass
class ImageUpsertPayload:
    source: str
    drive_item_id: str
    file_name: str
    date_time: Optional[datetime]
    date_value: Optional[date]
    time_value: Optional[time]
    time_zone: Optional[str]
    utc_offset_minutes: Optional[int]
    date_time_utc: Optional[datetime]
    taken_datetime_utc: Optional[datetime]
    latitude: Optional[float]
    longitude: Optional[float]
    altitude: Optional[float]
    raw_graph_json: str
    inserted_at_utc: datetime
    updated_at_utc: datetime


@dataclass
class CaptionResult:
    short_description: str
    model: str


@dataclass
class GpsCoordinates:
    latitude: float
    longitude: float
    altitude: Optional[float]


@dataclass
class LocalPhotoSyncResult:
    scanned_count: int
    eligible_count: int
    skipped_count: int
    upserted_count: int
    captioned_count: int
    geocoded_count: int
    timezone_enriched_count: int


@dataclass(frozen=True)
class LocalPhotoFile:
    path: Path
    stat: os.stat_result


class DbError(RuntimeError):
    pass


@dataclass
class LocationResolution:
    location_display_name: str
    street_address: Optional[str]
    original_street_number: Optional[str]
    neighborhood: Optional[str]
    city: Optional[str]
    county: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]
    country_code: Optional[str]


@dataclass(frozen=True)
class LocationNormalizationRule:
    name: str
    city_equals: Optional[str]
    state_in: List[str]
    country_in: List[str]
    street_contains_any: List[str]
    original_street_number_in: List[str]
    normalized_street_address: Optional[str]


@dataclass
class CaptureDateTimeInfo:
    local_datetime: datetime
    time_zone: Optional[str]
    utc_offset_minutes: Optional[int]
    utc_datetime: Optional[datetime]


@dataclass
class TimeZoneResolution:
    time_zone_id: str
    utc_offset_minutes: int


@dataclass
class CategoryAssignment:
    category: str
    category_source: str


class Captioner(Protocol):
    def generate_caption(self, image_bytes: bytes) -> Optional[CaptionResult]:
        ...


class GpsExtractor(Protocol):
    def extract(
        self,
        content_bytes: bytes,
        *,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[GpsCoordinates]:
        ...


class LocationResolver(Protocol):
    @property
    def provider(self) -> str:
        ...

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[LocationResolution]:
        ...


class CaptureDateTimeExtractor(Protocol):
    def extract(
        self,
        content_bytes: bytes,
        *,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[CaptureDateTimeInfo]:
        ...


class TimeZoneResolver(Protocol):
    @property
    def provider(self) -> str:
        ...

    def resolve_timezone(
        self,
        *,
        latitude: float,
        longitude: float,
        timestamp_utc: datetime,
    ) -> Optional[TimeZoneResolution]:
        ...


def _parse_dotenv_line(line: str) -> Optional[tuple[str, str]]:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()

    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1]

    return key, value


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(raw_line)
        if not parsed:
            continue
        key, value = parsed
        os.environ.setdefault(key, value)


def _split_space_list(value: str) -> List[str]:
    return [part for part in value.split() if part]


def _split_comma_list(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def load_settings() -> Settings:
    load_dotenv()

    scopes = _split_space_list(
        os.getenv("ONEDRIVE_SCOPES", "User.Read Files.Read.All offline_access")
    )

    primary_camera_path = os.getenv("ONEDRIVE_CAMERA_UPLOAD_PATH", "/Pictures/Camera Roll")
    fallback_paths = _split_comma_list(
        os.getenv(
            "ONEDRIVE_CAMERA_UPLOAD_FALLBACK_PATHS",
            "/Pictures/CameraRoll,/Pictures/Camera Uploads,/Pictures/OneDrive Camera Roll",
        )
    )
    fallback_paths = [path for path in fallback_paths if path != primary_camera_path]

    return Settings(
        onedrive_tenant=os.getenv("ONEDRIVE_TENANT", "consumers"),
        onedrive_client_id=os.getenv("ONEDRIVE_CLIENT_ID", ""),
        onedrive_scopes=scopes,
        onedrive_camera_upload_path=primary_camera_path,
        onedrive_camera_upload_fallback_paths=fallback_paths,
        mysql_dsn=os.getenv("MYSQL_DSN", ""),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        google_maps_api_key=os.getenv("GOOGLE_MAPS_API_KEY"),
        location_normalization_rules_path=os.getenv(
            "LOCATION_NORMALIZATION_RULES_PATH",
            "location_normalization_rules.json",
        ),
        openai_vision_model=os.getenv("OPENAI_VISION_MODEL", "gpt-5.2"),
        photo_sync_initial_cutoff_days=int(os.getenv("PHOTO_SYNC_INITIAL_CUTOFF_DAYS", "14")),
        photo_caption_max_words=int(os.getenv("PHOTO_CAPTION_MAX_WORDS", "18")),
    )


def _clean_optional_str(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _clean_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [part.strip() for part in value if isinstance(part, str) and part.strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def load_location_normalization_rules(path: Path) -> List[LocationNormalizationRule]:
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Location normalization rules skipped ({path}): {exc}", file=sys.stderr)
        return []

    entries: List[Any]
    if isinstance(payload, dict):
        rules_payload = payload.get("Rules")
        entries = rules_payload if isinstance(rules_payload, list) else []
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []

    rules: List[LocationNormalizationRule] = []
    for index, item in enumerate(entries, start=1):
        if not isinstance(item, dict):
            continue

        normalized_street_address = _clean_optional_str(item.get("NormalizedStreetAddress"))
        if not normalized_street_address:
            continue

        name = _clean_optional_str(item.get("Name")) or f"Rule{index}"
        rule = LocationNormalizationRule(
            name=name,
            city_equals=_clean_optional_str(item.get("CityEquals")),
            state_in=_clean_str_list(item.get("StateIn")),
            country_in=_clean_str_list(item.get("CountryIn")),
            street_contains_any=_clean_str_list(item.get("StreetContainsAny")),
            original_street_number_in=_clean_str_list(item.get("OriginalStreetNumberIn")),
            normalized_street_address=normalized_street_address,
        )
        rules.append(rule)

    return rules


def _first_env(names: tuple[str, ...], default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def parse_mysql_config(settings: Settings) -> MysqlConnectionConfig:
    if settings.mysql_dsn:
        parsed = urlparse(settings.mysql_dsn)
        if parsed.scheme.lower() not in {"mysql", "mysql+pymysql"}:
            raise DbError("MYSQL_DSN must use mysql:// or mysql+pymysql://")

        query_params = parse_qs(parsed.query)
        charset = query_params.get("charset", ["utf8mb4"])[0]

        if not parsed.hostname or not parsed.username or not parsed.path:
            raise DbError("MYSQL_DSN is missing host, username, or database")

        return MysqlConnectionConfig(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password or "",
            database=parsed.path.lstrip("/"),
            charset=charset,
        )

    host = _first_env(("IMAGETRACKER_MYSQL_HOST", "MYSQL_HOST"), "127.0.0.1")
    port = int(_first_env(("IMAGETRACKER_MYSQL_PORT", "MYSQL_PORT"), "3306"))
    user = _first_env(("IMAGETRACKER_MYSQL_USER", "MYSQL_USERID", "MYSQL_USER"), "")
    password = _first_env(("IMAGETRACKER_MYSQL_PASSWORD", "MYSQL_PASSWORD"), "")
    database = _first_env(
        ("IMAGETRACKER_MYSQL_DATABASE", "MYSQL_DATABASE_IMAGETRACKER", "MYSQL_DATABASE"),
        "",
    )

    if not user or not database:
        raise DbError(
            "Set MYSQL_DSN or IMAGETRACKER_MYSQL_* "
            "(fallbacks: MYSQL_HOST/MYSQL_PORT/MYSQL_USERID-or-MYSQL_USER/MYSQL_PASSWORD/"
            "MYSQL_DATABASE_IMAGETRACKER-or-MYSQL_DATABASE)"
        )

    return MysqlConnectionConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Database:
    def __init__(self, config: MysqlConnectionConfig):
        self._config = config

    def connect(self, *, autocommit: bool = False) -> Connection:
        return pymysql.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            charset=self._config.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=autocommit,
        )

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _split_sql_statements(script: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    in_single = False
    in_double = False

    for char in script:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


class MigrationRunner:
    def __init__(self, migrations_dir: Path):
        self._migrations_dir = migrations_dir

    def apply_all(self, conn: Connection) -> None:
        self._ensure_schema_migration_table(conn)

        with conn.cursor() as cursor:
            cursor.execute("SELECT `Version` FROM `SchemaMigration`")
            existing_versions = {row["Version"] for row in cursor.fetchall()}

        migration_files = sorted(self._migrations_dir.glob("*.sql"))
        for migration_file in migration_files:
            version = migration_file.stem.split("_", 1)[0]
            if version in existing_versions:
                continue

            sql = migration_file.read_text(encoding="utf-8")
            statements = _split_sql_statements(sql)
            with conn.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
                cursor.execute(
                    """
                    INSERT INTO `SchemaMigration` (`Version`, `Name`, `AppliedAtUtc`)
                    VALUES (%s, %s, %s)
                    """,
                    (version, migration_file.name, datetime.utcnow()),
                )

    def _ensure_schema_migration_table(self, conn: Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS `SchemaMigration` (
                    `Version` VARCHAR(32) NOT NULL PRIMARY KEY,
                    `Name` VARCHAR(255) NOT NULL,
                    `AppliedAtUtc` DATETIME NOT NULL
                )
                """
            )


class ImageAssetRepository:
    UPSERT_SQL = """
    INSERT INTO `ImageAsset` (
        `Source`,
        `DriveItemId`,
        `FileName`,
        `DateTime`,
        `Date`,
        `Time`,
        `TimeZone`,
        `UtcOffsetMinutes`,
        `DateTimeUtc`,
        `TakenDateTimeUtc`,
        `Latitude`,
        `Longitude`,
        `Altitude`,
        `RawGraphJson`,
        `InsertedAtUtc`,
        `UpdatedAtUtc`
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSON), %s, %s)
    ON DUPLICATE KEY UPDATE
        `FileName` = VALUES(`FileName`),
        `DateTime` = VALUES(`DateTime`),
        `Date` = VALUES(`Date`),
        `Time` = VALUES(`Time`),
        `TimeZone` = VALUES(`TimeZone`),
        `UtcOffsetMinutes` = VALUES(`UtcOffsetMinutes`),
        `DateTimeUtc` = VALUES(`DateTimeUtc`),
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
                    `Latitude`,
                    `Longitude`,
                    `Altitude`,
                    `DateTime`,
                    `Date`,
                    `Time`,
                    `TimeZone`,
                    `UtcOffsetMinutes`,
                    `DateTimeUtc`,
                    `Description`,
                    `DescriptionModel`,
                    `DescriptionUpdatedAtUtc`,
                    `LocationDisplayName`,
                    `StreetAddress`,
                    `OriginalStreetNumber`,
                    `Neighborhood`,
                    `City`,
                    `County`,
                    `State`,
                    `PostalCode`,
                    `Country`,
                    `CountryCode`,
                    `Category`,
                    `CategorySource`,
                    `LocationProvider`,
                    `LocationUpdatedAtUtc`
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
                    payload.date_time,
                    payload.date_value,
                    payload.time_value,
                    payload.time_zone,
                    payload.utc_offset_minutes,
                    payload.date_time_utc,
                    payload.taken_datetime_utc,
                    payload.latitude,
                    payload.longitude,
                    payload.altitude,
                    payload.raw_graph_json,
                    payload.inserted_at_utc,
                    payload.updated_at_utc,
                ),
            )

    def update_caption(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
        description: str,
        description_model: str,
        updated_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `ImageAsset`
                SET
                    `Description` = %s,
                    `DescriptionModel` = %s,
                    `DescriptionUpdatedAtUtc` = %s,
                    `UpdatedAtUtc` = %s
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (
                    description,
                    description_model,
                    updated_at_utc,
                    updated_at_utc,
                    source,
                    drive_item_id,
                ),
            )

    def update_location(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
        location: LocationResolution,
        location_provider: str,
        updated_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `ImageAsset`
                SET
                    `LocationDisplayName` = %s,
                    `StreetAddress` = %s,
                    `OriginalStreetNumber` = %s,
                    `Neighborhood` = %s,
                    `City` = %s,
                    `County` = %s,
                    `State` = %s,
                    `PostalCode` = %s,
                    `Country` = %s,
                    `CountryCode` = %s,
                    `LocationProvider` = %s,
                    `LocationUpdatedAtUtc` = %s,
                    `UpdatedAtUtc` = %s
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (
                    location.location_display_name,
                    location.street_address,
                    location.original_street_number,
                    location.neighborhood,
                    location.city,
                    location.county,
                    location.state,
                    location.postal_code,
                    location.country,
                    location.country_code,
                    location_provider,
                    updated_at_utc,
                    updated_at_utc,
                    source,
                    drive_item_id,
                ),
            )

    def update_category(
        self,
        conn: Connection,
        source: str,
        drive_item_id: str,
        category: str,
        category_source: str,
        updated_at_utc: datetime,
    ) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `ImageAsset`
                SET
                    `Category` = %s,
                    `CategorySource` = %s,
                    `UpdatedAtUtc` = %s
                WHERE `Source` = %s AND `DriveItemId` = %s
                """,
                (category, category_source, updated_at_utc, source, drive_item_id),
            )

    def find_category_by_street_address(
        self,
        conn: Connection,
        *,
        street_address: str,
        exclude_source: str,
        exclude_drive_item_id: str,
    ) -> Optional[str]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `Category`
                FROM `ImageAsset`
                WHERE `StreetAddress` = %s
                  AND `IsDeleted` = 0
                  AND `Category` IS NOT NULL
                  AND TRIM(`Category`) <> ''
                  AND NOT (`Source` = %s AND `DriveItemId` = %s)
                ORDER BY
                    CASE WHEN `CategorySource` = 'Manual' THEN 0 ELSE 1 END,
                    `ModifiedAt` DESC,
                    `UpdatedAtUtc` DESC,
                    `Id` DESC
                LIMIT 1
                """,
                (street_address, exclude_source, exclude_drive_item_id),
            )
            row = cursor.fetchone()
        if not row:
            return None
        value = row.get("Category")
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    def find_category_by_radius(
        self,
        conn: Connection,
        *,
        latitude: float,
        longitude: float,
        radius_meters: float,
        exclude_source: str,
        exclude_drive_item_id: str,
    ) -> Optional[str]:
        lat_delta = radius_meters / 111_320.0
        cos_lat = math.cos(math.radians(latitude))
        if abs(cos_lat) < 1e-6:
            lon_delta = 180.0
        else:
            lon_delta = radius_meters / (111_320.0 * abs(cos_lat))

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    `Category`,
                    `CategorySource`,
                    `Latitude`,
                    `Longitude`,
                    `ModifiedAt`,
                    `UpdatedAtUtc`,
                    `Id`
                FROM `ImageAsset`
                WHERE `Latitude` IS NOT NULL
                  AND `Longitude` IS NOT NULL
                  AND `IsDeleted` = 0
                  AND `Category` IS NOT NULL
                  AND TRIM(`Category`) <> ''
                  AND `Latitude` BETWEEN %s AND %s
                  AND `Longitude` BETWEEN %s AND %s
                  AND NOT (`Source` = %s AND `DriveItemId` = %s)
                ORDER BY `ModifiedAt` DESC, `UpdatedAtUtc` DESC, `Id` DESC
                """,
                (
                    latitude - lat_delta,
                    latitude + lat_delta,
                    longitude - lon_delta,
                    longitude + lon_delta,
                    exclude_source,
                    exclude_drive_item_id,
                ),
            )
            rows = cursor.fetchall()

        best_category: Optional[str] = None
        best_distance: Optional[float] = None
        best_is_manual = False

        for row in rows:
            candidate_category = row.get("Category")
            candidate_lat = row.get("Latitude")
            candidate_lon = row.get("Longitude")
            if not isinstance(candidate_category, str):
                continue
            try:
                row_lat = float(candidate_lat)
                row_lon = float(candidate_lon)
            except (TypeError, ValueError):
                continue

            distance = _haversine_meters(latitude, longitude, row_lat, row_lon)
            if distance > radius_meters:
                continue

            is_manual = str(row.get("CategorySource") or "").strip().lower() == "manual"
            cleaned_category = candidate_category.strip()
            if not cleaned_category:
                continue

            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_category = cleaned_category
                best_is_manual = is_manual
                continue

            if distance == best_distance and is_manual and not best_is_manual:
                best_category = cleaned_category
                best_is_manual = True

        return best_category


def _extract_text(response_json: Dict[str, Any]) -> str:
    output_text = response_json.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    outputs = response_json.get("output", [])
    for output in outputs:
        for content in output.get("content", []):
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


def _normalize_caption(text: str, max_words: int) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return ""

    sentence_split = re.split(r"(?<=[.!?])\s+", cleaned)
    first_sentence = sentence_split[0].strip()
    if not first_sentence:
        first_sentence = cleaned

    words = first_sentence.split()
    clipped = " ".join(words[:max_words])

    if clipped and clipped[-1] not in ".!?":
        clipped += "."

    return clipped


class OpenAIVisionCaptioner:
    def __init__(
        self,
        api_key: str,
        model: str,
        max_words: int,
        session: Optional[requests.Session] = None,
    ):
        self._api_key = api_key
        self._model = model
        self._max_words = max_words
        self._session = session or requests.Session()

    @property
    def model(self) -> str:
        return self._model

    def generate_caption(self, image_bytes: bytes) -> Optional[CaptionResult]:
        encoded = base64.b64encode(image_bytes).decode("ascii")
        instruction = (
            "Describe this image in exactly one sentence under "
            f"{self._max_words} words. Do not identify people, addresses, "
            "or sensitive attributes. If it is a screenshot or document, "
            "describe the content at a high level."
        )

        payload = {
            "model": self._model,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": instruction},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/jpeg;base64,{encoded}",
                        },
                    ],
                }
            ],
            "max_output_tokens": 100,
        }

        response = self._session.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Caption generation failed ({response.status_code}): {response.text}")

        text = _extract_text(response.json())
        normalized = _normalize_caption(text, self._max_words)
        if not normalized:
            return None

        return CaptionResult(short_description=normalized, model=self._model)


class GoogleMapsLocationResolver:
    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
    ):
        self._api_key = api_key
        self._session = session or requests.Session()
        self._cache: dict[str, Optional[LocationResolution]] = {}

    @property
    def provider(self) -> str:
        return "GoogleMapsGeocoding"

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[LocationResolution]:
        cache_key = f"{latitude:.5f},{longitude:.5f}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        response = self._session.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={
                "latlng": f"{latitude:.8f},{longitude:.8f}",
                "key": self._api_key,
                "language": "en",
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Google reverse geocoding failed ({response.status_code}): {response.text}"
            )

        payload = response.json()
        status = str(payload.get("status") or "").upper()
        if status == "ZERO_RESULTS":
            self._cache[cache_key] = None
            return None
        if status != "OK":
            error_message = payload.get("error_message")
            if isinstance(error_message, str) and error_message.strip():
                raise RuntimeError(f"Google reverse geocoding status={status}: {error_message.strip()}")
            raise RuntimeError(f"Google reverse geocoding status={status}")

        results = payload.get("results") or []
        if not results:
            self._cache[cache_key] = None
            return None

        best = results[0]
        components = best.get("address_components") or []

        street_number = self._component_value(components, "street_number")
        route = self._component_value(components, "route")
        street_address = self._join_non_empty(street_number, route, separator=" ") or best.get(
            "formatted_address"
        )

        neighborhood = self._component_value(components, "neighborhood") or self._component_value(
            components, "sublocality"
        ) or self._component_value(components, "sublocality_level_1")
        city = (
            self._component_value(components, "locality")
            or self._component_value(components, "postal_town")
            or self._component_value(components, "sublocality_level_1")
        )
        county = self._component_value(components, "administrative_area_level_2")
        state = self._component_value(components, "administrative_area_level_1")
        postal_code = self._component_value(components, "postal_code")
        country = self._component_value(components, "country")
        country_code = self._component_value(components, "country", use_short_name=True)

        location_display_name = self._join_non_empty(city, state, country, separator=", ") or best.get(
            "formatted_address"
        ) or (
            f"{latitude:.6f},{longitude:.6f}"
        )

        resolution = LocationResolution(
            location_display_name=location_display_name,
            street_address=street_address,
            original_street_number=street_number,
            neighborhood=neighborhood,
            city=city,
            county=county,
            state=state,
            postal_code=postal_code,
            country=country,
            country_code=country_code,
        )

        self._cache[cache_key] = resolution
        return resolution

    @staticmethod
    def _join_non_empty(*values: Optional[str], separator: str = ", ") -> Optional[str]:
        parts = [value.strip() for value in values if isinstance(value, str) and value.strip()]
        if not parts:
            return None
        return separator.join(parts)

    @staticmethod
    def _component_value(
        components: List[Dict[str, Any]],
        component_type: str,
        *,
        use_short_name: bool = False,
    ) -> Optional[str]:
        key = "short_name" if use_short_name else "long_name"
        for component in components:
            types = component.get("types") or []
            if component_type in types:
                value = component.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None


class GoogleTimeZoneResolver:
    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
    ):
        self._api_key = api_key
        self._session = session or requests.Session()
        self._cache: dict[str, Optional[TimeZoneResolution]] = {}

    @property
    def provider(self) -> str:
        return "GoogleMapsTimeZone"

    def resolve_timezone(
        self,
        *,
        latitude: float,
        longitude: float,
        timestamp_utc: datetime,
    ) -> Optional[TimeZoneResolution]:
        timestamp_seconds = int(timestamp_utc.replace(tzinfo=timezone.utc).timestamp())
        cache_key = f"{latitude:.4f},{longitude:.4f},{timestamp_seconds // 3600}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        response = self._session.get(
            "https://maps.googleapis.com/maps/api/timezone/json",
            params={
                "location": f"{latitude:.8f},{longitude:.8f}",
                "timestamp": timestamp_seconds,
                "key": self._api_key,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Google timezone lookup failed ({response.status_code}): {response.text}")

        payload = response.json()
        status = str(payload.get("status") or "").upper()
        if status == "ZERO_RESULTS":
            self._cache[cache_key] = None
            return None
        if status != "OK":
            error_message = payload.get("errorMessage") or payload.get("error_message")
            if isinstance(error_message, str) and error_message.strip():
                raise RuntimeError(f"Google timezone status={status}: {error_message.strip()}")
            raise RuntimeError(f"Google timezone status={status}")

        raw_offset = payload.get("rawOffset")
        dst_offset = payload.get("dstOffset")
        time_zone_id = payload.get("timeZoneId")
        if not isinstance(time_zone_id, str) or not time_zone_id.strip():
            self._cache[cache_key] = None
            return None
        if not isinstance(raw_offset, (int, float)) or not isinstance(dst_offset, (int, float)):
            self._cache[cache_key] = None
            return None

        utc_offset_minutes = int(round((float(raw_offset) + float(dst_offset)) / 60.0))
        resolution = TimeZoneResolution(time_zone_id=time_zone_id.strip(), utc_offset_minutes=utc_offset_minutes)
        self._cache[cache_key] = resolution
        return resolution


class ExifGpsExtractor:
    _IMAGE_EXTENSIONS = {
        ".jpg",
        ".jpeg",
        ".heic",
        ".heif",
        ".tif",
        ".tiff",
        ".png",
        ".webp",
    }

    def __init__(self):
        self._pil_available = False
        self._image_module = None
        self._gps_tag_id: Optional[int] = None
        self._gps_tags: dict[int, str] = {}

        try:
            try:
                import pillow_heif

                pillow_heif.register_heif_opener()
            except Exception:
                pass

            from PIL import ExifTags, Image

            self._image_module = Image
            self._gps_tags = ExifTags.GPSTAGS

            gps_tag_id = None
            for key, value in ExifTags.TAGS.items():
                if value == "GPSInfo":
                    gps_tag_id = key
                    break

            self._gps_tag_id = gps_tag_id
            self._pil_available = gps_tag_id is not None
        except Exception:
            self._pil_available = False

    @property
    def is_available(self) -> bool:
        return self._pil_available

    def extract(
        self,
        content_bytes: bytes,
        *,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[GpsCoordinates]:
        if not self._pil_available or self._image_module is None or not self._looks_like_image(file_name, mime_type):
            return None

        try:
            with self._image_module.open(io.BytesIO(content_bytes)) as image:
                exif = image.getexif()
        except Exception:
            return None

        if not exif or self._gps_tag_id is None:
            return None

        gps_info = exif.get(self._gps_tag_id)
        if not isinstance(gps_info, dict):
            gps_info = self._load_gps_ifd(exif)
        if not isinstance(gps_info, dict):
            return None

        mapped_gps: dict[str, Any] = {}
        for key, value in gps_info.items():
            mapped_key = self._gps_tags.get(key, str(key))
            mapped_gps[mapped_key] = value

        latitude = _dms_to_decimal(mapped_gps.get("GPSLatitude"), mapped_gps.get("GPSLatitudeRef"))
        longitude = _dms_to_decimal(mapped_gps.get("GPSLongitude"), mapped_gps.get("GPSLongitudeRef"))
        altitude = _altitude_to_float(mapped_gps.get("GPSAltitude"), mapped_gps.get("GPSAltitudeRef"))

        if latitude is None or longitude is None:
            return None
        if not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
            return None

        return GpsCoordinates(latitude=latitude, longitude=longitude, altitude=altitude)

    def _load_gps_ifd(self, exif: Any) -> Optional[dict[Any, Any]]:
        if self._gps_tag_id is None:
            return None

        get_ifd = getattr(exif, "get_ifd", None)
        if not callable(get_ifd):
            return None

        try:
            gps_ifd = get_ifd(self._gps_tag_id)
        except Exception:
            return None

        if isinstance(gps_ifd, dict):
            return gps_ifd
        return None

    def _looks_like_image(self, file_name: str, mime_type: Optional[str]) -> bool:
        if mime_type and mime_type.lower().startswith("image/"):
            return True

        extension = Path(file_name).suffix.lower()
        return extension in self._IMAGE_EXTENSIONS


class ExifCaptureDateTimeExtractor:
    _IMAGE_EXTENSIONS = {
        ".jpg",
        ".jpeg",
        ".heic",
        ".heif",
        ".tif",
        ".tiff",
        ".png",
        ".webp",
    }

    def __init__(self):
        self._pil_available = False
        self._image_module = None
        self._tag_date_time_original: Optional[int] = None
        self._tag_date_time_digitized: Optional[int] = None
        self._tag_date_time: Optional[int] = None
        self._tag_offset_time_original: Optional[int] = None
        self._tag_offset_time_digitized: Optional[int] = None
        self._tag_offset_time: Optional[int] = None
        self._exif_ifd_tag_id: Optional[int] = None

        try:
            try:
                import pillow_heif

                pillow_heif.register_heif_opener()
            except Exception:
                pass

            from PIL import ExifTags, Image

            self._image_module = Image
            self._tag_date_time_original = self._tag_id_by_name(ExifTags, "DateTimeOriginal")
            self._tag_date_time_digitized = self._tag_id_by_name(ExifTags, "DateTimeDigitized")
            self._tag_date_time = self._tag_id_by_name(ExifTags, "DateTime")
            self._tag_offset_time_original = self._tag_id_by_name(ExifTags, "OffsetTimeOriginal")
            self._tag_offset_time_digitized = self._tag_id_by_name(ExifTags, "OffsetTimeDigitized")
            self._tag_offset_time = self._tag_id_by_name(ExifTags, "OffsetTime")
            ifd_type = getattr(ExifTags, "IFD", None)
            self._exif_ifd_tag_id = int(getattr(ifd_type, "Exif", 34665))
            self._pil_available = self._tag_date_time_original is not None or self._tag_date_time is not None
        except Exception:
            self._pil_available = False

    @property
    def is_available(self) -> bool:
        return self._pil_available

    def extract(
        self,
        content_bytes: bytes,
        *,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[CaptureDateTimeInfo]:
        if not self._pil_available or self._image_module is None or not self._looks_like_image(file_name, mime_type):
            return None

        try:
            with self._image_module.open(io.BytesIO(content_bytes)) as image:
                exif = image.getexif()
                exif_ifd = self._load_exif_ifd(exif)
        except Exception:
            return None

        if not exif:
            return None

        sources = tuple(source for source in (exif_ifd, exif) if source is not None)
        candidates = (
            (self._tag_date_time_original, self._tag_offset_time_original),
            (self._tag_date_time_digitized, self._tag_offset_time_digitized),
            (self._tag_date_time, self._tag_offset_time),
        )
        for date_tag_id, offset_tag_id in candidates:
            if date_tag_id is None:
                continue
            for source in sources:
                local_datetime = _parse_exif_datetime(source.get(date_tag_id))
                if local_datetime is None:
                    continue

                offset_raw = source.get(offset_tag_id) if offset_tag_id is not None else None
                if offset_raw is None and offset_tag_id is not None:
                    offset_raw = self._first_exif_value(sources, offset_tag_id)
                utc_offset_minutes = _parse_utc_offset_minutes(offset_raw)
                time_zone = _format_utc_offset(utc_offset_minutes)
                utc_datetime = (
                    local_datetime - timedelta(minutes=utc_offset_minutes)
                    if utc_offset_minutes is not None
                    else None
                )
                return CaptureDateTimeInfo(
                    local_datetime=local_datetime,
                    time_zone=time_zone,
                    utc_offset_minutes=utc_offset_minutes,
                    utc_datetime=utc_datetime,
                )
        return None

    def _load_exif_ifd(self, exif: Any) -> Optional[Any]:
        if self._exif_ifd_tag_id is None:
            return None
        get_ifd = getattr(exif, "get_ifd", None)
        if not callable(get_ifd):
            return None
        try:
            result = get_ifd(self._exif_ifd_tag_id)
        except Exception:
            return None
        return result if hasattr(result, "get") else None

    @staticmethod
    def _first_exif_value(sources: tuple[Any, ...], tag_id: int) -> Any:
        for source in sources:
            value = source.get(tag_id)
            if value is not None:
                return value
        return None

    @staticmethod
    def _tag_id_by_name(exif_tags_module: Any, name: str) -> Optional[int]:
        for key, value in exif_tags_module.TAGS.items():
            if value == name:
                return int(key)
        return None

    def _looks_like_image(self, file_name: str, mime_type: Optional[str]) -> bool:
        if mime_type and mime_type.lower().startswith("image/"):
            return True

        extension = Path(file_name).suffix.lower()
        return extension in self._IMAGE_EXTENSIONS


def _rational_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    numerator = getattr(value, "numerator", None)
    denominator = getattr(value, "denominator", None)
    if numerator is not None and denominator not in {None, 0}:
        return float(numerator) / float(denominator)

    if isinstance(value, (tuple, list)) and len(value) == 2:
        num, den = value
        try:
            den_value = float(den)
            if den_value == 0:
                return None
            return float(num) / den_value
        except (TypeError, ValueError):
            return None

    return None


def _dms_to_decimal(values: Any, ref: Any) -> Optional[float]:
    if not isinstance(values, (tuple, list)) or len(values) < 3:
        return None

    degrees = _rational_to_float(values[0])
    minutes = _rational_to_float(values[1])
    seconds = _rational_to_float(values[2])

    if degrees is None or minutes is None or seconds is None:
        return None

    decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
    direction = str(ref).strip().upper() if ref is not None else ""
    if direction in {"S", "W"}:
        decimal = -decimal

    return decimal


def _altitude_to_float(value: Any, ref: Any) -> Optional[float]:
    altitude = _rational_to_float(value)
    if altitude is None:
        return None

    if str(ref).strip() in {"1", "b'\\x01'"} or ref == 1:
        return -altitude

    return altitude


def _parse_exif_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    try:
        return datetime.strptime(text, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None


def _parse_utc_offset_minutes(value: Any) -> Optional[int]:
    if not isinstance(value, str):
        return None

    text = value.strip()
    match = re.match(r"^([+-])(\d{2}):?(\d{2})$", text)
    if not match:
        return None

    sign = -1 if match.group(1) == "-" else 1
    hours = int(match.group(2))
    minutes = int(match.group(3))
    if hours > 14 or minutes > 59 or (hours == 14 and minutes != 0):
        return None
    return sign * (hours * 60 + minutes)


def _format_utc_offset(offset_minutes: Optional[int]) -> Optional[str]:
    if offset_minutes is None:
        return None

    sign = "+" if offset_minutes >= 0 else "-"
    absolute = abs(offset_minutes)
    hours = absolute // 60
    minutes = absolute % 60
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _get_local_timezone():
    try:
        from tzlocal import get_localzone

        return get_localzone()
    except Exception:
        return datetime.now().astimezone().tzinfo


def _derive_local_capture_datetime(fallback_utc: datetime) -> CaptureDateTimeInfo:
    aware_utc = fallback_utc.replace(tzinfo=timezone.utc)
    local_tz = _get_local_timezone()
    aware_local = aware_utc.astimezone(local_tz)
    local_datetime = aware_local.replace(tzinfo=None)

    offset = aware_local.utcoffset()
    offset_minutes = int(offset.total_seconds() // 60) if offset is not None else None

    time_zone: Optional[str] = None
    tz_key = getattr(local_tz, "key", None)
    if isinstance(tz_key, str) and tz_key.strip():
        time_zone = tz_key.strip()
    else:
        tz_name = aware_local.tzname()
        if isinstance(tz_name, str) and tz_name.strip():
            time_zone = tz_name.strip()
    if not time_zone:
        time_zone = _format_utc_offset(offset_minutes)

    return CaptureDateTimeInfo(
        local_datetime=local_datetime,
        time_zone=time_zone,
        utc_offset_minutes=offset_minutes,
        utc_datetime=fallback_utc,
    )


def parse_cutoff_date(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("cutoff date is required")

    if len(text) == 10:
        local_date = date.fromisoformat(text)
        local_datetime = datetime.combine(local_date, time.min)
        local_tz = _get_local_timezone()
        aware_local = local_datetime.replace(tzinfo=local_tz)
        return aware_local.astimezone(timezone.utc).replace(tzinfo=None)

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        local_tz = _get_local_timezone()
        parsed = parsed.replace(tzinfo=local_tz)

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_from_timestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(tzinfo=None)


def _drive_item_id_for_path(path: Path) -> str:
    normalized = str(path.resolve()).lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_m = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return earth_radius_m * c


_SUPPORTED_PHOTO_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".heic",
    ".heif",
    ".png",
    ".webp",
    ".tif",
    ".tiff",
}


def _is_supported_photo_name(file_name: str) -> bool:
    return Path(file_name).suffix.lower() in _SUPPORTED_PHOTO_EXTENSIONS


def _is_supported_photo(path: Path) -> bool:
    return _is_supported_photo_name(path.name)


def _iter_supported_photo_files(directory: Path) -> Iterator[LocalPhotoFile]:
    pending: list[Path] = [directory]

    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        if not _is_supported_photo_name(entry.name):
                            continue

                        stat = entry.stat(follow_symlinks=False)
                        yield LocalPhotoFile(path=Path(entry.path), stat=stat)
                    except OSError:
                        continue
        except OSError:
            continue


class LocalPhotoSyncService:
    SOURCE = "LocalFile"
    CATEGORY_RADIUS_METERS = 10.0
    PROGRESS_REPORT_INTERVAL_SECONDS = 5.0
    PROGRESS_REPORT_SCAN_INTERVAL = 2000

    def __init__(
        self,
        settings: Settings,
        database: Database,
        migration_runner: MigrationRunner,
        image_repo: Optional[ImageAssetRepository] = None,
        captioner: Optional[OpenAIVisionCaptioner] = None,
        gps_extractor: Optional[GpsExtractor] = None,
        capture_datetime_extractor: Optional[CaptureDateTimeExtractor] = None,
        location_resolver: Optional[LocationResolver] = None,
        time_zone_resolver: Optional[TimeZoneResolver] = None,
        location_normalization_rules: Optional[List[LocationNormalizationRule]] = None,
        now_fn=utc_now,
        progress_now_fn=time_module.monotonic,
    ):
        self._settings = settings
        self._database = database
        self._migration_runner = migration_runner
        self._image_repo = image_repo or ImageAssetRepository()
        self._captioner = captioner
        self._gps_extractor = gps_extractor or ExifGpsExtractor()
        self._capture_datetime_extractor = capture_datetime_extractor or ExifCaptureDateTimeExtractor()
        self._location_resolver = location_resolver
        self._time_zone_resolver = time_zone_resolver
        self._location_normalization_rules = location_normalization_rules or []
        self._now_fn = now_fn
        self._progress_now_fn = progress_now_fn

    def run_sync(
        self,
        *,
        directory: Path,
        cutoff_utc: datetime,
        force: bool,
    ) -> LocalPhotoSyncResult:
        if not directory.exists() or not directory.is_dir():
            raise RuntimeError(f"Directory does not exist or is not a directory: {directory}")

        self._apply_migrations()

        scanned_count = 0
        eligible_count = 0
        skipped_count = 0
        upserted_count = 0
        captioned_count = 0
        geocoded_count = 0
        timezone_enriched_count = 0

        now = self._now_fn()
        start_progress_time = self._progress_now_fn()
        last_progress_time = start_progress_time
        next_progress_scanned = self.PROGRESS_REPORT_SCAN_INTERVAL

        print(
            "Local sync progress: "
            f"started directory={directory} "
            f"cutoff_utc={cutoff_utc.isoformat()} "
            f"force={int(force)}"
        )

        conn = self._database.connect(autocommit=True)
        try:
            for file_record in _iter_supported_photo_files(directory):
                path = file_record.path
                scanned_count += 1
                progress_time = self._progress_now_fn()

                if (
                    scanned_count >= next_progress_scanned
                    or progress_time - last_progress_time >= self.PROGRESS_REPORT_INTERVAL_SECONDS
                ):
                    self._print_progress(
                        elapsed_seconds=progress_time - start_progress_time,
                        scanned_count=scanned_count,
                        eligible_count=eligible_count,
                        skipped_count=skipped_count,
                        upserted_count=upserted_count,
                        captioned_count=captioned_count,
                        geocoded_count=geocoded_count,
                        timezone_enriched_count=timezone_enriched_count,
                        current_path=path,
                    )
                    last_progress_time = progress_time
                    while next_progress_scanned <= scanned_count:
                        next_progress_scanned += self.PROGRESS_REPORT_SCAN_INTERVAL

                file_modified_utc = _utc_from_timestamp(file_record.stat.st_mtime)
                if file_modified_utc < cutoff_utc:
                    continue

                eligible_count += 1
                drive_item_id = _drive_item_id_for_path(path)
                existing = self._image_repo.get_by_source_and_drive_item(conn, self.SOURCE, drive_item_id)

                if existing and not force:
                    skipped_count += 1
                    continue

                try:
                    content_bytes = path.read_bytes()
                except Exception as exc:
                    print(f"Skipping unreadable file {path}: {exc}", file=sys.stderr)
                    continue

                mime_type, _ = mimetypes.guess_type(str(path))
                extracted_gps = self._extract_gps(
                    content_bytes=content_bytes,
                    mime_type=mime_type,
                    file_name=path.name,
                )
                capture_date_time = self._resolve_capture_datetime(
                    content_bytes=content_bytes,
                    mime_type=mime_type,
                    file_name=path.name,
                    fallback_utc=file_modified_utc,
                    existing=existing,
                )

                latitude = extracted_gps.latitude if extracted_gps else (self._as_float(existing, "Latitude") if existing else None)
                longitude = (
                    extracted_gps.longitude if extracted_gps else (self._as_float(existing, "Longitude") if existing else None)
                )
                altitude = (
                    extracted_gps.altitude
                    if extracted_gps and extracted_gps.altitude is not None
                    else (self._as_float(existing, "Altitude") if existing else None)
                )
                capture_date_time, was_timezone_enriched = self._enrich_capture_datetime_timezone(
                    capture_date_time,
                    latitude=latitude,
                    longitude=longitude,
                )
                if was_timezone_enriched:
                    timezone_enriched_count += 1

                payload = ImageUpsertPayload(
                    source=self.SOURCE,
                    drive_item_id=drive_item_id,
                    file_name=path.name,
                    date_time=capture_date_time.local_datetime,
                    date_value=capture_date_time.local_datetime.date(),
                    time_value=capture_date_time.local_datetime.time().replace(microsecond=0),
                    time_zone=capture_date_time.time_zone,
                    utc_offset_minutes=capture_date_time.utc_offset_minutes,
                    date_time_utc=capture_date_time.utc_datetime,
                    taken_datetime_utc=capture_date_time.utc_datetime,
                    latitude=latitude,
                    longitude=longitude,
                    altitude=altitude,
                    raw_graph_json=json.dumps(
                        {
                            "LocalPath": str(path.resolve()),
                            "FileModifiedUtc": file_modified_utc.isoformat() + "Z",
                            "FileSizeBytes": file_record.stat.st_size,
                        },
                        ensure_ascii=False,
                    ),
                    inserted_at_utc=now,
                    updated_at_utc=now,
                )

                self._image_repo.upsert(conn, payload)
                upserted_count += 1

                if self._should_resolve_location(existing, latitude, longitude, force=force):
                    location = self._reverse_geocode(latitude=latitude, longitude=longitude)
                    if location:
                        normalized_location = self._normalize_location(location)
                        self._image_repo.update_location(
                            conn,
                            source=self.SOURCE,
                            drive_item_id=drive_item_id,
                            location=normalized_location,
                            location_provider=self._location_resolver.provider,
                            updated_at_utc=now,
                        )
                        geocoded_count += 1

                current = self._image_repo.get_by_source_and_drive_item(conn, self.SOURCE, drive_item_id)
                self._assign_inferred_category(conn, current, updated_at_utc=now, force=force)

                if not self._should_caption(existing, force=force):
                    continue

                caption = self._generate_caption(content_bytes)
                if not caption:
                    continue

                self._image_repo.update_caption(
                    conn,
                    source=self.SOURCE,
                    drive_item_id=drive_item_id,
                    description=caption.short_description,
                    description_model=caption.model,
                    updated_at_utc=now,
                )
                captioned_count += 1
        finally:
            conn.close()

        return LocalPhotoSyncResult(
            scanned_count=scanned_count,
            eligible_count=eligible_count,
            skipped_count=skipped_count,
            upserted_count=upserted_count,
            captioned_count=captioned_count,
            geocoded_count=geocoded_count,
            timezone_enriched_count=timezone_enriched_count,
        )

    @staticmethod
    def _print_progress(
        *,
        elapsed_seconds: float,
        scanned_count: int,
        eligible_count: int,
        skipped_count: int,
        upserted_count: int,
        captioned_count: int,
        geocoded_count: int,
        timezone_enriched_count: int,
        current_path: Path,
    ) -> None:
        print(
            "Local sync progress: "
            f"elapsed={int(elapsed_seconds)}s "
            f"scanned={scanned_count} "
            f"eligible={eligible_count} "
            f"skipped={skipped_count} "
            f"upserted={upserted_count} "
            f"captioned={captioned_count} "
            f"geocoded={geocoded_count} "
            f"timezone_enriched={timezone_enriched_count} "
            f"current={current_path.name}"
        )

    def _extract_gps(
        self,
        *,
        content_bytes: bytes,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[GpsCoordinates]:
        if not self._gps_extractor:
            return None
        if hasattr(self._gps_extractor, "is_available") and not getattr(self._gps_extractor, "is_available"):
            return None

        try:
            return self._gps_extractor.extract(content_bytes, mime_type=mime_type, file_name=file_name)
        except Exception as exc:
            print(f"GPS extraction skipped for {file_name}: {exc}", file=sys.stderr)
            return None

    def _resolve_capture_datetime(
        self,
        *,
        content_bytes: bytes,
        mime_type: Optional[str],
        file_name: str,
        fallback_utc: datetime,
        existing: Optional[dict],
    ) -> CaptureDateTimeInfo:
        extracted: Optional[CaptureDateTimeInfo] = None
        if self._capture_datetime_extractor and (
            not hasattr(self._capture_datetime_extractor, "is_available")
            or getattr(self._capture_datetime_extractor, "is_available")
        ):
            try:
                extracted = self._capture_datetime_extractor.extract(
                    content_bytes,
                    mime_type=mime_type,
                    file_name=file_name,
                )
            except Exception as exc:
                print(f"Capture datetime extraction skipped for {file_name}: {exc}", file=sys.stderr)

        if extracted and extracted.local_datetime:
            resolved_offset = extracted.utc_offset_minutes
            resolved_tz = extracted.time_zone or _format_utc_offset(resolved_offset)
            resolved_utc = extracted.utc_datetime
            if resolved_offset is None and existing:
                resolved_offset = self._as_int(existing, "UtcOffsetMinutes")
                resolved_tz = self._as_text(existing, "TimeZone") or _format_utc_offset(
                    resolved_offset
                )
            if resolved_utc is None and resolved_offset is not None:
                resolved_utc = extracted.local_datetime - timedelta(minutes=resolved_offset)

            return CaptureDateTimeInfo(
                local_datetime=extracted.local_datetime,
                time_zone=resolved_tz,
                utc_offset_minutes=resolved_offset,
                utc_datetime=resolved_utc,
            )

        if existing:
            existing_local = self._as_datetime(existing, "DateTime")
            if existing_local:
                existing_offset = self._as_int(existing, "UtcOffsetMinutes")
                existing_utc = self._as_datetime(existing, "DateTimeUtc") or self._as_datetime(
                    existing,
                    "TakenDateTimeUtc",
                )
                if existing_utc is None and existing_offset is not None:
                    existing_utc = existing_local - timedelta(minutes=existing_offset)
                return CaptureDateTimeInfo(
                    local_datetime=existing_local,
                    time_zone=self._as_text(existing, "TimeZone"),
                    utc_offset_minutes=existing_offset,
                    utc_datetime=existing_utc,
                )

        return _derive_local_capture_datetime(fallback_utc)

    def _enrich_capture_datetime_timezone(
        self,
        capture: CaptureDateTimeInfo,
        *,
        latitude: Optional[float],
        longitude: Optional[float],
    ) -> tuple[CaptureDateTimeInfo, bool]:
        if capture.utc_offset_minutes is not None:
            corrected_utc = capture.local_datetime - timedelta(
                minutes=capture.utc_offset_minutes
            )
            if capture.time_zone:
                return CaptureDateTimeInfo(
                    local_datetime=capture.local_datetime,
                    time_zone=capture.time_zone,
                    utc_offset_minutes=capture.utc_offset_minutes,
                    utc_datetime=corrected_utc,
                ), False
        if latitude is None or longitude is None:
            return capture, False

        timestamp_hint = capture.utc_datetime or capture.local_datetime
        timezone_resolution = self._resolve_timezone(
            latitude=latitude,
            longitude=longitude,
            timestamp_utc=timestamp_hint,
        )
        if not timezone_resolution:
            return capture, False

        updated = CaptureDateTimeInfo(
            local_datetime=capture.local_datetime,
            time_zone=timezone_resolution.time_zone_id,
            utc_offset_minutes=timezone_resolution.utc_offset_minutes,
            utc_datetime=capture.local_datetime
            - timedelta(minutes=timezone_resolution.utc_offset_minutes),
        )
        return updated, True

    def _resolve_timezone(
        self,
        *,
        latitude: float,
        longitude: float,
        timestamp_utc: datetime,
    ) -> Optional[TimeZoneResolution]:
        if not self._time_zone_resolver:
            return None

        try:
            return self._time_zone_resolver.resolve_timezone(
                latitude=latitude,
                longitude=longitude,
                timestamp_utc=timestamp_utc,
            )
        except Exception as exc:
            print(
                f"Timezone enrichment skipped for {latitude},{longitude} at {timestamp_utc}: {exc}",
                file=sys.stderr,
            )
            return None

    def _generate_caption(self, content_bytes: bytes) -> Optional[CaptionResult]:
        if not self._captioner:
            return None

        try:
            return self._captioner.generate_caption(content_bytes)
        except Exception as exc:
            print(f"Caption generation skipped: {exc}", file=sys.stderr)
            return None

    def _should_caption(self, existing: Optional[dict], *, force: bool) -> bool:
        if not self._captioner:
            return False
        if force:
            return True
        if existing is None:
            return True

        description = existing.get("Description")
        if not description:
            return True

        existing_model = existing.get("DescriptionModel")
        current_model = getattr(self._captioner, "model", None)
        if current_model and existing_model != current_model:
            return True

        if existing.get("DescriptionUpdatedAtUtc") is None:
            return True

        return False

    def _reverse_geocode(
        self,
        *,
        latitude: Optional[float],
        longitude: Optional[float],
    ) -> Optional[LocationResolution]:
        if not self._location_resolver or latitude is None or longitude is None:
            return None

        try:
            return self._location_resolver.reverse_geocode(latitude, longitude)
        except Exception as exc:
            print(f"Reverse geocoding skipped for {latitude},{longitude}: {exc}", file=sys.stderr)
            return None

    def _should_resolve_location(
        self,
        existing: Optional[dict],
        latitude: Optional[float],
        longitude: Optional[float],
        *,
        force: bool,
    ) -> bool:
        if not self._location_resolver:
            return False
        if latitude is None or longitude is None:
            return False
        if force:
            return True
        if existing is None:
            return True

        location_display_name = existing.get("LocationDisplayName")
        state = existing.get("State")
        country = existing.get("Country")
        if not location_display_name and not state and not country:
            return True

        if existing.get("LocationUpdatedAtUtc") is None:
            return True

        location_provider = existing.get("LocationProvider")
        if location_provider != self._location_resolver.provider:
            return True

        return False

    def _assign_inferred_category(
        self,
        conn: Connection,
        current: Optional[dict],
        *,
        updated_at_utc: datetime,
        force: bool,
    ) -> None:
        if not current:
            return

        source = self._as_text(current, "Source")
        drive_item_id = self._as_text(current, "DriveItemId")
        if not source or not drive_item_id:
            return

        existing_category = self._as_text(current, "Category")
        existing_source = self._as_text(current, "CategorySource")
        if existing_category:
            if not force:
                return
            if (existing_source or "").strip().lower() == "manual":
                return

        assignment = self._resolve_category_assignment(
            conn,
            current=current,
            source=source,
            drive_item_id=drive_item_id,
        )
        if not assignment:
            return

        if (
            existing_category == assignment.category
            and (existing_source or "") == assignment.category_source
        ):
            return

        self._image_repo.update_category(
            conn,
            source=source,
            drive_item_id=drive_item_id,
            category=assignment.category,
            category_source=assignment.category_source,
            updated_at_utc=updated_at_utc,
        )

    def _resolve_category_assignment(
        self,
        conn: Connection,
        *,
        current: dict,
        source: str,
        drive_item_id: str,
    ) -> Optional[CategoryAssignment]:
        street_address = self._as_text(current, "StreetAddress")
        if street_address:
            matched_category = self._image_repo.find_category_by_street_address(
                conn,
                street_address=street_address,
                exclude_source=source,
                exclude_drive_item_id=drive_item_id,
            )
            if matched_category:
                return CategoryAssignment(
                    category=matched_category,
                    category_source="AddressPropagation",
                )

        latitude = self._as_float(current, "Latitude")
        longitude = self._as_float(current, "Longitude")
        if latitude is None or longitude is None:
            return None

        matched_category = self._image_repo.find_category_by_radius(
            conn,
            latitude=latitude,
            longitude=longitude,
            radius_meters=self.CATEGORY_RADIUS_METERS,
            exclude_source=source,
            exclude_drive_item_id=drive_item_id,
        )
        if not matched_category:
            return None

        return CategoryAssignment(
            category=matched_category,
            category_source=f"RadiusPropagation{int(self.CATEGORY_RADIUS_METERS)}m",
        )

    def _normalize_location(self, location: LocationResolution) -> LocationResolution:
        original_street_number = location.original_street_number or self._extract_street_number(
            location.street_address
        )
        normalized_street_address = location.street_address

        matched_rule = self._match_location_normalization_rule(location, original_street_number)
        if matched_rule and matched_rule.normalized_street_address:
            normalized_street_address = matched_rule.normalized_street_address

        return LocationResolution(
            location_display_name=location.location_display_name,
            street_address=normalized_street_address,
            original_street_number=original_street_number,
            neighborhood=location.neighborhood,
            city=location.city,
            county=location.county,
            state=location.state,
            postal_code=location.postal_code,
            country=location.country,
            country_code=location.country_code,
        )

    def _match_location_normalization_rule(
        self,
        location: LocationResolution,
        original_street_number: Optional[str],
    ) -> Optional[LocationNormalizationRule]:
        for rule in self._location_normalization_rules:
            if self._rule_matches_location(rule, location, original_street_number):
                return rule
        return None

    @staticmethod
    def _rule_matches_location(
        rule: LocationNormalizationRule,
        location: LocationResolution,
        original_street_number: Optional[str],
    ) -> bool:
        city = (location.city or "").strip().lower()
        state = (location.state or "").strip().lower()
        country = (location.country or "").strip().lower()
        street = (location.street_address or "").strip().lower()
        original = (original_street_number or "").strip().lower()

        if rule.city_equals and city != rule.city_equals.strip().lower():
            return False

        if rule.state_in:
            allowed_states = {item.strip().lower() for item in rule.state_in if item.strip()}
            if state not in allowed_states:
                return False

        if rule.country_in:
            allowed_countries = {item.strip().lower() for item in rule.country_in if item.strip()}
            if country not in allowed_countries:
                return False

        if rule.street_contains_any:
            fragments = [item.strip().lower() for item in rule.street_contains_any if item.strip()]
            if not fragments:
                return False
            if not any(fragment in street for fragment in fragments):
                return False

        if rule.original_street_number_in:
            allowed_numbers = {item.strip().lower() for item in rule.original_street_number_in if item.strip()}
            if original not in allowed_numbers:
                return False

        return True

    @staticmethod
    def _extract_street_number(street_address: Optional[str]) -> Optional[str]:
        if not street_address:
            return None

        match = re.match(r"^\s*(\d+[A-Za-z\-]?)\b", street_address)
        if not match:
            return None
        return match.group(1)

    def _apply_migrations(self) -> None:
        with self._database.connection() as conn:
            self._migration_runner.apply_all(conn)

    @staticmethod
    def _as_float(existing: Optional[dict], key: str) -> Optional[float]:
        if not existing:
            return None
        value = existing.get(key)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(existing: Optional[dict], key: str) -> Optional[int]:
        if not existing:
            return None
        value = existing.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_text(existing: Optional[dict], key: str) -> Optional[str]:
        if not existing:
            return None
        value = existing.get(key)
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text or None

    @staticmethod
    def _as_datetime(existing: Optional[dict], key: str) -> Optional[datetime]:
        if not existing:
            return None
        value = existing.get(key)
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is not None:
                    return parsed.astimezone(timezone.utc).replace(tzinfo=None)
                return parsed
            except ValueError:
                return None
        return None


def build_default_captioner(settings: Settings) -> Optional[OpenAIVisionCaptioner]:
    if not settings.openai_api_key:
        return None

    return OpenAIVisionCaptioner(
        api_key=settings.openai_api_key,
        model=settings.openai_vision_model,
        max_words=settings.photo_caption_max_words,
    )


def build_default_location_resolver(settings: Settings) -> Optional[GoogleMapsLocationResolver]:
    if not settings.google_maps_api_key:
        return None

    return GoogleMapsLocationResolver(api_key=settings.google_maps_api_key)


def build_default_time_zone_resolver(settings: Settings) -> Optional[GoogleTimeZoneResolver]:
    if not settings.google_maps_api_key:
        return None

    return GoogleTimeZoneResolver(api_key=settings.google_maps_api_key)


def build_location_normalization_rules(
    settings: Settings,
    *,
    project_root: Path,
) -> List[LocationNormalizationRule]:
    rules_path = Path(settings.location_normalization_rules_path)
    if not rules_path.is_absolute():
        rules_path = project_root / rules_path
    return load_location_normalization_rules(rules_path)


def run_local_sync(directory: str, cutoff_date: str, force: bool) -> int:
    settings = load_settings()

    try:
        mysql_config = parse_mysql_config(settings)
        database = Database(mysql_config)
    except DbError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        cutoff_utc = parse_cutoff_date(cutoff_date)
    except ValueError as exc:
        print(f"Invalid --cutoff-date: {exc}", file=sys.stderr)
        return 2

    root = Path(__file__).resolve().parent
    migration_runner = MigrationRunner(root / "migrations")
    captioner = build_default_captioner(settings)
    location_resolver = build_default_location_resolver(settings)
    time_zone_resolver = build_default_time_zone_resolver(settings)
    location_normalization_rules = build_location_normalization_rules(settings, project_root=root)

    service = LocalPhotoSyncService(
        settings=settings,
        database=database,
        migration_runner=migration_runner,
        captioner=captioner,
        location_resolver=location_resolver,
        time_zone_resolver=time_zone_resolver,
        location_normalization_rules=location_normalization_rules,
    )

    result = service.run_sync(directory=Path(directory), cutoff_utc=cutoff_utc, force=force)

    print(
        "Local sync complete: "
        f"scanned={result.scanned_count}, "
        f"eligible={result.eligible_count}, "
        f"skipped={result.skipped_count}, "
        f"upserted={result.upserted_count}, "
        f"captioned={result.captioned_count}, "
        f"geocoded={result.geocoded_count}, "
        f"timezone_enriched={result.timezone_enriched_count}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ImageTracker.py",
        description="Local photo sync into ImageTracker MySQL",
    )
    parser.add_argument("--directory", required=True, help="Photo directory to scan")
    parser.add_argument(
        "--cutoff-date",
        required=True,
        help="Cutoff date (YYYY-MM-DD or ISO datetime). Files >= cutoff are processed.",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess files even if already processed")

    args = parser.parse_args(argv)
    return run_local_sync(directory=args.directory, cutoff_date=args.cutoff_date, force=args.force)


if __name__ == "__main__":
    raise SystemExit(main())
