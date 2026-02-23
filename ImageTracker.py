from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import mimetypes
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
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


class DbError(RuntimeError):
    pass


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
        openai_vision_model=os.getenv("OPENAI_VISION_MODEL", "gpt-5.2"),
        photo_sync_initial_cutoff_days=int(os.getenv("PHOTO_SYNC_INITIAL_CUTOFF_DAYS", "14")),
        photo_caption_max_words=int(os.getenv("PHOTO_CAPTION_MAX_WORDS", "18")),
    )


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

    def connect(self) -> Connection:
        return pymysql.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            charset=self._config.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
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
                    `Latitude`,
                    `Longitude`,
                    `Altitude`,
                    `Description`,
                    `DescriptionModel`,
                    `DescriptionUpdatedAtUtc`
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

        return GpsCoordinates(latitude=latitude, longitude=longitude, altitude=altitude)

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


def parse_cutoff_date(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("cutoff date is required")

    if len(text) == 10:
        local_date = date.fromisoformat(text)
        local_datetime = datetime.combine(local_date, time.min)
        local_tz = datetime.now().astimezone().tzinfo
        aware_local = local_datetime.replace(tzinfo=local_tz)
        return aware_local.astimezone(timezone.utc).replace(tzinfo=None)

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        parsed = parsed.replace(tzinfo=local_tz)

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_from_timestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(tzinfo=None)


def _drive_item_id_for_path(path: Path) -> str:
    normalized = str(path.resolve()).lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _is_supported_photo(path: Path) -> bool:
    return path.suffix.lower() in {
        ".jpg",
        ".jpeg",
        ".heic",
        ".heif",
        ".png",
        ".webp",
        ".tif",
        ".tiff",
    }


class LocalPhotoSyncService:
    SOURCE = "LocalFile"

    def __init__(
        self,
        settings: Settings,
        database: Database,
        migration_runner: MigrationRunner,
        image_repo: Optional[ImageAssetRepository] = None,
        captioner: Optional[OpenAIVisionCaptioner] = None,
        gps_extractor: Optional[GpsExtractor] = None,
        now_fn=utc_now,
    ):
        self._settings = settings
        self._database = database
        self._migration_runner = migration_runner
        self._image_repo = image_repo or ImageAssetRepository()
        self._captioner = captioner
        self._gps_extractor = gps_extractor or ExifGpsExtractor()
        self._now_fn = now_fn

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

        now = self._now_fn()

        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            if not _is_supported_photo(path):
                continue

            scanned_count += 1
            stat = path.stat()
            file_modified_utc = _utc_from_timestamp(stat.st_mtime)

            if file_modified_utc < cutoff_utc:
                continue

            eligible_count += 1
            drive_item_id = _drive_item_id_for_path(path)

            with self._database.connection() as conn:
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

            latitude = extracted_gps.latitude if extracted_gps else (self._as_float(existing, "Latitude") if existing else None)
            longitude = (
                extracted_gps.longitude if extracted_gps else (self._as_float(existing, "Longitude") if existing else None)
            )
            altitude = (
                extracted_gps.altitude
                if extracted_gps and extracted_gps.altitude is not None
                else (self._as_float(existing, "Altitude") if existing else None)
            )

            payload = ImageUpsertPayload(
                source=self.SOURCE,
                drive_item_id=drive_item_id,
                file_name=path.name,
                taken_datetime_utc=file_modified_utc,
                latitude=latitude,
                longitude=longitude,
                altitude=altitude,
                raw_graph_json=json.dumps(
                    {
                        "LocalPath": str(path.resolve()),
                        "FileModifiedUtc": file_modified_utc.isoformat() + "Z",
                        "FileSizeBytes": stat.st_size,
                    },
                    ensure_ascii=False,
                ),
                inserted_at_utc=now,
                updated_at_utc=now,
            )

            with self._database.connection() as conn:
                self._image_repo.upsert(conn, payload)
            upserted_count += 1

            if not self._should_caption(existing, force=force):
                continue

            caption = self._generate_caption(content_bytes)
            if not caption:
                continue

            with self._database.connection() as conn:
                self._image_repo.update_caption(
                    conn,
                    source=self.SOURCE,
                    drive_item_id=drive_item_id,
                    description=caption.short_description,
                    description_model=caption.model,
                    updated_at_utc=now,
                )
            captioned_count += 1

        return LocalPhotoSyncResult(
            scanned_count=scanned_count,
            eligible_count=eligible_count,
            skipped_count=skipped_count,
            upserted_count=upserted_count,
            captioned_count=captioned_count,
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


def build_default_captioner(settings: Settings) -> Optional[OpenAIVisionCaptioner]:
    if not settings.openai_api_key:
        return None

    return OpenAIVisionCaptioner(
        api_key=settings.openai_api_key,
        model=settings.openai_vision_model,
        max_words=settings.photo_caption_max_words,
    )


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

    service = LocalPhotoSyncService(
        settings=settings,
        database=database,
        migration_runner=migration_runner,
        captioner=captioner,
    )

    result = service.run_sync(directory=Path(directory), cutoff_utc=cutoff_utc, force=force)

    print(
        "Local sync complete: "
        f"scanned={result.scanned_count}, "
        f"eligible={result.eligible_count}, "
        f"skipped={result.skipped_count}, "
        f"upserted={result.upserted_count}, "
        f"captioned={result.captioned_count}"
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
