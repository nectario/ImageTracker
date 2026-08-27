from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from ImageTracker import (
    ExifCaptureDateTimeExtractor,
    ExifGpsExtractor,
    _derive_local_capture_datetime,
)

from .state import CachedFile, LocalState


PHOTO_EXTENSIONS = {
    ".avif",
    ".bmp",
    ".cr2",
    ".cr3",
    ".dng",
    ".gif",
    ".jpg",
    ".jpeg",
    ".heic",
    ".heif",
    ".tif",
    ".tiff",
    ".png",
    ".arw",
    ".nef",
    ".rw2",
    ".webp",
}
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".m4v",
    ".avi",
    ".mkv",
    ".webm",
    ".3gp",
    ".mts",
    ".m2ts",
    ".mpeg",
    ".mpg",
    ".ogv",
    ".vob",
    ".wmv",
}
HASH_CHUNK_BYTES = 1024 * 1024
ISO6709_RE = re.compile(r"^(?P<lat>[+-]\d+(?:\.\d+)?)(?P<lon>[+-]\d+(?:\.\d+)?)")


def media_type_for(path: Path) -> str | None:
    extension = path.suffix.lower()
    if extension in PHOTO_EXTENSIONS:
        return "Photo"
    if extension in VIDEO_EXTENSIONS:
        return "Video"
    return None


def mime_type_for(path: Path, media_type: str) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    if guessed:
        return guessed
    return "image/jpeg" if media_type == "Photo" else "video/mp4"


def stream_sha256(path: Path, chunk_bytes: int = HASH_CHUNK_BYTES) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_bytes)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def source_item_id(relative_path: str) -> str:
    normalized = os.path.normcase(relative_path.replace("\\", "/"))
    return "path:" + hashlib.sha256(normalized.encode("utf-8", errors="surrogatepass")).hexdigest()


def source_revision(size: int, modified_ns: int, content_sha256: str) -> str:
    raw = f"{size}:{modified_ns}:{content_sha256}".encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def _utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class MetadataExtractor:
    def __init__(self, *, ffprobe_path: str | None = None):
        self.capture = ExifCaptureDateTimeExtractor()
        self.gps = ExifGpsExtractor()
        self.ffprobe_path = ffprobe_path if ffprobe_path is not None else shutil.which("ffprobe")

    def extract(
        self,
        path: Path,
        *,
        media_type: str,
        mime_type: str,
        modified_ns: int,
    ) -> dict[str, Any]:
        if media_type == "Photo":
            return self._extract_photo(path, mime_type=mime_type, modified_ns=modified_ns)
        return self._extract_video(path, modified_ns=modified_ns)

    def _fallback_temporal(self, modified_ns: int) -> dict[str, Any]:
        modified_utc = datetime.fromtimestamp(modified_ns / 1_000_000_000, tz=timezone.utc).replace(tzinfo=None)
        capture = _derive_local_capture_datetime(modified_utc)
        return {
            "capturedAtLocal": capture.local_datetime.isoformat(timespec="seconds"),
            "capturedAtUtc": _utc_text(capture.utc_datetime),
            "timeZoneId": capture.time_zone,
            "utcOffsetMinutes": capture.utc_offset_minutes,
            "provenance": [{"field": "capturedAt", "source": "FileMtime", "confidence": 0.5}],
        }

    def _extract_photo(self, path: Path, *, mime_type: str, modified_ns: int) -> dict[str, Any]:
        metadata: dict[str, Any] = self._fallback_temporal(modified_ns)
        try:
            content = path.read_bytes()
        except OSError:
            return metadata
        capture = self.capture.extract(content, mime_type=mime_type, file_name=path.name)
        if capture:
            metadata.update(
                {
                    "capturedAtLocal": capture.local_datetime.isoformat(timespec="seconds"),
                    "capturedAtUtc": _utc_text(capture.utc_datetime),
                    "timeZoneId": capture.time_zone,
                    "utcOffsetMinutes": capture.utc_offset_minutes,
                    "provenance": [{"field": "capturedAt", "source": "Exif", "confidence": 1.0}],
                }
            )
        gps = self.gps.extract(content, mime_type=mime_type, file_name=path.name)
        if gps:
            metadata["location"] = {
                "latitude": gps.latitude,
                "longitude": gps.longitude,
                "altitudeMeters": gps.altitude,
            }
            metadata.setdefault("provenance", []).append(
                {"field": "location", "source": "Exif", "confidence": 1.0}
            )
        try:
            from PIL import Image

            with Image.open(path) as image:
                width, height = image.size
            if width > 0 and height > 0:
                metadata["widthPixels"] = width
                metadata["heightPixels"] = height
        except Exception:
            pass
        return {key: value for key, value in metadata.items() if value is not None}

    def _extract_video(self, path: Path, *, modified_ns: int) -> dict[str, Any]:
        metadata = self._fallback_temporal(modified_ns)
        if not self.ffprobe_path:
            return metadata
        try:
            completed = subprocess.run(
                [
                    self.ffprobe_path,
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_streams",
                    "-show_format",
                    str(path),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            payload = json.loads(completed.stdout)
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            return metadata
        streams = payload.get("streams") or []
        video_stream = next((item for item in streams if item.get("codec_type") == "video"), {})
        width = video_stream.get("width")
        height = video_stream.get("height")
        if isinstance(width, int) and width > 0:
            metadata["widthPixels"] = width
        if isinstance(height, int) and height > 0:
            metadata["heightPixels"] = height
        duration_raw = (payload.get("format") or {}).get("duration") or video_stream.get("duration")
        try:
            metadata["durationMs"] = max(0, int(round(float(duration_raw) * 1000)))
        except (TypeError, ValueError):
            pass
        tags: dict[str, Any] = {}
        tags.update((payload.get("format") or {}).get("tags") or {})
        tags.update(video_stream.get("tags") or {})
        creation_raw = tags.get("creation_time") or tags.get("com.apple.quicktime.creationdate")
        if isinstance(creation_raw, str):
            try:
                parsed = datetime.fromisoformat(creation_raw.strip().replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    metadata.update(
                        {
                            "capturedAtLocal": parsed.replace(tzinfo=None).isoformat(timespec="seconds"),
                            "capturedAtUtc": _utc_text(parsed),
                            "utcOffsetMinutes": int(parsed.utcoffset().total_seconds() // 60),
                            "timeZoneId": parsed.tzname(),
                            "provenance": [
                                {"field": "capturedAt", "source": "Device", "confidence": 0.9}
                            ],
                        }
                    )
            except (ValueError, AttributeError):
                pass
        location_raw = tags.get("location") or tags.get("com.apple.quicktime.location.ISO6709")
        if isinstance(location_raw, str):
            match = ISO6709_RE.match(location_raw.strip())
            if match:
                latitude = float(match.group("lat"))
                longitude = float(match.group("lon"))
                if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    metadata["location"] = {"latitude": latitude, "longitude": longitude}
                    metadata.setdefault("provenance", []).append(
                        {"field": "location", "source": "Device", "confidence": 0.9}
                    )
        return {key: value for key, value in metadata.items() if value is not None}


@dataclass
class ScanResult:
    entries: list[dict[str, Any]] = field(default_factory=list)
    seen_source_item_ids: set[str] = field(default_factory=set)
    scanned: int = 0
    cached: int = 0
    hashed: int = 0
    skipped: int = 0
    failed: int = 0
    complete_read: bool = True
    errors: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "cached": self.cached,
            "hashed": self.hashed,
            "skipped": self.skipped,
            "failed": self.failed,
            "completeRead": self.complete_read,
        }


class MediaScanner:
    def __init__(
        self,
        state: LocalState,
        metadata_extractor: MetadataExtractor | None = None,
        *,
        hash_file: Callable[[Path], str] = stream_sha256,
    ):
        self.state = state
        self.metadata = metadata_extractor or MetadataExtractor()
        self.hash_file = hash_file

    def scan(self, source_id: str, root: Path, *, force_rehash: bool = False) -> ScanResult:
        root = root.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"Source path is not a directory: {root}")
        result = ScanResult()
        for path, traversal_error in self._walk(root):
            if traversal_error:
                result.complete_read = False
                result.failed += 1
                result.errors.append(traversal_error)
                continue
            assert path is not None
            media_type = media_type_for(path)
            if media_type is None:
                result.skipped += 1
                continue
            result.scanned += 1
            relative = path.relative_to(root).as_posix()
            item_id = source_item_id(relative)
            result.seen_source_item_ids.add(item_id)
            try:
                file_stat = path.stat()
                if file_stat.st_size <= 0:
                    raise OSError("empty media file")
                cached = None
                if not force_rehash:
                    cached = self.state.cached_file(
                        source_id, path, file_stat.st_size, file_stat.st_mtime_ns
                    )
                mime_type = mime_type_for(path, media_type)
                if cached:
                    sha256 = cached.sha256
                    metadata = dict(cached.metadata)
                    result.cached += 1
                else:
                    sha256 = self.hash_file(path)
                    metadata = self.metadata.extract(
                        path,
                        media_type=media_type,
                        mime_type=mime_type,
                        modified_ns=file_stat.st_mtime_ns,
                    )
                    self.state.cache_file(
                        source_id,
                        path,
                        size=file_stat.st_size,
                        modified_ns=file_stat.st_mtime_ns,
                        sha256=sha256,
                        metadata=metadata,
                    )
                    result.hashed += 1
                entry = {
                    "operation": "Upsert",
                    "sourceItemId": item_id,
                    "sourceRevision": source_revision(
                        file_stat.st_size, file_stat.st_mtime_ns, sha256
                    ),
                    "fileName": path.name,
                    "localLocator": str(path),
                    "contentSha256": sha256,
                    "mediaType": media_type,
                    "mimeType": mime_type,
                    "byteSize": file_stat.st_size,
                    **metadata,
                }
                result.entries.append(entry)
            except OSError as exc:
                result.failed += 1
                result.errors.append(f"{path}: {exc}")
        return result

    def _walk(self, root: Path) -> Iterator[tuple[Path | None, str | None]]:
        pending = [root]
        while pending:
            directory = pending.pop()
            try:
                with os.scandir(directory) as scan:
                    children = sorted(scan, key=lambda item: item.name.casefold())
            except OSError as exc:
                yield None, f"{directory}: {exc}"
                continue
            directories: list[Path] = []
            for child in children:
                try:
                    if child.is_symlink():
                        continue
                    if child.is_dir(follow_symlinks=False):
                        directories.append(Path(child.path))
                    elif child.is_file(follow_symlinks=False):
                        yield Path(child.path), None
                except OSError as exc:
                    yield None, f"{child.path}: {exc}"
            pending.extend(reversed(directories))
