from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from ImageTracker import (
    ExifCaptureDateTimeExtractor,
    ExifGpsExtractor,
    _derive_local_capture_datetime,
)

from .state import CachedFile, FileCacheUpdate, LocalState


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
HASH_CHUNK_BYTES = 8 * 1024 * 1024
MAX_SINGLE_READ_PHOTO_BYTES = 64 * 1024 * 1024
MAX_AUTO_SCAN_WORKERS = 64
MAX_SCAN_WORKERS = 256
CACHE_WRITE_BATCH_SIZE = 1_000
PROGRESS_INTERVAL_SECONDS = 0.75
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


def pending_source_revision(size: int, modified_ns: int) -> str:
    raw = f"{size}:{modified_ns}:pending-sha256".encode("ascii")
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

    def fallback_metadata(self, modified_ns: int) -> dict[str, Any]:
        """Return useful timeline metadata without opening the media file."""

        return self._fallback_temporal(modified_ns)

    def _extract_photo(
        self,
        path: Path,
        *,
        mime_type: str,
        modified_ns: int,
        content: bytes | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = self._fallback_temporal(modified_ns)
        if content is None:
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

            with Image.open(BytesIO(content)) as image:
                width, height = image.size
            if width > 0 and height > 0:
                metadata["widthPixels"] = width
                metadata["heightPixels"] = height
        except Exception:
            pass
        return {key: value for key, value in metadata.items() if value is not None}

    def hash_and_extract_photo(
        self,
        path: Path,
        *,
        mime_type: str,
        modified_ns: int,
    ) -> tuple[str, dict[str, Any]]:
        """Hash and inspect an ordinary photo from one physical file read."""

        content = path.read_bytes()
        return (
            hashlib.sha256(content).hexdigest(),
            self._extract_photo(
                path,
                mime_type=mime_type,
                modified_ns=modified_ns,
                content=content,
            ),
        )

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
    worker_count: int = 1
    elapsed_seconds: float = 0.0
    files_per_second: float = 0.0
    pending_hash: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "cached": self.cached,
            "hashed": self.hashed,
            "skipped": self.skipped,
            "failed": self.failed,
            "completeRead": self.complete_read,
            "workerCount": self.worker_count,
            "elapsedSeconds": round(self.elapsed_seconds, 3),
            "filesPerSecond": round(self.files_per_second, 1),
            "pendingHash": self.pending_hash,
        }


@dataclass(frozen=True, slots=True)
class _Candidate:
    position: int
    path: Path
    path_key: str
    source_item_id: str
    byte_size: int
    modified_ns: int
    media_type: str
    mime_type: str


@dataclass(frozen=True, slots=True)
class _Processed:
    candidate: _Candidate
    sha256: str | None = None
    metadata: Mapping[str, Any] | None = None
    error: str | None = None


class MediaScanner:
    def __init__(
        self,
        state: LocalState,
        metadata_extractor: MetadataExtractor | None = None,
        *,
        hash_file: Callable[[Path], str] = stream_sha256,
        workers: int | None = None,
    ):
        self.state = state
        self.metadata = metadata_extractor or MetadataExtractor()
        self.hash_file = hash_file
        self.workers = workers

    @staticmethod
    def recommended_worker_count() -> int:
        """Choose an aggressive I/O pool without blindly creating 192 readers."""

        logical_cpus = os.cpu_count() or 4
        return min(MAX_AUTO_SCAN_WORKERS, max(8, (logical_cpus + 1) // 2))

    @classmethod
    def resolve_worker_count(cls, value: int | None) -> int:
        selected = cls.recommended_worker_count() if value is None else value
        if isinstance(selected, bool) or not isinstance(selected, int):
            raise ValueError("Scan workers must be an integer")
        if not 1 <= selected <= MAX_SCAN_WORKERS:
            raise ValueError(f"Scan workers must be between 1 and {MAX_SCAN_WORKERS}")
        return selected

    def scan(
        self,
        source_id: str,
        root: Path,
        *,
        force_rehash: bool = False,
        fast_add: bool = False,
        workers: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> ScanResult:
        started = time.perf_counter()
        root = root.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"Source path is not a directory: {root}")
        emit = progress or (lambda _message: None)
        worker_count = self.resolve_worker_count(
            workers if workers is not None else self.workers
        )
        result = ScanResult(worker_count=worker_count)
        cache = {} if force_rehash else self.state.cached_files(source_id)
        discovered_paths: list[Path] = []
        last_progress_at = started

        discovery_workers = min(worker_count, 32)
        emit(f"Discovering media with {discovery_workers} directory workers")
        for path, _file_stat, traversal_error in self._walk(
            root, workers=discovery_workers
        ):
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
            discovered_paths.append(path)
            now = time.perf_counter()
            if now - last_progress_at >= PROGRESS_INTERVAL_SECONDS:
                emit(f"Discovering media · {result.scanned:,} files found")
                last_progress_at = now

        emit(
            f"Reading file metadata for {len(discovered_paths):,} media files "
            f"with {worker_count} workers"
        )
        candidates = self._stat_candidates(
            root=root,
            paths=discovered_paths,
            result=result,
            worker_count=worker_count,
            emit=emit,
        )
        candidates.sort(key=lambda item: str(item.path).casefold())
        candidates = [
            replace(candidate, position=position)
            for position, candidate in enumerate(candidates)
        ]
        entries: list[dict[str, Any] | None] = [None] * len(candidates)
        uncached: list[_Candidate] = []
        for candidate in candidates:
            cached = cache.get(candidate.path_key)
            if (
                cached is not None
                and cached.byte_size == candidate.byte_size
                and cached.modified_ns == candidate.modified_ns
            ):
                entries[candidate.position] = self._entry(
                    candidate,
                    sha256=cached.sha256,
                    metadata=cached.metadata,
                )
                result.cached += 1
            else:
                uncached.append(candidate)

        if fast_add and uncached:
            for candidate in uncached:
                metadata = (
                    self.metadata.fallback_metadata(candidate.modified_ns)
                    if isinstance(self.metadata, MetadataExtractor)
                    else {}
                )
                entries[candidate.position] = self._entry(
                    candidate,
                    sha256=None,
                    metadata=metadata,
                )
            result.pending_hash = len(uncached)
            emit(
                f"Fast add prepared {len(uncached):,} files without reading "
                "their contents; a normal sync will hash and enrich them later"
            )
        elif uncached:
            emit(
                f"Processing {len(uncached):,} new or changed media files "
                f"with {worker_count} workers · {result.cached:,} cache hits"
            )
            self._process_uncached(
                source_id=source_id,
                candidates=uncached,
                entries=entries,
                result=result,
                worker_count=worker_count,
                emit=emit,
            )

        result.entries = [entry for entry in entries if entry is not None]
        result.elapsed_seconds = max(0.0, time.perf_counter() - started)
        if result.elapsed_seconds:
            result.files_per_second = result.scanned / result.elapsed_seconds
        emit(
            f"Scan complete · {result.scanned:,} media files · "
            f"{result.files_per_second:,.0f} files/s · {worker_count} workers"
        )
        return result

    def _stat_candidates(
        self,
        *,
        root: Path,
        paths: list[Path],
        result: ScanResult,
        worker_count: int,
        emit: Callable[[str], None],
    ) -> list[_Candidate]:
        ordered_paths = sorted(paths, key=lambda path: str(path).casefold())
        path_iterator = iter(enumerate(ordered_paths))
        max_pending = max(worker_count, worker_count * 8)
        candidates: list[_Candidate] = []
        completed = 0
        started = time.perf_counter()
        last_progress_at = started

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="imagetracker-stat",
        ) as executor:
            pending: dict[
                Future[tuple[_Candidate | None, str, str | None]],
                tuple[int, Path],
            ] = {}

            def fill_pending() -> None:
                while len(pending) < max_pending:
                    try:
                        position, path = next(path_iterator)
                    except StopIteration:
                        return
                    pending[
                        executor.submit(
                            self._stat_candidate,
                            position,
                            path,
                            root,
                        )
                    ] = (position, path)

            fill_pending()
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future)
                    candidate, item_id, error = future.result()
                    completed += 1
                    result.seen_source_item_ids.add(item_id)
                    if error is not None:
                        result.failed += 1
                        result.errors.append(error)
                    elif candidate is not None:
                        candidates.append(candidate)
                    else:
                        result.skipped += 1
                fill_pending()
                now = time.perf_counter()
                if now - last_progress_at >= PROGRESS_INTERVAL_SECONDS:
                    elapsed = max(0.001, now - started)
                    emit(
                        f"Reading file metadata · {completed:,}/{len(paths):,} · "
                        f"{completed / elapsed:,.0f} files/s"
                    )
                    last_progress_at = now
        return candidates

    @classmethod
    def _stat_candidate(
        cls,
        position: int,
        path: Path,
        root: Path,
    ) -> tuple[_Candidate | None, str, str | None]:
        relative = path.relative_to(root).as_posix()
        item_id = source_item_id(relative)
        try:
            file_stat = path.stat()
        except OSError as exc:
            return None, item_id, f"{path}: {exc}"
        if file_stat.st_size <= 0:
            # An empty placeholder is neither a readable media item nor a
            # reason to make a 160K-photo import look unsuccessful.
            return None, item_id, None
        media_type = media_type_for(path)
        assert media_type is not None
        return (
            _Candidate(
                position=position,
                path=path,
                path_key=cls._fast_path_key(path),
                source_item_id=item_id,
                byte_size=file_stat.st_size,
                modified_ns=file_stat.st_mtime_ns,
                media_type=media_type,
                mime_type=mime_type_for(path, media_type),
            ),
            item_id,
            None,
        )

    def _process_uncached(
        self,
        *,
        source_id: str,
        candidates: list[_Candidate],
        entries: list[dict[str, Any] | None],
        result: ScanResult,
        worker_count: int,
        emit: Callable[[str], None],
    ) -> None:
        photo_slots = threading.Semaphore(min(worker_count, 32))
        video_slots = threading.Semaphore(min(worker_count, 8))
        cache_updates: list[FileCacheUpdate] = []
        completed = 0
        last_progress_at = time.perf_counter()
        processing_started = last_progress_at
        candidate_iterator = iter(candidates)
        max_pending = max(worker_count, worker_count * 4)

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="imagetracker-scan",
        ) as executor:
            pending: dict[Future[_Processed], _Candidate] = {}

            def fill_pending() -> None:
                while len(pending) < max_pending:
                    try:
                        candidate = next(candidate_iterator)
                    except StopIteration:
                        return
                    future = executor.submit(
                        self._process_candidate,
                        candidate,
                        photo_slots,
                        video_slots,
                    )
                    pending[future] = candidate

            fill_pending()
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    candidate = pending.pop(future)
                    processed = future.result()
                    completed += 1
                    if processed.error is not None:
                        result.failed += 1
                        result.errors.append(processed.error)
                        continue
                    assert processed.sha256 is not None
                    metadata = processed.metadata or {}
                    entries[candidate.position] = self._entry(
                        candidate,
                        sha256=processed.sha256,
                        metadata=metadata,
                    )
                    cache_updates.append(
                        FileCacheUpdate(
                            path_key=candidate.path_key,
                            file_path=str(candidate.path),
                            byte_size=candidate.byte_size,
                            modified_ns=candidate.modified_ns,
                            sha256=processed.sha256,
                            metadata=metadata,
                        )
                    )
                    result.hashed += 1

                if len(cache_updates) >= CACHE_WRITE_BATCH_SIZE:
                    self.state.cache_files(source_id, tuple(cache_updates))
                    cache_updates.clear()
                fill_pending()

                now = time.perf_counter()
                if now - last_progress_at >= PROGRESS_INTERVAL_SECONDS:
                    elapsed = max(0.001, now - processing_started)
                    emit(
                        f"Processing media · {completed:,}/{len(candidates):,} · "
                        f"{completed / elapsed:,.0f} files/s · "
                        f"{result.failed:,} failed"
                    )
                    last_progress_at = now

        if cache_updates:
            self.state.cache_files(source_id, tuple(cache_updates))

    def _process_candidate(
        self,
        candidate: _Candidate,
        photo_slots: threading.Semaphore,
        video_slots: threading.Semaphore,
    ) -> _Processed:
        try:
            if candidate.media_type == "Video":
                with video_slots:
                    sha256 = self.hash_file(candidate.path)
                    metadata = self.metadata.extract(
                        candidate.path,
                        media_type=candidate.media_type,
                        mime_type=candidate.mime_type,
                        modified_ns=candidate.modified_ns,
                    )
            elif (
                self.hash_file is stream_sha256
                and isinstance(self.metadata, MetadataExtractor)
                and candidate.byte_size <= MAX_SINGLE_READ_PHOTO_BYTES
            ):
                with photo_slots:
                    sha256, metadata = self.metadata.hash_and_extract_photo(
                        candidate.path,
                        mime_type=candidate.mime_type,
                        modified_ns=candidate.modified_ns,
                    )
            else:
                with photo_slots:
                    sha256 = self.hash_file(candidate.path)
                    metadata = self.metadata.extract(
                        candidate.path,
                        media_type=candidate.media_type,
                        mime_type=candidate.mime_type,
                        modified_ns=candidate.modified_ns,
                    )
            return _Processed(
                candidate=candidate,
                sha256=sha256,
                metadata=metadata,
            )
        except OSError as exc:
            return _Processed(candidate=candidate, error=f"{candidate.path}: {exc}")

    @staticmethod
    def _entry(
        candidate: _Candidate,
        *,
        sha256: str | None,
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "operation": "Upsert",
            "sourceItemId": candidate.source_item_id,
            "sourceRevision": (
                source_revision(candidate.byte_size, candidate.modified_ns, sha256)
                if sha256 is not None
                else pending_source_revision(
                    candidate.byte_size, candidate.modified_ns
                )
            ),
            "fileName": candidate.path.name,
            "localLocator": str(candidate.path),
            "contentSha256": sha256,
            "mediaType": candidate.media_type,
            "mimeType": candidate.mime_type,
            "byteSize": candidate.byte_size,
            **metadata,
        }

    @staticmethod
    def _fast_path_key(path: Path) -> str:
        # The walker begins from a resolved root and skips symlinks, so another
        # Path.resolve() per file would only add filesystem round trips.
        return os.path.normcase(os.path.abspath(os.fspath(path)))

    def _walk(
        self,
        root: Path,
        *,
        workers: int,
    ) -> Iterator[tuple[Path | None, os.stat_result | None, str | None]]:
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="imagetracker-discovery",
        ) as executor:
            pending: dict[
                Future[
                    tuple[
                        list[Path],
                        list[tuple[Path, os.stat_result | None]],
                        list[str],
                    ]
                ],
                Path,
            ] = {executor.submit(self._scan_directory, root): root}
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future)
                    directories, files, errors = future.result()
                    for directory in directories:
                        pending[
                            executor.submit(self._scan_directory, directory)
                        ] = directory
                    for error in errors:
                        yield None, None, error
                    for path, file_stat in files:
                        yield path, file_stat, None

    @staticmethod
    def _scan_directory(
        directory: Path,
    ) -> tuple[
        list[Path],
        list[tuple[Path, os.stat_result | None]],
        list[str],
    ]:
        directories: list[Path] = []
        files: list[tuple[Path, os.stat_result | None]] = []
        errors: list[str] = []
        try:
            with os.scandir(directory) as scan:
                for child in scan:
                    try:
                        if child.is_symlink():
                            continue
                        if child.is_dir(follow_symlinks=False):
                            directories.append(Path(child.path))
                        elif child.is_file(follow_symlinks=False):
                            files.append((Path(child.path), None))
                    except OSError as exc:
                        errors.append(f"{child.path}: {exc}")
        except OSError as exc:
            errors.append(f"{directory}: {exc}")
        return directories, files, errors
