from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Protocol


@dataclass
class GpsCoordinates:
    latitude: float
    longitude: float
    altitude: Optional[float]


class GpsExtractor(Protocol):
    def extract(
        self,
        content_bytes: bytes,
        *,
        mime_type: Optional[str],
        file_name: str,
    ) -> Optional[GpsCoordinates]:
        ...


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
            # Optional HEIC support when pillow-heif is installed.
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

        latitude = _dms_to_decimal(
            mapped_gps.get("GPSLatitude"),
            mapped_gps.get("GPSLatitudeRef"),
        )
        longitude = _dms_to_decimal(
            mapped_gps.get("GPSLongitude"),
            mapped_gps.get("GPSLongitudeRef"),
        )
        altitude = _altitude_to_float(
            mapped_gps.get("GPSAltitude"),
            mapped_gps.get("GPSAltitudeRef"),
        )

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

    # 0 = above sea level, 1 = below sea level.
    if str(ref).strip() in {"1", "b'\\x01'"} or ref == 1:
        return -altitude

    return altitude
