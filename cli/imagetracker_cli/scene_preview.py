from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageOps, UnidentifiedImageError


DEFAULT_MAX_LONG_EDGE = 1024
SCENE_PREVIEW_CAPABILITY_VERSION = "scene-preview-v2-mpo"
JPEG_CONTENT_TYPE = "image/jpeg"
JPEG_QUALITY = 85
JPEG_OPTIMIZE = True
JPEG_PROGRESSIVE = False
JPEG_SUBSAMPLING = 2

# These are the still-image formats decoded by Pillow (including pillow-heif)
# that ImageTracker accepts for scene-description previews. MPO is a valid
# multi-picture JPEG commonly produced by Sony cameras; its primary frame is
# used for search enrichment. Raw camera formats need a dedicated decoder and
# are intentionally rejected here.
SUPPORTED_INPUT_FORMATS = frozenset(
    {"AVIF", "BMP", "GIF", "HEIF", "JPEG", "MPO", "PNG", "TIFF", "WEBP"}
)

_SANITIZED_ERROR = "The file is not a supported or readable photo."


try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except ImportError:  # pragma: no cover - pillow-heif is a runtime dependency
    pass


class ScenePreviewError(ValueError):
    """A safe-to-display failure that never includes a source path."""

    code = "unsupported_or_corrupt_photo"


@dataclass(frozen=True, slots=True)
class ScenePreview:
    content: bytes
    content_type: str
    byte_size: int
    sha256_base64: str
    sha256_hex: str
    source_sha256_hex: str
    width_pixels: int
    height_pixels: int


def _clean_rgb(image: Image.Image) -> Image.Image:
    """Render pixels into a new RGB image without copying source metadata."""

    has_transparency = "A" in image.getbands() or "transparency" in image.info
    if has_transparency:
        rgba = image.convert("RGBA")
        try:
            result = Image.new("RGB", rgba.size, "white")
            result.paste(rgba, mask=rgba.getchannel("A"))
            return result
        finally:
            rgba.close()

    converted = image.convert("RGB")
    try:
        result = Image.new("RGB", converted.size)
        result.paste(converted)
        return result
    finally:
        converted.close()


def prepare_scene_preview(
    path: str | Path,
    *,
    max_long_edge: int = DEFAULT_MAX_LONG_EDGE,
) -> ScenePreview:
    """Create a deterministic, metadata-free JPEG preview for scene analysis.

    The source is opened as a file stream instead of being read wholesale.
    Pillow's decoder draft is requested before orientation and resizing, which
    substantially lowers peak memory for large JPEG phone photos.
    """

    if isinstance(max_long_edge, bool) or not isinstance(max_long_edge, int) or max_long_edge < 1:
        raise ValueError("max_long_edge must be a positive integer")

    oriented: Image.Image | None = None
    clean: Image.Image | None = None
    try:
        source_path = Path(path)
        with source_path.open("rb") as source:
            source_digest = hashlib.sha256()
            while chunk := source.read(1024 * 1024):
                source_digest.update(chunk)
            source.seek(0)
            with Image.open(source) as image:
                if image.format not in SUPPORTED_INPUT_FORMATS:
                    raise ScenePreviewError(_SANITIZED_ERROR)
                if image.width < 1 or image.height < 1:
                    raise ScenePreviewError(_SANITIZED_ERROR)

                # JPEG decoders can select a lower-resolution DCT draft before
                # decoding. The final thumbnail below still enforces the exact
                # size and never enlarges a small source.
                image.draft("RGB", (max_long_edge, max_long_edge))
                oriented = ImageOps.exif_transpose(image)
                oriented.thumbnail(
                    (max_long_edge, max_long_edge),
                    Image.Resampling.LANCZOS,
                    reducing_gap=3.0,
                )
                clean = _clean_rgb(oriented)

                output = BytesIO()
                clean.save(
                    output,
                    format="JPEG",
                    quality=JPEG_QUALITY,
                    optimize=JPEG_OPTIMIZE,
                    progressive=JPEG_PROGRESSIVE,
                    subsampling=JPEG_SUBSAMPLING,
                )
                content = output.getvalue()
                width, height = clean.size

        digest = hashlib.sha256(content).digest()
        return ScenePreview(
            content=content,
            content_type=JPEG_CONTENT_TYPE,
            byte_size=len(content),
            sha256_base64=base64.b64encode(digest).decode("ascii"),
            sha256_hex=digest.hex(),
            source_sha256_hex=source_digest.hexdigest(),
            width_pixels=width,
            height_pixels=height,
        )
    except ScenePreviewError:
        raise
    except (
        Image.DecompressionBombError,
        OSError,
        RuntimeError,
        SyntaxError,
        UnidentifiedImageError,
        ValueError,
    ):
        raise ScenePreviewError(_SANITIZED_ERROR) from None
    finally:
        if clean is not None:
            clean.close()
        if oriented is not None:
            oriented.close()
