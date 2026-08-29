from __future__ import annotations

import base64
import hashlib
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image, TiffImagePlugin

from cli.imagetracker_cli.scene_preview import (
    JPEG_CONTENT_TYPE,
    ScenePreviewError,
    prepare_scene_preview,
)


def _write_image(
    path: Path,
    *,
    size: tuple[int, int],
    mode: str = "RGB",
    color: tuple[int, ...] = (40, 90, 160),
    exif: Image.Exif | None = None,
) -> None:
    with Image.new(mode, size, color) as image:
        save_options = {"exif": exif} if exif is not None else {}
        image.save(path, **save_options)


def test_preview_applies_exif_orientation_resizes_and_strips_metadata(tmp_path: Path) -> None:
    source = tmp_path / "private-photo.jpg"
    exif = Image.Exif()
    exif[274] = 6  # rotate 90 degrees clockwise
    exif[315] = "Private camera owner"
    exif[270] = "Private description"
    rational = TiffImagePlugin.IFDRational
    exif[34853] = {
        1: "N",
        2: (rational(40), rational(42), rational(0)),
        3: "W",
        4: (rational(74), rational(0), rational(0)),
    }
    _write_image(source, size=(2000, 1000), exif=exif)

    with Image.open(source) as original:
        assert original.getexif().get_ifd(34853)

    preview = prepare_scene_preview(source)

    assert (preview.width_pixels, preview.height_pixels) == (512, 1024)
    assert preview.content_type == JPEG_CONTENT_TYPE
    assert preview.byte_size == len(preview.content)
    assert preview.sha256_hex == hashlib.sha256(preview.content).hexdigest()
    assert preview.source_sha256_hex == hashlib.sha256(
        source.read_bytes()
    ).hexdigest()
    assert preview.sha256_base64 == base64.b64encode(
        hashlib.sha256(preview.content).digest()
    ).decode("ascii")

    with Image.open(BytesIO(preview.content)) as output:
        assert output.format == "JPEG"
        assert output.mode == "RGB"
        assert output.size == (512, 1024)
        assert len(output.getexif()) == 0
        assert "exif" not in output.info
        assert "xmp" not in output.info
        assert "icc_profile" not in output.info


def test_preview_never_enlarges_and_is_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "small.png"
    _write_image(source, size=(320, 180))

    first = prepare_scene_preview(source)
    second = prepare_scene_preview(source)

    assert (first.width_pixels, first.height_pixels) == (320, 180)
    assert first == second


def test_preview_flattens_transparency_to_rgb(tmp_path: Path) -> None:
    source = tmp_path / "transparent.png"
    _write_image(source, size=(12, 8), mode="RGBA", color=(20, 40, 60, 0))

    preview = prepare_scene_preview(source)

    with Image.open(BytesIO(preview.content)) as output:
        assert output.mode == "RGB"
        red, green, blue = output.getpixel((0, 0))
        assert red > 245 and green > 245 and blue > 245


def test_preview_accepts_mpo_camera_jpg_and_uses_primary_frame(
    tmp_path: Path,
) -> None:
    source = tmp_path / "DSC05933.JPG"
    with Image.new("RGB", (1200, 800), "red") as primary:
        with Image.new("RGB", (1200, 800), "blue") as secondary:
            primary.save(
                source,
                format="MPO",
                save_all=True,
                append_images=[secondary],
            )

    preview = prepare_scene_preview(source)

    assert (preview.width_pixels, preview.height_pixels) == (1024, 683)
    with Image.open(BytesIO(preview.content)) as output:
        assert output.format == "JPEG"
        red, _green, blue = output.getpixel((100, 100))
        assert red > 240
        assert blue < 20


@pytest.mark.parametrize(
    "name,content",
    [
        ("video.mp4", b"not really a video or photo"),
        ("broken.jpg", b"\xff\xd8\xfftruncated"),
    ],
)
def test_preview_rejects_non_photos_and_corrupt_files_without_leaking_path(
    tmp_path: Path,
    name: str,
    content: bytes,
) -> None:
    source = tmp_path / name
    source.write_bytes(content)

    with pytest.raises(ScenePreviewError) as caught:
        prepare_scene_preview(source)

    assert caught.value.code == "unsupported_or_corrupt_photo"
    assert str(caught.value) == "The file is not a supported or readable photo."
    assert str(source) not in str(caught.value)


def test_preview_sanitizes_missing_file_error(tmp_path: Path) -> None:
    source = tmp_path / "missing-secret-name.jpg"

    with pytest.raises(ScenePreviewError, match="^The file is not a supported or readable photo\\.$"):
        prepare_scene_preview(source)


def test_preview_sanitizes_pillow_decompression_bomb(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "oversized-private-photo.png"
    _write_image(source, size=(10, 10))
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1)

    with pytest.raises(ScenePreviewError) as caught:
        prepare_scene_preview(source)

    assert caught.value.code == "unsupported_or_corrupt_photo"
    assert str(source) not in str(caught.value)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_preview_requires_positive_integer_limit(tmp_path: Path, value: object) -> None:
    source = tmp_path / "photo.jpg"
    _write_image(source, size=(10, 10))

    with pytest.raises(ValueError, match="max_long_edge must be a positive integer"):
        prepare_scene_preview(source, max_long_edge=value)  # type: ignore[arg-type]
