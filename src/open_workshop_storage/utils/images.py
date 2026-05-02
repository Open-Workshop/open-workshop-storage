from __future__ import annotations

from io import BytesIO
from typing import Any, cast

from PIL import Image, UnidentifiedImageError

from .blurhash import blurhash_encode

_RESAMPLE_LANCZOS = getattr(getattr(Image, "Resampling", Image), "LANCZOS", getattr(Image, "LANCZOS", 1))


def image_bytes_to_webp(data: bytes, quality: int = 80) -> bytes:
    try:
        with Image.open(BytesIO(data)) as img:
            img.load()
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                converted = img.convert("RGBA")
            else:
                converted = img.convert("RGB")

            out = BytesIO()
            converted.save(out, format="WEBP", quality=quality, method=6)
            return out.getvalue()
    except (UnidentifiedImageError, OSError) as exc:
        raise ValueError("not an image") from exc


def image_file_to_webp(src_path: str, dst_path: str, quality: int = 80) -> None:
    with open(src_path, "rb") as src_file:
        data = src_file.read()
    converted = image_bytes_to_webp(data, quality=quality)
    with open(dst_path, "wb") as dst_file:
        dst_file.write(converted)


def image_bytes_to_blurhash(
    data: bytes,
    *,
    components_x: int = 4,
    components_y: int = 3,
    max_dimension: int = 64,
) -> tuple[str, int, int]:
    with Image.open(BytesIO(data)) as img:
        img.load()
        width, height = img.size

        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            rgba = img.convert("RGBA")
            background = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
            rgb = Image.alpha_composite(background, rgba).convert("RGB")
        else:
            rgb = img.convert("RGB")

        if max(rgb.size) > max_dimension:
            rgb = rgb.copy()
            rgb.thumbnail((max_dimension, max_dimension), cast(Any, _RESAMPLE_LANCZOS))

        pixels = list(rgb.getdata())
        rows = [
            pixels[row_start:row_start + rgb.width]
            for row_start in range(0, len(pixels), rgb.width)
        ]
        blurhash = blurhash_encode(rows, components_x=components_x, components_y=components_y)
        return blurhash, width, height


def image_file_to_blurhash(
    src_path: str,
    *,
    components_x: int = 4,
    components_y: int = 3,
    max_dimension: int = 64,
) -> tuple[str, int, int]:
    with open(src_path, "rb") as src_file:
        data = src_file.read()
    return image_bytes_to_blurhash(
        data,
        components_x=components_x,
        components_y=components_y,
        max_dimension=max_dimension,
    )
