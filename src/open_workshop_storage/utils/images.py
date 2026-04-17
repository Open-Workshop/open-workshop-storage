from __future__ import annotations

from io import BytesIO

from PIL import Image, UnidentifiedImageError


def image_bytes_to_webp(data: bytes, quality: int = 80) -> bytes:
    try:
        with Image.open(BytesIO(data)) as img:
            img.load()
            if img.mode in ("RGBA", "LA") or (
                img.mode == "P" and "transparency" in img.info
            ):
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")

            out = BytesIO()
            img.save(out, format="WEBP", quality=quality, method=6)
            return out.getvalue()
    except (UnidentifiedImageError, OSError) as exc:
        raise ValueError("not an image") from exc


def image_file_to_webp(src_path: str, dst_path: str, quality: int = 80) -> None:
    with open(src_path, "rb") as src_file:
        data = src_file.read()
    converted = image_bytes_to_webp(data, quality=quality)
    with open(dst_path, "wb") as dst_file:
        dst_file.write(converted)
