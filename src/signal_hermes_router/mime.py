from __future__ import annotations

import mimetypes
from pathlib import Path

# webp is absent from the default mimetypes database on some Python builds;
# register it here (the MIME owner) so content_type_for_path resolves .webp
# consistently for every consumer.
mimetypes.add_type("image/webp", ".webp")

DEFAULT_CONTENT_TYPE = "application/octet-stream"

_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/heic": ".heic",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/ogg": ".ogg",
    "audio/wav": ".wav",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
    "application/json": ".json",
    DEFAULT_CONTENT_TYPE: ".bin",
}


def normalize_content_type(content_type: object, *, default: str = DEFAULT_CONTENT_TYPE) -> str:
    if not isinstance(content_type, str) or not content_type:
        return default
    return content_type.split(";", 1)[0].strip().lower() or default


def extension_for_content_type(content_type: str) -> str:
    normalized = normalize_content_type(content_type)
    if normalized in _EXTENSIONS:
        return _EXTENSIONS[normalized]
    return mimetypes.guess_extension(normalized) or ""


def content_type_for_path(path: Path, content_type: str | None = None) -> str:
    if content_type:
        return normalize_content_type(content_type)
    return mimetypes.guess_type(path.name)[0] or DEFAULT_CONTENT_TYPE


def is_image_content_type(content_type: str) -> bool:
    return normalize_content_type(content_type).startswith("image/")
