from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .mime import content_type_for_path
from .models import OutboundAttachment
from .private_fs import PRIVATE_FILE_MODE, _is_loopback_host, resolve_under_root

ALLOWED_OUTBOUND_IMAGE_CONTENT_TYPES = frozenset(
    {
        "image/gif",
        "image/jpeg",
        "image/png",
        "image/webp",
    }
)


class OutboundAttachmentError(ValueError):
    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code


def signal_base_url_supports_local_attachment_paths(base_url: str) -> bool:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    return _is_loopback_host(parsed.hostname)


def validate_outbound_attachments(
    raw: Any,
    *,
    media_root: Path,
    max_bytes: int,
) -> tuple[OutboundAttachment, ...]:
    if not isinstance(raw, list):
        raise OutboundAttachmentError(
            "invalid_attachment",
            "attachments must be a JSON array of path strings",
        )
    if len(raw) > 1:
        raise OutboundAttachmentError(
            "too_many_attachments",
            "only one outbound attachment is supported",
        )
    if not raw:
        return ()
    value = raw[0]
    if not isinstance(value, str) or not value:
        raise OutboundAttachmentError(
            "invalid_attachment",
            "attachment path must be a non-empty string",
        )
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise OutboundAttachmentError(
            "attachment_path_not_absolute",
            "attachment path must be absolute",
        )
    try:
        resolved = resolve_under_root(
            media_root.expanduser(),
            path,
            error_message="attachment path escaped media root",
        )
    except ValueError as exc:
        raise OutboundAttachmentError(
            "attachment_path_escaped_root",
            "attachment path must be under media_root",
        ) from exc
    try:
        file_stat = resolved.stat()
    except FileNotFoundError as exc:
        raise OutboundAttachmentError(
            "attachment_not_found",
            "attachment path does not exist",
        ) from exc
    except PermissionError as exc:
        raise OutboundAttachmentError(
            "attachment_not_readable",
            "attachment path is not readable",
        ) from exc
    except OSError as exc:
        raise OutboundAttachmentError(
            "attachment_not_found",
            "attachment path could not be inspected",
        ) from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise OutboundAttachmentError(
            "attachment_not_file",
            "attachment path must be a regular file",
        )
    _validate_private_attachment_modes(media_root.expanduser(), resolved, file_stat)
    if not os.access(resolved, os.R_OK):
        raise OutboundAttachmentError(
            "attachment_not_readable",
            "attachment path is not readable",
        )
    if file_stat.st_size > max_bytes:
        raise OutboundAttachmentError(
            "attachment_too_large",
            f"attachment exceeds {max_bytes} bytes",
        )
    content_type = content_type_for_path(resolved)
    if content_type not in ALLOWED_OUTBOUND_IMAGE_CONTENT_TYPES:
        raise OutboundAttachmentError(
            "attachment_not_image",
            "attachment must have an image content type",
        )
    return (OutboundAttachment(path=resolved, content_type=content_type, size=file_stat.st_size),)


def _validate_private_attachment_modes(
    media_root: Path,
    resolved: Path,
    file_stat: os.stat_result,
) -> None:
    root = media_root.resolve(strict=False)
    relative_parent = resolved.parent.relative_to(root)

    checked_dirs: list[Path] = []
    current = root
    checked_dirs.append(current)
    for part in relative_parent.parts:
        current /= part
        checked_dirs.append(current)

    for directory in checked_dirs:
        try:
            directory_stat = directory.stat()
        except OSError as exc:
            raise OutboundAttachmentError(
                "attachment_not_private",
                "attachment parent directories must be inspectable",
            ) from exc
        if not stat.S_ISDIR(directory_stat.st_mode):
            raise OutboundAttachmentError(
                "attachment_not_private",
                "attachment parents must be directories",
            )
        if stat.S_IMODE(directory_stat.st_mode) & 0o077:
            raise OutboundAttachmentError(
                "attachment_not_private",
                "attachment parent directories must not be group/world accessible",
            )

    if stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise OutboundAttachmentError(
            "attachment_not_private",
            f"attachment file must be private like mode {PRIVATE_FILE_MODE:o}",
        )
