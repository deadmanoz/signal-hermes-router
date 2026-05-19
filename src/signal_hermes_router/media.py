from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from .mime import extension_for_content_type
from .models import MediaManifest, SignalAttachment
from .private_fs import (
    ensure_private_dir_tree,
    ensure_private_file,
    resolve_under_root,
    write_private_bytes,
    write_private_text,
)


_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_REPEATED_UNDERSCORES = re.compile(r"_+")


def safe_filename(original: str | None, content_type: str) -> str:
    candidate = original or "attachment"
    candidate = candidate.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    candidate = _SAFE_CHARS.sub("_", candidate)
    candidate = _REPEATED_UNDERSCORES.sub("_", candidate)
    candidate = candidate.strip("._-")
    if not candidate:
        candidate = "attachment"

    ext = extension_for_content_type(content_type)
    if "." in candidate:
        candidate = candidate.rsplit(".", 1)[0]
    candidate = candidate[:80].strip("._-") or "attachment"
    return f"{candidate}{ext}"


def _assert_subpath(path: Path, root: Path) -> None:
    resolve_under_root(root, path, error_message="media path escaped configured media_root")


def write_attachment(
    *,
    media_root: Path,
    platform: str,
    timestamp: int,
    attachment: SignalAttachment,
    group_ref: str,
    sender_ref: str,
    max_bytes: int | None = None,
) -> MediaManifest:
    body = attachment.body
    if body is not None:
        _assert_size(len(body), max_bytes)
    if body is None and attachment.path is not None:
        if attachment.size is not None:
            _assert_size(int(attachment.size), max_bytes)
        _assert_size(attachment.path.stat().st_size, max_bytes)
        body = attachment.path.read_bytes()
    if body is None:
        raise ValueError("attachment has neither body nor path")

    digest = hashlib.sha256(body).hexdigest()
    when = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
    directory = media_root / platform / f"{when.year:04d}" / f"{when.month:02d}" / digest[:12]
    _assert_subpath(directory, media_root)
    ensure_private_dir_tree(media_root, directory)

    display = safe_filename(attachment.filename, attachment.content_type)
    target = directory / display
    _assert_subpath(target, media_root)
    if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        stem, suffix = target.stem, target.suffix
        target = directory / f"{stem}-{digest[:8]}{suffix}"
        display = target.name
        _assert_subpath(target, media_root)
    if not target.exists():
        write_private_bytes(target, body)
    else:
        ensure_private_file(target)

    manifest = MediaManifest(
        display_filename=display,
        canonical_path=target.resolve(),
        content_type=attachment.content_type,
        size=len(body),
        sha256=digest,
        group_ref=group_ref,
        sender_ref=sender_ref,
        signal_timestamp=timestamp,
    )
    sidecar = target.with_name(f"{target.name}.manifest.json")
    _assert_subpath(sidecar, media_root)
    write_private_text(sidecar, json.dumps(manifest.to_dict(), sort_keys=True, indent=2) + "\n")
    return manifest


def _assert_size(size: int, max_bytes: int | None) -> None:
    if max_bytes is not None and size > max_bytes:
        raise ValueError("attachment exceeds max_attachment_bytes")
