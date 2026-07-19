from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
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
_MANIFEST_SUFFIX = ".manifest.json"
_YEAR_RE = re.compile(r"\d{4}")
_MONTH_RE = re.compile(r"\d{2}")
_OUTBOUND_DIR = ".outbound"

# Retention sweep code constants (deliberately not configuration).
# MEDIA_SWEEP_MIN_AGE_SECONDS is a defense-in-depth grace: no pass ever
# deletes a file this young, even under a pathological size cap. Live-path
# tracking in the router is the primary no-mid-turn-deletion guarantee.
MEDIA_SWEEP_MIN_AGE_SECONDS = 3600.0
# Crash orphans under .outbound/ from a dead process; live artifacts of the
# running process are protected by live-path tracking, not by this age.
OUTBOUND_ORPHAN_MAX_AGE_SECONDS = 86400.0


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
        # Identical-content re-store (for example a redelivered event whose
        # attachment is entering a new prompt): refresh the retention clock
        # so the sweep's age and size passes see the file as current.
        os.utime(target)

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


@dataclass(frozen=True)
class MediaSweepEntry:
    """One deletion candidate: a lexical path under the resolved media root
    (leaf symlinks are never resolved), its size, and the mtime observed at
    plan time. Execution re-checks the mtime immediately before unlinking."""

    path: Path
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class MediaSweepPlan:
    """Deletion candidates grouped principal-with-sidecar, oldest first,
    plus the swept-subtree directories eligible for empty-dir cleanup."""

    groups: tuple[tuple[MediaSweepEntry, ...], ...]
    candidate_dirs: tuple[Path, ...]


@dataclass(frozen=True)
class MediaSweepResult:
    files_removed: int = 0
    bytes_removed: int = 0
    dirs_removed: int = 0


def plan_media_sweep(
    *,
    media_root: Path,
    now_ms: int,
    max_age_seconds: float | None,
    max_total_bytes: int | None,
) -> MediaSweepPlan:
    """Read-only sweep planning; safe to run in a worker thread.

    Only the router-written inbound archive layout
    (``<platform>/<YYYY>/<MM>/...``) is subject to the age and size passes,
    so operator-staged files elsewhere under ``media_root`` are never
    candidates. Stale ``.outbound`` crash orphans are planned separately by
    the fixed orphan age.
    """
    root = media_root.expanduser().resolve(strict=False)
    if not root.is_dir():
        return MediaSweepPlan(groups=(), candidate_dirs=())

    archive_groups: list[tuple[tuple[MediaSweepEntry, ...], int]] = []
    outbound_entries: list[MediaSweepEntry] = []
    candidate_dirs: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        directory = Path(dirpath)
        relative = directory.relative_to(root)
        if _is_sweepable_dir(relative):
            candidate_dirs.append(directory)
        in_archive = _is_archive_dir(relative)
        in_outbound = bool(relative.parts) and relative.parts[0] == _OUTBOUND_DIR
        if not in_archive and not in_outbound:
            continue
        entries: dict[str, MediaSweepEntry] = {}
        for name in filenames:
            path = directory / name
            try:
                stat_result = os.lstat(path)
            except OSError:
                continue
            entries[name] = MediaSweepEntry(
                path=path,
                size=int(stat_result.st_size),
                mtime_ns=int(stat_result.st_mtime_ns),
            )
        if in_archive:
            archive_groups.extend(_group_directory_entries(entries))
        else:
            outbound_entries.extend(entries.values())

    planned: list[tuple[tuple[MediaSweepEntry, ...], int]] = []
    remaining: list[tuple[tuple[MediaSweepEntry, ...], int]] = []
    min_age_cutoff_ns = _cutoff_ns(now_ms, MEDIA_SWEEP_MIN_AGE_SECONDS)
    if max_age_seconds is not None:
        age_cutoff_ns = _cutoff_ns(now_ms, max_age_seconds)
        for group, newest_ns in archive_groups:
            if newest_ns < age_cutoff_ns and newest_ns < min_age_cutoff_ns:
                planned.append((group, newest_ns))
            else:
                remaining.append((group, newest_ns))
    else:
        remaining = archive_groups

    if max_total_bytes is not None:
        remaining.sort(key=lambda item: item[1])
        remaining_bytes = sum(entry.size for group, _newest in remaining for entry in group)
        for group, newest_ns in remaining:
            if remaining_bytes <= max_total_bytes:
                break
            if newest_ns >= min_age_cutoff_ns:
                # Oldest-first order: every later group is at least as
                # young, so the grace floor ends the size pass.
                break
            planned.append((group, newest_ns))
            remaining_bytes -= sum(entry.size for entry in group)

    orphan_cutoff_ns = _cutoff_ns(now_ms, OUTBOUND_ORPHAN_MAX_AGE_SECONDS)
    for entry in outbound_entries:
        if entry.mtime_ns < orphan_cutoff_ns:
            planned.append(((entry,), entry.mtime_ns))

    planned.sort(key=lambda item: item[1])
    candidate_dirs.sort(key=lambda directory: len(directory.parts), reverse=True)
    return MediaSweepPlan(
        groups=tuple(group for group, _newest in planned),
        candidate_dirs=tuple(candidate_dirs),
    )


def execute_media_sweep_groups(
    groups: Iterable[tuple[MediaSweepEntry, ...]],
    *,
    is_live: Callable[[Path], bool],
) -> MediaSweepResult:
    """Delete planned groups; intended to run on the event loop so the
    pre-unlink mtime recheck cannot interleave with router media writes.

    A group is skipped whole when any member is live, vanished, or has a
    changed mtime since planning (for example an identical re-store that
    refreshed the retention clock)."""
    files_removed = 0
    bytes_removed = 0
    for group in groups:
        if any(is_live(entry.path) for entry in group):
            continue
        try:
            current = [os.lstat(entry.path) for entry in group]
        except OSError:
            continue
        if any(
            int(stat_result.st_mtime_ns) != entry.mtime_ns
            for stat_result, entry in zip(current, group, strict=True)
        ):
            continue
        for entry in group:
            try:
                os.unlink(entry.path)
            except OSError:
                continue
            files_removed += 1
            bytes_removed += entry.size
    return MediaSweepResult(files_removed=files_removed, bytes_removed=bytes_removed)


def remove_empty_sweep_dirs(candidate_dirs: Iterable[Path], media_root: Path) -> int:
    """Remove now-empty directories inside the swept subtrees, deepest
    first; never ``media_root`` itself and never operator staging dirs
    (the plan only nominates archive and ``.outbound`` directories)."""
    root = media_root.expanduser().resolve(strict=False)
    removed = 0
    for directory in candidate_dirs:
        if directory == root:
            continue
        with suppress(OSError):
            directory.rmdir()
            removed += 1
    return removed


def _cutoff_ns(now_ms: int, age_seconds: float) -> int:
    return int((now_ms - age_seconds * 1000.0) * 1_000_000)


def _is_archive_dir(relative: Path) -> bool:
    parts = relative.parts
    return (
        len(parts) >= 3
        and parts[0] != _OUTBOUND_DIR
        and _YEAR_RE.fullmatch(parts[1]) is not None
        and _MONTH_RE.fullmatch(parts[2]) is not None
    )


def _is_sweepable_dir(relative: Path) -> bool:
    parts = relative.parts
    if len(parts) >= 2 and parts[0] == _OUTBOUND_DIR:
        return True
    if len(parts) == 2:
        return _YEAR_RE.fullmatch(parts[1]) is not None
    return _is_archive_dir(relative)


def _group_directory_entries(
    entries: dict[str, MediaSweepEntry],
) -> list[tuple[tuple[MediaSweepEntry, ...], int]]:
    """Group principals with their manifest sidecars, conservatively.

    ``S`` is the sidecar of ``P`` iff ``S == P + ".manifest.json"``, ``P``
    exists, and ``S + ".manifest.json"`` does NOT exist: a file owning its
    own sidecar is always a principal in its own right, so an ambiguous
    chain (``X``, ``X.manifest.json``, ``X.manifest.json.manifest.json``)
    never deletes a fresh principal as an old file's sidecar. Group age is
    the NEWEST member mtime, so a group with any fresh member is retained.
    """
    sidecars: dict[str, str] = {}
    for name in entries:
        if not name.endswith(_MANIFEST_SUFFIX):
            continue
        principal = name[: -len(_MANIFEST_SUFFIX)]
        if principal in entries and name + _MANIFEST_SUFFIX not in entries:
            sidecars[name] = principal
    groups: list[tuple[tuple[MediaSweepEntry, ...], int]] = []
    for name, entry in entries.items():
        if name in sidecars:
            continue
        members = [entry]
        sidecar_name = name + _MANIFEST_SUFFIX
        if sidecars.get(sidecar_name) == name:
            members.append(entries[sidecar_name])
        newest_ns = max(member.mtime_ns for member in members)
        groups.append((tuple(members), newest_ns))
    return groups
