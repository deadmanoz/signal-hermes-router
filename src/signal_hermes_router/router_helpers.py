from __future__ import annotations

import math
import socket
from pathlib import Path
from typing import Any

from .private_fs import resolve_under_root


def _write_attachments(
    *,
    media_root: Path,
    platform: str,
    timestamp: int,
    attachments: Any,  # Sequence[SignalAttachment] - avoid circular import
    group_ref: str,
    sender_ref: str,
    max_bytes: int | None,
) -> list[Any]:
    # Blocking read/hash/write; runs in a media I/O worker thread
    # (_run_media_io_worker), never on the event loop.
    from .media import write_attachment  # avoid circular import at module level

    return [
        write_attachment(
            media_root=media_root,
            platform=platform,
            timestamp=timestamp,
            attachment=attachment,
            group_ref=group_ref,
            sender_ref=sender_ref,
            max_bytes=max_bytes,
        )
        for attachment in attachments
    ]


def _copy_outbound_attachment(source: Path, destination: Path, max_bytes: int) -> int:
    """Blocking outbound-attachment copy; runs in a media I/O worker thread.

    Returns the byte size written. The destination is written only after the
    size check passes, so a failed copy never leaves a partial file.
    """
    from .outbound_media import OutboundAttachmentError  # avoid circular import

    try:
        with source.open("rb") as handle:
            body = handle.read(max_bytes + 1)
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
            "attachment path could not be read",
        ) from exc
    if len(body) > max_bytes:
        raise OutboundAttachmentError(
            "attachment_too_large",
            f"attachment exceeds {max_bytes} bytes",
        )
    from .private_fs import write_private_bytes  # avoid circular import

    write_private_bytes(destination, body)
    return len(body)


def _group_id(route: Any) -> str:
    if not route.group_id:
        raise ValueError("group route requires group_id")
    return route.group_id


def _direct_recipient(route: Any) -> str:
    if not route.sender_id:
        raise ValueError("direct route requires sender_id")
    return route.sender_id


def _routed_sender_id(route: Any, event: Any) -> str:
    from .models import ChatType  # avoid circular import

    if route.chat_type == ChatType.DIRECT:
        return _direct_recipient(route)
    return event.dedupe_sender_id


def _session_sender_id(route: Any, event: Any) -> str:
    from .models import ChatType  # avoid circular import

    if route.chat_type == ChatType.DIRECT:
        return _direct_recipient(route)
    return event.sender_id


def _origin_for_synthetic_kind(kind: Any) -> Any:
    from .models import SyntheticTurnKind, TurnOrigin  # avoid circular import

    if kind == SyntheticTurnKind.SCHEDULED_JOB:
        return TurnOrigin.SCHEDULED_JOB
    if kind == SyntheticTurnKind.NOTIFICATION:
        return TurnOrigin.NOTIFICATION
    raise ValueError(f"unknown synthetic turn kind {kind!r}")


def _parse_control_scheduled_at(value: Any) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "invalid_scheduled_at"
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdecimal():
        parsed = int(value)
    else:
        return None, "invalid_scheduled_at"
    if parsed < 0:
        return None, "invalid_scheduled_at"
    return parsed, None


def _parse_control_idempotency_key(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str) or not value:
        return None, "invalid_idempotency_key"
    return value, None


def _parse_control_timeout(value: Any) -> tuple[float | None, str | None]:
    if value is None:
        return None, None
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        return None, "invalid_timeout"
    if not math.isfinite(timeout) or timeout < 0:
        return None, "invalid_timeout"
    return timeout, None


def _parse_route_status_filters(
    payload: dict[str, Any],
) -> tuple[tuple[str, ...], tuple[int, ...], tuple[str, ...]]:
    route_names = _string_filter_values(payload, "route", "routes", "route_names")
    profiles = _string_filter_values(payload, "profile", "profiles")
    route_indexes = _index_filter_values(payload, "route_index", "route_indexes")
    return route_names, route_indexes, profiles


def _string_filter_values(payload: dict[str, Any], *keys: str) -> tuple[str, ...]:
    values: list[str] = []
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if isinstance(raw, str):
            if not raw:
                raise ValueError(f"{key} must not be empty")
            values.append(raw)
            continue
        if isinstance(raw, list) and all(isinstance(item, str) and item for item in raw):
            values.extend(raw)
            continue
        raise ValueError(f"{key} must be a string or string list")
    return tuple(dict.fromkeys(values))


def _index_filter_values(payload: dict[str, Any], *keys: str) -> tuple[int, ...]:
    values: list[int] = []
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if isinstance(raw, bool):
            raise ValueError(f"{key} must be a non-negative integer")
        if isinstance(raw, int) and raw >= 0:
            values.append(raw)
            continue
        if isinstance(raw, list) and all(
            not isinstance(item, bool) and isinstance(item, int) and item >= 0 for item in raw
        ):
            values.extend(raw)
            continue
        raise ValueError(f"{key} must be a non-negative integer or integer list")
    return tuple(dict.fromkeys(values))


def _route_ref(index: int, route: Any) -> str:
    if route.name:
        return f"route:{route.name}"
    return f"routes[{index}]"


def _unix_socket_accepts_connections(path: Path) -> bool:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.2)
        sock.connect(str(path))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _is_under_root(path: Path, root: Path) -> bool:
    """Return True when ``path`` is contained under ``root`` (after tilde expansion)."""
    try:
        resolve_under_root(
            root.expanduser(),
            path.expanduser(),
            error_message="path escaped root",
        )
    except ValueError:
        return False
    return True
