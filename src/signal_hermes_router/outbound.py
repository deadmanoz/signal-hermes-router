from __future__ import annotations

from .config import Route

TRUNCATED_REPLY_SUFFIX = "\n\n[truncated by signal-hermes-router]"


def prepare_outgoing_message(route: Route, message: str, *, max_reply_chars: int) -> str:
    return _limit_reply_chars(_apply_canary_prefix(route, message), max_reply_chars)


def chunk_for_signal_bytes(message: str, *, max_bytes: int) -> list[str]:
    return _chunk_for_signal_bytes(message, max_bytes=max_bytes)


def _apply_canary_prefix(route: Route, message: str) -> str:
    prefix = route.route_context.get("canary_reply_prefix")
    if not isinstance(prefix, str) or not prefix:
        return message
    stripped = message.lstrip()
    if stripped.startswith(prefix):
        return stripped
    return f"{prefix} {stripped}"


def _limit_reply_chars(message: str, max_reply_chars: int) -> str:
    if len(message) <= max_reply_chars:
        return message
    if max_reply_chars <= len(TRUNCATED_REPLY_SUFFIX):
        return message[:max_reply_chars]
    return (
        message[: max_reply_chars - len(TRUNCATED_REPLY_SUFFIX)].rstrip() + TRUNCATED_REPLY_SUFFIX
    )


def _byte_prefix(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _split_greedy_bytes(text: str, *, limit: int) -> list[str]:
    out: list[str] = []
    remaining = text
    while len(remaining.encode("utf-8")) > limit:
        window = _byte_prefix(remaining, limit)
        cut = window.rfind("\n\n")
        if cut > 0:
            out.append(remaining[:cut])
            remaining = remaining[cut + 2 :]
            continue
        cut = window.rfind("\n")
        if cut > 0:
            out.append(remaining[:cut])
            remaining = remaining[cut + 1 :]
            continue
        cut = max(window.rfind(" "), window.rfind("\t"))
        if cut > 0:
            out.append(remaining[:cut])
            remaining = remaining[cut + 1 :]
            continue
        if window:
            out.append(window)
            remaining = remaining[len(window) :]
        else:
            remaining = remaining[1:]
    if remaining:
        out.append(remaining)
    return out


def _hard_byte_cut(message: str, max_bytes: int) -> list[str]:
    out: list[str] = []
    remaining = message
    while remaining:
        prefix = _byte_prefix(remaining, max_bytes)
        if prefix:
            out.append(prefix)
            remaining = remaining[len(prefix) :]
        else:
            remaining = remaining[1:]
    return out


def _chunk_for_signal_bytes(message: str, *, max_bytes: int) -> list[str]:
    if len(message.encode("utf-8")) <= max_bytes:
        return [message]

    chunks = _split_greedy_bytes(message, limit=max_bytes)
    if len(chunks) == 1:
        return chunks

    m = len(chunks)
    for _ in range(5):
        marker_width = len(f"[{m}/{m}] ")
        effective = max_bytes - marker_width
        if effective < 4:
            return _hard_byte_cut(message, max_bytes)
        new_chunks = _split_greedy_bytes(message, limit=effective)
        if len(new_chunks) == m:
            chunks = new_chunks
            break
        chunks = new_chunks
        m = len(chunks)
    else:
        return _hard_byte_cut(message, max_bytes)

    return [f"[{i + 1}/{m}] {c}" for i, c in enumerate(chunks)]
