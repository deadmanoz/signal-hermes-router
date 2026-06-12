from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any

from .mime import DEFAULT_CONTENT_TYPE, normalize_content_type
from .models import NormalizedEvent, SignalAttachment

DEFAULT_MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return None


def unwrap_signal_event(raw: dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw.get("envelope"), dict):
        return raw
    params = raw.get("params") or {}
    if "result" in params and isinstance(params["result"], dict):
        params = params["result"]
    return params


@dataclass(frozen=True)
class SignalEventSummary:
    shape: str
    message_type: str
    has_group: bool
    has_exception: bool = False

    def __str__(self) -> str:
        summary = (
            f"shape={self.shape} message_type={self.message_type} "
            f"has_group={str(self.has_group).lower()}"
        )
        if self.has_exception:
            summary += " has_exception=true"
        return summary


def inspect_signal_event(raw: dict[str, Any]) -> SignalEventSummary:
    params = unwrap_signal_event(raw)
    envelope = params.get("envelope") if isinstance(params, dict) else None
    has_exception = _has_exception(params)
    if not isinstance(envelope, dict):
        return SignalEventSummary(
            shape="unknown",
            message_type="none",
            has_group=False,
            has_exception=has_exception,
        )
    shape = "direct" if raw is params else "jsonrpc"
    message_type = _message_type(envelope)
    data_message = _data_message_from_envelope(envelope)
    has_group = False
    if isinstance(data_message, dict):
        has_group = _group_id(data_message) is not None
    return SignalEventSummary(
        shape=shape,
        message_type=message_type,
        has_group=has_group,
        has_exception=has_exception,
    )


def summarize_signal_event(raw: dict[str, Any]) -> str:
    return str(inspect_signal_event(raw))


def probe_routeability(raw: dict[str, Any]) -> tuple[str | None, SignalEventSummary]:
    """Lightweight probe: return the group_id (if any) and a content-free summary.

    Does not parse message text, attachments, or sender identifiers.
    """
    params = unwrap_signal_event(raw)
    envelope = params.get("envelope") if isinstance(params, dict) else None
    has_exception = _has_exception(params)
    if not isinstance(envelope, dict):
        return None, SignalEventSummary(
            shape="unknown",
            message_type="none",
            has_group=False,
            has_exception=has_exception,
        )
    shape = "direct" if raw is params else "jsonrpc"
    message_type = _message_type(envelope)
    data_message = _data_message_from_envelope(envelope)
    group_id = None
    if isinstance(data_message, dict):
        group_id = _group_id(data_message)
    has_group = group_id is not None
    return group_id, SignalEventSummary(
        shape=shape,
        message_type=message_type,
        has_group=has_group,
        has_exception=has_exception,
    )


def parse_signal_event(
    raw: dict[str, Any],
    platform: str = "signal",
    *,
    max_attachment_bytes: int = DEFAULT_MAX_ATTACHMENT_BYTES,
) -> NormalizedEvent | None:
    params = unwrap_signal_event(raw)
    envelope = params.get("envelope") or {}
    data_message = _data_message_from_envelope(envelope)
    group_id = _group_id(data_message)
    if not group_id:
        return None

    sender_id = str(
        _first_present(envelope, "sourceUuid", "sourceNumber", "source")
        or _first_present(params, "account")
        or "unknown"
    )
    source_uuid = str(_first_present(envelope, "sourceUuid", "source") or sender_id)
    timestamp = int(_first_present(envelope, "timestamp") or data_message.get("timestamp") or 0)
    text = str(data_message.get("message") or "")
    attachments = tuple(
        _parse_attachments(
            data_message.get("attachments") or [],
            max_attachment_bytes=max_attachment_bytes,
        )
    )
    return NormalizedEvent(
        platform=platform,
        group_id=str(group_id),
        sender_id=sender_id,
        source_uuid=source_uuid,
        timestamp=timestamp,
        text=text,
        attachments=attachments,
        raw=raw,
    )


def _message_type(envelope: dict[str, Any]) -> str:
    for key in (
        "dataMessage",
        "editMessage",
        "syncMessage",
        "storyMessage",
        "callMessage",
        "typingMessage",
        "receiptMessage",
    ):
        if key in envelope:
            return key
    return "unknown"


def _data_message_from_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    data_message = envelope.get("dataMessage") or {}
    if not data_message:
        data_message = (envelope.get("editMessage") or {}).get("dataMessage") or {}
    if not data_message:
        data_message = (envelope.get("syncMessage") or {}).get("sentMessage") or {}
    if isinstance(data_message, dict) and not _group_id(data_message):
        data_message = (data_message.get("editMessage") or {}).get("dataMessage") or data_message
    return data_message if isinstance(data_message, dict) else {}


def _has_exception(params: Any) -> bool:
    return isinstance(params, dict) and params.get("exception") is not None


def _group_id(data_message: dict[str, Any]) -> str | None:
    group_info = data_message.get("groupInfo") or {}
    return _first_present(
        data_message,
        "groupId",
        "group_id",
    ) or _first_present(group_info, "groupId", "group_id", "id")


def _parse_attachments(
    values: list[dict[str, Any]],
    *,
    max_attachment_bytes: int,
) -> list[SignalAttachment]:
    attachments: list[SignalAttachment] = []
    for value in values:
        body = None
        if "data" in value and isinstance(value["data"], str):
            if _estimated_base64_decoded_size(value["data"]) > max_attachment_bytes:
                raise ValueError("inline attachment exceeds max_attachment_bytes")
            body = base64.b64decode(value["data"])
            if len(body) > max_attachment_bytes:
                raise ValueError("inline attachment exceeds max_attachment_bytes")
        attachments.append(
            SignalAttachment(
                content_type=normalize_content_type(
                    value.get("contentType") or value.get("content_type") or DEFAULT_CONTENT_TYPE
                ),
                filename=value.get("filename") or value.get("fileName"),
                size=value.get("size"),
                body=body,
                signal_id=value.get("id"),
            )
        )
    return attachments


def _estimated_base64_decoded_size(value: str) -> int:
    stripped = value.rstrip("=")
    return (len(stripped) * 3) // 4
