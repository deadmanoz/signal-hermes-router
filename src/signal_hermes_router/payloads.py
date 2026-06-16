from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

CONTROL_JSON_SEPARATORS = (",", ":")


@dataclass(frozen=True)
class CanonicalNotificationPayload:
    value: dict[str, Any] | list[Any]
    text: str
    byte_length: int
    sha256: str


class NotificationPayloadError(ValueError):
    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code


def compact_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=CONTROL_JSON_SEPARATORS, allow_nan=False)


def encode_control_message(value: dict[str, Any]) -> bytes:
    return compact_json_dumps(value).encode("utf-8") + b"\n"


def canonicalize_notification_payload(
    value: Any,
    *,
    max_bytes: int | None,
) -> CanonicalNotificationPayload:
    if not isinstance(value, (dict, list)):
        raise NotificationPayloadError(
            "invalid_payload",
            "notification payload must be a JSON object or array",
        )
    try:
        text = compact_json_dumps(value)
    except (TypeError, ValueError) as exc:
        raise NotificationPayloadError(
            "invalid_payload",
            "notification payload must be canonical JSON",
        ) from exc
    encoded = text.encode("utf-8")
    if max_bytes is not None and len(encoded) > max_bytes:
        raise NotificationPayloadError(
            "payload_too_large",
            f"notification payload exceeds {max_bytes} bytes after canonical JSON compaction",
        )
    return CanonicalNotificationPayload(
        value=value,
        text=text,
        byte_length=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
    )
