from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class RouteState(StrEnum):
    SHADOW = "shadow"
    ACTIVE = "active"
    MAINTENANCE = "maintenance"
    DISABLED = "disabled"


class SessionPolicy(StrEnum):
    PERSISTENT_ROUTE = "persistent_route"
    PERSISTENT_SENDER = "persistent_sender"
    EPHEMERAL = "ephemeral"


class ChatType(StrEnum):
    GROUP = "group"
    DIRECT = "direct"


@dataclass(frozen=True)
class SignalAttachment:
    content_type: str
    filename: str | None = None
    size: int | None = None
    body: bytes | None = None
    path: Path | None = None
    signal_id: str | None = None


@dataclass(frozen=True)
class NormalizedEvent:
    platform: str
    sender_id: str
    timestamp: int
    text: str
    source_uuid: str | None = None
    chat_type: ChatType = ChatType.GROUP
    group_id: str | None = None
    source_number: str | None = None
    attachments: tuple[SignalAttachment, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def dedupe_sender_id(self) -> str:
        return str(self.source_uuid or self.source_number or self.sender_id or "unknown")

    @property
    def dedupe_key(self) -> tuple[str, int]:
        return (self.dedupe_sender_id, self.timestamp)


@dataclass(frozen=True)
class MediaManifest:
    display_filename: str
    canonical_path: Path
    content_type: str
    size: int
    sha256: str
    group_ref: str
    sender_ref: str
    signal_timestamp: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "display_filename": self.display_filename,
            "canonical_path": str(self.canonical_path),
            "content_type": self.content_type,
            "size": self.size,
            "sha256": self.sha256,
            "group_ref": self.group_ref,
            "sender_ref": self.sender_ref,
            "signal_timestamp": self.signal_timestamp,
        }

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "display_filename": self.display_filename,
            "content_type": self.content_type,
            "size": self.size,
            "sha256": self.sha256,
            "group_ref": self.group_ref,
            "sender_ref": self.sender_ref,
            "signal_timestamp": self.signal_timestamp,
        }

    def to_text(self) -> str:
        lines = ["attachment_manifest:"]
        for key, value in self.to_prompt_dict().items():
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)


@dataclass(frozen=True)
class TurnResult:
    text: str
    stop_reason: str = "end_turn"
