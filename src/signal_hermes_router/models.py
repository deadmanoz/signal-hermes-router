from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from .failures import FailureInfo


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


class TurnOrigin(StrEnum):
    SIGNAL = "signal"
    SCHEDULED_JOB = "scheduled_job"
    NOTIFICATION = "notification"


class SyntheticTurnKind(StrEnum):
    SCHEDULED_JOB = "scheduled_job"
    NOTIFICATION = "notification"


class TurnOutcomeStatus(StrEnum):
    DELIVERED = "delivered"
    SKIPPED = "skipped"
    DEDUPED = "deduped"
    BUSY = "busy"
    ERROR = "error"


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

    def to_prompt_dict(self, *, include_tool_path: bool = False) -> dict[str, Any]:
        prompt_dict: dict[str, Any] = {
            "display_filename": self.display_filename,
            "content_type": self.content_type,
            "size": self.size,
            "sha256": self.sha256,
            "group_ref": self.group_ref,
            "sender_ref": self.sender_ref,
            "signal_timestamp": self.signal_timestamp,
        }
        # Opt-in only: expose the router-managed stored path under tool_path so
        # profile-side tools can operate on the exact file. The canonical_path
        # key itself stays omitted from the prompt in every path.
        if include_tool_path:
            prompt_dict["tool_path"] = str(self.canonical_path)
        return prompt_dict

    def to_text(self, *, include_tool_path: bool = False) -> str:
        lines = ["attachment_manifest:"]
        for key, value in self.to_prompt_dict(include_tool_path=include_tool_path).items():
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)


@dataclass(frozen=True)
class OutboundAttachment:
    path: Path
    content_type: str
    size: int
    owned_by_router: bool = False


@dataclass(frozen=True)
class TurnResult:
    text: str
    stop_reason: str = "end_turn"


@dataclass(frozen=True)
class SessionKeyInput:
    sender_id: str
    timestamp: int


@dataclass(frozen=True)
class TurnOutcome:
    status: TurnOutcomeStatus
    route_state: RouteState | None = None
    result: TurnResult | None = None
    error: str | None = None
    failure: FailureInfo | None = None
    synthetic_id: str | None = None
    synthetic_kind: SyntheticTurnKind | None = None
    reply_sent: bool | None = None
    route_ref: str | None = None
    profile: str | None = None
    last_failure_at_ms: int | None = None

    def to_control_response(self) -> dict[str, Any]:
        response: dict[str, Any] = {"status": self.status.value}
        if self.route_state is not None:
            response["route_state"] = self.route_state.value
        if self.route_ref is not None:
            response["route_ref"] = self.route_ref
        if self.profile is not None:
            response["profile"] = self.profile
        if self.last_failure_at_ms is not None:
            response["last_failure_at_ms"] = self.last_failure_at_ms
        if self.synthetic_id is not None:
            if self.synthetic_kind == SyntheticTurnKind.SCHEDULED_JOB:
                response["job_id"] = self.synthetic_id
            response["synthetic_id"] = self.synthetic_id
        if self.synthetic_kind is not None:
            response["synthetic_kind"] = self.synthetic_kind.value
        if self.result is not None:
            response["stop_reason"] = self.result.stop_reason
        if self.reply_sent is not None:
            response["reply_sent"] = self.reply_sent
        if self.failure is not None:
            response["error"] = self.failure.code.value
            response["failure"] = self.failure.to_dict()
        elif self.error is not None:
            response["error"] = self.error
        return response


@dataclass(frozen=True)
class SessionStatus:
    policy: SessionPolicy
    cached: bool
    cached_sessions: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy.value,
            "cached": self.cached,
            "cached_sessions": self.cached_sessions,
        }


@dataclass(frozen=True)
class CircuitStatus:
    state: str
    failure_count: int
    tripped_at_ms: int | None = None
    cooldown_remaining_seconds: float | None = None
    last_reset_at_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "state": self.state,
            "failure_count": self.failure_count,
        }
        if self.tripped_at_ms is not None:
            value["tripped_at_ms"] = self.tripped_at_ms
        if self.cooldown_remaining_seconds is not None:
            value["cooldown_remaining_seconds"] = self.cooldown_remaining_seconds
        if self.last_reset_at_ms is not None:
            value["last_reset_at_ms"] = self.last_reset_at_ms
        return value


@dataclass(frozen=True)
class RouteHealth:
    route_ref: str
    profile: str
    route_state: RouteState
    configured_state: RouteState
    session: SessionStatus
    circuit: CircuitStatus
    last_success_at_ms: int | None = None
    last_failure_at_ms: int | None = None
    last_failure: FailureInfo | None = None

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "route_ref": self.route_ref,
            "profile": self.profile,
            "route_state": self.route_state.value,
            "configured_state": self.configured_state.value,
            "session": self.session.to_dict(),
            "circuit": self.circuit.to_dict(),
        }
        if self.last_success_at_ms is not None:
            value["last_success_at_ms"] = self.last_success_at_ms
        if self.last_failure_at_ms is not None:
            value["last_failure_at_ms"] = self.last_failure_at_ms
        if self.last_failure is not None:
            value["last_failure"] = self.last_failure.to_dict()
        return value
