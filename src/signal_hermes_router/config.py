from __future__ import annotations

import hashlib
import logging
import math
import re
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import urlparse

from .models import ChatType, NormalizedEvent, RouteState, SessionPolicy, SyntheticTurnKind
from .payloads import compact_json_dumps
from .permissions import StaticPermissionPolicy
from .secrets import resolve_secret_refs

LOGGER = logging.getLogger(__name__)

PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SAFE_TOKEN_RE = PROFILE_NAME_RE
SIGNAL_MESSAGE_WARNING_BYTES = 2000
MIN_SIGNAL_MESSAGE_BYTES = 16
DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES = 16 * 1024
CONTROL_REQUEST_HEADROOM_BYTES = 8 * 1024


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failures: int = 3
    window_seconds: float = 300.0
    recovery_seconds: float = 300.0


@dataclass(frozen=True)
class RouterControlConfig:
    enabled: bool = False
    socket_path: Path | None = None
    route_lock_timeout_seconds: float = 0.0
    max_notification_payload_bytes: int = DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES


@dataclass(frozen=True)
class RouterConfig:
    signal_base_url: str = "http://127.0.0.1:8080"
    allow_remote_signal_base_url: bool = False
    state_db: Path = Path("./private/state/router.db")
    media_root: Path = Path("./private/media")
    signal_attachment_root: Path = Path("~/.local/share/signal-cli/attachments")
    max_attachment_bytes: int = 25 * 1024 * 1024
    max_signal_event_bytes: int = 50 * 1024 * 1024
    max_acp_line_bytes: int = 8 * 1024 * 1024
    max_reply_chars: int = 12000
    max_signal_message_bytes: int = 1900
    work_root: Path = Path("./private/work")
    maintenance_reply: str = "This route is temporarily under maintenance."
    failure_reply: str = "I hit an internal router error handling that message."
    model_failure_reply: str = (
        "The model service is temporarily unavailable, so I could not finish that request. "
        "Please try again later."
    )
    busy_notice_after_seconds: float = 120.0
    busy_notice: str = "Still working on this."
    acp_prompt_timeout_seconds: float = 300.0
    acp_initialize_timeout_seconds: float = 30.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    control: RouterControlConfig = field(default_factory=RouterControlConfig)

    @property
    def control_socket_path(self) -> Path:
        return self.control.socket_path or self.work_root / "control.sock"

    @property
    def control_request_line_limit_bytes(self) -> int:
        return self.control.max_notification_payload_bytes + CONTROL_REQUEST_HEADROOM_BYTES


@dataclass(frozen=True)
class Route:
    platform: str
    profile: str
    session_policy: SessionPolicy
    state: RouteState
    name: str | None = None
    chat_type: ChatType = ChatType.GROUP
    group_id: str | None = None
    sender_id: str | None = None
    sender_number: str | None = None
    route_context: dict[str, Any] = field(default_factory=dict)
    permission_policy: StaticPermissionPolicy = field(default_factory=StaticPermissionPolicy)
    friendly_name: str | None = None
    maintenance_reply: str | None = None
    failure_reply: str | None = None
    recreate_session_on_resume_failure: bool = False

    @property
    def key(self) -> str:
        if self.chat_type == ChatType.GROUP:
            if not self.group_id:
                raise ValueError("group route requires group_id")
            return f"{self.platform}:{self.group_id}"
        if not self.sender_id:
            raise ValueError("direct route requires sender_id")
        digest = hashlib.sha256(self.sender_id.encode("utf-8")).hexdigest()[:24]
        return f"{self.platform}:direct:{digest}"


@dataclass(frozen=True)
class SyntheticRouteDefinition:
    id: str
    route_name: str
    prompt: str
    kind: SyntheticTurnKind
    description: str | None = None
    permission_policy: StaticPermissionPolicy | None = None

    @property
    def namespace(self) -> str:
        return f"synthetic:{self.kind.value}:{self.id}"


@dataclass(frozen=True)
class SyntheticRouteJob(SyntheticRouteDefinition):
    kind: SyntheticTurnKind = SyntheticTurnKind.SCHEDULED_JOB

    @property
    def namespace(self) -> str:
        return f"scheduled:{self.id}"


@dataclass(frozen=True)
class SyntheticRouteNotification(SyntheticRouteDefinition):
    kind: SyntheticTurnKind = SyntheticTurnKind.NOTIFICATION


SyntheticDefinitionT = TypeVar("SyntheticDefinitionT", bound=SyntheticRouteDefinition)


@dataclass(frozen=True)
class AppConfig:
    router: RouterConfig
    routes: tuple[Route, ...]
    scheduled_jobs: tuple[SyntheticRouteJob, ...] = ()
    notifications: tuple[SyntheticRouteNotification, ...] = ()

    def find_route(self, platform: str, group_id: str) -> Route | None:
        return self.find_group_route(platform, group_id)

    def find_route_by_name(self, name: str) -> Route | None:
        for route in self.routes:
            if route.name == name:
                return route
        return None

    def find_synthetic_job(self, job_id: str) -> SyntheticRouteJob | None:
        for job in self.scheduled_jobs:
            if job.id == job_id:
                return job
        return None

    def find_notification(self, notification_id: str) -> SyntheticRouteNotification | None:
        for notification in self.notifications:
            if notification.id == notification_id:
                return notification
        return None

    def find_group_route(self, platform: str, group_id: str) -> Route | None:
        for route in self.routes:
            if (
                route.platform == platform
                and route.chat_type == ChatType.GROUP
                and route.group_id == group_id
            ):
                return route
        return None

    def find_direct_route(
        self,
        platform: str,
        source_uuid: str | None,
        source_number: str | None = None,
    ) -> Route | None:
        if source_uuid:
            for route in self.routes:
                if (
                    route.platform == platform
                    and route.chat_type == ChatType.DIRECT
                    and route.sender_id == source_uuid
                ):
                    return route
            return None
        if not source_number:
            return None
        for route in self.routes:
            if (
                route.platform == platform
                and route.chat_type == ChatType.DIRECT
                and route.sender_number == source_number
            ):
                return route
        return None

    def find_route_for_event(self, event: NormalizedEvent) -> Route | None:
        if event.chat_type == ChatType.DIRECT:
            return self.find_direct_route(event.platform, event.source_uuid, event.source_number)
        if event.group_id is None:
            return None
        return self.find_group_route(event.platform, event.group_id)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - exercised only without dependency installed
        raise RuntimeError("PyYAML is required to load router YAML config") from exc
    with path.open("r", encoding="utf-8") as handle:
        value = yaml.safe_load(handle) or {}
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return value


def load_app_config(config_path: Path, routes_path: Path) -> AppConfig:
    raw_config = resolve_secret_refs(_load_yaml(config_path))
    raw_routes = resolve_secret_refs(_load_yaml(routes_path))
    router = parse_router_config(raw_config.get("router") or raw_config)
    routes = tuple(parse_routes(raw_routes))
    return AppConfig(
        router=router,
        routes=routes,
        scheduled_jobs=tuple(parse_scheduled_jobs(raw_routes, routes)),
        notifications=tuple(parse_notifications(raw_routes, routes)),
    )


def load_router_config(config_path: Path) -> RouterConfig:
    raw_config = resolve_secret_refs(_load_yaml(config_path))
    return parse_router_config(raw_config.get("router") or raw_config)


def parse_router_config(raw: dict[str, Any]) -> RouterConfig:
    defaults = RouterConfig()
    circuit_defaults = defaults.circuit_breaker
    signal = raw.get("signal") or {}
    circuit = raw.get("circuit_breaker") or {}
    control = raw.get("control") or {}
    signal_base_url = str(
        signal.get("base_url", raw.get("signal_base_url", defaults.signal_base_url))
    )
    allow_remote_signal_base_url = _as_bool(
        signal.get(
            "allow_remote_base_url",
            raw.get("allow_remote_signal_base_url", defaults.allow_remote_signal_base_url),
        )
    )
    _validate_signal_base_url(signal_base_url, allow_remote_signal_base_url)
    max_attachment_bytes = _as_positive_int(
        raw.get("max_attachment_bytes", defaults.max_attachment_bytes),
        "router.max_attachment_bytes",
    )
    max_signal_message_bytes = _as_positive_int(
        raw.get("max_signal_message_bytes", defaults.max_signal_message_bytes),
        "router.max_signal_message_bytes",
    )
    if max_signal_message_bytes < MIN_SIGNAL_MESSAGE_BYTES:
        raise ValueError(
            f"router.max_signal_message_bytes must be >= {MIN_SIGNAL_MESSAGE_BYTES} "
            f"to guarantee progress over UTF-8 codepoints"
        )
    if max_signal_message_bytes > SIGNAL_MESSAGE_WARNING_BYTES:
        LOGGER.warning(
            "router.max_signal_message_bytes=%d exceeds the router's conservative "
            "%d-byte safety margin (Signal-Desktop's long-attachment threshold is "
            "2048 bytes; Signal-Android caps the input UI at 2000 characters). "
            "Long messages may be truncated or converted to .txt attachments by "
            "signal-cli.",
            max_signal_message_bytes,
            SIGNAL_MESSAGE_WARNING_BYTES,
        )
    work_root = Path(raw.get("work_root", defaults.work_root))
    acp_initialize_timeout_seconds = float(
        raw.get("acp_initialize_timeout_seconds", defaults.acp_initialize_timeout_seconds)
    )
    if not math.isfinite(acp_initialize_timeout_seconds) or acp_initialize_timeout_seconds <= 0:
        raise ValueError("router.acp_initialize_timeout_seconds must be a positive finite number")
    route_lock_timeout_seconds = float(
        control.get(
            "route_lock_timeout_seconds",
            defaults.control.route_lock_timeout_seconds,
        )
    )
    if route_lock_timeout_seconds < 0:
        raise ValueError("router.control.route_lock_timeout_seconds must be non-negative")
    max_notification_payload_bytes = _as_positive_int(
        control.get(
            "max_notification_payload_bytes",
            defaults.control.max_notification_payload_bytes,
        ),
        "router.control.max_notification_payload_bytes",
    )
    socket_path = control.get("socket_path")
    return RouterConfig(
        signal_base_url=signal_base_url,
        allow_remote_signal_base_url=allow_remote_signal_base_url,
        state_db=Path(raw.get("state_db", defaults.state_db)),
        media_root=Path(raw.get("media_root", defaults.media_root)),
        signal_attachment_root=Path(
            raw.get("signal_attachment_root", defaults.signal_attachment_root)
        ).expanduser(),
        max_attachment_bytes=max_attachment_bytes,
        max_signal_event_bytes=_as_positive_int(
            raw.get("max_signal_event_bytes", max_attachment_bytes * 2),
            "router.max_signal_event_bytes",
        ),
        max_acp_line_bytes=_as_positive_int(
            raw.get("max_acp_line_bytes", defaults.max_acp_line_bytes),
            "router.max_acp_line_bytes",
        ),
        max_reply_chars=_as_positive_int(
            raw.get("max_reply_chars", defaults.max_reply_chars),
            "router.max_reply_chars",
        ),
        max_signal_message_bytes=max_signal_message_bytes,
        work_root=work_root,
        maintenance_reply=str(raw.get("maintenance_reply", defaults.maintenance_reply)),
        failure_reply=str(raw.get("failure_reply", defaults.failure_reply)),
        model_failure_reply=str(raw.get("model_failure_reply", defaults.model_failure_reply)),
        busy_notice_after_seconds=float(
            raw.get("busy_notice_after_seconds", defaults.busy_notice_after_seconds)
        ),
        busy_notice=str(raw.get("busy_notice", defaults.busy_notice)),
        acp_prompt_timeout_seconds=float(
            raw.get("acp_prompt_timeout_seconds", defaults.acp_prompt_timeout_seconds)
        ),
        acp_initialize_timeout_seconds=acp_initialize_timeout_seconds,
        circuit_breaker=CircuitBreakerConfig(
            failures=int(circuit.get("failures", circuit_defaults.failures)),
            window_seconds=float(circuit.get("window_seconds", circuit_defaults.window_seconds)),
            recovery_seconds=float(
                circuit.get("recovery_seconds", circuit_defaults.recovery_seconds)
            ),
        ),
        control=RouterControlConfig(
            enabled=_as_bool(control.get("enabled", defaults.control.enabled)),
            socket_path=Path(socket_path) if socket_path not in (None, "") else None,
            route_lock_timeout_seconds=route_lock_timeout_seconds,
            max_notification_payload_bytes=max_notification_payload_bytes,
        ),
    )


def parse_routes(raw: dict[str, Any]) -> list[Route]:
    values = raw.get("routes") or []
    if not isinstance(values, list):
        raise ValueError("routes.yaml requires a routes list")
    routes: list[Route] = []
    seen: dict[str, int] = {}
    seen_names: dict[str, int] = {}
    seen_direct_sender_ids: dict[tuple[str, str], int] = {}
    seen_direct_numbers: dict[tuple[str, str], int] = {}
    for index, value in enumerate(values):
        route = parse_route(value)
        if route.name is not None:
            if route.name in seen_names:
                raise ValueError(
                    f"duplicate route name {route.name!r} at routes[{index}] "
                    f"(first defined at routes[{seen_names[route.name]}])"
                )
            seen_names[route.name] = index
        if route.chat_type == ChatType.DIRECT:
            assert route.sender_id is not None
            sender_key = (route.platform, route.sender_id)
            if sender_key in seen_direct_sender_ids:
                raise ValueError(
                    f"duplicate direct sender_id at routes[{index}] "
                    f"(first defined at routes[{seen_direct_sender_ids[sender_key]}])"
                )
            seen_direct_sender_ids[sender_key] = index
            if route.sender_number:
                number_key = (route.platform, route.sender_number)
                if number_key in seen_direct_numbers:
                    raise ValueError(
                        f"duplicate direct sender_number at routes[{index}] "
                        f"(first defined at routes[{seen_direct_numbers[number_key]}])"
                    )
                seen_direct_numbers[number_key] = index
        if route.key in seen:
            raise ValueError(
                f"duplicate route key {route.key!r} at routes[{index}] "
                f"(first defined at routes[{seen[route.key]}])"
            )
        seen[route.key] = index
        routes.append(route)
    return routes


def parse_scheduled_jobs(raw: dict[str, Any], routes: tuple[Route, ...]) -> list[SyntheticRouteJob]:
    return _parse_synthetic_definitions(
        raw,
        routes,
        key="scheduled_jobs",
        label="scheduled job",
        factory=SyntheticRouteJob,
    )


def parse_notifications(
    raw: dict[str, Any],
    routes: tuple[Route, ...],
) -> list[SyntheticRouteNotification]:
    return _parse_synthetic_definitions(
        raw,
        routes,
        key="notifications",
        label="notification",
        factory=SyntheticRouteNotification,
    )


def _parse_synthetic_definitions(
    raw: dict[str, Any],
    routes: tuple[Route, ...],
    *,
    key: str,
    label: str,
    factory: type[SyntheticDefinitionT],
) -> list[SyntheticDefinitionT]:
    values = raw.get(key, [])
    if values is None:
        values = []
    if not isinstance(values, list):
        raise ValueError(f"routes.yaml {key} must be a list")
    named_routes = {route.name: route for route in routes if route.name is not None}
    definitions: list[SyntheticDefinitionT] = []
    seen: dict[str, int] = {}
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise ValueError(f"{key}[{index}] must be a mapping")
        synthetic_id = normalize_safe_token(value.get("id"), f"{label} id")
        if synthetic_id in seen:
            raise ValueError(
                f"duplicate {label} id {synthetic_id!r} at {key}[{index}] "
                f"(first defined at {key}[{seen[synthetic_id]}])"
            )
        seen[synthetic_id] = index
        route_name = normalize_safe_token(value.get("route"), f"{label} route")
        if route_name not in named_routes:
            raise ValueError(f"{key}[{index}] references unknown route name {route_name!r}")
        prompt = value.get("prompt")
        if not isinstance(prompt, str):
            raise ValueError(f"{key}[{index}] prompt must be a string")
        if not prompt.strip():
            raise ValueError(f"{key}[{index}] prompt must not be empty")
        raw_permissions = value.get("permissions")
        definitions.append(
            factory(
                id=synthetic_id,
                route_name=route_name,
                prompt=prompt,
                description=value.get("description"),
                permission_policy=(
                    StaticPermissionPolicy.from_config(raw_permissions)
                    if raw_permissions is not None
                    else None
                ),
            )
        )
    return definitions


def parse_route(raw: dict[str, Any]) -> Route:
    if "denylist" in raw or "deny" in raw:
        raise ValueError("route permission policy is allowlist-only; denylists are not supported")
    route_context = dict(raw.get("route_context") or {})
    try:
        compact_json_dumps(route_context)
    except (TypeError, ValueError) as exc:
        raise ValueError("route_context must be finite JSON serializable") from exc
    chat_type = ChatType(raw.get("chat_type", ChatType.GROUP))
    platform = str(raw["platform"])
    route_name = raw.get("name")
    group_id = raw.get("group_id")
    sender_id = raw.get("sender_id")
    sender_number = raw.get("sender_number")
    if chat_type == ChatType.GROUP:
        if group_id in (None, ""):
            raise ValueError("group route requires group_id")
        group_id = str(group_id)
        sender_id = None
        sender_number = None
    else:
        if "group_id" in raw:
            raise ValueError("direct routes must not set group_id")
        group_id = None
        sender_id = _normalize_direct_identity(sender_id, "sender_id")
        sender_number = (
            _normalize_direct_identity(sender_number, "sender_number")
            if sender_number is not None
            else None
        )
    return Route(
        platform=platform,
        name=normalize_safe_token(route_name, "route name") if route_name is not None else None,
        profile=normalize_profile_name(raw.get("profile")),
        session_policy=SessionPolicy(raw.get("session_policy", SessionPolicy.PERSISTENT_ROUTE)),
        state=RouteState(raw.get("state", RouteState.SHADOW)),
        chat_type=chat_type,
        group_id=group_id,
        sender_id=sender_id,
        sender_number=sender_number,
        route_context=route_context,
        permission_policy=StaticPermissionPolicy.from_config(raw.get("permissions") or []),
        friendly_name=raw.get("friendly_name"),
        maintenance_reply=raw.get("maintenance_reply"),
        failure_reply=raw.get("failure_reply"),
        recreate_session_on_resume_failure=_as_bool(
            raw.get("recreate_session_on_resume_failure", False)
        ),
    )


def _normalize_direct_identity(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"direct route requires {field_name}")
    identity = str(value).strip()
    if not identity:
        raise ValueError(f"direct route {field_name} must not be empty")
    if identity == "*" or "*" in identity or identity.lower() in {"any", "all", "default"}:
        raise ValueError(f"direct route {field_name} must be an exact identity, not a wildcard")
    return identity


def normalize_profile_name(value: Any) -> str:
    if value is None:
        raise ValueError("route profile is required")
    profile = str(value)
    if not PROFILE_NAME_RE.fullmatch(profile):
        raise ValueError(
            "route profile must match [A-Za-z0-9][A-Za-z0-9._-]{0,63} "
            "and must not contain path separators"
        )
    return profile


def normalize_safe_token(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    token = str(value)
    if not SAFE_TOKEN_RE.fullmatch(token):
        raise ValueError(
            f"{field_name} must match [A-Za-z0-9][A-Za-z0-9._-]{{0,63}} "
            "and must not contain path separators"
        )
    return token


def _validate_signal_base_url(base_url: str, allow_remote: bool) -> None:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("router.signal.base_url must be an HTTP URL with a hostname")
    if allow_remote or _is_loopback_host(parsed.hostname):
        return
    raise ValueError(
        "router.signal.base_url must use a loopback host unless "
        "allow_remote_signal_base_url is enabled"
    )


def _is_loopback_host(hostname: str) -> bool:
    if hostname.lower() == "localhost":
        return True
    try:
        return ip_address(hostname).is_loopback
    except ValueError:
        return False


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


def _as_positive_int(value: Any, name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{name} must be positive")
    return parsed
