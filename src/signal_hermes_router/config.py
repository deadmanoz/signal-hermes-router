from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .models import RouteState, SessionPolicy
from .permissions import StaticPermissionPolicy
from .secrets import resolve_secret_refs

LOGGER = logging.getLogger(__name__)

PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SIGNAL_MESSAGE_WARNING_BYTES = 2000
MIN_SIGNAL_MESSAGE_BYTES = 16


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failures: int = 3
    window_seconds: float = 300.0
    recovery_seconds: float = 300.0


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
    busy_notice_after_seconds: float = 120.0
    busy_notice: str = "Still working on this."
    acp_prompt_timeout_seconds: float = 300.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)


@dataclass(frozen=True)
class Route:
    platform: str
    group_id: str
    profile: str
    session_policy: SessionPolicy
    state: RouteState
    route_context: dict[str, Any] = field(default_factory=dict)
    permission_policy: StaticPermissionPolicy = field(default_factory=StaticPermissionPolicy)
    friendly_name: str | None = None
    maintenance_reply: str | None = None
    failure_reply: str | None = None

    @property
    def key(self) -> str:
        return f"{self.platform}:{self.group_id}"


@dataclass(frozen=True)
class AppConfig:
    router: RouterConfig
    routes: tuple[Route, ...]

    def find_route(self, platform: str, group_id: str) -> Route | None:
        for route in self.routes:
            if route.platform == platform and route.group_id == group_id:
                return route
        return None


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
    return AppConfig(router=router, routes=tuple(parse_routes(raw_routes)))


def parse_router_config(raw: dict[str, Any]) -> RouterConfig:
    defaults = RouterConfig()
    circuit_defaults = defaults.circuit_breaker
    signal = raw.get("signal") or {}
    circuit = raw.get("circuit_breaker") or {}
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
        work_root=Path(raw.get("work_root", defaults.work_root)),
        maintenance_reply=str(raw.get("maintenance_reply", defaults.maintenance_reply)),
        failure_reply=str(raw.get("failure_reply", defaults.failure_reply)),
        busy_notice_after_seconds=float(
            raw.get("busy_notice_after_seconds", defaults.busy_notice_after_seconds)
        ),
        busy_notice=str(raw.get("busy_notice", defaults.busy_notice)),
        acp_prompt_timeout_seconds=float(
            raw.get("acp_prompt_timeout_seconds", defaults.acp_prompt_timeout_seconds)
        ),
        circuit_breaker=CircuitBreakerConfig(
            failures=int(circuit.get("failures", circuit_defaults.failures)),
            window_seconds=float(circuit.get("window_seconds", circuit_defaults.window_seconds)),
            recovery_seconds=float(
                circuit.get("recovery_seconds", circuit_defaults.recovery_seconds)
            ),
        ),
    )


def parse_routes(raw: dict[str, Any]) -> list[Route]:
    values = raw.get("routes") or []
    if not isinstance(values, list):
        raise ValueError("routes.yaml requires a routes list")
    routes: list[Route] = []
    seen: dict[str, int] = {}
    for index, value in enumerate(values):
        route = parse_route(value)
        if route.key in seen:
            raise ValueError(
                f"duplicate route key {route.key!r} at routes[{index}] "
                f"(first defined at routes[{seen[route.key]}])"
            )
        seen[route.key] = index
        routes.append(route)
    return routes


def parse_route(raw: dict[str, Any]) -> Route:
    if "denylist" in raw or "deny" in raw:
        raise ValueError("route permission policy is allowlist-only; denylists are not supported")
    route_context = dict(raw.get("route_context") or {})
    try:
        json.dumps(route_context, sort_keys=True)
    except TypeError as exc:
        raise ValueError("route_context must be JSON serializable") from exc
    return Route(
        platform=str(raw["platform"]),
        group_id=str(raw["group_id"]),
        profile=normalize_profile_name(raw.get("profile")),
        session_policy=SessionPolicy(raw.get("session_policy", SessionPolicy.PERSISTENT_ROUTE)),
        state=RouteState(raw.get("state", RouteState.SHADOW)),
        route_context=route_context,
        permission_policy=StaticPermissionPolicy.from_config(raw.get("permissions") or []),
        friendly_name=raw.get("friendly_name"),
        maintenance_reply=raw.get("maintenance_reply"),
        failure_reply=raw.get("failure_reply"),
    )


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
