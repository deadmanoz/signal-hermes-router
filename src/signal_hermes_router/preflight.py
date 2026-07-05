from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .config import AppConfig, Route
from .models import RouteState
from .permissions import StaticPermissionPolicy


ProbeCallable = Callable[[str], Awaitable["ToolSurface"]]


class PreflightProbeUnavailable(RuntimeError):
    """Raised when the router has no trusted runtime tool-surface source yet."""


@dataclass(frozen=True)
class ToolSurface:
    profile: str
    tool_names: frozenset[str]
    source: str = "injected"

    @classmethod
    def from_names(
        cls,
        profile: str,
        tool_names: list[str] | tuple[str, ...] | set[str] | frozenset[str],
        *,
        source: str = "injected",
    ) -> "ToolSurface":
        return cls(
            profile=profile,
            tool_names=frozenset(str(tool) for tool in tool_names),
            source=source,
        )


def tool_surface_from_agent_capabilities(
    profile: str,
    agent_capabilities: dict[str, Any] | None,
) -> ToolSurface | None:
    if not isinstance(agent_capabilities, dict):
        return None
    meta = (
        agent_capabilities.get("_meta")
        if "_meta" in agent_capabilities
        else agent_capabilities.get("field_meta")
    )
    if not isinstance(meta, dict):
        return None
    for value in _tool_surface_metadata_candidates(meta):
        surface = tool_surface_from_value(profile, value, source="agent_capabilities_meta")
        if surface is not None:
            return surface
    return None


def tool_surface_from_value(
    profile: str,
    value: Any,
    *,
    source: str,
) -> ToolSurface | None:
    tool_names = _extract_tool_names(value)
    if tool_names is None:
        return None
    return ToolSurface.from_names(profile, tool_names, source=source)


@dataclass(frozen=True)
class PreflightScope:
    active_only: bool = False
    route_names: tuple[str, ...] = ()
    route_indexes: tuple[int, ...] = ()
    profiles: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "active_only": self.active_only,
            "route_names": list(self.route_names),
            "route_indexes": list(self.route_indexes),
            "profiles": list(self.profiles),
        }

    def matches_route(self, index: int, route: Route) -> bool:
        if self.active_only:
            if route.state != RouteState.ACTIVE:
                return False
        elif route.state not in {RouteState.ACTIVE, RouteState.SHADOW}:
            return False
        if self.route_names and route.name not in self.route_names:
            return False
        if self.route_indexes and index not in self.route_indexes:
            return False
        if self.profiles and route.profile not in self.profiles:
            return False
        return True


@dataclass(frozen=True)
class ExpectedPermissionTool:
    route_ref: str
    profile: str
    route_state: RouteState
    source_kind: str
    tool_name: str
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "route_ref": self.route_ref,
            "profile": self.profile,
            "route_state": self.route_state.value,
            "source_kind": self.source_kind,
            "tool": self.tool_name,
        }
        if self.source_id is not None:
            value["source_id"] = self.source_id
        return value

    def to_issue(self) -> dict[str, Any]:
        value = self.to_dict()
        value["code"] = "missing_tool"
        return value


@dataclass(frozen=True)
class PreflightProbeError:
    profile: str
    code: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"profile": self.profile, "code": self.code, "error": self.error or self.code}

    def to_issue(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "profile": self.profile,
            "error": self.error or self.code,
        }


@dataclass(frozen=True)
class PreflightScopeError:
    code: str
    error: str
    selector_kind: str | None = None
    selector: str | int | None = None

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"code": self.code, "error": self.error}
        if self.selector_kind is not None:
            value["selector_kind"] = self.selector_kind
        if self.selector is not None:
            value["selector"] = self.selector
        return value

    def to_issue(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class PreflightReport:
    expected_permissions: tuple[ExpectedPermissionTool, ...]
    missing_tools: tuple[ExpectedPermissionTool, ...]
    probe_errors: tuple[PreflightProbeError, ...]
    scope_errors: tuple[PreflightScopeError, ...]
    checked_profiles: tuple[str, ...]
    scope: PreflightScope

    @property
    def ok(self) -> bool:
        return not self.missing_tools and not self.probe_errors and not self.scope_errors

    @property
    def status(self) -> str:
        return "ok" if self.ok else "failed"

    def to_dict(self) -> dict[str, Any]:
        issues = [tool.to_issue() for tool in self.missing_tools]
        issues.extend(error.to_issue() for error in self.probe_errors)
        issues.extend(error.to_issue() for error in self.scope_errors)
        return {
            "status": self.status,
            "scope": self.scope.to_dict(),
            "checked_profiles": list(self.checked_profiles),
            "issue_count": len(issues),
            "issues": issues,
            "expected_permissions_count": len(self.expected_permissions),
            "expected_permissions": [tool.to_dict() for tool in self.expected_permissions],
            "missing_tools_count": len(self.missing_tools),
            "missing_tools": [tool.to_dict() for tool in self.missing_tools],
            "probe_errors": [error.to_dict() for error in self.probe_errors],
            "scope_errors": [error.to_dict() for error in self.scope_errors],
        }


def permission_tool_names(policy: StaticPermissionPolicy) -> tuple[str, ...]:
    return tuple(sorted({rule.tool_name for rule in policy.rules}))


def route_ref(index: int, route: Route) -> str:
    if route.name:
        return f"route:{route.name}"
    return f"routes[{index}]"


def parse_preflight_scope(value: dict[str, Any] | None = None) -> PreflightScope:
    raw = {} if value is None else value
    if not isinstance(raw, dict):
        raise ValueError("preflight scope must be a mapping")
    return PreflightScope(
        active_only=_as_bool(raw.get("active_only", False)),
        route_names=_string_tuple(raw, "route_names", aliases=("routes",)),
        route_indexes=_non_negative_int_tuple(raw, "route_indexes", aliases=("route_indices",)),
        profiles=_string_tuple(raw, "profiles"),
    )


def collect_expected_permission_tools(
    config: AppConfig,
    *,
    scope: PreflightScope | None = None,
) -> tuple[ExpectedPermissionTool, ...]:
    effective_scope = scope or PreflightScope()
    route_positions = {
        id(route): (index, route_ref(index, route))
        for index, route in enumerate(config.routes)
        if effective_scope.matches_route(index, route)
    }
    expected: list[ExpectedPermissionTool] = []
    for index, route in enumerate(config.routes):
        if not effective_scope.matches_route(index, route):
            continue
        expected.extend(
            _policy_expected_tools(
                route,
                route_ref(index, route),
                route.permission_policy,
                source_kind="route",
            )
        )
    for job in config.scheduled_jobs:
        route = config.find_route_by_name(job.route_name)
        if route is not None and id(route) in route_positions and job.permission_policy is not None:
            _index, ref = route_positions[id(route)]
            expected.extend(
                _policy_expected_tools(
                    route,
                    ref,
                    job.permission_policy,
                    source_kind="scheduled_job",
                    source_id=job.id,
                )
            )
    for notification in config.notifications:
        route = config.find_route_by_name(notification.route_name)
        if (
            route is not None
            and id(route) in route_positions
            and notification.permission_policy is not None
        ):
            _index, ref = route_positions[id(route)]
            expected.extend(
                _policy_expected_tools(
                    route,
                    ref,
                    notification.permission_policy,
                    source_kind="notification",
                    source_id=notification.id,
                )
            )
    return tuple(expected)


def scope_matches_route(config: AppConfig, scope: PreflightScope) -> bool:
    return any(scope.matches_route(index, route) for index, route in enumerate(config.routes))


def _unmatched_scope_selector_errors(
    config: AppConfig,
    scope: PreflightScope,
) -> tuple[PreflightScopeError, ...]:
    specs = (
        ("route_names", scope.route_names, "unmatched_route_name", "route selector", "route"),
        (
            "route_indexes",
            scope.route_indexes,
            "unmatched_route_index",
            "route-index selector",
            "route_index",
        ),
        ("profiles", scope.profiles, "unmatched_profile", "profile selector", "profile"),
    )
    errors: list[PreflightScopeError] = []
    for field, selectors, code, label, selector_kind in specs:
        for selector in selectors:
            selector_scope = replace(scope, **{field: (selector,)})
            if not scope_matches_route(config, selector_scope):
                errors.append(
                    PreflightScopeError(
                        code=code,
                        error=f"preflight {label} matched no route: {selector}",
                        selector_kind=selector_kind,
                        selector=selector,
                    )
                )
    return tuple(errors)


async def run_permission_preflight(
    config: AppConfig,
    probe: ProbeCallable,
    *,
    scope: PreflightScope | None = None,
) -> PreflightReport:
    effective_scope = scope or PreflightScope()
    expected = collect_expected_permission_tools(config, scope=effective_scope)
    scope_errors: list[PreflightScopeError] = list(
        _unmatched_scope_selector_errors(config, effective_scope)
    )
    if not scope_errors and not scope_matches_route(
        config,
        effective_scope,
    ):
        scope_errors.append(
            PreflightScopeError(
                code="scope_matched_no_routes",
                error="preflight scope did not match any route",
            )
        )
    profiles = tuple(sorted({tool.profile for tool in expected}))
    surfaces: dict[str, ToolSurface] = {}
    probe_errors: list[PreflightProbeError] = []
    profiles_to_probe = () if scope_errors else profiles
    for profile in profiles_to_probe:
        try:
            surfaces[profile] = await probe(profile)
        except PreflightProbeUnavailable as exc:
            code = str(exc) or "probe_unavailable"
            probe_errors.append(PreflightProbeError(profile=profile, code=code))
        except Exception as exc:
            probe_errors.append(
                PreflightProbeError(
                    profile=profile,
                    code="probe_failed",
                    error=exc.__class__.__name__,
                )
            )

    missing: list[ExpectedPermissionTool] = []
    for tool in expected:
        surface = surfaces.get(tool.profile)
        if surface is not None and tool.tool_name not in surface.tool_names:
            missing.append(tool)
    missing.sort(key=lambda item: (item.profile, item.route_ref, item.source_kind, item.tool_name))
    return PreflightReport(
        expected_permissions=expected,
        missing_tools=tuple(missing),
        probe_errors=tuple(probe_errors),
        scope_errors=tuple(scope_errors),
        checked_profiles=profiles,
        scope=effective_scope,
    )


async def unavailable_tool_surface_probe(profile: str) -> ToolSurface:
    raise PreflightProbeUnavailable("probe_contract_required")


def load_probe_contract(path: Path) -> ProbeCallable:
    surfaces = _parse_probe_contract(json.loads(path.read_text(encoding="utf-8")), source=str(path))

    async def probe(profile: str) -> ToolSurface:
        surface = surfaces.get(profile)
        if surface is None:
            raise PreflightProbeUnavailable("probe_profile_missing")
        return surface

    return probe


def format_preflight_report(report: PreflightReport) -> str:
    lines = [
        f"Permission preflight: {report.status}",
        f"Profiles targeted: {len(report.checked_profiles)}",
        f"Configured permission tool entries: {len(report.expected_permissions)}",
        f"Missing tool entries: {len(report.missing_tools)}",
    ]
    if report.probe_errors:
        lines.append("Probe errors:")
        for error in report.probe_errors:
            lines.append(f"- {error.profile}: {error.error or error.code}")
    if report.scope_errors:
        lines.append("Scope errors:")
        for error in report.scope_errors:
            lines.append(f"- {error.error}")
    if report.missing_tools:
        lines.append("Missing tools:")
        for tool in report.missing_tools:
            source = tool.source_kind
            if tool.source_id is not None:
                source = f"{source}:{tool.source_id}"
            lines.append(f"- {tool.route_ref} {tool.profile} {source} {tool.tool_name}")
    return "\n".join(lines)


def _policy_expected_tools(
    route: Route,
    ref: str,
    policy: StaticPermissionPolicy,
    *,
    source_kind: str,
    source_id: str | None = None,
) -> list[ExpectedPermissionTool]:
    return [
        ExpectedPermissionTool(
            route_ref=ref,
            profile=route.profile,
            route_state=route.state,
            source_kind=source_kind,
            source_id=source_id,
            tool_name=tool_name,
        )
        for tool_name in permission_tool_names(policy)
    ]


def _parse_probe_contract(raw: Any, *, source: str) -> dict[str, ToolSurface]:
    if not isinstance(raw, dict):
        raise ValueError("probe contract must be a JSON object")
    profiles = raw.get("profiles", raw)
    if not isinstance(profiles, dict):
        raise ValueError("probe contract profiles must be a mapping")
    surfaces: dict[str, ToolSurface] = {}
    for profile, value in profiles.items():
        profile_name = str(profile)
        if isinstance(value, dict):
            tools = value.get("tools", value.get("tool_names"))
        else:
            tools = value
        if not isinstance(tools, list) or not all(isinstance(tool, str) for tool in tools):
            raise ValueError("probe contract profile tools must be a string list")
        surfaces[profile_name] = ToolSurface.from_names(profile_name, tools, source=source)
    return surfaces


def _tool_surface_metadata_candidates(meta: dict[str, Any]) -> list[Any]:
    candidates: list[Any] = []
    for key in ("toolSurface", "tool_surface", "tools", "tool_names"):
        if key in meta:
            candidates.append(meta[key])
    for key in ("signalHermesRouter", "signal-hermes-router", "signal_hermes_router"):
        nested = meta.get(key)
        if isinstance(nested, dict):
            for nested_key in ("toolSurface", "tool_surface", "tools", "tool_names"):
                if nested_key in nested:
                    candidates.append(nested[nested_key])
    return candidates


def _extract_tool_names(value: Any) -> frozenset[str] | None:
    if isinstance(value, dict):
        for key in ("tools", "tool_names"):
            if key in value:
                return _extract_tool_names(value[key])
        function = value.get("function")
        if isinstance(function, dict):
            name = function.get("name")
            if isinstance(name, str) and name:
                return frozenset({name})
        name = value.get("name")
        if isinstance(name, str) and name:
            return frozenset({name})
        return None
    if not isinstance(value, list):
        return None
    names: set[str] = set()
    for item in value:
        if isinstance(item, str) and item:
            names.add(item)
            continue
        nested = _extract_tool_names(item)
        if nested is not None:
            names.update(nested)
    return frozenset(names)


def _string_tuple(
    raw: dict[str, Any],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
) -> tuple[str, ...]:
    value = _first_present(raw, key, aliases)
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"preflight scope {key} must be a non-empty string list")
    return tuple(value)


def _non_negative_int_tuple(
    raw: dict[str, Any],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
) -> tuple[int, ...]:
    value = _first_present(raw, key, aliases)
    if value is None:
        return ()
    if not isinstance(value, list) or any(
        isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in value
    ):
        raise ValueError(f"preflight scope {key} must be a non-negative integer list")
    return tuple(value)


def _first_present(raw: dict[str, Any], key: str, aliases: tuple[str, ...]) -> Any:
    for candidate in (key, *aliases):
        if candidate in raw:
            return raw[candidate]
    return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError("preflight scope active_only must be a boolean")
