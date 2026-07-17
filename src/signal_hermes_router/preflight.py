from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .config import AppConfig, Route
from .models import RouteState
from .permissions import StaticPermissionPolicy, is_local_tool


ProbeCallable = Callable[[str], Awaitable["ToolSurface"]]
SUPPORTED_TOOL_SURFACE_SCHEMA_VERSION = 1
FULL_CALLABLE_TOOL_SURFACE_SCOPE = "full_callable"
_MISSING = object()
# Catalog-carrying keys other than `tools` that the metadata path recognizes.
# They are never part of the dedicated _tool_surface/list contract, so their
# presence there makes producer intent ambiguous and the router fails closed
# rather than silently trusting `tools` (or picking one when `tools` is absent).
_TOOL_SURFACE_ALIAS_CATALOG_KEYS = ("toolSurface", "tool_surface", "tool_names")


def _validate_probe_error_fields(code: str, error: str | None) -> None:
    """Keep internal probe failures safe and useful at the reporting boundary."""
    if not isinstance(code, str) or not code:
        raise ValueError("probe error code must be a non-empty string")
    if error is not None and (not isinstance(error, str) or not error):
        raise ValueError("probe error detail must be a non-empty string when provided")


class PreflightProbeUnavailable(RuntimeError):
    """Raised when the router has no trusted runtime tool-surface source yet."""

    def __init__(self, code: str, error: str | None = None) -> None:
        _validate_probe_error_fields(code, error)
        super().__init__(code)
        self.code = code
        self.error = error


def _require_schema_version(value: Any = _MISSING) -> int:
    if value is _MISSING:
        raise PreflightProbeUnavailable(
            "probe_contract_version_missing",
            "tool-surface contract must declare schema_version=1",
        )
    if isinstance(value, bool):
        raise PreflightProbeUnavailable(
            "probe_contract_version_unsupported",
            "tool-surface schema_version is unsupported; expected integer 1",
        )
    if not isinstance(value, int) or value != SUPPORTED_TOOL_SURFACE_SCHEMA_VERSION:
        raise PreflightProbeUnavailable(
            "probe_contract_version_unsupported",
            "tool-surface schema_version is unsupported; expected integer 1",
        )
    return value


def _require_surface_scope(value: Any = _MISSING) -> str:
    if value is _MISSING:
        raise PreflightProbeUnavailable(
            "probe_contract_scope_missing",
            "tool-surface contract must declare scope=full_callable",
        )
    if value != FULL_CALLABLE_TOOL_SURFACE_SCOPE:
        raise PreflightProbeUnavailable(
            "probe_contract_scope_unsupported",
            "tool-surface scope must be full_callable",
        )
    return value


def _contract_tool_names(value: Any = _MISSING) -> frozenset[str]:
    # Wire contracts use JSON arrays, which standard json.loads represents as
    # lists. Programmatic callers normalize through ToolSurface.from_names.
    if not isinstance(value, list) or not all(isinstance(tool, str) and tool for tool in value):
        raise PreflightProbeUnavailable(
            "probe_contract_invalid",
            "tool-surface tools must be a list of non-empty strings",
        )
    return frozenset(value)


@dataclass(frozen=True)
class ToolSurface:
    # Direct construction is intentionally not trusted. The permission-
    # preflight consumer validates every injected probe result at its boundary.
    profile: str
    tool_names: frozenset[str]
    schema_version: int
    scope: str
    source: str = "injected"

    @classmethod
    def from_names(
        cls,
        profile: str,
        tool_names: list[str] | tuple[str, ...] | set[str] | frozenset[str],
        *,
        schema_version: int,
        scope: str,
        source: str = "injected",
    ) -> "ToolSurface":
        """Construct a programmatic surface; wire contracts remain list-only."""
        surface = cls(
            profile=profile,
            tool_names=frozenset(tool_names),
            schema_version=schema_version,
            scope=scope,
            source=source,
        )
        validate_tool_surface(surface)
        return surface


def validate_tool_surface(surface: ToolSurface) -> None:
    _require_schema_version(surface.schema_version)
    _require_surface_scope(surface.scope)
    # Keep the normalized representation immutable as well as validating its
    # elements; a tuple of valid strings would otherwise pass the next check.
    if not isinstance(surface.tool_names, frozenset):
        raise PreflightProbeUnavailable(
            "probe_contract_invalid",
            "normalized tool-surface names must be a frozenset",
        )
    if any(not isinstance(tool, str) or not tool for tool in surface.tool_names):
        raise PreflightProbeUnavailable(
            "probe_contract_invalid",
            "normalized tool-surface names must contain only non-empty strings",
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
    candidates = _tool_surface_metadata_candidates(meta)
    if not candidates:
        return None
    if len(candidates) != 1:
        # Never prefer an apparently valid candidate over a coexisting legacy
        # or model-facing list: their coexistence leaves producer intent unclear.
        raise PreflightProbeUnavailable(
            "probe_contract_ambiguous",
            "multiple tool-surface metadata candidates are present: "
            + ", ".join(path for path, _value in candidates),
        )
    return tool_surface_from_value(profile, candidates[0][1], source="agent_capabilities_meta")


def tool_surface_from_value(
    profile: str,
    value: Any,
    *,
    source: str,
) -> ToolSurface:
    if not isinstance(value, dict):
        raise PreflightProbeUnavailable(
            "probe_contract_invalid",
            "tool-surface contract must be a JSON object",
        )
    schema_version = _require_schema_version(value.get("schema_version", _MISSING))
    scope = _require_surface_scope(value.get("scope", _MISSING))
    return ToolSurface.from_names(
        profile,
        _contract_tool_names(value.get("tools", _MISSING)),
        schema_version=schema_version,
        scope=scope,
        source=source,
    )


def tool_surface_from_hermes_tool_surface_list(
    profile: str,
    value: Any,
) -> ToolSurface:
    """Normalize a Hermes _tool_surface/list response into router-owned metadata.

    Hermes returns its stable dedicated response as the native {tools: [...]}
    shape (observed on Hermes v0.18.2 during 0.1.29 production validation). The
    _tool_surface/list method name is itself the
    dedicated callable-catalog contract, so when the response carries NEITHER
    schema_version NOR scope the router injects its own version-1 full_callable
    contract and validates the tool list. This path is source-aware: the same
    unversioned {tools: [...]} shape from agentCapabilities metadata or generic
    external input remains rejected by tool_surface_from_value().

    Fail-closed handling is preserved for explicit envelopes even on this
    dedicated method: if the response declares schema_version and/or scope it is
    an explicit envelope, so it is validated strictly via tool_surface_from_value
    (an unsupported version, a partial envelope, or a model_facing scope still
    fails closed here). An alternative catalog key
    (toolSurface/tool_surface/tool_names) is never part of this method's contract,
    so its presence is rejected as ambiguous uniformly for both the explicit
    envelope and the native shape, mirroring the metadata path's redundant-alias
    handling. Hermes owns its extension response shape; the router owns
    normalization and validation.
    """
    if not isinstance(value, dict):
        raise PreflightProbeUnavailable(
            "probe_contract_invalid",
            "tool-surface contract must be a JSON object",
        )
    # The dedicated method's response is the `tools` catalog (optionally wrapped
    # in a version/scope envelope). An alternative catalog key
    # (toolSurface/tool_surface/tool_names) is never part of that contract, so its
    # presence leaves producer intent ambiguous. Reject it uniformly here — before
    # the envelope/native split — so redundant aliases fail closed on this path
    # exactly as the metadata path treats them, without changing the shared
    # tool_surface_from_value semantics used by the other sources.
    alternative_keys = [key for key in _TOOL_SURFACE_ALIAS_CATALOG_KEYS if key in value]
    if alternative_keys:
        raise PreflightProbeUnavailable(
            "probe_contract_ambiguous",
            "tool-surface response uses catalog keys other than tools: "
            + ", ".join(alternative_keys),
        )
    # An explicit envelope (or partial envelope) declares its own version/scope;
    # validate it strictly so unsupported/model_facing catalogs still fail closed.
    if "schema_version" in value or "scope" in value:
        return tool_surface_from_value(profile, value, source="_tool_surface/list")
    # Pure Hermes-native shape: only the tools array is present; schema_version
    # and scope are injected by the router because the dedicated method name is
    # the callable-catalog contract.
    return ToolSurface.from_names(
        profile,
        _contract_tool_names(value.get("tools", _MISSING)),
        schema_version=SUPPORTED_TOOL_SURFACE_SCHEMA_VERSION,
        scope=FULL_CALLABLE_TOOL_SURFACE_SCOPE,
        source="_tool_surface/list",
    )


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

    def __post_init__(self) -> None:
        _validate_probe_error_fields(self.code, self.error)

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
class LocalToolExposedIssue:
    route_ref: str
    profile: str
    tool_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": "local_tool_exposed",
            "route_ref": self.route_ref,
            "profile": self.profile,
            "tool": self.tool_name,
        }

    def to_issue(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class PreflightReport:
    expected_permissions: tuple[ExpectedPermissionTool, ...]
    missing_tools: tuple[ExpectedPermissionTool, ...]
    probe_errors: tuple[PreflightProbeError, ...]
    scope_errors: tuple[PreflightScopeError, ...]
    local_tools_exposed: tuple[LocalToolExposedIssue, ...]
    checked_profiles: tuple[str, ...]
    scope: PreflightScope

    @property
    def ok(self) -> bool:
        return (
            not self.missing_tools
            and not self.probe_errors
            and not self.scope_errors
            and not self.local_tools_exposed
        )

    @property
    def status(self) -> str:
        return "ok" if self.ok else "failed"

    def to_dict(self) -> dict[str, Any]:
        issues = [tool.to_issue() for tool in self.missing_tools]
        issues.extend(error.to_issue() for error in self.probe_errors)
        issues.extend(error.to_issue() for error in self.scope_errors)
        issues.extend(issue.to_issue() for issue in self.local_tools_exposed)
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
            "local_tools_exposed_count": len(self.local_tools_exposed),
            "local_tools_exposed": [issue.to_dict() for issue in self.local_tools_exposed],
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
    mcp_only_profiles = {
        route.profile
        for index, route in enumerate(config.routes)
        if effective_scope.matches_route(index, route) and route.mcp_only
    }
    profiles = tuple(sorted({tool.profile for tool in expected} | mcp_only_profiles))
    surfaces: dict[str, ToolSurface] = {}
    probe_errors: list[PreflightProbeError] = []
    profiles_to_probe = () if scope_errors else profiles
    for profile in profiles_to_probe:
        try:
            surface = await probe(profile)
            # Revalidate even built-in probe results so injected/custom probes
            # cannot bypass the callable-catalog contract.
            validate_tool_surface(surface)
            surfaces[profile] = surface
        except PreflightProbeUnavailable as exc:
            probe_errors.append(
                PreflightProbeError(
                    profile=profile,
                    code=exc.code,
                    error=exc.error,
                )
            )
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
        checked_surface = surfaces.get(tool.profile)
        if checked_surface is not None and tool.tool_name not in checked_surface.tool_names:
            missing.append(tool)
    missing.sort(key=lambda item: (item.profile, item.route_ref, item.source_kind, item.tool_name))

    local_tools: list[LocalToolExposedIssue] = []
    # Build an id-keyed position map so synthetic definitions can resolve
    # route_ref and scope consistently with collect_expected_permission_tools.
    route_positions = {
        id(route): (index, route_ref(index, route))
        for index, route in enumerate(config.routes)
        if effective_scope.matches_route(index, route)
    }
    for index, route in enumerate(config.routes):
        if not effective_scope.matches_route(index, route):
            continue
        if not route.mcp_only:
            continue
        checked_surface = surfaces.get(route.profile)
        ref = route_ref(index, route)
        if checked_surface is not None:
            for tool_name in sorted(checked_surface.tool_names):
                if is_local_tool(tool_name):
                    local_tools.append(
                        LocalToolExposedIssue(
                            route_ref=ref,
                            profile=route.profile,
                            tool_name=tool_name,
                        )
                    )
        # Also flag local tools in the route's own allowlist — the runtime
        # backstop rejects these, so preflight should surface the config mistake.
        if not scope_errors:
            for rule in route.permission_policy.rules:
                if is_local_tool(rule.tool_name):
                    local_tools.append(
                        LocalToolExposedIssue(
                            route_ref=ref,
                            profile=route.profile,
                            tool_name=rule.tool_name,
                        )
                    )
    # Also scan synthetic definitions (jobs/notifications) for local tools
    # on mcp_only routes, but only when the route is in scope.
    if not scope_errors:
        for job in config.scheduled_jobs:
            route = config.find_route_by_name(job.route_name)
            if route is None or id(route) not in route_positions:
                continue
            if not route.mcp_only:
                continue
            if job.permission_policy is not None:
                _index, ref = route_positions[id(route)]
                for rule in job.permission_policy.rules:
                    if is_local_tool(rule.tool_name):
                        local_tools.append(
                            LocalToolExposedIssue(
                                route_ref=ref,
                                profile=route.profile,
                                tool_name=rule.tool_name,
                            )
                        )
        for notification in config.notifications:
            route = config.find_route_by_name(notification.route_name)
            if route is None or id(route) not in route_positions:
                continue
            if not route.mcp_only:
                continue
            if notification.permission_policy is not None:
                _index, ref = route_positions[id(route)]
                for rule in notification.permission_policy.rules:
                    if is_local_tool(rule.tool_name):
                        local_tools.append(
                            LocalToolExposedIssue(
                                route_ref=ref,
                                profile=route.profile,
                                tool_name=rule.tool_name,
                            )
                        )
    # Deduplicate (case-insensitive because is_local_tool lowercases before matching).
    seen_local_tools: set[tuple[str, str, str]] = set()
    deduped: list[LocalToolExposedIssue] = []
    for issue in local_tools:
        key = (issue.route_ref, issue.profile, issue.tool_name.lower())
        if key not in seen_local_tools:
            seen_local_tools.add(key)
            deduped.append(issue)
    local_tools = deduped
    local_tools.sort(key=lambda item: (item.profile, item.route_ref, item.tool_name.lower()))

    return PreflightReport(
        expected_permissions=expected,
        missing_tools=tuple(missing),
        probe_errors=tuple(probe_errors),
        scope_errors=tuple(scope_errors),
        local_tools_exposed=tuple(local_tools),
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
    return format_preflight_report_dict(report.to_dict())


def format_preflight_report_dict(data: dict[str, Any]) -> str:
    lines = [
        f"Permission preflight: {data.get('status', 'unknown')}",
        f"Profiles targeted: {len(data.get('checked_profiles') or [])}",
        "Configured permission tool entries: "
        f"{data.get('expected_permissions_count', len(data.get('expected_permissions') or []))}",
        f"Missing tool entries: {data.get('missing_tools_count', len(data.get('missing_tools') or []))}",
        f"Local tool entries: {data.get('local_tools_exposed_count', len(data.get('local_tools_exposed') or []))}",
    ]
    probe_errors = data.get("probe_errors") or []
    if probe_errors:
        lines.append("Probe errors:")
        for error in probe_errors:
            if not isinstance(error, dict):
                continue
            lines.append(f"- {error.get('profile')}: {error.get('error') or error.get('code')}")
    scope_errors = data.get("scope_errors") or []
    if scope_errors:
        lines.append("Scope errors:")
        for error in scope_errors:
            if not isinstance(error, dict):
                continue
            lines.append(f"- {error.get('error') or error.get('code')}")
    missing_tools = data.get("missing_tools") or []
    if missing_tools:
        lines.append("Missing tools:")
        for tool in missing_tools:
            if not isinstance(tool, dict):
                continue
            source = tool.get("source_kind")
            if tool.get("source_id") is not None:
                source = f"{source}:{tool['source_id']}"
            lines.append(
                f"- {tool.get('route_ref')} {tool.get('profile')} {source} {tool.get('tool')}"
            )
    local_tools = data.get("local_tools_exposed") or []
    if local_tools:
        lines.append("Local tools exposed:")
        for issue in local_tools:
            if not isinstance(issue, dict):
                continue
            lines.append(f"- {issue.get('route_ref')} {issue.get('profile')} {issue.get('tool')}")
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
    try:
        schema_version = _require_schema_version(raw.get("schema_version", _MISSING))
        scope = _require_surface_scope(raw.get("scope", _MISSING))
    except PreflightProbeUnavailable as exc:
        raise ValueError(exc.error or exc.code) from exc
    profiles = raw.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("probe contract profiles must be a mapping")
    surfaces: dict[str, ToolSurface] = {}
    for profile, value in profiles.items():
        if not isinstance(profile, str) or not profile:
            raise ValueError("probe contract profile names must be non-empty strings")
        if not isinstance(value, dict):
            raise ValueError("probe contract profile must be a mapping with tools")
        try:
            surfaces[profile] = ToolSurface.from_names(
                profile,
                _contract_tool_names(value.get("tools", _MISSING)),
                schema_version=schema_version,
                scope=scope,
                source=source,
            )
        except PreflightProbeUnavailable as exc:
            raise ValueError(exc.error or exc.code) from exc
    return surfaces


def _tool_surface_metadata_candidates(meta: dict[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for key in ("toolSurface", "tool_surface", "tools", "tool_names"):
        if key in meta:
            candidates.append((key, meta[key]))
    for key in ("signalHermesRouter", "signal-hermes-router", "signal_hermes_router"):
        nested = meta.get(key)
        if isinstance(nested, dict):
            # Version/scope metadata makes the namespace itself an envelope
            # candidate. Passing the whole object preserves its contract metadata;
            # partial envelopes then fail closed in tool_surface_from_value().
            if "schema_version" in nested or "scope" in nested:
                candidates.append((key, nested))
            else:
                for nested_key in ("toolSurface", "tool_surface", "tools", "tool_names"):
                    if nested_key in nested:
                        candidates.append((f"{key}.{nested_key}", nested[nested_key]))
    return candidates


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
