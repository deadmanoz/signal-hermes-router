from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from signal_hermes_router.config import AppConfig, SyntheticRouteJob, SyntheticRouteNotification
from signal_hermes_router.models import RouteState
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.preflight import (
    FULL_CALLABLE_TOOL_SURFACE_SCOPE,
    SUPPORTED_TOOL_SURFACE_SCHEMA_VERSION,
    PreflightProbeError,
    PreflightProbeUnavailable,
    PreflightScope,
    ToolSurface,
    collect_expected_permission_tools,
    format_preflight_report,
    load_probe_contract,
    parse_preflight_scope,
    run_permission_preflight,
    tool_surface_from_agent_capabilities,
    tool_surface_from_hermes_tool_surface_list,
    tool_surface_from_value,
    unavailable_tool_surface_probe,
)
from tests.support import make_route, router_config_for_tmp


def policy(*tools: str) -> StaticPermissionPolicy:
    return StaticPermissionPolicy.from_config([{"tool": tool} for tool in tools])


def callable_surface(
    profile: str,
    tools: list[str] | tuple[str, ...],
    *,
    source: str = "injected",
) -> ToolSurface:
    return ToolSurface.from_names(
        profile,
        tools,
        schema_version=SUPPORTED_TOOL_SURFACE_SCHEMA_VERSION,
        scope=FULL_CALLABLE_TOOL_SURFACE_SCOPE,
        source=source,
    )


def preflight_app(tmp: str | Path) -> AppConfig:
    active = make_route(
        name="active-route",
        group_id="EXAMPLE_ACTIVE_GROUP",
        profile="calendar",
        state=RouteState.ACTIVE,
        permission_policy=policy("read_file", "web_search"),
    )
    shadow = make_route(
        name="shadow-route",
        group_id="EXAMPLE_SHADOW_GROUP",
        profile="calendar",
        state=RouteState.SHADOW,
        permission_policy=policy("image_analysis"),
    )
    maintenance = make_route(
        name="maintenance-route",
        group_id="EXAMPLE_MAINTENANCE_GROUP",
        profile="calendar",
        state=RouteState.MAINTENANCE,
        permission_policy=policy("ignored_tool"),
    )
    unnamed = make_route(
        name=None,
        group_id="EXAMPLE_UNNAMED_GROUP",
        profile="ops",
        state=RouteState.ACTIVE,
        permission_policy=policy("read_file"),
    )
    return AppConfig(
        router=router_config_for_tmp(tmp),
        routes=(active, shadow, maintenance, unnamed),
        scheduled_jobs=(
            SyntheticRouteJob(
                id="agenda",
                route_name="active-route",
                prompt="Build the agenda.",
                permission_policy=policy("todo_create"),
            ),
        ),
        notifications=(
            SyntheticRouteNotification(
                id="backup-report",
                route_name="active-route",
                prompt="Summarize the report.",
            ),
        ),
    )


class PreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_preflight_reports_missing_tools_without_private_route_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def probe(profile: str) -> ToolSurface:
                tools = {
                    "calendar": ["read_file", "todo_create"],
                    "ops": ["read_file"],
                }[profile]
                return callable_surface(profile, tools)

            report = await run_permission_preflight(app, probe)

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.checked_profiles, ("calendar", "ops"))
        self.assertEqual(
            [tool.tool_name for tool in report.missing_tools],
            ["web_search", "image_analysis"],
        )
        serialized = json.dumps(report.to_dict(), sort_keys=True)
        self.assertIn("route:active-route", serialized)
        self.assertIn("route:shadow-route", serialized)
        self.assertIn("routes[3]", serialized)
        self.assertNotIn("EXAMPLE_ACTIVE_GROUP", serialized)
        self.assertNotIn("EXAMPLE_SHADOW_GROUP", serialized)
        self.assertNotIn("EXAMPLE_MAINTENANCE_GROUP", serialized)
        self.assertNotIn("EXAMPLE_UNNAMED_GROUP", serialized)
        self.assertNotIn("ignored_tool", serialized)
        self.assertNotIn("backup-report", serialized)
        self.assertEqual(report.to_dict()["issue_count"], 2)
        self.assertEqual(
            [issue["code"] for issue in report.to_dict()["issues"]],
            ["missing_tool", "missing_tool"],
        )

    async def test_scope_filters_active_routes_profiles_and_route_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)
            active_expected = collect_expected_permission_tools(
                app,
                scope=PreflightScope(active_only=True),
            )
            profile_expected = collect_expected_permission_tools(
                app,
                scope=PreflightScope(profiles=("ops",)),
            )
            index_expected = collect_expected_permission_tools(
                app,
                scope=PreflightScope(route_indexes=(3,)),
            )

        self.assertEqual(
            [(tool.route_ref, tool.tool_name) for tool in active_expected],
            [
                ("route:active-route", "read_file"),
                ("route:active-route", "web_search"),
                ("routes[3]", "read_file"),
                ("route:active-route", "todo_create"),
            ],
        )
        self.assertEqual(
            [(tool.route_ref, tool.tool_name) for tool in profile_expected],
            [("routes[3]", "read_file")],
        )
        self.assertEqual(
            [(tool.route_ref, tool.tool_name) for tool in index_expected],
            [("routes[3]", "read_file")],
        )

    async def test_probe_contract_can_satisfy_expected_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            contract = Path(tmp) / "probe-contract.json"
            contract.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "profiles": {
                            "calendar": {
                                "tools": [
                                    "image_analysis",
                                    "read_file",
                                    "todo_create",
                                    "web_search",
                                ]
                            },
                            "ops": {"tools": ["read_file"]},
                        },
                    }
                ),
                encoding="utf-8",
            )
            report = await run_permission_preflight(
                preflight_app(tmp),
                load_probe_contract(contract),
            )

        self.assertTrue(report.ok)
        self.assertEqual(report.status, "ok")
        self.assertEqual(report.missing_tools, ())
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(report.scope_errors, ())

    async def test_probe_contract_reports_missing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            contract = Path(tmp) / "probe-contract.json"
            contract.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "profiles": {"ops": {"tools": ["read_file"]}},
                    }
                ),
                encoding="utf-8",
            )
            report = await run_permission_preflight(
                preflight_app(tmp),
                load_probe_contract(contract),
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.probe_errors[0].code, "probe_profile_missing")

    def test_probe_contract_rejects_invalid_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            contract = Path(tmp) / "probe-contract.json"

            contract.write_text(json.dumps(["not", "a", "mapping"]), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "JSON object"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps({"profiles": {"calendar": {"tools": ["read_file"]}}}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "declare schema_version=1"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "scope": "full_callable",
                        "profiles": {},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "expected integer 1"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps(
                    {
                        "schema_version": None,
                        "scope": "full_callable",
                        "profiles": {},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "expected integer 1"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps(
                    {
                        "schema_version": True,
                        "scope": "full_callable",
                        "profiles": {},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "expected integer 1"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps({"schema_version": 1, "profiles": {}}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "declare scope=full_callable"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps({"schema_version": 1, "scope": "model_facing", "profiles": {}}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "must be full_callable"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps({"schema_version": 1, "scope": "full_callable", "profiles": []}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "profiles must be a mapping"):
                load_probe_contract(contract)

            contract.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "profiles": {"calendar": {"tools": ["read_file", 7]}},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "list of non-empty strings"):
                load_probe_contract(contract)

    async def test_explicit_empty_tool_surface_reports_missing_tools(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def probe(profile: str) -> ToolSurface:
                return callable_surface(profile, [])

            report = await run_permission_preflight(
                app,
                probe,
                scope=PreflightScope(active_only=True, profiles=("ops",)),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(report.scope_errors, ())
        self.assertEqual(
            [tool.to_dict() for tool in report.missing_tools],
            [
                {
                    "route_ref": "routes[3]",
                    "profile": "ops",
                    "route_state": "active",
                    "source_kind": "route",
                    "tool": "read_file",
                }
            ],
        )

    def test_agent_capabilities_meta_accepts_explicit_empty_surface(self) -> None:
        surface = tool_surface_from_agent_capabilities(
            "empty-profile",
            {
                "_meta": {
                    "toolSurface": {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "tools": [],
                    }
                }
            },
        )

        self.assertIsNotNone(surface)
        assert surface is not None
        self.assertEqual(surface.tool_names, frozenset())

    def test_tool_surface_metadata_accepts_directly_namespaced_v1_envelope(self) -> None:
        for ns in ("signalHermesRouter", "signal-hermes-router", "signal_hermes_router"):
            with self.subTest(namespace=ns):
                surface = tool_surface_from_agent_capabilities(
                    "calendar",
                    {
                        "_meta": {
                            ns: {
                                "schema_version": 1,
                                "scope": "full_callable",
                                "tools": ["web_search", "read_file"],
                            }
                        }
                    },
                )
                self.assertIsNotNone(surface)
                assert surface is not None
                self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))

    def test_tool_surface_metadata_avoids_duplicate_when_namespace_is_envelope_and_has_nested_key(
        self,
    ) -> None:
        surface = tool_surface_from_agent_capabilities(
            "calendar",
            {
                "_meta": {
                    "signalHermesRouter": {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "tools": ["web_search", "read_file"],
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["web_search", "read_file"],
                        },
                    }
                }
            },
        )
        self.assertIsNotNone(surface)
        assert surface is not None
        self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))

    def test_tool_surface_metadata_rejects_malformed_namespace_envelopes(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_missing"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "signalHermesRouter": {
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        }
                    }
                },
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_unsupported"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "signalHermesRouter": {
                            "schema_version": 1,
                            "scope": "model_facing",
                            "tools": ["read_file"],
                        }
                    }
                },
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "signalHermesRouter": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file", 7],
                        }
                    }
                },
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "signalHermesRouter": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                    }
                },
            )

    def test_tool_surface_metadata_accepts_nested_and_named_shapes(self) -> None:
        surface = tool_surface_from_agent_capabilities(
            "calendar",
            {
                "_meta": {
                    "signalHermesRouter": {
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["web_search", "read_file"],
                        }
                    }
                }
            },
        )

        self.assertIsNotNone(surface)
        assert surface is not None
        self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))
        self.assertIsNone(tool_surface_from_agent_capabilities("calendar", None))
        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_missing"):
            tool_surface_from_value("calendar", {}, source="test")
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_value("calendar", "not-an-object", source="test")

    def test_agent_capabilities_empty_meta_does_not_fall_through(self) -> None:
        surface = tool_surface_from_agent_capabilities(
            "empty-profile",
            {"_meta": {}, "field_meta": {"tools": ["read_file"]}},
        )

        self.assertIsNone(surface)

    def test_hermes_tool_surface_list_normalizes_native_shape(self) -> None:
        surface = tool_surface_from_hermes_tool_surface_list(
            "calendar",
            {"tools": ["read_file", "web_search"]},
        )
        self.assertEqual(surface.profile, "calendar")
        self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))
        self.assertEqual(surface.schema_version, 1)
        self.assertEqual(surface.scope, "full_callable")
        self.assertEqual(surface.source, "_tool_surface/list")

    def test_hermes_tool_surface_list_accepts_explicit_empty_surface(self) -> None:
        surface = tool_surface_from_hermes_tool_surface_list(
            "empty-profile",
            {"tools": []},
        )
        self.assertEqual(surface.tool_names, frozenset())

    def test_hermes_tool_surface_list_rejects_malformed_tools(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"tools": ["read_file", 7]},
            )

    def test_hermes_tool_surface_list_rejects_non_dict_result(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_hermes_tool_surface_list("calendar", ["read_file"])

    def test_hermes_tool_surface_list_accepts_explicit_versioned_envelope(self) -> None:
        surface = tool_surface_from_hermes_tool_surface_list(
            "calendar",
            {"schema_version": 1, "scope": "full_callable", "tools": ["read_file"]},
        )
        self.assertEqual(surface.tool_names, frozenset({"read_file"}))
        self.assertEqual(surface.schema_version, 1)
        self.assertEqual(surface.scope, "full_callable")
        self.assertEqual(surface.source, "_tool_surface/list")

    def test_hermes_tool_surface_list_rejects_explicit_model_facing_scope(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_unsupported"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"schema_version": 1, "scope": "model_facing", "tools": ["tool_search"]},
            )

    def test_hermes_tool_surface_list_rejects_partial_envelope(self) -> None:
        # scope present but schema_version absent is a partial explicit envelope,
        # not the pure native shape, so strict validation still fails it closed.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_missing"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"scope": "full_callable", "tools": ["read_file"]},
            )

    def test_hermes_tool_surface_list_rejects_version_only_partial_envelope(self) -> None:
        # schema_version present but scope absent is the symmetric partial
        # explicit envelope; the wrapper routes it to strict validation, which
        # fails closed on the missing scope rather than injecting full_callable.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_missing"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"schema_version": 1, "tools": ["read_file"]},
            )

    def test_hermes_tool_surface_list_rejects_explicit_unsupported_version(self) -> None:
        # An explicit envelope declaring schema_version=2 is routed to the strict
        # validator, so an unsupported version fails closed rather than being
        # silently normalized as the native shape.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_unsupported"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"schema_version": 2, "scope": "full_callable", "tools": ["read_file"]},
            )

    def test_hermes_tool_surface_list_rejects_coexisting_catalog_keys(self) -> None:
        # A response that carries an alternative catalog key alongside `tools`
        # leaves producer intent ambiguous; the router fails closed instead of
        # silently picking `tools`. The guard is uniform, so it applies to both
        # the native shape and an explicit versioned envelope.
        for coexisting_key in ("toolSurface", "tool_surface", "tool_names"):
            with self.subTest(coexisting_key=coexisting_key):
                with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
                    tool_surface_from_hermes_tool_surface_list(
                        "calendar",
                        {"tools": ["read_file"], coexisting_key: ["web_search"]},
                    )

    def test_hermes_tool_surface_list_rejects_versioned_envelope_with_redundant_alias(
        self,
    ) -> None:
        # The alias guard runs before the envelope/native split, so a redundant
        # alias alongside an explicit versioned envelope fails closed too, matching
        # how the metadata path treats redundant aliases as ambiguous.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {
                    "schema_version": 1,
                    "scope": "full_callable",
                    "tools": ["read_file"],
                    "tool_names": ["web_search"],
                },
            )

    def test_hermes_tool_surface_list_rejects_alternative_key_without_tools(
        self,
    ) -> None:
        # The same guard fires when the response uses an alternative catalog key
        # with no `tools` key at all; it fails closed as ambiguous rather than
        # treating a non-dedicated shape as an empty catalog.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
            tool_surface_from_hermes_tool_surface_list(
                "calendar",
                {"tool_names": ["read_file"]},
            )

    def test_hermes_tool_surface_list_rejects_native_shape_missing_tools(self) -> None:
        # A dict with neither schema_version/scope nor a tools key takes the
        # native branch; a truncated response with no tools array fails closed
        # instead of normalizing to an empty callable catalog.
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_hermes_tool_surface_list("calendar", {"other": 1})

    def test_tool_surface_from_value_still_rejects_unversioned_dict(self) -> None:
        """Unversioned {tools: [...]} from generic external input remains rejected."""
        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_missing"):
            tool_surface_from_value(
                "calendar",
                {"tools": ["read_file"]},
                source="external_contract",
            )

    def test_tool_surface_from_value_still_rejects_missing_scope(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_missing"):
            tool_surface_from_value(
                "calendar",
                {"schema_version": 1, "tools": ["read_file"]},
                source="external_contract",
            )

    def test_agent_capabilities_still_rejects_unversioned_tools(self) -> None:
        """Unversioned {tools: [...]} in agentCapabilities metadata remains rejected."""
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_invalid"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {"_meta": {"tools": ["read_file"]}},
            )

    def test_agent_capabilities_rejects_ambiguous_or_non_callable_contracts(self) -> None:
        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                        "tool_surface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                    }
                },
            )

        with self.assertRaisesRegex(
            PreflightProbeUnavailable, "contract_ambiguous"
        ) as mixed_ambiguity:
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                        "signalHermesRouter": {
                            "tools": ["tool_search", "tool_call"],
                        },
                    }
                },
            )
        self.assertIn(
            "toolSurface, signalHermesRouter.tools",
            mixed_ambiguity.exception.error or "",
        )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "contract_ambiguous"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "full_callable",
                            "tools": ["read_file"],
                        },
                        "tools": ["tool_search", "tool_call"],
                    }
                },
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_unsupported"):
            tool_surface_from_agent_capabilities(
                "calendar",
                {
                    "_meta": {
                        "toolSurface": {
                            "schema_version": 1,
                            "scope": "model_facing",
                            "tools": ["tool_search", "tool_call"],
                        }
                    }
                },
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "scope_missing"):
            tool_surface_from_value(
                "calendar",
                {"schema_version": 1, "tools": ["read_file"]},
                source="test",
            )

        with self.assertRaisesRegex(PreflightProbeUnavailable, "version_unsupported"):
            tool_surface_from_value(
                "calendar",
                {"schema_version": 2, "scope": "full_callable", "tools": ["read_file"]},
                source="test",
            )

    def test_probe_errors_require_safe_non_empty_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "code"):
            PreflightProbeUnavailable("")
        with self.assertRaisesRegex(ValueError, "detail"):
            PreflightProbeUnavailable("probe_failed", "")
        with self.assertRaisesRegex(ValueError, "detail"):
            PreflightProbeError(profile="calendar", code="probe_failed", error="")

    async def test_preflight_validates_injected_tool_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def invalid_version(profile: str) -> ToolSurface:
                return ToolSurface(
                    profile=profile,
                    tool_names=frozenset({"read_file"}),
                    schema_version=2,
                    scope="full_callable",
                )

            report = await run_permission_preflight(
                app,
                invalid_version,
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.missing_tools, ())
        self.assertEqual(
            [error.code for error in report.probe_errors],
            ["probe_contract_version_unsupported"],
        )

        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def model_facing(profile: str) -> ToolSurface:
                return ToolSurface(
                    profile=profile,
                    tool_names=frozenset({"tool_search", "tool_call"}),
                    schema_version=1,
                    scope="model_facing",
                )

            report = await run_permission_preflight(
                app,
                model_facing,
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.missing_tools, ())
        self.assertEqual(
            [error.code for error in report.probe_errors],
            ["probe_contract_scope_unsupported"],
        )

        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def invalid_names(profile: str) -> ToolSurface:
                return ToolSurface(
                    profile=profile,
                    tool_names=frozenset({"read_file", ""}),
                    schema_version=1,
                    scope="full_callable",
                )

            report = await run_permission_preflight(
                app,
                invalid_names,
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.missing_tools, ())
        self.assertEqual(
            [error.code for error in report.probe_errors],
            ["probe_contract_invalid"],
        )

        with tempfile.TemporaryDirectory() as tmp:
            app = preflight_app(tmp)

            async def mutable_names(profile: str) -> ToolSurface:
                return ToolSurface(
                    profile=profile,
                    tool_names=["read_file"],  # type: ignore[arg-type]
                    schema_version=1,
                    scope="full_callable",
                )

            report = await run_permission_preflight(
                app,
                mutable_names,
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.missing_tools, ())
        self.assertEqual(
            [error.code for error in report.probe_errors],
            ["probe_contract_invalid"],
        )

    async def test_scope_with_selectors_must_match_a_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = await run_permission_preflight(
                preflight_app(tmp),
                unavailable_tool_surface_probe,
                scope=PreflightScope(route_names=("typo-route",)),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.expected_permissions, ())
        self.assertEqual(report.checked_profiles, ())
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(
            [error.to_dict() for error in report.scope_errors],
            [
                {
                    "code": "unmatched_route_name",
                    "error": "preflight route selector matched no route: typo-route",
                    "selector_kind": "route",
                    "selector": "typo-route",
                }
            ],
        )
        self.assertEqual(report.to_dict()["issues"][0]["code"], "unmatched_route_name")

    async def test_scope_reports_each_unmatched_selector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = await run_permission_preflight(
                preflight_app(tmp),
                unavailable_tool_surface_probe,
                scope=PreflightScope(
                    route_names=("active-route", "typo-route"),
                    route_indexes=(0, 999),
                    profiles=("calendar", "typo-profile"),
                ),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(
            [(tool.route_ref, tool.tool_name) for tool in report.expected_permissions],
            [
                ("route:active-route", "read_file"),
                ("route:active-route", "web_search"),
                ("route:active-route", "todo_create"),
            ],
        )
        self.assertEqual(report.checked_profiles, ("calendar",))
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(
            [error.to_dict() for error in report.scope_errors],
            [
                {
                    "code": "unmatched_route_name",
                    "error": "preflight route selector matched no route: typo-route",
                    "selector_kind": "route",
                    "selector": "typo-route",
                },
                {
                    "code": "unmatched_route_index",
                    "error": "preflight route-index selector matched no route: 999",
                    "selector_kind": "route_index",
                    "selector": 999,
                },
                {
                    "code": "unmatched_profile",
                    "error": "preflight profile selector matched no route: typo-profile",
                    "selector_kind": "profile",
                    "selector": "typo-profile",
                },
            ],
        )

    async def test_active_only_scope_must_match_active_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = AppConfig(
                router=router_config_for_tmp(tmp),
                routes=(
                    make_route(
                        name="shadow-route",
                        group_id="EXAMPLE_SHADOW_GROUP",
                        profile="calendar",
                        state=RouteState.SHADOW,
                        permission_policy=policy("read_file"),
                    ),
                ),
            )

            report = await run_permission_preflight(
                app,
                unavailable_tool_surface_probe,
                scope=PreflightScope(active_only=True),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.expected_permissions, ())
        self.assertEqual(report.checked_profiles, ())
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(report.scope_errors[0].code, "scope_matched_no_routes")

    async def test_default_scope_must_match_active_or_shadow_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = AppConfig(
                router=router_config_for_tmp(tmp),
                routes=(
                    make_route(
                        name="maintenance-route",
                        group_id="EXAMPLE_MAINTENANCE_GROUP",
                        profile="calendar",
                        state=RouteState.MAINTENANCE,
                        permission_policy=policy("read_file"),
                    ),
                ),
            )

            report = await run_permission_preflight(
                app,
                unavailable_tool_surface_probe,
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(report.expected_permissions, ())
        self.assertEqual(report.checked_profiles, ())
        self.assertEqual(report.probe_errors, ())
        self.assertEqual(report.scope_errors[0].code, "scope_matched_no_routes")

    def test_parse_preflight_scope_rejects_non_mapping(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be a mapping"):
            parse_preflight_scope([])  # type: ignore[arg-type]

    async def test_format_preflight_report_renders_all_issue_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = await run_permission_preflight(
                preflight_app(tmp),
                unavailable_tool_surface_probe,
                scope=PreflightScope(route_names=("active-route", "typo-route")),
            )

        output = format_preflight_report(report)
        self.assertIn("Permission preflight: failed", output)
        self.assertIn("Profiles targeted: 1", output)
        self.assertIn("Scope errors:", output)
        self.assertIn("preflight route selector matched no route: typo-route", output)

        with tempfile.TemporaryDirectory() as tmp:
            missing_report = await run_permission_preflight(
                preflight_app(tmp),
                lambda profile: _tool_surface_with_names(profile, ()),
                scope=PreflightScope(route_names=("active-route",)),
            )

        missing_output = format_preflight_report(missing_report)
        self.assertIn("Missing tools:", missing_output)
        self.assertIn("route:active-route calendar route read_file", missing_output)

    async def test_unavailable_probe_reports_contract_requirement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = await run_permission_preflight(
                preflight_app(tmp),
                unavailable_tool_surface_probe,
                scope=PreflightScope(active_only=True, profiles=("calendar",)),
            )

        self.assertEqual(report.status, "failed")
        self.assertEqual(
            [error.to_dict() for error in report.probe_errors],
            [
                {
                    "profile": "calendar",
                    "code": "probe_contract_required",
                    "error": "probe_contract_required",
                }
            ],
        )
        self.assertEqual(
            report.to_dict()["issues"],
            [
                {
                    "profile": "calendar",
                    "code": "probe_contract_required",
                    "error": "probe_contract_required",
                }
            ],
        )

        self.assertEqual(report.missing_tools, ())
        self.assertEqual(report.scope_errors, ())

    def test_parse_preflight_scope_validates_selector_shape(self) -> None:
        scope = parse_preflight_scope(
            {
                "active_only": True,
                "routes": ["active-route"],
                "route_indexes": [0],
                "profiles": ["calendar"],
            }
        )
        self.assertEqual(scope.route_names, ("active-route",))
        self.assertEqual(scope.route_indexes, (0,))

        with self.assertRaisesRegex(ValueError, "active_only"):
            parse_preflight_scope({"active_only": "true"})
        with self.assertRaisesRegex(ValueError, "route_indexes"):
            parse_preflight_scope({"route_indexes": [-1]})
        with self.assertRaisesRegex(ValueError, "profiles"):
            parse_preflight_scope({"profiles": [""]})


async def _tool_surface_with_names(profile: str, names: tuple[str, ...]) -> ToolSurface:
    return callable_surface(profile, names)
