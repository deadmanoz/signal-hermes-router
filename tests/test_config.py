from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from signal_hermes_router.config import (
    DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES,
    AppConfig,
    Route,
    RouterConfig,
    load_app_config,
    load_control_discovery,
    load_router_config,
    normalize_profile_name,
    parse_notifications,
    parse_route,
    parse_router_config,
    parse_routes,
    parse_scheduled_jobs,
)
from signal_hermes_router.models import (
    ChatType,
    NormalizedEvent,
    RouteState,
    SessionPolicy,
    SyntheticTurnKind,
)


class ConfigTests(unittest.TestCase):
    def test_route_mcp_only_parsing(self) -> None:
        from signal_hermes_router.config import parse_route

        # explicit true
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "EXAMPLE",
                "profile": "test-profile",
                "session_policy": "persistent_route",
                "state": "active",
                "mcp_only": True,
            }
        )
        self.assertTrue(route.mcp_only)
        self.assertTrue(route.permission_policy.mcp_only)
        # explicit false
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "EXAMPLE",
                "profile": "test-profile",
                "session_policy": "persistent_route",
                "state": "active",
                "mcp_only": False,
            }
        )
        self.assertFalse(route.mcp_only)
        self.assertFalse(route.permission_policy.mcp_only)
        # default
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "EXAMPLE",
                "profile": "test-profile",
                "session_policy": "persistent_route",
                "state": "active",
            }
        )
        self.assertFalse(route.mcp_only)
        self.assertFalse(route.permission_policy.mcp_only)

    def test_route_mcp_only_syncs_to_permission_policy(self) -> None:
        from signal_hermes_router.config import Route
        from signal_hermes_router.permissions import StaticPermissionPolicy

        # Route constructed with mcp_only=True syncs to policy even when
        # policy is passed without the flag.
        policy = StaticPermissionPolicy.from_config([{"tool": "read_file"}])
        route = Route(
            platform="signal",
            profile="test",
            session_policy=SessionPolicy.PERSISTENT_ROUTE,
            state=RouteState.ACTIVE,
            mcp_only=True,
            permission_policy=policy,
        )
        self.assertTrue(route.mcp_only)
        self.assertTrue(route.permission_policy.mcp_only)
        # Route constructed with mcp_only=False and a policy with mcp_only=True
        # is a programmer error — the stricter setting must not be silently dropped.
        policy = StaticPermissionPolicy.from_config([{"tool": "read_file"}], mcp_only=True)
        with self.assertRaises(ValueError) as ctx:
            Route(
                platform="signal",
                profile="test",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                mcp_only=False,
                permission_policy=policy,
            )
        self.assertIn(
            "mcp_only=False conflicts with permission_policy mcp_only=True", str(ctx.exception)
        )

    def test_load_app_config_reads_yaml_and_router_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.yaml"
            routes = root / "routes.yaml"
            config.write_text(
                """
router:
  signal:
    base_url: "http://127.0.0.1:18080"
  state_db: "./state/router.db"
  max_attachment_bytes: 1234
""",
                encoding="utf-8",
            )
            routes.write_text(
                """
routes:
  - platform: signal
    group_id: GROUP
    profile: profile-one
    state: shadow
""",
                encoding="utf-8",
            )

            app = load_app_config(config, routes)
            self.assertEqual(app.router.signal_base_url, "http://127.0.0.1:18080")
            self.assertEqual(app.router.state_db, Path("./state/router.db"))
            self.assertEqual(app.router.max_attachment_bytes, 1234)
            self.assertEqual(app.routes[0].key, "signal:GROUP")

    def test_load_router_config_reads_only_router_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                """
router:
  work_root: ./private-work
  control:
    enabled: true
""",
                encoding="utf-8",
            )

            router = load_router_config(config)

            self.assertTrue(router.control.enabled)
            self.assertEqual(router.control_socket_path, Path("private-work") / "control.sock")

    def test_load_control_discovery_skips_unresolvable_router_secrets(self) -> None:
        # Control-socket CLI commands talk to the already-running daemon, so
        # an unresolvable startup-only secret elsewhere in config.yaml must
        # not break socket discovery.
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                "router:\n"
                "  signal_base_url: env://SHR_TEST_UNSET_ROUTER_SECRET\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  control:\n"
                "    socket_path: " + str(Path(tmp) / "custom.sock") + "\n"
                "    max_notification_payload_bytes: 4096\n",
                encoding="utf-8",
            )

            socket_path, cap = load_control_discovery(config, resolve_payload_cap=True)

            self.assertEqual(socket_path, Path(tmp) / "custom.sock")
            self.assertEqual(cap, 4096)
            # Socket-only callers do not need the cap: it stays unread.
            socket_only, deferred_cap = load_control_discovery(config)
            self.assertEqual(socket_only, Path(tmp) / "custom.sock")
            self.assertIsNone(deferred_cap)

    def test_load_control_discovery_falls_back_to_work_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                "router:\n  work_root: " + str(Path(tmp) / "work") + "\n",
                encoding="utf-8",
            )

            socket_path, cap = load_control_discovery(config)

            self.assertEqual(socket_path, Path(tmp) / "work" / "control.sock")
            self.assertIsNone(cap)

    def test_load_control_discovery_defers_payload_cap_resolution(self) -> None:
        # Commands that only need the socket path must not fail when the
        # optional notify-only payload cap is an expired env:///op:// ref:
        # the cap is parsed only for the notify-route prevalidation path.
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  control:\n"
                "    socket_path: " + str(Path(tmp) / "custom.sock") + "\n"
                "    max_notification_payload_bytes: env://SHR_TEST_UNSET_CAP_SECRET\n",
                encoding="utf-8",
            )

            socket_path, cap = load_control_discovery(config)
            self.assertEqual(socket_path, Path(tmp) / "custom.sock")
            self.assertIsNone(cap)
            with self.assertRaises(KeyError):
                load_control_discovery(config, resolve_payload_cap=True)

    def test_load_control_discovery_defaults_payload_cap_when_field_absent(self) -> None:
        # A config that omits max_notification_payload_bytes must still get
        # the default cap on the notify-route prevalidation path: returning
        # None there would silently disable the client-side payload check
        # the full config parse applies.
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                "router:\n  work_root: " + str(Path(tmp) / "work") + "\n",
                encoding="utf-8",
            )

            socket_path, cap = load_control_discovery(config, resolve_payload_cap=True)
            self.assertEqual(socket_path, Path(tmp) / "work" / "control.sock")
            self.assertEqual(cap, DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES)
            # Socket-only callers still defer the cap entirely.
            _, deferred_cap = load_control_discovery(config)
            self.assertIsNone(deferred_cap)

    def test_load_control_discovery_resolves_refs_in_socket_values(self) -> None:
        # Secret refs in the values that locate the socket are still
        # resolved: they are needed to connect.
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            socket_file = Path(tmp) / "resolved.sock"
            socket_file.write_text("", encoding="utf-8")
            secret_file = Path(tmp) / "socket-path.txt"
            secret_file.write_text(str(socket_file), encoding="utf-8")
            config.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  control:\n"
                "    socket_path: file://" + str(secret_file) + "\n",
                encoding="utf-8",
            )

            socket_path, cap = load_control_discovery(config)

            self.assertEqual(socket_path, socket_file)
            self.assertIsNone(cap)

    def test_app_config_find_group_route_returns_none_for_missing_route(self) -> None:
        app = AppConfig(
            router=RouterConfig(),
            routes=(
                Route(
                    platform="signal",
                    group_id="GROUP",
                    profile="profile",
                    session_policy=SessionPolicy.PERSISTENT_ROUTE,
                    state=RouteState.SHADOW,
                ),
            ),
        )

        self.assertIsNone(app.find_group_route("signal", "OTHER"))

    def test_load_yaml_rejects_non_mapping_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            routes = Path(tmp) / "routes.yaml"
            config.write_text("[1]\n", encoding="utf-8")
            routes.write_text("routes: []\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must contain a YAML mapping"):
                load_app_config(config, routes)

    def test_parse_router_config_reads_flat_signal_and_circuit_values(self) -> None:
        router = parse_router_config(
            {
                "signal_base_url": "http://signal.example",
                "allow_remote_signal_base_url": True,
                "state_db": "state.db",
                "media_root": "media",
                "signal_attachment_root": "~/attachments",
                "max_attachment_bytes": "42",
                "max_signal_event_bytes": "84",
                "max_acp_line_bytes": "1024",
                "max_reply_chars": "2048",
                "max_signal_message_bytes": "1500",
                "work_root": "work",
                "maintenance_reply": "maintenance",
                "failure_reply": "failure",
                "model_failure_reply": "model failure",
                "busy_notice_after_seconds": "3.5",
                "busy_notice": "busy",
                "acp_prompt_timeout_seconds": "450",
                "acp_initialize_timeout_seconds": "20",
                "circuit_breaker": {
                    "failures": "7",
                    "window_seconds": "9.5",
                    "recovery_seconds": "12.5",
                },
            }
        )

        self.assertEqual(router.signal_base_url, "http://signal.example")
        self.assertTrue(router.allow_remote_signal_base_url)
        self.assertEqual(router.max_attachment_bytes, 42)
        self.assertEqual(router.max_signal_event_bytes, 84)
        self.assertEqual(router.max_acp_line_bytes, 1024)
        self.assertEqual(router.max_reply_chars, 2048)
        self.assertEqual(router.max_signal_message_bytes, 1500)
        self.assertEqual(router.acp_prompt_timeout_seconds, 450.0)
        self.assertEqual(router.acp_initialize_timeout_seconds, 20.0)
        self.assertEqual(router.circuit_breaker.failures, 7)
        self.assertEqual(router.circuit_breaker.window_seconds, 9.5)
        self.assertEqual(router.circuit_breaker.recovery_seconds, 12.5)
        self.assertEqual(router.maintenance_reply, "maintenance")
        self.assertEqual(router.model_failure_reply, "model failure")

    def test_parse_router_config_defaults_acp_prompt_timeout_and_recovery(self) -> None:
        router = parse_router_config({})
        self.assertEqual(router.acp_prompt_timeout_seconds, 300.0)
        self.assertEqual(router.acp_initialize_timeout_seconds, 30.0)
        self.assertEqual(router.circuit_breaker.recovery_seconds, 300.0)
        self.assertEqual(router.max_signal_message_bytes, 1900)
        self.assertFalse(router.control.enabled)
        self.assertEqual(router.control_socket_path, Path("./private/work") / "control.sock")

    def test_parse_router_config_defaults_max_concurrent_turns(self) -> None:
        self.assertEqual(parse_router_config({}).max_concurrent_turns, 8)

    def test_parse_router_config_reads_max_concurrent_turns(self) -> None:
        self.assertEqual(parse_router_config({"max_concurrent_turns": "4"}).max_concurrent_turns, 4)

    def test_parse_router_config_rejects_non_positive_max_concurrent_turns(self) -> None:
        for value in (0, -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError, "router.max_concurrent_turns must be positive"
                ):
                    parse_router_config({"max_concurrent_turns": value})

    def test_parse_router_config_rejects_non_positive_or_non_finite_initialize_timeout(
        self,
    ) -> None:
        for value in ("0", "-5", "nan", "inf", "-inf"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "router.acp_initialize_timeout_seconds must be a positive finite number",
                ):
                    parse_router_config({"acp_initialize_timeout_seconds": value})

    def test_parse_router_config_reads_control_socket_settings(self) -> None:
        router = parse_router_config(
            {
                "work_root": "work",
                "control": {
                    "enabled": "true",
                    "socket_path": "work/control.sock",
                    "route_lock_timeout_seconds": "2.5",
                    "max_notification_payload_bytes": "4096",
                },
            }
        )

        self.assertTrue(router.control.enabled)
        self.assertEqual(router.control.socket_path, Path("work/control.sock"))
        self.assertEqual(router.control_socket_path, Path("work/control.sock"))
        self.assertEqual(router.control.route_lock_timeout_seconds, 2.5)
        self.assertEqual(router.control.max_notification_payload_bytes, 4096)
        self.assertEqual(router.control_request_line_limit_bytes, 4096 + 8 * 1024)

        with self.assertRaisesRegex(ValueError, "non-negative"):
            parse_router_config({"control": {"route_lock_timeout_seconds": -1}})
        with self.assertRaisesRegex(ValueError, "positive"):
            parse_router_config({"control": {"max_notification_payload_bytes": 0}})

    def test_parse_router_config_warns_when_signal_message_bytes_above_threshold(self) -> None:
        with self.assertLogs("signal_hermes_router.config", level="WARNING") as logs:
            parse_router_config({"max_signal_message_bytes": 2500})
        self.assertIn("max_signal_message_bytes=2500", "\n".join(logs.output))

    def test_parse_router_config_rejects_signal_message_bytes_below_floor(self) -> None:
        with self.assertRaisesRegex(ValueError, ">= 16"):
            parse_router_config({"max_signal_message_bytes": 8})

    def test_parse_router_config_rejects_non_positive_signal_message_bytes(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            parse_router_config({"max_signal_message_bytes": 0})

    def test_parse_router_config_requires_loopback_signal_url_by_default(self) -> None:
        for value in (
            {"signal_base_url": "http://127.0.0.1:8080"},
            {"signal": {"base_url": "http://localhost:8080"}},
            {"signal": {"base_url": "http://[::1]:8080"}},
        ):
            with self.subTest(value=value):
                self.assertEqual(parse_router_config(value).allow_remote_signal_base_url, False)

        with self.assertRaisesRegex(ValueError, "loopback"):
            parse_router_config({"signal_base_url": "http://signal.example"})

        router = parse_router_config(
            {
                "signal": {
                    "base_url": "http://signal.example",
                    "allow_remote_base_url": "true",
                }
            }
        )
        self.assertTrue(router.allow_remote_signal_base_url)

    def test_load_app_config_remote_opt_in_belongs_inside_router_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            routes = root / "routes.yaml"
            routes.write_text(
                """
routes:
  - platform: signal
    group_id: GROUP
    profile: profile
""",
                encoding="utf-8",
            )
            config = root / "config.yaml"
            config.write_text(
                """
allow_remote_signal_base_url: true
router:
  signal:
    base_url: "http://signal.example"
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "loopback"):
                load_app_config(config, routes)

            config.write_text(
                """
router:
  allow_remote_signal_base_url: true
  signal:
    base_url: "http://signal.example"
""",
                encoding="utf-8",
            )
            app = load_app_config(config, routes)
            self.assertTrue(app.router.allow_remote_signal_base_url)

    def test_parse_router_config_rejects_invalid_signal_url(self) -> None:
        for value in ("signal.example", "file:///tmp/socket"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "HTTP URL"):
                parse_router_config({"signal_base_url": value})

    def test_parse_router_config_requires_positive_limits(self) -> None:
        for key in (
            "max_attachment_bytes",
            "max_signal_event_bytes",
            "max_acp_line_bytes",
            "max_reply_chars",
        ):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "positive"):
                parse_router_config({key: 0})

    def test_duplicate_route_keys_are_rejected(self) -> None:
        raw = {
            "routes": [
                {
                    "platform": "signal",
                    "group_id": "GROUP",
                    "profile": "profile-one",
                },
                {
                    "platform": "signal",
                    "group_id": "GROUP",
                    "profile": "profile-two",
                },
            ]
        }
        with self.assertRaisesRegex(ValueError, "duplicate route key"):
            parse_routes(raw)

    def test_route_names_are_optional_and_unique_safe_tokens(self) -> None:
        routes = parse_routes(
            {
                "routes": [
                    {"platform": "signal", "group_id": "GROUP", "profile": "profile"},
                    {
                        "platform": "signal",
                        "group_id": "OTHER",
                        "profile": "profile",
                        "name": "agenda-route_1",
                    },
                ]
            }
        )
        self.assertIsNone(routes[0].name)
        self.assertEqual(routes[1].name, "agenda-route_1")

        with self.assertRaisesRegex(ValueError, "duplicate route name"):
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "same",
                        },
                        {
                            "platform": "signal",
                            "group_id": "OTHER",
                            "profile": "profile",
                            "name": "same",
                        },
                    ]
                }
            )

        with self.assertRaisesRegex(ValueError, "route name must match"):
            parse_route(
                {
                    "platform": "signal",
                    "group_id": "GROUP",
                    "profile": "profile",
                    "name": "../bad",
                }
            )

    def test_scheduled_jobs_parse_against_named_routes(self) -> None:
        routes = tuple(
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "agenda-route",
                        }
                    ]
                }
            )
        )

        jobs = parse_scheduled_jobs(
            {
                "scheduled_jobs": [
                    {
                        "id": "daily-agenda",
                        "route": "agenda-route",
                        "prompt": "Prepare the daily agenda.",
                        "description": "Synthetic example",
                        "permissions": [
                            {
                                "tool": "read_file",
                                "arguments": {"path": {"prefix": "/tmp/example"}},
                            }
                        ],
                    }
                ]
            },
            routes,
        )

        self.assertEqual(jobs[0].id, "daily-agenda")
        self.assertEqual(jobs[0].route_name, "agenda-route")
        self.assertEqual(jobs[0].kind, SyntheticTurnKind.SCHEDULED_JOB)
        self.assertEqual(jobs[0].namespace, "scheduled:daily-agenda")
        self.assertIsNotNone(jobs[0].permission_policy)

    def test_scheduled_jobs_on_mcp_only_route_inherit_mcp_only_flag(self) -> None:
        routes = tuple(
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "mcp-route",
                            "mcp_only": True,
                        }
                    ]
                }
            )
        )

        jobs = parse_scheduled_jobs(
            {
                "scheduled_jobs": [
                    {
                        "id": "daily-agenda",
                        "route": "mcp-route",
                        "prompt": "Prepare the daily agenda.",
                        "permissions": [{"tool": "read_file"}],
                    }
                ]
            },
            routes,
        )

        self.assertTrue(jobs[0].permission_policy.mcp_only)

    def test_notifications_on_mcp_only_route_inherit_mcp_only_flag(self) -> None:
        routes = tuple(
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "mcp-route",
                            "mcp_only": True,
                        }
                    ]
                }
            )
        )

        notifications = parse_notifications(
            {
                "notifications": [
                    {
                        "id": "backup-report",
                        "route": "mcp-route",
                        "prompt": "Summarize the notification payload.",
                        "permissions": [{"tool": "read_file"}],
                    }
                ]
            },
            routes,
        )

        self.assertTrue(notifications[0].permission_policy.mcp_only)

    def test_notifications_parse_against_named_routes(self) -> None:
        routes = tuple(
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "agenda-route",
                        }
                    ]
                }
            )
        )

        notifications = parse_notifications(
            {
                "notifications": [
                    {
                        "id": "backup-report",
                        "route": "agenda-route",
                        "prompt": "Summarize the notification payload.",
                        "description": "Synthetic notification example",
                        "permissions": [
                            {
                                "tool": "read_file",
                                "arguments": {"path": {"prefix": "/tmp/example"}},
                            }
                        ],
                    }
                ]
            },
            routes,
        )

        self.assertEqual(notifications[0].id, "backup-report")
        self.assertEqual(notifications[0].route_name, "agenda-route")
        self.assertEqual(notifications[0].kind, SyntheticTurnKind.NOTIFICATION)
        self.assertEqual(notifications[0].namespace, "synthetic:notification:backup-report")
        self.assertIsNotNone(notifications[0].permission_policy)

    def test_scheduled_jobs_reject_invalid_shapes(self) -> None:
        routes = tuple(
            parse_routes(
                {
                    "routes": [
                        {
                            "platform": "signal",
                            "group_id": "GROUP",
                            "profile": "profile",
                            "name": "agenda-route",
                        }
                    ]
                }
            )
        )

        with self.assertRaisesRegex(ValueError, "scheduled_jobs must be a list"):
            parse_scheduled_jobs({"scheduled_jobs": {}}, routes)
        with self.assertRaisesRegex(ValueError, "duplicate scheduled job id"):
            parse_scheduled_jobs(
                {
                    "scheduled_jobs": [
                        {"id": "daily", "route": "agenda-route", "prompt": "one"},
                        {"id": "daily", "route": "agenda-route", "prompt": "two"},
                    ]
                },
                routes,
            )
        with self.assertRaisesRegex(ValueError, "unknown route name"):
            parse_scheduled_jobs(
                {"scheduled_jobs": [{"id": "daily", "route": "missing", "prompt": "one"}]},
                routes,
            )
        with self.assertRaisesRegex(ValueError, "prompt must not be empty"):
            parse_scheduled_jobs(
                {"scheduled_jobs": [{"id": "daily", "route": "agenda-route", "prompt": "  "}]},
                routes,
            )
        with self.assertRaisesRegex(ValueError, "prompt must be a string"):
            parse_scheduled_jobs(
                {"scheduled_jobs": [{"id": "daily", "route": "agenda-route", "prompt": None}]},
                routes,
            )
        with self.assertRaisesRegex(ValueError, "prompt must be a string"):
            parse_scheduled_jobs(
                {"scheduled_jobs": [{"id": "daily", "route": "agenda-route", "prompt": []}]},
                routes,
            )

        with self.assertRaisesRegex(ValueError, "notifications must be a list"):
            parse_notifications({"notifications": {}}, routes)
        with self.assertRaisesRegex(ValueError, "duplicate notification id"):
            parse_notifications(
                {
                    "notifications": [
                        {"id": "report", "route": "agenda-route", "prompt": "one"},
                        {"id": "report", "route": "agenda-route", "prompt": "two"},
                    ]
                },
                routes,
            )

    def test_parse_direct_route_requires_sender_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires sender_id"):
            parse_route(
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "profile": "profile",
                }
            )

    def test_parse_direct_route_rejects_wildcard_sender_identity(self) -> None:
        for value in ("", "*", "prefix-*", "any"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "wildcard|empty"):
                parse_route(
                    {
                        "platform": "signal",
                        "chat_type": "direct",
                        "sender_id": value,
                        "profile": "profile",
                    }
                )

    def test_parse_direct_route_rejects_group_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not set group_id"):
            parse_route(
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "group_id": "GROUP",
                    "sender_id": "sender-uuid",
                    "profile": "profile",
                }
            )

    def test_parse_direct_route_uses_hashed_route_key(self) -> None:
        route = parse_route(
            {
                "platform": "signal",
                "chat_type": "direct",
                "sender_id": "sender-uuid",
                "sender_number": "+00000000000",
                "profile": "profile",
            }
        )

        self.assertEqual(route.chat_type, ChatType.DIRECT)
        self.assertIsNone(route.group_id)
        self.assertEqual(route.sender_id, "sender-uuid")
        self.assertEqual(route.sender_number, "+00000000000")
        self.assertTrue(route.key.startswith("signal:direct:"))
        self.assertNotIn("sender-uuid", route.key)

    def test_duplicate_direct_routes_are_rejected(self) -> None:
        raw = {
            "routes": [
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "sender_id": "sender-uuid",
                    "profile": "profile-one",
                },
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "sender_id": "sender-uuid",
                    "profile": "profile-two",
                },
            ]
        }
        with self.assertRaisesRegex(ValueError, "duplicate direct sender_id"):
            parse_routes(raw)

    def test_duplicate_direct_sender_numbers_are_rejected(self) -> None:
        raw = {
            "routes": [
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "sender_id": "sender-one",
                    "sender_number": "+00000000000",
                    "profile": "profile-one",
                },
                {
                    "platform": "signal",
                    "chat_type": "direct",
                    "sender_id": "sender-two",
                    "sender_number": "+00000000000",
                    "profile": "profile-two",
                },
            ]
        }
        with self.assertRaisesRegex(ValueError, "duplicate direct sender_number"):
            parse_routes(raw)

    def test_find_direct_route_is_uuid_authoritative(self) -> None:
        direct = parse_route(
            {
                "platform": "signal",
                "chat_type": "direct",
                "sender_id": "sender-uuid",
                "sender_number": "+00000000000",
                "profile": "profile",
            }
        )
        app = AppConfig(router=RouterConfig(), routes=(direct,))

        self.assertEqual(app.find_direct_route("signal", "sender-uuid", None), direct)
        self.assertEqual(app.find_direct_route("signal", None, "+00000000000"), direct)
        self.assertIsNone(app.find_direct_route("signal", "other-uuid", "+00000000000"))

    def test_find_route_for_event_routes_direct_and_group_events(self) -> None:
        group = parse_route({"platform": "signal", "group_id": "GROUP", "profile": "group-profile"})
        direct = parse_route(
            {
                "platform": "signal",
                "chat_type": "direct",
                "sender_id": "sender-uuid",
                "profile": "direct-profile",
            }
        )
        app = AppConfig(router=RouterConfig(), routes=(group, direct))

        self.assertEqual(
            app.find_route_for_event(
                NormalizedEvent(
                    platform="signal",
                    chat_type=ChatType.GROUP,
                    group_id="GROUP",
                    sender_id="sender",
                    source_uuid="sender",
                    timestamp=1,
                    text="hello",
                )
            ),
            group,
        )
        self.assertEqual(
            app.find_route_for_event(
                NormalizedEvent(
                    platform="signal",
                    chat_type=ChatType.DIRECT,
                    group_id=None,
                    sender_id="sender-uuid",
                    source_uuid="sender-uuid",
                    timestamp=1,
                    text="hello",
                )
            ),
            direct,
        )

    def test_parse_routes_requires_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "routes list"):
            parse_routes({"routes": {"not": "a-list"}})

    def test_parse_route_rejects_denylist_and_non_json_context(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowlist-only"):
            parse_route(
                {
                    "platform": "signal",
                    "group_id": "GROUP",
                    "profile": "profile",
                    "deny": [],
                }
            )

        with self.assertRaisesRegex(ValueError, "JSON serializable"):
            parse_route(
                {
                    "platform": "signal",
                    "group_id": "GROUP",
                    "profile": "profile",
                    "route_context": {"bad": object()},
                }
            )

        for value in (float("nan"), float("inf"), float("-inf")):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(
                    ValueError,
                    "finite JSON serializable",
                ),
            ):
                parse_route(
                    {
                        "platform": "signal",
                        "group_id": "GROUP",
                        "profile": "profile",
                        "route_context": {"purpose": value},
                    }
                )

    def test_profile_names_reject_path_shapes(self) -> None:
        for value in (
            None,
            "../outside",
            "/absolute",
            "nested/profile",
            r"nested\profile",
            ".hidden",
            "-flag",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_profile_name(value)

    def test_parse_route_normalizes_profile_name(self) -> None:
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile-01",
            }
        )
        self.assertEqual(route.profile, "profile-01")

    def test_parse_route_reads_resume_failure_recovery_flag(self) -> None:
        default_route = parse_route(
            {"platform": "signal", "group_id": "GROUP", "profile": "profile"}
        )
        opt_in_route = parse_route(
            {
                "platform": "signal",
                "group_id": "OTHER",
                "profile": "profile",
                "recreate_session_on_resume_failure": "true",
            }
        )

        self.assertFalse(default_route.recreate_session_on_resume_failure)
        self.assertTrue(opt_in_route.recreate_session_on_resume_failure)

    def test_parse_route_burst_policy_defaults_off(self) -> None:
        route = parse_route({"platform": "signal", "group_id": "GROUP", "profile": "profile"})

        self.assertIsNone(route.max_event_age_seconds)
        self.assertIsNone(route.inbound_rate_limit)

    def test_parse_route_reads_max_event_age_seconds(self) -> None:
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "max_event_age_seconds": 900,
            }
        )

        self.assertEqual(route.max_event_age_seconds, 900.0)

    def test_parse_route_rejects_invalid_max_event_age_seconds(self) -> None:
        for value in (0, -1, float("inf"), float("nan"), True, "soon", None):
            raw = {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "max_event_age_seconds": value,
            }
            if value is None:
                # Explicit null means off, same as an absent key.
                self.assertIsNone(parse_route(raw).max_event_age_seconds)
                continue
            with self.assertRaises(ValueError, msg=repr(value)):
                parse_route(raw)

    def test_parse_route_reads_inbound_rate_limit(self) -> None:
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "inbound_rate_limit": {"max_turns": 10, "window_seconds": 60},
            }
        )

        assert route.inbound_rate_limit is not None
        self.assertEqual(route.inbound_rate_limit.max_turns, 10)
        self.assertEqual(route.inbound_rate_limit.window_seconds, 60.0)

    def test_parse_route_inbound_rate_limit_accepts_decimal_string_max_turns(self) -> None:
        # Secret-resolver values arrive as strings.
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "inbound_rate_limit": {"max_turns": "3", "window_seconds": "60"},
            }
        )

        assert route.inbound_rate_limit is not None
        self.assertEqual(route.inbound_rate_limit.max_turns, 3)
        self.assertEqual(route.inbound_rate_limit.window_seconds, 60.0)

    def test_parse_route_rejects_invalid_inbound_rate_limit(self) -> None:
        invalid_blocks = [
            "fast",
            ["max_turns"],
            {"max_turns": 10},
            {"window_seconds": 60},
            {"max_turns": 10, "window_seconds": 60, "burst": 5},
            {"max_turns": 0, "window_seconds": 60},
            {"max_turns": True, "window_seconds": 60},
            {"max_turns": 1.5, "window_seconds": 60},
            {"max_turns": "1.5", "window_seconds": 60},
            {"max_turns": 10, "window_seconds": 0},
            {"max_turns": 10, "window_seconds": -5},
            {"max_turns": 10, "window_seconds": float("inf")},
            {"max_turns": 10, "window_seconds": True},
        ]
        for block in invalid_blocks:
            with self.assertRaises(ValueError, msg=repr(block)):
                parse_route(
                    {
                        "platform": "signal",
                        "group_id": "GROUP",
                        "profile": "profile",
                        "inbound_rate_limit": block,
                    }
                )

    def test_parse_route_session_rotation_defaults_off(self) -> None:
        route = parse_route({"platform": "signal", "group_id": "GROUP", "profile": "profile"})

        self.assertIsNone(route.session_max_turns)
        self.assertIsNone(route.session_max_age_seconds)

    def test_parse_route_reads_session_rotation_knobs(self) -> None:
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "session_policy": "persistent_sender",
                "session_max_turns": 50,
                "session_max_age_seconds": 86400,
            }
        )

        self.assertEqual(route.session_max_turns, 50)
        self.assertEqual(route.session_max_age_seconds, 86400.0)

    def test_parse_route_session_rotation_accepts_decimal_string_max_turns(self) -> None:
        # Secret-resolver values arrive as strings.
        route = parse_route(
            {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "session_max_turns": "50",
            }
        )

        self.assertEqual(route.session_max_turns, 50)

    def test_parse_route_rejects_invalid_session_max_turns(self) -> None:
        for value in (0, -1, True, 1.5, "1.5", "many", None):
            raw = {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "session_max_turns": value,
            }
            if value is None:
                # Explicit null means off, same as an absent key.
                self.assertIsNone(parse_route(raw).session_max_turns)
                continue
            with self.assertRaises(ValueError, msg=repr(value)):
                parse_route(raw)

    def test_parse_route_rejects_invalid_session_max_age_seconds(self) -> None:
        for value in (0, -1, float("inf"), float("nan"), True, "soon", None):
            raw = {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "session_max_age_seconds": value,
            }
            if value is None:
                # Explicit null means off, same as an absent key.
                self.assertIsNone(parse_route(raw).session_max_age_seconds)
                continue
            with self.assertRaises(ValueError, msg=repr(value)):
                parse_route(raw)

    def test_parse_route_rejects_session_rotation_on_ephemeral_routes(self) -> None:
        for knob in ({"session_max_turns": 50}, {"session_max_age_seconds": 3600}):
            raw = {
                "platform": "signal",
                "group_id": "GROUP",
                "profile": "profile",
                "session_policy": "ephemeral",
                **knob,
            }
            with self.assertRaises(ValueError, msg=repr(knob)):
                parse_route(raw)

    def test_parse_router_config_reads_busy_notice_cooldown(self) -> None:
        default = parse_router_config({})
        self.assertEqual(default.busy_notice_cooldown_seconds, 0.0)

        configured = parse_router_config({"busy_notice_cooldown_seconds": 300})
        self.assertEqual(configured.busy_notice_cooldown_seconds, 300.0)

    def test_parse_router_config_rejects_invalid_busy_notice_cooldown(self) -> None:
        for value in (-1, float("inf"), float("nan"), True, "later"):
            with self.assertRaises(ValueError, msg=repr(value)):
                parse_router_config({"busy_notice_cooldown_seconds": value})


class RetentionConfigTests(unittest.TestCase):
    def test_defaults_prune_dedupe_and_leave_media_off(self) -> None:
        retention = parse_router_config({}).retention
        self.assertEqual(retention.sweep_interval_seconds, 21600.0)
        self.assertEqual(retention.dedupe_handled_seconds, 2592000.0)
        self.assertIsNone(retention.media_max_age_seconds)
        self.assertIsNone(retention.media_max_total_bytes)
        self.assertTrue(retention.dedupe_enabled)
        self.assertFalse(retention.media_enabled)
        self.assertTrue(retention.enabled)

    def test_parse_round_trip(self) -> None:
        retention = parse_router_config(
            {
                "retention": {
                    "sweep_interval_seconds": 3600,
                    "dedupe_handled_seconds": 86400,
                    "media_max_age_seconds": 7776000,
                    "media_max_total_bytes": 1073741824,
                }
            }
        ).retention
        self.assertEqual(retention.sweep_interval_seconds, 3600.0)
        self.assertEqual(retention.dedupe_handled_seconds, 86400.0)
        self.assertEqual(retention.media_max_age_seconds, 7776000.0)
        self.assertEqual(retention.media_max_total_bytes, 1073741824)
        self.assertTrue(retention.media_enabled)

    def test_null_windows_disable_each_store(self) -> None:
        retention = parse_router_config({"retention": {"dedupe_handled_seconds": None}}).retention
        self.assertFalse(retention.dedupe_enabled)
        self.assertFalse(retention.enabled)

        size_only = parse_router_config(
            {
                "retention": {
                    "dedupe_handled_seconds": None,
                    "media_max_total_bytes": 4096,
                }
            }
        ).retention
        self.assertTrue(size_only.media_enabled)
        self.assertTrue(size_only.enabled)

    def test_rejects_sub_day_retention_windows(self) -> None:
        for key in ("dedupe_handled_seconds", "media_max_age_seconds"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "86400"):
                parse_router_config({"retention": {key: 3600}})

    def test_rejects_invalid_values_and_unknown_keys(self) -> None:
        cases = [
            {"sweep_interval_seconds": 0},
            {"sweep_interval_seconds": float("inf")},
            {"dedupe_handled_seconds": True},
            {"media_max_total_bytes": 0},
            {"media_max_total_bytes": 1.5},
            {"media_max_total_bytes": True},
            {"unexpected_knob": 1},
        ]
        for raw in cases:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_router_config({"retention": raw})
        with self.assertRaisesRegex(ValueError, "must be a mapping"):
            parse_router_config({"retention": ["not", "a", "mapping"]})


if __name__ == "__main__":
    unittest.main()
