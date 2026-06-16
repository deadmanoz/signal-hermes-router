from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterConfig,
    load_app_config,
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

    def test_app_config_find_route_returns_none_for_missing_route(self) -> None:
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

        self.assertIsNone(app.find_route("signal", "OTHER"))

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
                "busy_notice_after_seconds": "3.5",
                "busy_notice": "busy",
                "acp_prompt_timeout_seconds": "450",
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
        self.assertEqual(router.circuit_breaker.failures, 7)
        self.assertEqual(router.circuit_breaker.window_seconds, 9.5)
        self.assertEqual(router.circuit_breaker.recovery_seconds, 12.5)
        self.assertEqual(router.maintenance_reply, "maintenance")

    def test_parse_router_config_defaults_acp_prompt_timeout_and_recovery(self) -> None:
        router = parse_router_config({})
        self.assertEqual(router.acp_prompt_timeout_seconds, 300.0)
        self.assertEqual(router.circuit_breaker.recovery_seconds, 300.0)
        self.assertEqual(router.max_signal_message_bytes, 1900)
        self.assertFalse(router.control.enabled)
        self.assertEqual(router.control_socket_path, Path("./private/work") / "control.sock")

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


if __name__ == "__main__":
    unittest.main()
