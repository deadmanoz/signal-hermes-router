from __future__ import annotations

import asyncio
import argparse
import logging
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import AsyncMock

from signal_hermes_router import cli as cli_module
from signal_hermes_router import sessions as sessions_module
from signal_hermes_router.config import (
    DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES,
    AppConfig,
    Route,
    RouterConfig,
)
from signal_hermes_router.models import RouteState, SessionKeyInput, SessionPolicy
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.preflight import PreflightScope
from signal_hermes_router.sessions import ProfileSupervisor, RoutedSession, SessionRegistry
from tests.support import make_event, make_route


class CliTests(unittest.IsolatedAsyncioTestCase):
    def test_main_parses_paths_log_level_and_runs_async_entrypoint(self) -> None:
        with (
            patch.object(cli_module.logging, "basicConfig") as basic_config,
            patch.object(cli_module.asyncio, "run") as run,
        ):
            cli_module.main(
                [
                    "--config",
                    "config.private.yaml",
                    "--routes",
                    "routes.private.yaml",
                    "--log-level",
                    "debug",
                ]
            )

        basic_config.assert_called_once_with(level=logging.DEBUG)
        coroutine = run.call_args.args[0]
        self.assertTrue(asyncio.iscoroutine(coroutine))
        coroutine.close()

    def test_main_accepts_explicit_serve_alias(self) -> None:
        with patch.object(cli_module.asyncio, "run") as run:
            cli_module.main(["--log-level", "warning", "serve"])

        coroutine = run.call_args.args[0]
        self.assertTrue(asyncio.iscoroutine(coroutine))
        coroutine.close()

    async def test_run_loads_config_runs_router_and_always_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = AppConfig(
                router=RouterConfig(
                    signal_base_url="http://signal.test",
                    work_root=Path(tmp) / "work",
                ),
                routes=(
                    make_route(),
                    Route(
                        platform="signal",
                        group_id="shadow-group",
                        profile="shadow-profile",
                        session_policy=SessionPolicy.PERSISTENT_ROUTE,
                        state=RouteState.SHADOW,
                    ),
                ),
            )
            instances = []

            class FakeRouter:
                def __init__(self, config: AppConfig) -> None:
                    self.config = config
                    self.ran = False
                    self.closed = False
                    instances.append(self)

                async def run_forever(self) -> None:
                    self.ran = True

                async def close(self) -> None:
                    self.closed = True

            with (
                patch.object(cli_module, "load_app_config", return_value=app) as load_config,
                patch.object(cli_module, "SignalHermesRouter", FakeRouter),
                self.assertLogs(level="INFO") as logs,
            ):
                await cli_module._run(Path("config.yaml"), Path("routes.yaml"))

            load_config.assert_called_once_with(Path("config.yaml"), Path("routes.yaml"))
            self.assertEqual(len(instances), 1)
            self.assertTrue(instances[0].ran)
            self.assertTrue(instances[0].closed)
            output = "\n".join(logs.output)
            self.assertIn("routes={'active': 1, 'shadow': 1}", output)
            self.assertNotIn("signal.test", output)
            self.assertNotIn(str(Path(tmp) / "work"), output)

    def test_parse_scheduled_at_accepts_epoch_ms_and_aware_iso(self) -> None:
        self.assertEqual(cli_module.parse_scheduled_at("1714521600000"), 1714521600000)
        self.assertEqual(
            cli_module.parse_scheduled_at("2024-05-01T00:00:00+00:00"),
            1714521600000,
        )
        self.assertEqual(cli_module.parse_scheduled_at("2024-05-01T00:00:00Z"), 1714521600000)

        with self.assertRaisesRegex(ValueError, "timezone"):
            cli_module.parse_scheduled_at("2024-05-01T00:00:00")
        with self.assertRaisesRegex(ValueError, "non-negative"):
            cli_module.parse_scheduled_at("-1")

    async def test_main_async_dispatches_control_commands_and_rejects_unknown_command(
        self,
    ) -> None:
        args = argparse.Namespace(command="trigger-job")
        with patch.object(cli_module, "_trigger_job", AsyncMock(return_value=0)) as trigger:
            self.assertEqual(await cli_module._main_async(args), 0)
        trigger.assert_awaited_once_with(args)

        notify_args = argparse.Namespace(command="notify-route")
        with patch.object(cli_module, "_notify_route", AsyncMock(return_value=0)) as notify:
            self.assertEqual(await cli_module._main_async(notify_args), 0)
        notify.assert_awaited_once_with(notify_args)

        preflight_args = argparse.Namespace(command="preflight-permissions")
        with patch.object(
            cli_module,
            "_preflight_permissions",
            AsyncMock(return_value=0),
        ) as preflight:
            self.assertEqual(await cli_module._main_async(preflight_args), 0)
        preflight.assert_awaited_once_with(preflight_args)

        status_args = argparse.Namespace(command="route-status")
        with patch.object(cli_module, "_route_status", AsyncMock(return_value=0)) as status:
            self.assertEqual(await cli_module._main_async(status_args), 0)
        status.assert_awaited_once_with(status_args)

        with self.assertRaisesRegex(ValueError, "unknown command"):
            await cli_module._main_async(argparse.Namespace(command="unknown"))

    def test_main_raises_for_nonzero_async_exit(self) -> None:
        with patch.object(cli_module.asyncio, "run", return_value=1) as run:
            with self.assertRaises(SystemExit) as raised:
                cli_module.main(["trigger-job", "daily-agenda"])

        coroutine = run.call_args.args[0]
        self.assertTrue(asyncio.iscoroutine(coroutine))
        coroutine.close()
        self.assertEqual(raised.exception.code, 1)

    async def test_trigger_job_via_control_socket_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            requests: list[dict] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                requests.append(json.loads((await reader.readline()).decode("utf-8")))
                writer.write(b'{"status":"delivered"}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.trigger_job_via_control_socket(
                    socket_path,
                    "daily-agenda",
                    scheduled_at=1714521600000,
                    idempotency_key="stable-fire",
                    timeout=1.5,
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

            self.assertEqual(response, {"status": "delivered"})
            self.assertEqual(
                requests,
                [
                    {
                        "command": "trigger_job",
                        "job_id": "daily-agenda",
                        "scheduled_at": 1714521600000,
                        "idempotency_key": "stable-fire",
                        "timeout": 1.5,
                    }
                ],
            )

    async def test_notify_route_via_control_socket_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            requests: list[dict] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                requests.append(json.loads((await reader.readline()).decode("utf-8")))
                writer.write(b'{"status":"delivered"}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.notify_route_via_control_socket(
                    socket_path,
                    "backup-report",
                    payload={"b": 2, "a": 1},
                    idempotency_key="backup-fire",
                    timeout=1.5,
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

            self.assertEqual(response, {"status": "delivered"})
            self.assertEqual(
                requests,
                [
                    {
                        "command": "notify_route",
                        "notification_id": "backup-report",
                        "payload": {"a": 1, "b": 2},
                        "idempotency_key": "backup-fire",
                        "timeout": 1.5,
                    }
                ],
            )

    async def test_notify_route_via_control_socket_includes_attachments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            attachment = Path(tmp) / "media" / "person.png"
            requests: list[dict] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                requests.append(json.loads((await reader.readline()).decode("utf-8")))
                writer.write(b'{"status":"delivered"}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.notify_route_via_control_socket(
                    socket_path,
                    "camera-person",
                    payload={"camera": "front"},
                    attachments=[attachment],
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

        self.assertEqual(response, {"status": "delivered"})
        self.assertEqual(
            requests,
            [
                {
                    "command": "notify_route",
                    "notification_id": "camera-person",
                    "payload": {"camera": "front"},
                    "attachments": [str(attachment)],
                }
            ],
        )

    async def test_preflight_permissions_via_control_socket_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            requests: list[dict] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                requests.append(json.loads((await reader.readline()).decode("utf-8")))
                writer.write(b'{"status":"ok","missing_tools":[]}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.preflight_permissions_via_control_socket(
                    socket_path,
                    scope=PreflightScope(
                        active_only=True,
                        route_names=("example-route",),
                        route_indexes=(0,),
                        profiles=("profile",),
                    ),
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

            self.assertEqual(response, {"status": "ok", "missing_tools": []})
            self.assertEqual(
                requests,
                [
                    {
                        "command": "preflight_permissions",
                        "scope": {
                            "active_only": True,
                            "route_names": ["example-route"],
                            "route_indexes": [0],
                            "profiles": ["profile"],
                        },
                    }
                ],
            )

    async def test_route_status_via_control_socket_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            requests: list[dict] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                requests.append(json.loads((await reader.readline()).decode("utf-8")))
                writer.write(b'{"status":"ok","routes":[],"route_count":0}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.route_status_via_control_socket(
                    socket_path,
                    route_names=("example-route",),
                    route_indexes=(0,),
                    profiles=("profile",),
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

            self.assertEqual(response, {"status": "ok", "routes": [], "route_count": 0})
            self.assertEqual(
                requests,
                [
                    {
                        "command": "route_status",
                        "routes": ["example-route"],
                        "route_indexes": [0],
                        "profiles": ["profile"],
                    }
                ],
            )

    async def test_preflight_permissions_via_control_socket_accepts_large_response(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            large_text = "x" * (70 * 1024)

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.write(json.dumps({"status": "ok", "padding": large_text}).encode("utf-8"))
                writer.write(b"\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module.preflight_permissions_via_control_socket(
                    socket_path,
                    scope=PreflightScope(active_only=True),
                    client_timeout=1.5,
                )
                server.close()
                await server.wait_closed()

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["padding"], large_text)

    async def test_trigger_job_via_control_socket_rejects_bad_responses(self) -> None:
        async def run_server(body: bytes) -> Path:
            socket_path = Path(tempfile.mkdtemp()) / "control.sock"

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                if body:
                    writer.write(body)
                    await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            servers.append(server)
            return socket_path

        servers: list[asyncio.Server] = []
        try:
            empty_socket = await run_server(b"")
            with self.assertRaisesRegex(RuntimeError, "closed without a response"):
                await cli_module.trigger_job_via_control_socket(empty_socket, "daily-agenda")

            list_socket = await run_server(b'["not-an-object"]\n')
            with self.assertRaisesRegex(RuntimeError, "non-object"):
                await cli_module.trigger_job_via_control_socket(list_socket, "daily-agenda")
        finally:
            for server in servers:
                server.close()
                await server.wait_closed()

    async def test_control_round_trip_accepts_no_client_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.write(b'{"status":"ok"}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                response = await cli_module._control_round_trip(
                    socket_path,
                    {"command": "preflight_permissions"},
                    client_timeout=None,
                )
                server.close()
                await server.wait_closed()

        self.assertEqual(response, {"status": "ok"})

    async def test_control_round_trip_reports_response_size_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            response_body = json.dumps({"status": "ok", "padding": "x" * 256}).encode("utf-8")

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.write(response_body + b"\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            async with server:
                with self.assertRaisesRegex(RuntimeError, "response exceeded 64 byte"):
                    await cli_module._control_round_trip(
                        socket_path,
                        {"command": "preflight_permissions"},
                        client_timeout=1.5,
                        response_limit_bytes=64,
                    )
                server.close()
                await server.wait_closed()

    async def test_trigger_job_via_control_socket_times_out_waiting_for_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "control.sock"
            release = asyncio.Event()

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                await release.wait()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_unix_server(handle, path=str(socket_path))
            try:
                async with server:
                    with self.assertRaises(asyncio.TimeoutError):
                        await cli_module.trigger_job_via_control_socket(
                            socket_path,
                            "daily-agenda",
                            client_timeout=0.001,
                        )
                    release.set()
                    server.close()
                    await server.wait_closed()
            finally:
                release.set()

    async def test_trigger_job_uses_control_socket_and_exit_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                """
router:
  control:
    socket_path: private/control.sock
""",
                encoding="utf-8",
            )
            args = argparse.Namespace(
                config=config,
                control_socket=None,
                job_id="daily-agenda",
                scheduled_at="2024-05-01T00:00:00Z",
                idempotency_key="stable-fire",
                timeout=1.5,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with (
                patch.object(
                    cli_module,
                    "trigger_job_via_control_socket",
                    AsyncMock(return_value={"status": "busy"}),
                ) as trigger,
                patch("builtins.print") as printed,
            ):
                code = await cli_module._trigger_job(args)

            self.assertEqual(code, 0)
            trigger.assert_awaited_once_with(
                Path("private/control.sock"),
                "daily-agenda",
                scheduled_at=1714521600000,
                idempotency_key="stable-fire",
                timeout=1.5,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )
            self.assertIn('"status": "busy"', printed.call_args.args[0])

            with (
                patch.object(
                    cli_module,
                    "trigger_job_via_control_socket",
                    AsyncMock(return_value={"status": "error", "error": "unknown_job"}),
                ),
                patch("builtins.print"),
            ):
                self.assertEqual(await cli_module._trigger_job(args), 1)

    async def test_notify_route_uses_control_socket_validates_payload_and_exit_mapping(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                """
router:
  control:
    socket_path: private/control.sock
    max_notification_payload_bytes: 64
""",
                encoding="utf-8",
            )
            payload_file = Path(tmp) / "payload.json"
            payload_file.write_text('{"b":2,"a":1}', encoding="utf-8")
            args = argparse.Namespace(
                config=config,
                control_socket=None,
                notification_id="backup-report",
                payload_file=payload_file,
                idempotency_key="backup-fire",
                timeout=1.5,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with (
                patch.object(
                    cli_module,
                    "notify_route_via_control_socket",
                    AsyncMock(return_value={"status": "busy"}),
                ) as notify,
                patch("builtins.print") as printed,
            ):
                code = await cli_module._notify_route(args)

            self.assertEqual(code, 0)
            notify.assert_awaited_once_with(
                Path("private/control.sock"),
                "backup-report",
                payload={"b": 2, "a": 1},
                idempotency_key="backup-fire",
                timeout=1.5,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )
            self.assertIn('"status": "busy"', printed.call_args.args[0])

            override_args = argparse.Namespace(
                config=Path(tmp) / "missing-config.yaml",
                control_socket=Path("override/control.sock"),
                notification_id="backup-report",
                payload_file=payload_file,
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )
            with (
                patch.object(cli_module, "load_router_config", side_effect=AssertionError),
                patch.object(
                    cli_module,
                    "notify_route_via_control_socket",
                    AsyncMock(return_value={"status": "delivered"}),
                ) as notify,
                patch("builtins.print"),
            ):
                code = await cli_module._notify_route(override_args)

            self.assertEqual(code, 0)
            notify.assert_awaited_once_with(
                Path("override/control.sock"),
                "backup-report",
                payload={"b": 2, "a": 1},
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            large_value = "x" * (DEFAULT_MAX_NOTIFICATION_PAYLOAD_BYTES + 1)
            payload_file.write_text(json.dumps({"a": large_value}), encoding="utf-8")
            with (
                patch.object(cli_module, "load_router_config", side_effect=AssertionError),
                patch.object(
                    cli_module,
                    "notify_route_via_control_socket",
                    AsyncMock(return_value={"status": "delivered"}),
                ) as notify,
                patch("builtins.print"),
            ):
                code = await cli_module._notify_route(override_args)

            self.assertEqual(code, 0)
            self.assertEqual(notify.await_args.kwargs["payload"], {"a": large_value})

            payload_file.write_text('{"a":"' + "x" * 57 + '"}', encoding="utf-8")
            with (
                patch.object(cli_module, "notify_route_via_control_socket", AsyncMock()) as notify,
                patch.object(cli_module.logging, "error"),
            ):
                self.assertEqual(await cli_module._notify_route(args), 1)
            notify.assert_not_awaited()

    async def test_notify_route_passes_cli_attachment_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload_file = Path(tmp) / "payload.json"
            payload_file.write_text('{"camera":"front"}', encoding="utf-8")
            attachment = Path(tmp) / "media" / "person.png"
            args = argparse.Namespace(
                config=Path(tmp) / "missing-config.yaml",
                control_socket=Path("override/control.sock"),
                notification_id="camera-person",
                payload_file=payload_file,
                attachment=[attachment],
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with (
                patch.object(cli_module, "load_router_config", side_effect=AssertionError),
                patch.object(
                    cli_module,
                    "notify_route_via_control_socket",
                    AsyncMock(return_value={"status": "delivered"}),
                ) as notify,
                patch("builtins.print"),
            ):
                code = await cli_module._notify_route(args)

        self.assertEqual(code, 0)
        notify.assert_awaited_once_with(
            Path("override/control.sock"),
            "camera-person",
            payload={"camera": "front"},
            attachments=[attachment],
            idempotency_key=None,
            timeout=None,
            client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        )

    async def test_notify_route_rejects_multiple_cli_attachments_locally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload_file = Path(tmp) / "payload.json"
            payload_file.write_text('{"camera":"front"}', encoding="utf-8")
            args = argparse.Namespace(
                config=Path(tmp) / "missing-config.yaml",
                control_socket=Path("override/control.sock"),
                notification_id="camera-person",
                payload_file=payload_file,
                attachment=[Path(tmp) / "a.png", Path(tmp) / "b.png"],
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with (
                patch.object(cli_module, "load_router_config", side_effect=AssertionError),
                patch.object(cli_module, "notify_route_via_control_socket", AsyncMock()) as notify,
                patch.object(cli_module.logging, "error"),
            ):
                code = await cli_module._notify_route(args)

        self.assertEqual(code, 1)
        notify.assert_not_awaited()

    async def test_preflight_permissions_uses_probe_contract_and_exit_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            routes = Path(tmp) / "routes.yaml"
            contract = Path(tmp) / "probe-contract.json"
            config.write_text("router:\n  work_root: private/work\n", encoding="utf-8")
            routes.write_text(
                """
routes:
  - name: example-route
    platform: signal
    group_id: private-group
    profile: example-profile
    state: active
    permissions:
      - tool: read_file
      - tool: web_search
""",
                encoding="utf-8",
            )
            contract.write_text(
                json.dumps({"profiles": {"example-profile": ["read_file", "web_search"]}}),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                config=config,
                routes=routes,
                active_only=True,
                route=["example-route"],
                route_index=[],
                profile=[],
                probe_contract_file=contract,
                json=True,
                control_socket=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with patch("builtins.print") as printed:
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 0)
            report = json.loads(printed.call_args.args[0])
            self.assertEqual(report["status"], "ok")
            self.assertEqual(report["missing_tools"], [])
            self.assertNotIn("private-group", printed.call_args.args[0])

            contract.write_text(
                json.dumps({"profiles": {"example-profile": ["read_file"]}}),
                encoding="utf-8",
            )
            with patch("builtins.print") as printed:
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            report = json.loads(printed.call_args.args[0])
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["missing_tools"][0]["tool"], "web_search")
            self.assertNotIn("private-group", printed.call_args.args[0])

            args.route_index = [-1]
            with (
                patch("builtins.print") as printed,
                patch.object(cli_module.logging, "error") as logged_error,
            ):
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            printed.assert_not_called()
            logged_error.assert_called()

            args.route_index = []
            args.client_timeout = -1
            with (
                patch("builtins.print") as printed,
                patch.object(cli_module.logging, "error") as logged_error,
            ):
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            printed.assert_not_called()
            self.assertIn("--client-timeout", logged_error.call_args.args[1])

            args.client_timeout = cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS
            args.probe_contract_file = Path(tmp) / "missing-contract.json"
            with (
                patch("builtins.print") as printed,
                patch.object(cli_module.logging, "error") as logged_error,
            ):
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            printed.assert_not_called()
            self.assertIn("probe contract file not found", logged_error.call_args.args[1])

            args.probe_contract_file = contract
            contract.write_text("{bad json", encoding="utf-8")
            with (
                patch("builtins.print") as printed,
                patch.object(cli_module.logging, "error") as logged_error,
            ):
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            printed.assert_not_called()
            self.assertIn("invalid JSON", logged_error.call_args.args[1])
            self.assertIn("line 1 column 2", logged_error.call_args.args[1])

            contract.write_text(
                json.dumps({"profiles": {"example-profile": {"tools": ["read_file", 7]}}}),
                encoding="utf-8",
            )
            with (
                patch("builtins.print") as printed,
                patch.object(cli_module.logging, "error") as logged_error,
            ):
                code = await cli_module._preflight_permissions(args)

            self.assertEqual(code, 1)
            printed.assert_not_called()
            self.assertIn("probe contract file is invalid", logged_error.call_args.args[1])

    async def test_preflight_permissions_without_probe_contract_reports_blocked_probe(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            routes = Path(tmp) / "routes.yaml"
            config.write_text("router:\n  work_root: private/work\n", encoding="utf-8")
            routes.write_text(
                """
routes:
  - name: example-route
    platform: signal
    group_id: private-group
    profile: example-profile
    state: active
    permissions:
      - tool: read_file
""",
                encoding="utf-8",
            )
            args = argparse.Namespace(
                config=config,
                routes=routes,
                active_only=True,
                route=[],
                route_index=[],
                profile=[],
                probe_contract_file=None,
                json=True,
                control_socket=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with patch("builtins.print") as printed:
                code = await cli_module._preflight_permissions(args)

        self.assertEqual(code, 1)
        report = json.loads(printed.call_args.args[0])
        self.assertEqual(
            report["probe_errors"],
            [
                {
                    "profile": "example-profile",
                    "code": "probe_contract_required",
                    "error": "probe_contract_required",
                }
            ],
        )
        self.assertEqual(report["issues"][0]["code"], "probe_contract_required")
        self.assertNotIn("private-group", printed.call_args.args[0])

    async def test_preflight_permissions_control_socket_honors_text_output(self) -> None:
        args = argparse.Namespace(
            config=Path("config.yaml"),
            routes=Path("routes.yaml"),
            active_only=True,
            route=[],
            route_index=[],
            profile=[],
            probe_contract_file=Path("ignored-contract.json"),
            json=False,
            control_socket=Path("control.sock"),
            client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        )
        response = {
            "status": "failed",
            "checked_profiles": ["example-profile"],
            "expected_permissions_count": 1,
            "missing_tools_count": 1,
            "missing_tools": [
                {
                    "route_ref": "route:example",
                    "profile": "example-profile",
                    "source_kind": "route",
                    "tool": "web_search",
                }
            ],
            "probe_errors": [
                {
                    "profile": "example-profile",
                    "code": "probe_unsupported",
                    "error": "probe_unsupported",
                }
            ],
            "scope_errors": [
                {
                    "code": "scope_matched_no_routes",
                    "error": "preflight scope did not match any route",
                }
            ],
        }
        with (
            patch.object(
                cli_module,
                "preflight_permissions_via_control_socket",
                AsyncMock(return_value=response),
            ),
            patch.object(cli_module.logging, "warning") as logged_warning,
            patch("builtins.print") as printed,
        ):
            code = await cli_module._preflight_permissions(args)

        self.assertEqual(code, 1)
        output = printed.call_args.args[0]
        self.assertIn("Permission preflight: failed", output)
        self.assertIn("Profiles targeted: 1", output)
        self.assertIn("example-profile: probe_unsupported", output)
        self.assertIn("preflight scope did not match any route", output)
        self.assertIn("route:example example-profile route web_search", output)
        logged_warning.assert_called_once()

    async def test_route_status_uses_control_socket_and_formats_text_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                """
router:
  control:
    socket_path: private/control.sock
""",
                encoding="utf-8",
            )
            args = argparse.Namespace(
                config=config,
                control_socket=None,
                route=["agenda-route"],
                route_index=[],
                profile=["profile"],
                json=False,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )
            response = {
                "status": "ok",
                "route_count": 1,
                "routes": [
                    {
                        "route_ref": "route:agenda-route",
                        "route_state": "active",
                        "profile": "profile",
                        "session": {
                            "policy": "persistent_route",
                            "cached_sessions": 1,
                        },
                        "circuit": {"state": "closed", "failure_count": 0},
                    }
                ],
            }
            with (
                patch.object(
                    cli_module,
                    "route_status_via_control_socket",
                    AsyncMock(return_value=response),
                ) as route_status,
                patch("builtins.print") as printed,
            ):
                code = await cli_module._route_status(args)

        self.assertEqual(code, 0)
        route_status.assert_awaited_once_with(
            Path("private/control.sock"),
            route_names=("agenda-route",),
            route_indexes=(),
            profiles=("profile",),
            client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        )
        output = printed.call_args.args[0]
        self.assertIn("Route status: 1 route(s)", output)
        self.assertIn("route:agenda-route state=active", output)

    async def test_trigger_job_expands_configured_control_socket_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                """
router:
  control:
    socket_path: ~/private/control.sock
""",
                encoding="utf-8",
            )
            args = argparse.Namespace(
                config=config,
                control_socket=None,
                job_id="daily-agenda",
                scheduled_at=None,
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

            with (
                patch.object(
                    cli_module,
                    "trigger_job_via_control_socket",
                    AsyncMock(return_value={"status": "deduped"}),
                ) as trigger,
                patch("builtins.print"),
            ):
                self.assertEqual(await cli_module._trigger_job(args), 0)

            trigger.assert_awaited_once_with(
                Path("~/private/control.sock").expanduser(),
                "daily-agenda",
                scheduled_at=None,
                idempotency_key=None,
                timeout=None,
                client_timeout=cli_module.DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
            )

    async def test_trigger_job_socket_failure_returns_nonzero_without_router_fallback(self) -> None:
        args = argparse.Namespace(
            config=Path("config.yaml"),
            control_socket=Path("/tmp/missing-router.sock"),
            job_id="daily-agenda",
            scheduled_at=None,
            idempotency_key=None,
            timeout=None,
        )

        with (
            patch.object(cli_module, "SignalHermesRouter") as router_class,
            patch.object(cli_module.logging, "error"),
            patch.object(
                cli_module,
                "trigger_job_via_control_socket",
                AsyncMock(side_effect=FileNotFoundError),
            ),
        ):
            code = await cli_module._trigger_job(args)

        self.assertEqual(code, 1)
        router_class.assert_not_called()


class FakeManagedProfile:
    instances: list[FakeManagedProfile] = []

    def __init__(
        self,
        profile: str,
        work_root: Path,
        command: list[str] | None = None,
        max_line_bytes: int | None = None,
        prompt_timeout_seconds: float = 300.0,
        initialize_timeout_seconds: float = 30.0,
    ) -> None:
        self.profile = profile
        self.work_root = work_root
        self.command = command
        self.max_line_bytes = max_line_bytes
        self.prompt_timeout_seconds = prompt_timeout_seconds
        self.initialize_timeout_seconds = initialize_timeout_seconds
        self.started = False
        self.closed = False
        self.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    def exit_suspected(self) -> bool:
        return False


class RegistryProfile:
    def __init__(
        self, *, resume_result: bool = True, resume_exception: Exception | None = None
    ) -> None:
        self.resume_result = resume_result
        self.resume_exception = resume_exception
        self.new_sessions = 0
        self.new_session_cwds: list[Path] = []
        self.resume_calls: list[tuple[str, Path]] = []
        self.policies: list[tuple[str, StaticPermissionPolicy]] = []

    async def new_session(self, cwd: Path) -> str:
        self.new_sessions += 1
        self.new_session_cwds.append(cwd)
        return f"session-{self.new_sessions}"

    async def resume_session(self, session_id: str, cwd: Path) -> bool:
        self.resume_calls.append((session_id, cwd))
        if self.resume_exception is not None:
            raise self.resume_exception
        return self.resume_result

    def set_permission_policy(self, session_id: str, policy: StaticPermissionPolicy) -> None:
        self.policies.append((session_id, policy))


class MutableSupervisor:
    def __init__(self, profile: RegistryProfile) -> None:
        self.profile = profile

    async def get_profile(self, route: Route) -> RegistryProfile:
        return self.profile


class SessionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_supervisor_starts_reuses_restarts_and_closes_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=0,
            )

            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                first = await supervisor.get_profile(route)
                second = await supervisor.get_profile(route)
                await supervisor.restart_profile("missing-profile")
                await supervisor.restart_profile("profile-a")
                replacement = await supervisor.get_profile(route)
                await supervisor.close()

            self.assertIs(first, second)
            self.assertTrue(first.started)
            self.assertEqual(first.command, ["hermes", "-p", "profile-a", "acp"])
            self.assertEqual(first.max_line_bytes, 8 * 1024 * 1024)
            self.assertEqual(first.initialize_timeout_seconds, 30.0)
            self.assertTrue(first.closed)
            self.assertIsNot(replacement, first)
            self.assertTrue(replacement.closed)

    async def test_profile_supervisor_threads_initialize_timeout_into_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                initialize_timeout_seconds=7.5,
                restart_cooldown_seconds=0,
            )

            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                profile = await supervisor.get_profile(route)
                await supervisor.close()

            self.assertEqual(profile.initialize_timeout_seconds, 7.5)

    async def test_profile_supervisor_restart_does_not_block_immediate_replacement(
        self,
    ) -> None:
        # Regression test for the W3 prompt-failure recovery path: after
        # restart_profile() closes a live subprocess, the next get_profile()
        # in the same handler must succeed — it is part of the normal
        # replace_after_restart flow, NOT a thundering-herd scenario. The
        # cooldown is only stamped on FAILED start, not on operator-initiated
        # close.
        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                first = await supervisor.get_profile(route)
                await supervisor.restart_profile("profile-a")
                # Must not raise — close-then-replace is normal recovery.
                replacement = await supervisor.get_profile(route)
                self.assertIsNot(replacement, first)
                self.assertTrue(replacement.started)
                await supervisor.close()

    async def test_profile_supervisor_records_cooldown_after_failed_start(self) -> None:
        class FailingProfile(FakeManagedProfile):
            async def start(self) -> None:
                raise RuntimeError("synthetic start failure")

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", FailingProfile):
                with self.assertRaises(RuntimeError):
                    await supervisor.get_profile(route)
                # Second call hits cooldown, NOT a fresh subprocess spawn.
                with self.assertRaisesRegex(RuntimeError, "cooldown"):
                    await supervisor.get_profile(route)
                # Only one FailingProfile was instantiated.
                self.assertEqual(
                    len([i for i in FakeManagedProfile.instances if i.profile == "profile-a"]),
                    1,
                )
                await supervisor.close()

    async def test_persistent_sender_sessions_reuse_per_sender_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = RegistryProfile()
            registry = SessionRegistry(
                Path(tmp),
                supervisor=MutableSupervisor(profile),  # type: ignore[arg-type]
            )
            route = make_route(session_policy=SessionPolicy.PERSISTENT_SENDER)

            first = await registry.get(route, make_event(sender_id="sender-a", timestamp=1))
            first_again = await registry.get(route, make_event(sender_id="sender-a", timestamp=2))
            second = await registry.get(route, make_event(sender_id="sender-b", timestamp=3))

            self.assertIs(first, first_again)
            self.assertIsNot(first, second)
            self.assertEqual(profile.new_sessions, 2)
            self.assertEqual(
                [policy[0] for policy in profile.policies], ["session-1"] * 2 + ["session-2"]
            )

    async def test_registry_accepts_explicit_session_key_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = RegistryProfile()
            registry = SessionRegistry(
                Path(tmp),
                supervisor=MutableSupervisor(profile),  # type: ignore[arg-type]
            )
            route = make_route(session_policy=SessionPolicy.PERSISTENT_SENDER)

            first = await registry.get(route, SessionKeyInput("scheduled:daily", 1))
            first_again = await registry.get(route, SessionKeyInput("scheduled:daily", 2))
            second = await registry.get(route, SessionKeyInput("scheduled:weekly", 2))

            self.assertIs(first, first_again)
            self.assertIsNot(first, second)
            self.assertEqual(profile.new_sessions, 2)

    async def test_stale_cached_session_is_resumed_on_restarted_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first_profile = RegistryProfile()
            supervisor = MutableSupervisor(first_profile)
            registry = SessionRegistry(Path(tmp), supervisor=supervisor)  # type: ignore[arg-type]
            route = make_route()
            event = make_event()

            first = await registry.get(route, event)
            replacement_profile = RegistryProfile(resume_result=True)
            supervisor.profile = replacement_profile
            replacement = await registry.get(route, event)

            self.assertIs(replacement.profile, replacement_profile)
            self.assertEqual(replacement.session_id, first.session_id)
            self.assertEqual(replacement_profile.resume_calls, [(first.session_id, first.cwd)])

    async def test_resume_exception_surfaces_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first_profile = RegistryProfile()
            supervisor = MutableSupervisor(first_profile)
            registry = SessionRegistry(Path(tmp), supervisor=supervisor)  # type: ignore[arg-type]
            route = make_route()
            event = make_event()

            first = await registry.get(route, event)
            replacement_profile = RegistryProfile(resume_exception=RuntimeError("resume failed"))
            supervisor.profile = replacement_profile

            with self.assertRaisesRegex(RuntimeError, "resume failed"):
                await registry.get(route, event)

            self.assertEqual(replacement_profile.resume_calls, [(first.session_id, first.cwd)])
            self.assertEqual(replacement_profile.new_sessions, 0)

    async def test_resume_exception_recreates_when_route_opts_in(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first_profile = RegistryProfile()
            supervisor = MutableSupervisor(first_profile)
            registry = SessionRegistry(Path(tmp), supervisor=supervisor)  # type: ignore[arg-type]
            route = make_route(recreate_session_on_resume_failure=True)
            event = make_event()

            first = await registry.get(route, event)
            replacement_profile = RegistryProfile(resume_exception=RuntimeError("resume failed"))
            supervisor.profile = replacement_profile
            replacement = await registry.get(route, event)

            self.assertIs(replacement.profile, replacement_profile)
            self.assertEqual(replacement_profile.resume_calls, [(first.session_id, first.cwd)])
            self.assertEqual(replacement_profile.new_sessions, 1)
            self.assertEqual(replacement.session_id, "session-1")

    async def test_resume_cancellation_is_not_recreated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first_profile = RegistryProfile()
            supervisor = MutableSupervisor(first_profile)
            registry = SessionRegistry(Path(tmp), supervisor=supervisor)  # type: ignore[arg-type]
            route = make_route(recreate_session_on_resume_failure=True)
            event = make_event()

            first = await registry.get(route, event)
            replacement_profile = RegistryProfile(resume_exception=asyncio.CancelledError())
            supervisor.profile = replacement_profile

            with self.assertRaises(asyncio.CancelledError):
                await registry.get(route, event)

            self.assertEqual(replacement_profile.resume_calls, [(first.session_id, first.cwd)])
            self.assertEqual(replacement_profile.new_sessions, 0)

    async def test_ephemeral_replacement_after_restart_is_not_cached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = RegistryProfile(resume_result=False)
            registry = SessionRegistry(
                Path(tmp),
                supervisor=MutableSupervisor(profile),  # type: ignore[arg-type]
            )
            route = make_route(profile="private-profile", session_policy=SessionPolicy.EPHEMERAL)
            event = make_event()
            previous = RoutedSession(
                profile=RegistryProfile(),
                session_id="old-session",
                cwd=Path(tmp) / "old-cwd",
                ephemeral=True,
            )

            with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                replacement = await registry.replace_after_restart(route, event, previous)

            self.assertEqual(replacement.session_id, "session-1")
            self.assertNotIn(registry._session_key(route, event), registry._sessions)
            self.assertNotIn("private-profile", "\n".join(logs.output))


_FAKE_ACP_AGENT = Path(__file__).parent / "fixtures" / "fake_acp_agent.py"


class ProfileExitWatcherSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_supervisor_logs_unexpected_exit_and_respawns_on_next_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(_FAKE_ACP_AGENT)],
            )
            try:
                first = await supervisor.get_profile(route)
                assert first.peer is not None and first.peer.process is not None
                process = first.peer.process
                with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                    process.kill()
                    # Acceptance bound: the exit is noticed, logged, and the
                    # profile marked dead within a second of the death.
                    for _ in range(100):
                        if "profile-a" not in supervisor._profiles:
                            break
                        await asyncio.sleep(0.01)
                self.assertNotIn("profile-a", supervisor._profiles)
                output = "\n".join(logs.output)
                self.assertIn("profile-a", output)
                self.assertIn("exited unexpectedly with returncode -9", output)
                self.assertIn("will respawn on next acquisition", output)
                # No cooldown was stamped: the next acquisition transparently
                # spawns a fresh child.
                replacement = await supervisor.get_profile(route)
                self.assertIsNot(replacement, first)
                assert replacement.peer is not None and replacement.peer.process is not None
                self.assertNotEqual(replacement.peer.process.pid, process.pid)
            finally:
                await supervisor.close()

    async def test_supervisor_restart_and_close_do_not_log_unexpected_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(_FAKE_ACP_AGENT)],
            )
            with self.assertNoLogs("signal_hermes_router.sessions", level="ERROR"):
                await supervisor.get_profile(route)
                await supervisor.restart_profile("profile-a")
                await supervisor.get_profile(route)
                await supervisor.close()
                # Let any stray watcher callback land before the check ends.
                await asyncio.sleep(0.05)

    async def test_supervisor_logs_exit_when_recovery_closes_first(self) -> None:
        # Lazy-discovery incident shape: the child dies mid-idle, the failure
        # is only observed when the next request fails, and recovery calls
        # restart_profile() immediately -- the exit must still be logged.
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(_FAKE_ACP_AGENT)],
            )
            try:
                profile = await supervisor.get_profile(route)
                assert profile.peer is not None and profile.peer.process is not None
                profile.peer.process.kill()
                with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                    with self.assertRaises(Exception):
                        await profile.peer.request("session/new", timeout=5)
                    await supervisor.restart_profile("profile-a")
                output = "\n".join(logs.output)
                self.assertIn("exited unexpectedly with returncode -9", output)
                replacement = await supervisor.get_profile(route)
                self.assertIsNot(replacement, profile)
            finally:
                await supervisor.close()

    async def test_supervisor_redacts_stderr_tail_and_profile_name_in_exit_log(self) -> None:
        # A child that answers initialize, lingers briefly, then writes
        # credential-bearing stderr and crashes. Both the profile name (via
        # the redaction hook) and the stderr credentials (via the sanitizer)
        # must be masked in the ERROR logs.
        agent_code = (
            "import json, sys, time\n"
            "message = json.loads(sys.stdin.readline())\n"
            "sys.stdout.write(json.dumps({'jsonrpc': '2.0', 'id': message['id'],"
            " 'result': {'agentCapabilities': {}}}) + '\\n')\n"
            "sys.stdout.flush()\n"
            "time.sleep(0.2)\n"
            "sys.stderr.write('private-marker Authorization: Bearer super-secret-token-1234\\n')\n"
            "sys.stderr.flush()\n"
            "sys.exit(5)\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            # The command template is str.format()-ed per part; a script file
            # avoids brace escaping in the inline agent code.
            agent_script = Path(tmp) / "crashing_agent.py"
            agent_script.write_text(agent_code, encoding="utf-8")
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(agent_script)],
            )
            supervisor.set_redactor(
                lambda text: text.replace("private-marker", "[marker]").replace(
                    "profile-a", "[profile]"
                )
            )
            try:
                with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                    await supervisor.get_profile(route)
                    for _ in range(200):
                        if "profile-a" not in supervisor._profiles:
                            break
                        await asyncio.sleep(0.01)
                output = "\n".join(logs.output)
                self.assertIn("returncode 5", output)
                self.assertIn("[profile]", output)
                self.assertNotIn("profile-a", output)
                self.assertIn("[marker]", output)
                self.assertNotIn("private-marker", output)
                self.assertNotIn("super-secret-token-1234", output)
            finally:
                await supervisor.close()

    async def test_supervisor_ignores_exit_of_replaced_profile_instance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            supervisor = ProfileSupervisor(Path(tmp))
            stale = object()
            replacement = object()
            supervisor._profiles["profile-a"] = replacement  # type: ignore[assignment]

            with self.assertLogs("signal_hermes_router.sessions", level="ERROR"):
                supervisor._handle_profile_exit("profile-a", stale, 1, ())  # type: ignore[arg-type]

            # A late callback for an already-replaced child still logs the
            # exit but must not evict the replacement.
            self.assertIs(supervisor._profiles["profile-a"], replacement)

    async def test_supervisor_does_not_cache_profile_that_exits_during_start(self) -> None:
        class DiesDuringStartProfile(FakeManagedProfile):
            async def start(self) -> None:
                self.started = True
                # Simulate the exit watcher firing while start() is in flight
                # (child answered initialize, then died immediately).
                assert self.on_exit is not None
                self.on_exit(-9, ())

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", DiesDuringStartProfile):
                with self.assertLogs("signal_hermes_router.sessions", level="ERROR"):
                    with self.assertRaisesRegex(RuntimeError, "exited during startup"):
                        await supervisor.get_profile(route)
            self.assertEqual(supervisor._profiles, {})
            # The exit did not stamp the failed-start cooldown: the next
            # acquisition spawns fresh instead of failing fast.
            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                replacement = await supervisor.get_profile(route)
            self.assertTrue(replacement.started)

    async def test_supervisor_evicts_cached_profile_with_exit_evidence(self) -> None:
        # A turn can arrive after the child died but before the exit watcher
        # finishes its settle window and evicts the cache entry. The cached
        # branch must consult the synchronous exit evidence and spawn a fresh
        # child instead of handing back the dead instance.
        class TogglableProfile(FakeManagedProfile):
            suspected = False

            def exit_suspected(self) -> bool:
                return self.suspected

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", TogglableProfile):
                first = await supervisor.get_profile(route)
                self.assertIs(await supervisor.get_profile(route), first)
                first.suspected = True
                replacement = await supervisor.get_profile(route)
            self.assertIsNot(replacement, first)
            self.assertTrue(replacement.started)
            self.assertIs(supervisor._profiles["profile-a"], replacement)
            # Eviction must close the old profile: broken-pipe evidence can
            # come from a child that has not fully exited.
            self.assertTrue(first.closed)

    async def test_supervisor_rejects_profile_with_exit_evidence_after_start(self) -> None:
        # A child can answer initialize and die before the exit watcher gets
        # CPU: the identity check alone would pass, so get_profile must also
        # consult the peer's synchronous exit evidence and refuse to hand out
        # (or cache) the dead instance.
        class ExitsAfterStartProfile(FakeManagedProfile):
            def exit_suspected(self) -> bool:
                return True

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", ExitsAfterStartProfile):
                with self.assertRaisesRegex(RuntimeError, "exited during startup"):
                    await supervisor.get_profile(route)
            self.assertEqual(supervisor._profiles, {})
            self.assertEqual(supervisor._last_restart, {})
            self.assertTrue(
                FakeManagedProfile.instances[-1].closed,
                "the rejected instance must be closed, not orphaned",
            )
            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                replacement = await supervisor.get_profile(route)
            self.assertTrue(replacement.started)

    async def test_supervisor_eviction_survives_failing_close(self) -> None:
        # A failing close on the evicted dead profile is contained: the
        # acquisition still spawns the replacement.
        class FailingCloseProfile(FakeManagedProfile):
            suspected = False

            def exit_suspected(self) -> bool:
                return self.suspected

            async def close(self) -> None:
                self.closed = True
                raise RuntimeError("close boom")

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=0,
            )
            with patch.object(sessions_module, "ACPProfile", FailingCloseProfile):
                first = await supervisor.get_profile(route)
                first.suspected = True
                with self.assertLogs("signal_hermes_router.sessions", level="WARNING") as logs:
                    replacement = await supervisor.get_profile(route)
            self.assertIsNot(replacement, first)
            self.assertTrue(first.closed)
            self.assertIn("evicted Hermes profile close failed", "\n".join(logs.output))

    async def test_supervisor_serializes_concurrent_acquisitions(self) -> None:
        # The provisional cache entry exists while start() is still awaiting;
        # a concurrent get_profile for the same profile must wait for startup
        # rather than returning the half-started instance.
        class SlowStartProfile(FakeManagedProfile):
            async def start(self) -> None:
                await asyncio.sleep(0.05)
                self.started = True

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=0,
            )

            async def acquire() -> tuple[FakeManagedProfile, bool]:
                profile = await supervisor.get_profile(route)
                return profile, profile.started

            with patch.object(sessions_module, "ACPProfile", SlowStartProfile):
                (first, first_started), (second, second_started) = await asyncio.gather(
                    acquire(), acquire()
                )
            self.assertIs(first, second)
            self.assertTrue(first_started)
            self.assertTrue(second_started, "no caller may observe a half-started profile")
            self.assertEqual(len(FakeManagedProfile.instances), 1)

    async def test_supervisor_cancellation_during_start_leaves_no_cached_profile(self) -> None:
        class CancelledStartProfile(FakeManagedProfile):
            async def start(self) -> None:
                raise asyncio.CancelledError

        with tempfile.TemporaryDirectory() as tmp:
            FakeManagedProfile.instances.clear()
            route = make_route(profile="profile-a")
            supervisor = ProfileSupervisor(
                Path(tmp),
                command_template=["hermes", "-p", "{profile}", "acp"],
                restart_cooldown_seconds=60,
            )
            with patch.object(sessions_module, "ACPProfile", CancelledStartProfile):
                with self.assertRaises(asyncio.CancelledError):
                    await supervisor.get_profile(route)
            self.assertEqual(supervisor._profiles, {})
            self.assertEqual(supervisor._last_restart, {})
            with patch.object(sessions_module, "ACPProfile", FakeManagedProfile):
                replacement = await supervisor.get_profile(route)
            self.assertTrue(replacement.started)


class _FakeServeRouter:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.started = asyncio.Event()
        self.close_started = asyncio.Event()
        self.close_release: asyncio.Event | None = None
        self.close_result: tuple = ()
        self.begin_shutdown_calls = 0
        self.closed = False

    def begin_shutdown(self) -> None:
        self.begin_shutdown_calls += 1

    async def run_forever(self) -> None:
        self.started.set()
        await asyncio.Event().wait()

    async def close(self) -> tuple:
        self.close_started.set()
        if self.close_release is not None:
            await self.close_release.wait()
        self.closed = True
        return self.close_result


def _serve_app(tmp: str) -> AppConfig:
    return AppConfig(
        router=RouterConfig(
            signal_base_url="http://signal.test",
            work_root=Path(tmp) / "work",
        ),
        routes=(make_route(),),
    )


class ServeShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def _start_run(self, tmp: str) -> tuple[asyncio.Task, list[_FakeServeRouter]]:
        instances: list[_FakeServeRouter] = []

        def make_router(config: AppConfig) -> _FakeServeRouter:
            router = _FakeServeRouter(config)
            instances.append(router)
            return router

        patcher_config = patch.object(cli_module, "load_app_config", return_value=_serve_app(tmp))
        patcher_router = patch.object(cli_module, "SignalHermesRouter", make_router)
        patcher_config.start()
        patcher_router.start()
        self.addCleanup(patcher_config.stop)
        self.addCleanup(patcher_router.stop)
        task = asyncio.create_task(cli_module._run(Path("config.yaml"), Path("routes.yaml")))
        for _ in range(100):
            if instances:
                break
            await asyncio.sleep(0)
        return task, instances

    async def test_sigterm_triggers_graceful_shutdown_and_clean_return(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            task, instances = await self._start_run(tmp)
            await asyncio.wait_for(instances[0].started.wait(), timeout=2)

            signal.raise_signal(signal.SIGTERM)

            await asyncio.wait_for(task, timeout=5)
            router = instances[0]
            self.assertEqual(router.begin_shutdown_calls, 1)
            self.assertTrue(router.closed)
            # The handler was deregistered after close() completed.
            loop = asyncio.get_running_loop()
            self.assertFalse(loop.remove_signal_handler(signal.SIGTERM))

    async def test_non_sigterm_cancellation_reraises_after_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            task, instances = await self._start_run(tmp)
            await asyncio.wait_for(instances[0].started.wait(), timeout=2)

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            router = instances[0]
            self.assertTrue(router.closed)
            self.assertEqual(router.begin_shutdown_calls, 0)
            loop = asyncio.get_running_loop()
            self.assertFalse(loop.remove_signal_handler(signal.SIGTERM))

    async def test_second_sigterm_forces_immediate_exit_during_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(cli_module, "_force_immediate_exit") as force_exit:
                task, instances = await self._start_run(tmp)
                router_holder: list[_FakeServeRouter] = instances
                await asyncio.wait_for(instances[0].started.wait(), timeout=2)
                router = router_holder[0]
                router.close_release = asyncio.Event()

                signal.raise_signal(signal.SIGTERM)
                await asyncio.wait_for(router.close_started.wait(), timeout=2)

                signal.raise_signal(signal.SIGTERM)
                for _ in range(100):
                    if force_exit.called:
                        break
                    await asyncio.sleep(0.01)
                force_exit.assert_called_once()

                router.close_release.set()
                await asyncio.wait_for(task, timeout=5)

    async def test_hard_exit_when_shutdown_cleanup_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pending = asyncio.create_task(asyncio.Event().wait())
            self.addCleanup(pending.cancel)
            with patch.object(cli_module, "_hard_exit_after_incomplete_shutdown") as hard_exit:
                task, instances = await self._start_run(tmp)
                await asyncio.wait_for(instances[0].started.wait(), timeout=2)
                instances[0].close_result = (pending,)

                signal.raise_signal(signal.SIGTERM)
                await asyncio.wait_for(task, timeout=5)
                hard_exit.assert_called_once()


class ServeSubprocessTests(unittest.TestCase):
    def test_serve_subprocess_sigterm_exits_zero_within_bound(self) -> None:
        src_root = Path(__file__).resolve().parent.parent / "src"
        with tempfile.TemporaryDirectory() as tmp:
            work_root = Path(tmp) / "work"
            socket_path = work_root / "control.sock"
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "router:",
                        "  signal:",
                        # Unreachable local port: the SSE client keeps
                        # reconnecting, so serve stays up without a daemon.
                        '    base_url: "http://127.0.0.1:9"',
                        f'  state_db: "{tmp}/state.db"',
                        f'  media_root: "{tmp}/media"',
                        f'  signal_attachment_root: "{tmp}/attachments"',
                        f'  work_root: "{work_root}"',
                        "  control:",
                        "    enabled: true",
                        f'    socket_path: "{socket_path}"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            routes_path.write_text(
                "\n".join(
                    [
                        "routes:",
                        '  - platform: "signal"',
                        '    name: "r1"',
                        '    group_id: "group"',
                        '    profile: "profile"',
                        '    session_policy: "persistent_route"',
                        '    state: "shadow"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            env = dict(os.environ)
            env["PYTHONPATH"] = str(src_root)
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "signal_hermes_router.cli",
                    "--config",
                    str(config_path),
                    "--routes",
                    str(routes_path),
                    "serve",
                ],
                env=env,
                cwd=tmp,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                ready = False
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        break
                    if socket_path.exists():
                        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        try:
                            probe.settimeout(0.2)
                            probe.connect(str(socket_path))
                            ready = True
                            break
                        except OSError:
                            pass
                        finally:
                            probe.close()
                    time.sleep(0.05)
                if not ready:
                    output = proc.stdout.read() if proc.stdout else ""
                    self.fail(f"serve did not become ready: rc={proc.poll()} output={output!r}")

                proc.send_signal(signal.SIGTERM)
                # The documented service-level bound is ~30s worst case; an
                # idle shutdown must come in far below it.
                returncode = proc.wait(timeout=40)
                self.assertEqual(returncode, 0)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
                if proc.stdout is not None:
                    proc.stdout.close()


if __name__ == "__main__":
    unittest.main()
