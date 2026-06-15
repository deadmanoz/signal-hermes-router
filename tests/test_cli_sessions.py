from __future__ import annotations

import asyncio
import argparse
import logging
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import AsyncMock

from signal_hermes_router import cli as cli_module
from signal_hermes_router import sessions as sessions_module
from signal_hermes_router.config import AppConfig, Route, RouterConfig
from signal_hermes_router.models import RouteState, SessionKeyInput, SessionPolicy
from signal_hermes_router.permissions import StaticPermissionPolicy
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

    async def test_main_async_dispatches_trigger_job_and_rejects_unknown_command(self) -> None:
        args = argparse.Namespace(command="trigger-job")
        with patch.object(cli_module, "_trigger_job", AsyncMock(return_value=0)) as trigger:
            self.assertEqual(await cli_module._main_async(args), 0)
        trigger.assert_awaited_once_with(args)

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
    ) -> None:
        self.profile = profile
        self.work_root = work_root
        self.command = command
        self.max_line_bytes = max_line_bytes
        self.prompt_timeout_seconds = prompt_timeout_seconds
        self.started = False
        self.closed = False
        self.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True


class RegistryProfile:
    def __init__(self, *, resume_result: bool = True) -> None:
        self.resume_result = resume_result
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
            self.assertTrue(first.closed)
            self.assertIsNot(replacement, first)
            self.assertTrue(replacement.closed)

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


if __name__ == "__main__":
    unittest.main()
