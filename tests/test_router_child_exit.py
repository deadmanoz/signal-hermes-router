from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    RouteState,
)
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.sessions import ProfileSupervisor
from signal_hermes_router.redaction import stable_ref
from tests.support import (
    FakeSignal,
    make_app,
    make_event,
    make_route,
    RouterTestCase,
)


class RouterChildExitTests(RouterTestCase):
    async def _wait_for_eviction(self, supervisor: ProfileSupervisor, profile: str) -> None:
        for _ in range(100):
            if profile not in supervisor._profiles:
                return
            await asyncio.sleep(0.01)

    _FIXTURE = Path(__file__).parent / "fixtures" / "fake_acp_agent.py"

    async def _wait_for_eviction(self, supervisor: ProfileSupervisor, profile: str) -> None:
        for _ in range(100):
            if profile not in supervisor._profiles:
                return
            await asyncio.sleep(0.01)

    async def test_unexpected_child_exit_mid_idle_recovers_transparently_on_next_turn(
        self,
    ) -> None:
        # Acceptance for the three silent-death incidents: killing the child
        # mid-idle produces an exit log with returncode within a second, and
        # the next turn transparently uses a fresh child without an ERROR
        # turn failure. (Resume-capable profile contract.)
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(self._FIXTURE)],
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,
                dedupe=DedupeStore(),
            )
            try:
                first_result = await router.handle_event(make_event(timestamp=1))
                assert first_result is not None
                first_profile = supervisor._profiles["profile"]
                assert first_profile.peer is not None and first_profile.peer.process is not None
                process = first_profile.peer.process

                with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                    process.kill()
                    await self._wait_for_eviction(supervisor, "profile")
                self.assertNotIn("profile", supervisor._profiles)
                self.assertIn("returncode -9", "\n".join(logs.output))

                second_result = await router.handle_event(make_event(timestamp=2))
                self.assertIsNotNone(second_result, "next turn must be delivered, not an error")
                self.assertEqual(signal.sends[-1][1], "denied")
                self.assertNotIn("signal:group", router.route_state_overrides)
                replacement = supervisor._profiles["profile"]
                assert replacement.peer is not None and replacement.peer.process is not None
                self.assertNotEqual(replacement.peer.process.pid, process.pid)
            finally:
                await router.close()

    async def test_unexpected_child_exit_recovers_with_recreate_on_resume_failure(self) -> None:
        # State-losing profile contract: a respawned child rejects the stale
        # session id, and the route's existing opt-in recreates the session.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[
                    sys.executable,
                    str(self._FIXTURE),
                    "--reject-unknown-resume",
                ],
            )
            route = make_route(recreate_session_on_resume_failure=True)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,
                dedupe=DedupeStore(),
            )
            try:
                first_result = await router.handle_event(make_event(timestamp=1))
                assert first_result is not None
                first_profile = supervisor._profiles["profile"]
                assert first_profile.peer is not None and first_profile.peer.process is not None
                first_profile.peer.process.kill()
                await self._wait_for_eviction(supervisor, "profile")

                with self.assertLogs("signal_hermes_router.sessions", level="WARNING") as logs:
                    second_result = await router.handle_event(make_event(timestamp=2))
                self.assertIsNotNone(second_result)
                self.assertEqual(signal.sends[-1][1], "denied")
                self.assertIn("creating a fresh session", "\n".join(logs.output))
                self.assertNotIn("signal:group", router.route_state_overrides)
            finally:
                await router.close()

    async def test_router_wires_redactor_into_injected_real_supervisor(self) -> None:
        # The router must wire its redactor into every router-owned real
        # supervisor, including an injected one carrying a command template,
        # so exit logs never disclose the raw configured profile name.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(self._FIXTURE)],
            )
            route = make_route(profile="private-profile")
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,
                dedupe=DedupeStore(),
            )
            try:
                first_result = await router.handle_event(make_event(timestamp=1))
                assert first_result is not None
                profile = supervisor._profiles["private-profile"]
                assert profile.peer is not None and profile.peer.process is not None

                with self.assertLogs("signal_hermes_router.sessions", level="ERROR") as logs:
                    profile.peer.process.kill()
                    await self._wait_for_eviction(supervisor, "private-profile")
                output = "\n".join(logs.output)
                self.assertNotIn("private-profile", output)
                self.assertIn(stable_ref("id", "private-profile"), output)
            finally:
                await router.close()
