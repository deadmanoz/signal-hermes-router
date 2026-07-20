from __future__ import annotations

import json
import tempfile
from pathlib import Path

from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterConfig,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    NormalizedEvent,
    RouteState,
    SessionPolicy,
)
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.redaction import stable_ref
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_event,
    make_route,
    make_router_harness,
    RouterTestCase,
)


class RouterSessionTests(RouterTestCase):
    async def test_persistent_route_sessions_are_isolated_by_signal_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                ),
                routes=(
                    Route(
                        platform="signal",
                        group_id="group-one",
                        profile="profile",
                        session_policy=SessionPolicy.PERSISTENT_ROUTE,
                        state=RouteState.ACTIVE,
                        route_context={"purpose": "synthetic", "route_alias": "one"},
                    ),
                    Route(
                        platform="signal",
                        group_id="group-two",
                        profile="profile",
                        session_policy=SessionPolicy.PERSISTENT_ROUTE,
                        state=RouteState.ACTIVE,
                        route_context={"purpose": "synthetic", "route_alias": "two"},
                    ),
                ),
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(
                NormalizedEvent(
                    platform="signal",
                    group_id="group-one",
                    sender_id="sender",
                    source_uuid="sender-one",
                    timestamp=1,
                    text="hello one",
                )
            )
            await router.handle_event(
                NormalizedEvent(
                    platform="signal",
                    group_id="group-two",
                    sender_id="sender",
                    source_uuid="sender-two",
                    timestamp=2,
                    text="hello two",
                )
            )
            self.assertEqual(profile.new_sessions, 2)
            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-2"])
            self.assertEqual(len({cwd.name for cwd in profile.new_session_cwds}), 2)

    async def test_ephemeral_sessions_are_released_after_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = Route(
                platform="signal",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.EPHEMERAL,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(route,),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event())
            await router.handle_event(
                NormalizedEvent(
                    platform="signal",
                    group_id="group",
                    sender_id="sender",
                    source_uuid="sender",
                    timestamp=2,
                    text="hello again",
                )
            )

            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-2"])
            self.assertEqual(profile.released_session_ids, ["session-1", "session-2"])

    async def test_session_max_turns_rotates_persistent_session_without_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            supervisor = FakeSupervisor(profile)
            route = make_route(state=RouteState.ACTIVE, session_max_turns=2)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            await router.handle_event(make_event(timestamp=2))
            with self.assertLogs("signal_hermes_router.sessions", level="INFO") as logs:
                await router.handle_event(make_event(timestamp=3))

            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1", "session-2"])
            self.assertEqual(profile.new_sessions, 2)
            # Rotation is pure session lifecycle: no subprocess restart, no resume.
            self.assertEqual(supervisor.restarts, 0)
            self.assertEqual(profile.resumes, 0)
            # The rotated-out session's per-session profile state is dropped.
            self.assertEqual(profile.released_session_ids, ["session-1"])
            joined = "\n".join(logs.output)
            self.assertIn("max_turns", joined)
            self.assertIn(stable_ref("route", "signal:group"), joined)
            self.assertNotIn("signal:group", joined)

    async def test_session_rotation_skips_release_on_replaced_profile_instance(self) -> None:
        class SwappingSupervisor(FakeSupervisor):
            """Returns a different profile instance on each acquisition,
            mirroring a subprocess replacement between turns."""

            def __init__(self, profiles: list[FakeProfile]) -> None:
                super().__init__(profiles[0])
                self.profiles = profiles
                self.calls = 0

            async def get_profile(self, route: Route) -> FakeProfile:
                profile = self.profiles[min(self.calls, len(self.profiles) - 1)]
                self.calls += 1
                return profile

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            first = FakeProfile()
            second = FakeProfile()
            supervisor = SwappingSupervisor([first, second])
            route = make_route(state=RouteState.ACTIVE, session_max_turns=1)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            await router.handle_event(make_event(timestamp=2))

            # The expired session belonged to the replaced profile instance:
            # rotation must not resume it and must not release it on the new
            # instance, only start a fresh session there.
            self.assertEqual(first.prompt_session_ids, ["session-1"])
            self.assertEqual(second.prompt_session_ids, ["session-1"])
            self.assertEqual(first.released_session_ids, [])
            self.assertEqual(second.released_session_ids, [])
            self.assertEqual(first.resumes + second.resumes, 0)
            self.assertEqual(second.new_sessions, 1)

    async def test_session_max_age_rotates_persistent_session_without_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            supervisor = FakeSupervisor(profile)
            route = make_route(state=RouteState.ACTIVE, session_max_age_seconds=3600.0)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            now = [0.0]
            router.sessions._clock = lambda: now[0]

            await router.handle_event(make_event(timestamp=1))
            now[0] = 1800.0
            await router.handle_event(make_event(timestamp=2))
            now[0] = 3600.0
            with self.assertLogs("signal_hermes_router.sessions", level="INFO") as logs:
                await router.handle_event(make_event(timestamp=3))

            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1", "session-2"])
            self.assertEqual(profile.new_sessions, 2)
            self.assertEqual(supervisor.restarts, 0)
            self.assertEqual(profile.resumes, 0)
            self.assertIn("max_age", "\n".join(logs.output))

    async def test_session_rotation_unset_reuses_one_session_indefinitely(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router

            for timestamp in range(1, 6):
                await router.handle_event(make_event(timestamp=timestamp))

            self.assertEqual(profile.new_sessions, 1)
            self.assertEqual(profile.prompt_session_ids, ["session-1"] * 5)
            self.assertEqual(profile.released_session_ids, [])

    async def test_session_rotation_budget_carries_over_resumed_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            supervisor = FakeSupervisor(profile)
            route = make_route(state=RouteState.ACTIVE, session_max_turns=2)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            # Second turn fails mid-prompt; recovery resumes the same session,
            # whose accumulated context (and rotation budget) carries over.
            profile.fail = True
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(make_event(timestamp=2))
            profile.fail = False
            await router.handle_event(make_event(timestamp=3))

            self.assertEqual(profile.resumes, 1)
            self.assertEqual(supervisor.restarts, 1)
            # Turn 3 rotates: the resumed session already served its 2 turns.
            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1", "session-2"])
            self.assertEqual(profile.new_sessions, 2)

    async def test_session_rotation_budget_resets_on_recreated_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.resume_available = False
            supervisor = FakeSupervisor(profile)
            route = make_route(state=RouteState.ACTIVE, session_max_turns=2)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            # Second turn fails; resume is unsupported, so recovery creates a
            # fresh session whose rotation budget starts over.
            profile.fail = True
            with (
                self.assertLogs("signal_hermes_router.router", level="ERROR"),
                self.assertLogs("signal_hermes_router.sessions", level="ERROR"),
            ):
                await router.handle_event(make_event(timestamp=2))
            profile.fail = False
            await router.handle_event(make_event(timestamp=3))
            await router.handle_event(make_event(timestamp=4))
            await router.handle_event(make_event(timestamp=5))

            # session-2 (recreated) serves two full turns before rotating.
            self.assertEqual(
                profile.prompt_session_ids,
                ["session-1", "session-1", "session-2", "session-2", "session-3"],
            )
            self.assertEqual(profile.new_sessions, 3)

    async def test_session_acquisition_failure_is_classified_and_trips_circuit(self) -> None:
        class FailingSupervisor:
            def __init__(self) -> None:
                self.restarts = 0

            async def get_profile(self, route: Route) -> FakeProfile:
                raise RuntimeError("session setup failed at http://127.0.0.1:8000/private")

            async def restart_profile(self, profile_name: str) -> None:
                self.restarts += 1

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            dedupe = DedupeStore()
            supervisor = FailingSupervisor()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, failures=1),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=dedupe,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                result = await router.handle_event(make_event())

            self.assertIsNone(result)
            self.assertEqual(supervisor.restarts, 1)
            self.assertEqual(router.route_state_overrides["signal:group"], RouteState.MAINTENANCE)
            self.assertEqual(
                signal.sends, [("group", "This route is temporarily under maintenance.")]
            )
            self.assertFalse(dedupe.claim("signal:group", "sender", 1))
            status = router._route_status_response({})
            route_status = status["routes"][0]
            self.assertEqual(route_status["last_failure"]["code"], "acp_session_failed")
            self.assertEqual(route_status["circuit"]["state"], "open")
            self.assertNotIn("profile.local", json.dumps(route_status, sort_keys=True))
            self.assertNotIn("/private", json.dumps(route_status, sort_keys=True))

    async def test_ephemeral_turn_failure_does_not_replace_session_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            route = Route(
                platform="signal",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.EPHEMERAL,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(route,),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(make_event())

            self.assertEqual(profile.resumes, 0)
            self.assertEqual(profile.released_session_ids, ["session-1"])
