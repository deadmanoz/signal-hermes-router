from __future__ import annotations

import asyncio
import tempfile
from contextlib import suppress
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.config import (
    AppConfig,
    Route,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import TurnResult
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    ClosedAwareSignal,
    FakeProfile,
    FakeSupervisor,
    make_group_raw,
    make_route,
    router_config_for_tmp,
    RouterTestCase,
)


class MultiProfileSupervisor:
    """Fake supervisor that hands each route its profile by name, so a test can
    make one route's profile slow while another's is fast."""

    def __init__(self, profiles: dict[str, FakeProfile]) -> None:
        self.profiles = profiles
        self.restarts = 0

    async def get_profile(self, route: Route) -> FakeProfile:
        return self.profiles[route.profile]

    async def restart_profile(self, profile_name: str) -> None:
        self.restarts += 1

    async def close(self) -> None:
        return None


def _concurrent_route(name: str, group_id: str, profile: str, **kwargs: Any) -> Route:
    return make_route(name=name, group_id=group_id, profile=profile, **kwargs)


def _concurrent_app(tmp: str, routes: tuple[Route, ...], **overrides: Any) -> AppConfig:
    return AppConfig(
        router=router_config_for_tmp(tmp, **overrides),
        routes=routes,
    )


class RouterConcurrencyTests(RouterTestCase):
    async def _settle(self, rounds: int = 20) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)

    async def _await_condition(self, predicate, *, timeout: float = 2.0) -> None:
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.01)

    async def _shutdown(self, router: SignalHermesRouter, run_task: asyncio.Task) -> None:
        await asyncio.wait_for(router.close(), timeout=5)
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(run_task, timeout=5)

    async def test_two_routes_distinct_profiles_progress_concurrently(self) -> None:
        started_a = asyncio.Event()
        gate_a = asyncio.Event()
        profile_a = FakeProfile(gate_started=started_a, gate_wait=gate_a)
        profile_b = FakeProfile()

        class TwoRouteSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-a", timestamp=1)
                yield make_group_raw(group_id="group-b", timestamp=2)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = TwoRouteSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (
                        _concurrent_route("route-a", "group-a", "profile-a"),
                        _concurrent_route("route-b", "group-b", "profile-b"),
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=MultiProfileSupervisor(  # type: ignore[arg-type]
                    {"profile-a": profile_a, "profile-b": profile_b}
                ),
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            # Route A's turn is gated (slow). Route B, on a distinct profile, must
            # still be delivered while A is stuck.
            await asyncio.wait_for(started_a.wait(), timeout=1)
            await self._await_condition(lambda: ("group-b", "reply") in signal.sends)
            self.assertIn(("group-b", "reply"), signal.sends)
            self.assertNotIn(("group-a", "reply"), signal.sends)
            self.assertFalse(gate_a.is_set())

            # Releasing A lets it complete too.
            gate_a.set()
            await self._await_condition(lambda: ("group-a", "reply") in signal.sends)
            await self._shutdown(router, run_task)

    async def test_same_route_events_preserve_arrival_order(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()

        class OrderRecordingProfile(FakeProfile):
            """Records the text of each prompt as it is ENTERED (before the gate),
            so the observed order reflects which same-route turn ran first even
            while the first is still gated."""

            def __init__(self, started: asyncio.Event, gate: asyncio.Event) -> None:
                super().__init__()
                self._started = started
                self._gate = gate
                self.entered: list[str] = []

            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.entered.append(str(blocks))
                if len(self.entered) == 1:
                    self._started.set()
                    await self._gate.wait()
                return await super().prompt(session_id, blocks)

        profile = OrderRecordingProfile(started, gate)

        class OrderedSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-a", timestamp=1, text="first")
                yield make_group_raw(group_id="group-a", timestamp=2, text="second")
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = OrderedSignal()
            router = SignalHermesRouter(
                _concurrent_app(tmp, (_concurrent_route("route-a", "group-a", "profile-a"),)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            # The first event's turn is gated and holds the route lock; the second
            # same-route event must wait behind it, not enter its prompt.
            await asyncio.wait_for(started.wait(), timeout=1)
            await self._settle()
            self.assertEqual(len(profile.entered), 1)
            self.assertIn("first", profile.entered[0])

            gate.set()
            await self._await_condition(lambda: len(profile.entered) == 2)
            # Strict arrival order preserved by the route lock.
            self.assertIn("first", profile.entered[0])
            self.assertIn("second", profile.entered[1])
            await self._shutdown(router, run_task)

    async def test_same_route_backlog_does_not_hold_execution_slot(self) -> None:
        # Round-1 regression: a same-route backlog queued on the route lock must
        # not consume execution capacity that another route needs.
        started_a = asyncio.Event()
        gate_a = asyncio.Event()
        profile_a = FakeProfile(gate_started=started_a, gate_wait=gate_a)
        profile_b = FakeProfile()

        class BurstSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-a", timestamp=1)  # A1 (gated)
                yield make_group_raw(group_id="group-a", timestamp=2)  # A2 (same route)
                yield make_group_raw(group_id="group-b", timestamp=3)  # B (other route)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = BurstSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (
                        _concurrent_route("route-a", "group-a", "profile-a"),
                        _concurrent_route("route-b", "group-b", "profile-b"),
                    ),
                    max_concurrent_turns=2,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=MultiProfileSupervisor(  # type: ignore[arg-type]
                    {"profile-a": profile_a, "profile-b": profile_b}
                ),
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(started_a.wait(), timeout=1)
            # A1 holds one execution permit; A2 is queued on route A's lock and
            # holds none. B (distinct profile) takes the second permit and replies
            # while A1 is still gated.
            await self._await_condition(lambda: ("group-b", "reply") in signal.sends)
            self.assertNotIn(("group-a", "reply"), signal.sends)
            self.assertFalse(gate_a.is_set())

            gate_a.set()
            await self._await_condition(lambda: ("group-a", "reply") in signal.sends)
            await self._shutdown(router, run_task)

    async def test_execution_semaphore_caps_concurrent_turns(self) -> None:
        # Round-2 verification: proves max_concurrent_turns is actually applied to
        # turn execution, not merely parsed.
        started_1 = asyncio.Event()
        gate_1 = asyncio.Event()
        started_2 = asyncio.Event()
        gate_2 = asyncio.Event()
        profile_1 = FakeProfile(gate_started=started_1, gate_wait=gate_1)
        profile_2 = FakeProfile(gate_started=started_2, gate_wait=gate_2)
        profile_3 = FakeProfile()

        class CapSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-1", timestamp=1)
                yield make_group_raw(group_id="group-2", timestamp=2)
                yield make_group_raw(group_id="group-3", timestamp=3)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = CapSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (
                        _concurrent_route("route-1", "group-1", "profile-1"),
                        _concurrent_route("route-2", "group-2", "profile-2"),
                        _concurrent_route("route-3", "group-3", "profile-3"),
                    ),
                    max_concurrent_turns=2,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=MultiProfileSupervisor(  # type: ignore[arg-type]
                    {"profile-1": profile_1, "profile-2": profile_2, "profile-3": profile_3}
                ),
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            # The two permits are held by the gated routes 1 and 2; route 3 must
            # not start its turn.
            await asyncio.wait_for(started_1.wait(), timeout=1)
            await asyncio.wait_for(started_2.wait(), timeout=1)
            await self._settle()
            self.assertEqual(profile_3.prompts, [])

            # Freeing one permit admits route 3.
            gate_1.set()
            await self._await_condition(lambda: len(profile_3.prompts) == 1)
            gate_2.set()
            await self._shutdown(router, run_task)

    async def test_stale_event_skipped_after_execution_permit_wait(self) -> None:
        # Round-2 blocker fix: an event fresh at profile-lock admission that ages
        # past max_event_age_seconds while parked on the execution permit must be
        # skipped without prompting.
        base_ms = 1_000_000
        clock = [base_ms]
        started_a = asyncio.Event()
        gate_a = asyncio.Event()
        profile_a = FakeProfile(gate_started=started_a, gate_wait=gate_a)
        profile_b = FakeProfile()

        class StaleSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-a", timestamp=base_ms)
                yield make_group_raw(group_id="group-b", timestamp=base_ms)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = StaleSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (
                        _concurrent_route("route-a", "group-a", "profile-a"),
                        _concurrent_route(
                            "route-b", "group-b", "profile-b", max_event_age_seconds=10
                        ),
                    ),
                    max_concurrent_turns=1,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=MultiProfileSupervisor(  # type: ignore[arg-type]
                    {"profile-a": profile_a, "profile-b": profile_b}
                ),
                dedupe=DedupeStore(),
                clock_ms=lambda: clock[0],
            )
            run_task = asyncio.create_task(router.run_forever())
            # A holds the only execution permit (gated). B passes its pre-permit
            # freshness check while fresh, then parks on the permit.
            await asyncio.wait_for(started_a.wait(), timeout=1)
            await self._settle()
            self.assertEqual(profile_b.prompts, [])

            # B ages out while waiting for the permit.
            clock[0] = base_ms + 20_000
            gate_a.set()
            await self._await_condition(lambda: ("group-a", "reply") in signal.sends)
            await self._settle()
            # B was skipped without prompting.
            self.assertEqual(profile_b.prompts, [])
            self.assertNotIn(("group-b", "reply"), signal.sends)
            await self._shutdown(router, run_task)

    async def test_inflight_buffer_bounds_dispatched_tasks(self) -> None:
        # No unbounded task growth under burst: the in-flight buffer caps the
        # tracked task set even when many same-route events pile up on the lock.
        started = asyncio.Event()
        gate = asyncio.Event()
        profile = FakeProfile(gate_started=started, gate_wait=gate)

        class FloodSignal(ClosedAwareSignal):
            async def events(self):
                for ts in range(1, 7):
                    yield make_group_raw(group_id="group-a", timestamp=ts)
                await asyncio.Event().wait()

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(router_module, "MAX_INFLIGHT_SIGNAL_DISPATCH_FLOOR", 3),
        ):
            signal = FloodSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (_concurrent_route("route-a", "group-a", "profile-a"),),
                    max_concurrent_turns=1,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            # inflight_bound = max(3, 1*2) = 3.
            self.assertEqual(router._inflight_dispatch_semaphore._value, 3)
            run_task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(started.wait(), timeout=1)
            # The consumer parks on the buffer once three tasks are in flight; the
            # tracked set never exceeds the bound despite six pending events.
            observed_max = 0
            for _ in range(30):
                observed_max = max(observed_max, len(router._signal_turn_tasks))
                self.assertLessEqual(len(router._signal_turn_tasks), 3)
                await asyncio.sleep(0)
            self.assertEqual(observed_max, 3)

            gate.set()
            await self._shutdown(router, run_task)

    async def test_close_drains_concurrent_in_flight_turns(self) -> None:
        started_a = asyncio.Event()
        gate_a = asyncio.Event()
        started_b = asyncio.Event()
        gate_b = asyncio.Event()
        profile_a = FakeProfile(gate_started=started_a, gate_wait=gate_a)
        profile_b = FakeProfile(gate_started=started_b, gate_wait=gate_b)

        class TwoRouteSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(group_id="group-a", timestamp=1)
                yield make_group_raw(group_id="group-b", timestamp=2)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = TwoRouteSignal()
            router = SignalHermesRouter(
                _concurrent_app(
                    tmp,
                    (
                        _concurrent_route("route-a", "group-a", "profile-a"),
                        _concurrent_route("route-b", "group-b", "profile-b"),
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=MultiProfileSupervisor(  # type: ignore[arg-type]
                    {"profile-a": profile_a, "profile-b": profile_b}
                ),
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(started_a.wait(), timeout=1)
            await asyncio.wait_for(started_b.wait(), timeout=1)

            # close() must drain both concurrently in-flight turns before closing
            # transport; it blocks while they are gated.
            close_task = asyncio.create_task(router.close())
            await self._settle()
            self.assertFalse(close_task.done())

            gate_a.set()
            gate_b.set()
            incomplete = await asyncio.wait_for(close_task, timeout=5)
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(run_task, timeout=5)

            self.assertEqual(incomplete, ())
            self.assertIn(("group-a", "reply"), signal.sends)
            self.assertIn(("group-b", "reply"), signal.sends)
