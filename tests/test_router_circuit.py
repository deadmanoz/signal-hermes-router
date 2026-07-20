from __future__ import annotations

import asyncio
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    NormalizedEvent,
    RouteState,
)
from signal_hermes_router.preflight import ToolSurface
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_event,
    make_router_harness,
    wait_until,
    RouterTestCase,
)


class RouterCircuitTests(RouterTestCase):
    async def test_breaker_recovery_after_unrelated_reload_still_probes(self) -> None:
        # A reload that leaves the route ACTIVE does not clear its breaker
        # override, so a later genuine cooldown recovery must still let the
        # queued turn probe the profile — the mask only survives reloads that
        # actually cleared the override.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    maintenance_reply: route under repair
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            harness.router.route_state_overrides[r1.key] = RouteState.MAINTENANCE
            harness.router._trip_times[r1.key] = time.monotonic()
            await harness.router._route_locks[r1.key].acquire()
            turn_task = asyncio.ensure_future(
                harness.router.handle_event(make_event(group_id="group-one"))
            )
            await asyncio.sleep(0.05)
            self.assertFalse(turn_task.done())
            # Unrelated reload: r1 stays ACTIVE, so its override survives and
            # no reload clear is recorded for it.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    maintenance_reply: route under repair
  - name: r2
    platform: signal
    group_id: group-two
    profile: p1
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertIn(r1.key, harness.router.route_state_overrides)
            # The cooldown then elapses: recovery, not a reload, clears the
            # override, so the turn probes the profile as designed.
            harness.router._trip_times[r1.key] = (
                time.monotonic() - harness.router.recovery_seconds - 1
            )
            harness.router._route_locks[r1.key].release()
            outcome = await asyncio.wait_for(turn_task, timeout=5)
            self.assertIsNotNone(outcome)
            self.assertEqual(len(harness.profile.prompts), 1)
            self.assertNotIn(("group-one", "route under repair"), harness.signal.sends)
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_reap_does_not_contest_non_turn_control_requests(self) -> None:
        # A preflight/route-status request never resolves a route: it is
        # marked as a non-turn task so the reaper does not read it as
        # unattributed and conservatively contest every drain key with it —
        # which would hold retired routes contested across follow-up
        # attempts until the chain force-completes under otherwise healthy
        # in-flight turns.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-x"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-x",
                cwd=Path(tmp) / "work" / "session-x",
            )
            harness.router.sessions._session_routes["session-x"] = r1.key
            block = asyncio.Event()
            entered = asyncio.Event()

            async def blocking_preflight(payload: dict[str, Any]) -> dict[str, Any]:
                entered.set()
                await block.wait()
                return {"status": "ok", "missing_tools": []}

            harness.router._handle_preflight_permissions_control = (  # type: ignore[method-assign]
                blocking_preflight
            )
            preflight_task = asyncio.ensure_future(
                harness.router._run_control_request(b'{"command":"preflight_permissions"}')
            )
            await asyncio.wait_for(entered.wait(), timeout=5)
            routes_path.write_text("routes: []\n", encoding="utf-8")
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.5):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertTrue(reapers)
                # The reaper completes without waiting out the blocked
                # non-turn request; an unattributed request would contest r1
                # for the whole chain (4 passes x 0.5s) and retire nothing
                # in the first pass.
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=1.5)
            self.assertEqual(harness.supervisor.retired, ["p1"])
            block.set()
            response = await asyncio.wait_for(preflight_task, timeout=5)
            self.assertEqual(response["status"], "ok")

    async def test_reap_does_not_contest_queued_reload_requests(self) -> None:
        # A reload request never resolves a route either: while it waits on
        # the reload lock or its bounded parse, it is marked as a non-turn
        # task so the reaper does not read it as unattributed and contest
        # every drain key with it — which would hold a just-retired route
        # contested (and its session/profile un-reaped) across follow-up
        # attempts for as long as the queued reload is still parsing.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-x"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-x",
                cwd=Path(tmp) / "work" / "session-x",
            )
            harness.router.sessions._session_routes["session-x"] = r1.key
            # r1 is already swapped out of the live config; the reaper pass
            # below stands in for the chain the first reload scheduled for
            # it, and the QUEUED second reload must not contest its drain.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            harness.router.config = load_app_config(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate
            parse_entered = threading.Event()
            block = threading.Event()

            def hanging_parse(*args: Any, **kwargs: Any) -> Any:
                parse_entered.set()
                block.wait()
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", hanging_parse):
                reload_task = asyncio.ensure_future(
                    harness.router._run_control_request(b'{"command":"reload_config"}')
                )
                try:
                    # The second reload is now a live request task parked inside
                    # its bounded parse; only then does the reaper pass start, so
                    # an unattributed reload task is deterministically in scope.
                    await wait_until(lambda: parse_entered.is_set(), timeout=5.0, interval=0.01)
                    parked = [
                        task for task in harness.router._control_request_tasks if not task.done()
                    ]
                    self.assertEqual(len(parked), 1)
                    self.assertEqual(
                        harness.router._turn_task_route_key(parked[0]),
                        "non-turn-control-request",
                    )
                    with patch.object(harness.router, "_reap_drain_seconds", return_value=0.5):
                        await asyncio.wait_for(harness.router._reap_after_drain(set()), timeout=1.5)
                    # The parked reload fell out of reap scope: r1 drained
                    # immediately instead of being contested for the whole 0.5s
                    # budget (and re-contested across follow-up passes), so its
                    # session and profile are reaped in the first pass.
                    self.assertEqual(harness.supervisor.retired, ["p1"])
                    self.assertEqual(set(harness.router.sessions._sessions), set())
                finally:
                    block.set()
                response = await asyncio.wait_for(reload_task, timeout=5)
                self.assertEqual(response["status"], "ok")
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if reapers:
                    await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_reap_does_not_contest_non_turn_request_tracked_before_it_runs(self) -> None:
        # The non-turn sentinel must be set when the request task is TRACKED,
        # not only when it first runs: a reaper that snapshots the tracked
        # set in the window between task creation and the task's first step
        # would otherwise read the request as unattributed and contest every
        # drain key with it — burning the drain budget on a request that can
        # never resolve a route.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-x"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-x",
                cwd=Path(tmp) / "work" / "session-x",
            )
            harness.router.sessions._session_routes["session-x"] = r1.key
            routes_path.write_text("routes: []\n", encoding="utf-8")
            harness.router.config = load_app_config(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate
            block = threading.Event()

            def hanging_parse(*args: Any, **kwargs: Any) -> Any:
                block.wait()
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", hanging_parse):
                outer = asyncio.ensure_future(
                    harness.router._run_control_request(b'{"command":"reload_config"}')
                )
                try:
                    # Exactly one loop turn: the outer coroutine created and
                    # tracked the inner request task, which itself has NOT
                    # run yet (FIFO: the test task resumes first).
                    await asyncio.sleep(0)
                    parked = [
                        task for task in harness.router._control_request_tasks if not task.done()
                    ]
                    self.assertEqual(len(parked), 1)
                    # Already attributed at tracking time, before the task's
                    # first step could set the sentinel itself.
                    self.assertEqual(
                        harness.router._turn_task_route_key(parked[0]),
                        "non-turn-control-request",
                    )
                    with patch.object(harness.router, "_reap_drain_seconds", return_value=0.5):
                        await asyncio.wait_for(harness.router._reap_after_drain(set()), timeout=1.5)
                    self.assertEqual(harness.supervisor.retired, ["p1"])
                    self.assertEqual(set(harness.router.sessions._sessions), set())
                finally:
                    block.set()
                response = await asyncio.wait_for(outer, timeout=5)
                self.assertEqual(response["status"], "ok")
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if reapers:
                    await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_reap_skips_retire_while_preflight_probe_in_flight(self) -> None:
        # The preflight tool-surface probe runs WITHOUT the profile lock for
        # most of its life, so route liveness alone is not a safe retirement
        # predicate: a reload that removes the route mid-probe must leave the
        # cached profile subprocess running until the probe finishes, or the
        # healthy operator preflight breaks underneath itself.
        entered = asyncio.Event()
        block = asyncio.Event()

        class BlockingSurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                entered.set()
                await block.wait()
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    permissions:
      - tool: read_file
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            profile = BlockingSurfaceProfile()
            harness = make_router_harness(tmp, app=app, profile=profile)
            harness.router.set_config_paths(config_path, routes_path)
            harness.supervisor.cached.append("p1")

            preflight_task = asyncio.ensure_future(
                harness.router._run_control_request(b'{"command":"preflight_permissions"}')
            )
            await asyncio.wait_for(entered.wait(), timeout=5)
            # The probe is now parked inside the ACP tool-surface read, NOT
            # holding the profile lock. Reloading r1 away must not retire
            # p1's cached subprocess out from under it.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertTrue(reapers)
            await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertEqual(harness.supervisor.retired, [])
            self.assertIn("p1", harness.supervisor.cached)

            block.set()
            response = await asyncio.wait_for(preflight_task, timeout=5)
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["probe_errors"], [])
            self.assertEqual(response["missing_tools_count"], 0)
            # The probe's completion schedules the deferred retirement reap;
            # drain it so no tracked task leaks out of the test.
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            if reapers:
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    def test_reap_drain_seconds_tracks_prompt_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp)
            # Supervisors without a prompt timeout (test doubles) get the floor.
            self.assertEqual(
                harness.router._reap_drain_seconds(),
                router_module.RELOAD_RETIRE_DRAIN_TIMEOUT_SECONDS,
            )
            # A healthy prompt may run up to the configured timeout; the reap
            # must wait it out plus a margin rather than closing under it.
            harness.supervisor.prompt_timeout_seconds = 300.0
            self.assertEqual(
                harness.router._reap_drain_seconds(),
                300.0 + router_module.RELOAD_RETIRE_DRAIN_MARGIN_SECONDS,
            )

    async def test_circuit_recovery_probes_after_cooldown_and_clears_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            supervisor = FakeSupervisor(profile)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, failures=1),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(make_event())
            self.assertEqual(router.route_state_overrides["signal:group"], RouteState.MAINTENANCE)

            # Backdate the trip so the cooldown is considered elapsed.
            router._trip_times["signal:group"] = time.monotonic() - router.recovery_seconds - 1.0
            profile.fail = False

            with self.assertLogs("signal_hermes_router.router", level="INFO"):
                result = await router.handle_event(
                    NormalizedEvent(
                        platform="signal",
                        group_id="group",
                        sender_id="sender",
                        source_uuid="sender-probe",
                        timestamp=2,
                        text="probe",
                    )
                )

            self.assertNotIn("signal:group", router.route_state_overrides)
            self.assertNotIn("signal:group", router._trip_times)
            self.assertIsNotNone(result)
            self.assertEqual(signal.sends[-1][1], "reply")

    async def test_circuit_recovery_probe_failure_does_not_immediately_retrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            supervisor = FakeSupervisor(profile)
            # failures=2 so a single probe failure can't trip on its own.
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, failures=2),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            # Force the trip directly to set up the state.
            router.route_state_overrides["signal:group"] = RouteState.MAINTENANCE
            router._trip_times["signal:group"] = time.monotonic() - router.recovery_seconds - 1.0
            router.circuit.record_failure("signal:group")
            router.circuit.record_failure("signal:group")

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(
                    NormalizedEvent(
                        platform="signal",
                        group_id="group",
                        sender_id="sender",
                        source_uuid="sender-probe",
                        timestamp=2,
                        text="probe",
                    )
                )

            # Override was cleared at probe entry; counter reset by record_success;
            # probe-failure counts as 1, below failures=2, so no re-trip yet.
            self.assertNotIn("signal:group", router.route_state_overrides)
            self.assertNotIn("signal:group", router._trip_times)
            self.assertEqual(
                signal.sends[-1][1], "I hit an internal router error handling that message."
            )

