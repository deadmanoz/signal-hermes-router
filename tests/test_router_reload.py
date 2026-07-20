from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.models import (
    RouteState,
    SessionPolicy,
)
from tests.support import (
    make_event,
    make_router_harness,
    wait_until,
    RouterTestCase,
)


class RouterReloadTests(RouterTestCase):
    async def test_reload_config_success(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: new-route
    platform: signal
    group_id: new-group
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["generation"], 1)
            self.assertEqual(response["route_count"], 1)
            self.assertEqual(
                harness.router.config.find_route_by_name("new-route").group_id, "new-group"
            )

    async def test_reload_config_rejects_invalid_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            routes_path.write_text("not: [valid yaml\n", encoding="utf-8")
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "config_invalid")
            self.assertEqual(response["generation"], 0)

    async def test_reload_config_rejects_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            routes_path.write_text(
                """
routes:
  - name: dup
    platform: signal
    group_id: g1
    profile: p
    state: active
  - name: dup
    platform: signal
    group_id: g2
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "config_invalid")
            self.assertEqual(response["generation"], 0)

    async def test_reload_config_rejects_router_config_change(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n"
                "  max_reply_chars: 9999\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r
    platform: signal
    group_id: g
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            # Router-level drift is detected against the raw config.yaml
            # fingerprint captured at registration, without re-resolving
            # startup-only secret refs: any structural/scalar edit rejects
            # the reload and leaves the active configuration unchanged.
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n"
                "  max_reply_chars: 8888\n",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_config_changed")
            self.assertEqual(response["generation"], 0)

    async def test_reload_config_ignores_unresolvable_router_secret(self) -> None:
        # Router-level env:///op:// values are startup-only: a routes-only
        # reload must not fail merely because such a secret is no longer
        # resolvable. The candidate is parsed against the active
        # RouterConfig, and router-level drift is detected on the raw,
        # unresolved config.yaml fingerprint instead.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: env://SHR_TEST_SIGNAL_BASE_URL\n"
                "  allow_remote_signal_base_url: true\n",
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
            with patch.dict(os.environ, {"SHR_TEST_SIGNAL_BASE_URL": "http://127.0.0.1:8080"}):
                from signal_hermes_router.config import load_app_config

                app = load_app_config(config_path, routes_path)
                harness = make_router_harness(tmp, app=app)
                harness.router.set_config_paths(config_path, routes_path)
            # The startup-only env secret is gone: re-resolving config.yaml
            # would now raise, but the routes-only reload never re-resolves
            # router-level values.
            os.environ.pop("SHR_TEST_SIGNAL_BASE_URL", None)
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["generation"], 1)

    async def test_reload_parse_runs_on_dedicated_executor(self) -> None:
        # A hung reload parse must not consume the shared default executor's
        # finite workers that dedupe and media turn I/O depend on.
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
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate
            parse_threads: list[threading.Thread] = []
            hung = threading.Event()

            def recording_parse(*args: Any, **kwargs: Any) -> Any:
                parse_threads.append(threading.current_thread())
                hung.wait(1.0)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", recording_parse):
                reload_task = asyncio.create_task(harness.router._handle_reload_config_control({}))
                await asyncio.sleep(0.1)
                # Turn I/O (dedupe, media) dispatches through the shared
                # default executor; it must not queue behind the hung parse.
                result = await asyncio.wait_for(
                    harness.router._run_io_worker(lambda: 42), timeout=1.0
                )
                self.assertEqual(result, 42)
                hung.set()
                response = await reload_task
            self.assertEqual(response["status"], "ok")
            self.assertTrue(parse_threads)
            self.assertTrue(all(t.name.startswith("shr-reload-parse") for t in parse_threads))
            # The reload-parse worker is a daemon thread: a parse abandoned
            # by its bounded wait (hung resolver) cannot hold the interpreter
            # open at exit.
            self.assertTrue(all(t.daemon for t in parse_threads))
            # And it is NOT registered with the interpreter-shutdown join:
            # _python_exit joins every thread in _threads_queues regardless
            # of the daemon flag, which would defeat the detach.
            from concurrent.futures.thread import _threads_queues

            self.assertTrue(all(t not in _threads_queues for t in parse_threads))

    async def test_reload_keeps_maintenance_mask_for_queued_turn(self) -> None:
        # A turn admitted under an open breaker is queued on the route lock
        # when a reload disables the route and clears the override; it must
        # still send the maintenance reply rather than prompt the profile
        # that was failing at admission.
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
            # The breaker is open for r1 (maintenance override).
            harness.router.route_state_overrides[r1.key] = RouteState.MAINTENANCE
            harness.router._trip_times[r1.key] = time.monotonic()
            # The turn is admitted under the override, then queues behind the
            # held route lock.
            await harness.router._route_locks[r1.key].acquire()
            turn_task = asyncio.ensure_future(
                harness.router.handle_event(make_event(group_id="group-one"))
            )
            await asyncio.sleep(0.05)
            self.assertFalse(turn_task.done())
            # The reload disables the route, clearing the breaker override
            # while the admitted turn is still queued.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
    maintenance_reply: route under repair
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            harness.router._route_locks[r1.key].release()
            outcome = await asyncio.wait_for(turn_task, timeout=5)
            self.assertIsNone(outcome)
            self.assertEqual(harness.profile.prompts, [])
            self.assertEqual(harness.signal.sends, [("group-one", "route under repair")])
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            if reapers:
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_reload_disable_readd_keeps_maintenance_mask_for_queued_turn(self) -> None:
        # Codex scenario: a turn admitted under an open breaker queues on the
        # route lock; one reload disables the route (clearing the override)
        # and a second reload re-adds it ACTIVE before the turn runs. The
        # route is currently ACTIVE, so the current-state check alone would
        # drop the mask — the reload-clear record must keep it.
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
            active_routes = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    maintenance_reply: route under repair
"""
            routes_path.write_text(active_routes, encoding="utf-8")
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
            # Reload 1 disables the route, clearing the override.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
    maintenance_reply: route under repair
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            # Reload 2 re-adds the route ACTIVE before the queued turn runs.
            routes_path.write_text(active_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            current = harness.router.config.find_route_by_name("r1")
            assert current is not None
            self.assertEqual(current.state, RouteState.ACTIVE)
            harness.router._route_locks[r1.key].release()
            outcome = await asyncio.wait_for(turn_task, timeout=5)
            self.assertIsNone(outcome)
            self.assertEqual(harness.profile.prompts, [])
            self.assertEqual(harness.signal.sends, [("group-one", "route under repair")])
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_reload_remove_readd_keeps_maintenance_mask_for_queued_turn(self) -> None:
        # Same reactivation cycle but the first reload removes the route
        # entirely, exercising the absent-key override clear path.
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
            active_routes = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    maintenance_reply: route under repair
"""
            routes_path.write_text(active_routes, encoding="utf-8")
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
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            routes_path.write_text(active_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            harness.router._route_locks[r1.key].release()
            outcome = await asyncio.wait_for(turn_task, timeout=5)
            self.assertIsNone(outcome)
            self.assertEqual(harness.profile.prompts, [])
            self.assertEqual(harness.signal.sends, [("group-one", "route under repair")])
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_reload_reap_force_completes_after_followup_budget(self) -> None:
        # A genuinely wedged turn (never finishes) must not keep the
        # follow-up reap chain alive forever: after REAP_MAX_FOLLOWUP_ATTEMPTS
        # retries the cleanup is completed under the wedge — sessions evicted,
        # profile retired — and no further reaper is scheduled.
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

            harness.router.sessions._sessions["session-1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "session-1",
            )
            harness.router.sessions._session_routes["session-1"] = r1.key
            # A turn that never finishes, attributed to the removed route.
            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)

            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                encoding="utf-8",
            )
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.2):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                for _ in range(100):
                    if harness.supervisor.retired:
                        break
                    await asyncio.sleep(0.1)
            # The bounded chain force-completed: cleanup ran under the wedge
            # and no reaper is left running.
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertEqual([t for t in harness.router._reap_tasks if not t.done()], [])
            stuck.set()
            await asyncio.wait_for(turn_task, timeout=5)

    async def test_reload_reap_force_skips_route_disabled_mid_wait(self) -> None:
        # The follow-up budget is a property of one reaper chain: a route
        # that leaves the active set during the force pass's own drain wait
        # (its own reload scheduled a fresh chain for it) must NOT inherit
        # this chain's exhausted budget — its healthy turn gets the full
        # drain grace from its own chain.
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
            both_active = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
"""
            routes_path.write_text(both_active, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            r1 = harness.router.config.find_route_by_name("r1")
            r2 = harness.router.config.find_route_by_name("r2")
            assert r1 is not None and r2 is not None
            harness.supervisor.cached.extend(["p1", "p2"])
            from signal_hermes_router.sessions import RoutedSession

            for session_id, key in (("session-x", r1.key), ("session-y", r2.key)):
                harness.router.sessions._sessions[session_id] = RoutedSession(
                    profile=harness.profile,  # type: ignore[arg-type]
                    session_id=session_id,
                    cwd=Path(tmp) / "work" / session_id,
                )
                harness.router.sessions._session_routes[session_id] = key
            # r1 is non-live with a wedged turn; r2 is still live when the
            # force pass starts (so it is not in this pass's drain set).
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            harness.router.config = load_app_config(config_path, routes_path)
            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)

            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.3):
                # r1 is this chain's carried un-drained key (it failed to
                # drain in the chain's earlier passes); r2 is still live at
                # pass start, so it cannot be force-eligible.
                reap = asyncio.ensure_future(
                    harness.router._reap_after_drain(
                        set(), followup_attempt=3, force_eligible={r1.key}
                    )
                )
                await asyncio.sleep(0.1)
                # Mid-wait, a later reload disables r2 as well: it joins the
                # tombstone leftovers but was never in this pass's drain set.
                routes_path.write_text(
                    """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: disabled
""",
                    encoding="utf-8",
                )
                harness.router.config = load_app_config(config_path, routes_path)
                await asyncio.wait_for(reap, timeout=5)
            # The force completed only r1's cleanup; r2's session and profile
            # survive for its own reaper chain to drain with a fresh budget.
            self.assertEqual(set(harness.router.sessions._sessions), {"session-y"})
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.profile.released_session_ids, ["session-x"])
            stuck.set()
            await asyncio.wait_for(turn_task, timeout=5)

    async def test_reload_reap_force_skips_route_retired_before_firing_pass(self) -> None:
        # drain_keys is recomputed from the tombstones on every pass, so a
        # route retired by a later reload BETWEEN passes shows up in the
        # firing pass's drain set without this chain ever having waited it
        # out. Only keys carried over from the previous pass's un-drained
        # set may be force-completed; the newly retired route keeps the full
        # drain grace of its own chain.
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
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: disabled
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            r1 = harness.router.config.find_route_by_name("r1")
            r2 = harness.router.config.find_route_by_name("r2")
            assert r1 is not None and r2 is not None
            harness.supervisor.cached.extend(["p1", "p2"])
            from signal_hermes_router.sessions import RoutedSession

            for session_id, key in (("session-x", r1.key), ("session-y", r2.key)):
                harness.router.sessions._sessions[session_id] = RoutedSession(
                    profile=harness.profile,  # type: ignore[arg-type]
                    session_id=session_id,
                    cwd=Path(tmp) / "work" / session_id,
                )
                harness.router.sessions._session_routes[session_id] = key
            # Both routes are already non-live at pass start: r2 (retired
            # between passes) IS in this pass's drain set, but only r1 was
            # carried over from the chain's previous un-drained set. r2's
            # lock stays held so nothing but a force could complete it.
            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)
            await harness.router._route_locks[r2.key].acquire()
            try:
                with patch.object(harness.router, "_reap_drain_seconds", return_value=0.3):
                    reap = asyncio.ensure_future(
                        harness.router._reap_after_drain(
                            set(), followup_attempt=3, force_eligible={r1.key}
                        )
                    )
                    await asyncio.wait_for(reap, timeout=5)
            finally:
                harness.router._route_locks[r2.key].release()
            # The force completed only r1's cleanup; r2's session and profile
            # survive for its own reaper chain with a fresh budget.
            self.assertEqual(set(harness.router.sessions._sessions), {"session-y"})
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.profile.released_session_ids, ["session-x"])
            stuck.set()
            await asyncio.wait_for(turn_task, timeout=5)

    async def test_reload_executor_replaced_after_parse_timeout(self) -> None:
        # A timed-out parse leaves its hung worker occupying the single-slot
        # executor; the router detaches it (bounded) so a later reload with a
        # fixed routes file parses on a fresh worker instead of timing out
        # behind the wedge until restart.
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
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate
            block = threading.Event()

            def hanging_parse(*args: Any, **kwargs: Any) -> Any:
                block.wait()
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", hanging_parse):
                with patch.object(router_module, "RELOAD_PARSE_TIMEOUT_SECONDS", 0.2):
                    for expected_abandoned in (1, 2, 3):
                        response = await harness.router._handle_reload_config_control({})
                        self.assertEqual(response["status"], "error")
                        self.assertEqual(response["error"], "config_parse_timeout")
                        self.assertIsNone(harness.router._reload_executor)
                        self.assertEqual(
                            len(harness.router._abandoned_reload_executors),
                            expected_abandoned,
                        )
                    # Past the cap the wedged executor is kept in place rather
                    # than accumulating another abandoned daemon thread.
                    response = await harness.router._handle_reload_config_control({})
                    self.assertEqual(response["error"], "config_parse_timeout")
                    self.assertIsNotNone(harness.router._reload_executor)
                    self.assertEqual(len(harness.router._abandoned_reload_executors), 3)
                    # Saturated: later reloads fail fast instead of queueing
                    # another parse behind the kept executor's hung worker
                    # to burn the bounded wait for nothing.
                    self.assertTrue(harness.router._reload_parse_saturated)
                    started = time.monotonic()
                    response = await harness.router._handle_reload_config_control({})
                    self.assertLess(time.monotonic() - started, 0.2)
                    self.assertEqual(response["status"], "error")
                    self.assertEqual(response["error"], "config_parse_saturated")
                    block.set()
            # With the wedge cleared, the kept executor's worker frees and a
            # later reload succeeds without a process restart.
            await asyncio.sleep(0.2)
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertFalse(harness.router._reload_parse_saturated)
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_reload_rejects_router_config_edit_landing_mid_parse(self) -> None:
        # The raw config.yaml fingerprint is read AFTER the candidate routes
        # parse, not before: the parse may block on YAML/secret I/O, and a
        # router-level edit landing mid-parse must still reject the reload
        # as router_config_changed instead of adopting a generation parsed
        # from mixed router/routes state.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_text = (
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n"
            )
            config_path.write_text(config_text, encoding="utf-8")
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
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            # Establish the baseline fingerprint with a clean reload.
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            real_load = router_module.load_routes_config

            def editing_load(*args: Any, **kwargs: Any) -> Any:
                # A router-level edit lands while the routes parse is in
                # flight: a value change, not a comment, so the fingerprint
                # must shift.
                config_path.write_text(config_text.replace("8080", "8081"), encoding="utf-8")
                return real_load(*args, **kwargs)

            with patch.object(router_module, "load_routes_config", editing_load):
                response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_config_changed")

    async def test_reload_skips_parse_when_config_already_drifted(self) -> None:
        # A reload against an already-drifted config.yaml is doomed: the
        # worker checks the baseline fingerprint BEFORE parsing, so the
        # rejection is the documented router_config_changed — not a routes
        # parse/validation error. The invalid routes file is the probe: a
        # parse attempt would surface config_invalid instead.
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
            routes_path.write_text("routes: []\n", encoding="utf-8")
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            # Router-level drift lands before the reload; the candidate
            # routes file is unparseable.
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8081\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_path.write_text("routes: [unclosed\n", encoding="utf-8")

            response = await harness.router._handle_reload_config_control({})

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_config_changed")

    async def test_reload_reap_skips_contested_pre_lock_turn_then_followup_reaps(self) -> None:
        # A turn still in its pre-lock awaits when the reap's pending-turn
        # wait times out must NOT leave its route marked drained: the reaper
        # skips it, and a follow-up reap completes the cleanup once the turn
        # finishes — no later reload required.
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

            harness.router.sessions._sessions["session-1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "session-1",
            )
            harness.router.sessions._session_routes["session-1"] = r1.key

            # A turn admitted before the reload is stuck in its pre-lock
            # awaits (e.g. a slow attachment freeze); attribution records the
            # route it can still touch.
            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)

            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                encoding="utf-8",
            )
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.3):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                first = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertEqual(len(first), 1)
                await asyncio.wait_for(asyncio.gather(*first), timeout=5)
            # The contested pre-lock turn was not treated as drained: the
            # session and profile survive, and a follow-up reap is pending.
            self.assertEqual(harness.supervisor.retired, [])
            self.assertEqual(set(harness.router.sessions._sessions), {"session-1"})
            followups = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertEqual(len(followups), 1)
            # Once the turn finishes, the follow-up drains and completes the
            # cleanup without another reload.
            stuck.set()
            await asyncio.wait_for(asyncio.gather(*followups), timeout=5)
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])

    async def test_reload_reap_does_not_wait_for_unrelated_live_route_turn(self) -> None:
        # A long turn on an unrelated ACTIVE route must not consume the
        # deadline a removed route's cleanup is drained against.
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
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            r2 = harness.router.config.find_route_by_name("r2")
            assert r1 is not None and r2 is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "session-1",
            )
            harness.router.sessions._session_routes["session-1"] = r1.key

            # A long turn on unrelated, still-active r2.
            slow = asyncio.Event()
            turn_task = asyncio.ensure_future(slow.wait())
            turn_task.signal_hermes_route_key = r2.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)

            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            with patch.object(harness.router, "_reap_drain_seconds", return_value=1.0):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertEqual(len(reapers), 1)
                # r1's lock is free and the r2 turn is out of drain scope, so
                # the reaper finishes well before the r2 turn does instead of
                # burning the whole drain bound on it.
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=0.8)
            self.assertFalse(turn_task.done())
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.router.sessions._sessions, {})
            slow.set()
            await turn_task

    async def test_reload_readded_route_with_changed_policy_evicts_stale_session(self) -> None:
        # A route removed in one reload and re-added with a different
        # session_policy in the next has no old-config entry to diff, but the
        # registry-level mismatch scan still finds its old-policy sessions.
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

            def write_routes(r1_block: str) -> None:
                routes_path.write_text(
                    r1_block
                    + """
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                    encoding="utf-8",
                )

            write_routes(
                """routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
"""
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "session-1",
            )
            harness.router.sessions._session_routes["session-1"] = r1.key

            # Reload 1 removes r1 while a stuck pre-lock turn keeps the reap
            # from draining it, so its old-policy session survives.
            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)
            write_routes("routes:\n")
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.3):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                first = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertEqual(len(first), 1)
                await asyncio.wait_for(asyncio.gather(*first), timeout=5)
            self.assertEqual(set(harness.router.sessions._sessions), {"session-1"})

            # Reload 2 re-adds r1 active with a different session_policy: the
            # old-vs-new diff cannot see the removal, but the stale session
            # is still cached, so the mismatch is rediscovered and evicted.
            write_routes(
                """routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    session_policy: ephemeral
"""
            )
            stuck.set()
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            await wait_until(
                lambda: not harness.router.sessions._sessions, timeout=5.0, interval=0.05
            )
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            # The route is live again under the same profile, so its profile
            # subprocess is never retired.
            self.assertEqual(harness.supervisor.retired, [])
            await turn_task

    async def test_reload_policy_mismatch_rediscovered_after_reap_timeout(self) -> None:
        # An active route whose session_policy changed keeps its old-policy
        # sessions when the reaper times out; a later reload has no
        # old-vs-new diff anymore, but the registry scan rediscovers the
        # stale sessions and schedules their eviction again.
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
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["session-1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "session-1",
            )
            harness.router.sessions._session_routes["session-1"] = r1.key

            stuck = asyncio.Event()
            turn_task = asyncio.ensure_future(stuck.wait())
            turn_task.signal_hermes_route_key = r1.key
            harness.router._signal_turn_tasks.add(turn_task)
            turn_task.add_done_callback(harness.router._settle_tracked_task)

            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    session_policy: ephemeral
""",
                encoding="utf-8",
            )
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.3):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                first = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertEqual(len(first), 1)
                await asyncio.wait_for(asyncio.gather(*first), timeout=5)
            # The contested turn kept r1 un-drained: the stale session lives.
            self.assertEqual(set(harness.router.sessions._sessions), {"session-1"})

            # A second reload with the SAME file has no old-vs-new policy
            # diff, yet the mismatch is still found and a reaper scheduled.
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            followups = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertTrue(followups)
            stuck.set()
            await wait_until(
                lambda: not harness.router.sessions._sessions, timeout=5.0, interval=0.05
            )
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            await turn_task

    async def test_reload_refund_targets_reserved_bucket_not_replacement(self) -> None:
        # An in-flight turn refunds its rate token to the exact bucket it
        # reserved from: after a remove+re-add cycle replaced the bucket, the
        # refund must not mint capacity in the replacement.
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

            def write_routes(r1_block: str) -> None:
                routes_path.write_text(
                    r1_block
                    + """
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                    encoding="utf-8",
                )

            write_routes(
                """routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    inbound_rate_limit:
      max_turns: 1
      window_seconds: 60
"""
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            bucket_a = harness.router._reserve_inbound_rate_token(r1)
            self.assertIsNotNone(bucket_a)

            # The route is removed (its bucket pruned) and re-added with the
            # same limit before the in-flight turn settles.
            write_routes("routes:\n")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            if reapers:
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            write_routes(
                """routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    inbound_rate_limit:
      max_turns: 1
      window_seconds: 60
"""
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            r1_new = harness.router.config.find_route_by_name("r1")
            assert r1_new is not None
            bucket_b = harness.router._reserve_inbound_rate_token(r1_new)
            self.assertIsNotNone(bucket_b)
            self.assertIsNot(bucket_a, bucket_b)
            # bucket_b is now spent (max_turns=1). The in-flight turn's
            # pre-prompt failure refunds the DEAD bucket it reserved from;
            # the replacement must stay spent.
            assert bucket_a is not None
            bucket_a.refund()
            self.assertIsNone(harness.router._reserve_inbound_rate_token(r1_new))

    async def test_reload_config_fails_when_paths_unknown(self) -> None:
        harness = make_router_harness(tempfile.mkdtemp())
        response = await harness.router._handle_reload_config_control({})
        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"], "reload_paths_unknown")
        self.assertEqual(response["generation"], 0)

    async def test_reload_config_rejects_invalid_candidate_routes_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp)
            harness.router._config_paths = (Path(tmp) / "config.yaml", Path(tmp) / "routes.yaml")
            response = await harness.router._handle_reload_config_control(
                {"candidate_routes": ["not", "a", "string"]}
            )
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "invalid_candidate_routes")
            self.assertEqual(response["generation"], 0)

    async def test_reload_config_rejects_relative_candidate_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp)
            harness.router._config_paths = (Path(tmp) / "config.yaml", Path(tmp) / "routes.yaml")
            # A relative override would resolve against the long-lived
            # router's cwd, not the operator's shell, so it is rejected
            # before any file is read.
            response = await harness.router._handle_reload_config_control(
                {"candidate_routes": "candidate.yaml"}
            )
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "candidate_routes_not_absolute")
            self.assertEqual(response["generation"], 0)

    async def test_reload_config_rejects_profile_change_for_existing_route(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: existing-route
    platform: signal
    group_id: g1
    profile: old-profile
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            # Now reload with same route key but different profile
            routes_path.write_text(
                """
routes:
  - name: existing-route
    platform: signal
    group_id: g1
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "profile_changed_for_existing_route")
            self.assertEqual(response["generation"], 0)
            # Active config unchanged
            self.assertEqual(
                harness.router.config.find_route_by_name("existing-route").profile,
                "old-profile",
            )

    async def test_reload_config_rebuilds_rate_limit_bucket(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: rl-route
    platform: signal
    group_id: g1
    profile: p
    state: active
    inbound_rate_limit:
      max_turns: 5
      window_seconds: 10
""",
                encoding="utf-8",
            )
            # Build harness from the config file so rl-route is live
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            # Prime the bucket with the old limit
            old_route = harness.router.config.find_route_by_name("rl-route")
            assert old_route is not None
            harness.router._reserve_inbound_rate_token(old_route)
            old_key = harness.router._rate_limit_bucket_key(old_route)
            old_bucket = harness.router._inbound_rate_buckets[old_key]

            # Reload with a new limit on the same route key
            routes_path.write_text(
                """
routes:
  - name: rl-route
    platform: signal
    group_id: g1
    profile: p
    state: active
    inbound_rate_limit:
      max_turns: 1
      window_seconds: 1
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            # The new bucket should have the new parameters
            new_route = harness.router.config.find_route_by_name("rl-route")
            assert new_route is not None
            # Reserve first to lazily create the new bucket, then inspect
            self.assertTrue(harness.router._reserve_inbound_rate_token(new_route))
            new_key = harness.router._rate_limit_bucket_key(new_route)
            new_bucket = harness.router._inbound_rate_buckets[new_key]
            self.assertIsNot(new_bucket, old_bucket)
            # Verify behaviorally: new bucket allows 1 token then rejects
            self.assertFalse(harness.router._reserve_inbound_rate_token(new_route))
            # The stale bucket is pruned from the dict: keeping it would let
            # a later restore of the same limit resurrect its spent token
            # state. In-flight turns are unaffected — they refund the exact
            # bucket OBJECT they reserved from, never a dict lookup, and the
            # refund cannot leak capacity into the new generation.
            self.assertNotIn(old_key, harness.router._inbound_rate_buckets)
            old_bucket.refund()
            self.assertFalse(harness.router._reserve_inbound_rate_token(new_route))

    async def test_reload_restored_rate_limit_starts_with_a_fresh_bucket(self) -> None:
        # A limit removed in one reload and restored (identically) in a later
        # one must not resurrect the preserved bucket's spent token state:
        # the operator reconfigured the limiter, so the restored generation
        # starts full instead of dropping fresh turns as rate_limited while
        # the stale bucket slowly refills.
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            limited_routes = """
routes:
  - name: rl-route
    platform: signal
    group_id: g1
    profile: p
    state: active
    inbound_rate_limit:
      max_turns: 2
      window_seconds: 60
"""
            routes_path.write_text(limited_routes, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            route = harness.router.config.find_route_by_name("rl-route")
            assert route is not None
            # Spend the whole burst; the window is long enough that wall
            # clock refill cannot mint another token during the test.
            self.assertTrue(harness.router._reserve_inbound_rate_token(route))
            self.assertTrue(harness.router._reserve_inbound_rate_token(route))
            self.assertFalse(harness.router._reserve_inbound_rate_token(route))

            # Reload 1: remove the limit entirely.
            routes_path.write_text(
                """
routes:
  - name: rl-route
    platform: signal
    group_id: g1
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            # Reload 2: restore the identical limit.
            routes_path.write_text(limited_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            restored = harness.router.config.find_route_by_name("rl-route")
            assert restored is not None
            # A fresh bucket: the burst is available again immediately, not
            # gated on the stale bucket's spent state.
            self.assertTrue(harness.router._reserve_inbound_rate_token(restored))
            self.assertTrue(harness.router._reserve_inbound_rate_token(restored))
            self.assertFalse(harness.router._reserve_inbound_rate_token(restored))
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_reload_config_rejects_readded_route_with_different_profile(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: removable-route
    platform: signal
    group_id: g1
    profile: original-profile
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            # Reload 1: remove the route entirely
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response1 = await harness.router._handle_reload_config_control({})
            self.assertEqual(response1["status"], "ok")
            self.assertEqual(len(harness.router.config.routes), 0)

            # Reload 2: re-add the same route key with a different profile
            routes_path.write_text(
                """
routes:
  - name: removable-route
    platform: signal
    group_id: g1
    profile: different-profile
    state: active
""",
                encoding="utf-8",
            )
            response2 = await harness.router._handle_reload_config_control({})
            self.assertEqual(response2["status"], "error")
            self.assertEqual(response2["error"], "profile_changed_for_existing_route")
            self.assertEqual(response2["generation"], response1["generation"])
            # Active config unchanged after rejected reload
            self.assertEqual(len(harness.router.config.routes), 0)

    async def test_reload_config_disabled_clears_breaker_override(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: g1
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            # Trip the breaker manually
            route = harness.router.config.find_route_by_name("breaker-route")
            assert route is not None
            for _ in range(3):
                harness.router.circuit.record_failure(route.key)
            harness.router.route_state_overrides[route.key] = RouteState.MAINTENANCE
            harness.router._trip_times[route.key] = time.monotonic()
            harness.router._trip_times_ms[route.key] = harness.router._clock_ms()
            self.assertEqual(
                harness.router.route_state_overrides.get(route.key),
                RouteState.MAINTENANCE,
            )

            # Reload the same route as disabled
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: g1
    profile: p
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            # The breaker override should be cleared
            self.assertIsNone(harness.router.route_state_overrides.get(route.key))
            # A subsequent turn should see DISABLED, not MAINTENANCE
            self.assertEqual(
                harness.router.config.find_route_by_name("breaker-route").state, RouteState.DISABLED
            )

    async def test_reload_config_shadow_clears_breaker_override(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("breaker-route")
            assert route is not None
            for _ in range(3):
                harness.router.circuit.record_failure(route.key)
            harness.router.route_state_overrides[route.key] = RouteState.MAINTENANCE
            harness.router._trip_times[route.key] = time.monotonic()
            harness.router._trip_times_ms[route.key] = harness.router._clock_ms()

            # Reload the same route as shadow
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: group-one
    profile: p
    state: shadow
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            # The stale MAINTENANCE override must not mask the reloaded shadow
            # state (new turns would otherwise send maintenance replies).
            self.assertIsNone(harness.router.route_state_overrides.get(route.key))
            self.assertNotIn(route.key, harness.router._trip_times)
            self.assertNotIn(route.key, harness.router._trip_times_ms)
            self.assertEqual(
                harness.router.config.find_route_by_name("breaker-route").state, RouteState.SHADOW
            )

    async def test_reload_config_active_route_keeps_breaker_override(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("breaker-route")
            assert route is not None
            for _ in range(3):
                harness.router.circuit.record_failure(route.key)
            harness.router.route_state_overrides[route.key] = RouteState.MAINTENANCE
            harness.router._trip_times[route.key] = time.monotonic()
            harness.router._trip_times_ms[route.key] = harness.router._clock_ms()

            # Reload with the route still active: a reload must not silently
            # reset a tripped breaker.
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertEqual(
                harness.router.route_state_overrides.get(route.key),
                RouteState.MAINTENANCE,
            )
            self.assertIn(route.key, harness.router._trip_times)

    async def test_reload_config_parses_candidate_off_the_event_loop(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: new-route
    platform: signal
    group_id: new-group
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate
            parse_threads: list[int] = []

            def recording_parse(*args: Any, **kwargs: Any) -> Any:
                parse_threads.append(threading.get_ident())
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", recording_parse):
                response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            # The blocking parse (secret resolvers, filesystem) must run in a
            # worker thread, not on the event loop's thread.
            self.assertEqual(len(parse_threads), 1)
            self.assertNotEqual(parse_threads[0], threading.get_ident())

    async def test_reload_config_retires_profile_and_sessions_with_no_active_routes(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: retire-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("retire-route")
            assert route is not None
            # Simulate a cached profile subprocess and a cached persistent session.
            harness.supervisor.cached.append("p")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # Reload with the route disabled: no active route remains for p.
            routes_path.write_text(
                """
routes:
  - name: retire-route
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            # Eviction and retirement are deferred to a tracked drain task so
            # an in-flight turn never loses its in-progress reply.
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            self.assertEqual(len(tasks), 1)
            await asyncio.gather(*tasks)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p"])
            self.assertEqual(harness.supervisor.cached, [])

    async def test_reload_config_retires_profile_for_removed_route(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: removed-route
    platform: signal
    group_id: group-one
    profile: p1
    state: active
  - name: kept-route
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            removed = harness.router.config.find_route_by_name("removed-route")
            assert removed is not None
            harness.supervisor.cached.extend(["p1", "p2"])
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = removed.key

            # Reload with removed-route gone entirely.
            routes_path.write_text(
                """
routes:
  - name: kept-route
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            self.assertEqual(len(tasks), 1)
            await asyncio.gather(*tasks)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.supervisor.cached, ["p2"])

    async def test_reload_config_keeps_profile_with_remaining_active_route(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p
    state: active
  - name: r2
    platform: signal
    group_id: group-two
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            r1 = harness.router.config.find_route_by_name("r1")
            r2 = harness.router.config.find_route_by_name("r2")
            assert r1 is not None and r2 is not None
            harness.supervisor.cached.append("p")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk1"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk1",
            )
            harness.router.sessions._session_routes["sk1"] = r1.key
            harness.router.sessions._sessions["sk2"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-2",
                cwd=Path(tmp) / "work" / "sk2",
            )
            harness.router.sessions._session_routes["sk2"] = r2.key

            # Reload with r1 disabled but r2 still active on the same profile.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            # A drain task runs for the disabled route's session...
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            self.assertEqual(len(tasks), 1)
            await asyncio.gather(*tasks)
            # ...evicting only that session; the active route's session is kept.
            self.assertEqual(list(harness.router.sessions._sessions), ["sk2"])
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            # The profile still has an active route: it is not retired.
            self.assertEqual(harness.supervisor.retired, [])
            self.assertEqual(harness.supervisor.cached, ["p"])

    async def test_reload_config_defers_eviction_until_in_flight_turn_drains(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: busy-route
    platform: signal
    group_id: group
    profile: profile
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            harness.profile.prompt_delay = 0.2
            harness.supervisor.cached.append("profile")

            turn_task = asyncio.create_task(harness.router.handle_event(make_event(timestamp=1)))
            try:
                for _ in range(100):
                    if harness.profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(harness.profile.prompts)

                # Reload the busy route to disabled while its turn is mid-prompt.
                routes_path.write_text(
                    """
routes:
  - name: busy-route
    platform: signal
    group_id: group
    profile: profile
    state: disabled
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")

                # The reap must not evict the in-flight session or retire the
                # profile while the turn is still prompting: releasing the
                # session mid-prompt would silently truncate the reply.
                await asyncio.sleep(0.05)
                self.assertEqual(harness.profile.released_session_ids, [])
                self.assertEqual(harness.supervisor.retired, [])
            finally:
                await turn_task

            # The in-flight turn completed normally with its reply delivered.
            self.assertEqual(harness.signal.sends, [("group", "reply")])
            # Only after the turn drained is the session evicted and the
            # profile (no remaining active routes) retired. The reap task may
            # already have finished during the turn await above.
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            if tasks:
                await asyncio.gather(*tasks)
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["profile"])
            self.assertEqual(harness.router.sessions._sessions, {})

    async def test_reload_config_reactivated_route_keeps_session_and_profile(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: flip-route
    platform: signal
    group_id: group-one
    profile: profile
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("flip-route")
            assert route is not None
            harness.supervisor.cached.append("profile")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # Park the reap task behind a held route lock (an in-flight turn).
            lock = harness.router._route_locks[route.key]
            await lock.acquire()
            try:
                # Reload #1 disables the route; the reap task parks on the lock.
                routes_path.write_text(
                    """
routes:
  - name: flip-route
    platform: signal
    group_id: group-one
    profile: profile
    state: disabled
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                await asyncio.sleep(0.02)
                self.assertEqual(harness.profile.released_session_ids, [])

                # Reload #2 re-activates the route before the drain completes.
                routes_path.write_text(
                    """
routes:
  - name: flip-route
    platform: signal
    group_id: group-one
    profile: profile
    state: active
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
            finally:
                lock.release()

            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            if tasks:
                await asyncio.gather(*tasks)
            # The drain re-validates against the current config: a route live
            # again keeps its session and its profile.
            self.assertEqual(list(harness.router.sessions._sessions), ["sk"])
            self.assertEqual(harness.profile.released_session_ids, [])
            self.assertEqual(harness.supervisor.retired, [])
            self.assertEqual(harness.supervisor.cached, ["profile"])

    async def test_reload_config_cross_reload_reap_waits_for_removed_route_turn(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            harness.profile.prompt_delay = 0.2
            harness.supervisor.cached.extend(["p1", "p2"])

            # A turn on r2 is mid-prompt and holds r2's route lock.
            turn_task = asyncio.create_task(
                harness.router.handle_event(make_event(group_id="group-two", timestamp=1))
            )
            try:
                for _ in range(100):
                    if harness.profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(harness.profile.prompts)

                # Reload #1 removes r2 entirely; its reap parks on r2's lock.
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
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")

                # Reload #2 disables r1 while the first reap is still parked.
                # Its drain must cover r2 as well, or it would cut down r2's
                # in-flight session that reload #1's reap is waiting out.
                routes_path.write_text(
                    """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")

                await asyncio.sleep(0.05)
                self.assertEqual(harness.profile.released_session_ids, [])
                self.assertEqual(harness.supervisor.retired, [])
            finally:
                await turn_task

            # r2's turn completed and delivered its reply despite two reloads.
            self.assertEqual(harness.signal.sends, [("group-two", "reply")])
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            if tasks:
                await asyncio.gather(*tasks)
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertCountEqual(harness.supervisor.retired, ["p1", "p2"])
            self.assertEqual(harness.router.sessions._sessions, {})

    async def test_reload_config_reap_does_not_wait_on_other_reapers(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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

            route = harness.router.config.find_route_by_name("r1")
            assert route is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # A still-running reaper from an earlier reload. It is tracked as
            # a signal task (shutdown drain coverage) AND as a reaper, so the
            # new reap must not treat it as a pre-swap turn and park on it
            # until the drain bound expires.
            lingering = asyncio.ensure_future(asyncio.sleep(30))
            harness.router._signal_turn_tasks.add(lingering)
            harness.router._reap_tasks.add(lingering)
            lingering.add_done_callback(harness.router._settle_tracked_task)
            try:
                # A long drain bound: if the new reap waited on the lingering
                # reaper, it would blow the wait_for budget below.
                with patch.object(harness.router, "_reap_drain_seconds", return_value=30.0):
                    routes_path.write_text(
                        """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                        encoding="utf-8",
                    )
                    response = await harness.router._handle_reload_config_control({})
                    self.assertEqual(response["status"], "ok")
                    reapers = [
                        t for t in harness.router._reap_tasks if t is not lingering and not t.done()
                    ]
                    self.assertEqual(len(reapers), 1)
                    await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            finally:
                lingering.cancel()
                await asyncio.sleep(0)
                await asyncio.sleep(0)

            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p1"])
            # Settling discarded the cancelled lingering reaper from every set.
            self.assertNotIn(lingering, harness.router._reap_tasks)
            self.assertNotIn(lingering, harness.router._signal_turn_tasks)

    async def test_reload_config_back_to_back_reaps_complete(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            from signal_hermes_router.sessions import RoutedSession

            for name, profile_name, session_id in (
                ("r1", "p1", "session-1"),
                ("r2", "p2", "session-2"),
            ):
                route = harness.router.config.find_route_by_name(name)
                assert route is not None
                harness.supervisor.cached.append(profile_name)
                harness.router.sessions._sessions[session_id] = RoutedSession(
                    profile=harness.profile,  # type: ignore[arg-type]
                    session_id=session_id,
                    cwd=Path(tmp) / "work" / session_id,
                )
                harness.router.sessions._session_routes[session_id] = route.key

            with patch.object(harness.router, "_reap_drain_seconds", return_value=30.0):
                # Reload #1 disables r1, reload #2 disables r2. Each schedules
                # its own reaper; both must finish promptly rather than
                # waiting on each other up to the drain bound.
                routes_path.write_text(
                    """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                routes_path.write_text(
                    """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: disabled
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")

                tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
                self.assertTrue(tasks)
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertCountEqual(harness.profile.released_session_ids, ["session-1", "session-2"])
            self.assertCountEqual(harness.supervisor.retired, ["p1", "p2"])

    async def test_reload_config_reap_rechecks_profile_liveness_before_retiring(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: old-route
    platform: signal
    group_id: group-one
    profile: p-old
    state: active
  - name: r1
    platform: signal
    group_id: group-two
    profile: p1
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            harness.supervisor.cached.extend(["p-old", "p1"])

            # Reload #1 removes old-route and disables r1: neither profile
            # has an active route, so the reaper will retire both.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-two
    profile: p1
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            # While the reaper is retiring p-old, a later reload re-activates
            # r1. The reaper must re-check liveness against the CURRENT
            # config instead of its pre-drain snapshot, or it would retire a
            # profile that is live again.
            original_retire = harness.supervisor.retire_profile
            reactivated = False

            async def retire_with_concurrent_reload(profile_name: str, **kwargs: Any) -> bool:
                nonlocal reactivated
                if not reactivated:
                    reactivated = True
                    routes_path.write_text(
                        """
routes:
  - name: r1
    platform: signal
    group_id: group-two
    profile: p1
    state: active
""",
                        encoding="utf-8",
                    )
                    reload_response = await harness.router._handle_reload_config_control({})
                    assert reload_response["status"] == "ok"
                return await original_retire(profile_name, **kwargs)

            harness.supervisor.retire_profile = retire_with_concurrent_reload  # type: ignore[method-assign]
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            self.assertTrue(tasks)
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

            self.assertTrue(reactivated)
            self.assertEqual(harness.supervisor.retired, ["p-old"])
            self.assertEqual(harness.supervisor.cached, ["p1"])

    async def test_reload_config_removed_route_clears_breaker_override(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("breaker-route")
            assert route is not None
            for _ in range(3):
                harness.router.circuit.record_failure(route.key)
            harness.router.route_state_overrides[route.key] = RouteState.MAINTENANCE
            harness.router._trip_times[route.key] = time.monotonic()
            harness.router._trip_times_ms[route.key] = harness.router._clock_ms()

            # Reload REMOVES the route: it never appears in the override
            # clearing loop, but its stale MAINTENANCE override must not
            # survive either.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertIsNone(harness.router.route_state_overrides.get(route.key))
            self.assertNotIn(route.key, harness.router._trip_times)
            self.assertNotIn(route.key, harness.router._trip_times_ms)

            # Re-added active before the cooldown expires, the route prompts
            # normally instead of answering with maintenance replies.
            routes_path.write_text(
                """
routes:
  - name: breaker-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            await harness.router.handle_event(make_event(group_id="group-one", timestamp=1))
            self.assertEqual(harness.signal.sends, [("group-one", "reply")])

    async def test_reload_config_reap_evicts_only_drained_route_keys(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            harness.profile.prompt_delay = 0.3

            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            # Hold r1's route lock so reload #1's reaper parks in its drain
            # while later events unfold.
            await harness.router._route_locks[r1.key].acquire()

            # Reload #1 disables r1; its reaper drains {r1} and parks.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            await asyncio.sleep(0.05)
            reaper_a = next(iter(harness.router._reap_tasks))
            self.assertFalse(reaper_a.done())

            # A turn starts on r2 (still active) AFTER reaper A snapshot its
            # pending-turn set, so A never waits for it.
            turn_task = asyncio.create_task(
                harness.router.handle_event(make_event(group_id="group-two", timestamp=1))
            )
            for _ in range(100):
                if harness.profile.prompts:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(harness.profile.prompts)

            # Reload #2 disables r2; its own reaper waits out the r2 turn.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reaper_b = next(t for t in harness.router._reap_tasks if t is not reaper_a)

            # Releasing r1's lock lets reaper A finish its drain. Its
            # revalidation sees BOTH routes non-live, but it must evict only
            # the keys it actually drained — r2's session belongs to reaper
            # B, which is still waiting out the in-flight turn.
            harness.router._route_locks[r1.key].release()
            await asyncio.wait_for(asyncio.gather(reaper_a), timeout=5)
            self.assertFalse(turn_task.done())
            self.assertEqual(harness.profile.released_session_ids, [])

            await turn_task
            self.assertEqual(harness.signal.sends, [("group-two", "reply")])
            await asyncio.wait_for(asyncio.gather(reaper_b), timeout=5)
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.router.sessions._sessions, {})

    async def test_reload_config_reap_retires_only_drained_profile_routes(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            # Both routes share one Hermes profile subprocess.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p
    state: active
  - name: r2
    platform: signal
    group_id: group-two
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            harness.profile.prompt_delay = 0.3
            harness.supervisor.cached.append("p")

            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            await harness.router._route_locks[r1.key].acquire()

            # Reload #1 disables r1; its reaper drains {r1} and parks.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            await asyncio.sleep(0.05)
            reaper_a = next(iter(harness.router._reap_tasks))

            # A turn starts on r2 (still active) after reaper A snapshot its
            # pending-turn set; the shared profile subprocess serves it.
            turn_task = asyncio.create_task(
                harness.router.handle_event(make_event(group_id="group-two", timestamp=1))
            )
            for _ in range(100):
                if harness.profile.prompts:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(harness.profile.prompts)

            # Reload #2 disables r2; its own reaper waits out the r2 turn.
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reaper_b = next(t for t in harness.router._reap_tasks if t is not reaper_a)

            # Reaper A finishes its drain with r2 still un-drained by it: it
            # must NOT retire the shared profile out from under the in-flight
            # r2 turn — that is reaper B's job after the turn drains.
            harness.router._route_locks[r1.key].release()
            await asyncio.wait_for(asyncio.gather(reaper_a), timeout=5)
            self.assertFalse(turn_task.done())
            self.assertEqual(harness.supervisor.retired, [])
            self.assertEqual(harness.supervisor.cached, ["p"])

            await turn_task
            self.assertEqual(harness.signal.sends, [("group-two", "reply")])
            await asyncio.wait_for(asyncio.gather(reaper_b), timeout=5)
            self.assertEqual(harness.supervisor.retired, ["p"])
            self.assertEqual(harness.supervisor.cached, [])

    async def test_reload_config_reap_ignores_idle_control_connection(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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

            route = harness.router.config.find_route_by_name("r1")
            assert route is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # An idle keep-alive control CONNECTION (no request in flight) is
            # not turn work: the reap must not wait for it up to the drain
            # bound. A request task, by contrast, IS waited out.
            idle_connection = asyncio.ensure_future(asyncio.sleep(30))
            harness.router._control_client_tasks.add(idle_connection)
            idle_connection.add_done_callback(harness.router._settle_tracked_task)
            try:
                with patch.object(harness.router, "_reap_drain_seconds", return_value=30.0):
                    routes_path.write_text(
                        """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                        encoding="utf-8",
                    )
                    response = await harness.router._handle_reload_config_control({})
                    self.assertEqual(response["status"], "ok")
                    reapers = [t for t in harness.router._reap_tasks if not t.done()]
                    self.assertEqual(len(reapers), 1)
                    await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            finally:
                idle_connection.cancel()
                await asyncio.sleep(0)
                await asyncio.sleep(0)

            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p1"])

    async def test_reload_config_reap_waits_for_inflight_control_request(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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

            route = harness.router.config.find_route_by_name("r1")
            assert route is not None
            harness.supervisor.cached.append("p1")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # An in-flight control REQUEST (e.g. a notify-route turn still in
            # its pre-lock awaits) must be waited out before the reap evicts.
            request_done = asyncio.Event()

            async def slow_request() -> None:
                await asyncio.sleep(0.3)
                request_done.set()

            request_task = asyncio.ensure_future(slow_request())
            harness.router._control_request_tasks.add(request_task)
            request_task.add_done_callback(harness.router._settle_tracked_task)

            started = time.monotonic()
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertEqual(len(reapers), 1)
            await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertTrue(request_done.is_set())
            self.assertGreaterEqual(time.monotonic() - started, 0.25)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])

    async def test_reload_config_parse_timeout_returns_error(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate

            def hung_parse(*args: Any, **kwargs: Any) -> Any:
                # A wedged secret resolver or filesystem read.
                time.sleep(1.0)
                return real_parse(*args, **kwargs)

            with (
                patch.object(router_module, "_parse_reload_candidate", hung_parse),
                patch.object(router_module, "RELOAD_PARSE_TIMEOUT_SECONDS", 0.2),
            ):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "error")
                self.assertEqual(response["error"], "config_parse_timeout")
                self.assertEqual(response["generation"], 0)
                # The parse worker was not cancelled by the timeout: it stays
                # tracked so close() can observe it, and finishes on its own.
                futures = list(harness.router._io_worker_futures)
                self.assertTrue(futures)
                await asyncio.sleep(1.0)
                self.assertTrue(all(future.done() for future in futures))
            # The reload lock was released: a later reload proceeds normally.
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

    async def test_reload_config_late_breaker_trip_does_not_mask_reloaded_state(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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

            route = harness.router.config.find_route_by_name("r1")
            assert route is not None
            # One failure short of the trip threshold.
            harness.router.circuit.record_failure(route.key)
            harness.router.circuit.record_failure(route.key)

            # A turn admitted while the route is active fails slowly; the
            # reload disabling the route lands before the failure handler
            # runs, so the trip must not mask the reloaded DISABLED state.
            harness.profile.fail = True
            harness.profile.prompt_delay = 0.2
            turn_task = asyncio.create_task(
                harness.router.handle_event(make_event(group_id="group-one", timestamp=1))
            )
            for _ in range(100):
                if harness.profile.prompts:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(harness.profile.prompts)
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            await turn_task
            # The failure reply still went out (the maintenance text on
            # trip), but no MAINTENANCE override survived to mask the
            # reloaded state.
            self.assertEqual(
                harness.signal.sends,
                [("group-one", "This route is temporarily under maintenance.")],
            )
            self.assertIsNone(harness.router.route_state_overrides.get(route.key))
            self.assertNotIn(route.key, harness.router._trip_times)

            # Flip back to active: another failure trips the breaker for
            # real (the window still holds the earlier failures), proving
            # the guard only blocks non-active routes.
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
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            await harness.router.handle_event(make_event(group_id="group-one", timestamp=2))
            self.assertEqual(
                harness.router.route_state_overrides.get(route.key),
                RouteState.MAINTENANCE,
            )

    async def test_reload_config_reap_timeout_evicts_only_actually_drained_keys(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
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
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            from signal_hermes_router.sessions import RoutedSession

            r1 = harness.router.config.find_route_by_name("r1")
            r2 = harness.router.config.find_route_by_name("r2")
            assert r1 is not None and r2 is not None
            for route, profile_name, session_id in (
                (r1, "p1", "session-1"),
                (r2, "p2", "session-2"),
            ):
                harness.supervisor.cached.append(profile_name)
                harness.router.sessions._sessions[session_id] = RoutedSession(
                    profile=harness.profile,  # type: ignore[arg-type]
                    session_id=session_id,
                    cwd=Path(tmp) / "work" / session_id,
                )
                harness.router.sessions._session_routes[session_id] = route.key

            # A wedged in-flight turn holds r2's lock past the drain bound.
            # r1 sorts first ("signal:group-one" < "signal:group-two"), so
            # the drain reaches it before the wedge.
            await harness.router._route_locks[r2.key].acquire()
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: disabled
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: disabled
""",
                encoding="utf-8",
            )
            with patch.object(harness.router, "_reap_drain_seconds", return_value=0.2):
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                self.assertEqual(len(reapers), 1)
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

            # Partial drain: r1 was actually drained, so its session and
            # profile are reaped; the drain timed out on r2's lock, so r2's
            # healthy session and profile must NOT pay for the wedge.
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(set(harness.router.sessions._sessions), {"session-2"})
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.supervisor.cached, ["p2"])

            # Once the wedge clears, a later reload's reaper drains r2 and
            # completes the cleanup.
            harness.router._route_locks[r2.key].release()
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertEqual(len(reapers), 1)
            await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertCountEqual(harness.profile.released_session_ids, ["session-1", "session-2"])
            self.assertCountEqual(harness.supervisor.retired, ["p1", "p2"])

    async def test_reload_config_serializes_concurrent_reloads(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: new-route
    platform: signal
    group_id: new-group
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            # A second reload cannot start its parse while one is in flight:
            # without serialization the slower parse could swap its older
            # candidate over the newer config.
            await harness.router._reload_lock.acquire()
            task = asyncio.create_task(harness.router._handle_reload_config_control({}))
            try:
                await asyncio.sleep(0.05)
                self.assertFalse(task.done())
            finally:
                harness.router._reload_lock.release()
            response = await task
            self.assertEqual(response["status"], "ok")

    async def test_reload_config_rejection_logging_omits_traceback_and_identifiers(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            # A duplicate route key makes validation fail with a message that
            # embeds the raw signal:<group_id> identifier.
            routes_path.write_text(
                """
routes:
  - name: dup-one
    platform: signal
    group_id: dup-group
    profile: p
    state: active
  - name: dup-two
    platform: signal
    group_id: dup-group
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as captured:
                response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "config_invalid")
            # The rejected candidate's identifiers were never registered with
            # the redactor, so no log record may carry a traceback or the raw
            # identifier at any level.
            for record in captured.records:
                self.assertIsNone(record.exc_info)
                self.assertNotIn("dup-group", record.getMessage())

    async def test_reload_config_reaps_late_session_from_pre_lock_turn(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: late-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("late-route")
            assert route is not None
            # A turn admitted just before the swap still holds the route lock
            # through its pre-session awaits.
            lock = harness.router._route_locks[route.key]
            await lock.acquire()
            try:
                routes_path.write_text(
                    """
routes:
  - name: late-route
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                # Nothing is cached yet, but the reap must still be scheduled:
                # only its post-drain re-validation can catch the session the
                # admitted turn is about to create.
                tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
                self.assertEqual(len(tasks), 1)
                await asyncio.sleep(0.02)
                # The admitted turn creates its session and profile while the
                # reap is still waiting on the lock.
                harness.supervisor.cached.append("p")
                from signal_hermes_router.sessions import RoutedSession

                harness.router.sessions._sessions["sk"] = RoutedSession(
                    profile=harness.profile,  # type: ignore[arg-type]
                    session_id="session-1",
                    cwd=Path(tmp) / "work" / "sk",
                )
                harness.router.sessions._session_routes["sk"] = route.key
            finally:
                lock.release()

            await asyncio.gather(*tasks)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p"])

    async def test_reload_config_evicts_sessions_on_session_policy_change(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: policy-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("policy-route")
            assert route is not None
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # The route stays active but switches to ephemeral sessions: the
            # persistent session cached under the old keying is unreachable.
            routes_path.write_text(
                """
routes:
  - name: policy-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
    session_policy: ephemeral
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            self.assertEqual(len(tasks), 1)
            await asyncio.gather(*tasks)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            reloaded = harness.router.config.find_route_by_name("policy-route")
            self.assertEqual(reloaded.state, RouteState.ACTIVE)
            self.assertEqual(reloaded.session_policy, SessionPolicy.EPHEMERAL)
            # The route is still active: the profile is not retired.
            self.assertEqual(harness.supervisor.retired, [])

    async def test_reload_config_policy_flip_flop_keeps_session(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: flip-policy-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("flip-policy-route")
            assert route is not None
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            # Park the reap behind a held route lock (an in-flight turn).
            lock = harness.router._route_locks[route.key]
            await lock.acquire()
            try:
                # Reload #1 switches to ephemeral; the reap parks on the lock.
                routes_path.write_text(
                    """
routes:
  - name: flip-policy-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
    session_policy: ephemeral
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
                await asyncio.sleep(0.02)

                # Reload #2 flips back before the drain completes: the cached
                # session is reachable under the restored keying again.
                routes_path.write_text(
                    """
routes:
  - name: flip-policy-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                    encoding="utf-8",
                )
                response = await harness.router._handle_reload_config_control({})
                self.assertEqual(response["status"], "ok")
            finally:
                lock.release()

            tasks = [t for t in harness.router._signal_turn_tasks if not t.done()]
            if tasks:
                await asyncio.gather(*tasks)
            self.assertEqual(list(harness.router.sessions._sessions), ["sk"])
            self.assertEqual(harness.profile.released_session_ids, [])
            reloaded = harness.router.config.find_route_by_name("flip-policy-route")
            self.assertEqual(reloaded.session_policy, SessionPolicy.PERSISTENT_ROUTE)

    async def test_reload_config_reap_waits_for_tracked_pre_lock_turn(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: pre-lock-route
    platform: signal
    group_id: group-one
    profile: p
    state: active
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            route = harness.router.config.find_route_by_name("pre-lock-route")
            assert route is not None
            # An admitted notify-route turn with attachments is tracked from
            # spawn but does not hold the route lock during its pre-lock
            # dedupe/freeze awaits.
            blocker = asyncio.ensure_future(asyncio.sleep(0.2))
            harness.router._signal_turn_tasks.add(blocker)
            blocker.add_done_callback(harness.router._settle_tracked_task)

            routes_path.write_text(
                """
routes:
  - name: pre-lock-route
    platform: signal
    group_id: group-one
    profile: p
    state: disabled
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reap_tasks = [
                t for t in harness.router._signal_turn_tasks if not t.done() and t is not blocker
            ]
            self.assertEqual(len(reap_tasks), 1)
            # The reap must still be waiting for the tracked turn, not
            # reaping around it.
            await asyncio.sleep(0.05)
            self.assertFalse(reap_tasks[0].done())
            # The turn creates its session/profile at the end of its
            # pre-lock window.
            harness.supervisor.cached.append("p")
            from signal_hermes_router.sessions import RoutedSession

            harness.router.sessions._sessions["sk"] = RoutedSession(
                profile=harness.profile,  # type: ignore[arg-type]
                session_id="session-1",
                cwd=Path(tmp) / "work" / "sk",
            )
            harness.router.sessions._session_routes["sk"] = route.key

            await asyncio.gather(blocker, *reap_tasks)
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p"])

    async def test_reload_config_tracks_parse_worker(self) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: new-route
    platform: signal
    group_id: new-group
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate

            def slow_parse(*args: Any, **kwargs: Any) -> Any:
                time.sleep(0.2)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", slow_parse):
                task = asyncio.create_task(harness.router._handle_reload_config_control({}))
                await asyncio.sleep(0.05)
                # The blocking parse runs in a router-tracked I/O worker so
                # close() can observe it even if the control task is cancelled.
                self.assertTrue(
                    any(not future.done() for future in harness.router._io_worker_futures)
                )
                response = await task
            self.assertEqual(response["status"], "ok")
            self.assertTrue(all(future.done() for future in harness.router._io_worker_futures))

    async def test_reload_config_cancelled_control_task_keeps_parse_worker_tracked(
        self,
    ) -> None:
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
                "  allow_remote_signal_base_url: false\n"
                "  circuit_breaker:\n"
                "    failures: 3\n"
                "    window_seconds: 60\n",
                encoding="utf-8",
            )
            routes_path.write_text(
                """
routes:
  - name: new-route
    platform: signal
    group_id: new-group
    profile: new-profile
    state: active
""",
                encoding="utf-8",
            )
            harness = make_router_harness(tmp)
            harness.router.set_config_paths(config_path, routes_path)

            real_parse = router_module._parse_reload_candidate

            def slow_parse(*args: Any, **kwargs: Any) -> Any:
                time.sleep(0.3)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", slow_parse):
                task = asyncio.create_task(harness.router._handle_reload_config_control({}))
                await asyncio.sleep(0.05)
                futures = [
                    future for future in harness.router._io_worker_futures if not future.done()
                ]
                self.assertTrue(futures)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                # Cancelling the control task must not cancel or untrack the
                # still-running parse worker: close() relies on the tracked
                # future to observe it during the bounded shutdown.
                self.assertTrue(all(not future.cancelled() for future in futures))
                self.assertTrue(
                    any(future in harness.router._io_worker_futures for future in futures)
                )
                await asyncio.sleep(0.4)
                self.assertTrue(all(future.done() for future in futures))
            # The cancelled reload never swapped the configuration.
            self.assertEqual(harness.router._config_generation, 0)
