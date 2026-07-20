from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from signal_hermes_router import router as router_module
from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterControlConfig,
    SyntheticRouteNotification,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    RouteState,
    SessionPolicy,
    TurnResult,
    TurnOutcomeStatus,
)
from signal_hermes_router.payloads import canonicalize_notification_payload, encode_control_message
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.sessions import ProfileSupervisor
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_event,
    make_group_raw,
    make_router_harness,
    make_synthetic_app,
    RouterTestCase,
    ClosedAwareSignal,
)


def _shutdown_route() -> Route:
    return Route(
        platform="signal",
        name="agenda-route",
        group_id="group",
        profile="profile",
        session_policy=SessionPolicy.PERSISTENT_ROUTE,
        state=RouteState.ACTIVE,
    )


def _notification_app(tmp: str | Path, **kwargs: Any) -> AppConfig:
    return make_synthetic_app(
        tmp,
        _shutdown_route(),
        notifications=(
            SyntheticRouteNotification(
                id="backup-report",
                route_name="agenda-route",
                prompt="Summarize the notification payload.",
            ),
        ),
        **kwargs,
    )


class RouterShutdownTests(RouterTestCase):

    async def _settle(self, rounds: int = 10) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)
    async def test_close_closes_signal_supervisor_and_dedupe(self) -> None:
        class CloseSignal(FakeSignal):
            def __init__(self) -> None:
                super().__init__()
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        class CloseSupervisor(FakeSupervisor):
            def __init__(self) -> None:
                super().__init__(FakeProfile())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        class CloseDedupe:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp:
            signal = CloseSignal()
            supervisor = CloseSupervisor()
            dedupe = CloseDedupe()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=dedupe,  # type: ignore[arg-type]
            )

            await router.close()

            self.assertTrue(signal.closed)
            self.assertTrue(supervisor.closed)
            self.assertTrue(dedupe.closed)

    async def test_close_waits_for_in_flight_turn_before_releasing_state_db(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()

        class GatedProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                started.set()
                await gate.wait()
                return await super().prompt(session_id, blocks)

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                route_context={"purpose": "synthetic", "route_alias": "agenda-route"},
            )
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(
                    SyntheticRouteNotification(
                        id="backup-report",
                        route_name="agenda-route",
                        prompt="Summarize the notification payload.",
                    ),
                ),
            )
            payload = canonicalize_notification_payload({"status": "ok"}, max_bytes=1024)
            dedupe_path = Path(tmp) / "dedupe.db"
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(GatedProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(dedupe_path),
                clock_ms=lambda: 1714521600100,
            )
            turn = asyncio.create_task(
                router.handle_notification(
                    "backup-report",
                    payload,
                    idempotency_key="backup-1714521600",
                )
            )
            await started.wait()

            close_task = asyncio.create_task(router.close())
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertFalse(close_task.done())

            gate.set()
            outcome = await turn
            await close_task
            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)

            inspect = sqlite3.connect(dedupe_path)
            try:
                rows = dict(
                    inspect.execute(
                        "SELECT status, COUNT(*) FROM dedupe_events GROUP BY status"
                    ).fetchall()
                )
            finally:
                inspect.close()
            self.assertEqual(rows, {"handled": 1})

    async def test_close_drains_reaper_scheduled_by_inflight_reload(self) -> None:
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

            # The candidate disables r1; a slow parse keeps the reload
            # request in flight when shutdown begins, so its reaper is
            # scheduled only after close()'s first task snapshot.
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
            real_parse = router_module._parse_reload_candidate

            def slow_parse(*args: Any, **kwargs: Any) -> Any:
                time.sleep(0.3)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "_parse_reload_candidate", slow_parse):
                reload_req = asyncio.ensure_future(harness.router._handle_reload_config_control({}))
                harness.router._control_request_tasks.add(reload_req)
                reload_req.add_done_callback(harness.router._settle_tracked_task)

                close_task = asyncio.create_task(harness.router.close())
                await asyncio.sleep(0.1)
                # A turn admitted after the first snapshot: the reaper waits
                # on it, so only an explicit reap drain after the
                # control-request drain lets close() return with the cleanup
                # actually finished.
                blocker = asyncio.ensure_future(asyncio.sleep(1.0))
                harness.router._signal_turn_tasks.add(blocker)
                blocker.add_done_callback(harness.router._settle_tracked_task)

                await asyncio.wait_for(close_task, timeout=15)

            self.assertTrue(reload_req.done())
            self.assertEqual(harness.router.sessions._sessions, {})
            self.assertEqual(harness.profile.released_session_ids, ["session-1"])
            self.assertEqual(harness.supervisor.retired, ["p1"])
            self.assertEqual(harness.router._reap_tasks, set())

    async def _settle(self, rounds: int = 10) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)

    async def test_close_drains_in_flight_turn_before_closing_dependencies(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()
        with tempfile.TemporaryDirectory() as tmp:
            signal = ClosedAwareSignal()
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate)),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            payload = canonicalize_notification_payload({"status": "ok"}, max_bytes=1024)
            turn = asyncio.create_task(
                router.handle_notification("backup-report", payload, idempotency_key="k-1")
            )
            await asyncio.wait_for(started.wait(), timeout=1)

            close_task = asyncio.create_task(router.close())
            await self._settle()
            self.assertFalse(close_task.done())

            gate.set()
            outcome = await asyncio.wait_for(turn, timeout=5)
            incomplete = await asyncio.wait_for(close_task, timeout=5)

            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(incomplete, ())
            self.assertTrue(signal.closed)

    async def test_signal_origin_turn_survives_cancellation_and_close_drains_it(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()

        class StreamingSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(text="hello", timestamp=42)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = StreamingSignal()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate)),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(started.wait(), timeout=1)

            run_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run_task

            close_task = asyncio.create_task(router.close())
            await self._settle()
            self.assertFalse(close_task.done())

            gate.set()
            incomplete = await asyncio.wait_for(close_task, timeout=5)

            self.assertEqual(incomplete, ())
            self.assertEqual(signal.sends, [("group", "reply")])

    async def test_direct_close_fences_signal_intake_and_run_forever_declines(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()

        class StreamingSignal(ClosedAwareSignal):
            async def events(self):
                yield make_group_raw(text="hello", timestamp=42)
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            signal = StreamingSignal()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate)),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            run_task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(started.wait(), timeout=1)

            close_task = asyncio.create_task(router.close())
            # Direct close (no external cancellation) fences intake itself:
            # run_forever returns cleanly instead of raising.
            await asyncio.wait_for(run_task, timeout=5)
            gate.set()
            incomplete = await asyncio.wait_for(close_task, timeout=5)

            self.assertEqual(incomplete, ())
            self.assertEqual(signal.sends, [("group", "reply")])
            with self.assertRaisesRegex(RuntimeError, "shutting down"):
                await router.run_forever()

    async def test_queued_turns_admitted_before_shutdown_complete(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()
        with tempfile.TemporaryDirectory() as tmp:
            signal = ClosedAwareSignal()
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate, gate_first_only=True)),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            payload = canonicalize_notification_payload({"status": "ok"}, max_bytes=1024)
            first = asyncio.create_task(
                router.handle_notification("backup-report", payload, idempotency_key="k-1")
            )
            await asyncio.wait_for(started.wait(), timeout=1)

            # Unchanged pre-shutdown semantics: a zero-timeout waiter is busy.
            busy = await router.handle_notification(
                "backup-report", payload, idempotency_key="k-busy", route_lock_timeout=0
            )
            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)

            queued_control = asyncio.create_task(
                router.handle_notification(
                    "backup-report", payload, idempotency_key="k-2", route_lock_timeout=30
                )
            )
            queued_signal = asyncio.create_task(router.handle_event(make_event(timestamp=77)))
            await self._settle()

            router.begin_shutdown()
            close_task = asyncio.create_task(router.close())
            await self._settle()
            self.assertFalse(close_task.done())

            gate.set()
            first_outcome = await asyncio.wait_for(first, timeout=5)
            control_outcome = await asyncio.wait_for(queued_control, timeout=5)
            signal_result = await asyncio.wait_for(queued_signal, timeout=5)
            incomplete = await asyncio.wait_for(close_task, timeout=5)

            self.assertEqual(first_outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(control_outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(signal_result, TurnResult("reply"))
            self.assertEqual(incomplete, ())
            self.assertEqual(len(signal.sends), 3)

    async def test_control_socket_gate_drain_and_refused_new_connections(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            router = SignalHermesRouter(
                _notification_app(
                    tmp,
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate)),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            server_task = asyncio.create_task(router._run_control_server())
            for _ in range(50):
                if socket_path.exists():
                    break
                await asyncio.sleep(0.01)

            reader_a, writer_a = await asyncio.open_unix_connection(str(socket_path))
            reader_b, writer_b = await asyncio.open_unix_connection(str(socket_path))
            try:
                writer_a.write(
                    encode_control_message(
                        {
                            "command": "notify_route",
                            "notification_id": "backup-report",
                            "payload": {"status": "ok"},
                        }
                    )
                )
                await writer_a.drain()
                await asyncio.wait_for(started.wait(), timeout=1)

                close_task = asyncio.create_task(router.close())
                await self._settle()
                self.assertFalse(close_task.done())

                # A preconnected client's new request is gated with busy.
                writer_b.write(
                    encode_control_message({"command": "trigger_job", "job_id": "daily-agenda"})
                )
                await writer_b.drain()
                response_b = json.loads(
                    (await asyncio.wait_for(reader_b.readline(), timeout=5)).decode("utf-8")
                )
                self.assertEqual(response_b["status"], "busy")
                self.assertEqual(response_b["error"], "router_shutting_down")

                # Brand-new connections are refused: the listener is closed
                # and the socket file removed.
                with self.assertRaises(OSError):
                    await asyncio.open_unix_connection(str(socket_path))

                gate.set()
                response_a = json.loads(
                    (await asyncio.wait_for(reader_a.readline(), timeout=5)).decode("utf-8")
                )
                self.assertEqual(response_a["status"], "delivered")

                writer_a.close()
                writer_b.close()
                incomplete = await asyncio.wait_for(close_task, timeout=10)
                self.assertEqual(incomplete, ())
                await asyncio.wait_for(server_task, timeout=5)
                self.assertFalse(socket_path.exists())
            finally:
                for writer in (writer_a, writer_b):
                    writer.close()
                    with suppress(Exception):
                        await writer.wait_closed()
                gate.set()
                if not server_task.done():
                    server_task.cancel()
                with suppress(asyncio.CancelledError):
                    await server_task

    async def test_close_drain_deadline_cancels_straggling_control_turn(self) -> None:
        started = asyncio.Event()
        gate = asyncio.Event()
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            dedupe_path = Path(tmp) / "dedupe.db"
            router = SignalHermesRouter(
                _notification_app(
                    tmp,
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile(gate_started=started, gate_wait=gate)),  # type: ignore[arg-type]
                dedupe=DedupeStore(dedupe_path),
            )
            server_task = asyncio.create_task(router._run_control_server())
            for _ in range(50):
                if socket_path.exists():
                    break
                await asyncio.sleep(0.01)

            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            try:
                writer.write(
                    encode_control_message(
                        {
                            "command": "notify_route",
                            "notification_id": "backup-report",
                            "payload": {"status": "ok"},
                        }
                    )
                )
                await writer.drain()
                await asyncio.wait_for(started.wait(), timeout=1)

                incomplete = await asyncio.wait_for(router.close(drain_timeout=0.05), timeout=10)
                self.assertEqual(incomplete, ())
                # The cancelled handler closed the connection without a reply.
                self.assertEqual(await asyncio.wait_for(reader.readline(), timeout=5), b"")
                await asyncio.wait_for(server_task, timeout=5)
            finally:
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
                gate.set()
                if not server_task.done():
                    server_task.cancel()
                with suppress(asyncio.CancelledError):
                    await server_task

            inspect = sqlite3.connect(dedupe_path)
            try:
                rows = inspect.execute("SELECT COUNT(*) FROM dedupe_events").fetchone()[0]
            finally:
                inspect.close()
            self.assertEqual(rows, 0)

    async def test_close_abandons_unsettleable_turn_and_still_closes_dedupe(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class ResistantProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                started.set()
                while True:
                    try:
                        await release.wait()
                        return await super().prompt(session_id, blocks)
                    except asyncio.CancelledError:
                        continue

        with tempfile.TemporaryDirectory() as tmp:
            dedupe_path = Path(tmp) / "dedupe.db"
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ResistantProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(dedupe_path),
            )
            payload = canonicalize_notification_payload({"status": "ok"}, max_bytes=1024)
            turn = asyncio.create_task(
                router.handle_notification("backup-report", payload, idempotency_key="k-1")
            )
            router._signal_turn_tasks.add(turn)
            turn.add_done_callback(router._settle_tracked_task)
            await asyncio.wait_for(started.wait(), timeout=1)

            try:
                with (
                    patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.1),
                    patch("signal_hermes_router.router.SHUTDOWN_SUPERVISOR_FLOOR_SECONDS", 0.1),
                ):
                    close_started = time.monotonic()
                    incomplete = await asyncio.wait_for(
                        router.close(drain_timeout=0.05), timeout=10
                    )
                    elapsed = time.monotonic() - close_started

                self.assertEqual(incomplete, (turn,))
                self.assertFalse(turn.done())
                self.assertLess(elapsed, 5.0)
                # The exclusive state-DB lock is released underneath the
                # abandoned turn: a replacement store can open the same DB.
                replacement = DedupeStore(dedupe_path)
                replacement.close()
            finally:
                release.set()
                with suppress(Exception):
                    await asyncio.wait_for(turn, timeout=5)

    async def test_close_reports_resistant_supervisor_close_incomplete(self) -> None:
        release = asyncio.Event()

        class ResistantSupervisor(FakeSupervisor):
            async def close(self) -> None:
                while True:
                    try:
                        await release.wait()
                        return
                    except asyncio.CancelledError:
                        continue

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=ResistantSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            try:
                with (
                    patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.1),
                    patch("signal_hermes_router.router.SHUTDOWN_SUPERVISOR_FLOOR_SECONDS", 0.1),
                    patch(
                        "signal_hermes_router.router.SHUTDOWN_CLEANUP_CANCEL_GRACE_SECONDS",
                        0.1,
                    ),
                ):
                    incomplete = await asyncio.wait_for(
                        router.close(drain_timeout=0.05), timeout=10
                    )
                self.assertEqual(len(incomplete), 1)
                self.assertFalse(incomplete[0].done())
            finally:
                release.set()
                for task in incomplete:
                    with suppress(Exception):
                        await asyncio.wait_for(task, timeout=5)

    async def test_close_reports_cancelled_supervisor_close_incomplete(self) -> None:
        class BlockedSupervisor(FakeSupervisor):
            async def close(self) -> None:
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=BlockedSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with (
                patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.1),
                patch("signal_hermes_router.router.SHUTDOWN_SUPERVISOR_FLOOR_SECONDS", 0.1),
            ):
                incomplete = await asyncio.wait_for(router.close(drain_timeout=0.05), timeout=10)
            self.assertEqual(len(incomplete), 1)
            self.assertTrue(incomplete[0].cancelled())

    async def test_close_runs_all_phases_and_closes_dedupe_when_cleanup_fails(self) -> None:
        class FailingWaitClosedServer:
            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                raise RuntimeError("wait_closed failed")

        class FailingSignal(ClosedAwareSignal):
            async def close(self) -> None:
                raise RuntimeError("signal close failed")

        class RecordingSupervisor(FakeSupervisor):
            def __init__(self, profile: FakeProfile) -> None:
                super().__init__(profile)
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp:
            dedupe_path = Path(tmp) / "dedupe.db"
            supervisor = RecordingSupervisor(FakeProfile())
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=FailingSignal(),  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(dedupe_path),
            )
            router._control_server = FailingWaitClosedServer()  # type: ignore[assignment]
            with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                incomplete = await asyncio.wait_for(router.close(), timeout=10)

            self.assertEqual(len(incomplete), 2)
            self.assertTrue(supervisor.closed)
            output = "\n".join(logs.output)
            self.assertIn("control server close failed", output)
            self.assertIn("Signal client close failed", output)
            replacement = DedupeStore(dedupe_path)
            replacement.close()

    async def test_close_reports_failed_supervisor_close_incomplete(self) -> None:
        class FailingSupervisor(FakeSupervisor):
            async def close(self) -> None:
                raise RuntimeError("profile close failed")

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FailingSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                incomplete = await asyncio.wait_for(router.close(), timeout=10)
            self.assertEqual(len(incomplete), 1)

    async def test_close_bounds_resistant_server_and_signal_cleanup(self) -> None:
        release = asyncio.Event()

        class ResistantWaitClosedServer:
            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                while True:
                    try:
                        await release.wait()
                        return
                    except asyncio.CancelledError:
                        continue

        class ResistantSignal(ClosedAwareSignal):
            async def close(self) -> None:
                while True:
                    try:
                        await release.wait()
                        return
                    except asyncio.CancelledError:
                        continue

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ResistantSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            router._control_server = ResistantWaitClosedServer()  # type: ignore[assignment]
            incomplete: tuple[asyncio.Task[Any], ...] = ()
            try:
                with (
                    patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.1),
                    patch("signal_hermes_router.router.SHUTDOWN_SUPERVISOR_FLOOR_SECONDS", 0.1),
                    patch(
                        "signal_hermes_router.router.SHUTDOWN_CLEANUP_CANCEL_GRACE_SECONDS",
                        0.1,
                    ),
                ):
                    close_started = time.monotonic()
                    incomplete = await asyncio.wait_for(
                        router.close(drain_timeout=0.05), timeout=10
                    )
                    elapsed = time.monotonic() - close_started
                self.assertEqual(len(incomplete), 2)
                self.assertLess(elapsed, 5.0)
            finally:
                release.set()
                for task in incomplete:
                    with suppress(Exception):
                        await asyncio.wait_for(task, timeout=5)

    async def test_accept_callback_registers_handler_task_synchronously(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            reader = asyncio.StreamReader()
            writer = Mock()
            writer.close = Mock()
            writer.wait_closed = AsyncMock()

            router._accept_control_client(reader, writer)

            # Registered before any event-loop yield: entry-time registration
            # would leave the set empty here.
            self.assertEqual(len(router._control_client_tasks), 1)
            task = next(iter(router._control_client_tasks))
            reader.feed_eof()
            await asyncio.wait_for(task, timeout=5)
            self.assertEqual(len(router._control_client_tasks), 0)

    async def test_control_server_startup_window_and_direct_close_unpark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            router = SignalHermesRouter(
                _notification_app(
                    tmp,
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            # Shutdown before startup: the server task must not serve and must
            # clean up the socket it bound.
            router.begin_shutdown()
            await asyncio.wait_for(router._run_control_server(), timeout=5)
            self.assertFalse(socket_path.exists())

        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            router = SignalHermesRouter(
                _notification_app(
                    tmp,
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            server_task = asyncio.create_task(router._run_control_server())
            for _ in range(50):
                if socket_path.exists():
                    break
                await asyncio.sleep(0.01)

            # A direct close() unparks the server task without cancellation.
            incomplete = await asyncio.wait_for(router.close(), timeout=10)
            self.assertEqual(incomplete, ())
            await asyncio.wait_for(server_task, timeout=5)
            self.assertFalse(server_task.cancelled())
            self.assertFalse(socket_path.exists())

    async def test_close_terminates_acp_child_gracefully(self) -> None:
        script = Path(__file__).parent / "fixtures" / "fake_acp_agent_graceful_shutdown.py"
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "marker.txt"
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                command_template=[sys.executable, str(script), str(marker)],
            )
            router = SignalHermesRouter(
                _notification_app(tmp),
                signal_client=ClosedAwareSignal(),  # type: ignore[arg-type]
                supervisor=supervisor,
                dedupe=DedupeStore(),
            )
            profile = await supervisor.get_profile(router.config.routes[0])
            assert profile.peer is not None
            process = profile.peer.process
            assert process is not None
            try:
                incomplete = await asyncio.wait_for(router.close(), timeout=30)
                self.assertEqual(incomplete, ())
                # Graceful SIGTERM within the terminate grace, not SIGKILL:
                # the fixture's handler ran, reaped its child, and exited 0.
                self.assertEqual(process.returncode, 0)
                self.assertEqual(
                    marker.read_text(encoding="utf-8").strip(),
                    "child_returncode=-15",
                )
            finally:
                if process.returncode is None:
                    with suppress(ProcessLookupError):
                        process.kill()
                childpid_path = marker.with_suffix(".childpid")
                if childpid_path.exists():
                    with suppress(ProcessLookupError, ValueError):
                        os.kill(int(childpid_path.read_text(encoding="utf-8")), 9)

    async def test_supervisor_close_is_concurrent_and_isolates_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            supervisor = ProfileSupervisor(Path(tmp))
            second_started = asyncio.Event()

            class WaitsForSibling:
                async def close(self) -> None:
                    # Serial closing (first-in-dict first) would deadlock here
                    # and trip the timeout.
                    await asyncio.wait_for(second_started.wait(), timeout=1)

            class SignalsSibling:
                async def close(self) -> None:
                    second_started.set()

            supervisor._profiles = {
                "a": WaitsForSibling(),  # type: ignore[dict-item]
                "b": SignalsSibling(),  # type: ignore[dict-item]
            }
            await asyncio.wait_for(supervisor.close(), timeout=5)
            self.assertEqual(supervisor._profiles, {})

            supervisor = ProfileSupervisor(Path(tmp))
            closed: list[str] = []

            class FailingClose:
                async def close(self) -> None:
                    raise RuntimeError("boom")

            class RecordingClose:
                async def close(self) -> None:
                    closed.append("b")

            supervisor._profiles = {
                "a": FailingClose(),  # type: ignore[dict-item]
                "b": RecordingClose(),  # type: ignore[dict-item]
            }
            with self.assertLogs("signal_hermes_router.sessions", level="WARNING"):
                with self.assertRaisesRegex(RuntimeError, "1 Hermes profile close"):
                    await asyncio.wait_for(supervisor.close(), timeout=5)
            self.assertEqual(closed, ["b"])
            self.assertEqual(supervisor._profiles, {})

