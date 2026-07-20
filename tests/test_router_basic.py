from __future__ import annotations

import asyncio
import base64
import re
import tempfile
import threading
import time
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.acp import JsonRpcError
from signal_hermes_router.config import (
    AppConfig,
    InboundRateLimitConfig,
    RetentionConfig,
    Route,
    RouterConfig,
    RouterControlConfig,
    SyntheticRouteNotification,
    SyntheticRouteJob,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.failures import FailureCode
from signal_hermes_router.models import (
    NormalizedEvent,
    RouteState,
    SessionPolicy,
    SignalAttachment,
    TurnResult,
    TurnOrigin,
    TurnOutcomeStatus,
)
from signal_hermes_router.outbound import NO_REPLY_SENTINEL
from signal_hermes_router.payloads import canonicalize_notification_payload, encode_control_message
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.preflight import ToolSurface
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.sessions import ProfileSupervisor
from signal_hermes_router.outbound_media import (
    validate_outbound_attachments,
)
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_event,
    make_group_raw,
    make_direct_route,
    make_direct_raw,
    make_route,
    make_router_harness,
    make_synthetic_app,
    record_dedupe_call_threads,
    write_test_file,
    wait_until,
    RouterTestCase,
)


class RouterBasicTests(RouterTestCase):
    async def _check_discard_event(self, raw: dict[str, Any], expected: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as logs:
                await router.handle_raw_event(raw)
            output = "\n".join(logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertIn(expected, output)
            self.assertNotIn("synthetic direct message without group", output)

    async def _check_canary_prefix(self, reply_text: str, expected: str) -> None:
        route_context = {"label": "synthetic", "canary_reply_prefix": "[router-canary]"}
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = reply_text
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, route_context=route_context),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(make_event())
            self.assertEqual(signal.sends, [("group", expected)])

    def assertFrozenAttachmentPath(self, raw_path: str, media_root: Path) -> None:
        path = Path(raw_path)
        self.assertEqual(path.name, "attachment.png")
        path.relative_to((media_root / ".outbound").resolve())
        self.assertFalse(path.exists())

    async def test_default_runtime_dependencies_can_be_constructed_and_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, acp_initialize_timeout_seconds=17.5)
            )
            try:
                self.assertIsNotNone(router.signal)
                self.assertIsNotNone(router.supervisor)
                self.assertIsNotNone(router.dedupe)
                self.assertEqual(router.supervisor.initialize_timeout_seconds, 17.5)
            finally:
                await router.close()

    async def test_run_forever_logs_handler_crashes_and_continues(self) -> None:
        class EventSignal(FakeSignal):
            async def events(self):
                yield {"id": 1}
                yield {"id": 2}

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=EventSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            seen: list[dict] = []

            async def handle(raw: dict, *, config: object = None, **_kwargs: object) -> None:
                seen.append(raw)
                if raw["id"] == 1:
                    raise RuntimeError("synthetic")

            router.handle_raw_event = handle  # type: ignore[method-assign]

            with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                await router.run_forever()

            self.assertEqual(seen, [{"id": 1}, {"id": 2}])
            self.assertIn("event handler crashed", "\n".join(logs.output))

    async def test_run_forever_with_control_cancels_sibling_when_signal_loop_ends(self) -> None:
        class EmptySignal(FakeSignal):
            async def events(self):
                if False:
                    yield {}

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    control=RouterControlConfig(enabled=True, socket_path=Path(tmp) / "c.sock"),
                ),
                signal_client=EmptySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            control_cancelled = False

            async def run_control() -> None:
                nonlocal control_cancelled
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    control_cancelled = True
                    raise

            router._run_control_server = run_control  # type: ignore[method-assign]

            await router.run_forever()

            # run_forever only requests sibling shutdown; settlement is owned
            # by close(). Let the loop deliver the cancellation.
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertTrue(control_cancelled)

    async def test_run_forever_with_control_cancels_children_when_parent_is_cancelled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    control=RouterControlConfig(
                        enabled=True,
                        socket_path=Path(tmp) / "work" / "control.sock",
                    ),
                ),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            signal_started = asyncio.Event()
            control_started = asyncio.Event()
            signal_cancelled = False
            control_cancelled = False

            async def run_signal() -> None:
                nonlocal signal_cancelled
                signal_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    signal_cancelled = True
                    raise

            async def run_control() -> None:
                nonlocal control_cancelled
                control_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    control_cancelled = True
                    raise

            router._run_signal_events = run_signal  # type: ignore[method-assign]
            router._run_control_server = run_control  # type: ignore[method-assign]
            task = asyncio.create_task(router.run_forever())
            await asyncio.wait_for(signal_started.wait(), timeout=1)
            await asyncio.wait_for(control_started.wait(), timeout=1)

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            # run_forever only requests child shutdown; settlement is owned
            # by close(). Let the loop deliver the cancellations.
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertTrue(signal_cancelled)
            self.assertTrue(control_cancelled)

    async def test_active_route_calls_backend_and_replies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router
            result = await router.handle_event(make_event())
            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(signal.typing, [("group", True), ("group", False)])
            self.assertTrue(profile.prompts[0][0]["text"].startswith("[route_context:"))
            self.assertEqual(profile.prompts[0][1]["text"], "hello")

    async def test_active_direct_route_calls_backend_and_replies_directly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_direct_route()
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

            result = await router.handle_raw_event(make_direct_raw())

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.direct_sends, [("sender-uuid", "reply")])
            self.assertEqual(signal.typing, [])
            self.assertEqual(signal.direct_typing, [("sender-uuid", True), ("sender-uuid", False)])
            self.assertTrue(profile.prompts[0][0]["text"].startswith("[route_context:"))
            self.assertEqual(profile.prompts[0][1]["text"], "hello direct")

    async def test_empty_group_data_message_is_ignored_and_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router

            result = await router.handle_raw_event(make_group_raw(text="", timestamp=1))

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.typing, [])
            self.assertEqual(dedupe.status("signal:group", "sender-uuid", 1), "handled")
            self.assertFalse(dedupe.claim("signal:group", "sender-uuid", 1))

    async def test_whitespace_only_group_data_message_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router

            result = await router.handle_raw_event(make_group_raw(text=" \n\t ", timestamp=2))

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(dedupe.status("signal:group", "sender-uuid", 2), "handled")

    async def test_empty_group_data_message_does_not_overtake_queued_real_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            app = make_app(tmp, RouteState.ACTIVE)
            route = app.routes[0]
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
            lock = router._route_lock(route)
            await lock.acquire()
            text_task = asyncio.create_task(
                router.handle_raw_event(make_group_raw(text="hello", timestamp=4))
            )
            await asyncio.sleep(0)
            empty_task = asyncio.create_task(
                router.handle_raw_event(make_group_raw(text="", timestamp=4))
            )
            await asyncio.sleep(0)
            try:
                self.assertIsNone(dedupe.status("signal:group", "sender-uuid", 4))
            finally:
                lock.release()

            text_result, empty_result = await asyncio.gather(text_task, empty_task)

            self.assertEqual(text_result, TurnResult("reply"))
            self.assertIsNone(empty_result)
            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(dedupe.status("signal:group", "sender-uuid", 4), "handled")

    async def test_normal_group_data_message_still_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router

            result = await router.handle_raw_event(make_group_raw(text="hello", timestamp=6))

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(profile.prompts[0][1]["text"], "hello")

    async def test_empty_direct_data_message_is_ignored_with_route_identity_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_direct_route()
            dedupe = DedupeStore()
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
                dedupe=dedupe,
            )

            result = await router.handle_raw_event(
                make_direct_raw(source_uuid=None, text="", timestamp=7)
            )

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.direct_sends, [])
            self.assertEqual(signal.direct_typing, [])
            self.assertEqual(dedupe.status(route.key, "sender-uuid", 7), "handled")
            self.assertIsNone(dedupe.status(route.key, "+00000000000", 7))
            self.assertFalse(dedupe.claim(route.key, "sender-uuid", 7))
            self.assertTrue(dedupe.claim(route.key, "+00000000000", 7))

    async def test_stale_group_event_is_skipped_and_marked_handled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            now_ms = 1_000_000_000
            route = make_route(max_event_age_seconds=60)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: now_ms,
            )
            stale_timestamp = now_ms - 60_001

            with self.assertLogs("signal_hermes_router.router", level="INFO") as logs:
                result = await router.handle_event(
                    make_event(text="old message", timestamp=stale_timestamp)
                )

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.typing, [])
            self.assertEqual(dedupe.status(route.key, "sender", stale_timestamp), "handled")
            self.assertTrue(any("discarding stale Signal event" in line for line in logs.output))

            # Redelivery of the skipped event stays deduped.
            redelivered = await router.handle_event(
                make_event(text="old message", timestamp=stale_timestamp)
            )
            self.assertIsNone(redelivered)
            self.assertEqual(profile.prompts, [])
            self.assertFalse(dedupe.claim(route.key, "sender", stale_timestamp))

    async def test_event_exactly_at_age_limit_still_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            now_ms = 1_000_000_000
            route = make_route(max_event_age_seconds=60)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: now_ms,
            )

            result = await router.handle_event(make_event(timestamp=now_ms - 60_000))

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(len(profile.prompts), 1)

    async def test_route_without_age_limit_prompts_old_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            now_ms = 1_000_000_000
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: now_ms,
            )

            result = await router.handle_event(make_event(timestamp=1))

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(len(profile.prompts), 1)

    async def test_unknown_timestamp_bypasses_age_limit(self) -> None:
        # The normalizer emits timestamp=0 when the envelope carries none; an
        # unknown timestamp must not be treated as infinitely old.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            now_ms = 1_000_000_000
            route = make_route(max_event_age_seconds=60)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: now_ms,
            )

            result = await router.handle_event(make_event(timestamp=0))

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(len(profile.prompts), 1)

    async def test_already_stale_event_skips_without_waiting_for_profile_lock(self) -> None:
        # An event that is already stale must be discarded under the route
        # lock alone: the Signal consumer awaits each handler, so blocking on
        # a busy shared profile just to throw the event away would stall
        # every following event behind an unrelated long turn.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            now_ms = 1_000_000_000
            route = make_route(max_event_age_seconds=60)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: now_ms,
            )
            stale_timestamp = now_ms - 3_600_000
            profile_lock = router._profile_lock("profile")
            await profile_lock.acquire()
            try:
                result = await asyncio.wait_for(
                    router.handle_event(make_event(timestamp=stale_timestamp)),
                    timeout=1.0,
                )
            finally:
                profile_lock.release()

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(dedupe.status(route.key, "sender", stale_timestamp), "handled")

    async def test_event_going_stale_behind_shared_profile_lock_is_skipped(self) -> None:
        # Two routes share a profile. An event that was fresh when its route
        # lock was acquired can outlive its age limit while waiting on the
        # shared profile lock; staleness must be evaluated at that admission
        # point, not at route-lock entry.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            clock = {"now": 1_000_000_000}
            route_a = make_route(group_id="group-one")
            route_b = make_route(group_id="group-two", max_event_age_seconds=60)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route_a, route_b)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: clock["now"],
            )
            fresh_timestamp = clock["now"]
            profile_lock = router._profile_lock("profile")
            await profile_lock.acquire()
            task = asyncio.create_task(
                router.handle_event(make_event(group_id="group-two", timestamp=fresh_timestamp))
            )
            await asyncio.sleep(0)
            clock["now"] += 3_600_000
            profile_lock.release()

            result = await task

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(dedupe.status(route_b.key, "sender", fresh_timestamp), "handled")

    async def test_inbound_rate_cap_drops_turns_beyond_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            clock = {"now": 1_000_000_000}
            route = make_route(
                inbound_rate_limit=InboundRateLimitConfig(max_turns=2, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: clock["now"],
            )

            first = await router.handle_event(make_event(timestamp=1))
            second = await router.handle_event(make_event(timestamp=2))
            with self.assertLogs("signal_hermes_router.router", level="INFO") as logs:
                third = await router.handle_event(make_event(timestamp=3))

            self.assertEqual(first, TurnResult("reply"))
            self.assertEqual(second, TurnResult("reply"))
            self.assertIsNone(third)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(dedupe.status(route.key, "sender", 3), "handled")
            self.assertTrue(
                any("discarding rate_limited Signal event" in line for line in logs.output)
            )

            # Refill: 2 turns per 60s means one token back after 30s.
            clock["now"] += 30_000
            fourth = await router.handle_event(make_event(timestamp=4))
            self.assertEqual(fourth, TurnResult("reply"))
            self.assertEqual(len(profile.prompts), 3)

    async def test_inbound_rate_cap_duplicate_does_not_burn_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            clock = {"now": 1_000_000_000}
            route = make_route(
                inbound_rate_limit=InboundRateLimitConfig(max_turns=2, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: clock["now"],
            )

            first = await router.handle_event(make_event(timestamp=1))
            duplicate = await router.handle_event(make_event(timestamp=1))
            second = await router.handle_event(make_event(timestamp=2))
            third = await router.handle_event(make_event(timestamp=3))

            self.assertEqual(first, TurnResult("reply"))
            self.assertIsNone(duplicate)
            self.assertEqual(second, TurnResult("reply"))
            self.assertIsNone(third)
            self.assertEqual(len(profile.prompts), 2)

    async def test_inbound_rate_cap_does_not_burn_tokens_for_non_active_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            clock = {"now": 1_000_000_000}
            route = make_route(
                inbound_rate_limit=InboundRateLimitConfig(max_turns=1, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: clock["now"],
            )
            router.route_state_overrides[route.key] = RouteState.MAINTENANCE

            await router.handle_event(make_event(timestamp=1))
            await router.handle_event(make_event(timestamp=2))
            router.route_state_overrides.pop(route.key)
            active_result = await router.handle_event(make_event(timestamp=3))
            capped_result = await router.handle_event(make_event(timestamp=4))

            maintenance_replies = [body for _, body in signal.sends if "maintenance" in body]
            self.assertEqual(len(maintenance_replies), 2)
            self.assertEqual(active_result, TurnResult("reply"))
            self.assertIsNone(capped_result)
            self.assertEqual(len(profile.prompts), 1)

    async def test_inbound_rate_cap_sheds_before_media_and_session_work(self) -> None:
        # An over-limit turn is dropped before attachment storage and before
        # ACP session acquisition, so a burst does not consume media I/O or
        # session/new calls on its way to being discarded.
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_route(
                session_policy=SessionPolicy.EPHEMERAL,
                inbound_rate_limit=InboundRateLimitConfig(max_turns=1, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            first = await router.handle_event(make_event(timestamp=1))
            dropped = await router.handle_event(
                make_event(
                    timestamp=2,
                    attachments=(
                        SignalAttachment(
                            content_type="image/png",
                            filename="photo.png",
                            body=b"png-bytes",
                        ),
                    ),
                )
            )

            self.assertEqual(first, TurnResult("reply"))
            self.assertIsNone(dropped)
            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(profile.new_sessions, 1)
            self.assertEqual(list((Path(tmp) / "media").rglob("*.png")), [])

    async def test_inbound_rate_cap_does_not_burn_tokens_on_session_failure(self) -> None:
        # A turn that fails before the prompt (here: session acquisition)
        # never consumes a token; the cap counts prompted turns only.
        class FlakySupervisor(FakeSupervisor):
            def __init__(self, profile: FakeProfile) -> None:
                super().__init__(profile)
                self.fail_next = True

            async def get_profile(self, route: Route) -> FakeProfile:
                if self.fail_next:
                    self.fail_next = False
                    raise RuntimeError("session setup failed")
                return self.profile

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_route(
                inbound_rate_limit=InboundRateLimitConfig(max_turns=1, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,), failures=5),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FlakySupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_event(make_event(timestamp=1))
            recovered = await router.handle_event(make_event(timestamp=2))
            capped = await router.handle_event(make_event(timestamp=3))

            self.assertIsNone(failed)
            self.assertEqual(recovered, TurnResult("reply"))
            self.assertIsNone(capped)
            self.assertEqual(len(profile.prompts), 1)

    async def test_shadow_route_with_rate_limit_never_creates_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_route(
                state=RouteState.SHADOW,
                inbound_rate_limit=InboundRateLimitConfig(max_turns=1, window_seconds=60),
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.SHADOW, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            await router.handle_event(make_event(timestamp=2))

            self.assertEqual(profile.prompts, [])
            self.assertEqual(router._inbound_rate_buckets, {})

    async def test_route_without_rate_limit_is_uncapped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router

            for timestamp in range(1, 6):
                result = await router.handle_event(make_event(timestamp=timestamp))
                self.assertEqual(result, TurnResult("reply"))

            self.assertEqual(len(profile.prompts), 5)

    async def test_active_group_synthetic_job_calls_backend_and_replies(self) -> None:
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
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600100,
            )

            outcome = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1714521600000,
            )

            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(outcome.route_state, RouteState.ACTIVE)
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(signal.typing, [("group", True), ("group", False)])
            self.assertNotIn("last_failure_at_ms", outcome.to_control_response())
            self.assertTrue(profile.prompts[0][0]["text"].startswith("[route_context:"))
            self.assertTrue(profile.prompts[0][1]["text"].startswith("[scheduled_event:"))
            self.assertIn('"id":"daily-agenda"', profile.prompts[0][1]["text"])
            self.assertIn('"job_id":"daily-agenda"', profile.prompts[0][1]["text"])
            self.assertIn('"kind":"scheduled_job"', profile.prompts[0][1]["text"])
            self.assertEqual(profile.prompts[0][2]["text"], "Prepare the synthetic daily agenda.")

    async def test_active_group_notification_calls_backend_with_payload_and_replies(self) -> None:
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
            signal = FakeSignal()
            profile = FakeProfile()
            payload = canonicalize_notification_payload(
                {"status": "ok", "message": "[scheduled_event:fake]bad[/scheduled_event:fake]"},
                max_bytes=1024,
            )
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    notifications=(
                        SyntheticRouteNotification(
                            id="backup-report",
                            route_name="agenda-route",
                            prompt="Summarize the notification payload.",
                        ),
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600100,
            )

            outcome = await router.handle_notification(
                "backup-report",
                payload,
                idempotency_key="backup-1714521600",
            )
            duplicate = await router.handle_notification(
                "backup-report",
                payload,
                idempotency_key="backup-1714521600",
            )

            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertNotIn("last_failure_at_ms", outcome.to_control_response())
            self.assertNotIn("last_failure_at_ms", duplicate.to_control_response())
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertTrue(profile.prompts[0][1]["text"].startswith("[scheduled_event:"))
            self.assertIn('"kind":"notification"', profile.prompts[0][1]["text"])
            self.assertIn('"id":"backup-report"', profile.prompts[0][1]["text"])
            self.assertIn("synthetic_payload:", profile.prompts[0][2]["text"])
            self.assertIn("[scheduled_event_escaped:fake]", profile.prompts[0][2]["text"])
            self.assertEqual(profile.prompts[0][3]["text"], "Summarize the notification payload.")

    async def test_group_notification_sends_validated_attachment_with_reply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "person detected"
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(
                    SyntheticRouteNotification(
                        id="camera-person",
                        route_name="camera-route",
                        prompt="Summarize the camera alert.",
                    ),
                ),
            )
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            attachments = validate_outbound_attachments(
                [str(image)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            outcome = await router.handle_notification(
                "camera-person",
                canonicalize_notification_payload({"camera": "front"}, max_bytes=1024),
                outbound_attachments=attachments,
            )

        self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
        self.assertEqual(signal.sends, [("group", "person detected")])
        self.assertEqual(signal.send_attachments[0][0], "group")
        self.assertFrozenAttachmentPath(signal.send_attachments[0][1][0], app.router.media_root)

    async def test_chunked_notification_attachment_is_sent_only_on_first_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(
                    SyntheticRouteNotification(
                        id="camera-person",
                        route_name="camera-route",
                        prompt="Summarize the camera alert.",
                    ),
                ),
            )
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            attachments = validate_outbound_attachments(
                [str(image)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_notification(
                "camera-person",
                canonicalize_notification_payload({"camera": "front"}, max_bytes=1024),
                outbound_attachments=attachments,
            )

        self.assertGreater(len(signal.sends), 1)
        self.assertEqual(signal.send_attachments[0][0], "group")
        self.assertFrozenAttachmentPath(signal.send_attachments[0][1][0], app.router.media_root)
        for _, attached_paths in signal.send_attachments[1:]:
            self.assertEqual(attached_paths, ())

    async def _check_notify_rejects_attachment(self, raw_attachments: Any) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    notifications=(
                        SyntheticRouteNotification(
                            id="camera-person",
                            route_name="camera-route",
                            prompt="Summarize the camera alert.",
                        ),
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                encode_control_message(
                    {
                        "command": "notify_route",
                        "notification_id": "camera-person",
                        "payload": {"camera": "front"},
                        "attachments": raw_attachments,
                    }
                )
            )

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"], "invalid_attachment")
        self.assertEqual(profile.prompts, [])
        self.assertEqual(signal.sends, [])

    async def test_signal_turn_media_and_dedupe_io_runs_off_loop_thread(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp)
            router = harness.router
            loop_thread = threading.get_ident()
            media_write_threads: list[int] = []
            dedupe_calls = record_dedupe_call_threads(harness.dedupe)
            from signal_hermes_router.media import write_attachment as original_write

            def recording_write(**kwargs: Any) -> Any:
                media_write_threads.append(threading.get_ident())
                return original_write(**kwargs)

            with patch("signal_hermes_router.media.write_attachment", recording_write):
                result = await router.handle_event(
                    make_event(
                        timestamp=10,
                        text="file",
                        attachments=(
                            SignalAttachment(
                                content_type="application/pdf",
                                filename="report.pdf",
                                body=b"%PDF synthetic",
                            ),
                        ),
                    )
                )
                # Skipped-event path (empty text, no attachments): its dedupe
                # claim/mark_handled must also run off the loop.
                skipped = await router.handle_event(make_event(timestamp=11, text="   "))

            self.assertIsNotNone(result)
            self.assertIsNone(skipped)
            self.assertTrue(media_write_threads)
            recorded_methods = {name for name, _ident in dedupe_calls}
            self.assertIn("claim", recorded_methods)
            self.assertIn("mark_handled", recorded_methods)
            for ident in media_write_threads:
                self.assertNotEqual(ident, loop_thread)
            for name, ident in dedupe_calls:
                self.assertNotEqual(ident, loop_thread, name)
            await router.close(drain_timeout=0.0)

    async def test_cancelled_claim_is_released_by_worker_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            loop_thread = threading.get_ident()
            started = threading.Event()
            release = threading.Event()

            def gated_clock() -> int:
                if threading.get_ident() != loop_thread:
                    started.set()
                    release.wait(timeout=30)
                return int(time.time() * 1000)

            harness = make_router_harness(
                tmp,
                dedupe=DedupeStore(clock_ms=gated_clock),
                retention=RetentionConfig(dedupe_handled_seconds=None),
            )
            router = harness.router
            route = router.config.routes[0]
            event = make_event(timestamp=10, text="hello")
            turn = asyncio.create_task(router.handle_event(event))
            try:
                async with asyncio.timeout(5):
                    while not started.is_set():
                        await asyncio.sleep(0.01)
                # Cancel while the claim statement is mid-commit in its
                # worker thread: the coroutine never records the claim.
                turn.cancel()
                with suppress(asyncio.CancelledError):
                    await turn
            finally:
                release.set()
            # The abandoned claim committed anyway; the worker's done
            # callback releases it so the identity is not wedged as a
            # duplicate until the next startup reclaim.
            sender_id = router_module._routed_sender_id(route, event)
            async with asyncio.timeout(5):
                while router.dedupe.status(route.key, sender_id, 10) is not None:
                    await asyncio.sleep(0.01)
            await router.close(drain_timeout=0.0)

    async def test_cancelled_finalization_still_marks_all_synthetic_identities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            loop_thread = threading.get_ident()
            worker_clock_calls = {"count": 0}
            started = threading.Event()
            release = threading.Event()

            def gated_clock() -> int:
                if threading.get_ident() != loop_thread:
                    worker_clock_calls["count"] += 1
                    # Calls 1 and 2 are the two claims; call 3 is the first
                    # mark_handled finalizer.
                    if worker_clock_calls["count"] == 3:
                        started.set()
                        release.wait(timeout=30)
                return int(time.time() * 1000)

            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(clock_ms=gated_clock),
            )
            job = router.config.find_synthetic_job("daily-agenda")
            assert job is not None
            task = asyncio.create_task(
                router.handle_synthetic_job(
                    "daily-agenda",
                    scheduled_at=1000,
                    idempotency_key="stable-fire",
                )
            )
            try:
                async with asyncio.timeout(5):
                    while not started.is_set():
                        await asyncio.sleep(0.01)
                # Cancel while the first mark_handled finalizer is mid-commit:
                # the second identity's finalizer was already dispatched, so
                # it must still complete. The cancellation is recorded on the
                # awaited outer future before the gate is released, so the
                # observation loop sees it; releasing before awaiting the
                # task lets the still-dispatched finalizers drain.
                task.cancel()
                release.set()
                with suppress(asyncio.CancelledError):
                    await task
            finally:
                release.set()
            key_sender, key_timestamp = router._synthetic_dedupe_identity(
                job.namespace,
                scheduled_at=None,
                idempotency_key="stable-fire",
                triggered_at_ms=0,
            )
            async with asyncio.timeout(5):
                while (
                    router.dedupe.status(route.key, key_sender, key_timestamp) != "handled"
                    or router.dedupe.status(route.key, job.namespace, 1000) != "handled"
                ):
                    await asyncio.sleep(0.01)
            await router.close(drain_timeout=0.0)

    async def test_active_direct_synthetic_job_replies_directly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = make_direct_route()
            route = Route(
                platform=route.platform,
                name="direct-agenda",
                chat_type=route.chat_type,
                sender_id=route.sender_id,
                sender_number=route.sender_number,
                profile=route.profile,
                session_policy=route.session_policy,
                state=route.state,
                route_context=route.route_context,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    SyntheticRouteJob(
                        id="direct-daily",
                        route_name="direct-agenda",
                        prompt="Prepare the direct synthetic agenda.",
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            outcome = await router.handle_synthetic_job("direct-daily", scheduled_at=1000)

            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.direct_sends, [("sender-uuid", "reply")])
            self.assertEqual(signal.direct_typing, [("sender-uuid", True), ("sender-uuid", False)])

    async def test_mcp_only_route_enforces_runtime_local_tool_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="mcp-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "bash"}]),
                mcp_only=True,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            # Route.__post_init__ already syncs permission_policy.mcp_only from the
            # route flag, so the router line is a no-op for route-level policies.
            # The synthetic job test below exercises the router upgrade path.
            self.assertTrue(profile.policies[-1][1].mcp_only)
            # Verify the policy actually rejects a local tool
            self.assertFalse(profile.policies[-1][1].allows_tool_call({"toolName": "bash"}))

    async def test_empty_model_failure_reply_falls_back_to_generic_failure_reply(self) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "auth failed",
                        "data": {
                            "code": "model_auth_failed",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="Generic router fallback.",
                    model_failure_reply="",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.to_control_response()["failure"]["code"], "model_auth_failed")
            self.assertTrue(failed.to_control_response()["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Generic router fallback.")])

    async def test_route_failure_reply_overrides_model_failure_reply(self) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "quota exceeded",
                        "data": {
                            "code": "model_rate_limited",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                failure_reply="Route-specific failure.",
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="Generic router fallback.",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(
                failed.to_control_response()["failure"]["code"],
                "model_rate_limited",
            )
            self.assertTrue(failed.to_control_response()["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Route-specific failure.")])

    async def test_signal_model_failure_uses_model_reply(self) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "quota exceeded",
                        "data": {
                            "code": "model_rate_limited",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    failure_reply="Generic router fallback.",
                    model_failure_reply="Model fallback.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                result = await router.handle_event(make_event())

            self.assertIsNone(result)
            self.assertEqual(signal.sends, [("group", "Model fallback.")])

    async def test_model_provider_failure_codes_use_model_reply(self) -> None:
        class ModelFailProfile(FakeProfile):
            def __init__(self, structured_code: str) -> None:
                super().__init__()
                self.structured_code = structured_code

            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "provider failure",
                        "data": {
                            "code": self.structured_code,
                            "provider_class": "cloud_api",
                        },
                    }
                )

        cases = {
            "model_auth_failed": FailureCode.MODEL_AUTH_FAILED,
            "model_rate_limited": FailureCode.MODEL_RATE_LIMITED,
            "model_unavailable": FailureCode.MODEL_UNAVAILABLE,
            "model_timeout": FailureCode.MODEL_TIMEOUT,
            "endpoint_unreachable": FailureCode.ENDPOINT_UNREACHABLE,
        }
        for structured_code, expected_code in cases.items():
            with self.subTest(structured_code=structured_code):
                with tempfile.TemporaryDirectory() as tmp:
                    route = Route(
                        platform="signal",
                        name="agenda-route",
                        group_id="group",
                        profile="profile",
                        session_policy=SessionPolicy.PERSISTENT_ROUTE,
                        state=RouteState.ACTIVE,
                    )
                    signal = FakeSignal()
                    router = SignalHermesRouter(
                        make_synthetic_app(
                            tmp,
                            route,
                            failure_reply="Generic router fallback.",
                            model_failure_reply="Model fallback.",
                        ),
                        signal_client=signal,  # type: ignore[arg-type]
                        supervisor=FakeSupervisor(ModelFailProfile(structured_code)),  # type: ignore[arg-type]
                        dedupe=DedupeStore(),
                    )

                    with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                        failed = await router.handle_synthetic_job(
                            "daily-agenda", scheduled_at=1000
                        )

                    self.assertEqual(
                        failed.to_control_response()["failure"]["code"],
                        expected_code.value,
                    )
                    self.assertTrue(failed.to_control_response()["reply_sent"])
                    self.assertEqual(signal.sends, [("group", "Model fallback.")])

    async def test_signal_hermes_failure_marks_event_handled_after_failure_reply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                first = await router.handle_event(make_event())
            profile.fail = False
            duplicate = await router.handle_event(make_event())

            self.assertIsNone(first)
            self.assertIsNone(duplicate)
            self.assertEqual(len(profile.prompts), 1)
            self.assertFalse(dedupe.claim("signal:group", "sender", 1))

    async def test_empty_router_failure_reply_suppresses_non_model_failure_send(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                failure_reply="",
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(failed.to_control_response()["failure"]["code"], "unknown")
            self.assertFalse(failed.to_control_response()["reply_sent"])
            self.assertEqual(signal.sends, [])

    async def test_empty_router_replies_suppress_model_failure_send(self) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "quota exceeded",
                        "data": {
                            "code": "model_rate_limited",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                failure_reply="",
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="",
                    model_failure_reply="",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(
                failed.to_control_response()["failure"]["code"],
                "model_rate_limited",
            )
            self.assertFalse(failed.to_control_response()["reply_sent"])
            self.assertEqual(signal.sends, [])

    async def test_signal_event_accepted_under_breaker_keeps_mask_after_reload(self) -> None:
        # The Signal consumer pins the breaker override map and config
        # generation when it ACCEPTS an event. The accepted task may not run
        # until after a reload removed and re-added the route (clearing the
        # override); the admission-time maintenance gate must still apply.
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
            # The breaker is open when the consumer accepts the event.
            harness.router.route_state_overrides[r1.key] = RouteState.MAINTENANCE
            harness.router._trip_times[r1.key] = time.monotonic()

            class OneEventSignal(FakeSignal):
                async def events(self):  # type: ignore[override]
                    yield make_group_raw(group_id="group-one", timestamp=int(time.time() * 1000))

            harness.router.signal = OneEventSignal()  # type: ignore[assignment]
            event_signal = harness.router.signal
            # The accepted turn queues behind the held route lock; the
            # consumer shield-awaits it.
            await harness.router._route_locks[r1.key].acquire()
            consumer_task = asyncio.ensure_future(harness.router._run_signal_events())
            for _ in range(100):
                if harness.router._signal_turn_tasks:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(harness.router._signal_turn_tasks)
            # Reload 1 removes the route (clearing the override), reload 2
            # re-adds it ACTIVE — all before the accepted task runs.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            routes_path.write_text(active_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            harness.router._route_locks[r1.key].release()
            await asyncio.wait_for(consumer_task, timeout=5)
            self.assertEqual(harness.profile.prompts, [])
            self.assertEqual(event_signal.sends, [("group-one", "route under repair")])
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_retire_follows_probe_completion_after_mid_probe_reload(self) -> None:
        # The reaper skips retiring a profile while a preflight probe is in
        # flight, but that skip schedules no follow-up of its own (the drain
        # set was already empty): the last probe to finish must re-trigger
        # retirement, or the cached subprocess for a reloaded-away profile
        # would linger until the next reload or shutdown.
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
            # Reload r1 away mid-probe: the reaper drains r1 but must skip
            # retiring p1 while the probe is parked.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            self.assertTrue(reapers)
            await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertEqual(harness.supervisor.retired, [])

            block.set()
            response = await asyncio.wait_for(preflight_task, timeout=5)
            self.assertEqual(response["status"], "ok")
            # The probe's completion schedules a fresh reap; it may run to
            # completion before this task resumes, and completed tasks are
            # settled out of the tracked set, so poll the OUTCOME against a
            # deadline instead of inspecting the task set.
            await wait_until(
                lambda: harness.supervisor.retired == ["p1"], timeout=5.0, interval=0.01
            )
            self.assertNotIn("p1", harness.supervisor.cached)
            # Drain whatever the deferred reap left in flight (it may already
            # have settled out of the tracked set, hence no done() filter).
            if harness.router._reap_tasks:
                await asyncio.wait_for(asyncio.gather(*harness.router._reap_tasks), timeout=5)
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_turn_on_reloaded_away_route_schedules_cleanup_reap(self) -> None:
        # A turn admitted before its route was reloaded away can outlive the
        # whole reap chain when it is stuck in pre-lock work (the force path
        # ends the chain under it with nothing yet to close). Whatever the
        # turn then creates for the dead route — a fresh session and cached
        # profile subprocess — must be reaped when the turn finishes, not
        # linger until the next reload or shutdown.
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(tmp, RouteState.ACTIVE)
            harness = make_router_harness(tmp, app=app)
            old_config = harness.router.config
            harness.supervisor.cached.append("profile")
            # The route is reloaded away while the turn is admitted; the
            # turn itself still runs against its admission-time config.
            from dataclasses import replace

            harness.router.config = replace(app, routes=())
            outcome = await harness.router.handle_event(make_event(), config=old_config)
            assert outcome is not None
            self.assertEqual(outcome.text, "reply")
            # The turn's completion scheduled a cleanup reap; poll the
            # OUTCOME against a deadline (completed reaps settle out of the
            # tracked set, so the task set is not a stable witness).
            await wait_until(
                lambda: harness.supervisor.retired == ["profile"], timeout=5.0, interval=0.01
            )
            self.assertEqual(set(harness.router.sessions._sessions), set())

    async def test_turn_on_now_disabled_route_schedules_cleanup_reap(self) -> None:
        # Same self-heal when the route is still PRESENT but no longer
        # ACTIVE: a turn admitted ACTIVE proceeds on its captured route
        # object and acquires a session even though a reload disabled the
        # route mid-turn, so the fresh session/profile must be reaped at turn
        # completion just like the reloaded-away case.
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(tmp, RouteState.ACTIVE)
            harness = make_router_harness(tmp, app=app)
            old_config = harness.router.config
            harness.supervisor.cached.append("profile")
            # A reload disables the route while the turn is admitted; the
            # turn still runs against its admission-time config.
            from dataclasses import replace

            route = old_config.routes[0]
            harness.router.config = replace(
                app, routes=(replace(route, state=RouteState.DISABLED),)
            )
            outcome = await harness.router.handle_event(make_event(), config=old_config)
            assert outcome is not None
            self.assertEqual(outcome.text, "reply")
            await wait_until(
                lambda: harness.supervisor.retired == ["profile"], timeout=5.0, interval=0.01
            )
            self.assertEqual(set(harness.router.sessions._sessions), set())

    async def test_turn_with_failed_session_acquisition_schedules_cleanup_reap(self) -> None:
        # The self-heal gates on acquisition being ATTEMPTED, not on it
        # succeeding: sessions.get spawns/caches the profile subprocess
        # BEFORE session/new, so a session/new failure still leaves a cached
        # profile behind that must be reaped once the route is gone.
        class FailingNewSessionProfile(FakeProfile):
            async def new_session(self, cwd: Path) -> str:
                raise RuntimeError("spawn ok, session failed")

        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(tmp, RouteState.ACTIVE)
            harness = make_router_harness(tmp, app=app, profile=FailingNewSessionProfile())
            old_config = harness.router.config
            harness.supervisor.cached.append("profile")
            # The route is reloaded away while the turn is admitted; the
            # turn itself still runs against its admission-time config.
            from dataclasses import replace

            harness.router.config = replace(app, routes=())
            outcome = await harness.router.handle_event(make_event(), config=old_config)
            # The acquisition failure took the Hermes-failure path (an ERROR
            # outcome, which handle_event surfaces as None) before any
            # session existed...
            self.assertIsNone(outcome)
            self.assertEqual(harness.profile.new_sessions, 0)
            # ...and the finally-block self-heal still reaps the cached
            # profile the failed acquisition left behind.
            await wait_until(
                lambda: harness.supervisor.retired == ["profile"], timeout=5.0, interval=0.01
            )
            self.assertEqual(set(harness.router.sessions._sessions), set())

    async def test_turn_outliving_policy_reload_schedules_policy_reap(self) -> None:
        # Same self-heal when the route stays ACTIVE but a reload flipped
        # its session_policy mid-turn: the turn cached its session under
        # the admission-time policy, and the reload-side reap call never
        # carries a still-live key in drain scope, so without the
        # turn-completion reap the stale-policy session lingers until the
        # next reload or shutdown. The profile must NOT be retired — the
        # route is still ACTIVE.
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(session_policy=SessionPolicy.PERSISTENT_ROUTE)
            app = make_app(tmp, RouteState.ACTIVE, routes=(route,))
            harness = make_router_harness(tmp, app=app)
            old_config = harness.router.config
            harness.supervisor.cached.append("profile")
            # A reload flips the session policy while the turn is admitted;
            # the turn still runs against its admission-time config. Both
            # policies cache sessions, so only the self-heal reap evicts.
            from dataclasses import replace

            harness.router.config = replace(
                app,
                routes=(replace(route, session_policy=SessionPolicy.PERSISTENT_SENDER),),
            )
            outcome = await harness.router.handle_event(make_event(), config=old_config)
            assert outcome is not None
            self.assertEqual(outcome.text, "reply")
            await wait_until(
                lambda: not set(harness.router.sessions._sessions), timeout=5.0, interval=0.01
            )
            self.assertEqual(harness.supervisor.retired, [])

    async def test_shadow_turn_does_not_schedule_cleanup_reap(self) -> None:
        # The self-heal is gated on the turn actually acquiring a session:
        # steady-state non-ACTIVE turns (state-gated before session creation)
        # must not churn a no-op reap per event. A no-op reap completes and
        # settles out of the tracked set almost immediately, so observe the
        # scheduling call itself rather than the task set.
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(tmp, RouteState.SHADOW)
            harness = make_router_harness(tmp, app=app)
            scheduled: list[asyncio.Task[None]] = []
            real_track = harness.router._track_reap_task

            def spy(task: asyncio.Task[None]) -> None:
                scheduled.append(task)
                real_track(task)

            harness.router._track_reap_task = spy  # type: ignore[method-assign]
            outcome = await harness.router.handle_event(make_event())
            self.assertIsNone(outcome)
            await asyncio.sleep(0.1)
            self.assertEqual(scheduled, [])
            self.assertEqual(harness.supervisor.retired, [])

    async def test_retire_profile_contains_close_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from signal_hermes_router.acp import ACPProfile

            supervisor = ProfileSupervisor(work_root=Path(tmp))
            bad = ACPProfile(profile="bad", work_root=Path(tmp), command=["true"])
            good = ACPProfile(profile="good", work_root=Path(tmp), command=["true"])
            supervisor._profiles["bad"] = bad
            supervisor._profiles["good"] = good

            async def failing_close() -> None:
                raise RuntimeError("boom")

            bad.close = failing_close  # type: ignore[method-assign]

            # A subprocess whose close() raises is still evicted and must not
            # abort the reload reaper before it retires the other profiles.
            with self.assertLogs("signal_hermes_router.sessions", level="WARNING"):
                self.assertTrue(await supervisor.retire_profile("bad"))
            self.assertNotIn("bad", supervisor._profiles)
            self.assertTrue(await supervisor.retire_profile("good"))
            self.assertEqual(supervisor._profiles, {})

    async def test_signal_event_pinned_to_admission_config(self) -> None:
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

            # Wiring: the consumer pins the config at acceptance.
            captured: dict[str, Any] = {}

            class OneEventSignal(FakeSignal):
                async def events(self):  # type: ignore[override]
                    yield make_group_raw(group_id="group-one", timestamp=9)

            harness.router.signal = OneEventSignal()  # type: ignore[assignment]

            async def recorder(
                raw: dict,
                *,
                config: Any = None,
                admission_overrides: Any = None,
                admission_generation: Any = None,
            ) -> None:
                captured["config"] = config
                captured["admission_overrides"] = admission_overrides
                captured["admission_generation"] = admission_generation

            harness.router.handle_raw_event = recorder  # type: ignore[method-assign]
            expected = harness.router.config
            expected_overrides = dict(harness.router.route_state_overrides)
            expected_generation = harness.router._config_generation
            await harness.router._run_signal_events()
            self.assertIs(captured["config"], expected)
            self.assertEqual(captured["admission_overrides"], expected_overrides)
            self.assertEqual(captured["admission_generation"], expected_generation)
            # Restore the real handler and signal client for the semantics
            # checks below.
            del harness.router.handle_raw_event  # type: ignore[attr-defined]
            harness.router.signal = harness.signal  # type: ignore[assignment]

            # Semantics: an event admitted before a reload that removed its
            # route still runs against the admission-time config...
            old_config = harness.router.config
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            now_ms = int(time.time() * 1000)
            await harness.router.handle_raw_event(
                make_group_raw(group_id="group-one", timestamp=now_ms),
                config=old_config,
            )
            self.assertEqual(harness.signal.sends, [("group-one", "reply")])
            # ...while an unpinned lookup after the swap discards it.
            result = await harness.router.handle_raw_event(
                make_group_raw(group_id="group-one", timestamp=now_ms + 1)
            )
            self.assertIsNone(result)
            self.assertEqual(harness.signal.sends, [("group-one", "reply")])

    async def test_trigger_job_pinned_to_admission_config(self) -> None:
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

scheduled_jobs:
  - id: j1
    route: r1
    prompt: job prompt
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            # Wiring: the control request runner pins the config at admission.
            captured: dict[str, Any] = {}

            async def recorder(
                line: bytes,
                *,
                config: Any = None,
                admitted: bool = False,
                admission_overrides: Any = None,
                admission_generation: Any = None,
            ) -> dict[str, Any]:
                captured["config"] = config
                captured["admitted"] = admitted
                captured["admission_overrides"] = admission_overrides
                captured["admission_generation"] = admission_generation
                return {"status": "ok"}

            harness.router._handle_control_line = recorder  # type: ignore[method-assign]
            expected = harness.router.config
            expected_overrides = dict(harness.router.route_state_overrides)
            expected_generation = harness.router._config_generation
            await harness.router._run_control_request(b'{"command":"route_status"}')
            self.assertIs(captured["config"], expected)
            self.assertEqual(captured["admission_overrides"], expected_overrides)
            self.assertEqual(captured["admission_generation"], expected_generation)

            # Semantics: a job admitted before a reload that removed it still
            # runs against the admission-time config...
            old_config = harness.router.config
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
            outcome = await harness.router.handle_synthetic_job("j1", config=old_config)
            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(harness.signal.sends, [("group-one", "reply")])
            # ...while an unpinned lookup after the swap reports unknown_job.
            outcome = await harness.router.handle_synthetic_job("j1")
            self.assertEqual(outcome.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(outcome.error, "unknown_job")

    async def test_retire_profile_serializes_with_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from signal_hermes_router.acp import ACPProfile

            supervisor = ProfileSupervisor(work_root=Path(tmp))
            profile = ACPProfile(profile="p", work_root=Path(tmp), command=["true"])
            supervisor._profiles["p"] = profile
            # A turn holding the acquisition lock must not observe the empty
            # cache slot and spawn a second subprocess while retirement is
            # still closing the first.
            lock = supervisor._acquire_locks["p"]
            await lock.acquire()
            task = asyncio.create_task(supervisor.retire_profile("p"))
            try:
                await asyncio.sleep(0.02)
                self.assertFalse(task.done())
            finally:
                lock.release()
            self.assertTrue(await task)
            self.assertNotIn("p", supervisor._profiles)

    async def test_retire_profile_honours_should_retire_under_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from signal_hermes_router.acp import ACPProfile

            supervisor = ProfileSupervisor(work_root=Path(tmp))
            supervisor._profiles["p"] = ACPProfile(
                profile="p", work_root=Path(tmp), command=["true"]
            )
            # A False verdict under the acquisition lock leaves the cache
            # untouched: a reload that re-activated the profile's route
            # while retirement waited on the lock must win over the reaper's
            # stale outside-the-lock check.
            self.assertFalse(await supervisor.retire_profile("p", should_retire=lambda: False))
            self.assertIn("p", supervisor._profiles)
            self.assertTrue(await supervisor.retire_profile("p", should_retire=lambda: True))
            self.assertNotIn("p", supervisor._profiles)

    async def test_failure_on_reloaded_away_route_skips_profile_recovery(self) -> None:
        # A turn admitted on an ACTIVE route whose prompt fails AFTER a
        # reload removed the route must still get its failure reply, but
        # must not restart the profile or cache a replacement session:
        # recovery exists for the route's next prompt, and a retired route
        # never prompts again — its profile is the reaper's responsibility.
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
scheduled_jobs:
  - id: j1
    route: r1
    prompt: job prompt
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            harness.profile.fail = True
            harness.profile.prompt_delay = 0.3
            request_task = asyncio.ensure_future(
                harness.router._run_control_request(
                    b'{"command":"trigger_job","job_id":"j1","timeout":5}'
                )
            )
            # Let the turn reach the (doomed) prompt, then remove the route
            # before the failure lands.
            await asyncio.sleep(0.05)
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            response = await asyncio.wait_for(request_task, timeout=5)

            self.assertEqual(response["status"], "error")
            # The failure reply still went out...
            self.assertEqual(len(harness.signal.sends), 1)
            self.assertEqual(harness.signal.sends[0][0], "group-one")
            # ...but no recovery ran for a route that can no longer prompt.
            self.assertEqual(harness.supervisor.restarts, 0)
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            self.assertEqual(harness.router.sessions._sessions, {})

    async def test_turn_prompt_block_guards_reject_malformed_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertRaisesRegex(ValueError, "Signal turn requires event"):
                router._build_turn_prompt_blocks(
                    router._signal_turn_input(route, make_event()).__class__(
                        route=route,
                        origin=TurnOrigin.SIGNAL,
                        dedupe_sender_id="sender",
                        dedupe_timestamp=1,
                        session=router._signal_turn_input(route, make_event()).session,
                    ),
                    [],
                )
            with self.assertRaisesRegex(ValueError, "synthetic turn requires definition"):
                router._build_turn_prompt_blocks(
                    router._signal_turn_input(route, make_event()).__class__(
                        route=route,
                        origin=TurnOrigin.SCHEDULED_JOB,
                        dedupe_sender_id="scheduled:job",
                        dedupe_timestamp=1,
                        session=router._signal_turn_input(route, make_event()).session,
                    ),
                    [],
                )

    async def test_route_lock_helper_covers_waiting_and_timeout_paths(self) -> None:
        lock = asyncio.Lock()
        self.assertTrue(await SignalHermesRouter._acquire_route_lock(lock, None))
        lock.release()

        await lock.acquire()
        try:
            self.assertFalse(await SignalHermesRouter._acquire_route_lock(lock, 0.001))
        finally:
            lock.release()

    async def test_number_only_direct_event_routes_with_canonical_route_sender_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            route = make_direct_route()
            dedupe = DedupeStore()
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
                dedupe=dedupe,
            )

            await router.handle_raw_event(make_direct_raw(source_uuid=None, timestamp=1))
            await router.handle_raw_event(make_direct_raw(source_uuid="sender-uuid", timestamp=1))
            await router.handle_raw_event(make_direct_raw(source_uuid="sender-uuid", timestamp=2))

            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1"])
            self.assertEqual(
                signal.direct_sends,
                [("sender-uuid", "reply"), ("sender-uuid", "reply")],
            )
            self.assertFalse(dedupe.claim(route.key, "sender-uuid", 1))
            self.assertTrue(dedupe.claim(route.key, "+00000000000", 1))

    async def test_unknown_direct_sender_is_discarded_before_parsing_attachments(self) -> None:
        private_payload = base64.b64encode(b"x" * 1024).decode("ascii")
        route = make_direct_route()
        raw = make_direct_raw(
            source_uuid="unknown-sender-uuid",
            source_number="+00000000000",
            attachments=[{"filename": "private.txt", "data": private_payload}],
        )
        with tempfile.TemporaryDirectory() as tmp:
            dedupe = DedupeStore()
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                        max_attachment_bytes=10,
                    ),
                    routes=(route,),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
            from signal_hermes_router import router as router_module

            parse_calls: list[bool] = []
            real_parse = router_module.parse_signal_event

            def spying_parse(*args, **kwargs):  # type: ignore[no-untyped-def]
                parse_calls.append(True)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "parse_signal_event", spying_parse):
                result = await router.handle_raw_event(raw)

            self.assertIsNone(result)
            self.assertEqual(parse_calls, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.direct_sends, [])
            self.assertEqual(profile.prompts, [])
            self.assertTrue(dedupe.claim(route.key, "unknown-sender-uuid", 1))
            self.assertEqual(list((Path(tmp) / "media").rglob("*")), [])

    async def test_active_route_with_unmarked_blank_reply_records_failure_without_restart(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = " \t\n"
            dedupe = DedupeStore()
            supervisor = FakeSupervisor(profile)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=dedupe,
            )

            await router.handle_event(make_event())

            self.assertEqual(
                signal.sends,
                [("group", "I hit an internal router error handling that message.")],
            )
            self.assertEqual(dedupe.status("signal:group", "sender", 1), "handled")
            last_failure_at_ms, failure = router._last_failures["signal:group"]
            self.assertGreater(last_failure_at_ms, 0)
            self.assertEqual(failure.code, FailureCode.ACP_EMPTY_RESPONSE)
            self.assertEqual(router.circuit.failure_count("signal:group"), 0)
            self.assertEqual(supervisor.restarts, 0)
            self.assertEqual(
                router._route_status_response({})["routes"][0]["last_failure"]["code"],
                "acp_empty_response",
            )

    async def test_blank_reply_uses_route_failure_reply_override_and_allows_suppression(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for route_failure_reply, expected_sends in (
                ("Route-specific fallback.", [("group", "Route-specific fallback.")]),
                ("", []),
            ):
                with self.subTest(route_failure_reply=route_failure_reply):
                    signal = FakeSignal()
                    profile = FakeProfile()
                    profile.reply_text = ""
                    route = make_route(failure_reply=route_failure_reply)
                    router = SignalHermesRouter(
                        make_app(tmp, RouteState.ACTIVE, routes=(route,)),
                        signal_client=signal,  # type: ignore[arg-type]
                        supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                        dedupe=DedupeStore(),
                    )

                    await router.handle_event(make_event())

                    self.assertEqual(signal.sends, expected_sends)

    async def test_sentinel_reply_suppresses_send_and_marks_handled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = NO_REPLY_SENTINEL
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router

            with self.assertLogs("signal_hermes_router.router", level="INFO") as logs:
                result = await router.handle_event(make_event())

            # The turn is delivered/recorded normally; only the send is skipped.
            self.assertEqual(result, TurnResult(NO_REPLY_SENTINEL))
            self.assertEqual(signal.sends, [])
            self.assertEqual(dedupe.status("signal:group", "sender", 1), "handled")
            suppression_lines = [
                line for line in logs.output if "profile emitted no-reply sentinel" in line
            ]
            self.assertEqual(len(suppression_lines), 1)
            # Redaction-safe: the raw route key must not appear in the line.
            self.assertNotIn("signal:group", suppression_lines[0])

    async def test_whitespace_padded_sentinel_reply_suppresses_send(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = f"  \n{NO_REPLY_SENTINEL}\t\n "
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router

            await router.handle_event(make_event())

            self.assertEqual(signal.sends, [])
            self.assertEqual(dedupe.status("signal:group", "sender", 1), "handled")

    async def test_reply_embedding_sentinel_is_delivered_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = f"Quiet day so far. {NO_REPLY_SENTINEL}"
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router

            result = await router.handle_event(make_event())

            self.assertEqual(result, TurnResult(profile.reply_text))
            self.assertEqual(signal.sends, [("group", profile.reply_text)])

    async def test_unrouted_and_duplicate_events_do_not_call_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router

            self.assertIsNone(
                await router.handle_event(
                    NormalizedEvent(
                        platform="signal",
                        group_id="missing-group",
                        sender_id="sender",
                        source_uuid="sender",
                        timestamp=1,
                        text="hello",
                    )
                )
            )
            await router.handle_event(make_event())
            self.assertIsNone(await router.handle_event(make_event()))

            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(signal.sends, [("group", "reply")])

    async def _check_canary_prefix(self, reply_text: str, expected: str) -> None:
        route_context = {"label": "synthetic", "canary_reply_prefix": "[router-canary]"}
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = reply_text
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, route_context=route_context),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(make_event())
            self.assertEqual(signal.sends, [("group", expected)])

    async def test_canary_reply_prefix_is_idempotent(self) -> None:
        for reply_text, expected in (
            ("reply", "[router-canary] reply"),
            ("   reply", "[router-canary] reply"),
            ("[router-canary] reply", "[router-canary] reply"),
            ("   [router-canary] reply", "[router-canary] reply"),
        ):
            with self.subTest(reply_text=reply_text):
                await self._check_canary_prefix(reply_text, expected)

    async def test_canary_reply_prefix_applies_to_maintenance_reply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.MAINTENANCE,
                    route_context={
                        "label": "synthetic",
                        "canary_reply_prefix": "[router-canary]",
                    },
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(make_event())
            self.assertEqual(
                signal.sends,
                [("group", "[router-canary] This route is temporarily under maintenance.")],
            )

    async def test_outgoing_reply_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 100
            base = make_app(tmp, RouteState.ACTIVE)
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                    max_reply_chars=50,
                ),
                routes=base.routes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event())

            self.assertEqual(len(signal.sends[0][1]), 50)
            self.assertTrue(signal.sends[0][1].endswith("[truncated by signal-hermes-router]"))

    async def test_routine_non_group_events_are_discarded_at_debug(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender",
                "timestamp": 1,
                "typingMessage": {"action": "STARTED", "timestamp": 1},
            },
            "account": "account",
        }
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as debug_logs:
                await router.handle_raw_event(raw)
            output = "\n".join(debug_logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertIn("message_type=typingMessage", output)
            with self.assertNoLogs("signal_hermes_router.router", level="INFO"):
                await router.handle_raw_event(
                    {
                        "envelope": {
                            "sourceUuid": "sender",
                            "timestamp": 2,
                            "receiptMessage": {"when": 2},
                        },
                        "account": "account",
                    }
                )

    async def _check_discard_event(self, raw: dict[str, Any], expected: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as logs:
                await router.handle_raw_event(raw)
            output = "\n".join(logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertIn(expected, output)
            self.assertNotIn("synthetic direct message without group", output)

    async def test_non_group_data_and_unknown_events_are_discarded_at_debug(self) -> None:
        for raw, expected in (
            (
                {
                    "envelope": {
                        "sourceUuid": "sender",
                        "timestamp": 1,
                        "dataMessage": {"message": "synthetic direct message without group"},
                    },
                    "account": "account",
                },
                "message_type=dataMessage",
            ),
            ({"not": "a signal envelope"}, "shape=unknown message_type=none"),
        ):
            with self.subTest(expected=expected):
                await self._check_discard_event(raw, expected)

    async def test_non_group_debug_discard_does_not_emit_private_payloads(self) -> None:
        private_payload = base64.b64encode(b"x" * 1024).decode("ascii")
        raw = {
            "envelope": {
                "source": "+00000000000",
                "sourceUuid": "synthetic-sender",
                "timestamp": 1,
                "dataMessage": {
                    "message": "private +00000000000",
                    "attachments": [{"data": private_payload}],
                },
            },
            "account": "+00000000000",
        }
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as logs:
                await router.handle_raw_event(raw)
            output = "\n".join(logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertIn("message_type=dataMessage", output)
            self.assertIsNone(re.search(r"\+[0-9]{8,}", output))
            self.assertIsNone(re.search(r"[A-Za-z0-9+/]{64,}={0,2}", output))

    async def test_receive_exception_discard_is_warning_without_private_payload(self) -> None:
        raw = {
            "envelope": {
                "source": "+00000000000",
                "sourceUuid": "synthetic-sender",
                "timestamp": 1,
            },
            "exception": {
                "message": "private exception detail +00000000000",
                "type": "RuntimeException",
            },
            "account": "+00000000000",
        }
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="WARNING") as logs:
                await router.handle_raw_event(raw)
            output = "\n".join(logs.output)
            self.assertIn("discarding Signal event with receive exception", output)
            self.assertIn("message_type=unknown", output)
            self.assertIn("has_exception=true", output)
            self.assertNotIn("private exception detail", output)
            self.assertIsNone(re.search(r"\+[0-9]{8,}", output))
            self.assertNotIn("synthetic-sender", output)

    async def test_unrouteable_group_event_is_discarded_without_parsing(self) -> None:
        # Inline attachment large enough that parse_signal_event would reject it
        # under the tiny max_attachment_bytes configured below.
        private_payload = base64.b64encode(b"x" * 1024).decode("ascii")
        group_id = "GROUP_ID_EXAMPLE_UNROUTED"
        source_uuid = "synthetic-sender-uuid"
        raw = {
            "envelope": {
                "sourceUuid": source_uuid,
                "timestamp": 1,
                "dataMessage": {
                    "message": "private message text",
                    "groupInfo": {"groupId": group_id},
                    "attachments": [
                        {
                            "contentType": "text/plain",
                            "filename": "private.txt",
                            "data": private_payload,
                        }
                    ],
                },
            },
            "account": "+00000000000",
        }
        with tempfile.TemporaryDirectory() as tmp:
            dedupe = DedupeStore()
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, max_attachment_bytes=10),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
            parse_calls: list[bool] = []

            from signal_hermes_router import router as router_module

            real_parse = router_module.parse_signal_event

            def spying_parse(*args, **kwargs):  # type: ignore[no-untyped-def]
                parse_calls.append(True)
                return real_parse(*args, **kwargs)

            with patch.object(router_module, "parse_signal_event", spying_parse):
                with self.assertLogs("signal_hermes_router.router", level="DEBUG") as logs:
                    result = await router.handle_raw_event(raw)
            self.assertIsNone(result)
            self.assertEqual(parse_calls, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(profile.prompts, [])
            self.assertTrue(dedupe.claim(f"signal:{group_id}", source_uuid, 1))
            output = "\n".join(logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertNotIn(group_id, output)
            self.assertNotIn(source_uuid, output)
            self.assertNotIn("private message text", output)
            self.assertNotIn("private.txt", output)
            self.assertIsNone(re.search(r"[A-Za-z0-9+/]{64,}={0,2}", output))
            media_files = list((Path(tmp) / "media").rglob("*"))
            self.assertEqual(media_files, [])

    async def test_unrouteable_normalized_event_is_discarded_before_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dedupe = DedupeStore()
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router
            source_uuid = "synthetic-sender-uuid"
            event = NormalizedEvent(
                platform="signal",
                group_id="missing-group",
                sender_id="synthetic-sender",
                source_uuid=source_uuid,
                timestamp=1,
                text="private message",
                attachments=(
                    SignalAttachment(
                        content_type="text/plain",
                        filename="private.txt",
                        body=b"private body",
                    ),
                ),
            )
            with self.assertLogs("signal_hermes_router.router", level="DEBUG") as logs:
                result = await router.handle_event(event)
            self.assertIsNone(result)
            self.assertEqual(signal.sends, [])
            self.assertEqual(profile.prompts, [])
            self.assertTrue(dedupe.claim("signal:missing-group", source_uuid, 1))
            output = "\n".join(logs.output)
            self.assertIn("discarding unrouted Signal event", output)
            self.assertNotIn("missing-group", output)
            self.assertNotIn("synthetic-sender", output)
            self.assertNotIn(source_uuid, output)
            self.assertNotIn("private message", output)
            self.assertNotIn("private.txt", output)
            media_files = list((Path(tmp) / "media").rglob("*"))
            self.assertEqual(media_files, [])

    async def test_shadow_disabled_and_maintenance_do_not_call_backend(self) -> None:
        for state in (RouteState.SHADOW, RouteState.DISABLED, RouteState.MAINTENANCE):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                signal = FakeSignal()
                profile = FakeProfile()
                router = SignalHermesRouter(
                    make_app(tmp, state),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                    dedupe=DedupeStore(),
                )
                await router.handle_event(make_event())
                self.assertEqual(profile.prompts, [])
                if state == RouteState.MAINTENANCE:
                    self.assertEqual(len(signal.sends), 1)
                else:
                    self.assertEqual(signal.sends, [])

    async def test_only_shadow_and_active_routes_store_media(self) -> None:
        for state, should_store in (
            (RouteState.SHADOW, True),
            (RouteState.ACTIVE, True),
            (RouteState.MAINTENANCE, False),
            (RouteState.DISABLED, False),
        ):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                signal = FakeSignal()
                profile = FakeProfile()
                router = SignalHermesRouter(
                    make_app(tmp, state),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                    dedupe=DedupeStore(),
                )
                event = NormalizedEvent(
                    platform="signal",
                    group_id="group",
                    sender_id="sender",
                    source_uuid="sender",
                    timestamp=10,
                    text="file",
                    attachments=(
                        SignalAttachment(
                            content_type="text/plain",
                            filename="note.txt",
                            body=b"hello",
                        ),
                    ),
                )
                await router.handle_event(event)
                media_files = list((Path(tmp) / "media").rglob("note.txt"))
                self.assertEqual(bool(media_files), should_store)

    async def test_signal_attachment_id_resolves_from_configured_attachment_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            attachment_root = Path(tmp) / "signal-attachments"
            attachment_root.mkdir()
            (attachment_root / "attachment-id").write_bytes(b"from signal store")
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.SHADOW),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            event = NormalizedEvent(
                platform="signal",
                group_id="group",
                sender_id="sender",
                source_uuid="sender",
                timestamp=10,
                text="file",
                attachments=(
                    SignalAttachment(
                        content_type="text/plain",
                        filename="note.txt",
                        signal_id="attachment-id",
                    ),
                ),
            )
            await router.handle_event(event)
            media_files = list((Path(tmp) / "media").rglob("note.txt"))
            self.assertEqual(len(media_files), 1)
            self.assertEqual(media_files[0].read_bytes(), b"from signal store")

    async def test_invalid_signal_attachment_id_releases_dedupe_claim_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.SHADOW),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            event = NormalizedEvent(
                platform="signal",
                group_id="group",
                sender_id="sender",
                source_uuid="sender",
                timestamp=10,
                text="file",
                attachments=(
                    SignalAttachment(
                        content_type="text/plain",
                        filename="note.txt",
                        signal_id="../bad",
                    ),
                ),
            )

            with self.assertRaisesRegex(ValueError, "invalid Signal attachment id"):
                await router.handle_event(event)
            self.assertTrue(router.dedupe.claim("signal:group", "sender", 10))

    async def test_send_and_typing_failures_are_logged_without_raising(self) -> None:
        class BrokenSignal:
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                raise RuntimeError("send failed")

            async def send_typing(self, group_id: str, enabled: bool) -> dict:
                raise RuntimeError("typing failed")

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=BrokenSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            route = router.config.routes[0]

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                self.assertFalse(await router._send_once(route, "reply"))
            await router._typing(route, True)

    async def test_turn_failure_restarts_profile_and_trips_circuit(self) -> None:
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
            self.assertEqual(supervisor.restarts, 1)
            self.assertEqual(profile.resumes, 1)
            self.assertEqual(router.route_state_overrides["signal:group"], RouteState.MAINTENANCE)
            self.assertEqual(signal.sends[0][1], "This route is temporarily under maintenance.")
            self.assertEqual(signal.typing, [("group", True), ("group", False)])

    async def test_turn_failure_recovery_preserves_cancellation(self) -> None:
        class CancellingSupervisor(FakeSupervisor):
            async def restart_profile(self, profile_name: str) -> None:
                raise asyncio.CancelledError()

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=CancellingSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertRaises(asyncio.CancelledError):
                await router.handle_event(make_event())

    async def test_turn_failure_recovery_works_with_real_profile_supervisor(self) -> None:
        # Regression test for the W3 reviewer-found bug: with the real
        # ProfileSupervisor (not FakeSupervisor), a prompt failure must
        # still produce a failure reply and record a circuit failure. The
        # supervisor's restart cooldown must NOT block the immediately-
        # following replace_after_restart -> get_profile call.
        from signal_hermes_router.sessions import ProfileSupervisor

        prompts: list[int] = []

        class FailingProfile:
            def __init__(self, *_args, **_kwargs) -> None:
                self.profile = "profile"
                self.work_root = Path(".")
                self.command = None
                self.max_line_bytes = None
                self.prompt_timeout_seconds = 300.0
                self.agent_capabilities = {"sessionCapabilities": {"resume": True}}
                self.permission_policies: dict = {}
                self.prompt_locks: dict = {}

            async def start(self) -> None:
                return None

            async def close(self) -> None:
                return None

            async def new_session(self, cwd: Path) -> str:
                cwd.mkdir(parents=True, exist_ok=True)
                return "session-1"

            async def resume_session(self, session_id: str, cwd: Path) -> bool:
                return True

            def set_permission_policy(self, *_args, **_kwargs) -> None:
                return None

            def release_session(self, *_args, **_kwargs) -> None:
                return None

            def exit_suspected(self) -> bool:
                return False

            async def prompt(self, session_id: str, blocks: list[dict]):
                prompts.append(1)
                raise RuntimeError("prompt boom")

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                restart_cooldown_seconds=60,  # production-like; reproduces the bug
            )
            from signal_hermes_router import sessions as sessions_module

            with patch.object(sessions_module, "ACPProfile", FailingProfile):
                router = SignalHermesRouter(
                    make_app(tmp, RouteState.ACTIVE, failures=2),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=supervisor,
                    dedupe=DedupeStore(),
                )
                with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                    await router.handle_event(make_event())
                # The handler must have sent a failure reply, NOT crashed
                # out with an unhandled cooldown RuntimeError.
                self.assertEqual(
                    signal.sends[-1][1],
                    "I hit an internal router error handling that message.",
                )
                # Circuit failure was recorded (count=1, below the
                # configured failures=2, so no trip yet).
                self.assertNotIn("signal:group", router.route_state_overrides)
                await router.close()

    async def test_replace_after_restart_failure_still_records_circuit_and_replies(
        self,
    ) -> None:
        # Regression for the failure-handler ordering gap (bead 3vn):
        # if replace_after_restart raises after a prompt failure, the
        # circuit breaker must still be incremented and the failure_reply
        # must still be sent. Pre-fix, the exception propagated past both.
        from signal_hermes_router.sessions import ProfileSupervisor

        instances: list = []

        class ReplacementFailsProfile:
            def __init__(self, *_args, **_kwargs) -> None:
                self.profile = "profile"
                self.work_root = Path(".")
                self.command = None
                self.max_line_bytes = None
                self.prompt_timeout_seconds = 300.0
                self.agent_capabilities = {"sessionCapabilities": {"resume": True}}
                self.permission_policies: dict = {}
                self.prompt_locks: dict = {}
                instances.append(self)

            async def start(self) -> None:
                # First spawn succeeds (initial get_profile). Second spawn
                # (replace_after_restart's get_profile) fails.
                if len(instances) > 1:
                    raise RuntimeError("replacement start failed")

            async def close(self) -> None:
                return None

            async def new_session(self, cwd: Path) -> str:
                cwd.mkdir(parents=True, exist_ok=True)
                return f"session-{len(instances)}"

            async def resume_session(self, session_id: str, cwd: Path) -> bool:
                return True

            def set_permission_policy(self, *_args, **_kwargs) -> None:
                return None

            def release_session(self, *_args, **_kwargs) -> None:
                return None

            def exit_suspected(self) -> bool:
                return False

            async def prompt(self, session_id: str, blocks: list[dict]):
                raise RuntimeError("prompt boom")

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            supervisor = ProfileSupervisor(
                Path(tmp) / "work",
                restart_cooldown_seconds=60,
            )
            from signal_hermes_router import sessions as sessions_module

            with patch.object(sessions_module, "ACPProfile", ReplacementFailsProfile):
                router = SignalHermesRouter(
                    make_app(tmp, RouteState.ACTIVE, failures=2),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=supervisor,
                    dedupe=DedupeStore(),
                )
                with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                    await router.handle_event(make_event())
                # Replacement spawn DID fail (two instances attempted).
                self.assertEqual(len(instances), 2)
                # But the failure reply still landed.
                self.assertEqual(
                    signal.sends[-1][1],
                    "I hit an internal router error handling that message.",
                )
                # And the circuit breaker still recorded the failure
                # (count=1, below failures=2, so no trip yet — but the
                # next failure WILL trip, proving the counter moved).
                self.assertNotIn("signal:group", router.route_state_overrides)
                self.assertEqual(len(router.circuit._failures.get("signal:group", [])), 1)
                await router.close()

    async def test_turn_failure_before_trip_sends_failure_reply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, failures=2),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(make_event())

            self.assertEqual(
                signal.sends[0][1], "I hit an internal router error handling that message."
            )

    async def test_long_reply_is_chunked_into_multiple_sends(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router
            await router.handle_event(make_event())
            self.assertEqual(len(signal.sends), 3)
            for i, (group, body) in enumerate(signal.sends, 1):
                self.assertEqual(group, "group")
                self.assertTrue(body.startswith(f"[{i}/3] "))

    async def test_chunked_reply_with_canary_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            # Realistic reply with word boundaries so the greedy splitter does
            # not collapse onto the post-canary space as its only break point.
            profile.reply_text = "word " * 800  # 4000 ASCII bytes
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    route_context={
                        "label": "synthetic",
                        "canary_reply_prefix": "[router-canary]",
                    },
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(make_event())
            total = len(signal.sends)
            self.assertGreaterEqual(total, 2)
            self.assertTrue(signal.sends[0][1].startswith(f"[1/{total}] [router-canary] "))
            for i in range(2, total + 1):
                body = signal.sends[i - 1][1]
                self.assertTrue(body.startswith(f"[{i}/{total}] "))
                self.assertNotIn("[router-canary]", body)

    async def test_truncation_and_chunking_compose(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 10000
            base = make_app(tmp, RouteState.ACTIVE)
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                    max_reply_chars=4000,
                    max_signal_message_bytes=1900,
                ),
                routes=base.routes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            await router.handle_event(make_event())
            self.assertGreater(len(signal.sends), 1)
            self.assertTrue(signal.sends[-1][1].endswith("[truncated by signal-hermes-router]"))
            # Strip markers and concatenate to verify total truncated payload size.
            bodies = [s[1].split("] ", 1)[1] for s in signal.sends]
            self.assertLessEqual(len("".join(bodies)), 4000)

    async def test_partial_send_failure_aborts_remaining_chunks(self) -> None:
        class FailingOnSecondSignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                self.sends.append((group_id, message))
                if len(self.sends) == 2:
                    raise RuntimeError("synthetic send failure")
                return {"timestamp": 1}

        with tempfile.TemporaryDirectory() as tmp:
            signal = FailingOnSecondSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router
            with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                await router.handle_event(make_event())
            self.assertEqual(len(signal.sends), 2)
            self.assertIn("chunk 2/3", "\n".join(logs.output))

    async def test_chunked_dispatch_logs_use_redacted_route_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router
            with self.assertLogs("signal_hermes_router.router", level="INFO") as logs:
                await router.handle_event(make_event())
            joined = "\n".join(logs.output)
            self.assertIn("split reply", joined)
            self.assertIn("into 3 chunks", joined)
            # Raw group_id must never appear in logs.
            self.assertNotIn("signal:group", joined)

    async def test_restart_without_resume_capability_replaces_stale_session_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            profile.resume_available = False
            supervisor = FakeSupervisor(profile)
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, failures=3),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with (
                self.assertLogs("signal_hermes_router.router", level="ERROR"),
                self.assertLogs("signal_hermes_router.sessions", level="ERROR"),
            ):
                await router.handle_event(make_event())
            self.assertEqual(profile.resumes, 1)
            self.assertEqual(profile.new_sessions, 2)
