from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from signal_hermes_router import router as router_module
from signal_hermes_router.acp import JsonRpcError
from signal_hermes_router.config import (
    AppConfig,
    CircuitBreakerConfig,
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
    ChatType,
    NormalizedEvent,
    OutboundAttachment,
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
from signal_hermes_router.private_fs import ensure_private_dir_tree, write_private_bytes
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.sessions import ProfileSupervisor
from signal_hermes_router.outbound_media import (
    OutboundAttachmentError,
    validate_outbound_attachments,
)
from signal_hermes_router.redaction import stable_ref
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
    write_config_pair,
    make_reload_harness,
    RouterTestCase,
    ToolSurfaceProfile,
    BlockingSurfaceProfile,
    ClosedAwareSignal,
    FailBeforeSendSignal,
    MutableFailSignal,
    ToggleFailSignal,
    ReadingSignal,
)



class RouterTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_duplicate_empty_group_data_message_does_not_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            dedupe = DedupeStore()
            harness = make_router_harness(tmp, signal=signal, profile=profile, dedupe=dedupe)
            router = harness.router
            raw = make_group_raw(text="", timestamp=3)

            await router.handle_raw_event(raw)
            result = await router.handle_raw_event(raw)

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])
            self.assertEqual(dedupe.status("signal:group", "sender-uuid", 3), "handled")
            self.assertFalse(dedupe.claim("signal:group", "sender-uuid", 3))

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

    async def test_attachment_only_group_data_message_still_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            harness = make_router_harness(tmp, signal=signal, profile=profile)
            router = harness.router
            attachment = {
                "contentType": "text/plain",
                "filename": "note.txt",
                "data": base64.b64encode(b"body").decode("ascii"),
            }

            result = await router.handle_raw_event(
                make_group_raw(text="", attachments=[attachment], timestamp=5)
            )

            self.assertEqual(result, TurnResult("reply"))
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertEqual(len(profile.prompts), 1)
            self.assertTrue(profile.prompts[0][0]["text"].startswith("[route_context:"))
            self.assertEqual(len(profile.prompts[0]), 2)
            media_files = list((Path(tmp) / "media").rglob("note.txt"))
            self.assertEqual(len(media_files), 1)

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

    async def test_notification_retry_after_crash_mid_turn_delivers_after_restart(self) -> None:
        class CrashedBeforeCleanupDedupeStore(DedupeStore):
            # Skipping cleanup leaves the committed 'processing' claim on disk,
            # exactly the state a process that died mid-turn leaves behind.
            def mark_handled(self, route_key: str, source_uuid: str, timestamp: int) -> None:
                pass

            def release(self, route_key: str, source_uuid: str, timestamp: int) -> None:
                pass

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

            crashed_store = CrashedBeforeCleanupDedupeStore(dedupe_path)
            crashed_router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=crashed_store,
                clock_ms=lambda: 1714521600100,
            )
            first = await crashed_router.handle_notification(
                "backup-report",
                payload,
                idempotency_key="backup-1714521600",
            )
            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            crashed_store.close()

            restarted_signal = FakeSignal()
            restarted_router = SignalHermesRouter(
                app,
                signal_client=restarted_signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(dedupe_path),
                clock_ms=lambda: 1714521700100,
            )
            retry = await restarted_router.handle_notification(
                "backup-report",
                payload,
                idempotency_key="backup-1714521600",
            )

            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(restarted_signal.sends, [("group", "reply")])

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

    async def test_notification_attachment_freezes_bytes_before_prompt_delay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png", b"old")

            class MutatingProfile(FakeProfile):
                async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                    write_test_file(image, b"new")
                    return await super().prompt(session_id, blocks)

            class ReadingSignal(FakeSignal):
                def __init__(self) -> None:
                    super().__init__()
                    self.attachment_bodies: list[bytes] = []

                async def send_group(
                    self,
                    group_id: str,
                    message: str,
                    *,
                    attachments: Sequence[str] = (),
                ) -> dict[str, int]:
                    self.attachment_bodies.extend(Path(path).read_bytes() for path in attachments)
                    return await super().send_group(group_id, message, attachments=attachments)

            signal = ReadingSignal()
            profile = MutatingProfile()
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
            mutated_source_body = image.read_bytes()

        self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
        self.assertEqual(signal.attachment_bodies, [b"old"])
        self.assertEqual(mutated_source_body, b"new")

    async def test_freeze_and_cleanup_respect_router_owned_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            app = make_synthetic_app(tmp, route)
            source = write_test_file(Path(tmp) / "media" / "camera" / "source.png")
            frozen_path = write_test_file(
                Path(tmp) / "media" / ".outbound" / "existing" / "attachment.png"
            )
            source_attachment = OutboundAttachment(
                path=source.resolve(),
                content_type="image/png",
                size=source.stat().st_size,
            )
            router_owned_attachment = OutboundAttachment(
                path=frozen_path.resolve(),
                content_type="image/png",
                size=frozen_path.stat().st_size,
                owned_by_router=True,
            )
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            frozen = await router._freeze_outbound_attachments((router_owned_attachment,))
            router._cleanup_owned_outbound_attachments((source_attachment,))
            source_still_exists = source.exists()
            router._cleanup_owned_outbound_attachments(frozen)

            self.assertEqual(frozen, (router_owned_attachment,))
            self.assertTrue(source_still_exists)
            self.assertFalse(frozen_path.exists())

    async def test_freeze_outbound_attachments_cleans_partial_copy_when_later_file_grows(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            app = make_synthetic_app(tmp, route, max_attachment_bytes=3)
            first = write_test_file(Path(tmp) / "media" / "camera" / "first.png", b"ok")
            second = write_test_file(Path(tmp) / "media" / "camera" / "second.png", b"abcd")
            first_attachment = validate_outbound_attachments(
                [str(first)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )[0]
            second_attachment = OutboundAttachment(
                path=second.resolve(),
                content_type="image/png",
                size=app.router.max_attachment_bytes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            def validate_with_growth_race(
                raw,
                *,
                media_root: Path,
                max_bytes: int,
            ):
                if raw == [str(second.resolve())]:
                    return (second_attachment,)
                return validate_outbound_attachments(
                    raw, media_root=media_root, max_bytes=max_bytes
                )

            with (
                patch(
                    "signal_hermes_router.router.validate_outbound_attachments",
                    side_effect=validate_with_growth_race,
                ),
                self.assertRaises(OutboundAttachmentError) as raised,
            ):
                await router._freeze_outbound_attachments((first_attachment, second_attachment))

            self.assertEqual(raised.exception.error_code, "attachment_too_large")
            self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_freeze_outbound_attachment_cleans_copy_when_frozen_name_fails_validation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            app = make_synthetic_app(tmp, route)
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png.gz")
            attachment = validate_outbound_attachments(
                [str(image)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )[0]
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertRaises(OutboundAttachmentError) as raised:
                await router._freeze_outbound_attachments((attachment,))

            self.assertEqual(raised.exception.error_code, "attachment_not_image")
            self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_freeze_outbound_attachment_reports_source_removed_after_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            app = make_synthetic_app(tmp, route)
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            attachment = validate_outbound_attachments(
                [str(image)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )[0]
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            removed = False

            def validate_then_remove(
                raw,
                *,
                media_root: Path,
                max_bytes: int,
            ):
                nonlocal removed
                result = validate_outbound_attachments(
                    raw,
                    media_root=media_root,
                    max_bytes=max_bytes,
                )
                if not removed and raw == [str(image.resolve())]:
                    removed = True
                    image.unlink()
                return result

            with (
                patch(
                    "signal_hermes_router.router.validate_outbound_attachments",
                    side_effect=validate_then_remove,
                ),
                self.assertRaises(OutboundAttachmentError) as raised,
            ):
                await router._freeze_outbound_attachments((attachment,))

            self.assertEqual(raised.exception.error_code, "attachment_not_found")
            self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_freeze_outbound_attachment_reports_source_permission_error_after_validation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            app = make_synthetic_app(tmp, route)
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            attachment = validate_outbound_attachments(
                [str(image)],
                media_root=app.router.media_root,
                max_bytes=app.router.max_attachment_bytes,
            )[0]
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            original_open = Path.open

            def deny_source_open(self, *args, **kwargs):
                if self == image.resolve():
                    raise PermissionError("denied")
                return original_open(self, *args, **kwargs)

            with (
                patch.object(Path, "open", deny_source_open),
                self.assertRaises(OutboundAttachmentError) as raised,
            ):
                await router._freeze_outbound_attachments((attachment,))

            self.assertEqual(raised.exception.error_code, "attachment_not_readable")
            self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_notification_attachment_uses_fallback_when_reply_text_is_empty(self) -> None:
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
            profile.reply_text = ""
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
        self.assertTrue(outcome.reply_sent)
        self.assertEqual(signal.sends, [("group", "Image attached.")])
        self.assertEqual(signal.send_attachments[0][0], "group")
        self.assertFrozenAttachmentPath(signal.send_attachments[0][1][0], app.router.media_root)

    async def test_notification_attachment_uses_fallback_when_reply_text_is_whitespace(
        self,
    ) -> None:
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
            profile.reply_text = "   "
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
        self.assertTrue(outcome.reply_sent)
        self.assertEqual(signal.sends, [("group", "Image attached.")])
        self.assertEqual(signal.send_attachments[0][0], "group")
        self.assertFrozenAttachmentPath(signal.send_attachments[0][1][0], app.router.media_root)

    async def test_direct_notification_sends_validated_attachment_with_reply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-direct",
                chat_type=ChatType.DIRECT,
                sender_id="sender-uuid",
                sender_number="+00000000000",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_SENDER,
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
                        route_name="camera-direct",
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
        self.assertEqual(signal.direct_sends, [("sender-uuid", "person detected")])
        self.assertEqual(signal.direct_send_attachments[0][0], "sender-uuid")
        self.assertFrozenAttachmentPath(
            signal.direct_send_attachments[0][1][0],
            app.router.media_root,
        )

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

    async def test_notify_route_rejects_invalid_attachment_before_backend(self) -> None:
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
                b'{"command":"notify_route","notification_id":"camera-person",'
                b'"payload":{"camera":"front"},"attachments":["relative.png"]}\n'
            )

        self.assertEqual(
            response,
            {
                "status": "error",
                "route_state": "active",
                "synthetic_id": "camera-person",
                "synthetic_kind": "notification",
                "error": "attachment_path_not_absolute",
            },
        )
        self.assertEqual(profile.prompts, [])
        self.assertEqual(signal.sends, [])

    async def test_notify_route_state_gate_skips_before_reading_unused_attachment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.DISABLED,
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
                        "attachments": [str(Path(tmp) / "media" / "camera" / "missing.png")],
                    }
                )
            )

        self.assertEqual(
            response,
            {
                "status": "skipped",
                "route_state": "disabled",
                "synthetic_id": "camera-person",
                "synthetic_kind": "notification",
            },
        )
        self.assertEqual(profile.prompts, [])
        self.assertEqual(signal.sends, [])

    async def test_notify_route_control_line_sends_valid_attachment(self) -> None:
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
            router = SignalHermesRouter(
                app,
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
                        "attachments": [str(image)],
                    }
                )
            )

        self.assertEqual(response["status"], "delivered")
        self.assertEqual(signal.sends, [("group", "person detected")])
        self.assertEqual(signal.send_attachments[0][0], "group")
        self.assertFrozenAttachmentPath(signal.send_attachments[0][1][0], app.router.media_root)

    async def test_notify_route_freezes_attachment_before_waiting_on_route_lock(self) -> None:
        class ReadingSignal(FakeSignal):
            def __init__(self) -> None:
                super().__init__()
                self.attachment_bodies: list[bytes] = []

            async def send_group(
                self,
                group_id: str,
                message: str,
                *,
                attachments: Sequence[str] = (),
            ) -> dict[str, int]:
                self.attachment_bodies.extend(Path(path).read_bytes() for path in attachments)
                return await super().send_group(group_id, message, attachments=attachments)

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = ReadingSignal()
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
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png", b"first")
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            lock = router._route_lock(route)
            await lock.acquire()
            task = asyncio.create_task(
                router._handle_control_line(
                    encode_control_message(
                        {
                            "command": "notify_route",
                            "notification_id": "camera-person",
                            "payload": {"camera": "front"},
                            "attachments": [str(image)],
                            "timeout": 1,
                        }
                    )
                )
            )
            try:
                outbound_root = app.router.media_root / ".outbound"
                for _ in range(50):
                    if outbound_root.exists():
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(outbound_root.exists())
                write_test_file(image, b"second")
            finally:
                lock.release()
            response = await task

        self.assertEqual(response["status"], "delivered")
        self.assertEqual(signal.attachment_bodies, [b"first"])
        self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_notify_route_admitted_under_breaker_skips_attachment_freeze(self) -> None:
        # A notify-route with attachments admitted under an open breaker
        # keeps the admission-time maintenance gate at the pre-freeze: a
        # reload that cleared the override (remove + re-add ACTIVE) must not
        # drop the masked turn onto the ACTIVE-only freeze path, where an
        # attachment validation failure would replace the maintenance reply
        # with an error outcome.
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                maintenance_reply="route under repair",
            )
            signal = FakeSignal()
            profile = FakeProfile()
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
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            # Admitted under the open breaker at generation 0; a reload has
            # since cleared the override (generation 1) and the route is
            # ACTIVE again. The attachment path does not even exist: the
            # masked turn must skip validation rather than erroring on it.
            router._reload_cleared_overrides[route.key] = 1
            missing = Path(tmp) / "media" / "camera" / "gone.png"

            outcome = await router.handle_notification(
                "camera-person",
                {"camera": "front"},
                outbound_attachments=[str(missing)],
                admission_overrides={route.key: RouteState.MAINTENANCE},
                admission_generation=0,
            )

        self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
        self.assertEqual(outcome.route_state, RouteState.MAINTENANCE)
        self.assertEqual(signal.sends, [("group", "route under repair")])
        self.assertEqual(signal.send_attachments, [("group", ())])
        self.assertEqual(profile.prompts, [])

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

    async def test_notify_route_rejects_falsey_malformed_attachments(self) -> None:
        for raw_attachments in (None, "", {}):
            with self.subTest(raw_attachments=raw_attachments):
                await self._check_notify_rejects_attachment(raw_attachments)

    async def test_notify_route_rejects_attachment_with_remote_signal_daemon(self) -> None:
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
                signal_base_url="http://signal.example:8080",
                allow_remote_signal_base_url=True,
            )
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            router = SignalHermesRouter(
                app,
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
                        "attachments": [str(image)],
                    }
                )
            )

        self.assertEqual(
            response,
            {
                "status": "error",
                "route_state": "active",
                "synthetic_id": "camera-person",
                "synthetic_kind": "notification",
                "error": "attachment_signal_daemon_not_local",
            },
        )
        self.assertEqual(profile.prompts, [])
        self.assertEqual(signal.sends, [])

    async def test_notify_route_idempotent_retry_dedupes_before_attachment_validation(
        self,
    ) -> None:
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
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            request = {
                "command": "notify_route",
                "notification_id": "camera-person",
                "payload": {"camera": "front"},
                "attachments": [str(image)],
                "idempotency_key": "camera-person-1",
            }

            delivered = await router._handle_control_line(encode_control_message(request))
            image.unlink()
            retried = await router._handle_control_line(encode_control_message(request))

        self.assertEqual(delivered["status"], "delivered")
        self.assertEqual(retried["status"], "deduped")
        self.assertEqual(signal.sends, [("group", "person detected")])

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

    async def test_notification_freeze_and_dedupe_io_runs_off_loop_thread(self) -> None:
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
            dedupe = DedupeStore()
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
            loop_thread = threading.get_ident()
            copy_threads: list[int] = []
            dedupe_calls = record_dedupe_call_threads(dedupe)
            original_copy = router_module._copy_outbound_attachment

            def recording_copy(source: Path, destination: Path, max_bytes: int) -> int:
                copy_threads.append(threading.get_ident())
                return original_copy(source, destination, max_bytes)

            request = {
                "command": "notify_route",
                "notification_id": "camera-person",
                "payload": {"camera": "front"},
                "attachments": [str(image)],
                "idempotency_key": "camera-person-1",
            }
            with patch("signal_hermes_router.router._copy_outbound_attachment", recording_copy):
                delivered = await router._handle_control_line(encode_control_message(request))
                image.unlink()
                # Idempotent fast path: served from the is_handled read alone.
                retried = await router._handle_control_line(encode_control_message(request))

            self.assertEqual(delivered["status"], "delivered")
            self.assertEqual(retried["status"], "deduped")
            self.assertTrue(copy_threads)
            recorded_methods = {name for name, _ident in dedupe_calls}
            self.assertIn("is_handled", recorded_methods)
            for ident in copy_threads:
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

    async def test_notify_route_processing_idempotency_skips_attachment_validation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            notification = SyntheticRouteNotification(
                id="camera-person",
                route_name="camera-route",
                prompt="Summarize the camera alert.",
            )
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(notification,),
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            idempotency_key = "camera-person-1"
            dedupe_sender_id, dedupe_timestamp = router._synthetic_dedupe_identity(
                notification.namespace,
                scheduled_at=None,
                idempotency_key=idempotency_key,
                triggered_at_ms=0,
            )
            self.assertTrue(router.dedupe.claim(route.key, dedupe_sender_id, dedupe_timestamp))

            with patch(
                "signal_hermes_router.router.validate_outbound_attachments",
                side_effect=AssertionError("processing retry should not validate attachments"),
            ):
                response = await router._handle_control_line(
                    encode_control_message(
                        {
                            "command": "notify_route",
                            "notification_id": "camera-person",
                            "payload": {"camera": "front"},
                            "attachments": [str(Path(tmp) / "media" / "camera" / "missing.png")],
                            "idempotency_key": idempotency_key,
                        }
                    )
                )

        self.assertEqual(response["status"], "deduped")
        self.assertEqual(profile.prompts, [])
        self.assertEqual(signal.sends, [])

    async def test_notify_route_in_flight_idempotency_does_not_short_circuit_as_deduped(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="camera-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            notification = SyntheticRouteNotification(
                id="camera-person",
                route_name="camera-route",
                prompt="Summarize the camera alert.",
            )
            signal = FakeSignal()
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(notification,),
            )
            image = write_test_file(Path(tmp) / "media" / "camera" / "person.png")
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            idempotency_key = "camera-person-1"
            dedupe_sender_id, dedupe_timestamp = router._synthetic_dedupe_identity(
                notification.namespace,
                scheduled_at=None,
                idempotency_key=idempotency_key,
                triggered_at_ms=0,
            )
            self.assertTrue(router.dedupe.claim(route.key, dedupe_sender_id, dedupe_timestamp))
            lock = router._route_lock(route)
            await lock.acquire()
            image.unlink()
            try:
                response = await router._handle_control_line(
                    encode_control_message(
                        {
                            "command": "notify_route",
                            "notification_id": "camera-person",
                            "payload": {"camera": "front"},
                            "attachments": [str(image)],
                            "idempotency_key": idempotency_key,
                            "timeout": 0,
                        }
                    )
                )
            finally:
                lock.release()

        self.assertEqual(response["status"], "busy")
        self.assertEqual(signal.sends, [])

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

    async def test_synthetic_route_states_match_signal_gates(self) -> None:
        for state in (RouteState.SHADOW, RouteState.DISABLED, RouteState.MAINTENANCE):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                route = Route(
                    platform="signal",
                    name="agenda-route",
                    group_id="group",
                    profile="profile",
                    session_policy=SessionPolicy.PERSISTENT_ROUTE,
                    state=state,
                )
                signal = FakeSignal()
                profile = FakeProfile()
                router = SignalHermesRouter(
                    make_synthetic_app(tmp, route),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                    dedupe=DedupeStore(),
                )

                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

                self.assertEqual(profile.prompts, [])
                self.assertNotIn("last_failure_at_ms", outcome.to_control_response())
                if state == RouteState.MAINTENANCE:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
                    self.assertEqual(
                        signal.sends,
                        [("group", "This route is temporarily under maintenance.")],
                    )
                else:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.SKIPPED)
                    self.assertEqual(signal.sends, [])

    async def test_synthetic_maintenance_send_failure_reports_metadata(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.MAINTENANCE,
            )
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600456,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = outcome.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["route_state"], "maintenance")
            self.assertEqual(response["failure"]["code"], "signal_send_failed")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600456)
            self.assertEqual(profile.prompts, [])

    async def test_synthetic_unknown_job_and_missing_route_return_errors(self) -> None:
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
                    SyntheticRouteJob(
                        id="orphan-job",
                        route_name="missing-route",
                        prompt="Synthetic orphan prompt.",
                    ),
                ),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            unknown = await router.handle_synthetic_job("missing-job")
            orphan = await router.handle_synthetic_job("orphan-job")

            self.assertEqual(unknown.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(unknown.error, "unknown_job")
            self.assertEqual(orphan.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(orphan.error, "unknown_route")

    async def test_synthetic_job_reuses_persistent_route_session_with_signal_turn(self) -> None:
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
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            await router.handle_synthetic_job("daily-agenda", scheduled_at=2)

            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1"])
            self.assertEqual(profile.new_sessions, 1)

    async def test_synthetic_permission_override_does_not_leak_to_signal_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/route"}}}]
            )
            job_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/job"}}}]
            )
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=route_policy,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    SyntheticRouteJob(
                        id="daily-agenda",
                        route_name="agenda-route",
                        prompt="Prepare the synthetic daily agenda.",
                        permission_policy=job_policy,
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-agenda", scheduled_at=1)
            self.assertEqual(
                profile.policies, [("session-1", job_policy), ("session-1", route_policy)]
            )
            await router.handle_event(make_event(timestamp=2))

            self.assertEqual(
                profile.policies,
                [
                    ("session-1", job_policy),
                    ("session-1", route_policy),
                    ("session-1", route_policy),
                ],
            )

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

    async def test_synthetic_job_on_mcp_only_route_enforces_local_tool_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="mcp-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config(
                    [{"tool": "read_file"}], mcp_only=True
                ),
                mcp_only=True,
            )
            job = SyntheticRouteJob(
                id="daily-job",
                route_name="mcp-route",
                prompt="Run daily task",
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "bash"}]),
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route, job),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-job", scheduled_at=1)
            # The job policy (policies[0]) should have been upgraded to mcp_only=True
            self.assertTrue(profile.policies[0][1].mcp_only)
            # Verify the job policy actually rejects a local tool
            self.assertFalse(profile.policies[0][1].allows_tool_call({"toolName": "bash"}))

    async def test_synthetic_failure_recovery_resets_replacement_session_policy(self) -> None:
        class RestartingSupervisor(FakeSupervisor):
            def __init__(self, initial: FakeProfile, replacement: FakeProfile) -> None:
                super().__init__(initial)
                self.replacement = replacement

            async def restart_profile(self, profile_name: str) -> None:
                await super().restart_profile(profile_name)
                self.profile = self.replacement

        with tempfile.TemporaryDirectory() as tmp:
            route_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/route"}}}]
            )
            job_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/job"}}}]
            )
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=route_policy,
            )
            job = SyntheticRouteJob(
                id="daily-agenda",
                route_name="agenda-route",
                prompt="Prepare the synthetic daily agenda.",
                permission_policy=job_policy,
            )
            initial = FakeProfile()
            initial.fail = True
            replacement = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route, job),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=RestartingSupervisor(initial, replacement),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1)

            self.assertEqual(outcome.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(
                replacement.policies,
                [("session-1", job_policy), ("session-1", route_policy)],
            )

    async def test_synthetic_scheduled_at_and_idempotency_key_are_deduped(self) -> None:
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
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 2000,
            )

            first = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            duplicate = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            keyed_first = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="stable-fire",
            )
            keyed_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1002,
                idempotency_key="stable-fire",
            )
            scheduled_duplicate_with_new_key = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="changed-fire-id",
            )

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(keyed_first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(keyed_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(scheduled_duplicate_with_new_key.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(len(signal.sends), 2)

    async def test_synthetic_job_keeps_released_dedupe_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            dedupe = DedupeStore()
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: 2000,
            )
            key_hash = hashlib.sha256(b"stable-fire").hexdigest()[:16]
            dedupe.mark_handled(route.key, "scheduled:daily-agenda", 1000)
            dedupe.mark_handled(
                route.key,
                f"scheduled:daily-agenda:key:{key_hash}",
                0,
            )

            scheduled_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1000,
            )
            keyed_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="stable-fire",
            )

            self.assertEqual(scheduled_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(keyed_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])

    async def test_synthetic_hermes_failure_releases_dedupe_for_retry(self) -> None:
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
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            profile.fail = False
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(failed.error, "unknown")
            self.assertEqual(failed.to_control_response()["failure"]["code"], "unknown")
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(
                [send[1] for send in signal.sends],
                ["I hit an internal router error handling that message.", "reply"],
            )

    async def test_synthetic_session_model_failure_uses_model_reply_and_metadata(self) -> None:
        class SessionFailProfile(FakeProfile):
            async def new_session(self, cwd: Path) -> str:
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "session failed",
                        "data": {
                            "code": "quota_exceeded",
                            "provider_class": "cloud_api",
                            "provider_detail": "429 usage_limit_reached",
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
                    model_failure_reply="The model is unavailable right now.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(SessionFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600123,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "model_rate_limited")
            self.assertEqual(response["failure"]["code"], "model_rate_limited")
            self.assertEqual(response["failure"]["provider_class"], "cloud_api")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600123)
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "The model is unavailable right now.")])

    async def test_synthetic_session_text_only_quota_failure_uses_generic_reply(self) -> None:
        class SessionFailProfile(FakeProfile):
            async def new_session(self, cwd: Path) -> str:
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "OpenAI Codex HTTP 429 usage_limit_reached",
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
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(SessionFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["error"], "acp_session_failed")
            self.assertEqual(response["failure"]["code"], "acp_session_failed")
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Generic router fallback.")])

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

    async def test_synthetic_maintenance_retry_does_not_mark_dedupe_handled(self) -> None:
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
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    circuit_breaker=CircuitBreakerConfig(failures=1, window_seconds=60),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            maintenance = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(maintenance.status, TurnOutcomeStatus.DELIVERED)
            self.assertIsNone(router.dedupe.status("signal:group", "scheduled:daily-agenda", 1000))

            router.route_state_overrides.pop("signal:group", None)
            router._trip_times.pop("signal:group", None)
            router._trip_times_ms.pop("signal:group", None)
            profile.fail = False
            retried = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(retried.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1"])

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

    async def test_synthetic_send_failure_returns_error_and_marks_dedupe_handled(self) -> None:
        class FlakySignal(FakeSignal):
            def __init__(self) -> None:
                super().__init__()
                self.fail_send = True

            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                if self.fail_send:
                    raise RuntimeError("send failed")
                return await super().send_group(group_id, message)

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FlakySignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            signal.fail_send = False
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(failed.error, "signal_send_failed")
            response = failed.to_control_response()
            self.assertEqual(response["failure"]["code"], "signal_send_failed")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertIsInstance(response["last_failure_at_ms"], int)
            self.assertEqual(retry.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(signal.sends, [])

    async def test_synthetic_failure_reply_send_failure_preserves_root_failure(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["failure"]["code"], "unknown")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertIsInstance(response["last_failure_at_ms"], int)
            self.assertEqual(
                router._route_status_response({})["routes"][0]["last_failure"]["code"],
                "unknown",
            )

    async def test_synthetic_model_failure_with_empty_route_failure_reply_suppresses(self) -> None:
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
                    failure_reply="Router fallback should not be sent.",
                    model_failure_reply="Model fallback should not be sent.",
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

    async def test_synthetic_model_failure_trip_uses_maintenance_reply_and_metadata(
        self,
    ) -> None:
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
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    circuit_breaker=CircuitBreakerConfig(failures=1, window_seconds=60),
                    maintenance_reply="Maintenance reply.",
                    failure_reply="Generic router fallback.",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600123,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["failure"]["code"], "model_rate_limited")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600123)
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Maintenance reply.")])
            self.assertEqual(router.route_state_overrides["signal:group"], RouteState.MAINTENANCE)

    async def test_synthetic_bare_manual_triggers_are_fresh_with_same_clock(self) -> None:
        nonces = iter(("nonce-one", "nonce-two"))
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
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 2000,
                nonce_factory=lambda: next(nonces),
            )

            first = await router.handle_synthetic_job("daily-agenda")
            second = await router.handle_synthetic_job("daily-agenda")

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(second.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(len(signal.sends), 2)

    async def test_synthetic_busy_does_not_leave_dedupe_claim(self) -> None:
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
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            lock = router._route_lock(route)
            await lock.acquire()
            try:
                busy = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            finally:
                lock.release()

            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 1)

    async def test_synthetic_trigger_contends_with_in_flight_signal_turn(self) -> None:
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
            profile = FakeProfile()
            profile.prompt_delay = 0.05
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            signal_task = asyncio.create_task(router.handle_event(make_event(timestamp=1)))
            try:
                for _ in range(50):
                    if profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                busy = await router.handle_synthetic_job(
                    "daily-agenda",
                    scheduled_at=2,
                    route_lock_timeout=0,
                )
            finally:
                await signal_task

            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=2)

            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)

    async def test_synthetic_triggers_on_distinct_routes_share_profile_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            agenda = Route(
                platform="signal",
                name="agenda-route",
                group_id="group-one",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            summary = Route(
                platform="signal",
                name="summary-route",
                group_id="group-two",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            profile = FakeProfile()
            profile.prompt_delay = 0.05
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(agenda, summary),
                    scheduled_jobs=(
                        SyntheticRouteJob(
                            id="daily-agenda",
                            route_name="agenda-route",
                            prompt="Prepare the synthetic daily agenda.",
                        ),
                        SyntheticRouteJob(
                            id="daily-summary",
                            route_name="summary-route",
                            prompt="Prepare the synthetic daily summary.",
                        ),
                    ),
                ),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            first_task = asyncio.create_task(
                router.handle_synthetic_job("daily-agenda", scheduled_at=1)
            )
            try:
                for _ in range(50):
                    if profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                busy = await router.handle_synthetic_job(
                    "daily-summary",
                    scheduled_at=2,
                    route_lock_timeout=0,
                )
            finally:
                first = await first_task
            retry = await router.handle_synthetic_job(
                "daily-summary",
                scheduled_at=2,
                route_lock_timeout=0,
            )

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)

    async def test_control_line_maps_requests_to_synthetic_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            signal = FakeSignal()
            profile = FakeProfile()
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
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            self.assertEqual(
                await router._handle_control_line(b"not-json\n"),
                {"status": "error", "error": "malformed_json"},
            )
            self.assertEqual(
                await router._handle_control_line(b'{"command":"missing"}\n'),
                {"status": "error", "error": "unknown_command"},
            )
            preflight = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )
            self.assertEqual(preflight["status"], "failed")
            self.assertEqual(preflight["expected_permissions_count"], 1)
            self.assertEqual(
                preflight["probe_errors"],
                [
                    {
                        "profile": "profile",
                        "code": "probe_unsupported",
                        "error": "probe_unsupported",
                    }
                ],
            )
            self.assertEqual(preflight["issues"][0]["code"], "probe_unsupported")
            self.assertEqual(preflight["failure"]["code"], "preflight_failed")
            self.assertNotIn("group", json.dumps(preflight, sort_keys=True))
            response = await router._handle_control_line(
                b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000}\n'
            )

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")
            response = await router._handle_control_line(
                b'{"command":"notify_route","notification_id":"backup-report",'
                b'"payload":{"status":"ok"},"idempotency_key":"backup-1"}\n'
            )

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["synthetic_id"], "backup-report")
            self.assertEqual(response["synthetic_kind"], "notification")
            self.assertEqual(signal.sends, [("group", "reply"), ("group", "reply")])

    async def test_control_preflight_uses_profile_tool_surface(self) -> None:
        class ToolSurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config(
                    [{"tool": "read_file"}, {"tool": "web_search"}]
                ),
            )
            profile = ToolSurfaceProfile()
            app = make_synthetic_app(tmp, route)
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["probe_errors"], [])
        self.assertEqual(response["missing_tools_count"], 1)
        self.assertEqual(response["missing_tools"][0]["tool"], "web_search")
        self.assertEqual(response["issues"][0]["code"], "missing_tool")
        self.assertEqual(response["failure"]["code"], "permission_denied")
        self.assertNotIn("EXAMPLE_GROUP", json.dumps(response, sort_keys=True))

    async def test_control_preflight_reports_local_tools_exposed_on_mcp_only_route(self) -> None:
        from signal_hermes_router.preflight import ToolSurface

        class LocalToolProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                return ToolSurface.from_names(
                    self.profile,
                    ["web_search", "bash"],
                    schema_version=1,
                    scope="full_callable",
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="mcp-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                mcp_only=True,
            )
            app = make_synthetic_app(tmp, route)
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(LocalToolProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["probe_errors"], [])
        self.assertEqual(response["missing_tools_count"], 0)
        self.assertEqual(response["local_tools_exposed_count"], 1)
        self.assertEqual(response["local_tools_exposed"][0]["tool"], "bash")
        self.assertEqual(response["issues"][0]["code"], "local_tool_exposed")
        self.assertEqual(response["failure"]["code"], "permission_denied")
        self.assertNotIn("EXAMPLE_GROUP", json.dumps(response, sort_keys=True))

    async def test_control_preflight_reports_busy_profile_without_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            lock = router._profile_lock("profile")
            await lock.acquire()
            try:
                response = await router._handle_control_line(
                    b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
                )
            finally:
                lock.release()

        self.assertEqual(response["status"], "failed")
        self.assertEqual(
            response["probe_errors"],
            [
                {
                    "profile": "profile",
                    "code": "probe_profile_busy",
                    "error": "probe_profile_busy",
                }
            ],
        )
        self.assertEqual(response["issues"][0]["code"], "probe_profile_busy")
        self.assertEqual(response["failure"]["code"], "preflight_failed")

    async def test_control_preflight_reports_supervisor_probe_failure_without_leaking(
        self,
    ) -> None:
        class FailingSupervisor:
            async def get_profile(self, _route: Route) -> FakeProfile:
                raise RuntimeError("private-startup-token")

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FailingSupervisor(),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(
            response["probe_errors"],
            [{"profile": "profile", "code": "probe_failed", "error": "RuntimeError"}],
        )
        self.assertEqual(response["issues"][0]["code"], "probe_failed")
        self.assertEqual(response["failure"]["code"], "preflight_failed")
        self.assertNotIn("private-startup-token", json.dumps(response, sort_keys=True))

    async def test_route_status_reports_health_without_private_route_values(self) -> None:
        class DetailFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise RuntimeError(
                    "failed for EXAMPLE_STATUS_GROUP at https://signal.test/v1 "
                    "/private/work/session +00000000000 [route_context:begin]"
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_STATUS_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            success_profile = FakeProfile()
            supervisor = FakeSupervisor(success_profile)
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            supervisor.profile = DetailFailProfile()
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_synthetic_job("daily-agenda", scheduled_at=1001)

            response = await router._handle_control_line(
                b'{"command":"route_status","routes":["agenda-route"]}\n'
            )

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["route_count"], 1)
        route_status = response["routes"][0]
        self.assertEqual(route_status["route_ref"], "route:agenda-route")
        self.assertEqual(route_status["route_state"], "active")
        self.assertEqual(route_status["session"]["cached_sessions"], 1)
        self.assertEqual(route_status["circuit"]["failure_count"], 1)
        self.assertIn("last_success_at_ms", route_status)
        self.assertEqual(route_status["last_failure"]["code"], "unknown")
        serialized = json.dumps(response, sort_keys=True)
        self.assertNotIn("EXAMPLE_STATUS_GROUP", serialized)
        self.assertNotIn("+00000000000", serialized)
        self.assertNotIn("signal.test", serialized)
        self.assertNotIn("/private/work", serialized)
        self.assertNotIn("route_context", serialized)

    async def test_control_line_rejects_invalid_request_fields_and_reports_exceptions(self) -> None:
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

            cases = (
                (b"[]\n", "malformed_request"),
                (b'{"command":"trigger_job"}\n', "missing_job_id"),
                (b'{"command":"notify_route"}\n', "missing_notification_id"),
                (
                    b'{"command":"notify_route","notification_id":"backup-report"}\n',
                    "missing_payload",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":"not-object-or-array"}\n',
                    "invalid_payload",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":"bad"}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":-1}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":true}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000.9}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","idempotency_key":1}\n',
                    "invalid_idempotency_key",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","idempotency_key":""}\n',
                    "invalid_idempotency_key",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","timeout":"bad"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","timeout":-1}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"nan"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"inf"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"-inf"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"preflight_permissions","scope":{"active_only":"yes"}}\n',
                    "invalid_preflight_scope",
                ),
            )
            for payload, error in cases:
                with self.subTest(error=error):
                    self.assertEqual(
                        await router._handle_control_line(payload),
                        {"status": "error", "error": error},
                    )

            async def fail_trigger(*_args, **_kwargs):
                raise RuntimeError("synthetic")

            router.handle_synthetic_job = fail_trigger  # type: ignore[method-assign]
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                response = await router._handle_control_line(
                    b'{"command":"trigger_job","job_id":"daily-agenda"}\n'
                )

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_error")
            self.assertEqual(response["failure"]["code"], "router_error")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_error")
            self.assertEqual(response["failure"]["code"], "router_error")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")

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

    async def test_control_request_admitted_before_shutdown_is_not_bounced(self) -> None:
        harness = make_router_harness(tempfile.mkdtemp())
        line = b'{"command":"route_status"}'
        # Admission is pinned when _run_control_request schedules the request
        # task: one loop tick lets the outer task run and pin admitted=True,
        # while this task's continuation (FIFO) still runs before the freshly
        # queued request task, so begin_shutdown() lands in between.
        request = asyncio.ensure_future(harness.router._run_control_request(line))
        await asyncio.sleep(0)
        harness.router.begin_shutdown()
        response = await request
        self.assertEqual(response["status"], "ok")
        # A request that arrives after shutdown begins still gets busy, so
        # the caller retries on the replacement router.
        response = await harness.router._run_control_request(line)
        self.assertEqual(response["status"], "busy")
        self.assertEqual(response["error"], "router_shutting_down")

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

    async def test_control_turn_accepted_under_breaker_keeps_mask_after_reload(self) -> None:
        # The control-socket acceptor pins the breaker override map and
        # config generation when it ACCEPTS a request. The request task may
        # not build its turn until after a reload removed and re-added the
        # route (clearing the override); the admission-time maintenance gate
        # must still apply, exactly as for Signal turns.
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
scheduled_jobs:
  - id: j1
    route: r1
    prompt: job prompt
"""
            routes_path.write_text(active_routes, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            # The breaker is open when the control request is accepted.
            harness.router.route_state_overrides[r1.key] = RouteState.MAINTENANCE
            harness.router._trip_times[r1.key] = time.monotonic()
            # The accepted turn queues behind the held route lock.
            await harness.router._route_locks[r1.key].acquire()
            request_task = asyncio.ensure_future(
                harness.router._run_control_request(
                    b'{"command":"trigger_job","job_id":"j1","timeout":5}'
                )
            )
            for _ in range(100):
                if harness.router._control_request_tasks:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(harness.router._control_request_tasks)
            # Reload 1 removes the route (clearing the override), reload 2
            # re-adds it ACTIVE — all before the accepted turn runs.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            routes_path.write_text(active_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            harness.router._route_locks[r1.key].release()
            response = await asyncio.wait_for(request_task, timeout=5)
            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "maintenance")
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
                    deadline = time.monotonic() + 5
                    while not parse_entered.is_set():
                        self.assertLess(time.monotonic(), deadline)
                        await asyncio.sleep(0.01)
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

    async def test_notify_route_attributes_before_dedupe_await(self) -> None:
        # An idempotent notify_route request resolves its route and then
        # awaits the dedupe worker BEFORE the notification turn is built: it
        # must be attributed with the route key at that point. A slow or
        # wedged dedupe check would otherwise leave a concurrent reload
        # reaper to read the request as unattributed and conservatively
        # contest every drain key with it.
        entered = threading.Event()
        block = threading.Event()

        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(name="r1")
            app = make_app(
                tmp,
                RouteState.ACTIVE,
                routes=(route,),
                notifications=(
                    SyntheticRouteNotification(
                        id="n1",
                        route_name="r1",
                        prompt="Summarize the notification payload.",
                    ),
                ),
            )
            harness = make_router_harness(tmp, app=app)
            real_is_handled = harness.dedupe.is_handled

            def blocking_is_handled(*args: Any, **kwargs: Any) -> Any:
                entered.set()
                block.wait()
                return real_is_handled(*args, **kwargs)

            harness.dedupe.is_handled = blocking_is_handled  # type: ignore[method-assign]
            request = asyncio.ensure_future(
                harness.router._run_control_request(
                    b'{"command":"notify_route","notification_id":"n1",'
                    b'"payload":{"status":"ok"},"idempotency_key":"k1"}'
                )
            )
            try:
                # The request is now parked inside the dedupe worker read,
                # before handle_notification could attribute the turn.
                deadline = time.monotonic() + 5
                while not entered.is_set():
                    self.assertLess(time.monotonic(), deadline)
                    await asyncio.sleep(0.01)
                parked = [t for t in harness.router._control_request_tasks if not t.done()]
                self.assertEqual(len(parked), 1)
                self.assertEqual(harness.router._turn_task_route_key(parked[0]), route.key)
            finally:
                block.set()
            response = await asyncio.wait_for(request, timeout=5)
            self.assertEqual(response["status"], "delivered")

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
            deadline = time.monotonic() + 5
            while harness.supervisor.retired != ["p1"]:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
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
            deadline = time.monotonic() + 5
            while harness.supervisor.retired != ["profile"]:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
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
            deadline = time.monotonic() + 5
            while harness.supervisor.retired != ["profile"]:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
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
            deadline = time.monotonic() + 5
            while harness.supervisor.retired != ["profile"]:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
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
            deadline = time.monotonic() + 5
            while set(harness.router.sessions._sessions):
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
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

    async def test_preflight_probes_use_admission_config_snapshot(self) -> None:
        # The preflight report is computed from one config snapshot, so every
        # per-profile probe must resolve its representative route from that
        # SAME snapshot: a reload swapping self.config mid-preflight must not
        # make a later probe miss its route (probe_profile_missing) or probe
        # a different one.
        p1_entered = asyncio.Event()
        p1_block = asyncio.Event()
        p2_entered = asyncio.Event()

        class BlockingSurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                p1_entered.set()
                await p1_block.wait()
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        class SurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                p2_entered.set()
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        class MappingSupervisor(FakeSupervisor):
            def __init__(self, profiles: dict[str, FakeProfile]) -> None:
                super().__init__(FakeProfile())
                self.profiles = profiles

            async def get_profile(self, route: Route) -> FakeProfile:
                return self.profiles[route.profile]

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
            routes_text = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    permissions:
      - tool: read_file
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
    permissions:
      - tool: read_file
"""
            routes_path.write_text(routes_text, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            p1 = BlockingSurfaceProfile()
            p1.profile = "p1"
            p2 = SurfaceProfile()
            p2.profile = "p2"
            supervisor = MappingSupervisor({"p1": p1, "p2": p2})
            harness = make_router_harness(tmp, app=app, supervisor=supervisor)
            harness.router.set_config_paths(config_path, routes_path)
            # Nothing is cached, so the reload's retire loop stays inert and
            # only the probe's route resolution is under test.

            preflight_task = asyncio.ensure_future(
                harness.router._run_control_request(b'{"command":"preflight_permissions"}')
            )
            # Profiles probe sequentially in sorted order: p1 parks inside
            # its tool-surface read, so p2's probe has not started yet. Pin
            # that ordering premise: if probing ever becomes concurrent this
            # test would go vacuous instead of failing.
            await asyncio.wait_for(p1_entered.wait(), timeout=5)
            self.assertFalse(p2_entered.is_set())
            # A reload swaps self.config, removing r2 BEFORE p2's probe
            # resolves its representative route.
            routes_path.write_text(
                routes_text.replace(
                    """  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
    permissions:
      - tool: read_file
""",
                    "",
                ),
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            if reapers:
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            p1_block.set()
            response = await asyncio.wait_for(preflight_task, timeout=5)
            # Both probes resolved their routes from the admission snapshot:
            # p2's probe does not see the swapped config and does not report
            # probe_profile_missing.
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["probe_errors"], [])
            self.assertEqual(response["missing_tools_count"], 0)
            self.assertTrue(p2_entered.is_set())
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

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
            deadline = time.monotonic() + 5
            while harness.router.sessions._sessions and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
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
            deadline = time.monotonic() + 5
            while harness.router.sessions._sessions and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
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

    async def test_notify_route_preserves_admitted_config_across_reload(self) -> None:
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

notifications:
  - id: n1
    route: r1
    prompt: original prompt
""",
                encoding="utf-8",
            )
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)

            # Slow the dedupe claim so a reload lands mid-request, after the
            # notification/route were read for the claim but before the turn.
            original_is_handled = harness.dedupe.is_handled

            def slow_is_handled(*args: Any, **kwargs: Any) -> bool:
                time.sleep(0.2)
                return original_is_handled(*args, **kwargs)

            harness.dedupe.is_handled = slow_is_handled  # type: ignore[method-assign]

            task = asyncio.create_task(
                harness.router._handle_notify_route_control(
                    {
                        "command": "notify_route",
                        "notification_id": "n1",
                        "payload": {"message": "hi"},
                        "idempotency_key": "k1",
                    }
                )
            )
            await asyncio.sleep(0.05)
            # The reload changes the notification definition mid-request. The
            # admitted request must still run the definition it was admitted
            # under, not the replacement (and must not fail as
            # unknown_notification).
            routes_path.write_text(
                """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active

notifications:
  - id: n1
    route: r1
    prompt: changed prompt
""",
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")

            response = await asyncio.wait_for(task, timeout=5)
            self.assertEqual(response["status"], "delivered")
            prompt_texts = [block["text"] for prompt in harness.profile.prompts for block in prompt]
            self.assertIn("original prompt", prompt_texts)
            self.assertNotIn("changed prompt", prompt_texts)

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

    async def test_control_socket_refuses_non_socket_path_and_removes_stale_socket(self) -> None:
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
            non_socket = Path(tmp) / "work" / "control" / "router.sock"
            non_socket.parent.mkdir(parents=True)
            non_socket.write_text("not a socket", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "not a socket"):
                router._prepare_control_socket(non_socket)

            non_socket.unlink()
            stale = non_socket
            import socket

            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(stale))
            finally:
                sock.close()

            router._prepare_control_socket(stale)
            self.assertFalse(stale.exists())

            with self.assertRaisesRegex(RuntimeError, "must include a private parent"):
                router._prepare_control_socket(Path("control.sock"))
            with self.assertRaisesRegex(RuntimeError, "under router.work_root"):
                router._prepare_control_socket(Path(tmp) / "outside" / "router.sock")

    async def test_control_socket_refuses_live_socket(self) -> None:
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
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            socket_path.parent.mkdir(parents=True)
            import socket

            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(socket_path))
                sock.listen(1)
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    router._prepare_control_socket(socket_path)
            finally:
                sock.close()

    async def test_control_socket_serves_trigger_job_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            route = Route(
                platform="signal",
                name="agenda-route",
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
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            server_task = asyncio.create_task(router._run_control_server())
            try:
                for _ in range(50):
                    if socket_path.exists():
                        break
                    await asyncio.sleep(0.01)
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                writer.write(
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000}\n'
                )
                await writer.drain()
                response = json.loads((await reader.readline()).decode("utf-8"))
                writer.close()
                await writer.wait_closed()
            finally:
                server_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await server_task

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertFalse(socket_path.exists())

    async def test_control_socket_serves_notification_and_reports_payload_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            route = Route(
                platform="signal",
                name="agenda-route",
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
                    control=RouterControlConfig(
                        enabled=True,
                        socket_path=socket_path,
                        max_notification_payload_bytes=64,
                    ),
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
            )

            async def control_request(request: dict) -> dict:
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                writer.write(encode_control_message(request))
                await writer.drain()
                response = json.loads((await reader.readline()).decode("utf-8"))
                writer.close()
                await writer.wait_closed()
                return response

            server_task = asyncio.create_task(router._run_control_server())
            try:
                for _ in range(50):
                    if socket_path.exists():
                        break
                    await asyncio.sleep(0.01)
                accepted = await control_request(
                    {
                        "command": "notify_route",
                        "notification_id": "backup-report",
                        "payload": {"a": "x" * 56},
                        "idempotency_key": "valid",
                    }
                )
                rejected = await control_request(
                    {
                        "command": "notify_route",
                        "notification_id": "backup-report",
                        "payload": {"a": "x" * 57},
                        "idempotency_key": "oversized",
                    }
                )
            finally:
                server_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await server_task

            self.assertEqual(accepted["status"], "delivered")
            self.assertEqual(rejected, {"status": "error", "error": "payload_too_large"})
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertFalse(socket_path.exists())

    async def test_control_client_disconnect_before_response_is_swallowed(self) -> None:
        class DisconnectedWriter:
            """Simulates a peer that vanished before the response drain.

            write() stays inert, matching a real transport buffering into a
            dead socket; the failure surfaces at drain(), the site observed
            in production.
            """

            def __init__(self, exc: BaseException) -> None:
                self.exc = exc
                self.closed = False
                self.drain_attempts = 0

            def write(self, data: bytes) -> None:
                pass

            async def drain(self) -> None:
                self.drain_attempts += 1
                raise self.exc

            def close(self) -> None:
                self.closed = True

            async def wait_closed(self) -> None:
                return None

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
            for exc_type in (ConnectionResetError, BrokenPipeError):
                with self.subTest(exc_type=exc_type.__name__):
                    reader = asyncio.StreamReader()
                    reader.feed_data(b'{"command":"route_status"}\n{"command":"route_status"}\n')
                    reader.feed_eof()
                    writer = DisconnectedWriter(exc_type())
                    with self.assertLogs(
                        "signal_hermes_router.router", level="DEBUG"
                    ) as debug_logs:
                        await router._handle_control_client(
                            reader,
                            writer,  # type: ignore[arg-type]
                        )
                    self.assertIn(
                        "control client disconnected before reading response",
                        "\n".join(debug_logs.output),
                    )
                    # The session loop ends on the first failed response; the
                    # second queued line is never processed.
                    self.assertEqual(writer.drain_attempts, 1)
                    self.assertTrue(writer.closed)

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

    async def test_direct_uuid_mismatch_does_not_fall_back_to_matching_number(self) -> None:
        private_payload = base64.b64encode(b"x" * 1024).decode("ascii")
        route = make_direct_route()
        raw = make_direct_raw(
            source_uuid="other-sender-uuid",
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
            self.assertTrue(dedupe.claim(route.key, "other-sender-uuid", 1))
            self.assertEqual(list((Path(tmp) / "media").rglob("*")), [])

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

    async def test_direct_sync_message_is_not_routed(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1,
                        "message": "linked-device direct text",
                    }
                },
            },
            "account": "account",
        }
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(make_direct_route(),),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            result = await router.handle_raw_event(raw)

            self.assertIsNone(result)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.direct_sends, [])

    async def test_direct_shadow_disabled_and_maintenance_do_not_call_backend(self) -> None:
        for state in (RouteState.SHADOW, RouteState.DISABLED, RouteState.MAINTENANCE):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                signal = FakeSignal()
                profile = FakeProfile()
                router = SignalHermesRouter(
                    AppConfig(
                        router=RouterConfig(
                            state_db=Path(tmp) / "state.db",
                            media_root=Path(tmp) / "media",
                            signal_attachment_root=Path(tmp) / "signal-attachments",
                            work_root=Path(tmp) / "work",
                        ),
                        routes=(make_direct_route(state=state),),
                    ),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                    dedupe=DedupeStore(),
                )

                await router.handle_raw_event(make_direct_raw())

                self.assertEqual(profile.prompts, [])
                self.assertEqual(signal.sends, [])
                if state == RouteState.MAINTENANCE:
                    self.assertEqual(
                        signal.direct_sends,
                        [("sender-uuid", "This route is temporarily under maintenance.")],
                    )
                else:
                    self.assertEqual(signal.direct_sends, [])

    async def test_direct_turn_failure_sends_failure_reply_to_primary_sender(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(make_direct_route(),),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_raw_event(make_direct_raw())

            self.assertEqual(
                signal.direct_sends,
                [("sender-uuid", "I hit an internal router error handling that message.")],
            )
            self.assertEqual(signal.sends, [])

    async def test_direct_long_reply_is_chunked_to_primary_sender(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(make_direct_route(),),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_raw_event(make_direct_raw())

            self.assertEqual(signal.sends, [])
            self.assertEqual(len(signal.direct_sends), 3)
            for i, (recipient, body) in enumerate(signal.direct_sends, 1):
                self.assertEqual(recipient, "sender-uuid")
                self.assertTrue(body.startswith(f"[{i}/3] "))

    async def test_direct_long_running_notice_targets_primary_sender(self) -> None:
        notice_sent = asyncio.Event()

        class SyncSignal(FakeSignal):
            async def send_direct(
                self, recipient: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                result = await super().send_direct(recipient, message)
                if message == "still working":
                    notice_sent.set()
                return result

        class SyncProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                await asyncio.wait_for(notice_sent.wait(), timeout=5.0)
                return TurnResult(self.reply_text)

        with tempfile.TemporaryDirectory() as tmp:
            signal = SyncSignal()
            profile = SyncProfile()
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                        busy_notice_after_seconds=0,
                        busy_notice="still working",
                    ),
                    routes=(make_direct_route(),),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_raw_event(make_direct_raw())

            self.assertEqual(
                signal.direct_sends,
                [("sender-uuid", "still working"), ("sender-uuid", "reply")],
            )

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

    async def test_synthetic_blank_reply_releases_dedupe_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(name="agenda-route")
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = ""
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            profile.reply_text = "reply"
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.error, "acp_empty_response")
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(
                [body for _, body in signal.sends],
                [
                    "I hit an internal router error handling that message.",
                    "reply",
                ],
            )

    async def test_synthetic_blank_reply_send_failure_preserves_acp_failure(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(name="agenda-route")
            profile = FakeProfile()
            profile.reply_text = ""
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.error, "acp_empty_response")
            self.assertEqual(failed.failure.code, FailureCode.ACP_EMPTY_RESPONSE)
            self.assertEqual(
                router._route_status_response({})["routes"][0]["last_failure"]["code"],
                "acp_empty_response",
            )

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

    async def test_notification_attachment_with_sentinel_reply_suppresses_whole_send(
        self,
    ) -> None:
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
            profile.reply_text = NO_REPLY_SENTINEL
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

            # Deliberate silence wins over the attachment-only fallback:
            # neither text nor attachment is sent, and the frozen outbound
            # copy is still cleaned up.
            self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
            self.assertFalse(outcome.reply_sent)
            self.assertEqual(signal.sends, [])
            self.assertEqual(signal.send_attachments, [])
            self.assertFalse((app.router.media_root / ".outbound").exists())

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

    async def test_dedupe_is_scoped_by_route(self) -> None:
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
                    ),
                    Route(
                        platform="signal",
                        group_id="group-two",
                        profile="profile",
                        session_policy=SessionPolicy.PERSISTENT_ROUTE,
                        state=RouteState.ACTIVE,
                    ),
                ),
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            for group_id in ("group-one", "group-two"):
                await router.handle_event(
                    NormalizedEvent(
                        platform="signal",
                        group_id=group_id,
                        sender_id="sender",
                        source_uuid="sender",
                        timestamp=1,
                        text=f"hello {group_id}",
                    )
                )

            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(signal.sends, [("group-one", "reply"), ("group-two", "reply")])

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

    async def test_media_failure_releases_dedupe_claim_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            attachment_root = Path(tmp) / "signal-attachments"
            attachment_root.mkdir()
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

            with self.assertRaises(FileNotFoundError):
                await router.handle_event(event)

            (attachment_root / "attachment-id").write_bytes(b"from signal store")
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

    async def test_typing_is_optional_and_long_running_notice_skips_when_done(self) -> None:
        class SignalWithoutTyping:
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                return {"timestamp": 1}

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=SignalWithoutTyping(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            route = router.config.routes[0]
            await router._typing(route, True)
            turn_done = asyncio.Event()
            turn_done.set()
            await router._long_running_notice(route, turn_done)

    async def test_busy_notice_cooldown_suppresses_repeat_notices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            clock = {"now": 1_000_000_000}
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    busy_notice_after_seconds=0,
                    busy_notice_cooldown_seconds=300,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: clock["now"],
            )
            route = router.config.routes[0]
            turn_done = asyncio.Event()

            await router._long_running_notice(route, turn_done)
            self.assertEqual(signal.sends, [("group", "Still working on this.")])

            # A second slow turn within the cooldown window stays quiet.
            clock["now"] += 100_000
            await router._long_running_notice(route, turn_done)
            self.assertEqual(len(signal.sends), 1)

            # Once the cooldown elapses the notice fires again.
            clock["now"] += 300_000
            await router._long_running_notice(route, turn_done)
            self.assertEqual(len(signal.sends), 2)

    async def test_busy_notice_cooldown_is_tracked_per_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            clock = {"now": 1_000_000_000}
            route_a = make_route(group_id="group-one")
            route_b = make_route(group_id="group-two")
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    routes=(route_a, route_b),
                    busy_notice_after_seconds=0,
                    busy_notice_cooldown_seconds=300,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: clock["now"],
            )
            turn_done = asyncio.Event()

            await router._long_running_notice(route_a, turn_done)
            await router._long_running_notice(route_b, turn_done)

            self.assertEqual(
                signal.sends,
                [
                    ("group-one", "Still working on this."),
                    ("group-two", "Still working on this."),
                ],
            )

    async def test_busy_notice_failed_send_does_not_start_cooldown(self) -> None:
        class FlakySignal(FakeSignal):
            def __init__(self) -> None:
                super().__init__()
                self.fail = True

            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                if self.fail:
                    raise RuntimeError("send failed")
                return await super().send_group(group_id, message)

        with tempfile.TemporaryDirectory() as tmp:
            signal = FlakySignal()
            clock = {"now": 1_000_000_000}
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    busy_notice_after_seconds=0,
                    busy_notice_cooldown_seconds=300,
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: clock["now"],
            )
            route = router.config.routes[0]
            turn_done = asyncio.Event()

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router._long_running_notice(route, turn_done)
            self.assertEqual(signal.sends, [])

            # The failed attempt did not start a cooldown window, so the next
            # slow turn retries the notice immediately.
            signal.fail = False
            await router._long_running_notice(route, turn_done)
            self.assertEqual(signal.sends, [("group", "Still working on this.")])

    async def test_busy_notice_default_has_no_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, busy_notice_after_seconds=0),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            route = router.config.routes[0]
            turn_done = asyncio.Event()

            await router._long_running_notice(route, turn_done)
            await router._long_running_notice(route, turn_done)

            self.assertEqual(len(signal.sends), 2)

    async def test_busy_notice_cooldown_applies_across_full_turns(self) -> None:
        # End-to-end: with a cooldown configured, the second consecutive slow
        # turn does not repeat the busy notice. The second turn's prompt waits
        # until the notice task has observably run its suppression check.
        suppressed = asyncio.Event()

        class WaitForSuppressionHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if "suppressing busy notice" in record.getMessage():
                    suppressed.set()

        notice_sent = asyncio.Event()

        class SyncSignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                result = await super().send_group(group_id, message)
                if message == "still working":
                    notice_sent.set()
                return result

        class SyncProfile(FakeProfile):
            def __init__(self) -> None:
                super().__init__()
                self.turn = 0

            async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                self.turn += 1
                gate = notice_sent if self.turn == 1 else suppressed
                await asyncio.wait_for(gate.wait(), timeout=5.0)
                return TurnResult(self.reply_text)

        with tempfile.TemporaryDirectory() as tmp:
            signal = SyncSignal()
            profile = SyncProfile()
            router = SignalHermesRouter(
                make_app(
                    tmp,
                    RouteState.ACTIVE,
                    busy_notice_after_seconds=0,
                    busy_notice_cooldown_seconds=300,
                    busy_notice="still working",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            handler = WaitForSuppressionHandler(level=logging.DEBUG)
            logger = logging.getLogger("signal_hermes_router.router")
            previous_level = logger.level
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            try:
                await router.handle_event(make_event(timestamp=1))
                await router.handle_event(make_event(timestamp=2))
            finally:
                logger.removeHandler(handler)
                logger.setLevel(previous_level)

            bodies = [body for _, body in signal.sends]
            self.assertEqual(bodies.count("still working"), 1)
            self.assertEqual(bodies.count("reply"), 2)

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

    async def test_route_status_rejects_invalid_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = router._route_status_response({"route_indexes": [True]})

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"], "invalid_route_status_scope")

    async def test_route_status_filters_by_route_index_and_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            alpha = Route(
                platform="signal",
                name="alpha",
                group_id="EXAMPLE_ALPHA_GROUP",
                profile="profile-a",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            beta = Route(
                platform="signal",
                name="beta",
                group_id="EXAMPLE_BETA_GROUP",
                profile="profile-b",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(alpha, beta)),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            by_name = router._route_status_response({"route": "alpha"})
            by_index = router._route_status_response({"route_index": 1})
            by_index_list = router._route_status_response({"route_indexes": [0, 1, 1]})
            by_profile = router._route_status_response({"profiles": ["profile-b"]})
            invalid_string_list = router._route_status_response({"routes": [""]})
            invalid_bool = router._route_status_response({"route_index": True})

        self.assertEqual([route["route_ref"] for route in by_name["routes"]], ["route:alpha"])
        self.assertEqual([route["route_ref"] for route in by_index["routes"]], ["route:beta"])
        self.assertEqual(
            [route["route_ref"] for route in by_index_list["routes"]],
            ["route:alpha", "route:beta"],
        )
        self.assertEqual([route["route_ref"] for route in by_profile["routes"]], ["route:beta"])
        self.assertEqual(invalid_string_list["error"], "invalid_route_status_scope")
        self.assertEqual(invalid_bool["error"], "invalid_route_status_scope")

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

    async def test_busy_notice_does_not_fire_after_failure_reply_during_slow_recovery(
        self,
    ) -> None:
        # Regression for bead c6u: with a tight busy_notice_after_seconds
        # and slow recovery (restart_profile awaiting), the busy notice
        # must NOT fire after the failure reply has already been sent.
        class SlowRestartSupervisor(FakeSupervisor):
            async def restart_profile(self, profile_name: str) -> None:
                self.restarts += 1
                # Sleep longer than busy_notice_after_seconds so the notice
                # task would fire mid-recovery if not stopped first.
                await asyncio.sleep(0.05)

        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            base = make_app(tmp, RouteState.ACTIVE, failures=2)
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                    busy_notice_after_seconds=0.01,
                ),
                routes=base.routes,
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=SlowRestartSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_event(make_event())
            bodies = [body for _, body in signal.sends]
            self.assertEqual(bodies, ["I hit an internal router error handling that message."])
            self.assertNotIn("Still working on this.", bodies)

    async def test_busy_notice_does_not_fire_during_long_chunked_success_reply(
        self,
    ) -> None:
        # Same race on the success path: a long chunked reply that takes
        # longer than busy_notice_after_seconds must NOT trigger the notice
        # mid-reply. Use a slow FakeSignal to simulate a chunk send taking
        # measurable time.
        class SlowSignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                await asyncio.sleep(0.02)
                return await super().send_group(group_id, message)

        with tempfile.TemporaryDirectory() as tmp:
            signal = SlowSignal()
            profile = FakeProfile()
            # Long reply → 3 chunks at default max_signal_message_bytes=1900.
            profile.reply_text = "x" * 4000
            base = make_app(tmp, RouteState.ACTIVE)
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                    busy_notice_after_seconds=0.01,
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
            bodies = [body for _, body in signal.sends]
            # 3 chunk sends; none of them should be the busy notice.
            self.assertEqual(len(bodies), 3)
            self.assertNotIn("Still working on this.", bodies)

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

    async def test_long_running_notice_is_sent_before_slow_reply(self) -> None:
        # The busy notice must reach the user BEFORE the eventual reply
        # when the prompt outlives busy_notice_after_seconds. Synchronise
        # on the notice send so the test does not depend on a scheduler
        # race between prompt-return + _stop_busy_notice and the notice
        # task's send_group under suite load.
        notice_sent = asyncio.Event()

        class SyncSignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict:
                result = await super().send_group(group_id, message)
                if message == "still working":
                    notice_sent.set()
                return result

        class SyncProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                await asyncio.wait_for(notice_sent.wait(), timeout=5.0)
                return TurnResult(self.reply_text)

        with tempfile.TemporaryDirectory() as tmp:
            signal = SyncSignal()
            profile = SyncProfile()
            app = AppConfig(
                router=RouterConfig(
                    state_db=Path(tmp) / "state.db",
                    media_root=Path(tmp) / "media",
                    signal_attachment_root=Path(tmp) / "signal-attachments",
                    work_root=Path(tmp) / "work",
                    busy_notice_after_seconds=0,
                    busy_notice="still working",
                ),
                routes=(make_app(tmp, RouteState.ACTIVE).routes[0],),
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event())

            self.assertEqual(signal.sends, [("group", "still working"), ("group", "reply")])

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


class ClosedAwareSignal(FakeSignal):
    """Fake Signal client whose send path fails once the client is closed,
    mirroring the real transport during shutdown."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        if self.closed:
            raise RuntimeError("signal client closed")
        return await super().send_group(group_id, message, attachments=attachments)


class GatedProfile(FakeProfile):
    def __init__(self, started: asyncio.Event, gate: asyncio.Event) -> None:
        super().__init__()
        self._started = started
        self._gate = gate

    async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
        self._started.set()
        await self._gate.wait()
        return await super().prompt(session_id, blocks)


class FirstGatedProfile(FakeProfile):
    def __init__(self, started: asyncio.Event, gate: asyncio.Event) -> None:
        super().__init__()
        self._started = started
        self._gate = gate
        self._first = True

    async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
        if self._first:
            self._first = False
            self._started.set()
            await self._gate.wait()
        return await super().prompt(session_id, blocks)


def _shutdown_route() -> Route:
    return Route(
        platform="signal",
        name="agenda-route",
        group_id="group",
        profile="profile",
        session_policy=SessionPolicy.PERSISTENT_ROUTE,
        state=RouteState.ACTIVE,
        route_context={"purpose": "synthetic", "route_alias": "agenda-route"},
    )


def _notification_app(tmp: str, **kwargs: Any) -> AppConfig:
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


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
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
                supervisor=FakeSupervisor(GatedProfile(started, gate)),  # type: ignore[arg-type]
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
                supervisor=FakeSupervisor(GatedProfile(started, gate)),  # type: ignore[arg-type]
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
                supervisor=FakeSupervisor(GatedProfile(started, gate)),  # type: ignore[arg-type]
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
                supervisor=FakeSupervisor(FirstGatedProfile(started, gate)),  # type: ignore[arg-type]
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
                supervisor=FakeSupervisor(GatedProfile(started, gate)),  # type: ignore[arg-type]
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
                supervisor=FakeSupervisor(GatedProfile(started, gate)),  # type: ignore[arg-type]
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


class UnexpectedChildExitRecoveryTests(unittest.IsolatedAsyncioTestCase):
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


class RetentionSweepRouterTests(unittest.IsolatedAsyncioTestCase):
    DAY_SECONDS = 86400.0

    def _retention_config(self, **overrides: Any) -> RetentionConfig:
        values: dict[str, Any] = {
            "sweep_interval_seconds": 3600.0,
            "dedupe_handled_seconds": 30 * self.DAY_SECONDS,
            "media_max_age_seconds": 30 * self.DAY_SECONDS,
        }
        values.update(overrides)
        return RetentionConfig(**values)

    def _write_archive_file(self, tmp: str, relative: str, age_seconds: float) -> Path:
        path = Path(tmp) / "media" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic media body")
        moment = time.time() - age_seconds
        os.utime(path, (moment, moment))
        return path

    async def test_startup_sweep_prunes_dedupe_and_media_with_count_only_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_clock = {"now_ms": int((time.time() - 40 * self.DAY_SECONDS) * 1000)}
            dedupe = DedupeStore(clock_ms=lambda: store_clock["now_ms"])
            dedupe.mark_handled("signal:group", "old-uuid", 1)
            store_clock["now_ms"] = int(time.time() * 1000)
            dedupe.mark_handled("signal:group", "fresh-uuid", 2)
            old_media = self._write_archive_file(
                tmp, "signal/2024/01/abc123/old.pdf", 40 * self.DAY_SECONDS
            )
            fresh_media = self._write_archive_file(
                tmp, "signal/2026/07/def456/fresh.pdf", 1 * self.DAY_SECONDS
            )
            harness = make_router_harness(
                tmp,
                dedupe=dedupe,
                retention=self._retention_config(),
            )

            with self.assertLogs("signal_hermes_router.router", level="INFO") as logs:
                await harness.router._run_retention_sweep_once()

            self.assertFalse(harness.dedupe.is_handled("signal:group", "old-uuid", 1))
            self.assertTrue(harness.dedupe.is_handled("signal:group", "fresh-uuid", 2))
            self.assertFalse(old_media.exists())
            self.assertTrue(fresh_media.exists())
            retention_lines = [line for line in logs.output if "retention sweep" in line]
            self.assertTrue(retention_lines)
            for line in retention_lines:
                self.assertNotIn(tmp, line)
                self.assertNotIn("old.pdf", line)
                self.assertNotIn("uuid", line)
            self.assertTrue(any("pruned 1 handled dedupe rows" in line for line in retention_lines))
            self.assertTrue(any("removed 1 media files" in line for line in retention_lines))
            await harness.router.close(drain_timeout=0.0)

    async def test_retention_loop_reschedules_periodic_sweeps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_clock = {"now_ms": int((time.time() - 40 * self.DAY_SECONDS) * 1000)}
            dedupe = DedupeStore(clock_ms=lambda: store_clock["now_ms"])
            dedupe.mark_handled("signal:group", "startup-uuid", 1)
            harness = make_router_harness(
                tmp,
                dedupe=dedupe,
                retention=self._retention_config(sweep_interval_seconds=0.05),
            )
            router = harness.router
            task = asyncio.create_task(router._run_retention_sweeps())
            router._retention_task = task
            try:
                async with asyncio.timeout(5):
                    while dedupe.is_handled("signal:group", "startup-uuid", 1):
                        await asyncio.sleep(0.01)
                # The startup sweep ran; a row backdated afterwards must be
                # pruned by a later interval sweep, proving rescheduling.
                dedupe.mark_handled("signal:group", "periodic-uuid", 2)
                async with asyncio.timeout(5):
                    while dedupe.is_handled("signal:group", "periodic-uuid", 2):
                        await asyncio.sleep(0.01)
            finally:
                router.begin_shutdown()
                with suppress(asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await task
                await router.close(drain_timeout=0.0)

    async def test_run_forever_spawns_no_retention_task_when_disabled(self) -> None:
        class ParkedSignal(FakeSignal):
            async def events(self):
                await asyncio.Event().wait()
                if False:
                    yield {}

        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(
                tmp,
                signal=ParkedSignal(),
                retention=RetentionConfig(dedupe_handled_seconds=None),
            )
            router = harness.router
            run_task = asyncio.create_task(router.run_forever())
            try:
                await asyncio.sleep(0.05)
                self.assertIsNone(router._retention_task)
            finally:
                router.begin_shutdown()
                with suppress(asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await run_task
                await router.close(drain_timeout=0.0)

    async def test_close_reports_blocked_sweep_worker_without_wedging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            import threading

            worker_started = threading.Event()
            worker_release = threading.Event()

            def blocked_plan(**_kwargs: Any) -> Any:
                worker_started.set()
                worker_release.wait(timeout=30)
                from signal_hermes_router.media import MediaSweepPlan

                return MediaSweepPlan(groups=(), candidate_dirs=())

            harness = make_router_harness(
                tmp,
                retention=self._retention_config(dedupe_handled_seconds=None),
            )
            router = harness.router
            with (
                patch("signal_hermes_router.router.plan_media_sweep", blocked_plan),
                patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.2),
            ):
                task = asyncio.create_task(router._run_retention_sweeps())
                router._retention_task = task
                try:
                    async with asyncio.timeout(5):
                        while not worker_started.is_set():
                            await asyncio.sleep(0.01)
                    with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                        started = time.monotonic()
                        await router.close(drain_timeout=0.0)
                        elapsed = time.monotonic() - started
                    # Bounded: the blocked worker is reported, not awaited to
                    # completion, and the dedupe store still closed cleanly.
                    self.assertLess(elapsed, 5.0)
                    self.assertTrue(any("retention sweep worker" in line for line in logs.output))
                    self.assertTrue(router.dedupe.close())
                finally:
                    worker_release.set()
                    task.cancel()
                    with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                        async with asyncio.timeout(5):
                            await task

    async def test_inbound_manifest_paths_live_during_prompt_and_released_after(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            live_seen: list[Path] = []

            class SnapshotProfile(FakeProfile):
                def __init__(self, router_ref: dict[str, Any]) -> None:
                    super().__init__()
                    self._router_ref = router_ref

                async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                    live_seen.extend(self._router_ref["router"]._live_media.keys())
                    return await super().prompt(session_id, blocks)

            router_ref: dict[str, Any] = {}
            profile = SnapshotProfile(router_ref)
            harness = make_router_harness(
                tmp,
                profile=profile,
                supervisor=FakeSupervisor(profile),
                retention=self._retention_config(),
            )
            router_ref["router"] = harness.router
            event = make_event(
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

            result = await harness.router.handle_event(event)

            self.assertIsNotNone(result)
            self.assertEqual(len(live_seen), 1)
            self.assertTrue(str(live_seen[0]).endswith("report.pdf"))
            self.assertEqual(len(harness.router._live_media), 0)
            await harness.router.close(drain_timeout=0.0)

    async def test_outbound_freeze_counter_balances_across_nested_scopes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp, retention=self._retention_config())
            router = harness.router
            media_root = Path(tmp) / "media"
            staged = media_root / "camera" / "person.png"
            ensure_private_dir_tree(media_root, staged.parent)
            write_private_bytes(staged, b"\x89PNG synthetic")

            outer = await router._freeze_outbound_attachments([str(staged)])
            self.assertEqual(len(outer), 1)
            frozen_path = outer[0].path
            self.assertEqual(router._live_media[frozen_path], 1)

            inner = await router._freeze_outbound_attachments(outer)
            self.assertEqual(router._live_media[frozen_path], 2)

            router._cleanup_owned_outbound_attachments(inner)
            self.assertEqual(router._live_media[frozen_path], 1)
            # Still live after the inner scope: the sweep must not delete it.
            self.assertTrue(router._is_live_media(frozen_path))

            router._cleanup_owned_outbound_attachments(outer)
            self.assertEqual(len(router._live_media), 0)
            await router.close(drain_timeout=0.0)

    async def test_media_sweep_defers_while_media_write_worker_in_flight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old_media = self._write_archive_file(
                tmp, "signal/2024/01/abc123/old.pdf", 40 * self.DAY_SECONDS
            )
            harness = make_router_harness(
                tmp,
                retention=self._retention_config(dedupe_handled_seconds=None),
            )
            router = harness.router
            started = threading.Event()
            release = threading.Event()
            from signal_hermes_router.media import write_attachment as original_write

            def gated_write(**kwargs: Any) -> Any:
                started.set()
                release.wait(timeout=30)
                return original_write(**kwargs)

            with patch("signal_hermes_router.media.write_attachment", gated_write):
                turn = asyncio.create_task(
                    router.handle_event(
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
                )
                try:
                    async with asyncio.timeout(5):
                        while not started.is_set():
                            await asyncio.sleep(0.01)
                    # Cancel the awaiting turn: the sweep guard must stay
                    # held by the worker, not the coroutine.
                    turn.cancel()
                    with suppress(asyncio.CancelledError):
                        await turn
                    self.assertEqual(router._media_io_inflight, 1)
                    await router._run_retention_sweep_once()
                    # The deletion batch was deferred, not executed.
                    self.assertTrue(old_media.exists())
                finally:
                    release.set()
                async with asyncio.timeout(5):
                    while router._media_io_inflight:
                        await asyncio.sleep(0.01)
            await router._run_retention_sweep_once()
            self.assertFalse(old_media.exists())
            await router.close(drain_timeout=0.0)

    async def test_cancelled_freeze_cleans_completed_and_pending_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            harness = make_router_harness(tmp, retention=self._retention_config())
            router = harness.router
            media_root = Path(tmp) / "media"
            first = media_root / "camera" / "one.png"
            second = media_root / "camera" / "two.png"
            ensure_private_dir_tree(media_root, first.parent)
            write_private_bytes(first, b"\x89PNG one")
            write_private_bytes(second, b"\x89PNG two")
            attachments = [
                OutboundAttachment(
                    path=path,
                    content_type="image/png",
                    size=path.stat().st_size,
                )
                for path in (first, second)
            ]
            started = threading.Event()
            release = threading.Event()
            copies = {"count": 0}
            original_copy = router_module._copy_outbound_attachment

            def gated_second_copy(source: Path, destination: Path, max_bytes: int) -> int:
                copies["count"] += 1
                if copies["count"] >= 2:
                    started.set()
                    release.wait(timeout=30)
                return original_copy(source, destination, max_bytes)

            with patch("signal_hermes_router.router._copy_outbound_attachment", gated_second_copy):
                freeze = asyncio.create_task(router._freeze_outbound_attachments(attachments))
                try:
                    async with asyncio.timeout(5):
                        while not started.is_set():
                            await asyncio.sleep(0.01)
                    freeze.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await freeze
                finally:
                    release.set()
                async with asyncio.timeout(5):
                    while router._media_io_inflight:
                        await asyncio.sleep(0.01)
                # One extra tick for the abandoned-artifact done callback.
                await asyncio.sleep(0)
            # The completed first copy and the abandoned second copy are both
            # cleaned; no live-media references leak.
            self.assertEqual(len(router._live_media), 0)
            self.assertFalse((media_root / ".outbound").exists())
            self.assertTrue(first.exists())
            self.assertTrue(second.exists())
            await router.close(drain_timeout=0.0)

    async def test_close_reports_blocked_turn_dedupe_worker_and_defers_store_close(
        self,
    ) -> None:
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
            with patch("signal_hermes_router.router.SHUTDOWN_SETTLE_TIMEOUT_SECONDS", 0.2):
                worker = asyncio.create_task(
                    router._run_io_worker(
                        lambda: router.dedupe.mark_handled("signal:group", "uuid", 1)
                    )
                )
                try:
                    async with asyncio.timeout(5):
                        while not started.is_set():
                            await asyncio.sleep(0.01)
                    # Abandon the awaiting task; the worker thread keeps the
                    # store operation in flight.
                    worker.cancel()
                    with suppress(asyncio.CancelledError):
                        await worker
                    with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                        begun = time.monotonic()
                        incomplete = await router.close(drain_timeout=0.0)
                        elapsed = time.monotonic() - begun
                    # Bounded: the blocked worker is reported and the store
                    # close is deferred to it, never awaited unboundedly.
                    self.assertLess(elapsed, 5.0)
                    self.assertTrue(incomplete)
                    self.assertTrue(any("turn I/O workers" in line for line in logs.output))
                    self.assertTrue(
                        any("dedupe store close deferred" in line for line in logs.output)
                    )
                finally:
                    release.set()
            # The released worker finishes its write and runs the deferred
            # finalizer; the store ends closed.
            async with asyncio.timeout(5):
                while not router.dedupe.close():
                    await asyncio.sleep(0.01)


if __name__ == "__main__":
    unittest.main()
