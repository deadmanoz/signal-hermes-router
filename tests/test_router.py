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
import time
import unittest
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from signal_hermes_router.acp import JsonRpcError
from signal_hermes_router.config import (
    AppConfig,
    CircuitBreakerConfig,
    InboundRateLimitConfig,
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
    make_route,
)


def make_direct_route(
    *,
    sender_id: str = "sender-uuid",
    sender_number: str | None = "+00000000000",
    state: RouteState = RouteState.ACTIVE,
    session_policy: SessionPolicy = SessionPolicy.PERSISTENT_SENDER,
) -> Route:
    return Route(
        platform="signal",
        chat_type=ChatType.DIRECT,
        sender_id=sender_id,
        sender_number=sender_number,
        profile="profile",
        session_policy=session_policy,
        state=state,
        route_context={"purpose": "synthetic", "route_alias": "direct-test"},
    )


def make_direct_raw(
    *,
    source_uuid: str | None = "sender-uuid",
    source_number: str | None = "+00000000000",
    timestamp: int = 1,
    text: str = "hello direct",
    attachments: list[dict] | None = None,
) -> dict:
    envelope: dict = {
        "timestamp": timestamp,
        "dataMessage": {
            "timestamp": timestamp,
            "message": text,
            "attachments": attachments or [],
        },
    }
    if source_uuid is not None:
        envelope["sourceUuid"] = source_uuid
    if source_number is not None:
        envelope["sourceNumber"] = source_number
        envelope["source"] = source_number
    return {"envelope": envelope, "account": "synthetic-account-number"}


def make_group_raw(
    *,
    group_id: str = "group",
    source_uuid: str = "sender-uuid",
    timestamp: int = 1,
    text: str | None = "hello",
    attachments: list[dict] | None = None,
) -> dict:
    data_message: dict = {
        "timestamp": timestamp,
        "groupInfo": {"groupId": group_id},
        "attachments": attachments or [],
    }
    if text is not None:
        data_message["message"] = text
    return {
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "envelope": {
                "sourceUuid": source_uuid,
                "timestamp": timestamp,
                "dataMessage": data_message,
            }
        },
    }


def make_synthetic_app(
    tmp: str | Path,
    route: Route,
    job: SyntheticRouteJob | None = None,
    *,
    control: RouterControlConfig | None = None,
    notifications: tuple[SyntheticRouteNotification, ...] = (),
    **router_overrides,
) -> AppConfig:
    return AppConfig(
        router=RouterConfig(
            state_db=Path(tmp) / "state.db",
            media_root=Path(tmp) / "media",
            signal_attachment_root=Path(tmp) / "signal-attachments",
            work_root=Path(tmp) / "work",
            control=control or RouterControlConfig(),
            **router_overrides,
        ),
        routes=(route,),
        scheduled_jobs=(
            job
            or SyntheticRouteJob(
                id="daily-agenda",
                route_name=route.name or "agenda-route",
                prompt="Prepare the synthetic daily agenda.",
            ),
        ),
        notifications=notifications,
    )


def write_png(path: Path, body: bytes = b"png") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = path.parts
    if "media" in parts:
        index = len(parts) - 1 - list(reversed(parts)).index("media")
        root = Path(*parts[: index + 1])
    else:
        root = path.parent
    ensure_private_dir_tree(root, path.parent)
    write_private_bytes(path, body)
    return path


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

            async def handle(raw: dict) -> None:
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )

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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )

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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png", b"old")

            class MutatingProfile(FakeProfile):
                async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                    write_png(image, b"new")
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
            source = write_png(Path(tmp) / "media" / "camera" / "source.png")
            frozen_path = write_png(
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

            frozen = router._freeze_outbound_attachments((router_owned_attachment,))
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
            first = write_png(Path(tmp) / "media" / "camera" / "first.png", b"ok")
            second = write_png(Path(tmp) / "media" / "camera" / "second.png", b"abcd")
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
                router._freeze_outbound_attachments((first_attachment, second_attachment))

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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png.gz")
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
                router._freeze_outbound_attachments((attachment,))

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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
                router._freeze_outbound_attachments((attachment,))

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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
                router._freeze_outbound_attachments((attachment,))

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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png", b"first")
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
                write_png(image, b"second")
            finally:
                lock.release()
            response = await task

        self.assertEqual(response["status"], "delivered")
        self.assertEqual(signal.attachment_bodies, [b"first"])
        self.assertFalse((app.router.media_root / ".outbound").exists())

    async def test_notify_route_rejects_falsey_malformed_attachments(self) -> None:
        for raw_attachments in (None, "", {}):
            with self.subTest(raw_attachments=raw_attachments):
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            image = write_png(Path(tmp) / "media" / "camera" / "person.png")
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
            async def send_group(self, group_id: str, message: str) -> dict[str, int]:
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )

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

            async def send_group(self, group_id: str, message: str) -> dict[str, int]:
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
            async def send_group(self, group_id: str, message: str) -> dict[str, int]:
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
                return ToolSurface.from_names(self.profile, ["read_file"])

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
            async def send_direct(self, recipient: str, message: str) -> dict[str, int]:
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

    async def test_active_route_with_empty_reply_marks_handled_without_send(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = ""
            dedupe = DedupeStore()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )

            await router.handle_event(make_event())

            self.assertEqual(signal.sends, [])
            self.assertFalse(dedupe.claim("signal:group", "sender", 1))

    async def test_unrouted_and_duplicate_events_do_not_call_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

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

    async def test_canary_reply_prefix_is_idempotent(self) -> None:
        route_context = {"label": "synthetic", "canary_reply_prefix": "[router-canary]"}
        for reply_text, expected in (
            ("reply", "[router-canary] reply"),
            ("   reply", "[router-canary] reply"),
            ("[router-canary] reply", "[router-canary] reply"),
            ("   [router-canary] reply", "[router-canary] reply"),
        ):
            with self.subTest(reply_text=reply_text), tempfile.TemporaryDirectory() as tmp:
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

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
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
            )
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
            async def send_group(self, group_id: str, message: str) -> dict:
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
            async def send_group(self, group_id: str, message: str) -> dict:
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

            async def send_group(self, group_id: str, message: str) -> dict:
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
            async def send_group(self, group_id: str, message: str) -> dict:
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
            async def send_group(self, group_id: str, message: str) -> dict:
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
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
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
            async def send_group(self, group_id: str, message: str) -> dict:
                self.sends.append((group_id, message))
                if len(self.sends) == 2:
                    raise RuntimeError("synthetic send failure")
                return {"timestamp": 1}

        with tempfile.TemporaryDirectory() as tmp:
            signal = FailingOnSecondSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            with self.assertLogs("signal_hermes_router.router", level="ERROR") as logs:
                await router.handle_event(make_event())
            self.assertEqual(len(signal.sends), 2)
            self.assertIn("chunk 2/3", "\n".join(logs.output))

    async def test_chunked_dispatch_logs_use_redacted_route_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = "x" * 4000
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
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
            async def send_group(self, group_id: str, message: str) -> dict:
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


if __name__ == "__main__":
    unittest.main()
