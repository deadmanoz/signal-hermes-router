from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterConfig,
    RouterControlConfig,
    SyntheticRouteNotification,
    SyntheticRouteJob,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    ChatType,
    NormalizedEvent,
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
from signal_hermes_router.outbound_media import OutboundAttachmentError, validate_outbound_attachments
from tests.support import FakeProfile, FakeSignal, FakeSupervisor, make_app, make_event


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
            router = SignalHermesRouter(make_app(tmp, RouteState.ACTIVE))
            try:
                self.assertIsNotNone(router.signal)
                self.assertIsNotNone(router.supervisor)
                self.assertIsNotNone(router.dedupe)
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
            {"status": "error", "error": "attachment_path_not_absolute"},
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

    async def test_notify_route_rechecks_idempotency_after_attachment_validation_race(
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

            def fail_after_original_completes(*_args, **_kwargs):
                router.dedupe.mark_handled(route.key, dedupe_sender_id, dedupe_timestamp)
                raise OutboundAttachmentError("attachment_not_found", "rotated away")

            with patch(
                "signal_hermes_router.router.validate_outbound_attachments",
                side_effect=fail_after_original_completes,
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
                if state == RouteState.MAINTENANCE:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
                    self.assertEqual(
                        signal.sends,
                        [("group", "This route is temporarily under maintenance.")],
                    )
                else:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.SKIPPED)
                    self.assertEqual(signal.sends, [])

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
            self.assertFalse(failed.to_control_response()["reply_sent"])
            self.assertEqual(retry.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(signal.sends, [])

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
        self.assertNotIn("private-startup-token", json.dumps(response, sort_keys=True))

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
            self.assertEqual(response["error"], "RuntimeError")
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


if __name__ == "__main__":
    unittest.main()
