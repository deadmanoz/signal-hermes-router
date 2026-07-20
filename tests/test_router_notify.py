from __future__ import annotations

import asyncio
import tempfile
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.config import (
    Route,
    SyntheticRouteNotification,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    RouteState,
    SessionPolicy,
    TurnResult,
    TurnOutcomeStatus,
)
from signal_hermes_router.outbound import NO_REPLY_SENTINEL
from signal_hermes_router.payloads import canonicalize_notification_payload, encode_control_message
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.outbound_media import (
    validate_outbound_attachments,
)
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_route,
    make_router_harness,
    make_synthetic_app,
    record_dedupe_call_threads,
    write_test_file,
    wait_until,
    RouterTestCase,
)


class RouterNotifyTests(RouterTestCase):
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
                await wait_until(lambda: entered.is_set(), timeout=5.0, interval=0.01)
                parked = [t for t in harness.router._control_request_tasks if not t.done()]
                self.assertEqual(len(parked), 1)
                self.assertEqual(harness.router._turn_task_route_key(parked[0]), route.key)
            finally:
                block.set()
            response = await asyncio.wait_for(request, timeout=5)
            self.assertEqual(response["status"], "delivered")

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
