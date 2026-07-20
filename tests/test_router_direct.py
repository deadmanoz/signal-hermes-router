from __future__ import annotations

import asyncio
import base64
import tempfile
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterConfig,
    SyntheticRouteNotification,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    ChatType,
    RouteState,
    SessionPolicy,
    TurnResult,
    TurnOutcomeStatus,
)
from signal_hermes_router.payloads import canonicalize_notification_payload
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.outbound_media import (
    validate_outbound_attachments,
)
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_direct_route,
    make_direct_raw,
    make_synthetic_app,
    write_test_file,
    RouterTestCase,
)


class RouterDirectTests(RouterTestCase):
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

