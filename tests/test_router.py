from __future__ import annotations

import asyncio
import base64
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.config import AppConfig, Route, RouterConfig
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    NormalizedEvent,
    RouteState,
    SessionPolicy,
    SignalAttachment,
    TurnResult,
)
from signal_hermes_router.router import SignalHermesRouter
from tests.support import FakeProfile, FakeSignal, FakeSupervisor, make_app, make_event


class RouterTests(unittest.IsolatedAsyncioTestCase):
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
                await router._send_once(route, "reply")
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
