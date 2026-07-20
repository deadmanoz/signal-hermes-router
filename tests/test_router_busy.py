from __future__ import annotations

import asyncio
import logging
import tempfile
from collections.abc import Sequence
from pathlib import Path

from signal_hermes_router.config import (
    AppConfig,
    RouterConfig,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    RouteState,
    TurnResult,
)
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_event,
    make_route,
    RouterTestCase,
)


class RouterBusyTests(RouterTestCase):
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

