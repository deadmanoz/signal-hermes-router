from __future__ import annotations

import tempfile
from pathlib import Path

from signal_hermes_router.config import (
    AppConfig,
    Route,
    RouterConfig,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    NormalizedEvent,
    RouteState,
    SessionPolicy,
)
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_group_raw,
    make_router_harness,
    RouterTestCase,
)


class RouterDedupeTests(RouterTestCase):
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

