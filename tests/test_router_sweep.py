from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import patch

from signal_hermes_router import router as router_module
from signal_hermes_router.config import (
    RetentionConfig,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    OutboundAttachment,
    SignalAttachment,
    TurnResult,
)
from signal_hermes_router.private_fs import ensure_private_dir_tree, write_private_bytes
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_event,
    make_router_harness,
    RouterTestCase,
)


class RouterSweepTests(RouterTestCase):
    DAY_SECONDS = 86400.0

    def _write_archive_file(self, tmp: str, relative: str, age_seconds: float) -> Path:
        path = Path(tmp) / "media" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic media body")
        moment = time.time() - age_seconds
        os.utime(path, (moment, moment))
        return path

    def _retention_config(self, **overrides: Any) -> RetentionConfig:
        values: dict[str, Any] = {
            "sweep_interval_seconds": 3600.0,
            "dedupe_handled_seconds": 30 * self.DAY_SECONDS,
            "media_max_age_seconds": 30 * self.DAY_SECONDS,
        }
        values.update(overrides)
        return RetentionConfig(**values)

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
