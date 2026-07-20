from __future__ import annotations

import base64
import tempfile
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.config import (
    Route,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    NormalizedEvent,
    OutboundAttachment,
    RouteState,
    SessionPolicy,
    SignalAttachment,
    TurnResult,
)
from signal_hermes_router.router import SignalHermesRouter
from signal_hermes_router.outbound_media import (
    OutboundAttachmentError,
    validate_outbound_attachments,
)
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_group_raw,
    make_router_harness,
    make_synthetic_app,
    write_test_file,
    RouterTestCase,
)


class RouterMediaTests(RouterTestCase):
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

