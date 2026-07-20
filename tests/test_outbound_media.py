from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from signal_hermes_router.outbound_media import (
    OutboundAttachmentError,
    signal_base_url_supports_local_attachment_paths,
    validate_outbound_attachments,
)
from tests.support import write_test_file


class OutboundMediaTests(unittest.TestCase):
    def test_signal_base_url_supports_only_local_http_hosts(self) -> None:
        supported = (
            "http://localhost:8080",
            "http://127.0.0.1:8080",
            "https://[::1]:8080",
        )
        unsupported = (
            "unix:///run/signal-cli.sock",
            "ftp://localhost:8080",
            "http://signal.example:8080",
            "http://" + "192.0.2.50" + ":8080",
        )

        for url in supported:
            with self.subTest(url=url):
                self.assertTrue(signal_base_url_supports_local_attachment_paths(url))
        for url in unsupported:
            with self.subTest(url=url):
                self.assertFalse(signal_base_url_supports_local_attachment_paths(url))

    def test_validate_outbound_attachment_accepts_one_image_under_media_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            image = write_test_file(media_root / "alerts" / "person.png")

            attachments = validate_outbound_attachments(
                [str(image)],
                media_root=media_root,
                max_bytes=1024,
            )

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].path, image.resolve())
        self.assertEqual(attachments[0].content_type, "image/png")
        self.assertEqual(attachments[0].size, 4)

    def test_validate_outbound_attachment_rejects_bad_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            cases = (
                ("not-a-list", "invalid_attachment"),
                ([1], "invalid_attachment"),
                ([""], "invalid_attachment"),
                (["/tmp/a.png", "/tmp/b.png"], "too_many_attachments"),
            )
            for raw, error in cases:
                with self.subTest(error=error):
                    with self.assertRaises(OutboundAttachmentError) as raised:
                        validate_outbound_attachments(raw, media_root=media_root, max_bytes=1024)
                    self.assertEqual(raised.exception.error_code, error)

    def test_validate_outbound_attachment_rejects_relative_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments(
                    ["alerts/person.png"],
                    media_root=Path(tmp) / "media",
                    max_bytes=1024,
                )

        self.assertEqual(raised.exception.error_code, "attachment_path_not_absolute")

    def test_validate_outbound_attachment_rejects_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outside = write_test_file(Path(tmp) / "outside.png")
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments(
                    [str(outside)],
                    media_root=Path(tmp) / "media",
                    max_bytes=1024,
                )

        self.assertEqual(raised.exception.error_code, "attachment_path_escaped_root")

    def test_validate_outbound_attachment_rejects_missing_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments(
                    [str(media_root / "missing.png")],
                    media_root=media_root,
                    max_bytes=1024,
                )

        self.assertEqual(raised.exception.error_code, "attachment_not_found")

    def test_validate_outbound_attachment_rejects_non_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            directory = media_root / "directory.png"
            directory.mkdir(parents=True)
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments(
                    [str(directory)],
                    media_root=media_root,
                    max_bytes=1024,
                )

        self.assertEqual(raised.exception.error_code, "attachment_not_file")

    def test_validate_outbound_attachment_rejects_oversized_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            image = write_test_file(media_root / "large.png", b"too-large")
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments([str(image)], media_root=media_root, max_bytes=3)

        self.assertEqual(raised.exception.error_code, "attachment_too_large")

    def test_validate_outbound_attachment_rejects_non_image_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            text = write_test_file(media_root / "note.txt")
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments([str(text)], media_root=media_root, max_bytes=1024)

        self.assertEqual(raised.exception.error_code, "attachment_not_image")

    def test_validate_outbound_attachment_rejects_public_file_or_parent_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            image = write_test_file(media_root / "alerts" / "person.png")
            image.chmod(0o644)
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments([str(image)], media_root=media_root, max_bytes=1024)
            self.assertEqual(raised.exception.error_code, "attachment_not_private")

            image.chmod(0o600)
            image.parent.chmod(0o755)
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments([str(image)], media_root=media_root, max_bytes=1024)
            self.assertEqual(raised.exception.error_code, "attachment_not_private")

    def test_validate_outbound_attachment_rejects_svg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            svg = write_test_file(media_root / "image.svg", b"<svg></svg>")
            with self.assertRaises(OutboundAttachmentError) as raised:
                validate_outbound_attachments([str(svg)], media_root=media_root, max_bytes=1024)

        self.assertEqual(raised.exception.error_code, "attachment_not_image")

    def test_validate_outbound_attachment_accepts_webp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            media_root = Path(tmp) / "media"
            image = write_test_file(media_root / "image.webp", b"webp")

            attachments = validate_outbound_attachments(
                [str(image)],
                media_root=media_root,
                max_bytes=1024,
            )

        self.assertEqual(attachments[0].content_type, "image/webp")


if __name__ == "__main__":
    unittest.main()
