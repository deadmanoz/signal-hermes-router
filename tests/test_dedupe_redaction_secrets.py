from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.redaction import Redactor, sanitize_subprocess_output
from signal_hermes_router.secrets import resolve_secret_refs, resolve_secret_uri
from tests.support import file_mode

_REAL_SQLITE_CONNECT = sqlite3.connect


def _impatient_connect(path: str, **kwargs: object) -> sqlite3.Connection:
    return _REAL_SQLITE_CONNECT(path, timeout=0.2, **kwargs)  # type: ignore[arg-type]


class DedupeTests(unittest.TestCase):
    def test_seen_or_record(self) -> None:
        store = DedupeStore()
        self.assertFalse(store.seen_or_record("uuid", 1))
        self.assertTrue(store.seen_or_record("uuid", 1))
        self.assertFalse(store.seen_or_record("uuid", 2))

    def test_claim_is_route_scoped_and_retryable_until_handled(self) -> None:
        store = DedupeStore()
        self.assertTrue(store.claim("signal:one", "uuid", 1))
        self.assertFalse(store.claim("signal:one", "uuid", 1))
        self.assertTrue(store.claim("signal:two", "uuid", 1))

        store.release("signal:one", "uuid", 1)
        self.assertTrue(store.claim("signal:one", "uuid", 1))
        store.mark_handled("signal:one", "uuid", 1)
        self.assertFalse(store.claim("signal:one", "uuid", 1))

    def test_is_handled_only_matches_completed_claims(self) -> None:
        store = DedupeStore()
        self.assertFalse(store.is_handled("signal:one", "uuid", 1))
        self.assertTrue(store.claim("signal:one", "uuid", 1))
        self.assertFalse(store.is_handled("signal:one", "uuid", 1))
        store.mark_handled("signal:one", "uuid", 1)
        self.assertTrue(store.is_handled("signal:one", "uuid", 1))

    def test_file_backed_store_migrates_legacy_table_and_context_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "router.db"
            path.parent.mkdir()
            connection = sqlite3.connect(path)
            connection.execute(
                "CREATE TABLE dedupe_events ("
                "source_uuid TEXT NOT NULL, "
                "timestamp INTEGER NOT NULL, "
                "PRIMARY KEY (source_uuid, timestamp))"
            )
            connection.execute(
                "INSERT INTO dedupe_events (source_uuid, timestamp) VALUES (?, ?)",
                ("legacy-uuid", 1),
            )
            connection.commit()
            connection.close()

            with DedupeStore(path) as store:
                self.assertFalse(store.claim("", "legacy-uuid", 1))
                store.mark_handled("signal:new-route", "new-uuid", 2)
                self.assertFalse(store.claim("signal:new-route", "new-uuid", 2))
                store.close()
                store.close()
            self.assertEqual(file_mode(path.parent), 0o700)
            self.assertEqual(file_mode(path), 0o600)

    def test_live_store_locks_out_overlapping_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "router.db"
            with DedupeStore(path) as store:
                self.assertTrue(store.claim("signal:route", "in-flight-uuid", 1))
                store.mark_handled("signal:route", "handled-uuid", 2)

                with patch("signal_hermes_router.dedupe.sqlite3.connect", _impatient_connect):
                    with self.assertRaises(sqlite3.OperationalError):
                        DedupeStore(path)

                self.assertEqual(store.status("signal:route", "in-flight-uuid", 1), "processing")
                # The failed same-process overlap must not have dropped the
                # live store's file locks: a separate process stays excluded.
                probe = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "import sqlite3, sys\n"
                        "db = sqlite3.connect(sys.argv[1], timeout=0.2)\n"
                        "try:\n"
                        "    db.execute(\"DELETE FROM dedupe_events WHERE status = 'processing'\")\n"
                        "    db.commit()\n"
                        "except sqlite3.OperationalError:\n"
                        "    sys.exit(3)\n"
                        "sys.exit(0)\n",
                        str(path),
                    ],
                    capture_output=True,
                )
                self.assertEqual(probe.returncode, 3)
                self.assertEqual(store.status("signal:route", "in-flight-uuid", 1), "processing")

            with DedupeStore(path) as reopened:
                self.assertTrue(reopened.is_handled("signal:route", "handled-uuid", 2))

    def test_directory_state_db_fails_without_touching_its_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state-dir"
            path.mkdir()
            os.chmod(path, 0o755)

            with self.assertRaises(sqlite3.OperationalError):
                DedupeStore(path)

            self.assertEqual(file_mode(path), 0o755)

    def test_fresh_live_store_holds_lock_before_any_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "router.db"
            with DedupeStore(path):
                with patch("signal_hermes_router.dedupe.sqlite3.connect", _impatient_connect):
                    with self.assertRaises(sqlite3.OperationalError):
                        DedupeStore(path)

    def test_fresh_store_reclaims_orphaned_processing_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "router.db"
            crashed = DedupeStore(path)
            self.assertTrue(crashed.claim("signal:route", "orphaned-uuid", 1))
            crashed.mark_handled("signal:route", "handled-uuid", 2)
            crashed.close()

            with DedupeStore(path) as store:
                self.assertIsNone(store.status("signal:route", "orphaned-uuid", 1))
                self.assertTrue(store.claim("signal:route", "orphaned-uuid", 1))
                self.assertTrue(store.is_handled("signal:route", "handled-uuid", 2))

    def test_file_backed_store_uses_private_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state" / "router.db"

            with DedupeStore(path):
                pass

            self.assertEqual(file_mode(path.parent), 0o700)
            self.assertEqual(file_mode(path), 0o600)

    def test_destructor_suppresses_close_errors(self) -> None:
        store = DedupeStore()

        def fail_close() -> None:
            raise RuntimeError("synthetic")

        store.close = fail_close  # type: ignore[method-assign]
        store.__del__()


class RedactionTests(unittest.TestCase):
    def test_redacts_known_ids_phones_and_uuids(self) -> None:
        redactor = Redactor({"GROUP-ID"})
        value = redactor.redact("GROUP-ID +00000000000 00000000-0000-0000-0000-000000000000")
        self.assertNotIn("GROUP-ID", value)
        self.assertIn("[phone_redacted]", value)
        self.assertIn("[uuid_redacted]", value)

    def test_sanitize_subprocess_output_masks_representative_secrets(self) -> None:
        raw = (
            "\x1b[31mTraceback (most recent call last):\x1b[0m\n"
            '  File "/opt/hermes/agent.py", line 10, in run\n'
            "Authorization: Bearer abc.DEF-123_456~789\n"
            "api_key=sk-live-abcdefghijklmnop123456\n"
            "password: hunter2secret\n"
            # ROUTER_TEST_SECRET marks this synthetic quoted passphrase for
            # the repo's public-boundary check.
            'password: "ROUTER_TEST_SECRET correct horse battery"\n'
            "Cookie: sid=alpha-sid-value; csrf=beta-csrf-value\n"
            "digest 0123456789abcdef0123456789abcdef01234567\n"
            # The EXAMPLE marker keeps the repo's public-boundary check happy;
            # the sanitizer only cares about the base64-ish shape and length.
            "blob EXAMPLEbase64EXAMPLEbase64EXAMPLEbase64EXAMPLE==\n"
            "bell\x07 and null\x00 bytes\n"
            "progress 42%\rprogress overwritten line\n"
        )

        sanitized = sanitize_subprocess_output(raw)

        self.assertNotIn("\x1b", sanitized)
        self.assertNotIn("\x07", sanitized)
        self.assertNotIn("\x00", sanitized)
        # Carriage returns could overwrite/forge visible log lines.
        self.assertNotIn("\r", sanitized)
        self.assertNotIn("abc.DEF-123_456~789", sanitized)
        self.assertNotIn("sk-live-abcdefghijklmnop123456", sanitized)
        self.assertNotIn("hunter2secret", sanitized)
        # Quoted and header-style multi-token values are masked in full, not
        # just their first whitespace-delimited token.
        self.assertNotIn("correct horse battery", sanitized)
        self.assertNotIn("sid=alpha-sid-value", sanitized)
        self.assertNotIn("csrf=beta-csrf-value", sanitized)
        self.assertNotIn("0123456789abcdef0123456789abcdef01234567", sanitized)
        self.assertNotIn("EXAMPLEbase64", sanitized)
        # Ordinary traceback text stays readable for diagnosis.
        self.assertIn("Traceback (most recent call last):", sanitized)
        self.assertIn('File "/opt/hermes/agent.py", line 10, in run', sanitized)


class SecretTests(unittest.TestCase):
    def test_file_env_and_systemd_credential_resolvers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            secret = Path(tmp) / "secret"
            secret.write_text("from-file\n", encoding="utf-8")
            os.environ["ROUTER_TEST_SECRET"] = "from-env"
            cred_dir = Path(tmp) / "creds"
            cred_dir.mkdir()
            (cred_dir / "account").write_text("from-credential\n", encoding="utf-8")
            os.environ["CREDENTIALS_DIRECTORY"] = str(cred_dir)

            self.assertEqual(resolve_secret_uri(secret.as_uri()), "from-file")
            self.assertEqual(resolve_secret_uri("env://ROUTER_TEST_SECRET"), "from-env")
            self.assertEqual(resolve_secret_uri("systemd-credential://account"), "from-credential")
            self.assertEqual(
                resolve_secret_refs({"a": secret.as_uri(), "b": ["env://ROUTER_TEST_SECRET"]}),
                {"a": "from-file", "b": ["from-env"]},
            )

    def test_systemd_credential_resolver_rejects_path_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["CREDENTIALS_DIRECTORY"] = tmp
            for uri in (
                "systemd-credential://../secret",
                "systemd-credential://nested/secret",
                r"systemd-credential://nested\secret",
                "systemd-credential://.",
            ):
                with self.subTest(uri=uri), self.assertRaises(ValueError):
                    resolve_secret_uri(uri)

    def test_op_resolver_and_secret_error_paths(self) -> None:
        completed = subprocess.CompletedProcess(
            ["op", "read", "op://vault/item/field"],
            0,
            stdout="from-op\n",
            stderr="",
        )
        with patch("signal_hermes_router.secrets.subprocess.run", return_value=completed) as run:
            self.assertEqual(resolve_secret_uri("op://vault/item/field"), "from-op")
        run.assert_called_once_with(
            ["op", "read", "op://vault/item/field"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(KeyError, "missing environment secret"):
                resolve_secret_uri("env://MISSING_SECRET")
            with self.assertRaisesRegex(KeyError, "CREDENTIALS_DIRECTORY"):
                resolve_secret_uri("systemd-credential://account")

        with self.assertRaisesRegex(ValueError, "unsupported secret URI scheme"):
            resolve_secret_uri("https://example.invalid/secret")
            self.assertEqual(resolve_secret_refs("literal"), "literal")

    def test_file_resolver_requires_absolute_local_file_uri(self) -> None:
        with self.assertRaisesRegex(ValueError, "file:///absolute/path"):
            resolve_secret_uri("file://host/secret")
        with self.assertRaisesRegex(ValueError, "file:///absolute/path"):
            resolve_secret_uri("file:relative-secret")


if __name__ == "__main__":
    unittest.main()
