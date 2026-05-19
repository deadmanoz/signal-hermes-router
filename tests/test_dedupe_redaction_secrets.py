from __future__ import annotations

import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.redaction import Redactor
from signal_hermes_router.secrets import resolve_secret_refs, resolve_secret_uri
from tests.support import file_mode


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
