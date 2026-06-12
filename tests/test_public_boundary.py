from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check-public-boundary.py"
SPEC = importlib.util.spec_from_file_location("check_public_boundary", SCRIPT)
assert SPEC is not None
public_boundary = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(public_boundary)


class PublicBoundaryCheckTests(unittest.TestCase):
    def test_markdown_allows_public_reference_links(self) -> None:
        findings: list[str] = []
        docs_url = "https://" + "docs.python.org" + "/3/"

        public_boundary.check_text(
            Path("README.md"),
            f"For background, see [Python docs]({docs_url}).\n",
            findings,
        )

        self.assertEqual(findings, [])

    def test_markdown_rejects_endpoint_context_with_real_hostname(self) -> None:
        findings: list[str] = []
        private_endpoint = "https://" + "router-control.public.tld" + "/events"

        public_boundary.check_text(
            Path("docs/configuration.md"),
            f"Set signal_base_url to {private_endpoint}.\n",
            findings,
        )

        self.assertIn("unexpected hostname", findings[0])

    def test_rejects_private_like_markdown_hosts(self) -> None:
        findings: list[str] = []
        private_url = "https://" + "signal-gateway.internal" + "/rpc"

        public_boundary.check_text(
            Path("README.md"),
            f"See [private deployment]({private_url}).\n",
            findings,
        )

        self.assertIn("unexpected hostname", findings[0])

    def test_group_ids_can_be_unquoted_and_must_be_synthetic(self) -> None:
        findings: list[str] = []
        key = "group_id"

        public_boundary.check_text(
            Path("routes.example.yaml"),
            f"{key}: REAL_SIGNAL_GROUP_ID\n",
            findings,
        )

        self.assertIn("non-synthetic group_id", findings[0])

    def test_direct_sender_uuid_and_number_must_not_be_real_values(self) -> None:
        findings: list[str] = []
        private_uuid = "-".join(["11111111", "1111", "1111", "1111", "111111111111"])
        private_phone = "+123" + "45678901"

        public_boundary.check_text(
            Path("routes.example.yaml"),
            f"""
routes:
  - platform: signal
    chat_type: direct
    sender_id: {private_uuid}
    sender_number: {private_phone}
    friendly_name: synthetic-direct-example
""",
            findings,
        )

        self.assertTrue(any("UUID-like identifier" in finding for finding in findings))
        self.assertTrue(any("phone-like identifier" in finding for finding in findings))

    def test_action_commit_shas_are_not_treated_as_base64_secrets(self) -> None:
        findings: list[str] = []

        public_boundary.check_text(
            Path(".github/workflows/ci.yml"),
            "uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5\n",
            findings,
        )

        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
