from __future__ import annotations

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check-private-config.py"
SPEC = importlib.util.spec_from_file_location("check_private_config", SCRIPT)
assert SPEC is not None
check_private_config = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(check_private_config)


class PrivateConfigCheckTests(unittest.TestCase):
    def test_reports_redacted_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.yaml"
            routes = root / "routes.yaml"
            config.write_text(
                """
router:
  signal:
    base_url: "http://127.0.0.1:8080"
""",
                encoding="utf-8",
            )
            routes.write_text(
                """
routes:
  - platform: signal
    group_id: GROUP
    friendly_name: synthetic-test-group
    profile: profile-one
    state: shadow
""",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = check_private_config.main([str(config), str(routes)])

        self.assertEqual(code, 0)
        self.assertEqual(
            stdout.getvalue().strip(),
            "routes_parsed=1 states={'shadow': 1} remote_signal_base_url=False",
        )

    def test_error_output_omits_private_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private_name = "secret-private-config"
            config = root / private_name / "config.yaml"
            routes = root / private_name / "routes.yaml"
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = check_private_config.main([str(config), str(routes)])

        self.assertEqual(code, 1)
        self.assertIn("private config validation failed: FileNotFoundError", stderr.getvalue())
        self.assertNotIn(private_name, stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
