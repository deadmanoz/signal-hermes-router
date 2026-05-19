from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "deploy-service-tree.sh"


class DeployScriptTests(unittest.TestCase):
    def test_script_has_valid_bash_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)

    def test_rsync_delete_protects_remote_virtualenv(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("--delete", text)
        self.assertIn("--exclude='/.venv/'", text)


if __name__ == "__main__":
    unittest.main()
