from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "deploy-service-tree.sh"


class DeployScriptTests(unittest.TestCase):
    def test_script_has_valid_bash_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)

    def test_rsync_delete_protects_deployment_local_state(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("--delete", text)
        self.assertIn("--exclude='/*.local.md'", text)
        self.assertIn("--exclude='/private/'", text)
        self.assertIn("--exclude='/.venv/'", text)

    def test_non_dry_run_invocation_reaches_rsync(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            bin_path = temp_path / "bin"
            bin_path.mkdir()
            log_path = temp_path / "rsync-args.txt"
            fake_rsync = bin_path / "rsync"
            fake_rsync.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                'printf \'%s\\n\' "$@" > "$RSYNC_ARGS_LOG"\n',
                encoding="utf-8",
            )
            fake_rsync.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = f"{bin_path}:{env.get('PATH', '')}"
            env["RSYNC_ARGS_LOG"] = str(log_path)

            subprocess.run(
                ["bash", str(SCRIPT), "example-host", "/srv/signal-hermes-router"],
                check=True,
                env=env,
            )

            args = log_path.read_text(encoding="utf-8").splitlines()
            self.assertIn("-av", args)
            self.assertIn("--delete", args)
            self.assertNotIn("--dry-run", args)


if __name__ == "__main__":
    unittest.main()
