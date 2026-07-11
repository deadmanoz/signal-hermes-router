#!/usr/bin/env python3
"""Fake ACP agent that spawns a child process and shuts down gracefully.

On SIGTERM it terminates and reaps its child, writes a marker file recording
the child's exit, and exits 0 — modelling a Hermes profile that cleans up its
own bridge children when the router's supervisor terminates it gracefully.
"""

from __future__ import annotations

import json
import signal
import subprocess
import sys
from pathlib import Path

marker_path = Path(sys.argv[1])
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
marker_path.with_suffix(".childpid").write_text(str(child.pid), encoding="utf-8")


def _terminate(_signum: int, _frame: object) -> None:
    child.terminate()
    returncode = child.wait()
    marker_path.write_text(f"child_returncode={returncode}\n", encoding="utf-8")
    sys.exit(0)


signal.signal(signal.SIGTERM, _terminate)


def send(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {
                        "sessionCapabilities": {"resume": {}},
                    },
                },
            }
        )
    elif request_id is not None:
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
