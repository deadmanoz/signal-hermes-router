#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import threading
import time


next_session = 1
write_lock = threading.Lock()


def send(payload: dict) -> None:
    with write_lock:
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()


def answer_prompt(request_id: int, session_id: str) -> None:
    if session_id.endswith("1"):
        time.sleep(0.05)
    else:
        time.sleep(0.01)
    send(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {"content": {"type": "text", "text": f"reply-{session_id}"}},
            },
        }
    )
    send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params") or {}
    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {"sessionCapabilities": {"resume": {}}},
                },
            }
        )
    elif method == "session/new":
        sid = f"session-{next_session}"
        next_session += 1
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": sid}})
    elif method == "session/resume":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/prompt":
        threading.Thread(
            target=answer_prompt,
            args=(request_id, params["sessionId"]),
            daemon=True,
        ).start()
    else:
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"unknown method {method}"},
            }
        )
