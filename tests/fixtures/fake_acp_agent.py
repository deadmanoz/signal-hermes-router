#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse


next_session = 1


def send(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


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
                    "agentCapabilities": {
                        "promptCapabilities": {
                            "image": True,
                            "audio": False,
                            "embeddedContext": False,
                        },
                        "sessionCapabilities": {"resume": {}},
                    },
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
        sid = params["sessionId"]
        prompt = params.get("prompt") or []
        send(
            {
                "jsonrpc": "2.0",
                "id": "permission-1",
                "method": "session/request_permission",
                "params": {
                    "sessionId": sid,
                    "toolCall": {
                        "toolCallId": "tool-1",
                        "toolName": "read_file",
                        "title": "read_file",
                        "rawInput": {"path": "/private/deployment/read-only/note.txt"},
                    },
                    "options": [
                        {"optionId": "allow", "name": "Allow", "kind": "allow_once"},
                        {"optionId": "reject", "name": "Reject", "kind": "reject_once"},
                    ],
                },
            }
        )
        permission_response = json.loads(sys.stdin.readline())
        selected = ((permission_response.get("result") or {}).get("outcome") or {}).get("optionId")
        text = "allowed" if selected == "allow" else "denied"
        for block in prompt:
            if block.get("type") != "resource_link":
                continue
            uri = str(block.get("uri", ""))
            if not uri.startswith("file://"):
                continue
            path = Path(unquote(urlparse(uri).path))
            if base64.b64encode(path.read_bytes()).decode("ascii"):
                text += " image"
                break
        if any("attachment_manifest:" in str(block.get("text", "")) for block in prompt):
            text += " manifest"
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {"content": {"type": "text", "text": text}},
                },
            }
        )
        send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})
    else:
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"unknown method {method}"},
            }
        )
