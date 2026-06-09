from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import __version__
from .models import TurnResult
from .permissions import StaticPermissionPolicy
from .private_fs import ensure_private_dir_tree

LOGGER = logging.getLogger(__name__)
DEFAULT_MAX_ACP_LINE_BYTES = 8 * 1024 * 1024
DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS = 300.0


class JsonRpcError(RuntimeError):
    def __init__(self, error: dict[str, Any]) -> None:
        self.error = error
        super().__init__(error.get("message", str(error)))


RequestHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def default_hermes_command(profile: str) -> list[str]:
    hermes = Path(sys.executable).with_name("hermes")
    executable = str(hermes) if hermes.exists() else "hermes"
    return [executable, "-p", profile, "acp"]


class JsonRpcStdioPeer:
    def __init__(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        max_line_bytes: int | None = DEFAULT_MAX_ACP_LINE_BYTES,
        request_handlers: dict[str, RequestHandler] | None = None,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = env
        self.max_line_bytes = max_line_bytes
        self.request_handlers = request_handlers or {}
        self.process: asyncio.subprocess.Process | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._session_updates: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    async def start(self) -> None:
        merged_env = os.environ.copy()
        if self.env:
            merged_env.update(self.env)
        stream_kwargs: dict[str, int] = {}
        if self.max_line_bytes is not None:
            stream_kwargs["limit"] = self.max_line_bytes + 1
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.cwd,
            env=merged_env,
            **stream_kwargs,
        )
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def close(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader_task
        if self._stderr_task:
            self._stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._stderr_task
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()

    async def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 300.0
    ) -> dict[str, Any]:
        if not self.process or not self.process.stdin:
            raise RuntimeError("JSON-RPC peer is not started")
        request_id = next(self._ids)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
        try:
            async with self._write_lock:
                self.process.stdin.write(json.dumps(payload).encode("utf-8") + b"\n")
                await self.process.stdin.drain()
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def _drain_stderr(self) -> None:
        assert self.process and self.process.stderr
        stderr = self.process.stderr
        while True:
            try:
                line = await stderr.readline()
            except ValueError:
                # asyncio.StreamReader.readline() internally converts the
                # underlying LimitOverrunError to ValueError after either
                # advancing past the buffered newline or clearing the
                # buffer outright. The stream is in a recoverable state on
                # the next iteration — just log and continue.
                LOGGER.warning("Hermes stderr line exceeded line buffer; discarded")
                continue
            if not line:
                break

    async def _read_loop(self) -> None:
        assert self.process and self.process.stdout
        try:
            while True:
                try:
                    line = await _read_limited_line(self.process.stdout, self.max_line_bytes)
                except ValueError:
                    LOGGER.warning(
                        "JSON-RPC peer stdout exceeded configured line limit; skipping line"
                    )
                    continue
                if not line:
                    break
                try:
                    payload = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                if "id" in payload and "method" not in payload:
                    self._handle_response(payload)
                elif "id" in payload and "method" in payload:
                    await self._handle_request(payload)
                elif "method" in payload:
                    self._handle_notification(payload)
        finally:
            error = RuntimeError("JSON-RPC peer exited")
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()

    def _handle_response(self, payload: dict[str, Any]) -> None:
        request_id = payload.get("id")
        future = self._pending.pop(request_id, None)
        if not future or future.done():
            return
        if "error" in payload:
            future.set_exception(JsonRpcError(payload["error"]))
        else:
            future.set_result(payload.get("result") or {})

    async def _handle_request(self, payload: dict[str, Any]) -> None:
        assert self.process and self.process.stdin
        handler = self.request_handlers.get(payload["method"])
        if handler is None:
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "error": {"code": -32601, "message": "method not supported by router"},
            }
        else:
            try:
                result = await handler(payload.get("params") or {})
                response = {"jsonrpc": "2.0", "id": payload["id"], "result": result}
            except Exception as exc:
                response = {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "error": {"code": -32603, "message": str(exc)},
                }
        self.process.stdin.write(json.dumps(response).encode("utf-8") + b"\n")
        await self.process.stdin.drain()

    def subscribe_session(self, session_id: str) -> asyncio.Queue[dict[str, Any]]:
        return self._session_updates.setdefault(session_id, asyncio.Queue())

    def release_session(self, session_id: str) -> None:
        self._session_updates.pop(session_id, None)

    def _handle_notification(self, payload: dict[str, Any]) -> None:
        if payload.get("method") != "session/update":
            return
        params = payload.get("params") or {}
        session_id = params.get("sessionId")
        if session_id is None:
            return
        queue = self._session_updates.get(str(session_id))
        if queue is not None:
            queue.put_nowait(payload)


@dataclass
class ACPProfile:
    profile: str
    work_root: Path
    command: list[str] | None = None
    peer: JsonRpcStdioPeer | None = None
    max_line_bytes: int | None = DEFAULT_MAX_ACP_LINE_BYTES
    prompt_timeout_seconds: float = DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS
    agent_capabilities: dict[str, Any] = field(default_factory=dict)
    permission_policies: dict[str, StaticPermissionPolicy] = field(default_factory=dict)
    prompt_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    async def start(self) -> None:
        command = self.command or default_hermes_command(self.profile)
        self.peer = JsonRpcStdioPeer(
            command,
            max_line_bytes=self.max_line_bytes,
            request_handlers={
                "session/request_permission": self._request_permission,
                "fs/read_text_file": self._unsupported_client_request,
                "fs/write_text_file": self._unsupported_client_request,
                "terminal/create": self._unsupported_client_request,
                "terminal/output": self._unsupported_client_request,
                "terminal/wait_for_exit": self._unsupported_client_request,
                "terminal/kill": self._unsupported_client_request,
                "terminal/release": self._unsupported_client_request,
            },
        )
        await self.peer.start()
        try:
            result = await self.peer.request(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientInfo": {"name": "signal-hermes-router", "version": __version__},
                    "clientCapabilities": {
                        "fs": {"readTextFile": False, "writeTextFile": False},
                        "terminal": False,
                    },
                },
            )
        except BaseException:
            # peer.start() already spawned the subprocess and the reader /
            # stderr-drain tasks. If initialize fails (timeout, JSON-RPC
            # error, cancellation), we own those resources and must close
            # them before propagating — otherwise the subprocess and tasks
            # are orphaned and the supervisor's cooldown then prevents
            # replacement.
            with suppress(Exception):
                await self.peer.close()
            self.peer = None
            raise
        self.agent_capabilities = result.get("agentCapabilities") or {}

    async def close(self) -> None:
        if self.peer:
            await self.peer.close()

    async def new_session(self, cwd: Path) -> str:
        assert self.peer
        ensure_private_dir_tree(self.work_root, cwd)
        result = await self.peer.request(
            "session/new", {"cwd": str(cwd.resolve()), "mcpServers": []}
        )
        session_id = str(result["sessionId"])
        self.peer.subscribe_session(session_id)
        return session_id

    async def resume_session(self, session_id: str, cwd: Path) -> bool:
        assert self.peer
        capabilities = self.agent_capabilities.get("sessionCapabilities") or {}
        if "resume" not in capabilities or capabilities.get("resume") is False:
            return False
        ensure_private_dir_tree(self.work_root, cwd)
        await self.peer.request(
            "session/resume", {"sessionId": session_id, "cwd": str(cwd.resolve())}
        )
        self.peer.subscribe_session(session_id)
        return True

    def set_permission_policy(self, session_id: str, policy: StaticPermissionPolicy) -> None:
        self.permission_policies[session_id] = policy

    def release_session(self, session_id: str) -> None:
        if self.peer:
            self.peer.release_session(session_id)
        self.permission_policies.pop(session_id, None)
        self.prompt_locks.pop(session_id, None)

    async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
        assert self.peer
        queue = self.peer.subscribe_session(session_id)
        lock = self.prompt_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            _drain_queue(queue)
            result = await self.peer.request(
                "session/prompt",
                {"sessionId": session_id, "prompt": blocks},
                timeout=self.prompt_timeout_seconds,
            )
            text = _collect_assistant_text(_drain_queue(queue))
        result_text = str(result.get("text") or "").strip()
        if not text and result_text:
            text = result_text
        return TurnResult(text=text, stop_reason=str(result.get("stopReason", "end_turn")))

    async def _request_permission(self, params: dict[str, Any]) -> dict[str, Any]:
        session_id = str(params.get("sessionId"))
        policy = self.permission_policies.get(session_id)
        if policy is None:
            LOGGER.warning(
                "permission request without registered policy for session %s", session_id
            )
            policy = StaticPermissionPolicy()
        return policy.acp_response(params)

    async def _unsupported_client_request(self, _params: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("router does not expose client filesystem or terminal capabilities")


def _collect_assistant_text(notifications: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for notification in notifications:
        if notification.get("method") != "session/update":
            continue
        params = notification.get("params") or {}
        update = params.get("update") or params
        if isinstance(update, dict):
            update_kind = update.get("session_update") or update.get("sessionUpdate")
            if update_kind and update_kind != "agent_message_chunk":
                continue
            if update_kind == "agent_message_chunk" and "content" in update:
                update = update["content"]
        _extract_text(update, chunks)
    return "".join(chunks).strip()


def _extract_text(value: Any, chunks: list[str]) -> None:
    if isinstance(value, dict):
        if value.get("type") == "text" and isinstance(value.get("text"), str):
            chunks.append(value["text"])
        for child in value.values():
            _extract_text(child, chunks)
    elif isinstance(value, list):
        for child in value:
            _extract_text(child, chunks)


def _drain_queue(queue: asyncio.Queue[dict[str, Any]]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    while True:
        try:
            values.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return values


async def _read_limited_line(reader: Any, max_line_bytes: int | None) -> bytes:
    if hasattr(reader, "readuntil"):
        try:
            line = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as exc:
            line = exc.partial
        except asyncio.LimitOverrunError as exc:
            if hasattr(reader, "read"):
                await reader.read(exc.consumed)
            raise ValueError("JSON-RPC stdout line exceeds max_acp_line_bytes") from exc
    else:
        line = await reader.readline()
    if max_line_bytes is not None and len(line.rstrip(b"\r\n")) > max_line_bytes:
        raise ValueError("JSON-RPC stdout line exceeds max_acp_line_bytes")
    return line
