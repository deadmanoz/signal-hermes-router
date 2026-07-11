from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import sys
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import __version__
from .models import TurnResult
from .permissions import StaticPermissionPolicy
from .preflight import (
    PreflightProbeUnavailable,
    ToolSurface,
    tool_surface_from_agent_capabilities,
    tool_surface_from_value,
)
from .private_fs import ensure_private_dir_tree

LOGGER = logging.getLogger(__name__)
DEFAULT_MAX_ACP_LINE_BYTES = 8 * 1024 * 1024
DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS = 300.0
DEFAULT_ACP_INITIALIZE_TIMEOUT_SECONDS = 30.0
DEFAULT_ACP_TOOL_SURFACE_TIMEOUT_SECONDS = 30.0
STDERR_TAIL_MAX_LINES = 20
STDERR_TAIL_MAX_LINE_CHARS = 400
# Post-exit settle bound for the pipe-drain tasks: long enough to capture the
# stderr written just before death and a final buffered stdout response, short
# enough that the exit is reported well within a second of the child dying
# even when a grandchild inherited the pipes and holds EOF open.
EXIT_SETTLE_TIMEOUT_SECONDS = 0.5
# asyncio's Process.wait() waiters are only woken once the child is reaped AND
# all of its pipes have disconnected, so a grandchild that inherited the pipes
# can delay wait() long past the actual death. The watcher therefore also
# polls the transport's returncode (set at reap time) on this interval.
EXIT_POLL_INTERVAL_SECONDS = 0.2
# Close-side bound for handing the exit report off to the watcher when the
# child demonstrably died on its own before close() was requested.
CLOSE_EXIT_REPORT_TIMEOUT_SECONDS = 2.0


class JsonRpcError(RuntimeError):
    def __init__(self, error: dict[str, Any]) -> None:
        self.error = error
        super().__init__(error.get("message", str(error)))


class JsonRpcPeerExited(RuntimeError):
    """Raised for pending JSON-RPC requests when the ACP subprocess exits."""


RequestHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
ExitCallback = Callable[[int | None, tuple[str, ...]], None]


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
        on_exit: ExitCallback | None = None,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = env
        self.max_line_bytes = max_line_bytes
        self.request_handlers = request_handlers or {}
        self.on_exit = on_exit
        self.process: asyncio.subprocess.Process | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._exit_watcher_task: asyncio.Task[None] | None = None
        self._expected_exit = False
        # Lazy-discovery exit evidence: the child's pipes broke on their own
        # (stdout EOF or a failed stdin write) before the exit was reaped.
        self._stdout_eof = False
        self._stdin_write_failed = False
        self._stderr_tail: deque[str] = deque(maxlen=STDERR_TAIL_MAX_LINES)
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
        self._exit_watcher_task = asyncio.create_task(self._watch_exit())

    async def close(self) -> None:
        watcher = self._exit_watcher_task
        try:
            if watcher is not None and not watcher.done() and self.exit_evidence():
                # The child's exit (or its broken pipes) predates this close:
                # let the watcher report the unexpected exit before it is
                # suppressed. This is the lazy-discovery incident class, where
                # recovery closes the peer as soon as a request fails and
                # would otherwise reclassify the crash as an intentional
                # shutdown. The bound covers the one non-death way evidence
                # can exist (a live child that closed its own stdout is never
                # reaped); such a child is terminated below as a normal
                # expected exit. Inside the try block so cancellation during
                # this wait still reaches the kill backstop.
                await asyncio.wait({watcher}, timeout=CLOSE_EXIT_REPORT_TIMEOUT_SECONDS)
            self._expected_exit = True
            if watcher is not None:
                watcher.cancel()
                with suppress(asyncio.CancelledError):
                    await watcher
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
        finally:
            # Cancellation-safe backstop: no exit path of close() may leave a
            # running subprocess (e.g. cancellation during the terminate
            # grace). The kill is synchronous; reaping falls to the event
            # loop's child watcher.
            if self.process and self.process.returncode is None:
                with suppress(ProcessLookupError):
                    self.process.kill()

    async def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 300.0
    ) -> Any:
        if not self.process or not self.process.stdin:
            raise RuntimeError("JSON-RPC peer is not started")
        request_id = next(self._ids)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
        try:
            async with self._write_lock:
                await self._write_frame(payload)
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def _write_frame(self, message: dict[str, Any]) -> None:
        try:
            self.process.stdin.write(json.dumps(message).encode("utf-8") + b"\n")
            await self.process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            # Exit evidence recorded before re-raising, so close() can hand
            # the exit report off to the watcher even when the child has not
            # been reaped yet.
            self._stdin_write_failed = True
            raise

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
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                self._stderr_tail.append(text[:STDERR_TAIL_MAX_LINE_CHARS])

    def stderr_tail(self) -> tuple[str, ...]:
        return tuple(self._stderr_tail)

    def exit_evidence(self) -> bool:
        process = self.process
        return (
            (process is not None and process.returncode is not None)
            or self._stdout_eof
            or self._stdin_write_failed
        )

    async def _wait_for_exit(self) -> int | None:
        # Process.wait() alone is not a reliable exit signal: its waiters are
        # woken only after every pipe has disconnected, so a grandchild that
        # inherited the child's stdio can postpone it long past the death.
        # The returncode attribute is set at reap time, so poll it alongside.
        assert self.process
        process = self.process
        wait_task = asyncio.ensure_future(process.wait())
        try:
            while True:
                done, _pending = await asyncio.wait({wait_task}, timeout=EXIT_POLL_INTERVAL_SECONDS)
                if done:
                    return wait_task.result()
                if process.returncode is not None:
                    # Reaped, but held-open pipes keep wait() from resolving.
                    return process.returncode
        finally:
            if not wait_task.done():
                wait_task.cancel()
                with suppress(asyncio.CancelledError):
                    await wait_task

    async def _watch_exit(self) -> None:
        assert self.process
        returncode = await self._wait_for_exit()
        # Settle bound: let the pipe drains deliver a final buffered stdout
        # response and the stderr written just before death, then cancel
        # whatever is still pending -- a grandchild that inherited the child's
        # pipes can hold EOF open indefinitely, and an evicted peer must not
        # leak permanently-pending drain tasks.
        drains = {task for task in (self._reader_task, self._stderr_task) if task is not None}
        if drains:
            _done, pending = await asyncio.wait(drains, timeout=EXIT_SETTLE_TIMEOUT_SECONDS)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending)
        if self._expected_exit or self.on_exit is None:
            return
        try:
            self.on_exit(returncode, self.stderr_tail())
        except Exception:
            # The callback is observability plumbing; its failure must not
            # kill the watcher task or propagate into a close() awaiting it.
            LOGGER.warning("ACP exit callback failed")
            LOGGER.debug("ACP exit callback failure details", exc_info=True)

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
                    # Genuine EOF: exit evidence for close(), recorded before
                    # the pending futures are failed in the finally below.
                    self._stdout_eof = True
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
            error = JsonRpcPeerExited("JSON-RPC peer exited")
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
            future.set_result(payload["result"] if "result" in payload else {})

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
        await self._write_frame(response)

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
    initialize_timeout_seconds: float = DEFAULT_ACP_INITIALIZE_TIMEOUT_SECONDS
    agent_capabilities: dict[str, Any] = field(default_factory=dict)
    permission_policies: dict[str, StaticPermissionPolicy] = field(default_factory=dict)
    prompt_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    on_exit: ExitCallback | None = None

    async def start(self) -> None:
        command = self.command or default_hermes_command(self.profile)
        self.peer = JsonRpcStdioPeer(
            command,
            max_line_bytes=self.max_line_bytes,
            on_exit=self._notify_exit,
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
                timeout=self.initialize_timeout_seconds,
            )
        except BaseException as exc:
            # peer.start() already spawned the subprocess and the reader /
            # stderr-drain tasks. If initialize fails (timeout, JSON-RPC
            # error, cancellation), we own those resources and must close
            # them before propagating — otherwise the subprocess and tasks
            # are orphaned and the supervisor's cooldown then prevents
            # replacement.
            with suppress(Exception):
                await self.peer.close()
            self.peer = None
            if isinstance(exc, TimeoutError):
                # A bare wait_for timeout has an empty message; name the
                # handshake and the configured bound so classified failure
                # detail is actionable.
                raise TimeoutError(
                    f"ACP initialize timed out after {self.initialize_timeout_seconds:g}s"
                ) from exc
            raise
        self.agent_capabilities = result.get("agentCapabilities") or {}

    async def close(self) -> None:
        if self.peer:
            await self.peer.close()

    def _notify_exit(self, returncode: int | None, stderr_tail: tuple[str, ...]) -> None:
        callback = self.on_exit
        if callback is not None:
            callback(returncode, stderr_tail)

    def exit_suspected(self) -> bool:
        """Best-effort synchronous check that the supervised child already
        died (reaped returncode, stdout EOF, or a failed stdin write) --
        usable before the exit watcher has had a chance to run."""
        return self.peer is not None and self.peer.exit_evidence()

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

    async def tool_surface(self) -> ToolSurface:
        surface = tool_surface_from_agent_capabilities(self.profile, self.agent_capabilities)
        if surface is not None:
            return surface
        if self.peer is None:
            raise PreflightProbeUnavailable("probe_not_started")
        try:
            result = await self.peer.request(
                "_tool_surface/list",
                timeout=DEFAULT_ACP_TOOL_SURFACE_TIMEOUT_SECONDS,
            )
        except JsonRpcError as exc:
            if exc.error.get("code") == -32601:
                raise PreflightProbeUnavailable("probe_unsupported") from exc
            raise
        surface = tool_surface_from_value(self.profile, result, source="_tool_surface/list")
        if surface is None:
            raise PreflightProbeUnavailable("probe_empty")
        return surface

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
