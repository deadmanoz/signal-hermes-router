from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any, Callable

import httpx

LOGGER = logging.getLogger(__name__)

DEFAULT_SSE_READ_TIMEOUT_SECONDS = 60.0
SEND_RETRY_DELAY_SECONDS = 0.5

# Retry only on failures where signal-cli could not have begun processing the
# request — strictly pre-send transport errors. signal-cli's `send` is not
# idempotent (each accepted send delivers a message), so ReadTimeout,
# WriteError, RemoteProtocolError, etc. must NOT be retried even though they
# are httpx.RequestError subclasses, because the daemon may have already
# accepted and delivered the message before the response was lost.
_RETRYABLE_SEND_ERRORS: tuple[type[Exception], ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


class SignalHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        sse_read_timeout: float = DEFAULT_SSE_READ_TIMEOUT_SECONDS,
        max_event_bytes: int | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_event_bytes = max_event_bytes
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            transport=transport,
        )
        # SSE stream is long-lived; signal-cli emits keepalive comments
        # every ~15s, so a longer per-read timeout avoids a tight reconnect
        # cycle on quiet routes while still failing fast on a dead daemon.
        connect_write_pool = min(timeout, 10.0)
        self._events_client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(
                connect=connect_write_pool,
                read=sse_read_timeout,
                write=connect_write_pool,
                pool=connect_write_pool,
            ),
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()
        await self._events_client.aclose()

    async def check(self) -> bool:
        response = await self._client.get("/api/v1/check")
        return response.status_code == 200

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, Any]:
        return await self._send("groupId", group_id, message, attachments)

    async def send_direct(
        self,
        recipient: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, Any]:
        return await self._send("recipient", [recipient], message, attachments)

    async def _send(
        self,
        recipient_key: str,
        recipient_value: str | list[str],
        message: str,
        attachments: Sequence[str],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {recipient_key: recipient_value, "message": message}
        if attachments:
            params["attachments"] = list(attachments)
        try:
            return await self.rpc("send", params)
        except _RETRYABLE_SEND_ERRORS as exc:
            LOGGER.warning(
                "Signal send pre-send %s; retrying once after %.1fs",
                exc.__class__.__name__,
                SEND_RETRY_DELAY_SECONDS,
            )
            await asyncio.sleep(SEND_RETRY_DELAY_SECONDS)
            return await self.rpc("send", params)

    async def send_typing(self, group_id: str, enabled: bool = True) -> dict[str, Any]:
        params: dict[str, Any] = {"groupId": group_id}
        if not enabled:
            params["stop"] = True
        return await self.rpc("sendTyping", params)

    async def send_typing_direct(self, recipient: str, enabled: bool = True) -> dict[str, Any]:
        params: dict[str, Any] = {"recipient": [recipient]}
        if not enabled:
            params["stop"] = True
        return await self.rpc("sendTyping", params)

    async def rpc(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = uuid.uuid4().hex
        response = await self._client.post(
            "/api/v1/rpc",
            json={"jsonrpc": "2.0", "method": method, "params": params or {}, "id": request_id},
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])
        return payload.get("result") or {}

    async def events(
        self,
        *,
        reconnect_delay: float = 2.0,
        max_reconnect_delay: float = 60.0,
    ) -> AsyncIterator[dict[str, Any]]:
        delay = reconnect_delay
        limiter = MalformedFrameLimiter()
        while True:
            try:
                async with self._events_client.stream("GET", "/api/v1/events") as response:
                    response.raise_for_status()
                    async for event in _iter_sse_json(
                        response, max_event_bytes=self.max_event_bytes, limiter=limiter
                    ):
                        delay = reconnect_delay
                        yield event
                LOGGER.warning("Signal event stream ended; reconnecting")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning(
                    "Signal event stream failed with %s; reconnecting in %.1fs",
                    exc.__class__.__name__,
                    delay,
                )
            jitter = random.uniform(0, min(1.0, delay * 0.2))
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, max_reconnect_delay)


class MalformedFrameLimiter:
    def __init__(
        self,
        *,
        max_logs: int = 3,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_logs = max_logs
        self.window_seconds = window_seconds
        self._clock = clock
        self._count = 0
        self._suppressed = 0
        self._window_start: float | None = None

    def log_malformed(self, frame_size: int) -> bool:
        now = self._clock()
        if self._window_start is None:
            self._window_start = now
        if now - self._window_start > self.window_seconds:
            if self._suppressed > 0:
                LOGGER.warning(
                    "%d malformed SSE frames suppressed in last %.0fs",
                    self._suppressed,
                    self.window_seconds,
                )
            self._count = 0
            self._suppressed = 0
            self._window_start = now
        if self._count < self.max_logs:
            self._count += 1
            return True
        self._suppressed += 1
        return False


async def _iter_sse_json(
    response: httpx.Response,
    *,
    max_event_bytes: int | None = None,
    limiter: MalformedFrameLimiter | None = None,
) -> AsyncIterator[dict[str, Any]]:
    data_lines: list[str] = []
    event_bytes = 0
    async for line in response.aiter_lines():
        if line == "":
            if data_lines:
                data = "\n".join(data_lines)
                frame_bytes = event_bytes
                data_lines.clear()
                event_bytes = 0
                event = _decode_sse_frame(data, frame_bytes, limiter=limiter)
                if event is not None:
                    yield event
            continue
        if line.startswith("data:"):
            data_line = line[5:].lstrip()
            event_bytes = _next_sse_event_size(event_bytes, data_line, max_event_bytes)
            data_lines.append(data_line)
    if data_lines:
        event = _decode_sse_frame("\n".join(data_lines), event_bytes, limiter=limiter)
        if event is not None:
            yield event


def _decode_sse_frame(
    data: str, size_bytes: int, *, limiter: MalformedFrameLimiter | None = None
) -> dict[str, Any] | None:
    # Mirror the ACP peer's log-and-continue handling of undecodable lines:
    # one malformed frame must not tear down the SSE stream, because a
    # reconnect can silently lose stream position (signal-cli has no replay).
    # The warning stays content-free; Signal payloads carry group IDs, phone
    # numbers, and message text. json.loads can fail with more than
    # JSONDecodeError: a plain ValueError for integers over the interpreter
    # digit limit and RecursionError for deeply nested payloads. All are
    # decode-local failures of this one frame, so treat them alike.
    try:
        payload = json.loads(data)
    except (ValueError, RecursionError):
        payload = None
    if not isinstance(payload, dict):
        if limiter is None or limiter.log_malformed(size_bytes):
            LOGGER.warning("Skipping malformed Signal SSE frame (%d bytes)", size_bytes)
        return None
    return payload


def _next_sse_event_size(current: int, line: str, max_event_bytes: int | None) -> int:
    next_size = current + len(line.encode("utf-8")) + (1 if current else 0)
    if max_event_bytes is not None and next_size > max_event_bytes:
        raise ValueError("Signal SSE event exceeds max_signal_event_bytes")
    return next_size
