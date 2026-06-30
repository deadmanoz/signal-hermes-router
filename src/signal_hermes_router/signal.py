from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any

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
        params = {"groupId": group_id, "message": message}
        if attachments:
            params["attachments"] = list(attachments)
        return await self._send(params)

    async def send_direct(
        self,
        recipient: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, Any]:
        params = {"recipient": [recipient], "message": message}
        if attachments:
            params["attachments"] = list(attachments)
        return await self._send(params)

    async def _send(self, params: dict[str, Any]) -> dict[str, Any]:
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
        while True:
            try:
                async with self._events_client.stream("GET", "/api/v1/events") as response:
                    response.raise_for_status()
                    async for event in _iter_sse_json(
                        response, max_event_bytes=self.max_event_bytes
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


async def _iter_sse_json(
    response: httpx.Response, *, max_event_bytes: int | None = None
) -> AsyncIterator[dict[str, Any]]:
    data_lines: list[str] = []
    event_bytes = 0
    async for line in response.aiter_lines():
        if line == "":
            if data_lines:
                data = "\n".join(data_lines)
                data_lines.clear()
                event_bytes = 0
                yield json.loads(data)
            continue
        if line.startswith("data:"):
            data_line = line[5:].lstrip()
            event_bytes = _next_sse_event_size(event_bytes, data_line, max_event_bytes)
            data_lines.append(data_line)
    if data_lines:
        yield json.loads("\n".join(data_lines))


def _next_sse_event_size(current: int, line: str, max_event_bytes: int | None) -> int:
    next_size = current + len(line.encode("utf-8")) + (1 if current else 0)
    if max_event_bytes is not None and next_size > max_event_bytes:
        raise ValueError("Signal SSE event exceeds max_signal_event_bytes")
    return next_size
