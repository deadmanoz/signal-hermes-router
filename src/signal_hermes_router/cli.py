from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from contextlib import suppress
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any

from .config import load_app_config, load_router_config
from .models import TurnOutcomeStatus
from .payloads import (
    NotificationPayloadError,
    canonicalize_notification_payload,
    encode_control_message,
)
from .router import SignalHermesRouter

DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS = 300.0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="signal-hermes-router")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--routes", type=Path, default=Path("routes.yaml"))
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("serve", help="run the long-lived Signal router")
    trigger = subparsers.add_parser("trigger-job", help="trigger a configured scheduled job")
    trigger.add_argument("job_id")
    trigger.add_argument("--scheduled-at")
    trigger.add_argument("--idempotency-key")
    trigger.add_argument("--timeout", type=float, help="router route-lock timeout in seconds")
    trigger.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    trigger.add_argument("--control-socket", type=Path)
    notify = subparsers.add_parser("notify-route", help="send a configured route notification")
    notify.add_argument("notification_id")
    notify.add_argument("--payload-file", type=Path, required=True)
    notify.add_argument("--idempotency-key")
    notify.add_argument("--timeout", type=float, help="router route-lock timeout in seconds")
    notify.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    notify.add_argument("--control-socket", type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    exit_code = asyncio.run(_main_async(args))
    if isinstance(exit_code, int) and exit_code != 0:
        raise SystemExit(exit_code)


async def _main_async(args: argparse.Namespace) -> int:
    if args.command in (None, "serve"):
        await _run(args.config, args.routes)
        return 0
    if args.command == "trigger-job":
        return await _trigger_job(args)
    if args.command == "notify-route":
        return await _notify_route(args)
    raise ValueError(f"unknown command {args.command!r}")


async def _run(config_path: Path, routes_path: Path) -> None:
    config = load_app_config(config_path, routes_path)
    route_counts = Counter(route.state.value for route in config.routes)
    logging.info(
        "starting signal-hermes-router routes=%s remote_signal_base_url=%s",
        dict(route_counts),
        config.router.allow_remote_signal_base_url,
    )
    router = SignalHermesRouter(config)
    try:
        await router.run_forever()
    finally:
        await router.close()


async def _trigger_job(args: argparse.Namespace) -> int:
    try:
        socket_path = (
            args.control_socket or load_router_config(args.config).control_socket_path
        ).expanduser()
        scheduled_at = parse_scheduled_at(args.scheduled_at) if args.scheduled_at else None
        client_timeout = getattr(args, "client_timeout", DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS)
        if client_timeout is not None and client_timeout < 0:
            raise ValueError("--client-timeout must be non-negative")
        response = await trigger_job_via_control_socket(
            socket_path,
            args.job_id,
            scheduled_at=scheduled_at,
            idempotency_key=args.idempotency_key,
            timeout=args.timeout,
            client_timeout=client_timeout,
        )
    except Exception as exc:
        logging.error("trigger-job failed: %s", exc.__class__.__name__)
        logging.debug("trigger-job failure details", exc_info=True)
        return 1
    print(json.dumps(response, sort_keys=True))
    if response.get("status") in {
        TurnOutcomeStatus.DELIVERED.value,
        TurnOutcomeStatus.DEDUPED.value,
        TurnOutcomeStatus.BUSY.value,
        TurnOutcomeStatus.SKIPPED.value,
    }:
        return 0
    return 1


async def _notify_route(args: argparse.Namespace) -> int:
    try:
        if args.control_socket is not None:
            socket_path = args.control_socket.expanduser()
            max_payload_bytes = None
        else:
            router_config = load_router_config(args.config)
            socket_path = router_config.control_socket_path.expanduser()
            max_payload_bytes = router_config.control.max_notification_payload_bytes
        client_timeout = getattr(args, "client_timeout", DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS)
        if client_timeout is not None and client_timeout < 0:
            raise ValueError("--client-timeout must be non-negative")
        raw_payload = json.loads(args.payload_file.read_text(encoding="utf-8"))
        payload = canonicalize_notification_payload(
            raw_payload,
            max_bytes=max_payload_bytes,
        )
        response = await notify_route_via_control_socket(
            socket_path,
            args.notification_id,
            payload=payload.value,
            idempotency_key=args.idempotency_key,
            timeout=args.timeout,
            client_timeout=client_timeout,
        )
    except (json.JSONDecodeError, NotificationPayloadError, OSError, ValueError) as exc:
        logging.error("notify-route failed: %s", exc.__class__.__name__)
        logging.debug("notify-route failure details", exc_info=True)
        return 1
    except Exception as exc:
        logging.error("notify-route failed: %s", exc.__class__.__name__)
        logging.debug("notify-route failure details", exc_info=True)
        return 1
    print(json.dumps(response, sort_keys=True))
    if response.get("status") in {
        TurnOutcomeStatus.DELIVERED.value,
        TurnOutcomeStatus.DEDUPED.value,
        TurnOutcomeStatus.BUSY.value,
        TurnOutcomeStatus.SKIPPED.value,
    }:
        return 0
    return 1


async def trigger_job_via_control_socket(
    socket_path: Path,
    job_id: str,
    *,
    scheduled_at: int | None = None,
    idempotency_key: str | None = None,
    timeout: float | None = None,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {"command": "trigger_job", "job_id": job_id}
    if scheduled_at is not None:
        request["scheduled_at"] = scheduled_at
    if idempotency_key is not None:
        request["idempotency_key"] = idempotency_key
    if timeout is not None:
        request["timeout"] = timeout
    return await _control_round_trip(socket_path, request, client_timeout=client_timeout)


async def notify_route_via_control_socket(
    socket_path: Path,
    notification_id: str,
    *,
    payload: dict[str, Any] | list[Any],
    idempotency_key: str | None = None,
    timeout: float | None = None,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "command": "notify_route",
        "notification_id": notification_id,
        "payload": payload,
    }
    if idempotency_key is not None:
        request["idempotency_key"] = idempotency_key
    if timeout is not None:
        request["timeout"] = timeout
    return await _control_round_trip(socket_path, request, client_timeout=client_timeout)


async def _control_round_trip(
    socket_path: Path,
    request: dict[str, Any],
    *,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    async def round_trip() -> dict[str, Any]:
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
        try:
            writer.write(encode_control_message(request))
            await writer.drain()
            line = await reader.readline()
        finally:
            writer.close()
            with suppress(Exception, asyncio.CancelledError):
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
        if not line:
            raise RuntimeError("control socket closed without a response")
        response = json.loads(line.decode("utf-8"))
        if not isinstance(response, dict):
            raise RuntimeError("control socket returned a non-object response")
        return response

    if client_timeout is None:
        return await round_trip()
    return await asyncio.wait_for(round_trip(), timeout=client_timeout)


def parse_scheduled_at(value: str) -> int:
    if value.removeprefix("-").isdecimal():
        parsed = int(value)
        if parsed < 0:
            raise ValueError("--scheduled-at must be non-negative")
        return parsed
    iso_value = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed_dt = datetime.fromisoformat(iso_value)
    if parsed_dt.tzinfo is None or parsed_dt.utcoffset() is None:
        raise ValueError("--scheduled-at ISO timestamp must include a timezone")
    return int(parsed_dt.astimezone(timezone.utc).timestamp() * 1000)


if __name__ == "__main__":
    main()
