from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Sequence
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
from .preflight import (
    ProbeCallable,
    PreflightScope,
    format_preflight_report,
    load_probe_contract,
    parse_preflight_scope,
    run_permission_preflight,
    unavailable_tool_surface_probe,
)
from .router import SignalHermesRouter

DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS = 300.0
DEFAULT_CONTROL_RESPONSE_LIMIT_BYTES = 8 * 1024 * 1024


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
    notify.add_argument("--attachment", type=Path, action="append", default=[])
    notify.add_argument("--idempotency-key")
    notify.add_argument("--timeout", type=float, help="router route-lock timeout in seconds")
    notify.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    notify.add_argument("--control-socket", type=Path)
    preflight = subparsers.add_parser(
        "preflight-permissions",
        help="compare route permission allowlists with ACP tool surfaces",
    )
    preflight.add_argument("--active-only", action="store_true")
    preflight.add_argument("--route", action="append", default=[])
    preflight.add_argument("--route-index", action="append", type=int, default=[])
    preflight.add_argument("--profile", action="append", default=[])
    preflight.add_argument("--probe-contract-file", type=Path)
    preflight.add_argument("--json", action="store_true")
    preflight.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    preflight.add_argument("--control-socket", type=Path)
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
    if args.command == "preflight-permissions":
        return await _preflight_permissions(args)
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
        attachments = getattr(args, "attachment", []) or []
        if len(attachments) > 1:
            raise ValueError("--attachment may be supplied at most once")
        attachment_kwargs = {"attachments": attachments} if attachments else {}
        response = await notify_route_via_control_socket(
            socket_path,
            args.notification_id,
            payload=payload.value,
            idempotency_key=args.idempotency_key,
            timeout=args.timeout,
            client_timeout=client_timeout,
            **attachment_kwargs,
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


async def _preflight_permissions(args: argparse.Namespace) -> int:
    try:
        scope = _preflight_scope_from_args(args)
        client_timeout = getattr(args, "client_timeout", DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS)
        if client_timeout is not None and client_timeout < 0:
            raise ValueError("--client-timeout must be non-negative")
        if args.control_socket is not None:
            if args.probe_contract_file is not None:
                logging.warning(
                    "--probe-contract-file is ignored when --control-socket is supplied"
                )
            response = await preflight_permissions_via_control_socket(
                args.control_socket.expanduser(),
                scope=scope,
                client_timeout=client_timeout,
            )
            if args.json:
                print(json.dumps(response, sort_keys=True))
            else:
                print(_format_preflight_response(response))
            return 0 if response.get("status") == "ok" else 1
        config = load_app_config(args.config, args.routes)
        probe = (
            _load_probe_contract_for_cli(args.probe_contract_file)
            if args.probe_contract_file is not None
            else unavailable_tool_surface_probe
        )
        report = await run_permission_preflight(config, probe, scope=scope)
    except Exception as exc:
        logging.error("preflight-permissions failed: %s", str(exc) or exc.__class__.__name__)
        logging.debug("preflight-permissions failure details", exc_info=True)
        return 1
    if args.json:
        print(json.dumps(report.to_dict(), sort_keys=True))
    else:
        print(format_preflight_report(report))
    return 0 if report.ok else 1


def _preflight_scope_from_args(args: argparse.Namespace) -> PreflightScope:
    return parse_preflight_scope(
        {
            "active_only": args.active_only,
            "route_names": args.route,
            "route_indexes": args.route_index,
            "profiles": args.profile,
        }
    )


def _format_preflight_response(response: dict[str, Any]) -> str:
    lines = [
        f"Permission preflight: {response.get('status', 'unknown')}",
        f"Profiles targeted: {len(response.get('checked_profiles') or [])}",
        "Configured permission tool entries: "
        f"{response.get('expected_permissions_count', len(response.get('expected_permissions') or []))}",
        f"Missing tool entries: {response.get('missing_tools_count', len(response.get('missing_tools') or []))}",
    ]
    probe_errors = response.get("probe_errors") or []
    if probe_errors:
        lines.append("Probe errors:")
        for error in probe_errors:
            if not isinstance(error, dict):
                continue
            lines.append(f"- {error.get('profile')}: {error.get('error') or error.get('code')}")
    scope_errors = response.get("scope_errors") or []
    if scope_errors:
        lines.append("Scope errors:")
        for error in scope_errors:
            if not isinstance(error, dict):
                continue
            lines.append(f"- {error.get('error') or error.get('code')}")
    missing_tools = response.get("missing_tools") or []
    if missing_tools:
        lines.append("Missing tools:")
        for tool in missing_tools:
            if not isinstance(tool, dict):
                continue
            source = tool.get("source_kind")
            if tool.get("source_id") is not None:
                source = f"{source}:{tool['source_id']}"
            lines.append(
                f"- {tool.get('route_ref')} {tool.get('profile')} {source} {tool.get('tool')}"
            )
    return "\n".join(lines)


def _load_probe_contract_for_cli(path: Path) -> ProbeCallable:
    try:
        return load_probe_contract(path)
    except FileNotFoundError as exc:
        raise ValueError(f"probe contract file not found: {path}") from exc
    except OSError as exc:
        detail = exc.strerror or str(exc) or exc.__class__.__name__
        raise ValueError(f"probe contract file could not be read: {path}: {detail}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"probe contract file is invalid JSON: {path}: line {exc.lineno} column {exc.colno}"
        ) from exc
    except ValueError as exc:
        raise ValueError(f"probe contract file is invalid: {path}: {exc}") from exc


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
    attachments: Sequence[str | Path] = (),
    idempotency_key: str | None = None,
    timeout: float | None = None,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "command": "notify_route",
        "notification_id": notification_id,
        "payload": payload,
    }
    if attachments:
        request["attachments"] = [str(path) for path in attachments]
    if idempotency_key is not None:
        request["idempotency_key"] = idempotency_key
    if timeout is not None:
        request["timeout"] = timeout
    return await _control_round_trip(socket_path, request, client_timeout=client_timeout)


async def preflight_permissions_via_control_socket(
    socket_path: Path,
    *,
    scope: PreflightScope,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "command": "preflight_permissions",
        "scope": scope.to_dict(),
    }
    return await _control_round_trip(socket_path, request, client_timeout=client_timeout)


async def _control_round_trip(
    socket_path: Path,
    request: dict[str, Any],
    *,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
    response_limit_bytes: int = DEFAULT_CONTROL_RESPONSE_LIMIT_BYTES,
) -> dict[str, Any]:
    async def round_trip() -> dict[str, Any]:
        reader, writer = await asyncio.open_unix_connection(
            str(socket_path),
            limit=response_limit_bytes,
        )
        try:
            writer.write(encode_control_message(request))
            await writer.drain()
            try:
                line = await reader.readline()
            except ValueError as exc:
                raise RuntimeError(
                    "control socket response exceeded "
                    f"{response_limit_bytes} byte client read limit"
                ) from exc
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
