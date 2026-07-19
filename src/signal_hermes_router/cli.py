from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime, timezone
import json
import logging
import os
import signal
from pathlib import Path
from typing import Any

from .config import load_app_config, load_control_discovery
from .models import TurnOutcomeStatus
from .payloads import (
    canonicalize_notification_payload,
    encode_control_message,
)
from .preflight import (
    ProbeCallable,
    PreflightScope,
    format_preflight_report,
    format_preflight_report_dict,
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
    preflight.add_argument(
        "--probe-contract-file",
        type=Path,
        help="version 1 full_callable ACP tool-surface contract",
    )
    preflight.add_argument("--json", action="store_true")
    preflight.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    preflight.add_argument("--control-socket", type=Path)
    status = subparsers.add_parser("route-status", help="inspect route health and recovery state")
    status.add_argument("--route", action="append", default=[])
    status.add_argument("--route-index", action="append", type=int, default=[])
    status.add_argument("--profile", action="append", default=[])
    status.add_argument("--json", action="store_true")
    status.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    status.add_argument("--control-socket", type=Path)
    reload_cfg = subparsers.add_parser(
        "reload-config", help="atomically reload router routes configuration from disk"
    )
    reload_cfg.add_argument(
        "--candidate-routes",
        type=Path,
        help="override routes.yaml path (defaults to router startup path)",
    )
    reload_cfg.add_argument(
        "--client-timeout",
        type=float,
        default=DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
        help="local control socket round-trip timeout in seconds",
    )
    reload_cfg.add_argument("--control-socket", type=Path)
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
    if args.command == "route-status":
        return await _route_status(args)
    if args.command == "reload-config":
        return await _reload_config(args)
    raise ValueError(f"unknown command {args.command!r}")


def _force_immediate_exit(loop: asyncio.AbstractEventLoop) -> None:
    # Restore the default disposition and re-deliver SIGTERM so the process
    # dies immediately and reports killed-by-SIGTERM to systemd.
    loop.remove_signal_handler(signal.SIGTERM)
    signal.raise_signal(signal.SIGTERM)


def _hard_exit_after_incomplete_shutdown() -> None:
    # asyncio.run's Runner ends by gathering every remaining task without a
    # timeout; a shutdown-abandoned task could wedge that gather past
    # systemd's stop timeout. Exiting hard keeps the stop bounded, and status
    # 0 keeps `systemctl stop` clean.
    os._exit(0)


async def _run(config_path: Path, routes_path: Path) -> None:
    config = load_app_config(config_path, routes_path)
    route_counts = Counter(route.state.value for route in config.routes)
    logging.info(
        "starting signal-hermes-router routes=%s remote_signal_base_url=%s",
        dict(route_counts),
        config.router.allow_remote_signal_base_url,
    )
    router = SignalHermesRouter(config)
    router.set_config_paths(config_path, routes_path)
    loop = asyncio.get_running_loop()
    serve_task = asyncio.current_task()
    assert serve_task is not None
    sigterm_requested = False

    def _on_sigterm() -> None:
        nonlocal sigterm_requested
        if sigterm_requested:
            logging.warning("second SIGTERM received; forcing immediate exit")
            _force_immediate_exit(loop)
            return
        sigterm_requested = True
        logging.info("SIGTERM received; shutting down gracefully")
        router.begin_shutdown()
        serve_task.cancel()

    loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    incomplete: tuple[asyncio.Task[Any], ...] = ()
    try:
        try:
            await router.run_forever()
        except asyncio.CancelledError:
            # Only the SIGTERM-marked cancellation is ours to absorb; Runner's
            # SIGINT cancel (and any external cancel) re-raises unchanged
            # after close() runs in the finally below.
            if not sigterm_requested:
                raise
            serve_task.uncancel()
        finally:
            incomplete = await router.close()
    finally:
        # Removed only after close() completes so a second SIGTERM stays
        # available as the escape hatch while the drain runs.
        loop.remove_signal_handler(signal.SIGTERM)
    if sigterm_requested and incomplete:
        logging.error(
            "shutdown cleanup incomplete (%d task(s)); forcing process exit",
            len(incomplete),
        )
        _hard_exit_after_incomplete_shutdown()


_CONTROL_SUCCESS_STATUSES = frozenset(
    {
        TurnOutcomeStatus.DELIVERED.value,
        TurnOutcomeStatus.DEDUPED.value,
        TurnOutcomeStatus.BUSY.value,
        TurnOutcomeStatus.SKIPPED.value,
    }
)


def _exit_code_for_status(response: dict[str, Any]) -> int:
    return 0 if response.get("status") in _CONTROL_SUCCESS_STATUSES else 1


def _resolve_client_timeout(args: argparse.Namespace) -> float | None:
    client_timeout = getattr(args, "client_timeout", DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS)
    if client_timeout is not None and client_timeout < 0:
        raise ValueError("--client-timeout must be non-negative")
    return client_timeout


async def _trigger_job(args: argparse.Namespace) -> int:
    try:
        socket_path = (args.control_socket or load_control_discovery(args.config)[0]).expanduser()
        scheduled_at = parse_scheduled_at(args.scheduled_at) if args.scheduled_at else None
        client_timeout = _resolve_client_timeout(args)
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
    return _exit_code_for_status(response)


async def _notify_route(args: argparse.Namespace) -> int:
    try:
        if args.control_socket is not None:
            socket_path = args.control_socket.expanduser()
            max_payload_bytes = None
        else:
            discovered_socket, max_payload_bytes = load_control_discovery(
                args.config, resolve_payload_cap=True
            )
            socket_path = discovered_socket.expanduser()
        client_timeout = _resolve_client_timeout(args)
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
    except Exception as exc:
        # Any failure here (payload parse, canonicalization, control-socket I/O)
        # is logged and mapped to exit 1, matching the other control commands.
        logging.error("notify-route failed: %s", exc.__class__.__name__)
        logging.debug("notify-route failure details", exc_info=True)
        return 1
    print(json.dumps(response, sort_keys=True))
    return _exit_code_for_status(response)


async def _preflight_permissions(args: argparse.Namespace) -> int:
    try:
        scope = _preflight_scope_from_args(args)
        client_timeout = _resolve_client_timeout(args)
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
                print(format_preflight_report_dict(response))
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


async def _route_status(args: argparse.Namespace) -> int:
    try:
        socket_path = (args.control_socket or load_control_discovery(args.config)[0]).expanduser()
        client_timeout = _resolve_client_timeout(args)
        response = await route_status_via_control_socket(
            socket_path,
            route_names=tuple(args.route),
            route_indexes=tuple(args.route_index),
            profiles=tuple(args.profile),
            client_timeout=client_timeout,
        )
    except Exception as exc:
        logging.error("route-status failed: %s", exc.__class__.__name__)
        logging.debug("route-status failure details", exc_info=True)
        return 1
    if args.json:
        print(json.dumps(response, sort_keys=True))
    else:
        print(_format_route_status_response(response))
    return 0 if response.get("status") == "ok" else 1


async def _reload_config(args: argparse.Namespace) -> int:
    try:
        socket_path = (args.control_socket or load_control_discovery(args.config)[0]).expanduser()
        client_timeout = _resolve_client_timeout(args)
        response = await reload_config_via_control_socket(
            socket_path,
            candidate_routes=args.candidate_routes,
            client_timeout=client_timeout,
        )
    except Exception as exc:
        logging.error("reload-config failed: %s", exc.__class__.__name__)
        logging.debug("reload-config failure details", exc_info=True)
        return 1
    print(json.dumps(response, sort_keys=True))
    return 0 if response.get("status") == "ok" else 1


def _preflight_scope_from_args(args: argparse.Namespace) -> PreflightScope:
    return parse_preflight_scope(
        {
            "active_only": args.active_only,
            "route_names": args.route,
            "route_indexes": args.route_index,
            "profiles": args.profile,
        }
    )


def _format_route_status_response(response: dict[str, Any]) -> str:
    if response.get("status") != "ok":
        return f"Route status: {response.get('status', 'unknown')}"
    lines = [
        f"Route status: {response.get('route_count', len(response.get('routes') or []))} route(s)"
    ]
    for route in response.get("routes") or []:
        if not isinstance(route, dict):
            continue
        session = route.get("session") or {}
        circuit = route.get("circuit") or {}
        last_failure = route.get("last_failure") or {}
        suffix = ""
        if last_failure:
            suffix = f" last_failure={last_failure.get('code', 'unknown')}"
        lines.append(
            f"- {route.get('route_ref')} state={route.get('route_state')} "
            f"profile={route.get('profile')} session={session.get('policy')} "
            f"cached={session.get('cached_sessions', 0)} "
            f"circuit={circuit.get('state')} failures={circuit.get('failure_count', 0)}"
            f"{suffix}"
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


async def route_status_via_control_socket(
    socket_path: Path,
    *,
    route_names: Sequence[str] = (),
    route_indexes: Sequence[int] = (),
    profiles: Sequence[str] = (),
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {"command": "route_status"}
    if route_names:
        request["routes"] = list(route_names)
    if route_indexes:
        request["route_indexes"] = list(route_indexes)
    if profiles:
        request["profiles"] = list(profiles)
    return await _control_round_trip(socket_path, request, client_timeout=client_timeout)


async def reload_config_via_control_socket(
    socket_path: Path,
    *,
    candidate_routes: Path | None = None,
    client_timeout: float | None = DEFAULT_CONTROL_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, Any] = {"command": "reload_config"}
    if candidate_routes is not None:
        # Expand ~ and resolve to an absolute path in the operator's shell:
        # Path.resolve() alone does not expand the home directory, and the
        # router rejects relative overrides because they would resolve
        # against the long-lived daemon's cwd, not the directory the
        # operator ran from.
        request["candidate_routes"] = str(candidate_routes.expanduser().resolve())
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
