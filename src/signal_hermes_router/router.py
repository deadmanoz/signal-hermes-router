from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from pathlib import Path

from .circuit import CircuitBreaker
from .config import AppConfig, Route
from .context import build_prompt_blocks
from .dedupe import DedupeStore
from .events import SignalEventSummary, inspect_signal_event, parse_signal_event
from .media import write_attachment
from .models import (
    MediaManifest,
    NormalizedEvent,
    RouteState,
    SessionPolicy,
    SignalAttachment,
    TurnResult,
)
from .outbound import chunk_for_signal_bytes, prepare_outgoing_message
from .private_fs import validate_path_component
from .redaction import Redactor
from .sessions import ProfileSupervisor, SessionRegistry
from .signal import SignalHttpClient

LOGGER = logging.getLogger(__name__)

ROUTINE_NON_GROUP_MESSAGE_TYPES = frozenset({"typingMessage", "receiptMessage"})


class SignalHermesRouter:
    def __init__(
        self,
        config: AppConfig,
        *,
        signal_client: SignalHttpClient | None = None,
        supervisor: ProfileSupervisor | None = None,
        dedupe: DedupeStore | None = None,
    ) -> None:
        self.config = config
        self.signal = signal_client or SignalHttpClient(
            config.router.signal_base_url,
            max_event_bytes=config.router.max_signal_event_bytes,
        )
        self.supervisor = supervisor or ProfileSupervisor(
            config.router.work_root,
            max_acp_line_bytes=config.router.max_acp_line_bytes,
            prompt_timeout_seconds=config.router.acp_prompt_timeout_seconds,
        )
        self.sessions = SessionRegistry(config.router.work_root, self.supervisor)
        self.dedupe = dedupe or DedupeStore(config.router.state_db)
        self.circuit = CircuitBreaker(
            failures=config.router.circuit_breaker.failures,
            window_seconds=config.router.circuit_breaker.window_seconds,
        )
        self.recovery_seconds = config.router.circuit_breaker.recovery_seconds
        self.redactor = Redactor()
        for route in config.routes:
            self.redactor.add(route.key, route.group_id, route.profile, route.friendly_name)
        self.route_state_overrides: dict[str, RouteState] = {}
        self._trip_times: dict[str, float] = {}

    async def run_forever(self) -> None:
        async for raw in self.signal.events():
            try:
                await self.handle_raw_event(raw)
            except Exception as exc:
                LOGGER.error("event handler crashed; continuing: %s", exc.__class__.__name__)
                LOGGER.debug("event handler crash details", exc_info=True)

    async def close(self) -> None:
        await self.signal.close()
        await self.supervisor.close()
        self.dedupe.close()

    async def handle_raw_event(self, raw: dict) -> TurnResult | None:
        event = parse_signal_event(
            raw,
            max_attachment_bytes=self.config.router.max_attachment_bytes,
        )
        if event is None:
            summary = inspect_signal_event(raw)
            LOGGER.log(
                _non_group_event_log_level(summary),
                "ignoring non-group Signal event %s",
                summary,
            )
            return None
        return await self.handle_event(event)

    async def handle_event(self, event: NormalizedEvent) -> TurnResult | None:
        self.redactor.add(event.group_id, event.sender_id, event.source_uuid)
        route = self.config.find_route(event.platform, event.group_id)
        if route is None:
            LOGGER.info("no route for %s", self.redactor.ref("group", event.group_id))
            return None

        if not self.dedupe.claim(route.key, event.source_uuid, event.timestamp):
            event_ref = f"{route.key}:{event.source_uuid}:{event.timestamp}"
            LOGGER.info("deduped Signal event %s", self.redactor.ref("event", event_ref))
            return None

        handled = False
        try:
            self._maybe_clear_breaker_override(route)
            state = self.route_state_overrides.get(route.key, route.state)
            LOGGER.info("route %s in state %s", self.redactor.ref("route", route.key), state)
            if state == RouteState.DISABLED:
                handled = True
                return None
            if state == RouteState.MAINTENANCE:
                await self._send_once(
                    route, route.maintenance_reply or self.config.router.maintenance_reply
                )
                handled = True
                return None

            manifests = self._store_media(event)
            if state == RouteState.SHADOW:
                handled = True
                return None

            blocks = build_prompt_blocks(
                route_context=route.route_context,
                user_text=event.text,
                manifests=manifests,
            )
            session = await self.sessions.get(route, event)
            await self._typing(route, True)
            turn_done = asyncio.Event()
            notice_task = asyncio.create_task(self._long_running_notice(route, turn_done))
            try:
                result = await session.profile.prompt(session.session_id, blocks)
                # Stop the busy-notice task immediately so it cannot fire
                # while a long chunked reply is still being sent.
                await self._stop_busy_notice(turn_done, notice_task)
                self.circuit.record_success(route.key)
                if result.text:
                    await self._send_once(route, result.text)
                handled = True
                return result
            except Exception as exc:
                # Stop the busy-notice task BEFORE the failure reply / recovery,
                # otherwise a slow restart_profile + replace_after_restart can
                # let the notice fire after the user already received the
                # failure_reply.
                await self._stop_busy_notice(turn_done, notice_task)
                LOGGER.error(
                    "Hermes turn failed for %s: %s",
                    self.redactor.ref("route", route.key),
                    self.redactor.redact(exc.__class__.__name__),
                )
                LOGGER.debug("Hermes turn failure details", exc_info=True)
                # Record the breaker hit and pick the user-facing reply
                # BEFORE attempting subprocess recovery. If restart or
                # replace_after_restart fails (e.g. binary missing, profile
                # broken, cooldown), the user still gets a reply and the
                # breaker still moves — recovery is best-effort for the
                # next event.
                trip = self.circuit.record_failure(route.key)
                if trip:
                    self.route_state_overrides[route.key] = RouteState.MAINTENANCE
                    self._trip_times[route.key] = time.monotonic()
                    LOGGER.error(
                        "route %s tripped circuit breaker after %s failures",
                        self.redactor.ref("route", route.key),
                        trip.failures,
                    )
                    reply_text = route.maintenance_reply or self.config.router.maintenance_reply
                else:
                    reply_text = route.failure_reply or self.config.router.failure_reply
                await self._send_once(route, reply_text)
                try:
                    await self.supervisor.restart_profile(route.profile)
                    if route.session_policy != SessionPolicy.EPHEMERAL:
                        await self.sessions.replace_after_restart(route, event, session)
                except Exception as recovery_exc:
                    LOGGER.warning(
                        "Hermes recovery failed for %s: %s; route will retry on next event",
                        self.redactor.ref("route", route.key),
                        self.redactor.redact(recovery_exc.__class__.__name__),
                    )
                    LOGGER.debug("Hermes recovery failure details", exc_info=True)
                handled = True
                return None
            finally:
                # Defensive backup for cancellation paths that skip the
                # try/except branches. Idempotent — no-op if already stopped.
                await self._stop_busy_notice(turn_done, notice_task)
                await self._typing(route, False)
                if session.ephemeral:
                    session.profile.release_session(session.session_id)
        finally:
            if handled:
                self.dedupe.mark_handled(route.key, event.source_uuid, event.timestamp)
            else:
                self.dedupe.release(route.key, event.source_uuid, event.timestamp)

    def _store_media(self, event: NormalizedEvent) -> list[MediaManifest]:
        manifests: list[MediaManifest] = []
        group_ref = self.redactor.ref("group", event.group_id)
        sender_ref = self.redactor.ref("sender", event.sender_id)
        for attachment in event.attachments:
            manifests.append(
                write_attachment(
                    media_root=Path(self.config.router.media_root),
                    platform=event.platform,
                    timestamp=event.timestamp,
                    attachment=self._resolve_signal_attachment(attachment),
                    group_ref=group_ref,
                    sender_ref=sender_ref,
                    max_bytes=self.config.router.max_attachment_bytes,
                )
            )
        return manifests

    def _resolve_signal_attachment(self, attachment: SignalAttachment) -> SignalAttachment:
        if attachment.body is not None or attachment.path is not None or not attachment.signal_id:
            return attachment
        signal_id = validate_path_component(
            str(attachment.signal_id),
            error_message="invalid Signal attachment id",
        )
        return SignalAttachment(
            content_type=attachment.content_type,
            filename=attachment.filename,
            size=attachment.size,
            path=self.config.router.signal_attachment_root / signal_id,
            signal_id=signal_id,
        )

    async def _send_once(self, route: Route, message: str) -> None:
        message = prepare_outgoing_message(
            route,
            message,
            max_reply_chars=self.config.router.max_reply_chars,
        )
        chunks = chunk_for_signal_bytes(
            message,
            max_bytes=self.config.router.max_signal_message_bytes,
        )
        total = len(chunks)
        if total > 1:
            LOGGER.info(
                "split reply for %s into %d chunks",
                self.redactor.ref("route", route.key),
                total,
            )
        index = 0
        try:
            for index, chunk in enumerate(chunks, 1):
                await self.signal.send_group(route.group_id, chunk)
        except Exception as exc:
            LOGGER.error(
                "failed Signal reply chunk %d/%d for %s: %s",
                index,
                total,
                self.redactor.ref("route", route.key),
                self.redactor.redact(exc.__class__.__name__),
            )
            LOGGER.debug("Signal reply failure details", exc_info=True)

    async def _typing(self, route: Route, enabled: bool) -> None:
        send_typing = getattr(self.signal, "send_typing", None)
        if send_typing is None:
            return
        try:
            await send_typing(route.group_id, enabled)
        except Exception:
            LOGGER.debug("Signal typing indicator failed", exc_info=True)

    def _maybe_clear_breaker_override(self, route: Route) -> None:
        if self.route_state_overrides.get(route.key) is not RouteState.MAINTENANCE:
            return
        trip_time = self._trip_times.get(route.key)
        if trip_time is None:
            return
        if time.monotonic() - trip_time < self.recovery_seconds:
            return
        self.route_state_overrides.pop(route.key, None)
        self._trip_times.pop(route.key, None)
        self.circuit.record_success(route.key)
        LOGGER.info(
            "route %s circuit breaker cooldown elapsed; probing route in configured state",
            self.redactor.ref("route", route.key),
        )

    async def _long_running_notice(self, route: Route, turn_done: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(
                turn_done.wait(), timeout=self.config.router.busy_notice_after_seconds
            )
            return
        except TimeoutError:
            pass
        if turn_done.is_set():
            return
        await self._send_once(route, self.config.router.busy_notice)

    @staticmethod
    async def _stop_busy_notice(turn_done: asyncio.Event, notice_task: asyncio.Task[None]) -> None:
        turn_done.set()
        notice_task.cancel()
        with suppress(asyncio.CancelledError):
            await notice_task


def _non_group_event_log_level(summary: SignalEventSummary) -> int:
    if not summary.has_group and summary.message_type in ROUTINE_NON_GROUP_MESSAGE_TYPES:
        return logging.DEBUG
    return logging.INFO
