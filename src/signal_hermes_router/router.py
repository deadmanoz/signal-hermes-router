from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import socket
import stat
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, TypeVar

from .circuit import CircuitBreaker
from .config import AppConfig, Route, SyntheticRouteDefinition
from .context import build_prompt_blocks, build_synthetic_prompt_blocks
from .dedupe import DedupeStore
from .events import SignalEventSummary, parse_signal_event, probe_signal_route
from .failures import (
    FailureCode,
    FailureInfo,
    classify_exception,
    failure_info,
    is_model_provider_failure,
    preflight_failure_from_report,
)
from .media import (
    MediaSweepPlan,
    MediaSweepResult,
    execute_media_sweep_groups,
    plan_media_sweep,
    remove_empty_sweep_dirs,
    write_attachment,
)
from .models import (
    ChatType,
    CircuitStatus,
    MediaManifest,
    NormalizedEvent,
    OutboundAttachment,
    RouteHealth,
    RouteState,
    SessionKeyInput,
    SessionPolicy,
    SignalAttachment,
    SyntheticTurnKind,
    TurnOrigin,
    TurnOutcome,
    TurnOutcomeStatus,
    TurnResult,
)
from .outbound import chunk_for_signal_bytes, prepare_outgoing_message
from .outbound_media import (
    OutboundAttachmentError,
    signal_base_url_supports_local_attachment_paths,
    validate_outbound_attachments,
)
from .payloads import (
    CanonicalNotificationPayload,
    NotificationPayloadError,
    canonicalize_notification_payload,
    encode_control_message,
)
from .permissions import StaticPermissionPolicy
from .preflight import (
    PreflightProbeUnavailable,
    ToolSurface,
    parse_preflight_scope,
    run_permission_preflight,
)
from .private_fs import (
    ensure_private_dir,
    ensure_private_dir_tree,
    validate_path_component,
    write_private_bytes,
)
from .ratelimit import TokenBucket
from .redaction import Redactor
from .sessions import ProfileSupervisor, RoutedSession, SessionRegistry
from .signal import SignalHttpClient

LOGGER = logging.getLogger(__name__)

SYNTHETIC_DEDUPE_TIMESTAMP_SENTINEL = 0
PREFLIGHT_PROFILE_LOCK_TIMEOUT_SECONDS = 0.0
ATTACHMENT_ONLY_FALLBACK_TEXT = "Image attached."
# Shutdown budget: graceful drain of in-flight work, then bounded settlement of
# cancelled stragglers, then supervisor close (never below its floor so the ACP
# peer's terminate grace is not cut short). Worst case is roughly
# drain + settle + supervisor floor, well inside systemd's default 90s stop
# timeout. These are code constants by design, not configuration.
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 15.0
SHUTDOWN_SETTLE_TIMEOUT_SECONDS = 5.0
SHUTDOWN_SUPERVISOR_FLOOR_SECONDS = 10.0
SHUTDOWN_CLEANUP_CANCEL_GRACE_SECONDS = 1.0
# Retention sweep deletions run on the event loop (so the pre-unlink mtime
# recheck cannot interleave with media writes) in bounded batches with a
# yield in between; this bounds each uninterrupted loop slice. A code
# constant by design, not configuration.
RETENTION_EXECUTE_BATCH_ITEMS = 200
_MISSING = object()
_T = TypeVar("_T")


@dataclass(frozen=True)
class RoutedTurnInput:
    route: Route
    origin: TurnOrigin
    dedupe_sender_id: str
    dedupe_timestamp: int
    session: SessionKeyInput
    secondary_dedupe: tuple[str, int] | None = None
    signal_event: NormalizedEvent | None = None
    synthetic: SyntheticRouteDefinition | None = None
    synthetic_prompt: str | None = None
    synthetic_payload: CanonicalNotificationPayload | None = None
    outbound_attachments: Any = ()
    scheduled_at_ms: int | None = None
    triggered_at_ms: int | None = None
    permission_policy: StaticPermissionPolicy | None = None


class SignalHermesRouter:
    def __init__(
        self,
        config: AppConfig,
        *,
        signal_client: SignalHttpClient | None = None,
        supervisor: ProfileSupervisor | None = None,
        dedupe: DedupeStore | None = None,
        clock_ms: Callable[[], int] | None = None,
        nonce_factory: Callable[[], str] | None = None,
    ) -> None:
        self.config = config
        self.redactor = Redactor()
        for route in config.routes:
            self.redactor.add(
                route.key,
                route.group_id,
                route.sender_id,
                route.sender_number,
                route.profile,
                route.friendly_name,
            )
        self.signal = signal_client or SignalHttpClient(
            config.router.signal_base_url,
            max_event_bytes=config.router.max_signal_event_bytes,
        )
        self.supervisor = supervisor or ProfileSupervisor(
            config.router.work_root,
            max_acp_line_bytes=config.router.max_acp_line_bytes,
            prompt_timeout_seconds=config.router.acp_prompt_timeout_seconds,
            initialize_timeout_seconds=config.router.acp_initialize_timeout_seconds,
        )
        # Every router-owned real supervisor (default-constructed or injected)
        # gets redaction-safe exit logs; feature detection keeps bare test
        # doubles working.
        set_redactor = getattr(self.supervisor, "set_redactor", None)
        if set_redactor is not None:
            set_redactor(self.redactor.redact)
        self.sessions = SessionRegistry(config.router.work_root, self.supervisor)
        self.dedupe = dedupe or DedupeStore(config.router.state_db)
        self.circuit = CircuitBreaker(
            failures=config.router.circuit_breaker.failures,
            window_seconds=config.router.circuit_breaker.window_seconds,
        )
        self.recovery_seconds = config.router.circuit_breaker.recovery_seconds
        self.route_state_overrides: dict[str, RouteState] = {}
        self._trip_times: dict[str, float] = {}
        self._trip_times_ms: dict[str, int] = {}
        self._last_breaker_reset_ms: dict[str, int] = {}
        self._last_success_ms: dict[str, int] = {}
        self._last_failures: dict[str, tuple[int, FailureInfo]] = {}
        self._route_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._profile_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._inbound_rate_buckets: dict[str, TokenBucket] = {}
        self._last_busy_notice_ms: dict[str, int] = {}
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._nonce_factory = nonce_factory or (lambda: uuid.uuid4().hex)
        self._control_server: asyncio.Server | None = None
        self._control_socket_path: Path | None = None
        self._closing = False
        self._shutdown_event = asyncio.Event()
        self._signal_turn_tasks: set[asyncio.Task[Any]] = set()
        self._control_client_tasks: set[asyncio.Task[None]] = set()
        self._signal_events_task: asyncio.Task[None] | None = None
        self._control_server_task: asyncio.Task[None] | None = None
        self._retention_task: asyncio.Task[None] | None = None
        # Un-cancelled handle for the current retention worker; cancelling
        # the retention task never cancels this (the awaits are shielded),
        # so close() can observe real worker completion.
        self._retention_inflight: asyncio.Future[Any] | None = None
        # Reference counts of media paths in use by in-flight turns (stored
        # inbound manifests and frozen outbound artifacts). Event-loop-only
        # mutation; the sweep's execute phase consults it at deletion time.
        self._live_media: Counter[Path] = Counter()

    async def run_forever(self) -> None:
        if self._closing:
            raise RuntimeError("router is shutting down")
        signal_events_task = asyncio.create_task(self._run_signal_events())
        self._signal_events_task = signal_events_task
        tasks = {signal_events_task}
        if self.config.router.control.enabled:
            control_server_task = asyncio.create_task(self._run_control_server())
            self._control_server_task = control_server_task
            tasks.add(control_server_task)
        if self.config.router.retention.enabled:
            retention_task = asyncio.create_task(self._run_retention_sweeps())
            self._retention_task = retention_task
            tasks.add(retention_task)
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled() and self._closing:
                    # begin_shutdown() cancelled the consumer under a direct
                    # close(); treat it as a clean shutdown, not a failure.
                    continue
                task.result()
        finally:
            # Request shutdown of the sibling lifecycle task(s) but do not
            # gather them here: close() is the sole bounded settlement owner,
            # so a resistant child can never wedge run_forever() teardown.
            for task in tasks:
                if not task.done():
                    task.cancel()

    async def _run_signal_events(self) -> None:
        async for raw in self.signal.events():
            # Each accepted event runs as a tracked task awaited through
            # shield: cancelling this consumer (SIGTERM/SIGINT teardown or
            # begin_shutdown) does not abort the accepted turn, which close()
            # then drains to normal delivery.
            task = asyncio.create_task(self.handle_raw_event(raw))
            self._signal_turn_tasks.add(task)
            task.add_done_callback(self._settle_tracked_task)
            try:
                await asyncio.shield(task)
            except Exception as exc:
                LOGGER.error("event handler crashed; continuing: %s", exc.__class__.__name__)
                LOGGER.debug("event handler crash details", exc_info=True)

    def begin_shutdown(self) -> None:
        """Synchronously fence new work: gate control lines, stop the control
        listener, and cancel the Signal consumer. Idempotent; safe to call
        from a signal handler callback before any teardown starts."""
        self._closing = True
        self._shutdown_event.set()
        events_task = self._signal_events_task
        if events_task is not None and not events_task.done():
            events_task.cancel()
        retention_task = self._retention_task
        if retention_task is not None and not retention_task.done():
            # Stops future sweep dispatches; an already-running worker is
            # observed (not cancelled) by close() via _retention_inflight.
            retention_task.cancel()
        self._close_control_listener()

    async def close(
        self,
        *,
        drain_timeout: float = SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
    ) -> tuple[asyncio.Task[Any], ...]:
        """Bounded, ordered shutdown. Returns the tasks whose cleanup did not
        complete (empty on a clean shutdown); a still-pending entry means the
        caller must not rely on the event loop reaching idle."""
        drain_deadline = time.monotonic() + max(drain_timeout, 0.0)
        settle_deadline = drain_deadline + SHUTDOWN_SETTLE_TIMEOUT_SECONDS
        self.begin_shutdown()
        incomplete: list[asyncio.Task[Any]] = []
        try:
            stragglers = await self._drain_tasks(
                set(self._signal_turn_tasks), drain_deadline, "Signal turn"
            )
            # Every turn holds its route lock through dedupe finalization, so
            # acquiring each known lock waits out in-flight turns before
            # close() releases the store's exclusive state-DB lock — while the
            # Signal client and supervisor are still open, so drained turns
            # finish via the normal delivery path.
            await self._drain_route_locks(drain_deadline)
            stragglers |= await self._drain_tasks(
                set(self._control_client_tasks), drain_deadline, "control client"
            )
            for task in (
                self._signal_events_task,
                self._control_server_task,
                self._retention_task,
            ):
                if task is not None and not task.done():
                    task.cancel()
                    stragglers.add(task)
            incomplete += await self._settle_cancelled(stragglers, settle_deadline)
            retention_worker = self._retention_inflight
            if retention_worker is not None and not retention_worker.done():
                # Task settlement is not worker completion: a cancelled
                # retention task leaves its executor thread running. Observe
                # the real worker handle (shielded, so the deadline path
                # records it incomplete instead of cancelling it) before the
                # dedupe store is finalized.
                incomplete += await self._observe_cleanup(
                    self._wait_retention_worker(retention_worker),
                    settle_deadline,
                    "retention sweep worker",
                )
            incomplete += await self._observe_cleanup(
                self._wait_control_server_closed(),
                settle_deadline,
                "control server close",
            )
            incomplete += await self._observe_cleanup(
                self.signal.close(), settle_deadline, "Signal client close"
            )
            supervisor_deadline = max(
                settle_deadline,
                time.monotonic() + SHUTDOWN_SUPERVISOR_FLOOR_SECONDS,
            )
            incomplete += await self._observe_cleanup(
                self.supervisor.close(), supervisor_deadline, "supervisor close"
            )
        finally:
            # Always release the exclusive state-DB lock, even if a cleanup
            # step raised unexpectedly; abandoned tasks cannot write to a
            # closed store, and the replacement router's startup reclaim
            # recovers any orphaned processing claim. The close never blocks
            # the event loop on a sweep statement: a mid-chunk retention
            # worker receives the deferred finalizer instead.
            if not self.dedupe.close():
                LOGGER.error(
                    "dedupe store close deferred to an in-flight retention worker; "
                    "cleanup is incomplete"
                )
        return tuple(incomplete)

    async def _drain_tasks(
        self,
        tasks: set[asyncio.Task[Any]],
        deadline: float,
        kind: str,
    ) -> set[asyncio.Task[Any]]:
        pending = {task for task in tasks if not task.done()}
        if not pending:
            return set()
        timeout = max(0.0, deadline - time.monotonic())
        _done, pending = await asyncio.wait(pending, timeout=timeout)
        if pending:
            LOGGER.warning(
                "cancelling %d %s task(s) still running at the shutdown drain deadline",
                len(pending),
                kind,
            )
            for task in pending:
                task.cancel()
        return set(pending)

    async def _settle_cancelled(
        self,
        tasks: set[asyncio.Task[Any]],
        settle_deadline: float,
    ) -> list[asyncio.Task[Any]]:
        pending = {task for task in tasks if not task.done()}
        if pending:
            timeout = max(0.0, settle_deadline - time.monotonic())
            _done, pending = await asyncio.wait(pending, timeout=timeout)
        if pending:
            LOGGER.error(
                "abandoning %d task(s) that did not settle by the shutdown settlement deadline",
                len(pending),
            )
        return list(pending)

    async def _observe_cleanup(
        self,
        cleanup: Any,
        deadline: float,
        kind: str,
    ) -> list[asyncio.Task[Any]]:
        # asyncio.wait (unlike wait_for) does not block on cancellation
        # settlement, so a cleanup step that resists cancellation is recorded
        # as incomplete instead of wedging close() past its bound.
        task: asyncio.Task[Any] = asyncio.ensure_future(cleanup)
        timeout = max(0.0, deadline - time.monotonic())
        _done, pending = await asyncio.wait({task}, timeout=timeout)
        if pending:
            LOGGER.error("%s did not finish by its shutdown deadline; cancelling", kind)
            task.cancel()
            _done, pending = await asyncio.wait(
                {task}, timeout=SHUTDOWN_CLEANUP_CANCEL_GRACE_SECONDS
            )
        if pending:
            LOGGER.error("%s resisted cancellation; abandoning", kind)
            task.add_done_callback(self._settle_tracked_task)
            return [task]
        if task.cancelled():
            LOGGER.error("%s was cancelled before completing; cleanup is incomplete", kind)
            return [task]
        exc = task.exception()
        if exc is not None:
            LOGGER.error("%s failed: %s", kind, self.redactor.redact(exc.__class__.__name__))
            LOGGER.debug("%s failure details", kind, exc_info=exc)
            return [task]
        return []

    def _settle_tracked_task(self, task: asyncio.Task[Any]) -> None:
        self._signal_turn_tasks.discard(task)
        self._control_client_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        # Retrieving the exception here silences the unretrieved-exception
        # warning for tasks that finish after their awaiter stopped listening
        # (shutdown races, abandoned stragglers). Outside shutdown the awaiter
        # logs the failure itself, so keep this at debug there.
        level = logging.WARNING if self._closing else logging.DEBUG
        LOGGER.log(
            level,
            "tracked task failed while unobserved: %s",
            self.redactor.redact(exc.__class__.__name__),
        )
        LOGGER.debug("tracked task failure details", exc_info=exc)

    async def _drain_route_locks(self, deadline: float) -> None:
        for key, lock in list(self._route_locks.items()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if lock.locked():
                    LOGGER.warning(
                        "shutdown drain deadline reached with route %s still busy",
                        self.redactor.ref("route", key),
                    )
                continue
            try:
                await asyncio.wait_for(lock.acquire(), timeout=remaining)
            except TimeoutError:
                LOGGER.warning(
                    "shutdown drain timed out waiting for in-flight turn on route %s",
                    self.redactor.ref("route", key),
                )
                continue
            lock.release()

    async def _run_retention_sweeps(self) -> None:
        # Startup sweep first, then one sweep per interval. Parks on the
        # shutdown event between sweeps (the control-server pattern), so
        # teardown is immediate. A failed sweep is logged and the loop
        # continues: retention must never take down transport.
        interval = self.config.router.retention.sweep_interval_seconds
        while not self._closing:
            try:
                await self._run_retention_sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.error(
                    "retention sweep failed: %s",
                    self.redactor.redact(exc.__class__.__name__),
                )
                LOGGER.debug("retention sweep failure details", exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval)
                return
            except TimeoutError:
                continue

    async def _run_retention_sweep_once(self) -> None:
        retention = self.config.router.retention
        now_ms = self._clock_ms()
        if retention.dedupe_enabled:
            dedupe_window_seconds = retention.dedupe_handled_seconds
            assert dedupe_window_seconds is not None
            cutoff_ms = now_ms - int(dedupe_window_seconds * 1000)
            pruned = await self._run_retention_worker(lambda: self._prune_dedupe(cutoff_ms))
            LOGGER.log(
                logging.INFO if pruned else logging.DEBUG,
                "retention sweep pruned %d handled dedupe rows",
                pruned,
            )
        if retention.media_enabled:
            plan = await self._run_retention_worker(
                lambda: plan_media_sweep(
                    media_root=Path(self.config.router.media_root),
                    now_ms=now_ms,
                    max_age_seconds=retention.media_max_age_seconds,
                    max_total_bytes=retention.media_max_total_bytes,
                )
            )
            result = await self._execute_media_sweep_plan(plan)
            LOGGER.log(
                logging.INFO if result.files_removed or result.dirs_removed else logging.DEBUG,
                "retention sweep removed %d media files (%d bytes, %d dirs)",
                result.files_removed,
                result.bytes_removed,
                result.dirs_removed,
            )

    def _prune_dedupe(self, cutoff_ms: int) -> int:
        pruned = self.dedupe.prune_handled_before(cutoff_ms)
        if pruned > 0:
            self.dedupe.incremental_vacuum()
        return pruned

    async def _run_retention_worker(self, work: Callable[[], _T]) -> _T:
        # The worker future is retained un-cancelled: shielding the await
        # means cancelling the retention task raises here without cancelling
        # the executor work, so close() can still observe real completion.
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(None, work)
        self._retention_inflight = future
        return await asyncio.shield(future)

    @staticmethod
    async def _wait_retention_worker(worker: asyncio.Future[Any]) -> None:
        with suppress(Exception):
            await asyncio.shield(worker)

    async def _execute_media_sweep_plan(self, plan: MediaSweepPlan) -> MediaSweepResult:
        # Deletions run on the event loop so the pre-unlink mtime recheck
        # and live-path check cannot interleave with media writes; bounded
        # batches with a yield keep the loop responsive.
        files_removed = 0
        bytes_removed = 0
        dirs_removed = 0
        for start in range(0, len(plan.groups), RETENTION_EXECUTE_BATCH_ITEMS):
            if self._closing:
                break
            partial = execute_media_sweep_groups(
                plan.groups[start : start + RETENTION_EXECUTE_BATCH_ITEMS],
                is_live=self._is_live_media,
            )
            files_removed += partial.files_removed
            bytes_removed += partial.bytes_removed
            await asyncio.sleep(0)
        media_root = Path(self.config.router.media_root)
        for start in range(0, len(plan.candidate_dirs), RETENTION_EXECUTE_BATCH_ITEMS):
            if self._closing:
                break
            dirs_removed += remove_empty_sweep_dirs(
                plan.candidate_dirs[start : start + RETENTION_EXECUTE_BATCH_ITEMS],
                media_root,
            )
            await asyncio.sleep(0)
        return MediaSweepResult(
            files_removed=files_removed,
            bytes_removed=bytes_removed,
            dirs_removed=dirs_removed,
        )

    def _register_live_media(self, path: Path) -> None:
        self._live_media[path] += 1

    def _release_live_media(self, path: Path) -> None:
        count = self._live_media.get(path, 0)
        if count <= 1:
            self._live_media.pop(path, None)
        else:
            self._live_media[path] = count - 1

    def _is_live_media(self, path: Path) -> bool:
        return path in self._live_media

    async def handle_raw_event(self, raw: dict) -> TurnResult | None:
        probe = probe_signal_route(raw)
        route = None
        if probe.group_id is not None:
            route = self.config.find_group_route("signal", probe.group_id)
        elif probe.is_direct_data_message:
            route = self.config.find_direct_route(
                "signal",
                probe.source_uuid,
                probe.source_number,
            )
        if route is None:
            _discard_event(probe.summary)
            return None
        event = parse_signal_event(
            raw,
            max_attachment_bytes=self.config.router.max_attachment_bytes,
        )
        if event is None:
            _discard_event(probe.summary)
            return None
        return await self.handle_event(event)

    async def handle_event(self, event: NormalizedEvent) -> TurnResult | None:
        route = self.config.find_route_for_event(event)
        if route is None:
            _discard_event(
                SignalEventSummary(
                    shape="normalized",
                    message_type="dataMessage",
                    has_group=event.chat_type == ChatType.GROUP and event.group_id is not None,
                )
            )
            return None
        self.redactor.add(event.group_id, event.sender_id, event.source_uuid, event.source_number)
        turn = self._signal_turn_input(route, event)
        lock = self._route_lock(route)
        async with lock:
            if self._is_empty_signal_event(event):
                self._mark_signal_turn_skipped(turn, reason="empty")
                return None
            # Early freshness check: an event that is already stale is
            # discarded without waiting on the shared profile lock, so a
            # backlog drain does not queue behind another route's long turn
            # just to be thrown away.
            if self._is_stale_signal_event(route, event):
                self._mark_signal_turn_skipped(turn, reason="stale")
                return None
            profile_lock = self._profile_lock(route.profile)
            async with profile_lock:
                # Re-check freshness at the real admission point: routes can
                # share a profile, so an event fresh at route-lock acquisition
                # can age out waiting behind another route's turn.
                if self._is_stale_signal_event(route, event):
                    self._mark_signal_turn_skipped(turn, reason="stale")
                    return None
                outcome = await self._run_turn(turn)
        return outcome.result if outcome.status == TurnOutcomeStatus.DELIVERED else None

    async def handle_synthetic_job(
        self,
        job_id: str,
        *,
        scheduled_at: int | None = None,
        idempotency_key: str | None = None,
        route_lock_timeout: float | None = None,
    ) -> TurnOutcome:
        job = self.config.find_synthetic_job(job_id)
        if job is None:
            return TurnOutcome(
                TurnOutcomeStatus.ERROR,
                error="unknown_job",
                synthetic_id=job_id,
                synthetic_kind=SyntheticTurnKind.SCHEDULED_JOB,
            )
        return await self._handle_synthetic_definition(
            job,
            scheduled_at=scheduled_at,
            idempotency_key=idempotency_key,
            route_lock_timeout=route_lock_timeout,
        )

    async def handle_notification(
        self,
        notification_id: str,
        payload: CanonicalNotificationPayload,
        *,
        outbound_attachments: Any = (),
        idempotency_key: str | None = None,
        route_lock_timeout: float | None = None,
    ) -> TurnOutcome:
        notification = self.config.find_notification(notification_id)
        if notification is None:
            return TurnOutcome(
                TurnOutcomeStatus.ERROR,
                error="unknown_notification",
                synthetic_id=notification_id,
                synthetic_kind=SyntheticTurnKind.NOTIFICATION,
            )
        return await self._handle_synthetic_definition(
            notification,
            scheduled_at=None,
            idempotency_key=idempotency_key,
            route_lock_timeout=route_lock_timeout,
            payload=payload,
            outbound_attachments=outbound_attachments,
        )

    async def _handle_synthetic_definition(
        self,
        synthetic: SyntheticRouteDefinition,
        *,
        scheduled_at: int | None = None,
        idempotency_key: str | None = None,
        route_lock_timeout: float | None = None,
        payload: CanonicalNotificationPayload | None = None,
        outbound_attachments: Any = (),
    ) -> TurnOutcome:
        def _synthetic_outcome(
            status: TurnOutcomeStatus,
            *,
            route_state: RouteState | None = None,
            error: str | None = None,
        ) -> TurnOutcome:
            return TurnOutcome(
                status,
                route_state=route_state,
                error=error,
                synthetic_id=synthetic.id,
                synthetic_kind=synthetic.kind,
            )

        route = self.config.find_route_by_name(synthetic.route_name)
        if route is None:
            return _synthetic_outcome(TurnOutcomeStatus.ERROR, error="unknown_route")
        self.redactor.add(route.key, route.group_id, route.sender_id, route.sender_number)
        turn = self._synthetic_turn_input(
            synthetic,
            route,
            scheduled_at,
            idempotency_key,
            payload,
            outbound_attachments,
        )
        timeout = (
            self.config.router.control.route_lock_timeout_seconds
            if route_lock_timeout is None
            else route_lock_timeout
        )
        lock = self._route_lock(route)
        profile_lock = self._profile_lock(route.profile)
        frozen_attachments: tuple[OutboundAttachment, ...] = ()
        try:
            if outbound_attachments:
                if self._turn_dedupe_has_status(turn, "handled"):
                    return _synthetic_outcome(
                        TurnOutcomeStatus.DEDUPED,
                        route_state=self.route_state_overrides.get(route.key, route.state),
                    )
                if not self._turn_dedupe_has_status(turn, "processing"):
                    self._maybe_clear_breaker_override(route)
                    state = self.route_state_overrides.get(route.key, route.state)
                    if state == RouteState.ACTIVE:
                        if timeout <= 0 and (lock.locked() or profile_lock.locked()):
                            return _synthetic_outcome(TurnOutcomeStatus.BUSY, route_state=state)
                        if not signal_base_url_supports_local_attachment_paths(
                            self.config.router.signal_base_url
                        ):
                            return _synthetic_outcome(
                                TurnOutcomeStatus.ERROR,
                                route_state=state,
                                error="attachment_signal_daemon_not_local",
                            )
                        try:
                            frozen_attachments = self._freeze_outbound_attachments(
                                outbound_attachments
                            )
                        except OutboundAttachmentError as exc:
                            if self._turn_dedupe_has_status(turn, "handled"):
                                return _synthetic_outcome(
                                    TurnOutcomeStatus.DEDUPED,
                                    route_state=self.route_state_overrides.get(
                                        route.key, route.state
                                    ),
                                )
                            return _synthetic_outcome(
                                TurnOutcomeStatus.ERROR,
                                route_state=state,
                                error=exc.error_code,
                            )
                        turn = replace(turn, outbound_attachments=frozen_attachments)

            if not await self._acquire_route_lock(lock, timeout):
                return _synthetic_outcome(
                    TurnOutcomeStatus.BUSY,
                    route_state=self.route_state_overrides.get(route.key, route.state),
                )
            try:
                profile_lock_acquired = await self._acquire_route_lock(profile_lock, timeout)
                if not profile_lock_acquired:
                    return _synthetic_outcome(
                        TurnOutcomeStatus.BUSY,
                        route_state=self.route_state_overrides.get(route.key, route.state),
                    )
                try:
                    return await self._run_turn(turn)
                finally:
                    profile_lock.release()
            finally:
                lock.release()
        finally:
            self._cleanup_owned_outbound_attachments(frozen_attachments)

    async def _run_turn(self, turn: RoutedTurnInput) -> TurnOutcome:
        route = turn.route
        dedupe_identities = self._turn_dedupe_identities(turn)
        claimed_dedupe: list[tuple[str, int]] = []
        for dedupe_sender_id, dedupe_timestamp in dedupe_identities:
            if not self.dedupe.claim(route.key, dedupe_sender_id, dedupe_timestamp):
                for claimed_sender_id, claimed_timestamp in claimed_dedupe:
                    self.dedupe.release(route.key, claimed_sender_id, claimed_timestamp)
                event_ref = f"{route.key}:{dedupe_sender_id}:{dedupe_timestamp}"
                LOGGER.info("deduped routed turn %s", self.redactor.ref("event", event_ref))
                return TurnOutcome(
                    TurnOutcomeStatus.DEDUPED,
                    route_state=self.route_state_overrides.get(route.key, route.state),
                    **self._synthetic_outcome_fields(turn),
                )
            claimed_dedupe.append((dedupe_sender_id, dedupe_timestamp))

        handled = False
        rate_token_reserved = False
        prompt_attempted = False
        live_manifest_paths: list[Path] = []
        try:
            self._maybe_clear_breaker_override(route)
            state = self.route_state_overrides.get(route.key, route.state)
            LOGGER.info("route %s in state %s", self.redactor.ref("route", route.key), state)
            if state == RouteState.DISABLED:
                handled = True
                return TurnOutcome(
                    TurnOutcomeStatus.SKIPPED,
                    route_state=state,
                    **self._synthetic_outcome_fields(turn),
                )
            if state == RouteState.MAINTENANCE:
                sent = await self._send_once(
                    route, route.maintenance_reply or self.config.router.maintenance_reply
                )
                if not sent:
                    failure, last_failure_at_ms = self._signal_send_failure(route)
                    handled = turn.synthetic is None
                    return TurnOutcome(
                        TurnOutcomeStatus.ERROR,
                        route_state=state,
                        error=failure.code.value,
                        failure=failure,
                        reply_sent=False,
                        **self._synthetic_failure_fields(turn, route, last_failure_at_ms),
                    )
                handled = turn.synthetic is None
                return TurnOutcome(
                    TurnOutcomeStatus.DELIVERED,
                    route_state=state,
                    **self._synthetic_outcome_fields(turn),
                )

            # Rate admission for inbound Signal turns happens here: after the
            # route-state gate (only active turns can prompt; shadow routes
            # store media by design and stay uncapped) but before media
            # storage and session acquisition, so over-limit turns shed
            # before consuming attachment I/O or ACP session work. The token
            # is refunded in the finally below if the turn fails before the
            # prompt, keeping the cap a prompt cap.
            if (
                turn.origin == TurnOrigin.SIGNAL
                and route.inbound_rate_limit is not None
                and state == RouteState.ACTIVE
            ):
                if not self._reserve_inbound_rate_token(route):
                    handled = True
                    LOGGER.info(
                        "discarding rate_limited Signal event for route %s",
                        self.redactor.ref("route", route.key),
                    )
                    return TurnOutcome(
                        TurnOutcomeStatus.SKIPPED,
                        route_state=state,
                        **self._synthetic_outcome_fields(turn),
                    )
                rate_token_reserved = True

            manifests: list[MediaManifest] = []
            if turn.signal_event is not None:
                manifests = self._store_media(route, turn.signal_event)
                for manifest in manifests:
                    # Keep stored attachments exempt from retention deletion
                    # for the duration of this turn, however long it waits
                    # on locks or the prompt.
                    self._register_live_media(manifest.canonical_path)
                    live_manifest_paths.append(manifest.canonical_path)
            if state == RouteState.SHADOW:
                handled = True
                return TurnOutcome(
                    TurnOutcomeStatus.SKIPPED,
                    route_state=state,
                    **self._synthetic_outcome_fields(turn),
                )

            frozen_attachments: tuple[OutboundAttachment, ...] = ()
            try:
                if (
                    turn.outbound_attachments
                    and not signal_base_url_supports_local_attachment_paths(
                        self.config.router.signal_base_url
                    )
                ):
                    return TurnOutcome(
                        TurnOutcomeStatus.ERROR,
                        route_state=state,
                        error="attachment_signal_daemon_not_local",
                        **self._synthetic_outcome_fields(turn),
                    )
                try:
                    frozen_attachments = self._freeze_outbound_attachments(
                        turn.outbound_attachments
                    )
                except OutboundAttachmentError as exc:
                    return TurnOutcome(
                        TurnOutcomeStatus.ERROR,
                        route_state=state,
                        error=exc.error_code,
                        **self._synthetic_outcome_fields(turn),
                    )

                blocks = self._build_turn_prompt_blocks(turn, manifests)
                permission_policy = turn.permission_policy or route.permission_policy
                try:
                    session = await self.sessions.get(
                        route,
                        turn.session,
                        permission_policy=permission_policy,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "Hermes session acquisition failed for %s: %s",
                        self.redactor.ref("route", route.key),
                        self.redactor.redact(exc.__class__.__name__),
                    )
                    LOGGER.debug("Hermes session acquisition failure details", exc_info=True)
                    failure = classify_exception(
                        exc,
                        redactor=self.redactor.redact,
                        context=FailureCode.ACP_SESSION_FAILED,
                        prefer_structured_provider_failure=True,
                    )
                    outcome = await self._handle_hermes_failure(
                        turn,
                        route,
                        state,
                        failure,
                        session=None,
                        permission_policy=permission_policy,
                    )
                    # Signal events are marked handled after the failure reply
                    # attempt; synthetic jobs release dedupe so schedulers can
                    # retry the same logical job.
                    handled = turn.synthetic is None
                    return outcome
                await self._typing(route, True)
                turn_done = asyncio.Event()
                notice_task = asyncio.create_task(self._long_running_notice(route, turn_done))
                try:
                    prompt_attempted = True
                    result = await session.profile.prompt(session.session_id, blocks)
                    # Stop the busy-notice task immediately so it cannot fire
                    # while a long chunked reply is still being sent.
                    await self._stop_busy_notice(turn_done, notice_task)
                    reply_text = result.text
                    if frozen_attachments and not reply_text.strip():
                        reply_text = ATTACHMENT_ONLY_FALLBACK_TEXT
                    if reply_text and not await self._send_once(
                        route,
                        reply_text,
                        attachments=frozen_attachments,
                    ):
                        failure, last_failure_at_ms = self._signal_send_failure(route)
                        handled = True
                        return TurnOutcome(
                            TurnOutcomeStatus.ERROR,
                            route_state=state,
                            result=result,
                            error=failure.code.value,
                            failure=failure,
                            reply_sent=False,
                            **self._synthetic_failure_fields(turn, route, last_failure_at_ms),
                        )
                    reply_sent = bool(reply_text)
                    self.circuit.record_success(route.key)
                    self._record_route_success(route)
                    handled = True
                    return TurnOutcome(
                        TurnOutcomeStatus.DELIVERED,
                        route_state=state,
                        result=result,
                        reply_sent=reply_sent,
                        **self._synthetic_outcome_fields(turn),
                    )
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
                    failure = classify_exception(exc, redactor=self.redactor.redact)
                    outcome = await self._handle_hermes_failure(
                        turn,
                        route,
                        state,
                        failure,
                        session=session,
                        permission_policy=permission_policy,
                    )
                    handled = turn.synthetic is None
                    return outcome
                finally:
                    # Defensive backup for cancellation paths that skip the
                    # try/except branches. Idempotent: no-op if already stopped.
                    await self._stop_busy_notice(turn_done, notice_task)
                    await self._typing(route, False)
                    if turn.synthetic is not None and turn.synthetic.permission_policy is not None:
                        session.profile.set_permission_policy(
                            session.session_id, route.permission_policy
                        )
                    if session.ephemeral:
                        session.profile.release_session(session.session_id)
            finally:
                self._cleanup_owned_outbound_attachments(frozen_attachments)
        finally:
            for manifest_path in live_manifest_paths:
                self._release_live_media(manifest_path)
            if rate_token_reserved and not prompt_attempted:
                # The turn was admitted but failed before the prompt (media
                # storage, attachment freeze, or session acquisition), so it
                # did not spend Hermes capacity; return the token.
                self._refund_inbound_rate_token(route)
            if handled:
                for dedupe_sender_id, dedupe_timestamp in claimed_dedupe:
                    self.dedupe.mark_handled(route.key, dedupe_sender_id, dedupe_timestamp)
            else:
                for dedupe_sender_id, dedupe_timestamp in claimed_dedupe:
                    self.dedupe.release(route.key, dedupe_sender_id, dedupe_timestamp)

    @staticmethod
    def _turn_dedupe_identities(turn: RoutedTurnInput) -> tuple[tuple[str, int], ...]:
        identities = [(turn.dedupe_sender_id, turn.dedupe_timestamp)]
        if turn.secondary_dedupe is not None:
            identities.append(turn.secondary_dedupe)
        return tuple(identities)

    def _turn_dedupe_has_status(self, turn: RoutedTurnInput, status: str) -> bool:
        return any(
            self.dedupe.status(turn.route.key, dedupe_sender_id, dedupe_timestamp) == status
            for dedupe_sender_id, dedupe_timestamp in self._turn_dedupe_identities(turn)
        )

    def _freeze_outbound_attachments(
        self,
        attachments: Any,
    ) -> tuple[OutboundAttachment, ...]:
        media_root = Path(self.config.router.media_root).expanduser()
        if isinstance(attachments, list | tuple) and all(
            isinstance(attachment, OutboundAttachment) for attachment in attachments
        ):
            attachment_requests = tuple(attachments)
        else:
            attachment_requests = validate_outbound_attachments(
                attachments,
                media_root=media_root,
                max_bytes=self.config.router.max_attachment_bytes,
            )
        if not attachment_requests:
            return ()
        frozen: list[OutboundAttachment] = []
        send_dirs: list[Path] = []
        try:
            for attachment in attachment_requests:
                if attachment.owned_by_router:
                    # Pass-through of an already-owned artifact (the nested
                    # notification flow). Ownership rule: every freeze
                    # invocation acquires one live-media reference per owned
                    # attachment it returns, and every cleanup invocation
                    # releases one, so nested freeze/cleanup pairs balance
                    # and the artifact stays live until the outer cleanup.
                    frozen.append(attachment)
                    self._register_live_media(attachment.path)
                    continue
                validated = validate_outbound_attachments(
                    [str(attachment.path)],
                    media_root=media_root,
                    max_bytes=self.config.router.max_attachment_bytes,
                )[0]
                send_dir = ensure_private_dir_tree(
                    media_root,
                    media_root / ".outbound" / uuid.uuid4().hex,
                )
                send_dirs.append(send_dir)
                suffix = validated.path.suffix.lower()
                destination = send_dir / f"attachment{suffix}"
                try:
                    with validated.path.open("rb") as handle:
                        body = handle.read(self.config.router.max_attachment_bytes + 1)
                except FileNotFoundError as exc:
                    raise OutboundAttachmentError(
                        "attachment_not_found",
                        "attachment path does not exist",
                    ) from exc
                except PermissionError as exc:
                    raise OutboundAttachmentError(
                        "attachment_not_readable",
                        "attachment path is not readable",
                    ) from exc
                except OSError as exc:
                    raise OutboundAttachmentError(
                        "attachment_not_found",
                        "attachment path could not be read",
                    ) from exc
                if len(body) > self.config.router.max_attachment_bytes:
                    raise OutboundAttachmentError(
                        "attachment_too_large",
                        f"attachment exceeds {self.config.router.max_attachment_bytes} bytes",
                    )
                write_private_bytes(destination, body)
                frozen.append(
                    OutboundAttachment(
                        path=destination.resolve(),
                        content_type=validated.content_type,
                        size=len(body),
                        owned_by_router=True,
                    )
                )
                self._register_live_media(frozen[-1].path)
                frozen_validated = validate_outbound_attachments(
                    [str(destination)],
                    media_root=media_root,
                    max_bytes=self.config.router.max_attachment_bytes,
                )[0]
                if frozen_validated.path != frozen[-1].path:
                    # Keep the live-media key aligned with the attachment
                    # object cleanup will eventually release.
                    self._release_live_media(frozen[-1].path)
                    self._register_live_media(frozen_validated.path)
                frozen[-1] = OutboundAttachment(
                    path=frozen_validated.path,
                    content_type=frozen_validated.content_type,
                    size=frozen_validated.size,
                    owned_by_router=True,
                )
        except Exception:
            self._cleanup_owned_outbound_attachments(tuple(frozen))
            for send_dir in reversed(send_dirs):
                with suppress(OSError):
                    send_dir.rmdir()
            with suppress(OSError):
                (media_root / ".outbound").rmdir()
            raise
        return tuple(frozen)

    def _cleanup_owned_outbound_attachments(
        self,
        attachments: Sequence[OutboundAttachment],
    ) -> None:
        media_root = Path(self.config.router.media_root).expanduser()
        outbound_root = media_root / ".outbound"
        for attachment in attachments:
            if not attachment.owned_by_router:
                continue
            # Matches the acquisition in _freeze_outbound_attachments: one
            # release per cleanup invocation per owned attachment.
            self._release_live_media(attachment.path)
            with suppress(FileNotFoundError):
                attachment.path.unlink()
            with suppress(OSError):
                attachment.path.parent.rmdir()
        with suppress(OSError):
            outbound_root.rmdir()

    def _signal_turn_input(self, route: Route, event: NormalizedEvent) -> RoutedTurnInput:
        return RoutedTurnInput(
            route=route,
            origin=TurnOrigin.SIGNAL,
            dedupe_sender_id=_routed_sender_id(route, event),
            dedupe_timestamp=event.timestamp,
            session=SessionKeyInput(
                sender_id=_session_sender_id(route, event),
                timestamp=event.timestamp,
            ),
            signal_event=event,
            permission_policy=route.permission_policy,
        )

    @staticmethod
    def _is_empty_signal_event(event: NormalizedEvent) -> bool:
        return not event.text.strip() and not event.attachments

    def _is_stale_signal_event(self, route: Route, event: NormalizedEvent) -> bool:
        if route.max_event_age_seconds is None:
            return False
        if event.timestamp <= 0:
            # The normalizer emits timestamp=0 for events with no envelope or
            # data-message timestamp; an unknown timestamp bypasses the
            # freshness policy rather than being treated as infinitely old.
            return False
        return self._clock_ms() - event.timestamp > route.max_event_age_seconds * 1000.0

    def _reserve_inbound_rate_token(self, route: Route) -> bool:
        bucket = self._inbound_rate_buckets.get(route.key)
        if bucket is None:
            limit = route.inbound_rate_limit
            assert limit is not None
            bucket = TokenBucket(
                capacity=float(limit.max_turns),
                refill_per_second=limit.max_turns / limit.window_seconds,
            )
            self._inbound_rate_buckets[route.key] = bucket
        return bucket.try_acquire(self._clock_ms())

    def _refund_inbound_rate_token(self, route: Route) -> None:
        bucket = self._inbound_rate_buckets.get(route.key)
        if bucket is not None:
            bucket.refund()

    def _mark_signal_turn_skipped(self, turn: RoutedTurnInput, *, reason: str) -> None:
        route = turn.route
        dedupe_identities = self._turn_dedupe_identities(turn)
        claimed_dedupe: list[tuple[str, int]] = []
        for dedupe_sender_id, dedupe_timestamp in dedupe_identities:
            if not self.dedupe.claim(route.key, dedupe_sender_id, dedupe_timestamp):
                for claimed_sender_id, claimed_timestamp in claimed_dedupe:
                    self.dedupe.release(route.key, claimed_sender_id, claimed_timestamp)
                event_ref = f"{route.key}:{dedupe_sender_id}:{dedupe_timestamp}"
                LOGGER.info("deduped routed turn %s", self.redactor.ref("event", event_ref))
                return
            claimed_dedupe.append((dedupe_sender_id, dedupe_timestamp))
        LOGGER.info(
            "discarding %s Signal event for route %s",
            reason,
            self.redactor.ref("route", route.key),
        )
        for dedupe_sender_id, dedupe_timestamp in claimed_dedupe:
            self.dedupe.mark_handled(route.key, dedupe_sender_id, dedupe_timestamp)

    def _synthetic_turn_input(
        self,
        synthetic: SyntheticRouteDefinition,
        route: Route,
        scheduled_at: int | None,
        idempotency_key: str | None,
        payload: CanonicalNotificationPayload | None = None,
        outbound_attachments: Any = (),
    ) -> RoutedTurnInput:
        triggered_at_ms = self._clock_ms()
        dedupe_sender_id, dedupe_timestamp = self._synthetic_dedupe_identity(
            synthetic.namespace,
            scheduled_at=scheduled_at,
            idempotency_key=idempotency_key,
            triggered_at_ms=triggered_at_ms,
        )
        secondary_dedupe = None
        if idempotency_key and scheduled_at is not None:
            secondary_dedupe = (synthetic.namespace, scheduled_at)
        return RoutedTurnInput(
            route=route,
            origin=_origin_for_synthetic_kind(synthetic.kind),
            dedupe_sender_id=dedupe_sender_id,
            dedupe_timestamp=dedupe_timestamp,
            secondary_dedupe=secondary_dedupe,
            session=SessionKeyInput(
                sender_id=synthetic.namespace,
                timestamp=scheduled_at if scheduled_at is not None else triggered_at_ms,
            ),
            synthetic=synthetic,
            synthetic_prompt=synthetic.prompt,
            synthetic_payload=payload,
            outbound_attachments=outbound_attachments,
            scheduled_at_ms=scheduled_at,
            triggered_at_ms=triggered_at_ms,
            permission_policy=synthetic.permission_policy or route.permission_policy,
        )

    def _synthetic_dedupe_identity(
        self,
        namespace: str,
        *,
        scheduled_at: int | None,
        idempotency_key: str | None,
        triggered_at_ms: int,
    ) -> tuple[str, int]:
        if idempotency_key:
            key_hash = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:16]
            return (
                f"{namespace}:key:{key_hash}",
                SYNTHETIC_DEDUPE_TIMESTAMP_SENTINEL,
            )
        if scheduled_at is not None:
            return namespace, scheduled_at
        return f"{namespace}:manual:{self._nonce_factory()}", triggered_at_ms

    def _build_turn_prompt_blocks(
        self,
        turn: RoutedTurnInput,
        manifests: list[MediaManifest],
    ) -> list[dict[str, str]]:
        if turn.origin == TurnOrigin.SIGNAL:
            if turn.signal_event is None:
                raise ValueError("Signal turn requires event")
            return build_prompt_blocks(
                route_context=turn.route.route_context,
                user_text=turn.signal_event.text,
                manifests=manifests,
            )
        if turn.synthetic is None or turn.synthetic_prompt is None:
            raise ValueError("synthetic turn requires definition and prompt")
        metadata: dict[str, Any] = {
            "id": turn.synthetic.id,
            "kind": turn.synthetic.kind.value,
            "origin": turn.origin.value,
            "scheduled_at_ms": turn.scheduled_at_ms,
            "triggered_at_ms": turn.triggered_at_ms,
        }
        if turn.synthetic.kind == SyntheticTurnKind.SCHEDULED_JOB:
            metadata["job_id"] = turn.synthetic.id
        if turn.synthetic_payload is not None:
            metadata["payload_bytes"] = turn.synthetic_payload.byte_length
            metadata["payload_sha256"] = turn.synthetic_payload.sha256
        return build_synthetic_prompt_blocks(
            route_context=turn.route.route_context,
            synthetic_metadata=metadata,
            synthetic_prompt=turn.synthetic_prompt,
            payload_json=turn.synthetic_payload.text
            if turn.synthetic_payload is not None
            else None,
        )

    @staticmethod
    def _synthetic_outcome_fields(turn: RoutedTurnInput) -> dict[str, Any]:
        if turn.synthetic is None:
            return {}
        return {"synthetic_id": turn.synthetic.id, "synthetic_kind": turn.synthetic.kind}

    def _synthetic_failure_fields(
        self,
        turn: RoutedTurnInput,
        route: Route,
        last_failure_at_ms: int,
    ) -> dict[str, Any]:
        if turn.synthetic is None:
            return {}
        fields = self._synthetic_outcome_fields(turn)
        fields["route_ref"] = self._route_ref_for_route(route)
        fields["profile"] = route.profile
        fields["last_failure_at_ms"] = last_failure_at_ms
        return fields

    def _route_ref_for_route(self, route: Route) -> str:
        for index, candidate in enumerate(self.config.routes):
            if candidate is route or candidate.key == route.key:
                return _route_ref(index, candidate)
        return "route:unknown"

    def _route_lock(self, route: Route) -> asyncio.Lock:
        return self._route_locks[route.key]

    def _profile_lock(self, profile: str) -> asyncio.Lock:
        return self._profile_locks[profile]

    async def _handle_hermes_failure(
        self,
        turn: RoutedTurnInput,
        route: Route,
        state: RouteState,
        failure: FailureInfo,
        *,
        session: RoutedSession | None,
        permission_policy: StaticPermissionPolicy,
    ) -> TurnOutcome:
        # Record the breaker hit and pick the user-facing reply BEFORE attempting
        # subprocess recovery. If restart or replace_after_restart fails (binary
        # missing, profile broken, cooldown), the user still gets a reply and the
        # breaker still moves; recovery is best-effort for the next event.
        last_failure_at_ms = self._record_route_failure(route, failure)
        trip = self.circuit.record_failure(route.key)
        if trip:
            self.route_state_overrides[route.key] = RouteState.MAINTENANCE
            self._trip_times[route.key] = time.monotonic()
            self._trip_times_ms[route.key] = self._clock_ms()
            LOGGER.error(
                "route %s tripped circuit breaker after %s failures",
                self.redactor.ref("route", route.key),
                trip.failures,
            )
            reply_text = route.maintenance_reply or self.config.router.maintenance_reply
        else:
            reply_text = self._failure_reply_for(route, failure)
        reply_sent = False
        if reply_text:
            reply_sent = await self._send_once(route, reply_text)
            if not reply_sent:
                LOGGER.error(
                    "failure reply delivery failed for %s; preserving original route failure",
                    self.redactor.ref("route", route.key),
                )
        try:
            await self.supervisor.restart_profile(route.profile)
            if session is not None:
                if route.session_policy != SessionPolicy.EPHEMERAL:
                    replacement = await self.sessions.replace_after_restart(
                        route,
                        turn.session,
                        session,
                        permission_policy=permission_policy,
                    )
                    if turn.synthetic is not None and turn.synthetic.permission_policy is not None:
                        replacement.profile.set_permission_policy(
                            replacement.session_id,
                            route.permission_policy,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as recovery_exc:
            LOGGER.warning(
                "Hermes recovery failed for %s: %s; route will retry on next event",
                self.redactor.ref("route", route.key),
                self.redactor.redact(recovery_exc.__class__.__name__),
            )
            LOGGER.debug("Hermes recovery failure details", exc_info=True)
        return TurnOutcome(
            TurnOutcomeStatus.ERROR,
            route_state=state,
            error=failure.code.value,
            failure=failure,
            reply_sent=reply_sent,
            **self._synthetic_failure_fields(turn, route, last_failure_at_ms),
        )

    def _failure_reply_for(self, route: Route, failure: FailureInfo) -> str:
        if route.failure_reply is not None:
            return route.failure_reply
        if is_model_provider_failure(failure) and self.config.router.model_failure_reply:
            return self.config.router.model_failure_reply
        return self.config.router.failure_reply

    def _signal_send_failure(self, route: Route) -> tuple[FailureInfo, int]:
        failure = failure_info(
            FailureCode.SIGNAL_SEND_FAILED,
            detail="Signal reply send failed",
            redactor=self.redactor.redact,
        )
        # Signal transport-out failures are surfaced in route health, but they
        # do not feed the Hermes circuit breaker: maintenance replies would use
        # the same broken Signal send path and would not protect the profile.
        last_failure_at_ms = self._record_route_failure(route, failure)
        return failure, last_failure_at_ms

    def _record_route_success(self, route: Route) -> None:
        self._last_success_ms[route.key] = self._clock_ms()

    def _record_route_failure(self, route: Route, failure: FailureInfo) -> int:
        last_failure_at_ms = self._clock_ms()
        self._last_failures[route.key] = (last_failure_at_ms, failure)
        return last_failure_at_ms

    def _route_status_response(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            route_names, route_indexes, profiles = _parse_route_status_filters(payload)
        except ValueError:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "invalid_route_status_scope"}
        routes: list[dict[str, Any]] = []
        for index, route in enumerate(self.config.routes):
            if route_names and route.name not in route_names:
                continue
            if route_indexes and index not in route_indexes:
                continue
            if profiles and route.profile not in profiles:
                continue
            routes.append(self._route_health(index, route).to_dict())
        return {
            "status": "ok",
            "routes": routes,
            "route_count": len(routes),
        }

    def _route_health(self, index: int, route: Route) -> RouteHealth:
        return RouteHealth(
            route_ref=_route_ref(index, route),
            profile=route.profile,
            route_state=self.route_state_overrides.get(route.key, route.state),
            configured_state=route.state,
            session=self.sessions.status_for_route(route),
            circuit=self._circuit_status(route),
            last_success_at_ms=self._last_success_ms.get(route.key),
            # Route status exposes the most recent sanitized failure only. It is
            # a quick health surface, not a failure history.
            last_failure_at_ms=(
                self._last_failures[route.key][0] if route.key in self._last_failures else None
            ),
            last_failure=(
                self._last_failures[route.key][1] if route.key in self._last_failures else None
            ),
        )

    def _circuit_status(self, route: Route) -> CircuitStatus:
        trip_time = self._trip_times.get(route.key)
        tripped = (
            self.route_state_overrides.get(route.key) == RouteState.MAINTENANCE
            and trip_time is not None
        )
        remaining = None
        if tripped:
            remaining = max(0.0, self.recovery_seconds - (time.monotonic() - trip_time))
        return CircuitStatus(
            state="open" if tripped else "closed",
            failure_count=self.circuit.failure_count(route.key),
            tripped_at_ms=self._trip_times_ms.get(route.key) if tripped else None,
            cooldown_remaining_seconds=remaining,
            last_reset_at_ms=self._last_breaker_reset_ms.get(route.key),
        )

    @staticmethod
    async def _acquire_route_lock(lock: asyncio.Lock, timeout: float | None) -> bool:
        if timeout is None:
            await lock.acquire()
            return True
        if timeout <= 0:
            if lock.locked():
                return False
            await lock.acquire()
            return True
        try:
            await asyncio.wait_for(lock.acquire(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    def _store_media(self, route: Route, event: NormalizedEvent) -> list[MediaManifest]:
        manifests: list[MediaManifest] = []
        if event.chat_type == ChatType.DIRECT:
            group_ref = self.redactor.ref("direct", _routed_sender_id(route, event))
        else:
            if event.group_id is None:
                raise ValueError("group event requires group_id")
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

    async def _send_once(
        self,
        route: Route,
        message: str,
        *,
        attachments: Sequence[OutboundAttachment] = (),
    ) -> bool:
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
        attachment_paths = tuple(str(attachment.path) for attachment in attachments)
        index = 0
        try:
            for index, chunk in enumerate(chunks, 1):
                chunk_attachments = attachment_paths if index == 1 else ()
                if route.chat_type == ChatType.DIRECT:
                    if chunk_attachments:
                        await self.signal.send_direct(
                            _direct_recipient(route),
                            chunk,
                            attachments=chunk_attachments,
                        )
                    else:
                        await self.signal.send_direct(_direct_recipient(route), chunk)
                else:
                    if chunk_attachments:
                        await self.signal.send_group(
                            _group_id(route),
                            chunk,
                            attachments=chunk_attachments,
                        )
                    else:
                        await self.signal.send_group(_group_id(route), chunk)
        except Exception as exc:
            LOGGER.error(
                "failed Signal reply chunk %d/%d for %s: %s",
                index,
                total,
                self.redactor.ref("route", route.key),
                self.redactor.redact(exc.__class__.__name__),
            )
            LOGGER.debug("Signal reply failure details", exc_info=True)
            return False
        return True

    async def _typing(self, route: Route, enabled: bool) -> None:
        if route.chat_type == ChatType.DIRECT:
            send_typing = getattr(self.signal, "send_typing_direct", None)
            target = _direct_recipient(route)
        else:
            send_typing = getattr(self.signal, "send_typing", None)
            target = _group_id(route)
        if send_typing is None:
            return
        try:
            await send_typing(target, enabled)
        except Exception:
            LOGGER.debug("Signal typing indicator failed", exc_info=True)

    async def _run_control_server(self) -> None:
        path = self.config.router.control_socket_path.expanduser()
        self._prepare_control_socket(path)
        # start_serving=False so the server and socket path are published for
        # begin_shutdown() before any connection can be accepted; the park on
        # the shutdown event (never serve_forever, whose cancellation path
        # embeds an unbounded wait_closed) keeps teardown synchronous here and
        # leaves connection draining to close().
        server = await asyncio.start_unix_server(
            self._accept_control_client,
            path=str(path),
            limit=self.config.router.control_request_line_limit_bytes,
            start_serving=False,
        )
        self._control_server = server
        self._control_socket_path = path
        try:
            try:
                path.chmod(0o600)
            except OSError:
                LOGGER.debug("control socket chmod unsupported for %s", path)
            if not self._closing:
                await server.start_serving()
                LOGGER.info(
                    "router control socket listening at %s",
                    self.redactor.ref("socket", str(path)),
                )
                await self._shutdown_event.wait()
        finally:
            self._close_control_listener()

    def _accept_control_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Plain synchronous accept callback: the handler task is created and
        # registered before this returns, so there is no accept-to-entry
        # window where a live connection is unknown to the tracking set.
        task = asyncio.get_running_loop().create_task(self._handle_control_client(reader, writer))
        self._control_client_tasks.add(task)
        task.add_done_callback(self._settle_tracked_task)

    def _close_control_listener(self) -> None:
        server = self._control_server
        if server is not None:
            server.close()
        path = self._control_socket_path
        if path is not None:
            with suppress(FileNotFoundError):
                path.unlink()

    async def _wait_control_server_closed(self) -> None:
        server = self._control_server
        self._control_server = None
        self._control_socket_path = None
        if server is not None:
            server.close()
            await server.wait_closed()

    def _prepare_control_socket(self, path: Path) -> None:
        if path.parent == Path("."):
            raise RuntimeError("router control socket path must include a private parent directory")
        if not _is_relative_to(path.parent, self.config.router.work_root):
            raise RuntimeError("router control socket path must be under router.work_root")
        ensure_private_dir(path.parent)
        if not path.exists():
            return
        try:
            mode = path.stat().st_mode
        except OSError:
            mode = 0
        if not stat.S_ISSOCK(mode):
            raise RuntimeError("router control socket path exists and is not a socket")
        if _unix_socket_accepts_connections(path):
            raise RuntimeError("router control socket is already in use")
        path.unlink()

    async def _handle_control_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                try:
                    line = await reader.readline()
                except ValueError:
                    await self._write_control_response(
                        writer,
                        {
                            "status": TurnOutcomeStatus.ERROR.value,
                            "error": "request_too_large",
                        },
                    )
                    break
                if not line:
                    break
                response = await self._handle_control_line(line)
                if not await self._write_control_response(writer, response):
                    break
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _write_control_response(
        self,
        writer: asyncio.StreamWriter,
        response: dict[str, Any],
    ) -> bool:
        try:
            writer.write(encode_control_message(response))
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            # Only the peer-disconnect exceptions asyncio raises for a client
            # that went away are swallowed; anything else (encoding bugs,
            # unexpected transport failures) must propagate, not be masked.
            # The control socket is unix-domain, so TCP-only disconnect
            # variants such as ConnectionAbortedError are not part of its
            # failure surface.
            LOGGER.debug(
                "control client disconnected before reading response",
                exc_info=True,
            )
            return False
        return True

    async def _handle_control_line(self, line: bytes) -> dict[str, Any]:
        if self._closing:
            # Admission gate: busy is the success-class "retry later" status,
            # so the caller's retry lands on the replacement router. Turns
            # admitted before shutdown are unaffected; they drain via close().
            return {
                "status": TurnOutcomeStatus.BUSY.value,
                "error": "router_shutting_down",
            }
        try:
            payload = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "malformed_json"}
        if not isinstance(payload, dict):
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "malformed_request"}
        command = payload.get("command")
        if command == "trigger_job":
            return await self._handle_trigger_job_control(payload)
        if command == "notify_route":
            return await self._handle_notify_route_control(payload)
        if command == "preflight_permissions":
            return await self._handle_preflight_permissions_control(payload)
        if command == "route_status":
            return self._route_status_response(payload)
        return {"status": TurnOutcomeStatus.ERROR.value, "error": "unknown_command"}

    async def _handle_trigger_job_control(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = payload.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "missing_job_id"}
        scheduled_at, error = _parse_control_scheduled_at(payload.get("scheduled_at"))
        if error is not None:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": error}
        idempotency_key, error = _parse_control_idempotency_key(payload.get("idempotency_key"))
        if error is not None:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": error}
        timeout, error = _parse_control_timeout(payload.get("timeout"))
        if error is not None:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": error}
        try:
            outcome = await self.handle_synthetic_job(
                job_id,
                scheduled_at=scheduled_at,
                idempotency_key=idempotency_key,
                route_lock_timeout=timeout,
            )
        except Exception as exc:
            LOGGER.error(
                "control trigger failed for job %s: %s",
                self.redactor.ref("job", job_id),
                self.redactor.redact(exc.__class__.__name__),
            )
            LOGGER.debug("control trigger failure details", exc_info=True)
            failure = classify_exception(
                exc,
                redactor=self.redactor.redact,
                context=FailureCode.ROUTER_ERROR,
            )
            return {
                "status": TurnOutcomeStatus.ERROR.value,
                "job_id": job_id,
                "synthetic_id": job_id,
                "synthetic_kind": SyntheticTurnKind.SCHEDULED_JOB.value,
                "error": failure.code.value,
                "failure": failure.to_dict(),
            }
        return outcome.to_control_response()

    async def _handle_notify_route_control(self, payload: dict[str, Any]) -> dict[str, Any]:
        notification_id = payload.get("notification_id")
        if not isinstance(notification_id, str) or not notification_id:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "missing_notification_id"}
        raw_payload = payload.get("payload", _MISSING)
        if raw_payload is _MISSING:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "missing_payload"}
        try:
            notification_payload = canonicalize_notification_payload(
                raw_payload,
                max_bytes=self.config.router.control.max_notification_payload_bytes,
            )
        except NotificationPayloadError as exc:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": exc.error_code}
        idempotency_key, error = _parse_control_idempotency_key(payload.get("idempotency_key"))
        if error is not None:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": error}
        timeout, error = _parse_control_timeout(payload.get("timeout"))
        if error is not None:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": error}
        deduped_response = self._deduped_notification_control_response(
            notification_id,
            idempotency_key,
        )
        if deduped_response is not None:
            return deduped_response
        try:
            outcome = await self.handle_notification(
                notification_id,
                notification_payload,
                outbound_attachments=payload.get("attachments", []),
                idempotency_key=idempotency_key,
                route_lock_timeout=timeout,
            )
        except Exception as exc:
            LOGGER.error(
                "control notification failed for notification %s: %s",
                self.redactor.ref("notification", notification_id),
                self.redactor.redact(exc.__class__.__name__),
            )
            LOGGER.debug("control notification failure details", exc_info=True)
            failure = classify_exception(
                exc,
                redactor=self.redactor.redact,
                context=FailureCode.ROUTER_ERROR,
            )
            return {
                "status": TurnOutcomeStatus.ERROR.value,
                "synthetic_id": notification_id,
                "synthetic_kind": SyntheticTurnKind.NOTIFICATION.value,
                "error": failure.code.value,
                "failure": failure.to_dict(),
            }
        return outcome.to_control_response()

    def _deduped_notification_control_response(
        self,
        notification_id: str,
        idempotency_key: str | None,
    ) -> dict[str, Any] | None:
        if not idempotency_key:
            return None
        notification = self.config.find_notification(notification_id)
        if notification is None:
            return None
        route = self.config.find_route_by_name(notification.route_name)
        if route is None:
            return None
        dedupe_sender_id, dedupe_timestamp = self._synthetic_dedupe_identity(
            notification.namespace,
            scheduled_at=None,
            idempotency_key=idempotency_key,
            triggered_at_ms=0,
        )
        if not self.dedupe.is_handled(route.key, dedupe_sender_id, dedupe_timestamp):
            return None
        return TurnOutcome(
            TurnOutcomeStatus.DEDUPED,
            route_state=self.route_state_overrides.get(route.key, route.state),
            synthetic_id=notification.id,
            synthetic_kind=notification.kind,
        ).to_control_response()

    async def _handle_preflight_permissions_control(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            scope = parse_preflight_scope(payload.get("scope"))
        except ValueError:
            return {"status": TurnOutcomeStatus.ERROR.value, "error": "invalid_preflight_scope"}
        report = await run_permission_preflight(
            self.config,
            self._probe_profile_tool_surface,
            scope=scope,
        )
        response = report.to_dict()
        failure = preflight_failure_from_report(report, redactor=self.redactor.redact)
        if failure is not None:
            response["failure"] = failure.to_dict()
        return response

    async def _probe_profile_tool_surface(self, profile: str) -> ToolSurface:
        route = self._representative_route_for_profile(profile)
        if route is None:
            raise PreflightProbeUnavailable("probe_profile_missing")
        profile_lock = self._profile_lock(profile)
        if not await self._acquire_route_lock(
            profile_lock,
            PREFLIGHT_PROFILE_LOCK_TIMEOUT_SECONDS,
        ):
            raise PreflightProbeUnavailable("probe_profile_busy")
        try:
            managed_profile = await self.supervisor.get_profile(route)
        finally:
            profile_lock.release()
        # ACP request IDs let this read-only probe run alongside later turns without
        # holding the profile lock for the full tool-surface timeout.
        probe = getattr(managed_profile, "tool_surface", None)
        if not callable(probe):
            raise PreflightProbeUnavailable("probe_unsupported")
        return await probe()

    def _representative_route_for_profile(self, profile: str) -> Route | None:
        fallback = None
        for route in self.config.routes:
            if route.profile != profile:
                continue
            if route.state == RouteState.ACTIVE:
                return route
            if fallback is None and route.state == RouteState.SHADOW:
                fallback = route
        return fallback

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
        self._trip_times_ms.pop(route.key, None)
        self._last_breaker_reset_ms[route.key] = self._clock_ms()
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
        cooldown_seconds = self.config.router.busy_notice_cooldown_seconds
        if cooldown_seconds > 0:
            last_notice_ms = self._last_busy_notice_ms.get(route.key)
            if (
                last_notice_ms is not None
                and self._clock_ms() - last_notice_ms < cooldown_seconds * 1000.0
            ):
                LOGGER.debug(
                    "suppressing busy notice for route %s during cooldown",
                    self.redactor.ref("route", route.key),
                )
                return
        if await self._send_once(route, self.config.router.busy_notice):
            # A failed send does not start a cooldown window, so the next slow
            # turn retries the notice as before.
            self._last_busy_notice_ms[route.key] = self._clock_ms()

    @staticmethod
    async def _stop_busy_notice(turn_done: asyncio.Event, notice_task: asyncio.Task[None]) -> None:
        turn_done.set()
        notice_task.cancel()
        with suppress(asyncio.CancelledError):
            await notice_task


def _discard_event(summary: SignalEventSummary) -> None:
    if summary.has_exception:
        LOGGER.warning("discarding Signal event with receive exception %s", summary)
    elif summary.message_type == "unknown":
        LOGGER.info("discarding unrouted Signal event %s", summary)
    else:
        LOGGER.debug("discarding unrouted Signal event %s", summary)


def _group_id(route: Route) -> str:
    if not route.group_id:
        raise ValueError("group route requires group_id")
    return route.group_id


def _direct_recipient(route: Route) -> str:
    if not route.sender_id:
        raise ValueError("direct route requires sender_id")
    return route.sender_id


def _routed_sender_id(route: Route, event: NormalizedEvent) -> str:
    if route.chat_type == ChatType.DIRECT:
        return _direct_recipient(route)
    return event.dedupe_sender_id


def _session_sender_id(route: Route, event: NormalizedEvent) -> str:
    if route.chat_type == ChatType.DIRECT:
        return _direct_recipient(route)
    return event.sender_id


def _origin_for_synthetic_kind(kind: SyntheticTurnKind) -> TurnOrigin:
    if kind == SyntheticTurnKind.SCHEDULED_JOB:
        return TurnOrigin.SCHEDULED_JOB
    if kind == SyntheticTurnKind.NOTIFICATION:
        return TurnOrigin.NOTIFICATION
    raise ValueError(f"unknown synthetic turn kind {kind!r}")


def _parse_control_scheduled_at(value: Any) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "invalid_scheduled_at"
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdecimal():
        parsed = int(value)
    else:
        return None, "invalid_scheduled_at"
    if parsed < 0:
        return None, "invalid_scheduled_at"
    return parsed, None


def _parse_control_idempotency_key(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str) or not value:
        return None, "invalid_idempotency_key"
    return value, None


def _parse_control_timeout(value: Any) -> tuple[float | None, str | None]:
    if value is None:
        return None, None
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        return None, "invalid_timeout"
    if not math.isfinite(timeout) or timeout < 0:
        return None, "invalid_timeout"
    return timeout, None


def _parse_route_status_filters(
    payload: dict[str, Any],
) -> tuple[tuple[str, ...], tuple[int, ...], tuple[str, ...]]:
    route_names = _string_filter_values(payload, "route", "routes", "route_names")
    profiles = _string_filter_values(payload, "profile", "profiles")
    route_indexes = _index_filter_values(payload, "route_index", "route_indexes")
    return route_names, route_indexes, profiles


def _string_filter_values(payload: dict[str, Any], *keys: str) -> tuple[str, ...]:
    values: list[str] = []
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if isinstance(raw, str):
            if not raw:
                raise ValueError(f"{key} must not be empty")
            values.append(raw)
            continue
        if isinstance(raw, list) and all(isinstance(item, str) and item for item in raw):
            values.extend(raw)
            continue
        raise ValueError(f"{key} must be a string or string list")
    return tuple(dict.fromkeys(values))


def _index_filter_values(payload: dict[str, Any], *keys: str) -> tuple[int, ...]:
    values: list[int] = []
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if isinstance(raw, bool):
            raise ValueError(f"{key} must be a non-negative integer")
        if isinstance(raw, int) and raw >= 0:
            values.append(raw)
            continue
        if isinstance(raw, list) and all(
            not isinstance(item, bool) and isinstance(item, int) and item >= 0 for item in raw
        ):
            values.extend(raw)
            continue
        raise ValueError(f"{key} must be a non-negative integer or integer list")
    return tuple(dict.fromkeys(values))


def _route_ref(index: int, route: Route) -> str:
    if route.name:
        return f"route:{route.name}"
    return f"routes[{index}]"


def _unix_socket_accepts_connections(path: Path) -> bool:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.2)
        sock.connect(str(path))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.expanduser().resolve(strict=False).relative_to(
            parent.expanduser().resolve(strict=False)
        )
    except ValueError:
        return False
    return True
