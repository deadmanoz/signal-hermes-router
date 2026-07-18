from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .acp import (
    ACPProfile,
    DEFAULT_ACP_INITIALIZE_TIMEOUT_SECONDS,
    DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS,
    DEFAULT_MAX_ACP_LINE_BYTES,
)
from .config import Route
from .models import ChatType, NormalizedEvent, SessionKeyInput, SessionPolicy, SessionStatus
from .permissions import StaticPermissionPolicy
from .redaction import sanitize_subprocess_output, stable_ref

LOGGER = logging.getLogger(__name__)

DEFAULT_RESTART_COOLDOWN_SECONDS = 5.0


@dataclass
class RoutedSession:
    profile: ACPProfile
    session_id: str
    cwd: Path
    ephemeral: bool = False
    # The route's session_policy at creation time. Config reload compares it
    # against the route's current policy: a mismatch means the session is
    # unreachable under the new keying and is evicted; a match (including a
    # flip-flop reload that restored the original policy) keeps it.
    policy: SessionPolicy = SessionPolicy.PERSISTENT_ROUTE
    # Rotation bookkeeping for cached persistent sessions: monotonic creation
    # time of the underlying ACP session's context window and the number of
    # turns it has served. Both survive a session/resume (context is
    # preserved) and reset when a fresh session replaces the old one.
    created_at: float = field(default_factory=time.monotonic)
    turn_count: int = 0


class ProfileSupervisor:
    def __init__(
        self,
        work_root: Path,
        command_template: list[str] | None = None,
        *,
        max_acp_line_bytes: int | None = DEFAULT_MAX_ACP_LINE_BYTES,
        prompt_timeout_seconds: float = DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS,
        initialize_timeout_seconds: float = DEFAULT_ACP_INITIALIZE_TIMEOUT_SECONDS,
        restart_cooldown_seconds: float = DEFAULT_RESTART_COOLDOWN_SECONDS,
    ) -> None:
        self.work_root = work_root
        self.command_template = command_template
        self.max_acp_line_bytes = max_acp_line_bytes
        self.prompt_timeout_seconds = prompt_timeout_seconds
        self.initialize_timeout_seconds = initialize_timeout_seconds
        self.restart_cooldown_seconds = restart_cooldown_seconds
        self._profiles: dict[str, ACPProfile] = {}
        self._last_restart: dict[str, float] = {}
        self._acquire_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._redact: Callable[[str], str] = lambda text: text

    def set_redactor(self, redact: Callable[[str], str]) -> None:
        """Route the free-form exit-log content (profile name, stderr tail)
        through the caller's redactor. Identity by default."""
        self._redact = redact

    async def get_profile(self, route: Route) -> ACPProfile:
        # Serialized per profile name so a concurrent acquisition can never
        # observe the provisional (still-starting) cache entry below.
        async with self._acquire_locks[route.profile]:
            return await self._acquire_profile(route)

    async def _acquire_profile(self, route: Route) -> ACPProfile:
        profile = self._profiles.get(route.profile)
        if profile is not None:
            if not profile.exit_suspected():
                return profile
            # The child died but the exit watcher is still inside its settle
            # window and has not evicted the entry yet. Evict now and fall
            # through to spawn a fresh child so this turn recovers
            # transparently; the watcher still logs the exit.
            if self._profiles.get(route.profile) is profile:
                del self._profiles[route.profile]
            await self._close_evicted_profile(profile)
        last = self._last_restart.get(route.profile)
        if last is not None and self.restart_cooldown_seconds > 0:
            elapsed = time.monotonic() - last
            if elapsed < self.restart_cooldown_seconds:
                raise RuntimeError(
                    f"Hermes profile {route.profile!r} restarted "
                    f"{elapsed:.1f}s ago; cooldown is "
                    f"{self.restart_cooldown_seconds:.1f}s"
                )
        command = None
        if self.command_template:
            command = [part.format(profile=route.profile) for part in self.command_template]
        profile = ACPProfile(
            profile=route.profile,
            work_root=self.work_root,
            command=command,
            max_line_bytes=self.max_acp_line_bytes,
            prompt_timeout_seconds=self.prompt_timeout_seconds,
            initialize_timeout_seconds=self.initialize_timeout_seconds,
        )

        def _on_exit(returncode: int | None, stderr_tail: tuple[str, ...]) -> None:
            self._handle_profile_exit(route.profile, profile, returncode, stderr_tail)

        profile.on_exit = _on_exit
        # Provisional registration BEFORE start(): a child that answers
        # initialize and dies immediately must be evictable by the exit
        # watcher; a post-start cache write could store a corpse the watcher
        # already reported.
        self._profiles[route.profile] = profile
        try:
            await profile.start()
        except BaseException as exc:
            if self._profiles.get(route.profile) is profile:
                del self._profiles[route.profile]
            if isinstance(exc, Exception):
                # Record so the next get_profile within the cooldown window
                # refuses fast rather than spawning another doomed subprocess.
                # Cancellation is not a failed start and stamps no cooldown.
                self._last_restart[route.profile] = time.monotonic()
            raise
        # Let a post-initialize death that has already been signalled surface
        # before the final evidence check: each yield gives the event loop a
        # poll cycle, so a stdout EOF that raced start()'s return is processed
        # by the reader task and becomes visible to exit_suspected(). Bounded
        # and latency-free (sleep(0) yields, no real waiting); a child that
        # dies after this window is caught by the exit watcher or the lazy
        # write-failure path instead.
        for _ in range(3):
            if profile.exit_suspected():
                break
            await asyncio.sleep(0)
        if self._profiles.get(route.profile) is not profile or profile.exit_suspected():
            # Either the exit watcher evicted this instance while start() was
            # in flight, or the child is already demonstrably dead (a child
            # can answer initialize and die before the watcher gets CPU);
            # never hand out a known-dead profile.
            if self._profiles.get(route.profile) is profile:
                del self._profiles[route.profile]
            await self._close_evicted_profile(profile)
            raise RuntimeError(f"Hermes profile {route.profile!r} exited during startup")
        return profile

    async def _close_evicted_profile(self, profile: ACPProfile) -> None:
        # Exit evidence can come from broken pipes on a child that has not
        # fully exited (for example one that closed its stdout while wedged).
        # Close the evicted profile so eviction never orphans a subprocess
        # that _profiles no longer owns; close() is safe on an already-dead
        # peer and still lets the watcher report the exit first.
        try:
            await profile.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.warning("evicted Hermes profile close failed")
            LOGGER.debug("evicted Hermes profile close failure details", exc_info=True)

    def _handle_profile_exit(
        self,
        profile_name: str,
        profile: ACPProfile,
        returncode: int | None,
        stderr_tail: tuple[str, ...],
    ) -> None:
        if self._profiles.get(profile_name) is profile:
            # Mark dead: the next acquisition spawns a fresh subprocess. No
            # eager respawn (a crash loop must not spin without traffic) and
            # no failed-start cooldown stamp (the next turn must recover
            # transparently; a genuinely broken binary still trips the
            # cooldown on its next failed start).
            del self._profiles[profile_name]
        LOGGER.error(
            "Hermes profile %s subprocess exited unexpectedly with returncode %s; "
            "marked dead, will respawn on next acquisition",
            self._redact(profile_name),
            returncode,
        )
        if stderr_tail:
            # Sanitize per line so a credential assignment masks the rest of
            # its own stderr line, not everything joined after it.
            sanitized_tail = " | ".join(sanitize_subprocess_output(line) for line in stderr_tail)
            LOGGER.error(
                "Hermes profile %s stderr tail near exit: %s",
                self._redact(profile_name),
                self._redact(sanitized_tail),
            )

    async def restart_profile(self, profile_name: str) -> None:
        existing = self._profiles.pop(profile_name, None)
        if existing is not None:
            await existing.close()
        # Cooldown is recorded by get_profile when start() fails — that is
        # the actual thundering-herd scenario (binary missing, broken
        # profile, OOM at startup). A successful close after a prompt
        # failure is part of the normal recovery flow and must not block
        # the immediately-following replace_after_restart -> get_profile
        # call that re-spawns a fresh subprocess.

    def cached_profile_names(self) -> list[str]:
        """Names of profiles with a cached subprocess entry, live or dead."""
        return list(self._profiles)

    async def retire_profile(self, profile_name: str) -> bool:
        """Close and evict the cached subprocess for a profile that no route
        can use anymore (a live config reload left it with no active routes).
        Returns True when a cached entry was retired. Unlike restart_profile
        there is no successor acquisition, so the profile simply goes away.
        Serialized per profile name with get_profile: without the lock a
        concurrent turn could observe the empty cache slot and spawn a second
        subprocess while this one is still closing."""
        async with self._acquire_locks[profile_name]:
            existing = self._profiles.pop(profile_name, None)
            if existing is None:
                return False
            await existing.close()
            return True

    async def close(self) -> None:
        profiles = list(self._profiles.values())
        self._profiles.clear()
        if not profiles:
            return
        # Close concurrently so one slow profile cannot serialize the rest,
        # and isolate failures so every profile is attempted. Failures are
        # re-raised as an aggregate afterwards so the router's shutdown path
        # reports the close as incomplete instead of silently succeeding.
        results = await asyncio.gather(
            *(profile.close() for profile in profiles),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        for failure in failures:
            LOGGER.warning("Hermes profile close failed: %s", failure.__class__.__name__)
            LOGGER.debug("Hermes profile close failure details", exc_info=failure)
        if failures:
            raise RuntimeError(f"{len(failures)} Hermes profile close call(s) failed")


class SessionRegistry:
    def __init__(
        self,
        work_root: Path,
        supervisor: ProfileSupervisor,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.work_root = work_root
        self.supervisor = supervisor
        self._clock = clock
        self._sessions: dict[str, RoutedSession] = {}
        self._session_routes: dict[str, str] = {}

    async def get(
        self,
        route: Route,
        session_ref: NormalizedEvent | SessionKeyInput,
        *,
        permission_policy: StaticPermissionPolicy | None = None,
    ) -> RoutedSession:
        profile = await self.supervisor.get_profile(route)
        session_key = self._session_key(route, session_ref)
        policy = permission_policy or route.permission_policy
        existing = self._sessions.get(session_key)
        if existing is not None and self._rotate_expired(route, session_key, existing, profile):
            existing = None
        if existing:
            if existing.profile is not profile:
                existing = await self._resume_or_recreate(route, profile, existing, policy)
                self._sessions[session_key] = existing
                self._session_routes[session_key] = route.key
            existing.turn_count += 1
            profile.set_permission_policy(existing.session_id, policy)
            return existing
        cwd = self._cwd(route.profile, session_key)
        session_id = await profile.new_session(cwd)
        profile.set_permission_policy(session_id, policy)
        ephemeral = route.session_policy == SessionPolicy.EPHEMERAL
        session = RoutedSession(
            profile=profile,
            session_id=session_id,
            cwd=cwd,
            ephemeral=ephemeral,
            policy=route.session_policy,
            created_at=self._clock(),
            turn_count=1,
        )
        if not ephemeral:
            self._sessions[session_key] = session
            self._session_routes[session_key] = route.key
        return session

    def _rotate_expired(
        self,
        route: Route,
        session_key: str,
        session: RoutedSession,
        profile: ACPProfile,
    ) -> bool:
        """Evict a cached persistent session whose rotation budget is spent so
        the caller creates a fresh session (session/new) for this turn. Purely
        transport session lifecycle: the Hermes subprocess keeps running."""
        age = self._clock() - session.created_at
        if route.session_max_turns is not None and session.turn_count >= route.session_max_turns:
            reason = "max_turns"
        elif route.session_max_age_seconds is not None and age >= route.session_max_age_seconds:
            reason = "max_age"
        else:
            return False
        self._sessions.pop(session_key, None)
        self._session_routes.pop(session_key, None)
        if session.profile is profile:
            # Drop the rotated session's per-session state (permission policy,
            # prompt lock, update subscription) on the live profile so long
            # rotation histories do not accumulate entries. A session cached
            # against an evicted profile instance has nothing to release here.
            profile.release_session(session.session_id)
        # Hashed route ref plus code-controlled reason and counters only:
        # redaction-safe by construction.
        LOGGER.info(
            "rotating Hermes session for %s after %d turn(s) (age %.0fs): %s; "
            "creating a fresh session",
            stable_ref("route", route.key),
            session.turn_count,
            age,
            reason,
        )
        return True

    async def replace_after_restart(
        self,
        route: Route,
        session_ref: NormalizedEvent | SessionKeyInput,
        previous: RoutedSession,
        *,
        permission_policy: StaticPermissionPolicy | None = None,
    ) -> RoutedSession:
        profile = await self.supervisor.get_profile(route)
        policy = permission_policy or route.permission_policy
        replacement = await self._resume_or_recreate(route, profile, previous, policy)
        if route.session_policy != SessionPolicy.EPHEMERAL:
            session_key = self._session_key(route, session_ref)
            self._sessions[session_key] = replacement
            self._session_routes[session_key] = route.key
        return replacement

    def status_for_route(self, route: Route) -> SessionStatus:
        keys = [
            session_key
            for session_key, route_key in self._session_routes.items()
            if route_key == route.key and session_key in self._sessions
        ]
        return SessionStatus(
            policy=route.session_policy,
            cached=bool(keys),
            cached_sessions=len(keys),
        )

    def drop_sessions_not_in(
        self, live_route_keys: set[str], *, only_route_keys: set[str] | None = None
    ) -> int:
        """Evict cached sessions whose route can no longer prompt (a live
        config reload made the route non-active or removed it), releasing
        per-session state on the owning profile. Returns the number evicted.
        Sessions for routes that stayed configured-active are kept, including
        ones currently under a breaker override — the route prompts again as
        soon as the breaker recovers. When only_route_keys is given, only
        sessions for those route keys are eligible: a reaper must not evict
        the session of a route disabled by a LATER reload, whose in-flight
        turn it never drained."""
        return self._drop_sessions_matching(
            lambda _sk, route_key, _s: route_key not in live_route_keys
            and (only_route_keys is None or route_key in only_route_keys)
        )

    def drop_sessions_with_mismatched_policy(
        self,
        current_policies: dict[str, SessionPolicy],
        *,
        only_route_keys: set[str] | None = None,
    ) -> int:
        """Evict cached sessions whose creation-time session_policy no longer
        matches their route's current policy (a live config reload changed
        the keying, leaving them unreachable), releasing per-session state on
        the owning profile. Returns the number evicted. Comparing per session
        means a flip-flop reload that restored the original policy keeps the
        still-reachable session. only_route_keys scopes the eviction the same
        way as drop_sessions_not_in."""
        return self._drop_sessions_matching(
            lambda _sk, route_key, session: (
                route_key in current_policies
                and session.policy != current_policies[route_key]
                and (only_route_keys is None or route_key in only_route_keys)
            )
        )

    def _drop_sessions_matching(
        self, predicate: Callable[[str, str, RoutedSession], bool]
    ) -> int:
        evicted = 0
        for session_key, route_key in list(self._session_routes.items()):
            session = self._sessions.get(session_key)
            if session is None or not predicate(session_key, route_key, session):
                continue
            self._sessions.pop(session_key, None)
            self._session_routes.pop(session_key, None)
            session.profile.release_session(session.session_id)
            evicted += 1
        return evicted

    async def _resume_or_recreate(
        self,
        route: Route,
        profile: ACPProfile,
        previous: RoutedSession,
        permission_policy: StaticPermissionPolicy,
    ) -> RoutedSession:
        try:
            resumed = await profile.resume_session(previous.session_id, previous.cwd)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not route.recreate_session_on_resume_failure:
                raise
            # This option deliberately treats every resume exception as stale
            # session state, including structured provider errors.
            LOGGER.warning(
                "Hermes session resume failed for profile %s; creating a fresh session",
                route.profile,
            )
            LOGGER.debug("Hermes session resume failure details", exc_info=True)
            session_id = await profile.new_session(previous.cwd)
        else:
            if resumed:
                session_id = previous.session_id
            else:
                LOGGER.error(
                    "Hermes profile does not advertise ACP session resume; creating a fresh session"
                )
                session_id = await profile.new_session(previous.cwd)
        profile.set_permission_policy(session_id, permission_policy)
        # A resumed session keeps its accumulated context, so the rotation
        # budget carries over; a fresh session starts a new budget. The caller
        # (or the next turn's get) counts the turn, so no increment here.
        resumed_previous = session_id == previous.session_id
        return RoutedSession(
            profile=profile,
            session_id=session_id,
            cwd=previous.cwd,
            ephemeral=previous.ephemeral,
            policy=route.session_policy,
            created_at=previous.created_at if resumed_previous else self._clock(),
            turn_count=previous.turn_count if resumed_previous else 0,
        )

    def _session_key(self, route: Route, session_ref: NormalizedEvent | SessionKeyInput) -> str:
        session_input = _session_key_input(route, session_ref)
        if route.session_policy == SessionPolicy.PERSISTENT_ROUTE:
            raw = route.key
        elif route.session_policy == SessionPolicy.PERSISTENT_SENDER:
            raw = f"{route.key}:{session_input.sender_id}"
        else:
            raw = (
                f"{route.key}:{session_input.sender_id}:"
                f"{session_input.timestamp}:{uuid.uuid4().hex}"
            )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _cwd(self, profile: str, session_key: str) -> Path:
        profiles_root = (self.work_root / "profiles").resolve(strict=False)
        cwd = profiles_root / profile / "sessions" / session_key
        try:
            cwd.resolve(strict=False).relative_to(profiles_root)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ValueError("session cwd escaped configured work_root") from exc
        return cwd


def _routed_sender_id(route: Route, event: NormalizedEvent) -> str:
    if route.chat_type == ChatType.DIRECT:
        if not route.sender_id:
            raise ValueError("direct route requires sender_id")
        return route.sender_id
    return event.sender_id


def _session_key_input(
    route: Route,
    session_ref: NormalizedEvent | SessionKeyInput,
) -> SessionKeyInput:
    if isinstance(session_ref, SessionKeyInput):
        return session_ref
    return SessionKeyInput(
        sender_id=_routed_sender_id(route, session_ref),
        timestamp=session_ref.timestamp,
    )
