from __future__ import annotations

import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from .acp import ACPProfile, DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS, DEFAULT_MAX_ACP_LINE_BYTES
from .config import Route
from .models import NormalizedEvent, SessionPolicy

LOGGER = logging.getLogger(__name__)

DEFAULT_RESTART_COOLDOWN_SECONDS = 5.0


@dataclass
class RoutedSession:
    profile: ACPProfile
    session_id: str
    cwd: Path
    ephemeral: bool = False


class ProfileSupervisor:
    def __init__(
        self,
        work_root: Path,
        command_template: list[str] | None = None,
        *,
        max_acp_line_bytes: int | None = DEFAULT_MAX_ACP_LINE_BYTES,
        prompt_timeout_seconds: float = DEFAULT_ACP_PROMPT_TIMEOUT_SECONDS,
        restart_cooldown_seconds: float = DEFAULT_RESTART_COOLDOWN_SECONDS,
    ) -> None:
        self.work_root = work_root
        self.command_template = command_template
        self.max_acp_line_bytes = max_acp_line_bytes
        self.prompt_timeout_seconds = prompt_timeout_seconds
        self.restart_cooldown_seconds = restart_cooldown_seconds
        self._profiles: dict[str, ACPProfile] = {}
        self._last_restart: dict[str, float] = {}

    async def get_profile(self, route: Route) -> ACPProfile:
        profile = self._profiles.get(route.profile)
        if profile is not None:
            return profile
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
        )
        try:
            await profile.start()
        except Exception:
            # Record so the next get_profile within the cooldown window
            # refuses fast rather than spawning another doomed subprocess.
            self._last_restart[route.profile] = time.monotonic()
            raise
        self._profiles[route.profile] = profile
        return profile

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

    async def close(self) -> None:
        for profile in list(self._profiles.values()):
            await profile.close()
        self._profiles.clear()


class SessionRegistry:
    def __init__(self, work_root: Path, supervisor: ProfileSupervisor) -> None:
        self.work_root = work_root
        self.supervisor = supervisor
        self._sessions: dict[str, RoutedSession] = {}

    async def get(self, route: Route, event: NormalizedEvent) -> RoutedSession:
        profile = await self.supervisor.get_profile(route)
        session_key = self._session_key(route, event)
        existing = self._sessions.get(session_key)
        if existing:
            if existing.profile is not profile:
                existing = await self._resume_or_recreate(route, profile, existing)
                self._sessions[session_key] = existing
            profile.set_permission_policy(existing.session_id, route.permission_policy)
            return existing
        cwd = self._cwd(route.profile, session_key)
        session_id = await profile.new_session(cwd)
        profile.set_permission_policy(session_id, route.permission_policy)
        ephemeral = route.session_policy == SessionPolicy.EPHEMERAL
        session = RoutedSession(
            profile=profile, session_id=session_id, cwd=cwd, ephemeral=ephemeral
        )
        if not ephemeral:
            self._sessions[session_key] = session
        return session

    async def replace_after_restart(
        self,
        route: Route,
        event: NormalizedEvent,
        previous: RoutedSession,
    ) -> RoutedSession:
        profile = await self.supervisor.get_profile(route)
        replacement = await self._resume_or_recreate(route, profile, previous)
        if route.session_policy != SessionPolicy.EPHEMERAL:
            self._sessions[self._session_key(route, event)] = replacement
        return replacement

    async def _resume_or_recreate(
        self,
        route: Route,
        profile: ACPProfile,
        previous: RoutedSession,
    ) -> RoutedSession:
        if await profile.resume_session(previous.session_id, previous.cwd):
            session_id = previous.session_id
        else:
            LOGGER.error(
                "Hermes profile does not advertise ACP session resume; creating a fresh session"
            )
            session_id = await profile.new_session(previous.cwd)
        profile.set_permission_policy(session_id, route.permission_policy)
        return RoutedSession(
            profile=profile,
            session_id=session_id,
            cwd=previous.cwd,
            ephemeral=previous.ephemeral,
        )

    def _session_key(self, route: Route, event: NormalizedEvent) -> str:
        if route.session_policy == SessionPolicy.PERSISTENT_ROUTE:
            raw = route.key
        elif route.session_policy == SessionPolicy.PERSISTENT_SENDER:
            raw = f"{route.key}:{event.sender_id}"
        else:
            raw = f"{route.key}:{event.sender_id}:{event.timestamp}:{uuid.uuid4().hex}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _cwd(self, profile: str, session_key: str) -> Path:
        profiles_root = (self.work_root / "profiles").resolve(strict=False)
        cwd = profiles_root / profile / "sessions" / session_key
        try:
            cwd.resolve(strict=False).relative_to(profiles_root)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ValueError("session cwd escaped configured work_root") from exc
        return cwd
