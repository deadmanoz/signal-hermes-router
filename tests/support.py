from __future__ import annotations

import stat
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from signal_hermes_router.acp import ACPProfile
from signal_hermes_router.config import (
    AppConfig,
    CircuitBreakerConfig,
    InboundRateLimitConfig,
    Route,
    RouterConfig,
    SyntheticRouteNotification,
    SyntheticRouteJob,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    ChatType,
    NormalizedEvent,
    RouteState,
    SessionPolicy,
    SignalAttachment,
    TurnResult,
)
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.router import SignalHermesRouter


def file_mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def router_config_for_tmp(tmp: str | Path, **overrides: Any) -> RouterConfig:
    root = Path(tmp)
    values: dict[str, Any] = {
        "state_db": root / "state.db",
        "media_root": root / "media",
        "signal_attachment_root": root / "signal-attachments",
        "work_root": root / "work",
    }
    values.update(overrides)
    return RouterConfig(**values)


def make_route(
    *,
    platform: str = "signal",
    chat_type: ChatType = ChatType.GROUP,
    group_id: str | None = "group",
    sender_id: str | None = None,
    sender_number: str | None = None,
    profile: str = "profile",
    session_policy: SessionPolicy = SessionPolicy.PERSISTENT_ROUTE,
    state: RouteState = RouteState.ACTIVE,
    name: str | None = None,
    route_context: dict[str, Any] | None = None,
    permission_policy: StaticPermissionPolicy | None = None,
    friendly_name: str | None = None,
    maintenance_reply: str | None = None,
    failure_reply: str | None = None,
    recreate_session_on_resume_failure: bool = False,
    max_event_age_seconds: float | None = None,
    inbound_rate_limit: InboundRateLimitConfig | None = None,
) -> Route:
    return Route(
        platform=platform,
        name=name,
        chat_type=chat_type,
        group_id=None if chat_type == ChatType.DIRECT and group_id == "group" else group_id,
        sender_id=sender_id,
        sender_number=sender_number,
        profile=profile,
        session_policy=session_policy,
        state=state,
        route_context=route_context or {},
        permission_policy=permission_policy or StaticPermissionPolicy(),
        friendly_name=friendly_name,
        maintenance_reply=maintenance_reply,
        failure_reply=failure_reply,
        recreate_session_on_resume_failure=recreate_session_on_resume_failure,
        max_event_age_seconds=max_event_age_seconds,
        inbound_rate_limit=inbound_rate_limit,
    )


def make_event(
    *,
    platform: str = "signal",
    chat_type: ChatType = ChatType.GROUP,
    group_id: str | None = "group",
    sender_id: str = "sender",
    source_uuid: str | None = None,
    source_number: str | None = None,
    timestamp: int = 1,
    text: str = "hello",
    attachments: tuple[SignalAttachment, ...] = (),
    raw: dict[str, Any] | None = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        platform=platform,
        chat_type=chat_type,
        group_id=None if chat_type == ChatType.DIRECT and group_id == "group" else group_id,
        sender_id=sender_id,
        source_uuid=source_uuid or sender_id,
        source_number=source_number,
        timestamp=timestamp,
        text=text,
        attachments=attachments,
        raw=raw or {},
    )


def make_attachment_event(
    attachment: SignalAttachment,
    *,
    timestamp: int = 10,
    text: str = "file",
    **overrides: Any,
) -> NormalizedEvent:
    return make_event(
        timestamp=timestamp,
        text=text,
        attachments=(attachment,),
        **overrides,
    )


def make_app(
    tmp: str | Path,
    state: RouteState,
    failures: int = 3,
    route_context: dict[str, Any] | None = None,
    routes: tuple[Route, ...] | None = None,
    scheduled_jobs: tuple[SyntheticRouteJob, ...] = (),
    notifications: tuple[SyntheticRouteNotification, ...] = (),
    **router_overrides: Any,
) -> AppConfig:
    route = make_route(
        state=state,
        route_context=route_context or {"label": "synthetic"},
    )
    router_overrides.setdefault(
        "circuit_breaker",
        CircuitBreakerConfig(failures=failures, window_seconds=60),
    )
    return AppConfig(
        router=router_config_for_tmp(tmp, **router_overrides),
        routes=routes or (route,),
        scheduled_jobs=scheduled_jobs,
        notifications=notifications,
    )


class FakeSignal:
    def __init__(self) -> None:
        self.sends: list[tuple[str, str]] = []
        self.direct_sends: list[tuple[str, str]] = []
        self.send_attachments: list[tuple[str, tuple[str, ...]]] = []
        self.direct_send_attachments: list[tuple[str, tuple[str, ...]]] = []
        self.typing: list[tuple[str, bool]] = []
        self.direct_typing: list[tuple[str, bool]] = []

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        self.sends.append((group_id, message))
        self.send_attachments.append((group_id, tuple(attachments)))
        return {"timestamp": 1}

    async def send_direct(
        self,
        recipient: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        self.direct_sends.append((recipient, message))
        self.direct_send_attachments.append((recipient, tuple(attachments)))
        return {"timestamp": 1}

    async def send_typing(self, group_id: str, enabled: bool) -> dict[str, int]:
        self.typing.append((group_id, enabled))
        return {"timestamp": 1}

    async def send_typing_direct(self, recipient: str, enabled: bool) -> dict[str, int]:
        self.direct_typing.append((recipient, enabled))
        return {"timestamp": 1}

    async def close(self) -> None:
        return None


class FakeProfile:
    def __init__(self) -> None:
        self.profile = "profile"
        self.prompts: list[list[dict[str, Any]]] = []
        self.resumes = 0
        self.new_sessions = 0
        self.new_session_cwds: list[Path] = []
        self.prompt_session_ids: list[str] = []
        self.released_session_ids: list[str] = []
        self.resume_available = True
        self.fail = False
        self.reply_text = "reply"
        self.prompt_delay = 0.0
        self.policies: list[tuple[str, StaticPermissionPolicy]] = []

    async def new_session(self, cwd: Path) -> str:
        cwd.mkdir(parents=True, exist_ok=True)
        self.new_sessions += 1
        self.new_session_cwds.append(cwd)
        return f"session-{self.new_sessions}"

    async def resume_session(self, session_id: str, cwd: Path) -> bool:
        self.resumes += 1
        return self.resume_available

    def set_permission_policy(self, session_id: str, policy: StaticPermissionPolicy) -> None:
        self.policies.append((session_id, policy))
        return None

    def release_session(self, session_id: str) -> None:
        self.released_session_ids.append(session_id)

    async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
        self.prompt_session_ids.append(session_id)
        self.prompts.append(blocks)
        if self.prompt_delay:
            import asyncio

            await asyncio.sleep(self.prompt_delay)
        if self.fail:
            raise RuntimeError("boom")
        return TurnResult(self.reply_text)


class FakeSupervisor:
    def __init__(self, profile: FakeProfile) -> None:
        self.profile = profile
        self.restarts = 0

    async def get_profile(self, route: Route) -> FakeProfile:
        return self.profile

    async def restart_profile(self, profile_name: str) -> None:
        self.restarts += 1

    async def close(self) -> None:
        return None


@dataclass
class RouterHarness:
    router: SignalHermesRouter
    signal: FakeSignal
    profile: FakeProfile
    supervisor: FakeSupervisor
    dedupe: DedupeStore


def make_router_harness(
    tmp: str | Path,
    *,
    state: RouteState = RouteState.ACTIVE,
    failures: int = 3,
    route_context: dict[str, Any] | None = None,
    app: AppConfig | None = None,
    signal: FakeSignal | None = None,
    profile: FakeProfile | None = None,
    supervisor: FakeSupervisor | None = None,
    dedupe: DedupeStore | None = None,
    **router_overrides: Any,
) -> RouterHarness:
    signal = signal or FakeSignal()
    profile = profile or FakeProfile()
    supervisor = supervisor or FakeSupervisor(profile)
    dedupe = dedupe or DedupeStore()
    app = app or make_app(
        tmp,
        state,
        failures=failures,
        route_context=route_context,
        **router_overrides,
    )
    router = SignalHermesRouter(
        app,
        signal_client=signal,  # type: ignore[arg-type]
        supervisor=supervisor,  # type: ignore[arg-type]
        dedupe=dedupe,
    )
    return RouterHarness(
        router=router,
        signal=signal,
        profile=profile,
        supervisor=supervisor,
        dedupe=dedupe,
    )


def read_file_allow_policy(
    prefix: str = "/private/deployment/read-only/",
) -> StaticPermissionPolicy:
    return StaticPermissionPolicy.from_config(
        [
            {
                "tool": "read_file",
                "arguments": {"path": {"prefix": prefix}},
            }
        ]
    )


@asynccontextmanager
async def started_acp_profile(
    tmp: str | Path,
    *,
    fixture: str = "fake_acp_agent.py",
    profile: str = "synthetic",
) -> AsyncIterator[ACPProfile]:
    script = Path(__file__).parent / "fixtures" / fixture
    acp_profile = ACPProfile(
        profile=profile,
        work_root=Path(tmp),
        command=[sys.executable, str(script)],
    )
    await acp_profile.start()
    try:
        yield acp_profile
    finally:
        await acp_profile.close()
