from __future__ import annotations

import asyncio
import stat
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from signal_hermes_router.acp import ACPProfile, JsonRpcError
from signal_hermes_router.config import (
    AppConfig,
    CircuitBreakerConfig,
    InboundRateLimitConfig,
    Route,
    RouterConfig,
    RouterControlConfig,
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
from signal_hermes_router.preflight import ToolSurface
from signal_hermes_router.private_fs import ensure_private_dir_tree, write_private_bytes
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
    session_max_turns: int | None = None,
    session_max_age_seconds: float | None = None,
    max_event_age_seconds: float | None = None,
    inbound_rate_limit: InboundRateLimitConfig | None = None,
    mcp_only: bool = False,
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
        session_max_turns=session_max_turns,
        session_max_age_seconds=session_max_age_seconds,
        max_event_age_seconds=max_event_age_seconds,
        inbound_rate_limit=inbound_rate_limit,
        mcp_only=mcp_only,
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
    def __init__(
        self,
        *,
        events_to_yield: list[dict[str, Any]] | None = None,
        fail_send_after_n: int | None = None,
        closed: bool = False,
    ) -> None:
        self.sends: list[tuple[str, str]] = []
        self.direct_sends: list[tuple[str, str]] = []
        self.send_attachments: list[tuple[str, tuple[str, ...]]] = []
        self.direct_send_attachments: list[tuple[str, tuple[str, ...]]] = []
        self.typing: list[tuple[str, bool]] = []
        self.direct_typing: list[tuple[str, bool]] = []
        self._events_to_yield = events_to_yield or []
        self._fail_send_after_n = fail_send_after_n
        self._closed = closed

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        for ev in self._events_to_yield:
            yield ev
        await asyncio.Event().wait()

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        if self._closed:
            raise RuntimeError("signal client closed")
        self.sends.append((group_id, message))
        self.send_attachments.append((group_id, tuple(attachments)))
        if self._fail_send_after_n is not None and len(self.sends) == self._fail_send_after_n:
            raise RuntimeError("synthetic send failure")
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
    def __init__(
        self,
        *,
        fail: bool = False,
        resume_available: bool = True,
        reply_text: str = "reply",
        prompt_delay: float = 0.0,
        prompt_exception: Exception | None = None,
        new_session_exception: Exception | None = None,
        resume_exception: Exception | None = None,
        gate_started: asyncio.Event | None = None,
        gate_wait: asyncio.Event | None = None,
        gate_first_only: bool = False,
    ) -> None:
        self.profile = "profile"
        self.prompts: list[list[dict[str, Any]]] = []
        self.resumes = 0
        self.new_sessions = 0
        self.new_session_cwds: list[Path] = []
        self.prompt_session_ids: list[str] = []
        self.released_session_ids: list[str] = []
        self.policies: list[tuple[str, StaticPermissionPolicy]] = []
        self.resume_available = resume_available
        self.fail = fail
        self.reply_text = reply_text
        self.prompt_delay = prompt_delay
        self._prompt_exception = prompt_exception
        self._new_session_exception = new_session_exception
        self._resume_exception = resume_exception
        self._gate_started = gate_started
        self._gate_wait = gate_wait
        self._gate_first_only = gate_first_only
        self._gated_once = False

    async def new_session(self, cwd: Path) -> str:
        cwd.mkdir(parents=True, exist_ok=True)
        self.new_sessions += 1
        self.new_session_cwds.append(cwd)
        if self._new_session_exception is not None:
            raise self._new_session_exception
        return f"session-{self.new_sessions}"

    async def resume_session(self, session_id: str, cwd: Path) -> bool:
        self.resumes += 1
        if self._resume_exception is not None:
            raise self._resume_exception
        if not self.resume_available:
            return False
        return True

    def set_permission_policy(self, session_id: str, policy: StaticPermissionPolicy) -> None:
        self.policies.append((session_id, policy))
        return None

    def release_session(self, session_id: str) -> None:
        self.released_session_ids.append(session_id)

    async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
        self.prompt_session_ids.append(session_id)
        self.prompts.append(blocks)
        if self._gate_started is not None:
            self._gate_started.set()
        if self._gate_wait is not None:
            if self._gate_first_only:
                if not self._gated_once:
                    self._gated_once = True
                    await self._gate_wait.wait()
            else:
                await self._gate_wait.wait()
        if self.prompt_delay:
            await asyncio.sleep(self.prompt_delay)
        if self.fail:
            raise RuntimeError("boom")
        if self._prompt_exception is not None:
            raise self._prompt_exception
        return TurnResult(self.reply_text)


class ToolSurfaceProfile(FakeProfile):
    def __init__(self, surface: ToolSurface) -> None:
        super().__init__()
        self._surface = surface

    async def tool_surface(self) -> ToolSurface:
        return self._surface


class BlockingSurfaceProfile(FakeProfile):
    def __init__(
        self,
        surface: ToolSurface,
        *,
        entered: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        super().__init__()
        self._surface = surface
        self._entered = entered
        self._release = release

    async def tool_surface(self) -> ToolSurface:
        if self._entered is not None:
            self._entered.set()
        if self._release is not None:
            await self._release.wait()
        return self._surface


class ClosedAwareSignal(FakeSignal):
    """Fake Signal client whose send path fails once the client is closed."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        if self.closed:
            raise RuntimeError("signal client closed")
        return await super().send_group(group_id, message, attachments=attachments)


class FailBeforeSendSignal(FakeSignal):
    """Fake Signal that fails on send without recording."""

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        raise RuntimeError("fail before send")


class MutableFailSignal(FakeSignal):
    """Fake Signal whose send can be toggled off; when off it fails without recording."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_send = False

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        if self.fail_send:
            raise RuntimeError("mutable fail")
        return await super().send_group(group_id, message, attachments=attachments)


class ToggleFailSignal(FakeSignal):
    """Fake Signal whose send can be toggled on/off dynamically."""

    def __init__(self) -> None:
        super().__init__()
        self.fail = False

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        if self.fail:
            raise RuntimeError("toggle fail")
        return await super().send_group(group_id, message, attachments=attachments)


class ReadingSignal(FakeSignal):
    """Fake Signal that captures attachment body bytes for inspection."""

    def __init__(self) -> None:
        super().__init__()
        self.read_bodies: list[bytes] = []

    async def send_group(
        self,
        group_id: str,
        message: str,
        *,
        attachments: Sequence[str] = (),
    ) -> dict[str, int]:
        for path in attachments:
            self.read_bodies.append(Path(path).read_bytes())
        return await super().send_group(group_id, message, attachments=attachments)


class FakeSupervisor:
    def __init__(
        self,
        profile: FakeProfile,
        *,
        restart_exception: Exception | None = None,
        retire_should_return: bool | None = None,
    ) -> None:
        self.profile = profile
        self.restarts = 0
        self.cached: list[str] = []
        self.retired: list[str] = []
        self._restart_exception = restart_exception
        self._retire_should_return = retire_should_return

    async def get_profile(self, route: Route) -> FakeProfile:
        return self.profile

    async def restart_profile(self, profile_name: str) -> None:
        self.restarts += 1
        if self._restart_exception is not None:
            raise self._restart_exception

    def cached_profile_names(self) -> list[str]:
        return list(self.cached)

    async def retire_profile(
        self,
        profile_name: str,
        *,
        should_retire: Callable[[], bool] | None = None,
    ) -> bool:
        if should_retire is not None and not should_retire():
            return False
        if self._retire_should_return is not None:
            return self._retire_should_return
        if profile_name not in self.cached:
            return False
        self.cached.remove(profile_name)
        self.retired.append(profile_name)
        return True

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


class RouterTestCase(unittest.IsolatedAsyncioTestCase):
    """Base test case providing self.tmp via addCleanup and shared assertion helpers."""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def assertFrozenAttachmentPath(self, raw_path: str, media_root: Path) -> None:
        path = Path(raw_path)
        self.assertEqual(path.name, "attachment.png")
        path.relative_to((media_root / ".outbound").resolve())
        self.assertFalse(path.exists())


def write_test_file(path: Path, body: bytes = b"body") -> Path:
    """Write bytes under a private dir tree, inferring media_root from the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = path.parts
    if "media" in parts:
        index = len(parts) - 1 - list(reversed(parts)).index("media")
        root = Path(*parts[: index + 1])
    else:
        root = path.parent
    ensure_private_dir_tree(root, path.parent)
    write_private_bytes(path, body)
    return path


async def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    """Poll until predicate is true or raise AssertionError on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"wait_until timed out after {timeout}s")


def write_config_pair(
    tmp: str | Path,
    *,
    routes_yaml: str = "routes:\n  - name: default\n    platform: signal\n    group_id: group\n    profile: profile\n    state: active\n",
    router_yaml: str | None = None,
) -> tuple[Path, Path]:
    """Write config.yaml and routes.yaml under tmp, returning both paths.

    The default router YAML uses standard test defaults. For reload tests that
    need custom router fields, pass a complete `router_yaml` string; the
    helper will not attempt to merge overrides into the default.
    """
    tmp_path = Path(tmp)
    config_path = tmp_path / "config.yaml"
    routes_path = tmp_path / "routes.yaml"
    if router_yaml is None:
        router_yaml = (
            "router:\n"
            "  work_root: " + str(tmp_path / "work") + "\n"
            "  state_db: " + str(tmp_path / "state.db") + "\n"
            "  media_root: " + str(tmp_path / "media") + "\n"
            "  signal_attachment_root: " + str(tmp_path / "signal-attachments") + "\n"
            "  signal_base_url: http://127.0.0.1:8080\n"
            "  allow_remote_signal_base_url: false\n"
            "  circuit_breaker:\n"
            "    failures: 3\n"
            "    window_seconds: 60\n"
        )
    config_path.write_text(router_yaml, encoding="utf-8")
    routes_path.write_text(routes_yaml, encoding="utf-8")
    return config_path, routes_path


def make_reload_harness(
    tmp: str | Path,
    *,
    routes_yaml: str = "routes:\n  - name: default\n    platform: signal\n    group_id: group\n    profile: profile\n    state: active\n",
    router_yaml: str | None = None,
    **router_overrides: Any,
) -> tuple[RouterHarness, Path, Path]:
    """Create a harness with config files written and paths registered."""
    config_path, routes_path = write_config_pair(tmp, routes_yaml=routes_yaml, router_yaml=router_yaml)
    harness = make_router_harness(tmp, **router_overrides)
    harness.router.set_config_paths(config_path, routes_path)
    return harness, config_path, routes_path


def make_direct_route(
    *,
    sender_id: str = "sender-uuid",
    sender_number: str | None = "+00000000000",
    state: RouteState = RouteState.ACTIVE,
    session_policy: SessionPolicy = SessionPolicy.PERSISTENT_SENDER,
) -> Route:
    return Route(
        platform="signal",
        chat_type=ChatType.DIRECT,
        sender_id=sender_id,
        sender_number=sender_number,
        profile="profile",
        session_policy=session_policy,
        state=state,
        route_context={"purpose": "synthetic", "route_alias": "direct-test"},
    )


def make_direct_raw(
    *,
    source_uuid: str | None = "sender-uuid",
    source_number: str | None = "+00000000000",
    timestamp: int = 1,
    text: str = "hello direct",
    attachments: list[dict] | None = None,
) -> dict:
    envelope: dict = {
        "timestamp": timestamp,
        "dataMessage": {
            "timestamp": timestamp,
            "message": text,
            "attachments": attachments or [],
        },
    }
    if source_uuid is not None:
        envelope["sourceUuid"] = source_uuid
    if source_number is not None:
        envelope["sourceNumber"] = source_number
        envelope["source"] = source_number
    return {"envelope": envelope, "account": "synthetic-account-number"}


def make_group_raw(
    *,
    group_id: str = "group",
    source_uuid: str = "sender-uuid",
    timestamp: int = 1,
    text: str | None = "hello",
    attachments: list[dict] | None = None,
) -> dict:
    data_message: dict = {
        "timestamp": timestamp,
        "groupInfo": {"groupId": group_id},
        "attachments": attachments or [],
    }
    if text is not None:
        data_message["message"] = text
    return {
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "envelope": {
                "sourceUuid": source_uuid,
                "timestamp": timestamp,
                "dataMessage": data_message,
            }
        },
    }


def make_synthetic_app(
    tmp: str | Path,
    route: Route,
    job: SyntheticRouteJob | None = None,
    *,
    control: RouterControlConfig | None = None,
    notifications: tuple[SyntheticRouteNotification, ...] = (),
    **router_overrides,
) -> AppConfig:
    return AppConfig(
        router=RouterConfig(
            state_db=Path(tmp) / "state.db",
            media_root=Path(tmp) / "media",
            signal_attachment_root=Path(tmp) / "signal-attachments",
            work_root=Path(tmp) / "work",
            control=control or RouterControlConfig(),
            **router_overrides,
        ),
        routes=(route,),
        scheduled_jobs=(
            job
            or SyntheticRouteJob(
                id="daily-agenda",
                route_name=route.name or "agenda-route",
                prompt="Prepare the synthetic daily agenda.",
            ),
        ),
        notifications=notifications,
    )


def record_dedupe_call_threads(store: DedupeStore) -> list[tuple[str, int]]:
    """Wrap the store's statement methods with executing-thread recorders."""
    calls: list[tuple[str, int]] = []
    for name in ("claim", "status", "is_handled", "mark_handled", "release"):
        original = getattr(store, name)

        def recorder(*args: Any, _name: str = name, _original: Any = original) -> Any:
            calls.append((_name, threading.get_ident()))
            return _original(*args)

        setattr(store, name, recorder)
    return calls


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
