from __future__ import annotations

import asyncio
import hashlib
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from signal_hermes_router.acp import JsonRpcError
from signal_hermes_router.config import (
    AppConfig,
    CircuitBreakerConfig,
    Route,
    RouterConfig,
    SyntheticRouteJob,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.failures import FailureCode
from signal_hermes_router.models import (
    RouteState,
    SessionPolicy,
    TurnResult,
    TurnOutcomeStatus,
)
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_event,
    make_route,
    make_synthetic_app,
    RouterTestCase,
)


class RouterSyntheticTests(RouterTestCase):
    async def test_synthetic_route_states_match_signal_gates(self) -> None:
        for state in (RouteState.SHADOW, RouteState.DISABLED, RouteState.MAINTENANCE):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                route = Route(
                    platform="signal",
                    name="agenda-route",
                    group_id="group",
                    profile="profile",
                    session_policy=SessionPolicy.PERSISTENT_ROUTE,
                    state=state,
                )
                signal = FakeSignal()
                profile = FakeProfile()
                router = SignalHermesRouter(
                    make_synthetic_app(tmp, route),
                    signal_client=signal,  # type: ignore[arg-type]
                    supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                    dedupe=DedupeStore(),
                )

                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

                self.assertEqual(profile.prompts, [])
                self.assertNotIn("last_failure_at_ms", outcome.to_control_response())
                if state == RouteState.MAINTENANCE:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.DELIVERED)
                    self.assertEqual(
                        signal.sends,
                        [("group", "This route is temporarily under maintenance.")],
                    )
                else:
                    self.assertEqual(outcome.status, TurnOutcomeStatus.SKIPPED)
                    self.assertEqual(signal.sends, [])

    async def test_synthetic_maintenance_send_failure_reports_metadata(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.MAINTENANCE,
            )
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600456,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = outcome.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["route_state"], "maintenance")
            self.assertEqual(response["failure"]["code"], "signal_send_failed")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600456)
            self.assertEqual(profile.prompts, [])

    async def test_synthetic_unknown_job_and_missing_route_return_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    SyntheticRouteJob(
                        id="orphan-job",
                        route_name="missing-route",
                        prompt="Synthetic orphan prompt.",
                    ),
                ),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            unknown = await router.handle_synthetic_job("missing-job")
            orphan = await router.handle_synthetic_job("orphan-job")

            self.assertEqual(unknown.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(unknown.error, "unknown_job")
            self.assertEqual(orphan.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(orphan.error, "unknown_route")

    async def test_synthetic_job_reuses_persistent_route_session_with_signal_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_event(make_event(timestamp=1))
            await router.handle_synthetic_job("daily-agenda", scheduled_at=2)

            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1"])
            self.assertEqual(profile.new_sessions, 1)

    async def test_synthetic_permission_override_does_not_leak_to_signal_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/route"}}}]
            )
            job_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/job"}}}]
            )
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=route_policy,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    SyntheticRouteJob(
                        id="daily-agenda",
                        route_name="agenda-route",
                        prompt="Prepare the synthetic daily agenda.",
                        permission_policy=job_policy,
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-agenda", scheduled_at=1)
            self.assertEqual(
                profile.policies, [("session-1", job_policy), ("session-1", route_policy)]
            )
            await router.handle_event(make_event(timestamp=2))

            self.assertEqual(
                profile.policies,
                [
                    ("session-1", job_policy),
                    ("session-1", route_policy),
                    ("session-1", route_policy),
                ],
            )

    async def test_synthetic_job_on_mcp_only_route_enforces_local_tool_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="mcp-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config(
                    [{"tool": "read_file"}], mcp_only=True
                ),
                mcp_only=True,
            )
            job = SyntheticRouteJob(
                id="daily-job",
                route_name="mcp-route",
                prompt="Run daily task",
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "bash"}]),
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route, job),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-job", scheduled_at=1)
            # The job policy (policies[0]) should have been upgraded to mcp_only=True
            self.assertTrue(profile.policies[0][1].mcp_only)
            # Verify the job policy actually rejects a local tool
            self.assertFalse(profile.policies[0][1].allows_tool_call({"toolName": "bash"}))

    async def test_synthetic_failure_recovery_resets_replacement_session_policy(self) -> None:
        class RestartingSupervisor(FakeSupervisor):
            def __init__(self, initial: FakeProfile, replacement: FakeProfile) -> None:
                super().__init__(initial)
                self.replacement = replacement

            async def restart_profile(self, profile_name: str) -> None:
                await super().restart_profile(profile_name)
                self.profile = self.replacement

        with tempfile.TemporaryDirectory() as tmp:
            route_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/route"}}}]
            )
            job_policy = StaticPermissionPolicy.from_config(
                [{"tool": "read_file", "arguments": {"path": {"prefix": "/job"}}}]
            )
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=route_policy,
            )
            job = SyntheticRouteJob(
                id="daily-agenda",
                route_name="agenda-route",
                prompt="Prepare the synthetic daily agenda.",
                permission_policy=job_policy,
            )
            initial = FakeProfile()
            initial.fail = True
            replacement = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route, job),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=RestartingSupervisor(initial, replacement),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                outcome = await router.handle_synthetic_job("daily-agenda", scheduled_at=1)

            self.assertEqual(outcome.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(
                replacement.policies,
                [("session-1", job_policy), ("session-1", route_policy)],
            )

    async def test_synthetic_scheduled_at_and_idempotency_key_are_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 2000,
            )

            first = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            duplicate = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            keyed_first = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="stable-fire",
            )
            keyed_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1002,
                idempotency_key="stable-fire",
            )
            scheduled_duplicate_with_new_key = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="changed-fire-id",
            )

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(keyed_first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(keyed_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(scheduled_duplicate_with_new_key.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(len(signal.sends), 2)

    async def test_synthetic_job_keeps_released_dedupe_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            dedupe = DedupeStore()
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=dedupe,
                clock_ms=lambda: 2000,
            )
            key_hash = hashlib.sha256(b"stable-fire").hexdigest()[:16]
            dedupe.mark_handled(route.key, "scheduled:daily-agenda", 1000)
            dedupe.mark_handled(
                route.key,
                f"scheduled:daily-agenda:key:{key_hash}",
                0,
            )

            scheduled_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1000,
            )
            keyed_duplicate = await router.handle_synthetic_job(
                "daily-agenda",
                scheduled_at=1001,
                idempotency_key="stable-fire",
            )

            self.assertEqual(scheduled_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(keyed_duplicate.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(profile.prompts, [])
            self.assertEqual(signal.sends, [])

    async def test_synthetic_hermes_failure_releases_dedupe_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            profile.fail = False
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(failed.error, "unknown")
            self.assertEqual(failed.to_control_response()["failure"]["code"], "unknown")
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(
                [send[1] for send in signal.sends],
                ["I hit an internal router error handling that message.", "reply"],
            )

    async def test_synthetic_session_model_failure_uses_model_reply_and_metadata(self) -> None:
        class SessionFailProfile(FakeProfile):
            async def new_session(self, cwd: Path) -> str:
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "session failed",
                        "data": {
                            "code": "quota_exceeded",
                            "provider_class": "cloud_api",
                            "provider_detail": "429 usage_limit_reached",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    model_failure_reply="The model is unavailable right now.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(SessionFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600123,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "model_rate_limited")
            self.assertEqual(response["failure"]["code"], "model_rate_limited")
            self.assertEqual(response["failure"]["provider_class"], "cloud_api")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600123)
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "The model is unavailable right now.")])

    async def test_synthetic_session_text_only_quota_failure_uses_generic_reply(self) -> None:
        class SessionFailProfile(FakeProfile):
            async def new_session(self, cwd: Path) -> str:
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "OpenAI Codex HTTP 429 usage_limit_reached",
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="Generic router fallback.",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(SessionFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["error"], "acp_session_failed")
            self.assertEqual(response["failure"]["code"], "acp_session_failed")
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Generic router fallback.")])

    async def test_synthetic_maintenance_retry_does_not_mark_dedupe_handled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    circuit_breaker=CircuitBreakerConfig(failures=1, window_seconds=60),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            maintenance = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(maintenance.status, TurnOutcomeStatus.DELIVERED)
            self.assertIsNone(router.dedupe.status("signal:group", "scheduled:daily-agenda", 1000))

            router.route_state_overrides.pop("signal:group", None)
            router._trip_times.pop("signal:group", None)
            router._trip_times_ms.pop("signal:group", None)
            profile.fail = False
            retried = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(retried.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(profile.prompt_session_ids, ["session-1", "session-1"])

    async def test_synthetic_send_failure_returns_error_and_marks_dedupe_handled(self) -> None:
        class FlakySignal(FakeSignal):
            def __init__(self) -> None:
                super().__init__()
                self.fail_send = True

            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                if self.fail_send:
                    raise RuntimeError("send failed")
                return await super().send_group(group_id, message)

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FlakySignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            signal.fail_send = False
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(failed.error, "signal_send_failed")
            response = failed.to_control_response()
            self.assertEqual(response["failure"]["code"], "signal_send_failed")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertIsInstance(response["last_failure_at_ms"], int)
            self.assertEqual(retry.status, TurnOutcomeStatus.DEDUPED)
            self.assertEqual(len(profile.prompts), 1)
            self.assertEqual(signal.sends, [])

    async def test_synthetic_failure_reply_send_failure_preserves_root_failure(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            profile = FakeProfile()
            profile.fail = True
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertFalse(response["reply_sent"])
            self.assertEqual(response["failure"]["code"], "unknown")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertIsInstance(response["last_failure_at_ms"], int)
            self.assertEqual(
                router._route_status_response({})["routes"][0]["last_failure"]["code"],
                "unknown",
            )

    async def test_synthetic_model_failure_with_empty_route_failure_reply_suppresses(self) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "quota exceeded",
                        "data": {
                            "code": "model_rate_limited",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                failure_reply="",
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    failure_reply="Router fallback should not be sent.",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.status, TurnOutcomeStatus.ERROR)
            self.assertEqual(
                failed.to_control_response()["failure"]["code"],
                "model_rate_limited",
            )
            self.assertFalse(failed.to_control_response()["reply_sent"])
            self.assertEqual(signal.sends, [])

    async def test_synthetic_model_failure_trip_uses_maintenance_reply_and_metadata(
        self,
    ) -> None:
        class ModelFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict[str, Any]]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise JsonRpcError(
                    {
                        "code": -32000,
                        "message": "quota exceeded",
                        "data": {
                            "code": "model_rate_limited",
                            "provider_class": "cloud_api",
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            router = SignalHermesRouter(
                make_synthetic_app(
                    tmp,
                    route,
                    circuit_breaker=CircuitBreakerConfig(failures=1, window_seconds=60),
                    maintenance_reply="Maintenance reply.",
                    failure_reply="Generic router fallback.",
                    model_failure_reply="Model fallback should not be sent.",
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(ModelFailProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 1714521600123,
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            response = failed.to_control_response()
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["failure"]["code"], "model_rate_limited")
            self.assertEqual(response["route_ref"], "route:agenda-route")
            self.assertEqual(response["profile"], "profile")
            self.assertEqual(response["last_failure_at_ms"], 1714521600123)
            self.assertTrue(response["reply_sent"])
            self.assertEqual(signal.sends, [("group", "Maintenance reply.")])
            self.assertEqual(router.route_state_overrides["signal:group"], RouteState.MAINTENANCE)

    async def test_synthetic_bare_manual_triggers_are_fresh_with_same_clock(self) -> None:
        nonces = iter(("nonce-one", "nonce-two"))
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
                clock_ms=lambda: 2000,
                nonce_factory=lambda: next(nonces),
            )

            first = await router.handle_synthetic_job("daily-agenda")
            second = await router.handle_synthetic_job("daily-agenda")

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(second.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)
            self.assertEqual(len(signal.sends), 2)

    async def test_synthetic_busy_does_not_leave_dedupe_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            lock = router._route_lock(route)
            await lock.acquire()
            try:
                busy = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            finally:
                lock.release()

            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 1)

    async def test_synthetic_trigger_contends_with_in_flight_signal_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            profile = FakeProfile()
            profile.prompt_delay = 0.05
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            signal_task = asyncio.create_task(router.handle_event(make_event(timestamp=1)))
            try:
                for _ in range(50):
                    if profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                busy = await router.handle_synthetic_job(
                    "daily-agenda",
                    scheduled_at=2,
                    route_lock_timeout=0,
                )
            finally:
                await signal_task

            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=2)

            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)

    async def test_synthetic_triggers_on_distinct_routes_share_profile_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            agenda = Route(
                platform="signal",
                name="agenda-route",
                group_id="group-one",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            summary = Route(
                platform="signal",
                name="summary-route",
                group_id="group-two",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            profile = FakeProfile()
            profile.prompt_delay = 0.05
            router = SignalHermesRouter(
                AppConfig(
                    router=RouterConfig(
                        state_db=Path(tmp) / "state.db",
                        media_root=Path(tmp) / "media",
                        signal_attachment_root=Path(tmp) / "signal-attachments",
                        work_root=Path(tmp) / "work",
                    ),
                    routes=(agenda, summary),
                    scheduled_jobs=(
                        SyntheticRouteJob(
                            id="daily-agenda",
                            route_name="agenda-route",
                            prompt="Prepare the synthetic daily agenda.",
                        ),
                        SyntheticRouteJob(
                            id="daily-summary",
                            route_name="summary-route",
                            prompt="Prepare the synthetic daily summary.",
                        ),
                    ),
                ),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            first_task = asyncio.create_task(
                router.handle_synthetic_job("daily-agenda", scheduled_at=1)
            )
            try:
                for _ in range(50):
                    if profile.prompts:
                        break
                    await asyncio.sleep(0.001)
                busy = await router.handle_synthetic_job(
                    "daily-summary",
                    scheduled_at=2,
                    route_lock_timeout=0,
                )
            finally:
                first = await first_task
            retry = await router.handle_synthetic_job(
                "daily-summary",
                scheduled_at=2,
                route_lock_timeout=0,
            )

            self.assertEqual(first.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(busy.status, TurnOutcomeStatus.BUSY)
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(len(profile.prompts), 2)

    async def test_synthetic_blank_reply_releases_dedupe_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(name="agenda-route")
            signal = FakeSignal()
            profile = FakeProfile()
            profile.reply_text = ""
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            profile.reply_text = "reply"
            retry = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.error, "acp_empty_response")
            self.assertEqual(retry.status, TurnOutcomeStatus.DELIVERED)
            self.assertEqual(
                [body for _, body in signal.sends],
                [
                    "I hit an internal router error handling that message.",
                    "reply",
                ],
            )

    async def test_synthetic_blank_reply_send_failure_preserves_acp_failure(self) -> None:
        class FlakySignal(FakeSignal):
            async def send_group(
                self, group_id: str, message: str, *, attachments: Sequence[str] = ()
            ) -> dict[str, int]:
                raise RuntimeError("send failed")

        with tempfile.TemporaryDirectory() as tmp:
            route = make_route(name="agenda-route")
            profile = FakeProfile()
            profile.reply_text = ""
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FlakySignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                failed = await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)

            self.assertEqual(failed.error, "acp_empty_response")
            self.assertEqual(failed.failure.code, FailureCode.ACP_EMPTY_RESPONSE)
            self.assertEqual(
                router._route_status_response({})["routes"][0]["last_failure"]["code"],
                "acp_empty_response",
            )

