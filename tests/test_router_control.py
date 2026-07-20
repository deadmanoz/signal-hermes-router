from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path

from signal_hermes_router.config import (
    Route,
    RouterControlConfig,
    SyntheticRouteNotification,
)
from signal_hermes_router.dedupe import DedupeStore
from signal_hermes_router.models import (
    RouteState,
    SessionPolicy,
    TurnResult,
)
from signal_hermes_router.payloads import encode_control_message
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.preflight import ToolSurface
from signal_hermes_router.router import SignalHermesRouter
from tests.support import (
    FakeProfile,
    FakeSignal,
    FakeSupervisor,
    make_app,
    make_router_harness,
    make_synthetic_app,
    RouterTestCase,
)


class RouterControlTests(RouterTestCase):
    async def test_control_line_maps_requests_to_synthetic_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="group",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            signal = FakeSignal()
            profile = FakeProfile()
            app = make_synthetic_app(
                tmp,
                route,
                notifications=(
                    SyntheticRouteNotification(
                        id="backup-report",
                        route_name="agenda-route",
                        prompt="Summarize the notification payload.",
                    ),
                ),
            )
            router = SignalHermesRouter(
                app,
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            self.assertEqual(
                await router._handle_control_line(b"not-json\n"),
                {"status": "error", "error": "malformed_json"},
            )
            self.assertEqual(
                await router._handle_control_line(b'{"command":"missing"}\n'),
                {"status": "error", "error": "unknown_command"},
            )
            preflight = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )
            self.assertEqual(preflight["status"], "failed")
            self.assertEqual(preflight["expected_permissions_count"], 1)
            self.assertEqual(
                preflight["probe_errors"],
                [
                    {
                        "profile": "profile",
                        "code": "probe_unsupported",
                        "error": "probe_unsupported",
                    }
                ],
            )
            self.assertEqual(preflight["issues"][0]["code"], "probe_unsupported")
            self.assertEqual(preflight["failure"]["code"], "preflight_failed")
            self.assertNotIn("group", json.dumps(preflight, sort_keys=True))
            response = await router._handle_control_line(
                b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000}\n'
            )

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")
            response = await router._handle_control_line(
                b'{"command":"notify_route","notification_id":"backup-report",'
                b'"payload":{"status":"ok"},"idempotency_key":"backup-1"}\n'
            )

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "active")
            self.assertEqual(response["synthetic_id"], "backup-report")
            self.assertEqual(response["synthetic_kind"], "notification")
            self.assertEqual(signal.sends, [("group", "reply"), ("group", "reply")])

    async def test_control_preflight_uses_profile_tool_surface(self) -> None:
        class ToolSurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config(
                    [{"tool": "read_file"}, {"tool": "web_search"}]
                ),
            )
            profile = ToolSurfaceProfile()
            app = make_synthetic_app(tmp, route)
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["probe_errors"], [])
        self.assertEqual(response["missing_tools_count"], 1)
        self.assertEqual(response["missing_tools"][0]["tool"], "web_search")
        self.assertEqual(response["issues"][0]["code"], "missing_tool")
        self.assertEqual(response["failure"]["code"], "permission_denied")
        self.assertNotIn("EXAMPLE_GROUP", json.dumps(response, sort_keys=True))

    async def test_control_preflight_reports_local_tools_exposed_on_mcp_only_route(self) -> None:
        from signal_hermes_router.preflight import ToolSurface

        class LocalToolProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                return ToolSurface.from_names(
                    self.profile,
                    ["web_search", "bash"],
                    schema_version=1,
                    scope="full_callable",
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="mcp-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                mcp_only=True,
            )
            app = make_synthetic_app(tmp, route)
            router = SignalHermesRouter(
                app,
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(LocalToolProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["probe_errors"], [])
        self.assertEqual(response["missing_tools_count"], 0)
        self.assertEqual(response["local_tools_exposed_count"], 1)
        self.assertEqual(response["local_tools_exposed"][0]["tool"], "bash")
        self.assertEqual(response["issues"][0]["code"], "local_tool_exposed")
        self.assertEqual(response["failure"]["code"], "permission_denied")
        self.assertNotIn("EXAMPLE_GROUP", json.dumps(response, sort_keys=True))

    async def test_control_preflight_reports_busy_profile_without_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            lock = router._profile_lock("profile")
            await lock.acquire()
            try:
                response = await router._handle_control_line(
                    b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
                )
            finally:
                lock.release()

        self.assertEqual(response["status"], "failed")
        self.assertEqual(
            response["probe_errors"],
            [
                {
                    "profile": "profile",
                    "code": "probe_profile_busy",
                    "error": "probe_profile_busy",
                }
            ],
        )
        self.assertEqual(response["issues"][0]["code"], "probe_profile_busy")
        self.assertEqual(response["failure"]["code"], "preflight_failed")

    async def test_control_preflight_reports_supervisor_probe_failure_without_leaking(
        self,
    ) -> None:
        class FailingSupervisor:
            async def get_profile(self, _route: Route) -> FakeProfile:
                raise RuntimeError("private-startup-token")

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
                permission_policy=StaticPermissionPolicy.from_config([{"tool": "read_file"}]),
            )
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FailingSupervisor(),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = await router._handle_control_line(
                b'{"command":"preflight_permissions","scope":{"active_only":true}}\n'
            )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(
            response["probe_errors"],
            [{"profile": "profile", "code": "probe_failed", "error": "RuntimeError"}],
        )
        self.assertEqual(response["issues"][0]["code"], "probe_failed")
        self.assertEqual(response["failure"]["code"], "preflight_failed")
        self.assertNotIn("private-startup-token", json.dumps(response, sort_keys=True))

    async def test_route_status_reports_health_without_private_route_values(self) -> None:
        class DetailFailProfile(FakeProfile):
            async def prompt(self, session_id: str, blocks: list[dict]) -> TurnResult:
                self.prompt_session_ids.append(session_id)
                self.prompts.append(blocks)
                raise RuntimeError(
                    "failed for EXAMPLE_STATUS_GROUP at https://signal.test/v1 "
                    "/private/work/session +00000000000 [route_context:begin]"
                )

        with tempfile.TemporaryDirectory() as tmp:
            route = Route(
                platform="signal",
                name="agenda-route",
                group_id="EXAMPLE_STATUS_GROUP",
                profile="profile",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            signal = FakeSignal()
            success_profile = FakeProfile()
            supervisor = FakeSupervisor(success_profile)
            router = SignalHermesRouter(
                make_synthetic_app(tmp, route),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=supervisor,  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            await router.handle_synthetic_job("daily-agenda", scheduled_at=1000)
            supervisor.profile = DetailFailProfile()
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                await router.handle_synthetic_job("daily-agenda", scheduled_at=1001)

            response = await router._handle_control_line(
                b'{"command":"route_status","routes":["agenda-route"]}\n'
            )

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["route_count"], 1)
        route_status = response["routes"][0]
        self.assertEqual(route_status["route_ref"], "route:agenda-route")
        self.assertEqual(route_status["route_state"], "active")
        self.assertEqual(route_status["session"]["cached_sessions"], 1)
        self.assertEqual(route_status["circuit"]["failure_count"], 1)
        self.assertIn("last_success_at_ms", route_status)
        self.assertEqual(route_status["last_failure"]["code"], "unknown")
        serialized = json.dumps(response, sort_keys=True)
        self.assertNotIn("EXAMPLE_STATUS_GROUP", serialized)
        self.assertNotIn("+00000000000", serialized)
        self.assertNotIn("signal.test", serialized)
        self.assertNotIn("/private/work", serialized)
        self.assertNotIn("route_context", serialized)

    async def test_control_line_rejects_invalid_request_fields_and_reports_exceptions(self) -> None:
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
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            cases = (
                (b"[]\n", "malformed_request"),
                (b'{"command":"trigger_job"}\n', "missing_job_id"),
                (b'{"command":"notify_route"}\n', "missing_notification_id"),
                (
                    b'{"command":"notify_route","notification_id":"backup-report"}\n',
                    "missing_payload",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":"not-object-or-array"}\n',
                    "invalid_payload",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":"bad"}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":-1}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":true}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000.9}\n',
                    "invalid_scheduled_at",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","idempotency_key":1}\n',
                    "invalid_idempotency_key",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","idempotency_key":""}\n',
                    "invalid_idempotency_key",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","timeout":"bad"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"trigger_job","job_id":"daily-agenda","timeout":-1}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"nan"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"inf"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"notify_route","notification_id":"backup-report",'
                    b'"payload":{"status":"ok"},"timeout":"-inf"}\n',
                    "invalid_timeout",
                ),
                (
                    b'{"command":"preflight_permissions","scope":{"active_only":"yes"}}\n',
                    "invalid_preflight_scope",
                ),
            )
            for payload, error in cases:
                with self.subTest(error=error):
                    self.assertEqual(
                        await router._handle_control_line(payload),
                        {"status": "error", "error": error},
                    )

            async def fail_trigger(*_args, **_kwargs):
                raise RuntimeError("synthetic")

            router.handle_synthetic_job = fail_trigger  # type: ignore[method-assign]
            with self.assertLogs("signal_hermes_router.router", level="ERROR"):
                response = await router._handle_control_line(
                    b'{"command":"trigger_job","job_id":"daily-agenda"}\n'
                )

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_error")
            self.assertEqual(response["failure"]["code"], "router_error")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"], "router_error")
            self.assertEqual(response["failure"]["code"], "router_error")
            self.assertEqual(response["job_id"], "daily-agenda")
            self.assertEqual(response["synthetic_id"], "daily-agenda")
            self.assertEqual(response["synthetic_kind"], "scheduled_job")

    async def test_control_request_admitted_before_shutdown_is_not_bounced(self) -> None:
        harness = make_router_harness(tempfile.mkdtemp())
        line = b'{"command":"route_status"}'
        # Admission is pinned when _run_control_request schedules the request
        # task: one loop tick lets the outer task run and pin admitted=True,
        # while this task's continuation (FIFO) still runs before the freshly
        # queued request task, so begin_shutdown() lands in between.
        request = asyncio.ensure_future(harness.router._run_control_request(line))
        await asyncio.sleep(0)
        harness.router.begin_shutdown()
        response = await request
        self.assertEqual(response["status"], "ok")
        # A request that arrives after shutdown begins still gets busy, so
        # the caller retries on the replacement router.
        response = await harness.router._run_control_request(line)
        self.assertEqual(response["status"], "busy")
        self.assertEqual(response["error"], "router_shutting_down")

    async def test_control_turn_accepted_under_breaker_keeps_mask_after_reload(self) -> None:
        # The control-socket acceptor pins the breaker override map and
        # config generation when it ACCEPTS a request. The request task may
        # not build its turn until after a reload removed and re-added the
        # route (clearing the override); the admission-time maintenance gate
        # must still apply, exactly as for Signal turns.
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            active_routes = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    maintenance_reply: route under repair
scheduled_jobs:
  - id: j1
    route: r1
    prompt: job prompt
"""
            routes_path.write_text(active_routes, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            harness = make_router_harness(tmp, app=app)
            harness.router.set_config_paths(config_path, routes_path)
            r1 = harness.router.config.find_route_by_name("r1")
            assert r1 is not None
            # The breaker is open when the control request is accepted.
            harness.router.route_state_overrides[r1.key] = RouteState.MAINTENANCE
            harness.router._trip_times[r1.key] = time.monotonic()
            # The accepted turn queues behind the held route lock.
            await harness.router._route_locks[r1.key].acquire()
            request_task = asyncio.ensure_future(
                harness.router._run_control_request(
                    b'{"command":"trigger_job","job_id":"j1","timeout":5}'
                )
            )
            for _ in range(100):
                if harness.router._control_request_tasks:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(harness.router._control_request_tasks)
            # Reload 1 removes the route (clearing the override), reload 2
            # re-adds it ACTIVE — all before the accepted turn runs.
            routes_path.write_text("routes: []\n", encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            self.assertNotIn(r1.key, harness.router.route_state_overrides)
            routes_path.write_text(active_routes, encoding="utf-8")
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            harness.router._route_locks[r1.key].release()
            response = await asyncio.wait_for(request_task, timeout=5)
            self.assertEqual(response["status"], "delivered")
            self.assertEqual(response["route_state"], "maintenance")
            self.assertEqual(harness.profile.prompts, [])
            self.assertEqual(harness.signal.sends, [("group-one", "route under repair")])
            for _ in range(5):
                reapers = [t for t in harness.router._reap_tasks if not t.done()]
                if not reapers:
                    break
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)

    async def test_preflight_probes_use_admission_config_snapshot(self) -> None:
        # The preflight report is computed from one config snapshot, so every
        # per-profile probe must resolve its representative route from that
        # SAME snapshot: a reload swapping self.config mid-preflight must not
        # make a later probe miss its route (probe_profile_missing) or probe
        # a different one.
        p1_entered = asyncio.Event()
        p1_block = asyncio.Event()
        p2_entered = asyncio.Event()

        class BlockingSurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                p1_entered.set()
                await p1_block.wait()
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        class SurfaceProfile(FakeProfile):
            async def tool_surface(self) -> ToolSurface:
                p2_entered.set()
                return ToolSurface.from_names(
                    self.profile,
                    ["read_file"],
                    schema_version=1,
                    scope="full_callable",
                )

        class MappingSupervisor(FakeSupervisor):
            def __init__(self, profiles: dict[str, FakeProfile]) -> None:
                super().__init__(FakeProfile())
                self.profiles = profiles

            async def get_profile(self, route: Route) -> FakeProfile:
                return self.profiles[route.profile]

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            routes_path = Path(tmp) / "routes.yaml"
            config_path.write_text(
                "router:\n"
                "  work_root: " + str(Path(tmp) / "work") + "\n"
                "  state_db: " + str(Path(tmp) / "state.db") + "\n"
                "  media_root: " + str(Path(tmp) / "media") + "\n"
                "  signal_attachment_root: " + str(Path(tmp) / "signal-attachments") + "\n"
                "  signal_base_url: http://127.0.0.1:8080\n"
                "  allow_remote_signal_base_url: false\n",
                encoding="utf-8",
            )
            routes_text = """
routes:
  - name: r1
    platform: signal
    group_id: group-one
    profile: p1
    state: active
    permissions:
      - tool: read_file
  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
    permissions:
      - tool: read_file
"""
            routes_path.write_text(routes_text, encoding="utf-8")
            from signal_hermes_router.config import load_app_config

            app = load_app_config(config_path, routes_path)
            p1 = BlockingSurfaceProfile()
            p1.profile = "p1"
            p2 = SurfaceProfile()
            p2.profile = "p2"
            supervisor = MappingSupervisor({"p1": p1, "p2": p2})
            harness = make_router_harness(tmp, app=app, supervisor=supervisor)
            harness.router.set_config_paths(config_path, routes_path)
            # Nothing is cached, so the reload's retire loop stays inert and
            # only the probe's route resolution is under test.

            preflight_task = asyncio.ensure_future(
                harness.router._run_control_request(b'{"command":"preflight_permissions"}')
            )
            # Profiles probe sequentially in sorted order: p1 parks inside
            # its tool-surface read, so p2's probe has not started yet. Pin
            # that ordering premise: if probing ever becomes concurrent this
            # test would go vacuous instead of failing.
            await asyncio.wait_for(p1_entered.wait(), timeout=5)
            self.assertFalse(p2_entered.is_set())
            # A reload swaps self.config, removing r2 BEFORE p2's probe
            # resolves its representative route.
            routes_path.write_text(
                routes_text.replace(
                    """  - name: r2
    platform: signal
    group_id: group-two
    profile: p2
    state: active
    permissions:
      - tool: read_file
""",
                    "",
                ),
                encoding="utf-8",
            )
            response = await harness.router._handle_reload_config_control({})
            self.assertEqual(response["status"], "ok")
            reapers = [t for t in harness.router._reap_tasks if not t.done()]
            if reapers:
                await asyncio.wait_for(asyncio.gather(*reapers), timeout=5)
            p1_block.set()
            response = await asyncio.wait_for(preflight_task, timeout=5)
            # Both probes resolved their routes from the admission snapshot:
            # p2's probe does not see the swapped config and does not report
            # probe_profile_missing.
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["probe_errors"], [])
            self.assertEqual(response["missing_tools_count"], 0)
            self.assertTrue(p2_entered.is_set())
            executor = harness.router._reload_executor
            if executor is not None:
                executor.shutdown(wait=False)
                harness.router._reload_executor = None
            for abandoned in harness.router._abandoned_reload_executors:
                abandoned.shutdown(wait=False)

    async def test_control_socket_refuses_non_socket_path_and_removes_stale_socket(self) -> None:
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
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            non_socket = Path(tmp) / "work" / "control" / "router.sock"
            non_socket.parent.mkdir(parents=True)
            non_socket.write_text("not a socket", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "not a socket"):
                router._prepare_control_socket(non_socket)

            non_socket.unlink()
            stale = non_socket
            import socket

            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(stale))
            finally:
                sock.close()

            router._prepare_control_socket(stale)
            self.assertFalse(stale.exists())

            with self.assertRaisesRegex(RuntimeError, "must include a private parent"):
                router._prepare_control_socket(Path("control.sock"))
            with self.assertRaisesRegex(RuntimeError, "under router.work_root"):
                router._prepare_control_socket(Path(tmp) / "outside" / "router.sock")

    async def test_control_socket_refuses_live_socket(self) -> None:
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
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
            socket_path.parent.mkdir(parents=True)
            import socket

            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(socket_path))
                sock.listen(1)
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    router._prepare_control_socket(socket_path)
            finally:
                sock.close()

    async def test_control_socket_serves_trigger_job_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
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
                make_synthetic_app(
                    tmp,
                    route,
                    control=RouterControlConfig(enabled=True, socket_path=socket_path),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            server_task = asyncio.create_task(router._run_control_server())
            try:
                for _ in range(50):
                    if socket_path.exists():
                        break
                    await asyncio.sleep(0.01)
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                writer.write(
                    b'{"command":"trigger_job","job_id":"daily-agenda","scheduled_at":1000}\n'
                )
                await writer.drain()
                response = json.loads((await reader.readline()).decode("utf-8"))
                writer.close()
                await writer.wait_closed()
            finally:
                server_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await server_task

            self.assertEqual(response["status"], "delivered")
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertFalse(socket_path.exists())

    async def test_control_socket_serves_notification_and_reports_payload_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "work" / "control" / "router.sock"
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
                make_synthetic_app(
                    tmp,
                    route,
                    control=RouterControlConfig(
                        enabled=True,
                        socket_path=socket_path,
                        max_notification_payload_bytes=64,
                    ),
                    notifications=(
                        SyntheticRouteNotification(
                            id="backup-report",
                            route_name="agenda-route",
                            prompt="Summarize the notification payload.",
                        ),
                    ),
                ),
                signal_client=signal,  # type: ignore[arg-type]
                supervisor=FakeSupervisor(profile),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            async def control_request(request: dict) -> dict:
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                writer.write(encode_control_message(request))
                await writer.drain()
                response = json.loads((await reader.readline()).decode("utf-8"))
                writer.close()
                await writer.wait_closed()
                return response

            server_task = asyncio.create_task(router._run_control_server())
            try:
                for _ in range(50):
                    if socket_path.exists():
                        break
                    await asyncio.sleep(0.01)
                accepted = await control_request(
                    {
                        "command": "notify_route",
                        "notification_id": "backup-report",
                        "payload": {"a": "x" * 56},
                        "idempotency_key": "valid",
                    }
                )
                rejected = await control_request(
                    {
                        "command": "notify_route",
                        "notification_id": "backup-report",
                        "payload": {"a": "x" * 57},
                        "idempotency_key": "oversized",
                    }
                )
            finally:
                server_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await server_task

            self.assertEqual(accepted["status"], "delivered")
            self.assertEqual(rejected, {"status": "error", "error": "payload_too_large"})
            self.assertEqual(signal.sends, [("group", "reply")])
            self.assertFalse(socket_path.exists())

    async def test_control_client_disconnect_before_response_is_swallowed(self) -> None:
        class DisconnectedWriter:
            """Simulates a peer that vanished before the response drain.

            write() stays inert, matching a real transport buffering into a
            dead socket; the failure surfaces at drain(), the site observed
            in production.
            """

            def __init__(self, exc: BaseException) -> None:
                self.exc = exc
                self.closed = False
                self.drain_attempts = 0

            def write(self, data: bytes) -> None:
                pass

            async def drain(self) -> None:
                self.drain_attempts += 1
                raise self.exc

            def close(self) -> None:
                self.closed = True

            async def wait_closed(self) -> None:
                return None

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
                make_synthetic_app(tmp, route),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )
            for exc_type in (ConnectionResetError, BrokenPipeError):
                with self.subTest(exc_type=exc_type.__name__):
                    reader = asyncio.StreamReader()
                    reader.feed_data(b'{"command":"route_status"}\n{"command":"route_status"}\n')
                    reader.feed_eof()
                    writer = DisconnectedWriter(exc_type())
                    with self.assertLogs(
                        "signal_hermes_router.router", level="DEBUG"
                    ) as debug_logs:
                        await router._handle_control_client(
                            reader,
                            writer,  # type: ignore[arg-type]
                        )
                    self.assertIn(
                        "control client disconnected before reading response",
                        "\n".join(debug_logs.output),
                    )
                    # The session loop ends on the first failed response; the
                    # second queued line is never processed.
                    self.assertEqual(writer.drain_attempts, 1)
                    self.assertTrue(writer.closed)

    async def test_route_status_rejects_invalid_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            response = router._route_status_response({"route_indexes": [True]})

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"], "invalid_route_status_scope")

    async def test_route_status_filters_by_route_index_and_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            alpha = Route(
                platform="signal",
                name="alpha",
                group_id="EXAMPLE_ALPHA_GROUP",
                profile="profile-a",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            beta = Route(
                platform="signal",
                name="beta",
                group_id="EXAMPLE_BETA_GROUP",
                profile="profile-b",
                session_policy=SessionPolicy.PERSISTENT_ROUTE,
                state=RouteState.ACTIVE,
            )
            router = SignalHermesRouter(
                make_app(tmp, RouteState.ACTIVE, routes=(alpha, beta)),
                signal_client=FakeSignal(),  # type: ignore[arg-type]
                supervisor=FakeSupervisor(FakeProfile()),  # type: ignore[arg-type]
                dedupe=DedupeStore(),
            )

            by_name = router._route_status_response({"route": "alpha"})
            by_index = router._route_status_response({"route_index": 1})
            by_index_list = router._route_status_response({"route_indexes": [0, 1, 1]})
            by_profile = router._route_status_response({"profiles": ["profile-b"]})
            invalid_string_list = router._route_status_response({"routes": [""]})
            invalid_bool = router._route_status_response({"route_index": True})

        self.assertEqual([route["route_ref"] for route in by_name["routes"]], ["route:alpha"])
        self.assertEqual([route["route_ref"] for route in by_index["routes"]], ["route:beta"])
        self.assertEqual(
            [route["route_ref"] for route in by_index_list["routes"]],
            ["route:alpha", "route:beta"],
        )
        self.assertEqual([route["route_ref"] for route in by_profile["routes"]], ["route:beta"])
        self.assertEqual(invalid_string_list["error"], "invalid_route_status_scope")
        self.assertEqual(invalid_bool["error"], "invalid_route_status_scope")
