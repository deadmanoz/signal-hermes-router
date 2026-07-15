from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from signal_hermes_router import __version__
from signal_hermes_router import acp as acp_module
from signal_hermes_router.acp import (
    ACPProfile,
    JsonRpcError,
    JsonRpcPeerExited,
    JsonRpcStdioPeer,
    STDERR_TAIL_MAX_LINE_CHARS,
    STDERR_TAIL_MAX_LINES,
    _collect_assistant_text,
    default_hermes_command,
)
from signal_hermes_router.permissions import StaticPermissionPolicy
from signal_hermes_router.preflight import PreflightProbeUnavailable
from signal_hermes_router.sessions import SessionRegistry
from tests.support import file_mode, read_file_allow_policy, started_acp_profile


class FakeStdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


class FakeLineReader:
    def __init__(self, lines: list[bytes]) -> None:
        self.lines = list(lines)

    async def readline(self) -> bytes:
        if self.lines:
            return self.lines.pop(0)
        return b""


class ACPTests(unittest.IsolatedAsyncioTestCase):
    def test_default_hermes_command_prefers_current_venv_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = Path(tmp) / "bin"
            bin_dir.mkdir()
            python = bin_dir / "python"
            hermes = bin_dir / "hermes"
            python.write_text("", encoding="utf-8")
            hermes.write_text("", encoding="utf-8")
            with patch.object(acp_module.sys, "executable", str(python)):
                self.assertEqual(
                    default_hermes_command("router-smoke"),
                    [str(hermes), "-p", "router-smoke", "acp"],
                )

    def test_default_hermes_command_falls_back_to_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = Path(tmp) / "bin"
            bin_dir.mkdir()
            python = bin_dir / "python"
            python.write_text("", encoding="utf-8")
            with patch.object(acp_module.sys, "executable", str(python)):
                self.assertEqual(
                    default_hermes_command("router-smoke"),
                    ["hermes", "-p", "router-smoke", "acp"],
                )

    async def test_acp_profile_handles_permission_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            async with started_acp_profile(tmp) as profile:
                session_id = await profile.new_session(Path(tmp) / "session")
                profile.set_permission_policy(session_id, read_file_allow_policy())
                result = await profile.prompt(session_id, [{"type": "text", "text": "hello"}])
                self.assertEqual(result.text, "allowed")
                await profile.resume_session(session_id, Path(tmp) / "session")

    async def test_acp_profile_hands_off_image_and_manifest_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp) / "image.png"
            image.write_bytes(b"synthetic")
            async with started_acp_profile(tmp) as profile:
                session_id = await profile.new_session(Path(tmp) / "session")
                profile.set_permission_policy(session_id, read_file_allow_policy())
                result = await profile.prompt(
                    session_id,
                    [
                        {
                            "type": "resource_link",
                            "name": "image.png",
                            "mimeType": "image/png",
                            "uri": image.as_uri(),
                        },
                        {"type": "text", "text": "attachment_manifest:\n  display_filename: a.pdf"},
                    ],
                )
                self.assertEqual(result.text, "allowed image manifest")

    async def test_concurrent_prompts_do_not_mix_session_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            async with started_acp_profile(tmp, fixture="fake_concurrent_acp_agent.py") as profile:
                session_one = await profile.new_session(Path(tmp) / "session-one")
                session_two = await profile.new_session(Path(tmp) / "session-two")
                first, second = await asyncio.gather(
                    profile.prompt(session_one, [{"type": "text", "text": "one"}]),
                    profile.prompt(session_two, [{"type": "text", "text": "two"}]),
                )
                self.assertEqual(first.text, "reply-session-1")
                self.assertEqual(second.text, "reply-session-2")

    async def test_acp_profile_denies_without_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            async with started_acp_profile(tmp) as profile:
                session_id = await profile.new_session(Path(tmp) / "session")
                with self.assertLogs("signal_hermes_router.acp", level="WARNING"):
                    result = await profile.prompt(session_id, [{"type": "text", "text": "hello"}])
                self.assertEqual(result.text, "denied")

    async def test_acp_profile_start_closes_peer_when_initialize_fails(self) -> None:
        # If `initialize` fails after the subprocess is already spawned, the
        # ACPProfile must close the peer (terminating the subprocess and
        # cancelling the reader/stderr-drain tasks) before re-raising —
        # otherwise the supervisor's cooldown blocks replacement while an
        # orphaned subprocess lingers.
        instances: list[FlakyInitPeer] = []

        class FlakyInitPeer:
            def __init__(self, *_args, **_kwargs) -> None:
                self.started = False
                self.closed = False
                instances.append(self)

            async def start(self) -> None:
                self.started = True

            async def request(self, *_args, **_kwargs):
                raise RuntimeError("initialize failed")

            async def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            with patch.object(acp_module, "JsonRpcStdioPeer", FlakyInitPeer):
                with self.assertRaisesRegex(RuntimeError, "initialize failed"):
                    await profile.start()
            self.assertIsNone(profile.peer, "peer reference cleared on failure")
            self.assertEqual(len(instances), 1)
            self.assertTrue(instances[0].started)
            self.assertTrue(instances[0].closed, "peer.close() must run on initialize failure")

    async def test_acp_profile_initialize_advertises_package_version(self) -> None:
        instances: list[CapturingInitPeer] = []

        class CapturingInitPeer:
            def __init__(self, *_args, **_kwargs) -> None:
                self.initialize_params: dict[str, object] | None = None
                self.closed = False
                instances.append(self)

            async def start(self) -> None:
                return None

            async def request(self, method: str, params: dict[str, object], **_kwargs):
                if method != "initialize":
                    raise AssertionError(f"unexpected method {method}")
                self.initialize_params = params
                return {"agentCapabilities": {}}

            async def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            with patch.object(acp_module, "JsonRpcStdioPeer", CapturingInitPeer):
                await profile.start()
                await profile.close()

        client_info = instances[0].initialize_params["clientInfo"]  # type: ignore[index]
        self.assertEqual(client_info, {"name": "signal-hermes-router", "version": __version__})
        self.assertTrue(instances[0].closed)

    async def test_acp_profile_initialize_uses_configured_timeout(self) -> None:
        instances: list[TimeoutCapturingInitPeer] = []

        class TimeoutCapturingInitPeer:
            def __init__(self, *_args, **_kwargs) -> None:
                self.initialize_timeout: float | None = None
                instances.append(self)

            async def start(self) -> None:
                return None

            async def request(self, method: str, _params: dict[str, object], **kwargs):
                if method != "initialize":
                    raise AssertionError(f"unexpected method {method}")
                self.initialize_timeout = kwargs.get("timeout")
                return {"agentCapabilities": {}}

            async def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(
                profile="synthetic", work_root=Path(tmp), initialize_timeout_seconds=12.5
            )
            with patch.object(acp_module, "JsonRpcStdioPeer", TimeoutCapturingInitPeer):
                await profile.start()
                await profile.close()

        self.assertEqual(instances[0].initialize_timeout, 12.5)

    async def test_acp_profile_start_times_out_when_initialize_hangs(self) -> None:
        # A subprocess that spawns but never answers `initialize` must fail
        # profile startup within the configured initialize timeout (not the
        # 300s JSON-RPC request default) and leave no orphaned peer behind.
        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(
                profile="synthetic",
                work_root=Path(tmp),
                command=[sys.executable, "-c", "import time; time.sleep(10)"],
                initialize_timeout_seconds=0.05,
            )
            with self.assertRaisesRegex(TimeoutError, "ACP initialize timed out after 0.05s"):
                await profile.start()
            self.assertIsNone(profile.peer, "peer reference cleared on initialize timeout")

    async def test_acp_profile_tool_surface_reads_agent_capabilities_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.agent_capabilities = {
                "_meta": {
                    "toolSurface": {
                        "schema_version": 1,
                        "scope": "full_callable",
                        "tools": ["read_file", "web_search"],
                    }
                }
            }

            surface = await profile.tool_surface()

        self.assertEqual(surface.profile, "synthetic")
        self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))
        self.assertEqual(surface.schema_version, 1)
        self.assertEqual(surface.scope, "full_callable")
        self.assertEqual(surface.source, "agent_capabilities_meta")

    async def test_acp_profile_tool_surface_uses_optional_json_rpc_method(self) -> None:
        class ToolSurfacePeer:
            def __init__(self) -> None:
                self.requests: list[tuple[str, float | None]] = []

            async def request(self, method: str, *args, **kwargs):
                self.requests.append((method, kwargs.get("timeout")))
                return {
                    "schema_version": 1,
                    "scope": "full_callable",
                    "tools": ["read_file"],
                }

        with tempfile.TemporaryDirectory() as tmp:
            peer = ToolSurfacePeer()
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = peer  # type: ignore[assignment]

            surface = await profile.tool_surface()

        self.assertEqual(surface.tool_names, frozenset({"read_file"}))
        self.assertEqual(surface.source, "_tool_surface/list")
        self.assertEqual(peer.requests, [("_tool_surface/list", 30.0)])

    async def test_acp_profile_tool_surface_reports_unsupported_optional_method(self) -> None:
        class UnsupportedPeer:
            async def request(self, *_args, **_kwargs):
                raise JsonRpcError({"code": -32601, "message": "method not found"})

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = UnsupportedPeer()  # type: ignore[assignment]

            with self.assertRaisesRegex(PreflightProbeUnavailable, "probe_unsupported"):
                await profile.tool_surface()

    async def test_acp_profile_tool_surface_propagates_json_rpc_probe_errors(self) -> None:
        class FailingPeer:
            async def request(self, *_args, **_kwargs):
                raise JsonRpcError({"code": -32603, "message": "internal error"})

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = FailingPeer()  # type: ignore[assignment]

            with self.assertRaises(JsonRpcError):
                await profile.tool_surface()

    async def test_acp_profile_tool_surface_reports_not_started(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))

            with self.assertRaisesRegex(PreflightProbeUnavailable, "probe_not_started"):
                await profile.tool_surface()

    async def test_acp_profile_tool_surface_accepts_hermes_native_unversioned_response(self) -> None:
        class HermesNativePeer:
            async def request(self, *_args, **_kwargs):
                return {"tools": ["read_file"]}

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = HermesNativePeer()  # type: ignore[assignment]

            surface = await profile.tool_surface()

        self.assertEqual(surface.tool_names, frozenset({"read_file"}))
        self.assertEqual(surface.schema_version, 1)
        self.assertEqual(surface.scope, "full_callable")
        self.assertEqual(surface.source, "_tool_surface/list")

    async def test_acp_profile_tool_surface_normalizes_hermes_native_response(self) -> None:
        class HermesNativePeer:
            async def request(self, *_args, **_kwargs):
                return {"tools": ["read_file", "web_search"]}

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = HermesNativePeer()  # type: ignore[assignment]

            surface = await profile.tool_surface()

        self.assertEqual(surface.tool_names, frozenset({"read_file", "web_search"}))
        self.assertEqual(surface.schema_version, 1)
        self.assertEqual(surface.scope, "full_callable")
        self.assertEqual(surface.source, "_tool_surface/list")

    async def test_acp_profile_tool_surface_accepts_explicit_empty_surface(self) -> None:
        class EmptySurfacePeer:
            async def request(self, *_args, **_kwargs):
                return {"schema_version": 1, "scope": "full_callable", "tools": []}

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = EmptySurfacePeer()  # type: ignore[assignment]

            surface = await profile.tool_surface()

        self.assertEqual(surface.tool_names, frozenset())
        self.assertEqual(surface.source, "_tool_surface/list")

    async def test_acp_profile_tool_surface_rejects_invalid_present_meta_without_fallback(
        self,
    ) -> None:
        class TrackingPeer:
            def __init__(self) -> None:
                self.requested = False

            async def request(self, *_args, **_kwargs):
                self.requested = True
                return {"schema_version": 1, "scope": "full_callable", "tools": []}

        with tempfile.TemporaryDirectory() as tmp:
            peer = TrackingPeer()
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = peer  # type: ignore[assignment]
            profile.agent_capabilities = {"_meta": {"toolSurface": {"tools": ["read_file"]}}}

            with self.assertRaisesRegex(
                PreflightProbeUnavailable, "probe_contract_version_missing"
            ):
                await profile.tool_surface()

        self.assertFalse(peer.requested)

    async def test_acp_profile_tool_surface_rejects_model_facing_extension(self) -> None:
        class ModelFacingPeer:
            async def request(self, *_args, **_kwargs):
                return {
                    "schema_version": 1,
                    "scope": "model_facing",
                    "tools": ["tool_search", "tool_call"],
                }

        with tempfile.TemporaryDirectory() as tmp:
            profile = ACPProfile(profile="synthetic", work_root=Path(tmp))
            profile.peer = ModelFacingPeer()  # type: ignore[assignment]

            with self.assertRaisesRegex(
                PreflightProbeUnavailable, "probe_contract_scope_unsupported"
            ):
                await profile.tool_surface()

    async def test_json_rpc_timeout_cleans_pending_request(self) -> None:
        peer = JsonRpcStdioPeer(
            [
                sys.executable,
                "-c",
                "import sys, time; sys.stderr.write('noise\\n'); sys.stderr.flush(); time.sleep(10)",
            ]
        )
        await peer.start()
        try:
            with self.assertRaises(TimeoutError):
                await peer.request("never", timeout=0.01)
            self.assertEqual(peer._pending, {})
        finally:
            await peer.close()

    async def test_json_rpc_start_merges_environment(self) -> None:
        captured: dict[str, object] = {}

        async def fake_wait() -> int:
            return 0

        async def fake_create_subprocess_exec(*command: str, **kwargs: object):
            captured["command"] = command
            captured["env"] = kwargs["env"]
            return SimpleNamespace(
                stdin=FakeStdin(),
                stdout=FakeLineReader([]),
                stderr=FakeLineReader([]),
                returncode=0,
                wait=fake_wait,
            )

        peer = JsonRpcStdioPeer(["hermes", "acp"], env={"ROUTER_TEST_ENV": "yes"})
        with patch.object(
            acp_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec
        ):
            await peer.start()
            await asyncio.sleep(0)
            await peer.close()

        self.assertEqual(captured["command"], ("hermes", "acp"))
        self.assertEqual(captured["env"]["ROUTER_TEST_ENV"], "yes")  # type: ignore[index]

    async def test_json_rpc_close_kills_process_after_terminate_timeout(self) -> None:
        class SlowProcess:
            returncode = None

            def __init__(self) -> None:
                self.terminated = False
                self.killed = False
                self.waits = 0

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True

            async def wait(self) -> None:
                self.waits += 1

        async def fake_wait_for(awaitable, timeout: float) -> None:
            awaitable.close()
            raise TimeoutError

        process = SlowProcess()
        peer = JsonRpcStdioPeer(["unused"])
        peer.process = process  # type: ignore[assignment]

        with patch.object(acp_module.asyncio, "wait_for", fake_wait_for):
            await peer.close()

        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertEqual(process.waits, 1)

    async def test_json_rpc_request_requires_started_process(self) -> None:
        peer = JsonRpcStdioPeer(["never-started"])
        with self.assertRaisesRegex(RuntimeError, "not started"):
            await peer.request("session/new")

    async def test_json_rpc_response_error_sets_future_exception(self) -> None:
        peer = JsonRpcStdioPeer(["unused"])
        future = asyncio.get_running_loop().create_future()
        peer._pending[7] = future

        peer._handle_response({"jsonrpc": "2.0", "id": 7, "error": {"message": "synthetic"}})

        with self.assertRaises(JsonRpcError) as caught:
            await future
        self.assertEqual(caught.exception.error["message"], "synthetic")

    async def test_json_rpc_response_preserves_falsey_result(self) -> None:
        peer = JsonRpcStdioPeer(["unused"])
        future = asyncio.get_running_loop().create_future()
        peer._pending[7] = future

        peer._handle_response({"jsonrpc": "2.0", "id": 7, "result": []})

        self.assertEqual(await future, [])

    async def test_json_rpc_read_loop_routes_payloads_and_fails_pending_on_exit(self) -> None:
        stdin = FakeStdin()
        peer = JsonRpcStdioPeer(["unused"])
        peer.process = SimpleNamespace(
            stdin=stdin,
            stdout=FakeLineReader(
                [
                    b"{not-json}\n",
                    b'{"jsonrpc": "2.0", "id": 1, "result": {"ok": true}}\n',
                    b'{"jsonrpc": "2.0", "id": 99, "method": "missing", "params": {}}\n',
                    b'{"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1"}}\n',
                    b"",
                ]
            ),
        )  # type: ignore[assignment]
        future_done = asyncio.get_running_loop().create_future()
        future_exited = asyncio.get_running_loop().create_future()
        peer._pending[1] = future_done
        peer._pending[2] = future_exited
        queue = peer.subscribe_session("s1")

        await peer._read_loop()

        self.assertEqual(await future_done, {"ok": True})
        with self.assertRaisesRegex(RuntimeError, "peer exited"):
            await future_exited
        self.assertEqual(queue.get_nowait()["params"]["sessionId"], "s1")
        self.assertEqual(json.loads(stdin.writes[-1])["error"]["code"], -32601)

    async def test_json_rpc_read_loop_skips_oversize_lines_and_keeps_peer_alive(
        self,
    ) -> None:
        # Oversize line followed by a valid response for id 1; the loop must
        # skip the bad line and resolve the pending future from the good one.
        oversize_line = b'{"jsonrpc":"2.0","id":2,"result":{"oversize":' + b"x" * 200 + b"}}\n"
        valid_line = b'{"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n'
        peer = JsonRpcStdioPeer(["unused"], max_line_bytes=80)
        peer.process = SimpleNamespace(
            stdout=FakeLineReader([oversize_line, valid_line, b""]),
        )  # type: ignore[assignment]
        future = asyncio.get_running_loop().create_future()
        peer._pending[1] = future

        with self.assertLogs("signal_hermes_router.acp", level="WARNING") as logs:
            await peer._read_loop()

        self.assertEqual(await future, {"ok": True})
        self.assertIn("exceeded configured line limit", "\n".join(logs.output))

    async def test_drain_stderr_recovers_from_oversize_line(self) -> None:
        # asyncio.StreamReader.readline() converts LimitOverrunError to
        # ValueError internally after clearing the buffer; the fake matches
        # that production behavior rather than the underlying LimitOverrunError.
        class OverrunOnceReader:
            def __init__(self) -> None:
                self.calls = 0

            async def readline(self) -> bytes:
                self.calls += 1
                if self.calls == 1:
                    raise ValueError("Separator is not found, and chunk exceed the limit")
                if self.calls == 2:
                    return b"ok\n"
                return b""

        stderr = OverrunOnceReader()
        peer = JsonRpcStdioPeer(["unused"])
        peer.process = SimpleNamespace(stderr=stderr)  # type: ignore[assignment]

        with self.assertLogs("signal_hermes_router.acp", level="WARNING") as logs:
            await peer._drain_stderr()

        # The loop continued past the ValueError, read the subsequent line,
        # and then EOF.
        self.assertEqual(stderr.calls, 3)
        self.assertIn("stderr line exceeded", "\n".join(logs.output))

    async def test_drain_stderr_handles_real_subprocess_oversize_line(self) -> None:
        # Real-subprocess integration test: spawn a process that writes a
        # large stderr line without a newline followed by an exit. With a
        # tight max_line_bytes, asyncio.StreamReader.readline() will raise
        # ValueError; _drain_stderr must recover and not deadlock.
        peer = JsonRpcStdioPeer(
            [
                sys.executable,
                "-c",
                "import sys; sys.stderr.write('x' * 5000); sys.stderr.flush()",
            ],
            max_line_bytes=128,
        )
        with self.assertLogs("signal_hermes_router.acp", level="WARNING") as logs:
            await peer.start()
            try:
                assert peer._stderr_task is not None
                # The drain task should complete (process exits after write).
                # Wait for it without using close() so we genuinely exercise
                # the natural-exit path through _drain_stderr.
                await asyncio.wait_for(peer._stderr_task, timeout=5.0)
            finally:
                await peer.close()
        self.assertIn("stderr line exceeded", "\n".join(logs.output))

    async def test_json_rpc_client_requests_return_success_and_errors(self) -> None:
        async def ok(params: dict) -> dict:
            return {"echo": params["value"]}

        async def fail(_params: dict) -> dict:
            raise RuntimeError("handler failed")

        stdin = FakeStdin()
        peer = JsonRpcStdioPeer(["unused"], request_handlers={"ok": ok, "fail": fail})
        peer.process = SimpleNamespace(stdin=stdin)  # type: ignore[assignment]

        await peer._handle_request(
            {"jsonrpc": "2.0", "id": 1, "method": "ok", "params": {"value": 3}}
        )
        self.assertEqual(
            json.loads(stdin.writes[-1]), {"jsonrpc": "2.0", "id": 1, "result": {"echo": 3}}
        )

        await peer._handle_request({"jsonrpc": "2.0", "id": 2, "method": "missing"})
        self.assertEqual(json.loads(stdin.writes[-1])["error"]["code"], -32601)

        await peer._handle_request({"jsonrpc": "2.0", "id": 3, "method": "fail"})
        response = json.loads(stdin.writes[-1])
        self.assertEqual(response["error"]["code"], -32603)
        self.assertEqual(response["error"]["message"], "handler failed")

    async def test_json_rpc_session_notifications_route_to_subscribers(self) -> None:
        peer = JsonRpcStdioPeer(["unused"])
        queue = peer.subscribe_session("session-1")

        peer._handle_notification({"method": "ignored"})
        peer._handle_notification({"method": "session/update", "params": {}})
        peer._handle_notification(
            {"method": "session/update", "params": {"sessionId": "other-session"}}
        )
        peer._handle_notification(
            {
                "method": "session/update",
                "params": {
                    "sessionId": "session-1",
                    "update": {"type": "text", "text": "hello"},
                },
            }
        )

        self.assertEqual(queue.get_nowait()["params"]["sessionId"], "session-1")

    async def test_acp_profile_close_resume_release_and_unsupported_edges(self) -> None:
        profile = ACPProfile(profile="synthetic", work_root=Path("/tmp"))
        await profile.close()
        profile.release_session("missing")

        peer = SimpleNamespace(
            requests=[],
            subscribed=[],
            async_request=None,
            close=lambda: None,
        )

        async def request(method: str, params: dict) -> dict:
            peer.requests.append((method, params))
            return {}

        def subscribe_session(session_id: str) -> None:
            peer.subscribed.append(session_id)

        def release_session(session_id: str) -> None:
            peer.released = session_id

        peer.request = request
        peer.subscribe_session = subscribe_session
        peer.release_session = release_session
        profile.peer = peer  # type: ignore[assignment]

        self.assertFalse(await profile.resume_session("session-1", Path("/tmp/session")))
        profile.release_session("session-1")
        self.assertEqual(peer.released, "session-1")
        with self.assertRaisesRegex(RuntimeError, "does not expose"):
            await profile._unsupported_client_request({})

    async def test_acp_session_directories_are_private(self) -> None:
        class Peer:
            def subscribe_session(self, _session_id: str) -> None:
                return None

            async def request(self, method: str, _params: dict) -> dict:
                if method == "session/new":
                    return {"sessionId": "session-1"}
                return {}

        with tempfile.TemporaryDirectory() as tmp:
            work_root = Path(tmp) / "work"
            cwd = work_root / "profiles" / "synthetic" / "sessions" / "session-key"
            profile = ACPProfile(
                profile="synthetic",
                work_root=work_root,
                peer=Peer(),  # type: ignore[arg-type]
            )

            await profile.new_session(cwd)

            self.assertEqual(file_mode(work_root), 0o700)
            self.assertEqual(file_mode(cwd.parent), 0o700)
            self.assertEqual(file_mode(cwd), 0o700)

    async def test_acp_profile_prompt_uses_text_result_fallback(self) -> None:
        class Peer:
            def __init__(self) -> None:
                self.queue: asyncio.Queue[dict] = asyncio.Queue()

            def subscribe_session(self, session_id: str) -> asyncio.Queue[dict]:
                return self.queue

            async def request(self, method: str, params: dict, *, timeout: float = 300.0) -> dict:
                return {"text": "fallback text", "stopReason": "complete"}

        profile = ACPProfile(profile="synthetic", work_root=Path("/tmp"), peer=Peer())  # type: ignore[arg-type]
        result = await profile.prompt("session-1", [{"type": "text", "text": "hello"}])

        self.assertEqual(result.text, "fallback text")
        self.assertEqual(result.stop_reason, "complete")

    async def test_acp_profile_prompt_keeps_streamed_text_over_result_text(self) -> None:
        class Peer:
            def __init__(self) -> None:
                self.queue: asyncio.Queue[dict] = asyncio.Queue()

            def subscribe_session(self, session_id: str) -> asyncio.Queue[dict]:
                return self.queue

            async def request(self, method: str, params: dict, *, timeout: float = 300.0) -> dict:
                await self.queue.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionId": "session-1",
                            "update": {
                                "session_update": "agent_message_chunk",
                                "content": {
                                    "type": "text",
                                    "text": "streamed complete final answer",
                                },
                            },
                        },
                    }
                )
                return {"text": "partial answer", "stopReason": "complete"}

        profile = ACPProfile(profile="synthetic", work_root=Path("/tmp"), peer=Peer())  # type: ignore[arg-type]
        result = await profile.prompt("session-1", [{"type": "text", "text": "hello"}])

        self.assertEqual(result.text, "streamed complete final answer")
        self.assertEqual(result.stop_reason, "complete")

    def test_collect_assistant_text_handles_nested_updates(self) -> None:
        text = _collect_assistant_text(
            [
                {"method": "ignored"},
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session-1",
                        "update": [
                            {"type": "text", "text": " hello"},
                            {"nested": {"type": "text", "text": " world "}},
                        ],
                    },
                },
            ]
        )
        self.assertEqual(text, "hello world")

    def test_collect_assistant_text_ignores_thought_chunks(self) -> None:
        text = _collect_assistant_text(
            [
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session-1",
                        "update": {
                            "sessionUpdate": "agent_thought_chunk",
                            "content": {"type": "text", "text": "private reasoning"},
                        },
                    },
                },
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session-1",
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": " public answer "},
                        },
                    },
                },
            ]
        )
        self.assertEqual(text, "public answer")

    async def test_acp_profile_release_session_clears_local_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            async with started_acp_profile(tmp) as profile:
                session_id = await profile.new_session(Path(tmp) / "session")
                profile.set_permission_policy(session_id, StaticPermissionPolicy())
                await profile.prompt(session_id, [{"type": "text", "text": "hello"}])
                self.assertIn(session_id, profile.permission_policies)
                self.assertIn(session_id, profile.prompt_locks)
                assert profile.peer is not None
                self.assertIn(session_id, profile.peer._session_updates)

                profile.release_session(session_id)
                self.assertNotIn(session_id, profile.permission_policies)
                self.assertNotIn(session_id, profile.prompt_locks)
                self.assertNotIn(session_id, profile.peer._session_updates)


class SessionRegistryTests(unittest.TestCase):
    def test_session_cwd_is_confined_to_work_root_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            work_root = Path(tmp) / "work"
            registry = SessionRegistry(work_root, supervisor=object())  # type: ignore[arg-type]
            cwd = registry._cwd("profile-01", "session-key")
            profiles_root = (work_root / "profiles").resolve(strict=False)
            cwd.resolve(strict=False).relative_to(profiles_root)

            with self.assertRaises(ValueError):
                registry._cwd("../../outside", "session-key")


class PeerCloseCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_kills_process_when_cancelled_during_terminate_grace(self) -> None:
        wait_gate = asyncio.Event()

        class FakeProcess:
            def __init__(self) -> None:
                self.returncode: int | None = None
                self.terminated = False
                self.killed = False

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9

            async def wait(self) -> int | None:
                await wait_gate.wait()
                return self.returncode

        peer = JsonRpcStdioPeer(["unused"])
        process = FakeProcess()
        peer.process = process  # type: ignore[assignment]

        close_task = asyncio.create_task(peer.close())
        for _ in range(10):
            await asyncio.sleep(0)
        self.assertTrue(process.terminated)
        self.assertFalse(process.killed)

        # Cancellation during the terminate grace must still kill the child:
        # no exit path of close() may leave a running subprocess.
        close_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close_task
        self.assertTrue(process.killed)

    async def test_close_leaves_exited_process_alone_when_cancelled(self) -> None:
        wait_gate = asyncio.Event()

        class FakeProcess:
            def __init__(self) -> None:
                self.returncode: int | None = None
                self.kill_calls = 0

            def terminate(self) -> None:
                return None

            def kill(self) -> None:
                self.kill_calls += 1
                self.returncode = -9

            async def wait(self) -> int | None:
                await wait_gate.wait()
                return self.returncode

        peer = JsonRpcStdioPeer(["unused"])
        process = FakeProcess()
        peer.process = process  # type: ignore[assignment]

        close_task = asyncio.create_task(peer.close())
        for _ in range(10):
            await asyncio.sleep(0)
        # The child exits on its own during the grace wait.
        process.returncode = 0
        close_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close_task
        self.assertEqual(process.kill_calls, 0)


class PeerExitWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_peer_exit_watcher_reports_returncode_and_stderr_tail(self) -> None:
        exits: list[tuple[int | None, tuple[str, ...]]] = []
        peer = JsonRpcStdioPeer(
            [
                sys.executable,
                "-c",
                "import sys; sys.stderr.write('boom-detail\\n'); sys.stderr.flush(); sys.exit(3)",
            ],
            on_exit=lambda returncode, tail: exits.append((returncode, tail)),
        )
        await peer.start()
        try:
            for _ in range(200):
                if exits:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(len(exits), 1)
            returncode, tail = exits[0]
            self.assertEqual(returncode, 3)
            self.assertIn("boom-detail", tail)
        finally:
            await peer.close()

    async def test_peer_close_suppresses_exit_callback(self) -> None:
        exits: list[tuple[int | None, tuple[str, ...]]] = []
        peer = JsonRpcStdioPeer(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            on_exit=lambda returncode, tail: exits.append((returncode, tail)),
        )
        await peer.start()
        await peer.close()
        for _ in range(5):
            await asyncio.sleep(0)
        self.assertEqual(exits, [])

    async def test_peer_close_reports_exit_of_already_dead_child(self) -> None:
        # Crash-then-close race: the child died on its own and was reaped,
        # but the watcher task has not had CPU yet when close() is called
        # (recovery reacts to the failed request first). close() must hand
        # the exit report off to the watcher before suppressing it.
        exits: list[tuple[int | None, tuple[str, ...]]] = []

        class DeadProcess:
            returncode = -9

            async def wait(self) -> int:
                return -9

        peer = JsonRpcStdioPeer(
            ["unused"],
            on_exit=lambda returncode, tail: exits.append((returncode, tail)),
        )
        peer.process = DeadProcess()  # type: ignore[assignment]
        peer._exit_watcher_task = asyncio.create_task(peer._watch_exit())

        await peer.close()

        self.assertEqual(exits, [(-9, ())])

    async def test_peer_close_reports_crash_discovered_by_failed_request(self) -> None:
        # The production incident shape: the child is dead, the router only
        # notices when the next request fails, and recovery immediately
        # closes the peer -- without ever awaiting process.wait() first.
        exits: list[tuple[int | None, tuple[str, ...]]] = []
        peer = JsonRpcStdioPeer(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            on_exit=lambda returncode, tail: exits.append((returncode, tail)),
        )
        await peer.start()
        assert peer.process is not None
        peer.process.kill()
        with self.assertRaises((JsonRpcPeerExited, ConnectionResetError, BrokenPipeError)):
            await peer.request("ping", timeout=5)
        await peer.close()

        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0][0], -9)

    async def test_watcher_cleans_up_stale_pipe_tasks_when_grandchild_holds_stderr(self) -> None:
        # A grandchild inheriting the child's pipes holds EOF open past the
        # child's death. The watcher must still report the exit after the
        # settle bound and must cancel the drain tasks so an evicted peer
        # cannot leak permanently-pending tasks.
        script = (
            "import os, subprocess, sys\n"
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(5)'])\n"
            "sys.stderr.write('dying\\n')\n"
            "sys.stderr.flush()\n"
            "os._exit(7)\n"
        )
        exits: list[tuple[int | None, tuple[str, ...]]] = []
        peer = JsonRpcStdioPeer(
            [sys.executable, "-c", script],
            on_exit=lambda returncode, tail: exits.append((returncode, tail)),
        )
        await peer.start()
        try:
            for _ in range(300):
                if exits:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(len(exits), 1)
            self.assertEqual(exits[0][0], 7)
            self.assertIn("dying", exits[0][1])
            assert peer._reader_task is not None and peer._stderr_task is not None
            self.assertTrue(peer._reader_task.done())
            self.assertTrue(peer._stderr_task.done())
            # The router-side pipe fds are released even though the grandchild
            # still holds the child-side ends: an evicted peer must retain no
            # OS resources, and a stdin-reading grandchild must see EOF.
            assert peer.process is not None and peer.process.stdin is not None
            self.assertTrue(peer.process.stdin.transport.is_closing())
        finally:
            await peer.close()

    async def test_peer_close_cancelled_during_exit_handoff_still_kills_child(self) -> None:
        # Cancellation while close() waits for the watcher handoff (live
        # child with stdout-EOF evidence) must still reach the kill backstop:
        # no exit path of close() may leave a running subprocess.
        wait_gate = asyncio.Event()

        class LiveProcess:
            def __init__(self) -> None:
                self.returncode: int | None = None
                self.killed = False

            def terminate(self) -> None:
                return None

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9

            async def wait(self) -> int | None:
                await wait_gate.wait()
                return self.returncode

        peer = JsonRpcStdioPeer(["unused"])
        process = LiveProcess()
        peer.process = process  # type: ignore[assignment]
        # Evidence without a reaped exit: the child closed its own stdout.
        peer._stdout_eof = True
        peer._exit_watcher_task = asyncio.create_task(peer._watch_exit())

        close_task = asyncio.create_task(peer.close())
        for _ in range(10):
            await asyncio.sleep(0)
        self.assertFalse(close_task.done(), "close() must be parked in the handoff wait")

        close_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close_task
        self.assertTrue(process.killed, "kill backstop must run despite handoff cancellation")
        wait_gate.set()
        assert peer._exit_watcher_task is not None
        await asyncio.wait_for(peer._exit_watcher_task, timeout=5)

    async def test_peer_exit_callback_failure_does_not_break_close(self) -> None:
        def bad_callback(_returncode: int | None, _tail: tuple[str, ...]) -> None:
            raise RuntimeError("callback boom")

        peer = JsonRpcStdioPeer(
            [sys.executable, "-c", "import sys; sys.exit(1)"],
            on_exit=bad_callback,
        )
        with self.assertLogs("signal_hermes_router.acp", level="WARNING") as logs:
            await peer.start()
            assert peer._exit_watcher_task is not None
            await asyncio.wait_for(peer._exit_watcher_task, timeout=5)
        await peer.close()
        self.assertIn("exit callback failed", "\n".join(logs.output))

    async def test_read_loop_eof_sets_stdout_exit_evidence(self) -> None:
        peer = JsonRpcStdioPeer(["unused"])
        peer.process = SimpleNamespace(  # type: ignore[assignment]
            stdout=FakeLineReader([]), returncode=None
        )

        await peer._read_loop()

        self.assertTrue(peer._stdout_eof)
        self.assertTrue(peer.exit_evidence())

    async def test_write_frame_broken_pipe_sets_stdin_exit_evidence(self) -> None:
        class BrokenStdin:
            def write(self, _data: bytes) -> None:
                raise ConnectionResetError

            async def drain(self) -> None:
                return None

        peer = JsonRpcStdioPeer(["unused"])
        peer.process = SimpleNamespace(stdin=BrokenStdin(), returncode=None)  # type: ignore[assignment]

        with self.assertRaises(ConnectionResetError):
            await peer._write_frame({"jsonrpc": "2.0"})

        self.assertTrue(peer._stdin_write_failed)
        self.assertTrue(peer.exit_evidence())

    async def test_drain_stderr_bounds_tail_lines_and_line_length(self) -> None:
        lines = [f"line-{index}\n".encode() for index in range(STDERR_TAIL_MAX_LINES + 5)]
        lines.append(b"x" * (STDERR_TAIL_MAX_LINE_CHARS + 100) + b"\n")
        lines.append(b"")
        peer = JsonRpcStdioPeer(["unused"])
        peer.process = SimpleNamespace(stderr=FakeLineReader(lines))  # type: ignore[assignment]

        await peer._drain_stderr()

        tail = peer.stderr_tail()
        self.assertEqual(len(tail), STDERR_TAIL_MAX_LINES)
        self.assertEqual(tail[-1], "x" * STDERR_TAIL_MAX_LINE_CHARS)
        self.assertNotIn("line-0", tail)
        self.assertIn(f"line-{STDERR_TAIL_MAX_LINES + 4}", tail)

    def test_acp_profile_notify_exit_forwards_and_guards_none(self) -> None:
        profile = ACPProfile(profile="synthetic", work_root=Path("/tmp"))
        profile._notify_exit(1, ("ignored",))  # no callback registered: no-op

        seen: list[tuple[int | None, tuple[str, ...]]] = []
        profile.on_exit = lambda returncode, tail: seen.append((returncode, tail))
        profile._notify_exit(2, ("boom",))

        self.assertEqual(seen, [(2, ("boom",))])


if __name__ == "__main__":
    unittest.main()
