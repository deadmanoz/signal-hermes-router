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
    JsonRpcStdioPeer,
    _collect_assistant_text,
    default_hermes_command,
)
from signal_hermes_router.permissions import StaticPermissionPolicy
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

        async def fake_create_subprocess_exec(*command: str, **kwargs: object):
            captured["command"] = command
            captured["env"] = kwargs["env"]
            return SimpleNamespace(
                stdin=FakeStdin(),
                stdout=FakeLineReader([]),
                stderr=FakeLineReader([]),
                returncode=0,
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


if __name__ == "__main__":
    unittest.main()
