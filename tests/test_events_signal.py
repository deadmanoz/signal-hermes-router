from __future__ import annotations

import asyncio
import base64
import json
import unittest
from unittest.mock import patch

import httpx

from signal_hermes_router import signal as signal_module
from signal_hermes_router.events import (
    inspect_signal_event,
    parse_signal_event,
    probe_signal_route,
    probe_routeability,
    summarize_signal_event,
)
from signal_hermes_router.models import ChatType
from signal_hermes_router.signal import SignalHttpClient, _iter_sse_json


class FakeSseResponse:
    def __init__(self, lines: list[str]) -> None:
        self.lines = lines

    async def aiter_lines(self):
        for line in self.lines:
            yield line


class FakeAsyncByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


class EventTests(unittest.TestCase):
    def test_parse_signal_group_event(self) -> None:
        raw = {
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "envelope": {
                    "sourceUuid": "sender-uuid",
                    "timestamp": 1714521600000,
                    "dataMessage": {
                        "message": "hello",
                        "groupInfo": {"groupId": "group-id"},
                        "attachments": [
                            {
                                "contentType": "text/plain",
                                "filename": "a.txt",
                                "data": base64.b64encode(b"body").decode("ascii"),
                            }
                        ],
                    },
                }
            },
        }
        event = parse_signal_event(raw)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.group_id, "group-id")
        self.assertEqual(event.dedupe_key, ("sender-uuid", 1714521600000))
        self.assertEqual(event.attachments[0].body, b"body")

    def test_parse_signal_group_event_rejects_oversize_inline_attachment(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "dataMessage": {
                    "message": "hello",
                    "groupInfo": {"groupId": "group-id"},
                    "attachments": [
                        {
                            "contentType": "text/plain",
                            "filename": "a.txt",
                            "data": base64.b64encode(b"body").decode("ascii"),
                        }
                    ],
                },
            }
        }
        with self.assertRaises(ValueError):
            parse_signal_event(raw, max_attachment_bytes=3)

    def test_parse_signal_direct_sse_group_event(self) -> None:
        raw = {
            "envelope": {
                "source": "synthetic-source-number",
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "dataMessage": {
                    "timestamp": 1714521600000,
                    "message": "synthetic direct SSE text",
                    "groupInfo": {
                        "groupId": "group-id=",
                        "groupName": "synthetic",
                        "revision": 4,
                        "type": "DELIVER",
                    },
                },
            },
            "account": "synthetic-account-number",
        }
        event = parse_signal_event(raw)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.group_id, "group-id=")
        self.assertEqual(event.sender_id, "sender-uuid")
        self.assertEqual(event.dedupe_key, ("sender-uuid", 1714521600000))
        self.assertEqual(event.text, "synthetic direct SSE text")

    def test_parse_signal_direct_data_message(self) -> None:
        raw = {
            "envelope": {
                "source": "+00000000000",
                "sourceUuid": "sender-uuid",
                "sourceNumber": "+00000000000",
                "timestamp": 1714521600000,
                "dataMessage": {
                    "timestamp": 1714521600000,
                    "message": "synthetic direct text",
                    "attachments": [
                        {
                            "contentType": "text/plain",
                            "filename": "a.txt",
                            "data": base64.b64encode(b"body").decode("ascii"),
                        }
                    ],
                },
            },
            "account": "synthetic-account-number",
        }

        event = parse_signal_event(raw)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.chat_type, ChatType.DIRECT)
        self.assertIsNone(event.group_id)
        self.assertEqual(event.sender_id, "sender-uuid")
        self.assertEqual(event.source_uuid, "sender-uuid")
        self.assertEqual(event.source_number, "+00000000000")
        self.assertEqual(event.dedupe_key, ("sender-uuid", 1714521600000))
        self.assertEqual(event.text, "synthetic direct text")
        self.assertEqual(event.attachments[0].body, b"body")

    def test_parse_signal_direct_data_message_without_uuid_keeps_number_for_dedupe(
        self,
    ) -> None:
        raw = {
            "envelope": {
                "source": "+00000000000",
                "sourceNumber": "+00000000000",
                "timestamp": 1714521600000,
                "dataMessage": {"message": "synthetic direct text"},
            },
            "account": "synthetic-account-number",
        }

        event = parse_signal_event(raw)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.chat_type, ChatType.DIRECT)
        self.assertIsNone(event.source_uuid)
        self.assertEqual(event.source_number, "+00000000000")
        self.assertEqual(event.sender_id, "+00000000000")
        self.assertEqual(event.dedupe_key, ("+00000000000", 1714521600000))

    def test_parse_signal_direct_sync_message_is_not_a_direct_data_message(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1714521600000,
                        "message": "linked-device direct text",
                    }
                },
            },
            "account": "account",
        }

        self.assertIsNone(parse_signal_event(raw))

    def test_summarize_signal_event_is_content_free(self) -> None:
        raw = {
            "envelope": {
                "source": "synthetic-source-number",
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "typingMessage": {"action": "STARTED", "timestamp": 1714521600000},
            },
            "account": "synthetic-account-number",
        }
        summary = summarize_signal_event(raw)
        self.assertEqual(summary, "shape=direct message_type=typingMessage has_group=false")
        self.assertNotIn("synthetic-source-number", summary)
        self.assertNotIn("sender-uuid", summary)

    def test_inspect_signal_event_returns_structured_content_free_summary(self) -> None:
        raw = {
            "envelope": {
                "source": "synthetic-source-number",
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "receiptMessage": {"when": 1714521600000},
            },
            "account": "synthetic-account-number",
        }
        summary = inspect_signal_event(raw)
        self.assertEqual(summary.shape, "direct")
        self.assertEqual(summary.message_type, "receiptMessage")
        self.assertFalse(summary.has_group)
        self.assertNotIn("synthetic-source-number", str(summary))
        self.assertNotIn("sender-uuid", str(summary))

    def test_probe_routeability_returns_group_id_without_parsing_content(self) -> None:
        raw = {
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "envelope": {
                    "sourceUuid": "sender-uuid",
                    "timestamp": 1714521600000,
                    "dataMessage": {
                        "message": "secret text",
                        "groupInfo": {"groupId": "group-id"},
                        "attachments": [{"filename": "secret.txt", "data": "c2VjcmV0"}],
                    },
                }
            },
        }
        group_id, summary = probe_routeability(raw)
        self.assertEqual(group_id, "group-id")
        self.assertEqual(summary.shape, "jsonrpc")
        self.assertEqual(summary.message_type, "dataMessage")
        self.assertTrue(summary.has_group)
        summary_str = str(summary)
        self.assertNotIn("group-id", summary_str)
        self.assertNotIn("secret", summary_str)
        self.assertNotIn("sender-uuid", summary_str)

    def test_probe_routeability_returns_sync_message_group_id(self) -> None:
        raw = {
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "envelope": {
                    "sourceUuid": "sender-uuid",
                    "timestamp": 1714521600000,
                    "syncMessage": {
                        "sentMessage": {
                            "message": "secret linked-device text",
                            "groupInfo": {"groupId": "group-id="},
                            "attachments": [{"filename": "secret.txt", "data": "c2VjcmV0"}],
                        }
                    },
                }
            },
        }
        group_id, summary = probe_routeability(raw)
        self.assertEqual(group_id, "group-id=")
        self.assertEqual(summary.shape, "jsonrpc")
        self.assertEqual(summary.message_type, "syncMessage")
        self.assertTrue(summary.has_group)
        summary_str = str(summary)
        self.assertNotIn("group-id=", summary_str)
        self.assertNotIn("secret", summary_str)
        self.assertNotIn("sender-uuid", summary_str)

    def test_probe_routeability_returns_edit_message_group_id(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "editMessage": {
                    "targetSentTimestamp": 1714521500000,
                    "dataMessage": {
                        "message": "edited secret",
                        "groupInfo": {"groupId": "group-id="},
                    },
                },
            },
            "account": "account",
        }
        group_id, summary = probe_routeability(raw)
        self.assertEqual(group_id, "group-id=")
        self.assertEqual(summary.shape, "direct")
        self.assertEqual(summary.message_type, "editMessage")
        self.assertTrue(summary.has_group)
        self.assertNotIn("group-id=", str(summary))
        self.assertNotIn("edited secret", str(summary))

    def test_probe_routeability_returns_sync_edit_message_group_id(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
                "syncMessage": {
                    "sentMessage": {
                        "destination": None,
                        "editMessage": {
                            "targetSentTimestamp": 1714521500000,
                            "dataMessage": {
                                "message": "linked-device edited secret",
                                "groupInfo": {"groupId": "group-id="},
                            },
                        },
                    }
                },
            },
            "account": "account",
        }
        group_id, summary = probe_routeability(raw)
        self.assertEqual(group_id, "group-id=")
        self.assertEqual(summary.shape, "direct")
        self.assertEqual(summary.message_type, "syncMessage")
        self.assertTrue(summary.has_group)
        self.assertNotIn("group-id=", str(summary))
        self.assertNotIn("linked-device edited secret", str(summary))

    def test_probe_routeability_marks_receive_exception_without_payload(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1714521600000,
            },
            "exception": {
                "message": "private exception detail",
                "type": "RuntimeException",
            },
            "account": "account",
        }
        group_id, summary = probe_routeability(raw)
        self.assertIsNone(group_id)
        self.assertEqual(summary.shape, "direct")
        self.assertEqual(summary.message_type, "unknown")
        self.assertFalse(summary.has_group)
        self.assertTrue(summary.has_exception)
        summary_str = str(summary)
        self.assertIn("has_exception=true", summary_str)
        self.assertNotIn("private exception detail", summary_str)
        self.assertNotIn("sender-uuid", summary_str)

    def test_probe_routeability_returns_none_for_non_group_event(self) -> None:
        raw = {
            "envelope": {
                "sourceUuid": "sender-uuid",
                "timestamp": 1,
                "dataMessage": {"message": "secret direct payload"},
            },
            "account": "account",
        }
        group_id, summary = probe_routeability(raw)
        self.assertIsNone(group_id)
        self.assertEqual(summary.shape, "direct")
        self.assertEqual(summary.message_type, "dataMessage")
        self.assertFalse(summary.has_group)

    def test_probe_signal_route_returns_direct_facts_without_leaking_summary(self) -> None:
        raw = {
            "envelope": {
                "source": "+00000000000",
                "sourceUuid": "sender-uuid",
                "sourceNumber": "+00000000000",
                "timestamp": 1,
                "dataMessage": {"message": "direct message"},
            },
            "account": "account",
        }

        probe = probe_signal_route(raw)

        self.assertIsNone(probe.group_id)
        self.assertEqual(probe.source_uuid, "sender-uuid")
        self.assertEqual(probe.source_number, "+00000000000")
        self.assertTrue(probe.is_direct_data_message)
        summary = str(probe.summary)
        self.assertEqual(summary, "shape=direct message_type=dataMessage has_group=false")
        self.assertNotIn("sender-uuid", summary)
        self.assertNotIn("+00000000000", summary)
        self.assertNotIn("secret direct payload", summary)

    def test_probe_routeability_returns_none_for_unknown_shape(self) -> None:
        raw = {"not": "a signal envelope"}
        group_id, summary = probe_routeability(raw)
        self.assertIsNone(group_id)
        self.assertEqual(summary.shape, "unknown")
        self.assertEqual(summary.message_type, "none")
        self.assertFalse(summary.has_group)


class SignalHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_iter_sse_json_handles_multiline_and_trailing_data(self) -> None:
        response = FakeSseResponse(
            [
                ": keepalive",
                "event: message",
                'data: {"a":',
                "data: 1}",
                "",
                'data: {"b": 2}',
            ]
        )
        events = [event async for event in _iter_sse_json(response)]  # type: ignore[arg-type]
        self.assertEqual(events, [{"a": 1}, {"b": 2}])

    async def test_iter_sse_json_rejects_oversize_events(self) -> None:
        response = FakeSseResponse(['data: {"message": "too long"}', ""])

        with self.assertRaisesRegex(ValueError, "max_signal_event_bytes"):
            [event async for event in _iter_sse_json(response, max_event_bytes=10)]  # type: ignore[arg-type]

    async def test_iter_sse_json_skips_malformed_frame_and_continues(self) -> None:
        response = FakeSseResponse(
            [
                'data: {"message": "synthetic-not-json',
                "",
                'data: {"message": "valid"}',
                "",
            ]
        )

        with self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs:
            events = [event async for event in _iter_sse_json(response)]  # type: ignore[arg-type]

        self.assertEqual(events, [{"message": "valid"}])
        output = "\n".join(logs.output)
        self.assertIn("Skipping malformed Signal SSE frame", output)
        self.assertNotIn("synthetic-not-json", output)

    async def test_iter_sse_json_skips_malformed_trailing_frame(self) -> None:
        response = FakeSseResponse(['data: {"message": "valid"}', "", "data: synthetic-trailing"])

        with self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs:
            events = [event async for event in _iter_sse_json(response)]  # type: ignore[arg-type]

        self.assertEqual(events, [{"message": "valid"}])
        output = "\n".join(logs.output)
        self.assertIn("Skipping malformed Signal SSE frame", output)
        self.assertNotIn("synthetic-trailing", output)

    async def test_iter_sse_json_skips_undecodable_frame_variants(self) -> None:
        # json.loads failures beyond JSONDecodeError: a plain ValueError for
        # integers over the interpreter digit limit and RecursionError for
        # deeply nested payloads. Raise each explicitly so the except clause
        # is exercised regardless of interpreter limits. Both must skip the
        # frame, not end the stream.
        response = FakeSseResponse(
            ["data: synthetic-a", "", "data: synthetic-b", "", "data: synthetic-valid", ""]
        )

        with (
            patch.object(
                signal_module.json,
                "loads",
                side_effect=[ValueError("synthetic"), RecursionError(), {"message": "valid"}],
            ),
            self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs,
        ):
            events = [event async for event in _iter_sse_json(response)]  # type: ignore[arg-type]

        self.assertEqual(events, [{"message": "valid"}])
        self.assertEqual(
            len([line for line in logs.output if "Skipping malformed Signal SSE frame" in line]),
            2,
        )

    async def test_iter_sse_json_skips_non_dict_frame(self) -> None:
        response = FakeSseResponse(["data: 5", "", 'data: {"message": "valid"}', ""])

        with self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs:
            events = [event async for event in _iter_sse_json(response)]  # type: ignore[arg-type]

        self.assertEqual(events, [{"message": "valid"}])
        self.assertIn("Skipping malformed Signal SSE frame", "\n".join(logs.output))

    async def test_check_and_send_group_rpc_shape(self) -> None:
        requests: list[dict] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v1/check":
                return httpx.Response(200)
            payload = json.loads(request.content.decode("utf-8"))
            requests.append(payload)
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": 1}}
            )

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        try:
            self.assertTrue(await client.check())
            result = await client.send_group("group-id", "reply")
            await client.send_direct("sender-uuid", "direct reply")
            await client.send_typing("group-id", True)
            await client.send_typing("group-id", False)
            await client.send_typing_direct("sender-uuid", True)
            await client.send_typing_direct("sender-uuid", False)
            self.assertEqual(result["timestamp"], 1)
            self.assertEqual(requests[0]["method"], "send")
            self.assertEqual(requests[0]["params"], {"groupId": "group-id", "message": "reply"})
            self.assertEqual(requests[1]["method"], "send")
            self.assertEqual(
                requests[1]["params"],
                {"recipient": ["sender-uuid"], "message": "direct reply"},
            )
            self.assertEqual(requests[2]["method"], "sendTyping")
            self.assertEqual(requests[2]["params"], {"groupId": "group-id"})
            self.assertEqual(requests[3]["method"], "sendTyping")
            self.assertEqual(requests[3]["params"], {"groupId": "group-id", "stop": True})
            self.assertEqual(requests[4]["method"], "sendTyping")
            self.assertEqual(requests[4]["params"], {"recipient": ["sender-uuid"]})
            self.assertEqual(requests[5]["method"], "sendTyping")
            self.assertEqual(requests[5]["params"], {"recipient": ["sender-uuid"], "stop": True})
        finally:
            await client.close()

    async def test_send_rpc_shape_includes_attachments_when_present(self) -> None:
        requests: list[dict] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            requests.append(payload)
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": 1}}
            )

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        try:
            await client.send_group("group-id", "reply", attachments=("/private/media/a.png",))
            await client.send_direct("sender-uuid", "direct", attachments=("/private/media/b.png",))
        finally:
            await client.close()

        self.assertEqual(requests[0]["method"], "send")
        self.assertEqual(
            requests[0]["params"],
            {
                "groupId": "group-id",
                "message": "reply",
                "attachments": ["/private/media/a.png"],
            },
        )
        self.assertEqual(requests[1]["method"], "send")
        self.assertEqual(
            requests[1]["params"],
            {
                "recipient": ["sender-uuid"],
                "message": "direct",
                "attachments": ["/private/media/b.png"],
            },
        )

    async def test_rpc_error_payload_raises(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "error": {"code": -1, "message": "synthetic"},
                },
            )

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        try:
            with self.assertRaises(RuntimeError):
                await client.rpc("send", {"groupId": "group"})
        finally:
            await client.close()

    async def test_events_yields_sse_payloads(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/api/v1/events")
            return httpx.Response(
                200,
                stream=FakeAsyncByteStream([b'data: {"message": "hello"}\n\n']),
            )

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        stream = client.events(reconnect_delay=999)
        try:
            self.assertEqual(await anext(stream), {"message": "hello"})
        finally:
            await stream.aclose()
            await client.close()

    async def test_events_skips_malformed_frame_without_reconnect(self) -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(
                200,
                stream=FakeAsyncByteStream(
                    [b"data: synthetic-not-json\n\n", b'data: {"message": "valid"}\n\n']
                ),
            )

        async def fail_sleep(_delay: float) -> None:
            raise AssertionError("unexpected reconnect")

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        stream = client.events(reconnect_delay=0.5)
        try:
            with (
                patch.object(signal_module.asyncio, "sleep", fail_sleep),
                self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs,
            ):
                event = await asyncio.wait_for(anext(stream), 5)
            self.assertEqual(event, {"message": "valid"})
            self.assertEqual(calls, 1)
            output = "\n".join(logs.output)
            self.assertIn("Skipping malformed Signal SSE frame", output)
            self.assertNotIn("reconnecting", output)
        finally:
            await stream.aclose()
            await client.close()

    async def test_events_logs_stream_end_before_reconnect_sleep(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=FakeAsyncByteStream([]))

        async def cancel_sleep(_delay: float) -> None:
            raise asyncio.CancelledError

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        stream = client.events(reconnect_delay=0.5)
        try:
            with (
                patch.object(signal_module.random, "uniform", return_value=0),
                patch.object(signal_module.asyncio, "sleep", cancel_sleep),
                self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs,
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await anext(stream)
            self.assertIn("Signal event stream ended", "\n".join(logs.output))
        finally:
            await stream.aclose()
            await client.close()

    async def test_events_logs_transport_failure_before_reconnect_sleep(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down", request=request)

        async def cancel_sleep(_delay: float) -> None:
            raise asyncio.CancelledError

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        stream = client.events(reconnect_delay=0.5)
        try:
            with (
                patch.object(signal_module.random, "uniform", return_value=0),
                patch.object(signal_module.asyncio, "sleep", cancel_sleep),
                self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs,
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await anext(stream)
            output = "\n".join(logs.output)
            self.assertIn("Signal event stream failed", output)
            self.assertIn("ConnectError", output)
        finally:
            await stream.aclose()
            await client.close()

    async def test_send_group_retries_on_transient_request_error(self) -> None:
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError("synthetic", request=request)
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": 7}},
            )

        async def fast_sleep(_delay: float) -> None:
            return None

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        try:
            with (
                patch.object(signal_module.asyncio, "sleep", fast_sleep),
                self.assertLogs("signal_hermes_router.signal", level="WARNING") as logs,
            ):
                result = await client.send_group("group", "reply")
            self.assertEqual(attempts, 2)
            self.assertEqual(result, {"timestamp": 7})
            self.assertIn("ConnectError", "\n".join(logs.output))
        finally:
            await client.close()

    async def test_send_group_does_not_retry_on_signal_cli_error_payload(self) -> None:
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "error": {"code": -1, "message": "rejected"},
                },
            )

        client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
        try:
            with self.assertRaises(RuntimeError):
                await client.send_group("group", "reply")
            # signal-cli rejection is a definitive failure — no retry.
            self.assertEqual(attempts, 1)
        finally:
            await client.close()

    async def test_events_client_uses_separate_httpx_client(self) -> None:
        client = SignalHttpClient("http://test")
        try:
            self.assertIsNot(client._client, client._events_client)
            # Events client's read timeout must outlast the short-RPC default
            # so signal-cli's ~15s keepalive cadence does not trigger a
            # reconnect cycle on quiet routes.
            events_read = client._events_client.timeout.read
            short_read = client._client.timeout.read
            self.assertGreater(events_read, short_read)
        finally:
            await client.close()

    async def test_send_group_does_not_retry_on_ambiguous_post_send_errors(self) -> None:
        # signal-cli's `send` is not idempotent. Any error where the daemon
        # may have already accepted the message must NOT be retried, or the
        # router could duplicate Signal replies. This covers ReadTimeout,
        # WriteError, and RemoteProtocolError — all httpx.RequestError
        # subclasses but all post-send-ambiguous.
        for synth_exc in (
            httpx.ReadTimeout("synthetic read timeout"),
            httpx.WriteError("synthetic write error"),
            httpx.RemoteProtocolError("synthetic remote protocol error"),
        ):
            with self.subTest(exc=type(synth_exc).__name__):
                attempts = 0

                async def handler(
                    request: httpx.Request, exc: Exception = synth_exc
                ) -> httpx.Response:
                    nonlocal attempts
                    attempts += 1
                    raise exc

                client = SignalHttpClient("http://test", transport=httpx.MockTransport(handler))
                try:
                    with self.assertRaises(type(synth_exc)):
                        await client.send_group("group", "reply")
                    self.assertEqual(
                        attempts,
                        1,
                        f"{type(synth_exc).__name__} must not be retried "
                        "(send may be non-idempotent)",
                    )
                finally:
                    await client.close()


if __name__ == "__main__":
    unittest.main()
