from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from signal_hermes_router.acp import JsonRpcError, JsonRpcPeerExited
from signal_hermes_router.failures import (
    FailureCode,
    ProviderClass,
    classify_exception,
    failure_info,
    preflight_failure_from_report,
    sanitize_detail,
)


class FailureClassificationTests(unittest.TestCase):
    def test_structured_json_rpc_error_data_drives_public_code(self) -> None:
        failure = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "provider said no",
                    "data": {
                        "code": "quota_exceeded",
                        "provider_class": "cloud_api",
                        "provider_detail": "429 quota exceeded for token secret",
                    },
                }
            ),
            redactor=lambda text: text.replace("secret", "[redacted]"),
        )

        self.assertEqual(failure.code, FailureCode.MODEL_RATE_LIMITED)
        self.assertEqual(failure.provider_class, ProviderClass.CLOUD_API)
        self.assertIn("429 quota exceeded", failure.to_dict()["provider_detail"])
        self.assertIn("[redacted]", failure.to_dict()["provider_detail"])
        self.assertNotIn("error", failure.to_dict())

    def test_context_code_preserves_structured_provider_detail(self) -> None:
        failure = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "session failed",
                    "data": {
                        "code": "model_auth_failed",
                        "provider_class": "cloud_api",
                        "provider_detail": (
                            "expired token sk-abc123abc123abc123abc123abc123abc123"
                        ),
                    },
                }
            ),
            context=FailureCode.ACP_SESSION_FAILED,
        )

        self.assertEqual(failure.code, FailureCode.ACP_SESSION_FAILED)
        self.assertEqual(failure.provider_class, ProviderClass.CLOUD_API)
        self.assertIn("expired token [token]", failure.to_dict()["provider_detail"])

    def test_provider_class_is_not_inferred_from_text(self) -> None:
        failure = classify_exception(
            RuntimeError("local endpoint connection refused at http://provider.invalid:11434")
        )

        self.assertEqual(failure.code, FailureCode.ENDPOINT_UNREACHABLE)
        self.assertEqual(failure.provider_class, ProviderClass.UNKNOWN)
        self.assertIn("[url]", failure.to_dict()["provider_detail"])
        self.assertNotIn("provider.invalid", failure.to_dict()["provider_detail"])

    def test_asyncio_timeout_error_is_acp_prompt_timeout(self) -> None:
        failure = classify_exception(asyncio.TimeoutError())

        self.assertEqual(failure.code, FailureCode.ACP_PROMPT_TIMEOUT)
        self.assertEqual(failure.provider_class, ProviderClass.UNKNOWN)

    def test_os_error_is_endpoint_unreachable(self) -> None:
        failure = classify_exception(ConnectionRefusedError("refused by 192.168.1.10:11434"))

        self.assertEqual(failure.code, FailureCode.ENDPOINT_UNREACHABLE)
        self.assertIn("[ip]", failure.to_dict()["provider_detail"])
        self.assertNotIn("192.168.1.10", failure.to_dict()["provider_detail"])

    def test_broken_acp_pipe_is_subprocess_failure(self) -> None:
        failure = classify_exception(BrokenPipeError("ACP stdin pipe closed"))

        self.assertEqual(failure.code, FailureCode.ACP_SUBPROCESS_FAILED)
        self.assertEqual(failure.provider_class, ProviderClass.UNKNOWN)

    def test_context_code_for_non_json_rpc_exception(self) -> None:
        failure = classify_exception(
            RuntimeError("session setup failed"),
            context=FailureCode.ACP_SESSION_FAILED,
        )

        self.assertEqual(failure.code, FailureCode.ACP_SESSION_FAILED)
        self.assertEqual(failure.provider_class, ProviderClass.UNKNOWN)
        self.assertIn("session setup failed", failure.to_dict()["provider_detail"])

    def test_context_code_for_peer_exit_preserves_sanitized_detail(self) -> None:
        failure = classify_exception(
            JsonRpcPeerExited("session setup failed at localhost:11434"),
            context=FailureCode.ACP_SESSION_FAILED,
        )

        self.assertEqual(failure.code, FailureCode.ACP_SESSION_FAILED)
        self.assertEqual(failure.provider_class, ProviderClass.UNKNOWN)
        self.assertIn("[host]", failure.to_dict()["provider_detail"])
        self.assertNotIn("localhost", failure.to_dict()["provider_detail"])

    def test_structured_provider_timeout_is_model_timeout(self) -> None:
        failure = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "timed out",
                    "data": {
                        "code": "model_timeout",
                        "provider_class": "cloud_api",
                        "detail": "provider timeout after 30s",
                    },
                }
            )
        )

        self.assertEqual(failure.code, FailureCode.MODEL_TIMEOUT)
        self.assertEqual(failure.provider_class, ProviderClass.CLOUD_API)

    def test_structured_failure_code_alias_and_local_endpoint_provider_class(self) -> None:
        failure = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "local endpoint failed",
                    "data": {
                        "failure_code": "endpoint_unreachable",
                        "provider_class": "local_endpoint",
                        "detail": "connection refused to 127.0.0.1:11434",
                    },
                }
            )
        )

        self.assertEqual(failure.code, FailureCode.ENDPOINT_UNREACHABLE)
        self.assertEqual(failure.provider_class, ProviderClass.LOCAL_ENDPOINT)
        self.assertIn("[ip]", failure.to_dict()["provider_detail"])

    def test_json_rpc_text_fallback_and_invalid_structured_fields(self) -> None:
        fallback = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "provider request timed out",
                    "data": {"code": 123, "provider_class": 456},
                }
            )
        )
        invalid_provider = classify_exception(
            JsonRpcError(
                {
                    "code": -32000,
                    "message": "provider failed",
                    "data": {
                        "code": "unknown",
                        "provider_class": "not-a-provider-class",
                        "reason": "custom provider reason",
                    },
                }
            )
        )

        self.assertEqual(fallback.code, FailureCode.MODEL_TIMEOUT)
        self.assertEqual(fallback.provider_class, ProviderClass.UNKNOWN)
        self.assertEqual(invalid_provider.code, FailureCode.UNKNOWN)
        self.assertEqual(invalid_provider.provider_class, ProviderClass.UNKNOWN)
        self.assertIn("custom provider reason", invalid_provider.to_dict()["provider_detail"])

    def test_protocol_and_peer_exit_errors_have_acp_codes(self) -> None:
        protocol = classify_exception(JsonRpcError({"code": -32602, "message": "bad params"}))
        peer = classify_exception(JsonRpcPeerExited("JSON-RPC peer exited"))

        self.assertEqual(protocol.code, FailureCode.ACP_PROTOCOL_ERROR)
        self.assertEqual(peer.code, FailureCode.ACP_SUBPROCESS_FAILED)

    def test_generic_text_fallback_is_conservative(self) -> None:
        self.assertEqual(
            classify_exception(RuntimeError("401 unauthorized")).code,
            FailureCode.MODEL_AUTH_FAILED,
        )
        self.assertEqual(
            classify_exception(RuntimeError("too many requests")).code,
            FailureCode.MODEL_RATE_LIMITED,
        )
        self.assertEqual(
            classify_exception(RuntimeError("service unavailable")).code,
            FailureCode.MODEL_UNAVAILABLE,
        )
        self.assertEqual(
            classify_exception(RuntimeError("model timeout")).code,
            FailureCode.MODEL_TIMEOUT,
        )
        self.assertEqual(
            classify_exception(RuntimeError("request timed out")).code,
            FailureCode.MODEL_TIMEOUT,
        )
        self.assertEqual(
            classify_exception(RuntimeError("opaque provider wording")).code,
            FailureCode.UNKNOWN,
        )
        self.assertEqual(
            classify_exception(RuntimeError("sequence 4290 quotation 14032")).code,
            FailureCode.UNKNOWN,
        )

    def test_sanitizer_bounds_and_removes_router_owned_sensitive_shapes(self) -> None:
        detail = sanitize_detail(
            "secret-id https://signal.test/v1 /private/work/session "
            '{"prompt":"private"} [route_context:begin] hello [/route_context:end] '
            "sk-abc123abc123abc123abc123abc123abc123 "
            "localhost:11434 192.168.1.10:11434 fe80::1",
            redactor=lambda text: text.replace("secret-id", "id_redacted"),
            limit=120,
        )

        self.assertLessEqual(len(detail), 120)
        self.assertIn("id_redacted", detail)
        self.assertIn("[url]", detail)
        self.assertIn("[path]", detail)
        self.assertIn("[object]", detail)
        self.assertIn("[token]", detail)
        self.assertIn("[ip]", detail)
        self.assertIn("[host]", detail)
        self.assertNotIn("signal.test", detail)
        self.assertNotIn("/private/work", detail)
        self.assertNotIn("route_context", detail)
        self.assertNotIn("sk-abc123", detail)
        self.assertNotIn("localhost", detail)
        self.assertNotIn("192.168.1.10", detail)
        self.assertNotIn("fe80::1", detail)

    def test_sanitizer_redacts_long_json_diagnostics(self) -> None:
        private_value = "private-provider-payload-" * 20
        detail = sanitize_detail(
            'provider returned {"request":{"messages":["'
            + private_value
            + '"]},"metadata":{"path":"/private/work/session"}}'
        )
        array_detail = sanitize_detail('provider returned ["' + private_value + '"]')

        self.assertIn("[object]", detail)
        self.assertIn("[array]", array_detail)
        self.assertNotIn("private-provider-payload", detail)
        self.assertNotIn("private-provider-payload", array_detail)
        self.assertNotIn("/private/work/session", detail)

    def test_sanitizer_redacts_marker_delimited_prompt_blocks(self) -> None:
        detail = sanitize_detail(
            "[route_context:abc]\n"
            "private route label and account metadata\n"
            "[/route_context:abc]\n"
            "[notification_payload]\n"
            '{"body":"private notification text"}\n'
            "[/notification_payload]"
        )

        self.assertIn("[router_block]", detail)
        self.assertNotIn("private route label", detail)
        self.assertNotIn("private notification", detail)
        self.assertNotIn("route_context", detail)
        self.assertNotIn("notification_payload", detail)

    def test_sanitizer_handles_none_raw_limit_and_truncation(self) -> None:
        self.assertEqual(sanitize_detail(None), "")

        detail = sanitize_detail("x" * 5000, limit=10)

        self.assertEqual(detail, "xxxxxxxxx\u2026")

    def test_preflight_report_failure_classification(self) -> None:
        permission = preflight_failure_from_report(
            SimpleNamespace(missing_tools=(object(),), probe_errors=(), scope_errors=())
        )
        preflight = preflight_failure_from_report(
            SimpleNamespace(missing_tools=(), probe_errors=(object(),), scope_errors=())
        )
        ok = preflight_failure_from_report(
            SimpleNamespace(missing_tools=(), probe_errors=(), scope_errors=())
        )

        self.assertIsNotNone(permission)
        self.assertEqual(permission.code, FailureCode.PERMISSION_DENIED)
        self.assertIsNotNone(preflight)
        self.assertEqual(preflight.code, FailureCode.PREFLIGHT_FAILED)
        self.assertIsNone(ok)

    def test_failure_info_serializes_fixed_public_message(self) -> None:
        payload = failure_info(
            FailureCode.SIGNAL_SEND_FAILED,
            detail="send failed at /private/path",
        ).to_dict()

        self.assertEqual(payload["code"], "signal_send_failed")
        self.assertEqual(payload["message"], "Signal delivery failed.")
        self.assertEqual(payload["provider_class"], "unknown")
        self.assertNotIn("/private/path", payload["detail"])


if __name__ == "__main__":
    unittest.main()
