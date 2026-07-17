from __future__ import annotations

import unittest

from signal_hermes_router.permissions import ArgPredicate, PermissionRule, StaticPermissionPolicy
from tests.support import read_file_allow_policy


REQUEST = {
    "sessionId": "s1",
    "toolCall": {
        "toolName": "read_file",
        "rawInput": {"path": "/private/deployment/read-only/a.txt"},
    },
    "options": [
        {"optionId": "allow", "kind": "allow_once", "name": "Allow"},
        {"optionId": "reject", "kind": "reject_once", "name": "Reject"},
    ],
}


class PermissionTests(unittest.TestCase):
    def test_mcp_only_policy_rejects_local_tools(self) -> None:
        from signal_hermes_router.permissions import StaticPermissionPolicy
        # MCP-only policy with an allowlisted local tool — the defense-in-depth backstop rejects it
        policy = StaticPermissionPolicy.from_config([{"tool": "terminal/create"}], mcp_only=True)
        self.assertFalse(policy.allows_tool_call({"toolName": "terminal/create"}))
        self.assertFalse(policy.allows_tool_call({"toolName": "fs/read_text_file"}))
        # Non-local allowed tool on MCP-only route is still allowed
        policy = StaticPermissionPolicy.from_config([{"tool": "read_file"}], mcp_only=True)
        self.assertTrue(policy.allows_tool_call({"toolName": "read_file"}))
        # On non-MCP-only route, local tools are governed by the allowlist only
        policy = StaticPermissionPolicy.from_config([{"tool": "terminal/create"}], mcp_only=False)
        self.assertTrue(policy.allows_tool_call({"toolName": "terminal/create"}))

    def test_mcp_only_policy_allows_benign_mcp_tools(self) -> None:
        from signal_hermes_router.permissions import StaticPermissionPolicy
        policy = StaticPermissionPolicy.from_config([{"tool": "code_search"}, {"tool": "python_docs"}], mcp_only=True)
        self.assertTrue(policy.allows_tool_call({"toolName": "code_search"}))
        self.assertTrue(policy.allows_tool_call({"toolName": "python_docs"}))

    def test_default_denies(self) -> None:
        response = StaticPermissionPolicy().acp_response(REQUEST)
        self.assertEqual(response["outcome"]["optionId"], "reject")

    def test_allowlist_matches_tool_and_argument_predicate(self) -> None:
        policy = read_file_allow_policy()
        response = policy.acp_response(REQUEST)
        self.assertEqual(response["outcome"]["optionId"], "allow")

    def test_path_prefix_predicate_rejects_traversal(self) -> None:
        policy = read_file_allow_policy()
        request = {
            **REQUEST,
            "toolCall": {
                "toolName": "read_file",
                "rawInput": {"path": "/private/deployment/read-only/../secret.txt"},
            },
        }
        response = policy.acp_response(request)
        self.assertEqual(response["outcome"]["optionId"], "reject")

    def test_path_prefix_predicate_requires_absolute_paths(self) -> None:
        policy = read_file_allow_policy()
        request = {
            **REQUEST,
            "toolCall": {
                "toolName": "read_file",
                "rawInput": {"path": "read-only/a.txt"},
            },
        }
        response = policy.acp_response(request)
        self.assertEqual(response["outcome"]["optionId"], "reject")

    def test_non_path_prefix_predicate_remains_string_prefix(self) -> None:
        policy = StaticPermissionPolicy.from_config(
            [
                {
                    "tool": "send",
                    "arguments": {"topic": {"prefix": "ops:"}},
                }
            ]
        )
        self.assertTrue(
            policy.allows_tool_call({"toolName": "send", "rawInput": {"topic": "ops:a"}})
        )
        self.assertFalse(
            policy.allows_tool_call({"toolName": "send", "rawInput": {"topic": "dev:a"}})
        )

    def test_argument_predicate_matrix(self) -> None:
        policy = StaticPermissionPolicy.from_config(
            [
                {
                    "tool": "send",
                    "arguments": {
                        "channel": "ops",
                        "priority": {"one_of": ["high", "urgent"]},
                        "ticket": {"regex": r"OPS-[0-9]+"},
                        "dry_run": {"present": False},
                    },
                }
            ]
        )
        self.assertTrue(
            policy.allows_tool_call(
                {
                    "toolName": "send",
                    "rawInput": {
                        "channel": "ops",
                        "priority": "high",
                        "ticket": "OPS-123",
                    },
                }
            )
        )
        for raw_input in (
            {"channel": "dev", "priority": "high", "ticket": "OPS-123"},
            {"channel": "ops", "priority": "low", "ticket": "OPS-123"},
            {"channel": "ops", "priority": "high", "ticket": "xOPS-123"},
            {"channel": "ops", "priority": "high", "ticket": "OPS-123", "dry_run": True},
        ):
            with self.subTest(raw_input=raw_input):
                self.assertFalse(
                    policy.allows_tool_call({"toolName": "send", "rawInput": raw_input})
                )

    def test_non_dict_raw_input_is_wrapped_for_value_predicates(self) -> None:
        policy = StaticPermissionPolicy.from_config(
            [{"tool": "echo", "arguments": {"value": "hello"}}]
        )
        self.assertTrue(policy.allows_tool_call({"toolName": "echo", "rawInput": "hello"}))
        self.assertFalse(policy.allows_tool_call({"toolName": "echo", "rawInput": "bye"}))

    def test_allow_always_is_not_selected_but_reject_always_is_a_fallback(self) -> None:
        policy = StaticPermissionPolicy.from_config([{"tool": "send"}])
        request = {
            **REQUEST,
            "toolCall": {"toolName": "send", "rawInput": {}},
            "options": [{"optionId": "allow-always", "kind": "allow_always"}],
        }
        self.assertEqual(policy.acp_response(request), {"outcome": {"outcome": "cancelled"}})

        request = {
            **REQUEST,
            "options": [{"optionId": "reject-always", "kind": "reject_always"}],
        }
        self.assertEqual(
            StaticPermissionPolicy().acp_response(request)["outcome"]["optionId"], "reject-always"
        )

    def test_invalid_permission_predicates_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            StaticPermissionPolicy.from_config(
                [{"tool": "send", "arguments": {"path": {"unknown": True}}}]
            )
        with self.assertRaises(ValueError):
            StaticPermissionPolicy.from_config([{"tool": "send", "arguments": []}])

    def test_denylist_config_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            PermissionRule.from_config({"tool": "read_file", "denylist": []})

    def test_argument_predicate_config_and_match_failure_shapes(self) -> None:
        self.assertTrue(ArgPredicate.from_config("exact").matches("exact"))
        with self.assertRaisesRegex(ValueError, "unknown argument predicate"):
            ArgPredicate.from_config({"unknown": "value"})

        self.assertFalse(ArgPredicate(present=True).matches(None, exists=False))
        self.assertTrue(ArgPredicate(present=False).matches(None, exists=False))
        self.assertFalse(ArgPredicate(equals="expected").matches("actual"))
        self.assertFalse(ArgPredicate(prefix="pre").matches("actual", argument_name="label"))
        self.assertFalse(ArgPredicate(one_of=("a", "b")).matches("c"))
        self.assertFalse(ArgPredicate(regex=r"a+").matches("bbb"))

    def test_permission_rule_config_errors_and_args_alias(self) -> None:
        with self.assertRaisesRegex(ValueError, "deny rules"):
            PermissionRule.from_config({"tool": "read_file", "deny": True})
        with self.assertRaisesRegex(ValueError, "requires tool"):
            PermissionRule.from_config({"arguments": {}})
        with self.assertRaisesRegex(ValueError, "arguments must be a mapping"):
            PermissionRule.from_config({"tool": "read_file", "arguments": []})  # type: ignore[arg-type]

        rule = PermissionRule.from_config(
            {"tool_name": "read_file", "args": {"path": {"present": True}}}
        )
        self.assertFalse(rule.matches("write_file", {"path": "/tmp"}))

    def test_cancelled_response_when_no_option_matches(self) -> None:
        self.assertEqual(
            StaticPermissionPolicy().acp_response({"toolCall": {}, "options": []}),
            {"outcome": {"outcome": "cancelled"}},
        )

    def test_policy_normalizes_alternate_tool_names(self) -> None:
        policy = StaticPermissionPolicy.from_config(
            [{"tool": "inspect", "arguments": {"value": {"one_of": ["payload"]}}}]
        )
        self.assertTrue(policy.allows_tool_call({"name": "inspect", "rawInput": "payload"}))
        self.assertTrue(policy.allows_tool_call({"title": "inspect", "raw_input": "payload"}))
        self.assertFalse(policy.allows_tool_call({"toolName": "inspect", "rawInput": "other"}))

    def test_path_prefix_rejects_non_string_and_relative_paths(self) -> None:
        predicate = ArgPredicate(prefix="/tmp")
        self.assertFalse(predicate.matches(42, argument_name="path"))
        self.assertFalse(predicate.matches("relative/path", argument_name="path"))
        self.assertTrue(predicate.matches("/tmp/file", argument_name=None))


if __name__ == "__main__":
    unittest.main()
