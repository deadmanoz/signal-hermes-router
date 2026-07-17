from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

# Tool names that are considered local-terminal/fs execution primitives.
# These are defense-in-depth rejected on mcp_only routes via StaticPermissionPolicy.
_LOCAL_TOOL_NAMES = frozenset(
    {
        "shell",
        "bash",
        "sh",
        "zsh",
        "python",
        "exec",
        "execute",
        "run",
        "run_command",
        "run_shell_command",
        "subprocess",
        "code_interpreter",
        "terminal",
        "fs",
        "read_file",
        "write_file",
        "edit_file",
        "list_directory",
        "create_directory",
        "delete_file",
        "move_file",
        "copy_file",
    }
)
_LOCAL_TOOL_PREFIXES = ("terminal/", "fs/")


def is_local_tool(tool_name: str) -> bool:
    """Return True if tool_name matches a known local-terminal/fs pattern."""
    lower = tool_name.lower()
    if lower in _LOCAL_TOOL_NAMES:
        return True
    return lower.startswith(_LOCAL_TOOL_PREFIXES)


@dataclass(frozen=True)
class ArgPredicate:
    equals: Any | None = None
    prefix: str | None = None
    one_of: tuple[Any, ...] | None = None
    regex: str | None = None
    present: bool | None = None

    @classmethod
    def from_config(cls, value: Any) -> "ArgPredicate":
        if not isinstance(value, dict):
            return cls(equals=value)
        allowed = {"equals", "prefix", "one_of", "regex", "present"}
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown argument predicate keys: {sorted(unknown)}")
        one_of = value.get("one_of")
        return cls(
            equals=value.get("equals"),
            prefix=value.get("prefix"),
            one_of=tuple(one_of) if one_of is not None else None,
            regex=value.get("regex"),
            present=value.get("present"),
        )

    def matches(self, value: Any, *, exists: bool = True, argument_name: str | None = None) -> bool:
        if self.present is not None and exists is not self.present:
            return False
        if self.present is False:
            return True
        if self.equals is not None and value != self.equals:
            return False
        if self.prefix is not None:
            if _is_path_argument(argument_name):
                if not _path_prefix_matches(value, self.prefix):
                    return False
            elif not str(value).startswith(self.prefix):
                return False
        if self.one_of is not None and value not in self.one_of:
            return False
        if self.regex is not None and re.fullmatch(self.regex, str(value)) is None:
            return False
        return True


@dataclass(frozen=True)
class PermissionRule:
    tool_name: str
    arguments: dict[str, ArgPredicate] = field(default_factory=dict)

    @classmethod
    def from_config(cls, value: dict[str, Any]) -> "PermissionRule":
        if "deny" in value or "denylist" in value:
            raise ValueError("permission policy is allowlist-only; deny rules are not supported")
        tool_name = value.get("tool") or value.get("tool_name")
        if not tool_name:
            raise ValueError("permission rule requires tool or tool_name")
        if "arguments" in value:
            raw_args = value["arguments"]
        elif "args" in value:
            raw_args = value["args"]
        else:
            raw_args = {}
        if not isinstance(raw_args, dict):
            raise ValueError("permission rule arguments must be a mapping")
        return cls(
            tool_name=str(tool_name),
            arguments={str(key): ArgPredicate.from_config(pred) for key, pred in raw_args.items()},
        )

    def matches(self, tool_name: str, raw_input: dict[str, Any]) -> bool:
        if tool_name != self.tool_name:
            return False
        for key, predicate in self.arguments.items():
            exists = key in raw_input
            value = raw_input.get(key)
            if not predicate.matches(value, exists=exists, argument_name=key):
                return False
        return True


@dataclass(frozen=True)
class StaticPermissionPolicy:
    rules: tuple[PermissionRule, ...] = ()
    mcp_only: bool = False

    @classmethod
    def from_config(
        cls, values: list[dict[str, Any]] | None, *, mcp_only: bool = False
    ) -> "StaticPermissionPolicy":
        return cls(
            tuple(PermissionRule.from_config(value) for value in values or []),
            mcp_only=mcp_only,
        )

    def allows_tool_call(self, tool_call: dict[str, Any]) -> bool:
        tool_name = (
            tool_call.get("toolName")
            or tool_call.get("tool_name")
            or tool_call.get("name")
            or tool_call.get("title")
            or ""
        )
        if self.mcp_only and is_local_tool(str(tool_name)):
            safe_name = str(tool_name).replace("\n", "\\n").replace("\r", "\\r")[:80]
            LOGGER.info(
                "mcp_only policy rejected local tool call: %s",
                safe_name,
            )
            return False
        raw_input = tool_call.get("rawInput") or tool_call.get("raw_input") or {}
        if not isinstance(raw_input, dict):
            raw_input = {"value": raw_input}
        return any(rule.matches(str(tool_name), raw_input) for rule in self.rules)

    def select_option(self, request: dict[str, Any]) -> str | None:
        if not self.allows_tool_call(request.get("toolCall") or {}):
            return self._reject_option(request)
        return self._allow_option(request)

    @staticmethod
    def _allow_option(request: dict[str, Any]) -> str | None:
        return _first_option_id(request, ("allow_once",))

    @staticmethod
    def _reject_option(request: dict[str, Any]) -> str | None:
        return _first_option_id(request, ("reject_once", "reject_always"))

    def with_mcp_only(self, mcp_only: bool) -> "StaticPermissionPolicy":
        """Return a copy with mcp_only set, for enforcement-point override."""
        if self.mcp_only == mcp_only:
            return self
        return replace(self, mcp_only=mcp_only)

    def acp_response(self, request: dict[str, Any]) -> dict[str, Any]:
        option_id = self.select_option(request)
        if option_id is None:
            return {"outcome": {"outcome": "cancelled"}}
        return {"outcome": {"outcome": "selected", "optionId": option_id}}


def _is_path_argument(argument_name: str | None) -> bool:
    if argument_name is None:
        return False
    normalized = argument_name.lower().replace("-", "_")
    return normalized in {"path", "file_path", "filepath", "cwd"} or normalized.endswith("_path")


def _first_option_id(request: dict[str, Any], kinds: tuple[str, ...]) -> str | None:
    options = request.get("options") or []
    for kind in kinds:
        for option in options:
            if option.get("kind") == kind:
                return option.get("optionId")
    return None


def _path_prefix_matches(value: Any, prefix: str) -> bool:
    if not isinstance(value, str):
        return False
    try:
        allowed_root = Path(prefix).expanduser()
        requested = Path(value).expanduser()
        if not allowed_root.is_absolute() or not requested.is_absolute():
            return False
        requested.resolve(strict=False).relative_to(allowed_root.resolve(strict=False))
    except (OSError, RuntimeError, ValueError):
        return False
    return True
