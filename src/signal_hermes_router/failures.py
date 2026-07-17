from __future__ import annotations

import errno
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class FailureCode(StrEnum):
    ROUTER_ERROR = "router_error"
    ACP_SUBPROCESS_FAILED = "acp_subprocess_failed"
    ACP_PROTOCOL_ERROR = "acp_protocol_error"
    ACP_EMPTY_RESPONSE = "acp_empty_response"
    ACP_SESSION_FAILED = "acp_session_failed"
    ACP_PROMPT_TIMEOUT = "acp_prompt_timeout"
    PERMISSION_DENIED = "permission_denied"
    PREFLIGHT_FAILED = "preflight_failed"
    SIGNAL_SEND_FAILED = "signal_send_failed"
    MODEL_AUTH_FAILED = "model_auth_failed"
    MODEL_RATE_LIMITED = "model_rate_limited"
    MODEL_UNAVAILABLE = "model_unavailable"
    MODEL_TIMEOUT = "model_timeout"
    ENDPOINT_UNREACHABLE = "endpoint_unreachable"
    UNKNOWN = "unknown"


class ProviderClass(StrEnum):
    CLOUD_API = "cloud_api"
    LOCAL_ENDPOINT = "local_endpoint"
    UNKNOWN = "unknown"


DETAIL_LIMIT = 240
RAW_DETAIL_LIMIT = 4096

_CODE_MESSAGES: dict[FailureCode, str] = {
    FailureCode.ROUTER_ERROR: "Router-owned failure while handling the turn.",
    FailureCode.ACP_SUBPROCESS_FAILED: "ACP subprocess exited or could not be reached.",
    FailureCode.ACP_PROTOCOL_ERROR: "ACP protocol exchange failed.",
    FailureCode.ACP_EMPTY_RESPONSE: "ACP returned an unmarked empty response.",
    FailureCode.ACP_SESSION_FAILED: "ACP session setup or recovery failed.",
    FailureCode.ACP_PROMPT_TIMEOUT: "ACP prompt request timed out in the router.",
    FailureCode.PERMISSION_DENIED: "Required permission was denied or unavailable.",
    FailureCode.PREFLIGHT_FAILED: "Permission preflight failed.",
    FailureCode.SIGNAL_SEND_FAILED: "Signal delivery failed.",
    FailureCode.MODEL_AUTH_FAILED: "Model provider authentication failed.",
    FailureCode.MODEL_RATE_LIMITED: "Model provider rate limit or quota was reached.",
    FailureCode.MODEL_UNAVAILABLE: "Model provider is unavailable.",
    FailureCode.MODEL_TIMEOUT: "Model provider request timed out.",
    FailureCode.ENDPOINT_UNREACHABLE: "Configured provider endpoint was unreachable.",
    FailureCode.UNKNOWN: "Failure cause is unknown.",
}

_STRUCTURED_CODE_MAP: dict[str, FailureCode] = {
    "router_error": FailureCode.ROUTER_ERROR,
    "acp_subprocess_failed": FailureCode.ACP_SUBPROCESS_FAILED,
    "acp_protocol_error": FailureCode.ACP_PROTOCOL_ERROR,
    "acp_empty_response": FailureCode.ACP_EMPTY_RESPONSE,
    "acp_session_failed": FailureCode.ACP_SESSION_FAILED,
    "acp_prompt_timeout": FailureCode.ACP_PROMPT_TIMEOUT,
    "permission_denied": FailureCode.PERMISSION_DENIED,
    "preflight_failed": FailureCode.PREFLIGHT_FAILED,
    "signal_send_failed": FailureCode.SIGNAL_SEND_FAILED,
    "model_auth_failed": FailureCode.MODEL_AUTH_FAILED,
    "auth_failed": FailureCode.MODEL_AUTH_FAILED,
    "authentication_failed": FailureCode.MODEL_AUTH_FAILED,
    "unauthorized": FailureCode.MODEL_AUTH_FAILED,
    "forbidden": FailureCode.MODEL_AUTH_FAILED,
    "model_rate_limited": FailureCode.MODEL_RATE_LIMITED,
    "rate_limited": FailureCode.MODEL_RATE_LIMITED,
    "quota_exceeded": FailureCode.MODEL_RATE_LIMITED,
    "model_unavailable": FailureCode.MODEL_UNAVAILABLE,
    "service_unavailable": FailureCode.MODEL_UNAVAILABLE,
    "provider_unavailable": FailureCode.MODEL_UNAVAILABLE,
    "model_timeout": FailureCode.MODEL_TIMEOUT,
    "provider_timeout": FailureCode.MODEL_TIMEOUT,
    "request_timeout": FailureCode.MODEL_TIMEOUT,
    "endpoint_unreachable": FailureCode.ENDPOINT_UNREACHABLE,
    "connection_failed": FailureCode.ENDPOINT_UNREACHABLE,
    "connection_refused": FailureCode.ENDPOINT_UNREACHABLE,
    "dns_error": FailureCode.ENDPOINT_UNREACHABLE,
    "unknown": FailureCode.UNKNOWN,
}

_MODEL_PROVIDER_FAILURE_CODES = frozenset(
    {
        FailureCode.MODEL_AUTH_FAILED,
        FailureCode.MODEL_RATE_LIMITED,
        FailureCode.MODEL_UNAVAILABLE,
        FailureCode.MODEL_TIMEOUT,
        FailureCode.ENDPOINT_UNREACHABLE,
    }
)

_URL_RE = re.compile(r"\bhttps?://[^\s<>)\"']+", re.IGNORECASE)
# These intentionally over-redact dotted tokens and IP-shaped values. Losing
# some diagnostic precision is preferable to surfacing hostnames or local
# endpoint addresses through operator status output.
_HOST_RE = re.compile(
    r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"(?:[a-z]{2,63}|local|localhost)\b",
    re.IGNORECASE,
)
_LOCALHOST_RE = re.compile(r"\blocalhost(?::\d{1,5})?\b", re.IGNORECASE)
_IP_LITERAL_RE = re.compile(
    r"(?<![0-9A-Fa-f:.])(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?(?![0-9A-Fa-f:.])"
    r"|"
    r"(?<![0-9A-Fa-f:])(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}"
    r"(?:%[A-Za-z0-9_.-]+)?(?![0-9A-Fa-f:])"
)
_PATH_RE = re.compile(r"(?<!\w)(?:~|/|[A-Za-z]:\\)(?:[^\s\"'<>|{}[\]]+[\\/])*[^\s\"'<>|{}[\]]*")
_ROUTER_BLOCK_RE = re.compile(
    r"\[(?P<tag>route_context|synthetic_event|notification_payload|attachment_manifest)"
    r"(?::[^\]]*)?\].*?\[/\s*(?P=tag)(?::[^\]]*)?\]",
    re.IGNORECASE | re.DOTALL,
)
_ROUTER_MARKER_RE = re.compile(
    r"\[/?(?:route_context|synthetic_event|notification_payload|attachment_manifest)[^\]]*\]",
    re.IGNORECASE,
)
_OPAQUE_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9._~+/=-])"
    r"(?=[A-Za-z0-9._~+/=-]{24,}(?![A-Za-z0-9._~+/=-]))"
    r"(?=[A-Za-z0-9._~+/=-]*\d)"
    r"[A-Za-z0-9._~+/=-]{24,}"
    r"(?![A-Za-z0-9._~+/=-])"
)
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+")
_SPACE_RE = re.compile(r"\s+")
_AUTH_STATUS_RE = re.compile(r"\b(?:401|403)\b")
_RATE_LIMIT_STATUS_RE = re.compile(r"\b429\b")
_UNAVAILABLE_STATUS_RE = re.compile(r"\b(?:502|503|504)\b")
_QUOTA_RE = re.compile(r"\bquota\b")
_DNS_RE = re.compile(r"\bdns\b")
_TIMEOUT_TEXT_RE = re.compile(r"\b(?:(?:provider|model|request) timeout|timed out)\b")
_SUBPROCESS_PIPE_ERRNOS = {errno.EPIPE}


@dataclass(frozen=True)
class FailureInfo:
    code: FailureCode
    provider_class: ProviderClass = ProviderClass.UNKNOWN
    detail: str | None = None
    provider_detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "code": self.code.value,
            "message": _CODE_MESSAGES[self.code],
            "provider_class": self.provider_class.value,
        }
        if self.detail:
            value["detail"] = self.detail
        if self.provider_detail:
            value["provider_detail"] = self.provider_detail
        return value


def failure_info(
    code: FailureCode,
    *,
    provider_class: ProviderClass = ProviderClass.UNKNOWN,
    detail: Any | None = None,
    provider_detail: Any | None = None,
    redactor: Callable[[str], str] | None = None,
) -> FailureInfo:
    return FailureInfo(
        code=code,
        provider_class=provider_class,
        detail=sanitize_detail(detail, redactor=redactor) if detail not in (None, "") else None,
        provider_detail=(
            sanitize_detail(provider_detail, redactor=redactor)
            if provider_detail not in (None, "")
            else None
        ),
    )


def classify_exception(
    exc: BaseException,
    *,
    redactor: Callable[[str], str] | None = None,
    context: FailureCode | None = None,
    prefer_structured_provider_failure: bool = False,
) -> FailureInfo:
    from .acp import JsonRpcError, JsonRpcPeerExited

    def _detail(code: FailureCode) -> FailureInfo:
        return failure_info(code, provider_detail=_exception_detail(exc), redactor=redactor)

    if context is not None:
        if isinstance(exc, JsonRpcError):
            error = exc.error if isinstance(getattr(exc, "error", None), dict) else {}
            data = error.get("data")
            code = _structured_failure_code(data)
            if prefer_structured_provider_failure and is_model_provider_failure_code(code):
                return failure_info(
                    code,
                    provider_class=_structured_provider_class(data),
                    provider_detail=_structured_provider_detail(error, data),
                    redactor=redactor,
                )
            return failure_info(
                context,
                provider_class=_structured_provider_class(data),
                provider_detail=_structured_provider_detail(error, data),
                redactor=redactor,
            )
        # Only JSON-RPC errors can carry structured provider metadata. Peer
        # exits and generic exceptions keep the caller-supplied context code
        # with a sanitized diagnostic string.
        return _detail(context)
    if isinstance(exc, JsonRpcError):
        return _classify_json_rpc_error(exc, redactor=redactor)
    if isinstance(exc, JsonRpcPeerExited):
        return _detail(FailureCode.ACP_SUBPROCESS_FAILED)
    if _is_subprocess_pipe_error(exc):
        return _detail(FailureCode.ACP_SUBPROCESS_FAILED)
    # Keep timeout classification before OSError: router-owned wait timeouts
    # should stay acp_prompt_timeout even when a runtime exposes them through a
    # broader OSError-compatible type.
    if isinstance(exc, TimeoutError):
        return _detail(FailureCode.ACP_PROMPT_TIMEOUT)
    if isinstance(exc, OSError):
        return _detail(FailureCode.ENDPOINT_UNREACHABLE)
    return _classify_text_fallback(_exception_detail(exc), redactor=redactor)


def is_model_provider_failure_code(code: FailureCode | None) -> bool:
    return code in _MODEL_PROVIDER_FAILURE_CODES


def is_model_provider_failure(failure: FailureInfo) -> bool:
    return is_model_provider_failure_code(failure.code)


def preflight_failure_from_report(
    report: Any, *, redactor: Callable[[str], str] | None = None
) -> FailureInfo | None:
    missing_tools = getattr(report, "missing_tools", ())
    if missing_tools:
        return failure_info(
            FailureCode.PERMISSION_DENIED,
            detail="configured permission tool is missing from profile tool surface",
            redactor=redactor,
        )
    local_tools_exposed = getattr(report, "local_tools_exposed", ())
    if local_tools_exposed:
        # Checked before probe/scope errors so that a config-level local-tool
        # exposure (which does not depend on a successful probe) is classified
        # as a permission problem, not a transient preflight problem.
        return failure_info(
            FailureCode.PERMISSION_DENIED,
            detail="local terminal tools are exposed on an mcp_only route",
            redactor=redactor,
        )
    probe_errors = getattr(report, "probe_errors", ())
    scope_errors = getattr(report, "scope_errors", ())
    if probe_errors or scope_errors:
        return failure_info(
            FailureCode.PREFLIGHT_FAILED,
            detail="permission preflight could not validate the requested scope",
            redactor=redactor,
        )
    return None


def sanitize_detail(
    value: Any,
    *,
    redactor: Callable[[str], str] | None = None,
    limit: int = DETAIL_LIMIT,
) -> str:
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    if redactor is not None:
        text = redactor(text)
    if len(text) > RAW_DETAIL_LIMIT:
        text = text[:RAW_DETAIL_LIMIT]
    text = _CONTROL_RE.sub(" ", text)
    text = _ROUTER_BLOCK_RE.sub("[router_block]", text)
    text = _ROUTER_MARKER_RE.sub("[router_marker]", text)
    text = _redact_json_spans(text)
    text = _URL_RE.sub("[url]", text)
    text = _PATH_RE.sub("[path]", text)
    text = _LOCALHOST_RE.sub("[host]", text)
    text = _IP_LITERAL_RE.sub("[ip]", text)
    text = _HOST_RE.sub("[host]", text)
    text = _OPAQUE_TOKEN_RE.sub("[token]", text)
    text = _SPACE_RE.sub(" ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _is_subprocess_pipe_error(exc: BaseException) -> bool:
    if isinstance(exc, BrokenPipeError):
        return True
    return isinstance(exc, OSError) and getattr(exc, "errno", None) in _SUBPROCESS_PIPE_ERRNOS


def _redact_json_spans(text: str) -> str:
    result: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in "{[":
            replacement = "[object]" if char == "{" else "[array]"
            end = _json_span_end(text, index)
            if end is None:
                if _looks_like_json_start(text, index, char):
                    result.append(replacement)
                    break
            else:
                span = text[index:end]
                if _looks_like_json_span(span, char):
                    result.append(replacement)
                    index = end
                    continue
        result.append(char)
        index += 1
    return "".join(result)


def _json_span_end(text: str, start: int) -> int | None:
    stack = [text[start]]
    in_string = False
    escaped = False
    for index in range(start + 1, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char in "{[":
            stack.append(char)
        elif char in "}]":
            if not stack or not _json_brackets_match(stack[-1], char):
                return None
            stack.pop()
            if not stack:
                return index + 1
    return None


def _json_brackets_match(opening: str, closing: str) -> bool:
    return (opening == "{" and closing == "}") or (opening == "[" and closing == "]")


def _looks_like_json_start(text: str, start: int, opening: str) -> bool:
    sample = text[start : start + DETAIL_LIMIT]
    return _looks_like_json_span(sample, opening)


def _looks_like_json_span(span: str, opening: str) -> bool:
    inner = span[1:]
    if opening == "{":
        return ":" in inner or '"' in inner or len(span) > DETAIL_LIMIT
    return any(token in inner for token in ('"', ":", ",", "{", "}"))


def _classify_json_rpc_error(
    exc: Any,
    *,
    redactor: Callable[[str], str] | None,
) -> FailureInfo:
    error = exc.error if isinstance(getattr(exc, "error", None), dict) else {}
    data = error.get("data")
    code = _structured_failure_code(data)
    provider_class = _structured_provider_class(data)
    provider_detail = _structured_provider_detail(error, data)
    if code is not None:
        return failure_info(
            code,
            provider_class=provider_class,
            provider_detail=provider_detail,
            redactor=redactor,
        )
    json_rpc_code = error.get("code")
    if json_rpc_code in {-32600, -32601, -32602, -32603, -32700}:
        return failure_info(
            FailureCode.ACP_PROTOCOL_ERROR,
            provider_class=provider_class,
            provider_detail=provider_detail,
            redactor=redactor,
        )
    # This text fallback is deliberately small and provider-agnostic. It only
    # recognizes generic auth, rate-limit, unavailable, timeout, and endpoint
    # reachability signals; unrecognized provider wording remains unknown.
    return _classify_text_fallback(
        str(error.get("message") or provider_detail or exc),
        provider_class=provider_class,
        redactor=redactor,
    )


def _structured_failure_code(data: Any) -> FailureCode | None:
    if not isinstance(data, dict):
        return None
    raw = data.get("code", data.get("failure_code"))
    if not isinstance(raw, str):
        return None
    return _STRUCTURED_CODE_MAP.get(_normalize_code(raw))


def _structured_provider_class(data: Any) -> ProviderClass:
    if not isinstance(data, dict):
        return ProviderClass.UNKNOWN
    raw = data.get("provider_class")
    if not isinstance(raw, str):
        return ProviderClass.UNKNOWN
    try:
        return ProviderClass(raw)
    except ValueError:
        return ProviderClass.UNKNOWN


def _structured_provider_detail(error: dict[str, Any], data: Any) -> str:
    if isinstance(data, dict):
        for key in ("provider_detail", "detail", "message", "reason"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
    value = error.get("message")
    return str(value) if value is not None else ""


def _classify_text_fallback(
    text: str,
    *,
    provider_class: ProviderClass = ProviderClass.UNKNOWN,
    redactor: Callable[[str], str] | None,
) -> FailureInfo:
    normalized = text.lower()
    code = FailureCode.UNKNOWN
    if _AUTH_STATUS_RE.search(normalized) or any(
        marker in normalized for marker in ("unauthorized", "forbidden")
    ):
        code = FailureCode.MODEL_AUTH_FAILED
    elif (
        _RATE_LIMIT_STATUS_RE.search(normalized)
        or _QUOTA_RE.search(normalized)
        or any(marker in normalized for marker in ("rate limit", "rate-limit", "too many requests"))
    ):
        code = FailureCode.MODEL_RATE_LIMITED
    elif _UNAVAILABLE_STATUS_RE.search(normalized) or any(
        marker in normalized for marker in ("service unavailable", "temporarily unavailable")
    ):
        code = FailureCode.MODEL_UNAVAILABLE
    elif _TIMEOUT_TEXT_RE.search(normalized):
        code = FailureCode.MODEL_TIMEOUT
    elif any(
        marker in normalized
        for marker in (
            "connection refused",
            "connection reset",
            "connection failed",
            "network unreachable",
            "econnrefused",
        )
    ) or _DNS_RE.search(normalized):
        code = FailureCode.ENDPOINT_UNREACHABLE
    return failure_info(
        code,
        provider_class=provider_class,
        provider_detail=text,
        redactor=redactor,
    )


def _normalize_code(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_").replace(".", "_")
    return re.sub(r"[^a-z0-9_]+", "_", normalized).strip("_")


def _exception_detail(exc: BaseException) -> str:
    message = str(exc)
    if message:
        return message
    return exc.__class__.__name__
