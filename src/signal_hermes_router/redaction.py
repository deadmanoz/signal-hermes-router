from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field


PHONE_RE = re.compile(r"\+\d{6,15}")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

# Sanitizer for arbitrary subprocess output (Hermes stderr near a crash). The
# Redactor above only masks REGISTERED identifiers plus phone/UUID shapes, so
# free-form child output needs its own code-controlled pass for unregistered
# credentials and terminal control sequences. Ordinary traceback text (paths,
# module names, exception messages) stays readable.
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[@-_]")
# Everything below 0x20 except tab and newline is normalized to a space --
# including carriage returns, which would otherwise let child output like
# "progress\r..." overwrite or forge the visible log line in terminal and
# journal viewers.
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
BEARER_RE = re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}")
# A credential assignment masks the REST OF THE LINE, not just the first
# whitespace-delimited token: a quoted multi-word passphrase and header-style
# multi-part values ('Cookie: sid=...; csrf=...') must not leave residual
# credential text behind. Callers sanitize per stderr line, so the blast
# radius of the greedy match is one line of child output.
CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(api[-_]?key|apikey|authorization|auth[-_]?token|access[-_]?token|"
    r"refresh[-_]?token|token|secret|password|passwd|set[-_]?cookie|cookie)\b\s*[:=][^\n]*"
)
KEY_LITERAL_RE = re.compile(r"\b(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}\b")
LONG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{32,}\b")
LONG_BASE64_RE = re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}")


def sanitize_subprocess_output(text: str) -> str:
    """Mask credential-shaped content and control characters in arbitrary
    subprocess output before it reaches a log line. The pattern list is
    code-controlled by design (never configuration-driven)."""
    sanitized = ANSI_ESCAPE_RE.sub("", str(text))
    sanitized = CONTROL_CHARS_RE.sub(" ", sanitized)
    # Bearer values first, so a following key/value pass cannot leave the
    # token of "Authorization: Bearer <token>" behind as a second word.
    sanitized = BEARER_RE.sub(r"\1 [redacted]", sanitized)
    sanitized = CREDENTIAL_ASSIGNMENT_RE.sub(r"\1=[redacted]", sanitized)
    sanitized = KEY_LITERAL_RE.sub("[key_redacted]", sanitized)
    sanitized = LONG_HEX_RE.sub("[hex_redacted]", sanitized)
    sanitized = LONG_BASE64_RE.sub("[b64_redacted]", sanitized)
    return sanitized


def stable_ref(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


@dataclass
class Redactor:
    identifiers: set[str] = field(default_factory=set)

    def add(self, *values: str | None) -> None:
        for value in values:
            if value:
                self.identifiers.add(str(value))

    def ref(self, prefix: str, value: str) -> str:
        self.add(value)
        return stable_ref(prefix, value)

    def redact(self, message: str) -> str:
        redacted = str(message)
        for ident in sorted(self.identifiers, key=len, reverse=True):
            if ident:
                redacted = redacted.replace(ident, stable_ref("id", ident))
        redacted = PHONE_RE.sub("[phone_redacted]", redacted)
        redacted = UUID_RE.sub("[uuid_redacted]", redacted)
        return redacted
