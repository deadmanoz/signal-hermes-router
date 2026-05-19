from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field


PHONE_RE = re.compile(r"\+\d{6,15}")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


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
