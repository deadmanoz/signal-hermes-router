from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .private_fs import resolve_under_root, validate_path_component


SECRET_PREFIXES = ("file://", "env://", "op://", "systemd-credential://")


def resolve_secret_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        if parsed.netloc:
            raise ValueError("file secret URI must use file:///absolute/path")
        path = Path(unquote(parsed.path))
        if not path.is_absolute():
            raise ValueError("file secret URI must use file:///absolute/path")
        return path.read_text(encoding="utf-8").strip()
    if parsed.scheme == "env":
        name = f"{parsed.netloc}{parsed.path}".lstrip("/")
        try:
            return os.environ[name]
        except KeyError as exc:
            raise KeyError(f"missing environment secret {name}") from exc
    if parsed.scheme == "op":
        result = subprocess.run(
            ["op", "read", uri],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()
    if parsed.scheme == "systemd-credential":
        name = _credential_basename(parsed)
        directory = os.environ.get("CREDENTIALS_DIRECTORY")
        if not directory:
            raise KeyError("CREDENTIALS_DIRECTORY is not set for systemd credential lookup")
        root = Path(directory)
        path = resolve_under_root(
            root,
            root / name,
            error_message="systemd credential path escaped CREDENTIALS_DIRECTORY",
        )
        return path.read_text(encoding="utf-8").strip()
    raise ValueError(f"unsupported secret URI scheme: {parsed.scheme}")


def resolve_secret_refs(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: resolve_secret_refs(child) for key, child in value.items()}
    if isinstance(value, list):
        return [resolve_secret_refs(child) for child in value]
    if isinstance(value, str) and value.startswith(SECRET_PREFIXES):
        return resolve_secret_uri(value)
    return value


def _credential_basename(parsed: Any) -> str:
    name = unquote(f"{parsed.netloc}{parsed.path}").lstrip("/")
    return validate_path_component(
        name,
        error_message="systemd credential name must be a single basename",
    )
