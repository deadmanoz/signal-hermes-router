from __future__ import annotations

import os
from pathlib import Path

PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


def resolve_under_root(root: Path, leaf: Path, *, error_message: str) -> Path:
    root_resolved = root.resolve(strict=False)
    leaf_resolved = leaf.resolve(strict=False)
    try:
        leaf_resolved.relative_to(root_resolved)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(error_message) from exc
    return leaf_resolved


def validate_path_component(value: str, *, error_message: str) -> str:
    if value in {"", ".", ".."} or "/" in value or "\\" in value:
        raise ValueError(error_message)
    return value


def ensure_private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIR_MODE)
    path.chmod(PRIVATE_DIR_MODE)
    return path


def ensure_private_dir_tree(root: Path, leaf: Path) -> Path:
    ensure_private_dir(root)
    root_resolved = root.resolve(strict=False)
    leaf_resolved = resolve_under_root(
        root,
        leaf,
        error_message="private runtime path escaped configured root",
    )
    relative = leaf_resolved.relative_to(root_resolved)

    leaf.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIR_MODE)
    current = root_resolved
    current.chmod(PRIVATE_DIR_MODE)
    for part in relative.parts:
        current /= part
        current.chmod(PRIVATE_DIR_MODE)
    return leaf


def ensure_private_file(path: Path) -> Path:
    fd = os.open(path, os.O_RDWR | os.O_CREAT, PRIVATE_FILE_MODE)
    os.close(fd)
    path.chmod(PRIVATE_FILE_MODE)
    return path


def write_private_bytes(path: Path, body: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, PRIVATE_FILE_MODE)
    with os.fdopen(fd, "wb") as handle:
        handle.write(body)
    path.chmod(PRIVATE_FILE_MODE)


def write_private_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    write_private_bytes(path, text.encode(encoding))
