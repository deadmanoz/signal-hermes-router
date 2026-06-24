from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from .mime import content_type_for_path, is_image_content_type
from .models import MediaManifest
from .payloads import compact_json_dumps

# This set is deliberately code-controlled, not config-controlled. Adding a key
# here exposes that route_context value to the LLM via the prompt preamble.
PROMPT_SAFE_CONTEXT_KEYS = frozenset({"purpose", "route_alias"})


def new_context_nonce() -> str:
    return uuid.uuid4().hex


def context_for_prompt(route_context: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in route_context.items() if key in PROMPT_SAFE_CONTEXT_KEYS}


def render_route_context(route_context: dict[str, Any], nonce: str | None = None) -> dict[str, str]:
    nonce = nonce or new_context_nonce()
    body = compact_json_dumps(route_context)
    return {"type": "text", "text": f"[route_context:{nonce}]{body}[/route_context:{nonce}]"}


def render_scheduled_event(metadata: dict[str, Any], nonce: str | None = None) -> dict[str, str]:
    nonce = nonce or new_context_nonce()
    body = compact_json_dumps(metadata)
    return {
        "type": "text",
        "text": f"[scheduled_event:{nonce}]{body}[/scheduled_event:{nonce}]",
    }


def escape_prompt_text(text: str) -> str:
    return (
        text.replace("[route_context:", "[route_context_escaped:")
        .replace("[/route_context:", "[/route_context_escaped:")
        .replace("[scheduled_event:", "[scheduled_event_escaped:")
        .replace("[/scheduled_event:", "[/scheduled_event_escaped:")
    )


def escape_user_text(text: str) -> str:
    return escape_prompt_text(text)


def text_block(text: str) -> dict[str, str]:
    return {"type": "text", "text": text}


def image_block(path: Path, content_type: str | None = None) -> dict[str, str]:
    resolved = path.resolve()
    return {
        "type": "resource_link",
        "name": resolved.name,
        "mimeType": content_type_for_path(resolved, content_type),
        "uri": resolved.as_uri(),
    }


def manifest_block(manifest: MediaManifest, *, include_tool_path: bool = False) -> dict[str, str]:
    return text_block(manifest.to_text(include_tool_path=include_tool_path))


def build_prompt_blocks(
    *,
    route_context: dict[str, Any],
    user_text: str,
    manifests: list[MediaManifest] | None = None,
) -> list[dict[str, str]]:
    # Router-consumed opt-in. Strict boolean identity (not bool(...)): route_context
    # carries arbitrary JSON-serialisable values without per-key validation, so a
    # quoted YAML value like "false" is a truthy string. Exposing the stored
    # attachment path must require an explicit boolean true. This key is deliberately
    # NOT in PROMPT_SAFE_CONTEXT_KEYS, so it never reaches the preamble.
    include_tool_path = route_context.get("attachment_tool_paths") is True
    blocks: list[dict[str, str]] = [render_route_context(context_for_prompt(route_context))]
    if user_text:
        blocks.append(text_block(escape_prompt_text(user_text)))
    for manifest in manifests or []:
        file_exists = os.path.exists(manifest.canonical_path)
        if is_image_content_type(manifest.content_type) and file_exists:
            blocks.append(image_block(manifest.canonical_path, manifest.content_type))
            # Images keep their resource_link; when opted in, also emit a manifest
            # block carrying tool_path so profile tools get the exact stored path.
            if include_tool_path:
                blocks.append(manifest_block(manifest, include_tool_path=True))
        else:
            # tool_path only when opted in AND the stored file is present (a missing
            # file has nothing to operate on).
            blocks.append(
                manifest_block(manifest, include_tool_path=include_tool_path and file_exists)
            )
    return blocks


def build_scheduled_prompt_blocks(
    *,
    route_context: dict[str, Any],
    scheduled_metadata: dict[str, Any],
    scheduled_prompt: str,
) -> list[dict[str, str]]:
    return build_synthetic_prompt_blocks(
        route_context=route_context,
        synthetic_metadata=scheduled_metadata,
        synthetic_prompt=scheduled_prompt,
    )


def build_synthetic_prompt_blocks(
    *,
    route_context: dict[str, Any],
    synthetic_metadata: dict[str, Any],
    synthetic_prompt: str,
    payload_json: str | None = None,
) -> list[dict[str, str]]:
    blocks = [
        render_route_context(context_for_prompt(route_context)),
        render_scheduled_event(synthetic_metadata),
    ]
    if payload_json is not None:
        blocks.append(text_block("synthetic_payload:\n" + escape_prompt_text(payload_json)))
    if synthetic_prompt:
        blocks.append(text_block(escape_prompt_text(synthetic_prompt)))
    return blocks
