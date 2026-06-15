from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from .mime import content_type_for_path, is_image_content_type
from .models import MediaManifest

# This set is deliberately code-controlled, not config-controlled. Adding a key
# here exposes that route_context value to the LLM via the prompt preamble.
PROMPT_SAFE_CONTEXT_KEYS = frozenset({"purpose", "route_alias"})


def new_context_nonce() -> str:
    return uuid.uuid4().hex


def context_for_prompt(route_context: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in route_context.items() if key in PROMPT_SAFE_CONTEXT_KEYS}


def render_route_context(route_context: dict[str, Any], nonce: str | None = None) -> dict[str, str]:
    nonce = nonce or new_context_nonce()
    body = json.dumps(route_context, sort_keys=True, separators=(",", ":"))
    return {"type": "text", "text": f"[route_context:{nonce}]{body}[/route_context:{nonce}]"}


def render_scheduled_event(metadata: dict[str, Any], nonce: str | None = None) -> dict[str, str]:
    nonce = nonce or new_context_nonce()
    body = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
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


def manifest_block(manifest: MediaManifest) -> dict[str, str]:
    return text_block(manifest.to_text())


def build_prompt_blocks(
    *,
    route_context: dict[str, Any],
    user_text: str,
    manifests: list[MediaManifest] | None = None,
) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = [render_route_context(context_for_prompt(route_context))]
    if user_text:
        blocks.append(text_block(escape_prompt_text(user_text)))
    for manifest in manifests or []:
        if is_image_content_type(manifest.content_type) and os.path.exists(manifest.canonical_path):
            blocks.append(image_block(manifest.canonical_path, manifest.content_type))
        else:
            blocks.append(manifest_block(manifest))
    return blocks


def build_scheduled_prompt_blocks(
    *,
    route_context: dict[str, Any],
    scheduled_metadata: dict[str, Any],
    scheduled_prompt: str,
) -> list[dict[str, str]]:
    blocks = [
        render_route_context(context_for_prompt(route_context)),
        render_scheduled_event(scheduled_metadata),
    ]
    if scheduled_prompt:
        blocks.append(text_block(escape_prompt_text(scheduled_prompt)))
    return blocks
