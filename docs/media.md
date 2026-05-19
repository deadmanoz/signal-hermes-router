# Media handling

## Storage layout

```
${MEDIA_ROOT}/${platform}/${YYYY}/${MM}/${sha256_prefix}/${safe_filename}
```

Filenames are whitelist-sanitised: non-`[A-Za-z0-9._-]` runs are replaced with a single underscore, repeated underscores are then collapsed, the stem is capped at 80 characters, and an extension derived from the content type is appended (so the final filename can be a few characters longer than 80). Signal-provided extensions are not trusted. Each saved attachment also gets a sidecar manifest JSON written alongside it as `<safe_filename>.manifest.json`.

If the target filename already exists on disk with different content (sha256 mismatch), the router inserts `-<digest[:8]>` before the extension and writes the new attachment under that disambiguated name. Identical content under the same name is treated as already-stored and only has its file permissions reasserted.

Attachment ingestion is bounded by `router.max_attachment_bytes`, defaulting to
25 MiB. The limit is checked before inline base64 decode and before reading
path-backed attachment files into memory.

## Manifest contents

Private sidecar manifests include:

- `display_filename`
- canonical path
- content type
- size
- SHA-256
- redacted group and sender refs
- Signal timestamp

Raw original filenames are not persisted.

Prompt-visible text manifests omit the canonical local path. They include only
the display filename, content type, size, SHA-256, redacted group/sender refs,
and Signal timestamp.

## ACP delivery

Attachments with `image/*` content types are sent to ACP as `type: resource_link` content blocks with a `file://` URI. Hermes can read the local file from the ACP server process and inline the pixels for downstream model providers. Everything else — audio, video, PDFs, text, archives, `application/octet-stream`, and images whose stored file is missing — is sent as a text manifest block.

Router core does not transcribe, OCR, or summarise media. That is profile behaviour.

## Attachment-by-ID resolution

When signal-cli events reference an attachment by ID instead of carrying inline bytes, the router resolves the ID under `signal_attachment_root`, defaulting to `~/.local/share/signal-cli/attachments`.

## PII

Treat media roots, manifests, Hermes `state.db`, profile `skills/`, and audit checklists as PII-bearing. Encrypt off-host backups.
