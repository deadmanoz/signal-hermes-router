# Configuration

Start from the synthetic examples in the repo root:

```bash
cp config.example.yaml /path/to/private/config.yaml
cp routes.example.yaml /path/to/private/routes.yaml
```

Both files live outside this public tree in deployment.

## Signal endpoint safety

`router.signal.base_url` defaults to `http://127.0.0.1:8080` and must use a
loopback host (`127.0.0.1`, `localhost`, or another loopback IP) unless remote
access is explicitly enabled. This keeps the router from accidentally sending
Signal events or reply authority to an unauthenticated remote `signal-cli`
daemon.

Run `signal-cli` in single-account daemon mode, for example
`signal-cli -a "$SIGNAL_ACCOUNT" daemon --http 127.0.0.1:8080`. The router
does not send an `account` JSON-RPC parameter on each request; account selection
belongs to the upstream daemon invocation. If `signal-cli` is started without
`-a`, it enters multi-account mode and signal-cli requires per-request account
parameters that this router deliberately does not own.

To intentionally use a remote endpoint, set either
`router.allow_remote_signal_base_url: true` or
`router.signal.allow_remote_base_url: true` in the private deployment config.
For legacy flat configs that omit the `router:` wrapper, use
`allow_remote_signal_base_url: true` beside `signal_base_url`.

## Filesystem layout

The router writes private state under a handful of paths. All defaults are
relative to the working directory of the running process; in production
deployments these are typically absolute paths under the service's private
data root. The router-managed roots (`media_root` and `work_root`) are
created with `0700` permissions, and files written beneath them - including
the dedupe sqlite DB at `state_db` - are written with `0600` (see
`signal_hermes_router.private_fs`). `signal_attachment_root` is read-only
from the router's perspective and is not created or chmodded by the router.

- `router.state_db` (default `./private/state/router.db`) - sqlite database
  used by the dedupe layer (`signal_hermes_router.dedupe`) to record
  route-scoped event claims. The file itself is `0600`; its parent directory
  is created `0700`.
- `router.media_root` (default `./private/media`) - root for attachments and
  their sidecar manifests written by `signal_hermes_router.media`. See
  [media handling](media.md) for the on-disk layout.
- `router.work_root` (default `./private/work`) - root for per-profile working
  state used by the ACP subprocess supervisor
  (`signal_hermes_router.sessions`).
- `router.control.socket_path` (default `router.work_root / "control.sock"`
  when control is enabled and no explicit path is set) - local Unix socket
  used by `signal-hermes-router trigger-job` and `notify-route` to ask the
  running router to inject a configured synthetic turn.
- `router.signal_attachment_root` (default
  `~/.local/share/signal-cli/attachments`) - read-only path used to resolve
  signal-cli events that reference an attachment by ID instead of carrying
  inline bytes. The value is `expanduser`-ed at config load time. The router
  does not create or change permissions on this directory.

## Secret resolvers

String values in YAML are passed through `signal_hermes_router.secrets.resolve_secret_refs`. Supported URI schemes:

- `file:///absolute/path` - read the file contents
- `env://VARIABLE_NAME` - read the named environment variable
- `op://...` - run `op read`; the 1Password CLI must be installed and authenticated
- `systemd-credential://credential-name` - read a single credential basename from `$CREDENTIALS_DIRECTORY`

`systemd-credential://` names must be basenames. Path separators and dot
segments are rejected so a credential URI cannot escape the systemd credential
directory.

## Route states

Defined in `signal_hermes_router.models.RouteState`:

- `shadow` - store media and log redacted route decisions only; do not call Hermes, do not reply
- `active` - call Hermes and reply to Signal
- `maintenance` - store minimal event information and send one bounded maintenance reply
- `disabled` - redacted audit only; nothing else

A route's state is read at config load time. A circuit-breaker trip can override a route to `maintenance` at runtime (`signal_hermes_router.circuit`).

## Session policies

Defined in `signal_hermes_router.models.SessionPolicy`:

- `persistent_route` - one ACP session per route, shared across senders and turns
- `persistent_sender` - one ACP session per `(route, sender)` pair
- `ephemeral` - a fresh ACP session per turn

Sessions are replaced when the underlying Hermes subprocess restarts. If the Hermes profile advertises `sessionCapabilities.resume`, the router will call `session/resume` rather than creating a new session.

## Route schema

Each entry in `routes.yaml` is parsed by `signal_hermes_router.config.parse_route`.
Required keys are listed first; optional keys carry their defaults.

- `platform` (required) - transport identifier; currently only `"signal"` is
  used in production.
- `profile` (required) - Hermes profile name. Must match
  `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and must not contain path separators; the
  router supervises one `hermes -p <profile> acp` subprocess per profile.
- `chat_type` (optional, default `group`) - route target type. `group` routes
  target Signal groups. `direct` routes target one exact Signal sender identity.
- `group_id` (required for `group`, forbidden for `direct`) - opaque
  per-platform group identifier. For Signal, this is the base64 group-v2 ID
  emitted by `signal-cli`.
- `sender_id` (required for `direct`, ignored for `group`) - exact direct
  sender identity. Use the Signal `sourceUuid` value and load it from private
  config through a secret resolver such as `env://SIGNAL_DIRECT_SENDER_UUID`.
- `sender_number` (optional for `direct`, ignored for `group`) - secondary
  exact sender number used only when an inbound direct event lacks
  `sourceUuid`. If `sourceUuid` is present and does not match `sender_id`, the
  router discards the event even when `sender_number` matches.
- `session_policy` (optional, default `persistent_route`) - one of the
  [session policy](#session-policies) values.
- `state` (optional, default `shadow`) - one of the [route state](#route-states)
  values.
- `name` (optional) - stable private selector used by `scheduled_jobs` and
  `notifications`.
  Must match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and be unique when present.
  Do not use `friendly_name` for synthetic route selection.
- `route_context` (optional, default `{}`) - JSON-serialisable mapping of
  private route metadata. Only the code-controlled prompt-safe keys are sent
  to Hermes; the rest stay in `routes.yaml`. See [route context](route-context.md).
- `permissions` (optional, default `[]`) - static ACP permission allowlist
  for this route. Denylists are rejected at parse time. See [permissions](permissions.md)
  for the predicate shape.
- `friendly_name` (optional) - private operator-facing label. Never sent over
  ACP; only used in redacted logs.
- `maintenance_reply` (optional) - per-route override for
  `router.maintenance_reply` (see [Operational reply strings](#operational-reply-strings)).
- `failure_reply` (optional) - per-route override for `router.failure_reply`.

For group routes, `(platform, group_id)` must be unique across the routes list.
For direct routes, `(platform, sender_id)` must be unique, and any configured
`sender_number` must not be reused by another direct route. Direct routes do not
provide a default DM route or wildcard sender; wildcard-like identities such as
`*` are rejected at config load.

## Scheduled synthetic route jobs

`scheduled_jobs` is a top-level list in `routes.yaml`. A job is trusted
deployment config for a local scheduler; it is not a Signal event and it does
not contain raw Signal group IDs. Each job targets a route by the route's
stable `name`.

```yaml
routes:
  - platform: "signal"
    name: "agenda-route"
    group_id: "SIGNAL_GROUP_ID_BASE64_EXAMPLE"
    profile: "example-hermes-profile"
    state: "active"

scheduled_jobs:
  - id: "daily-agenda"
    route: "agenda-route"
    prompt: "Prepare the synthetic daily agenda for this route."
    description: "Optional operator note, not sent to Hermes."
```

Job keys:

- `id` (required) - safe token used by `trigger-job` and host timers. Must
  match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and be unique.
- `route` (required) - a configured route `name`.
- `prompt` (required) - trusted scheduled prompt text from private deployment
  config. Empty prompts are rejected.
- `description` (optional) - operator note. It is not sent to Hermes.
- `permissions` (optional) - static ACP permission allowlist for this one
  scheduled turn. When omitted, the route's normal `permissions` apply.

Scheduled turns use the selected route's state gate and session policy. A
`persistent_route` scheduled turn shares the route session with later Signal
messages; a `persistent_sender` scheduled turn is keyed to a synthetic sender
for that job; an `ephemeral` scheduled turn gets a fresh session.

## External route notifications

`notifications` is a top-level list in `routes.yaml`. A notification is trusted
deployment config for a local script that already has a structured result to
report. Scripts pass only a configured notification ID and a bounded JSON
payload to the router; they do not send Signal, choose raw Signal targets, or
start Hermes sessions themselves.

```yaml
notifications:
  - id: "backup-report"
    route: "agenda-route"
    prompt: "Summarize the notification payload for this route."
    description: "Optional operator note, not sent to Hermes."
```

Notification keys:

- `id` (required) - safe token used by `notify-route`. Must match
  `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and be unique within `notifications`.
- `route` (required) - a configured route `name`.
- `prompt` (required) - trusted notification prompt text from private
  deployment config. Empty prompts are rejected.
- `description` (optional) - operator note. It is not sent to Hermes.
- `permissions` (optional) - static ACP permission allowlist for this one
  notification turn. When omitted, the route's normal `permissions` apply.

Notification payloads must be JSON objects or arrays. The CLI and router both
canonicalize the payload to compact JSON with sorted object keys before
applying `router.control.max_notification_payload_bytes`.

## Router control socket

`router.control` is disabled by default. When enabled, the running router
serves a local Unix socket and accepts JSON-lines control commands. The CLI
uses that socket; it does not send Signal, start Hermes, or call ACP on its
own.

```yaml
router:
  work_root: "./private/work"
  control:
    enabled: true
    # Optional. Defaults to ./private/work/control.sock for this work_root.
    socket_path: "./private/work/control.sock"
    # 0 means acquire-or-return-busy immediately.
    route_lock_timeout_seconds: 0
    # Compact JSON bytes after canonicalization. Default: 16384.
    max_notification_payload_bytes: 16384
```

The socket path must be under `router.work_root`. The socket parent is created
with `0700` permissions and the socket is chmodded to `0600` where the platform
supports it. Startup refuses a path outside `router.work_root`, a non-socket
file at the configured path, or a live socket already accepting connections
there. A stale socket is removed only after the router proves no listener is
accepting connections there.

Use `trigger-job` from a host scheduler:

```bash
signal-hermes-router --config /path/to/private/config.yaml trigger-job daily-agenda --scheduled-at 1714521600000 --idempotency-key daily-agenda-1714521600000
```

Use `notify-route` from a local script:

```bash
signal-hermes-router --config /path/to/private/config.yaml notify-route backup-report --payload-file /path/to/private/payload.json --idempotency-key backup-report-1714521600000
```

To include one trusted image with a configured notification, stage it under
`router.media_root` and pass its absolute path:

```bash
signal-hermes-router --config /path/to/private/config.yaml notify-route camera-person --payload-file /path/to/private/payload.json --attachment /path/to/private/media/camera/person.png --idempotency-key camera-person-1714521600000
```

Use `preflight-permissions` before route activation or allowlist changes:

```bash
signal-hermes-router --config /path/to/private/config.yaml --routes /path/to/private/routes.yaml preflight-permissions --active-only --probe-contract-file /path/to/private/probe-contract.json --json
```

To inspect profiles through the running router's normal ACP supervisor, target
the control socket instead of supplying a recorded contract:

```bash
signal-hermes-router --config /path/to/private/config.yaml preflight-permissions --active-only --control-socket /path/to/private/control.sock --json
```

`--scheduled-at` accepts either an epoch millisecond integer or a timezone-aware
ISO 8601 timestamp. Naive datetimes are rejected. `--idempotency-key` is hashed
before it is used in the dedupe identity. Reusing the same `--scheduled-at` or
the same idempotency key dedupes repeated timer attempts for the same job fire.
`--client-timeout` bounds the local control socket round trip and defaults to
300 seconds. `notify-route` reads `--payload-file` as UTF-8 JSON, rejects
non-object and non-array payloads, and applies the configured compact JSON byte
limit before writing to the socket. The router repeats that validation and
returns a JSON `payload_too_large` error for marginally over-limit requests
that fit inside the control request headroom.

`notify-route --attachment` accepts one image path. The router rejects
non-arrays from the control socket, multiple paths, non-string or relative
paths, paths that escape `router.media_root`, missing paths, non-files,
oversize files, non-private file or parent modes, and paths whose filename
does not infer an `image/*` content type. MIME gating is filename-based through
Python's `mimetypes`; stage PNG, JPEG, GIF, or WebP files rather than relying
on magic-byte sniffing or platform HEIC mappings. The producer must stage
images under `router.media_root` using `0700` directories and `0600` files.

Before the ACP turn runs, the router copies each accepted image to a private
router-owned `.outbound` artifact under `router.media_root`. Signal-cli sees
that frozen path, not the producer's original path, and the router deletes the
frozen artifact after the send attempt. Attachment sends require a loopback
`router.signal_base_url` because signal-cli must read the same filesystem path;
remote signal-cli base URLs are rejected for attachment-bearing notifications
even when `allow_remote_signal_base_url` permits text-only routing.

`preflight-permissions` compares configured permission tool names against a
recorded ACP tool-surface contract in offline mode, or against structured
tool-surface data from profiles managed by the running router when invoked
through the control socket. The live path uses the router's normal
`ProfileSupervisor`, so probing an idle profile can start that profile's ACP
subprocess and supervisor cooldowns still apply. If a profile is already
handling a turn when preflight starts probing it, live preflight reports
`probe_profile_busy` instead of waiting behind that turn. Live profile
inspection reads `agentCapabilities._meta` first and then tries the optional
JSON-RPC extension method `_tool_surface/list`. Agents that expose neither
source report `probe_unsupported`.

Reports use only `route:<name>` or `routes[<index>]` references, profile names,
source IDs, and tool names. They do not report raw Signal IDs, direct sender
IDs, route keys, secrets, or permission argument predicates.

CLI exit status is zero for `delivered`, `deduped`, `busy`, and expected
`skipped` outcomes such as shadow or disabled routes. It is non-zero for an
unavailable socket, malformed request or response, unknown synthetic ID, config
parse error, or router-reported `error`.
For `preflight-permissions`, exit status is zero only when the report status is
`ok`.

## Runtime size limits

`router.max_attachment_bytes` bounds inline Signal attachment decoding and
path-backed attachment reads. The default is `26214400` bytes (25 MiB).
Oversize attachments raise before storage or ACP delivery. For path-backed
attachments the dedupe claim is released so the event can be retried after
config or source data is corrected; for inline base64 attachments the
size check fires inside event parsing before any dedupe claim is taken,
so the event is retried only if the upstream Signal stream replays it.

`router.max_signal_event_bytes` bounds each Signal SSE event before JSON parsing.
When omitted, it defaults to twice `max_attachment_bytes`; with the default
attachment cap this is `52428800` bytes (50 MiB). This leaves room for base64
encoding overhead around a maximum-size inline attachment while still preventing
unbounded event accumulation.

`router.max_acp_line_bytes` bounds each ACP JSON-RPC stdout line from the Hermes
subprocess. The default is `8388608` bytes (8 MiB). This is a defensive
allocation cap, not an ACP protocol limit; if a single Hermes stdout line
exceeds the cap, the offending line is logged and skipped and the peer is
kept alive. The same byte cap is applied to the Hermes stderr stream so an
oversized stderr line cannot back-pressure the subprocess into deadlock.

`router.max_reply_chars` bounds each outbound Signal reply after any route
canary prefix is applied. The default is `12000` characters. Oversize replies
are truncated and marked before they are sent; this is an operational
spam/resource guardrail, not a Signal protocol limit.

`router.max_signal_message_bytes` bounds each individual Signal message
dispatched by the router, measured in UTF-8 bytes (not characters - non-ASCII
text like emoji or CJK can produce more bytes than characters). Replies
longer than this are split into multiple sequential messages prefixed with
`[N/M] ` ordering markers; single-chunk replies are sent without a marker.
The default is `1900` bytes, chosen to sit safely below Signal-Desktop's
2048-byte long-attachment threshold (the marker itself consumes part of each
chunk's budget). Values below `16` are rejected at config load; values above
`2000` are accepted with a warning - Signal-Android may silently truncate the
body and Signal-Desktop may convert it to an attachment. At pathologically
tight settings (a budget close to the 16-byte floor combined with very long
multibyte input), the marker may not fit alongside even one codepoint of
body; the router then falls back to safe unmarked chunks at the byte cap.
Production defaults have ample marker headroom.

## Hermes turn timeout and busy notice

`router.acp_prompt_timeout_seconds` bounds each `session/prompt` JSON-RPC
request to the Hermes subprocess. The default is `300` seconds (5 minutes).
When the timeout is exceeded the router restarts the Hermes profile and
records a circuit-breaker failure for the route. Other ACP requests
(`initialize`, `session/new`, `session/resume`) use a fixed 5-minute timeout
and are not affected by this key.

`router.busy_notice_after_seconds` (default `120`) controls how long a turn
may run before a one-shot busy notice is sent to the Signal target. To keep
the notice meaningful, configure
`busy_notice_after_seconds < acp_prompt_timeout_seconds`.

## Circuit breaker

The per-route circuit breaker (`signal_hermes_router.circuit`) tracks Hermes
turn failures and parks repeatedly-failing routes in `maintenance` state
until cooldown elapses.

- `router.circuit_breaker.failures` (default `3`) - failures within
  `window_seconds` required to trip the breaker.
- `router.circuit_breaker.window_seconds` (default `300`) - sliding window
  for the failure count. Failures older than this are discarded.
- `router.circuit_breaker.recovery_seconds` (default `300`) - cooldown after
  a trip. When this much time has elapsed since the trip, the next event for
  the route clears the override and the route is evaluated in its configured
  state for one probe. A successful probe leaves the route running. A failed
  probe starts fresh failure counting (the route does not immediately
  re-trip on the probe failure alone), so a transient fault during recovery
  does not lock the route in maintenance.

For low-volume routes, consider raising `failures` and `window_seconds` to
avoid tripping on a single bad afternoon; a tripped route requires
`recovery_seconds` of quiet before the next probe.

## Operational reply strings

These bounded canned strings are sent back to Signal from non-Hermes code
paths (state gates, circuit-breaker trip, send-side failure handling, busy
notice). Each can be overridden per route in `routes.yaml` (`maintenance_reply`
and `failure_reply` only); otherwise the router-level default applies.

- `router.maintenance_reply` (default `"This route is temporarily under
  maintenance."`) - sent on every event for a route in `maintenance` state,
  whether that state was configured directly or installed by the circuit
  breaker.
- `router.failure_reply` (default `"I hit an internal router error handling
  that message."`) - sent when a Hermes turn fails but the failure did not
  trip the circuit breaker.
- `router.busy_notice` (default `"Still working on this."`) - the one-shot
  notice fired at `busy_notice_after_seconds` if the turn has not completed.

Per-route overrides in `routes.yaml`:

```yaml
routes:
  - platform: "signal"
    group_id: "SIGNAL_GROUP_ID_BASE64_EXAMPLE"
    profile: "example-hermes-profile"
    state: "active"
    maintenance_reply: "Custom maintenance text for this route."
    failure_reply: "Custom failure text for this route."
```

All four operational replies (`maintenance_reply`, `failure_reply`,
`busy_notice`, and assistant replies from Hermes) flow through the same
canary prefix and chunking pipeline as ordinary assistant text - see
[Runtime size limits](#runtime-size-limits) above.

Notification image attachments are attached only to the first Signal reply
chunk. If Hermes returns empty text for an attachment-bearing notification, the
router sends `Image attached.` through the same canary prefix and chunking
pipeline.

## Inbound discard and observability

The router discards unrouteable Signal events - non-group non-direct events,
unknown shapes, events for group IDs with no configured route, and direct
messages from non-matching senders - without normalising message text, decoding
attachments, writing media, taking dedupe claims, or calling ACP.
Allowlisted direct `dataMessage` events are routed like group events, subject to
the exact `sender_id` / `sender_number` matching rules above. Each discarded
event produces exactly one content-free summary (`shape`, `message_type`,
`has_group`). Routine non-message events are logged at DEBUG, unknown inbound
envelopes at INFO, and receive exception envelopes at WARNING with
`has_exception=true`. No sender identifier, group ID value, message text,
attachment filename, exception message, or attachment payload appears in these
summaries.

The router avoids retaining unrouteable payloads in router-owned objects, but
Python cannot guarantee byte-level zeroisation of transient raw JSON/string
buffers already allocated by the HTTP/SSE layer or CPython internals.
