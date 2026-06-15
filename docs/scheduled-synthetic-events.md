# Scheduled Synthetic Route Events

Scheduled synthetic route events let a trusted local scheduler trigger a
configured Signal route through the running router. The scheduler supplies a
job ID, not a Signal group ID. The router looks up the job, injects a synthetic
turn for the target route, calls Hermes through the normal ACP supervisor, and
delivers any assistant reply through the normal Signal outbound path.

Use this when a host timer needs to send a routed Signal report but you still
want router-owned route state, session policy, permission allowlists, chunking,
Signal retries, redacted logs, dedupe, and circuit breaker behavior.

Do not use a Hermes Signal gateway cron job or a direct `signal-cli send` for
these route-owned messages. Those paths bypass the router's route policy and
can create a separate ACP session, a second Signal sender, or unredacted
delivery behavior.

## Setup

1. Give the target route a stable private `name` in `routes.yaml`.
2. Add a top-level `scheduled_jobs` entry that references that route name.
3. Enable `router.control` in `config.yaml`.
4. Start the router normally.
5. Run `signal-hermes-router trigger-job <job-id>` from the same host.

Example route and job:

```yaml
routes:
  - platform: "signal"
    name: "agenda-route"
    group_id: "SIGNAL_GROUP_ID_BASE64_EXAMPLE"
    profile: "example-hermes-profile"
    session_policy: "persistent_route"
    state: "active"
    route_context:
      purpose: "Synthetic public example route."
      route_alias: "agenda"

scheduled_jobs:
  - id: "daily-agenda"
    route: "agenda-route"
    prompt: "Prepare the synthetic daily agenda for this route."
```

Example control config:

```yaml
router:
  work_root: "./private/work"
  control:
    enabled: true
    socket_path: "./private/work/control.sock"
    route_lock_timeout_seconds: 0
```

With that router already running:

```bash
signal-hermes-router --config /path/to/private/config.yaml trigger-job daily-agenda --scheduled-at 1714521600000 --idempotency-key daily-agenda-1714521600000
```

`trigger-job` only talks to the local control socket. It does not send Signal,
start Hermes, create sessions, or read route targets itself.

If `socket_path` is set explicitly, it must stay under `router.work_root`. This
keeps the socket inside a router-owned private directory instead of changing
permissions on shared directories such as `/tmp` or `/run`.

`--client-timeout` bounds the local control socket round trip and defaults to
300 seconds. `--timeout` is forwarded to the router as the route/profile lock
wait budget.

## Prompt Shape

Synthetic turns use the same prompt-safe route context preamble as Signal
turns. They then add a generated scheduled-event metadata block containing
safe fields such as origin, job ID, scheduled time, and trigger time. The
configured job prompt is a separate text block after that metadata.

The router escapes route-context and scheduled-event delimiters in both Signal
user text and configured scheduled prompt text. A job prompt cannot inject fake
router metadata by including delimiter-looking text.

## Route States

Scheduled turns use the same route state gate as inbound Signal turns:

- `active`: call Hermes and deliver the assistant reply through Signal.
- `shadow`: do not call Hermes and do not send Signal output.
- `maintenance`: send the route's maintenance reply through Signal.
- `disabled`: do not call Hermes and do not send Signal output.

The first implementation is text-only for scheduled jobs. Synthetic turns do
not store media.

## Session Policy

Scheduled turns use the selected route's session policy:

- `persistent_route`: reuse the route session. Later human Signal messages can
  discuss the scheduled report in the same ACP session.
- `persistent_sender`: use a scheduler-specific sender identity for the job,
  such as `scheduled:daily-agenda`.
- `ephemeral`: create a fresh session for each scheduled turn.

## Idempotency

Use `--scheduled-at` for timer fires with a natural scheduled instant. It can
be an epoch millisecond integer or a timezone-aware ISO 8601 timestamp.

Use `--idempotency-key` when the scheduler has a stable fire identifier. The
router hashes the key before using it in the dedupe identity, so the raw key is
not stored in the dedupe database.

Repeated triggers with the same job and `--scheduled-at`, or the same
`--idempotency-key`, are deduped. Bare manual triggers without either field
are treated as fresh attempts, even when two invocations land in the same
millisecond.

## Route Lock Contention

The router holds one async lock per route while a turn is in flight. That lock
covers dedupe claim, state gate, session lookup, permission policy install,
Hermes prompt, Signal reply, and dedupe cleanup. This prevents a scheduled
turn and a human Signal turn for the same route session from swapping
permission policies mid-turn.

`router.control.route_lock_timeout_seconds: 0` means acquire the lock
immediately or return `busy`. A `busy` response is not a failure, does not call
Hermes, does not send Signal output, and does not write a dedupe row. Retrying
the same stable fire identity can still deliver later.

Positive timeout values wait up to that many seconds before returning `busy`.

## Permission Overrides

If a job defines `permissions`, that static allowlist applies only to the
scheduled turn. The next ordinary Signal turn on the same route restores the
route's normal permission policy. If a job omits `permissions`, the route
policy is used.

Prefer narrow job permissions. Scheduled prompts are deployment-owned, but the
profile still runs as the same Hermes profile and can request tools during the
turn.

## Shared Health

Scheduled and human-origin turns share the same route circuit breaker and
maintenance behavior. A failing scheduled turn can trip the route breaker that
also protects human Signal traffic. A scheduled trigger during maintenance can
send the route's maintenance reply. This coupling is intentional for the first
implementation because scheduled delivery is route-owned delivery.

## Timer Examples

Cron example:

```cron
5 6 * * * /path/to/venv/bin/signal-hermes-router --config /path/to/private/config.yaml trigger-job daily-agenda --scheduled-at "$(date -u +\%Y-\%m-\%dT06:05:00+00:00)" --idempotency-key "daily-agenda-$(date -u +\%Y\%m\%d)"
```

Systemd service example:

```ini
[Unit]
Description=Trigger synthetic daily agenda route event

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'exec /path/to/venv/bin/signal-hermes-router --config /path/to/private/config.yaml trigger-job daily-agenda --scheduled-at "$$(date -u +%%Y-%%m-%%dT%%H:%%M:%%S+00:00)"'
```

Systemd timer example:

```ini
[Unit]
Description=Run synthetic daily agenda route event

[Timer]
OnCalendar=*-*-* 06:05:00
Persistent=true

[Install]
WantedBy=timers.target
```

## Public Boundary

Keep real Signal group IDs, sender UUIDs, phone numbers, hostnames, private
route names, profile names, credential names, and real scheduled prompts in
the private deployment repo. The public examples in this repo use synthetic
identifiers only.
