# Trade-offs vs. the Hermes Signal Gateway

Hermes ships a built-in Signal gateway. `signal-hermes-router` is **not** a drop-in replacement for it - the two have different scopes. The router exists to fan one Signal account out to many independent Hermes profiles. The built-in gateway can monitor multiple Signal groups from one configured account, but those groups feed one profile-scoped gateway process; it does not provide native group-to-profile routing from one Signal identity.

This page is a grounded comparison so an operator picking between the two knows what they're gaining and giving up. Behaviour described for the Hermes gateway is taken from the upstream [Signal integration docs](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/signal) and upstream source links; behaviour described for the router is taken from this repo's source.

## Why this router exists

The router is justified by a narrow gap in the upstream Hermes gateway model:

- **Profiles own gateways.** Hermes's profile docs say "Each profile runs its own gateway as a separate process", and the same page's token-lock section lists Signal as a protected platform ([Hermes profiles docs](https://hermes-agent.nousresearch.com/docs/user-guide/profiles/)).
- **The Signal adapter is single-account shaped.** The adapter requires one configured account, stores it as `self.account`, subscribes to `signal-cli` events with that account, and includes that same account in outbound JSON-RPC calls ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py)).
- **Account sharing is deliberately blocked.** The Signal adapter acquires a `signal-phone` scoped lock for the configured account, and Hermes's scoped-lock helper says these locks prevent multiple gateways using "the same external identity at once" ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py), [status.py](https://github.com/NousResearch/hermes-agent/blob/main/gateway/status.py#L578-L583)).
- **Group allowlisting is not profile routing.** The Signal adapter filters group messages through `SIGNAL_GROUP_ALLOWED_USERS`, but accepted groups continue into the same adapter and therefore the same profile ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py)).
- **Upstream multi-profile gateway work is a design question, not a shipped mode.** The broader Hermes issue for "multi-profile deployments in a single gateway process" describes today's canonical shape as N gateway processes, one per profile, and asks whether one gateway hosting many profiles should exist ([NousResearch/hermes-agent#23735](https://github.com/NousResearch/hermes-agent/issues/23735)).

Changing Hermes directly would mean teaching the gateway to load multiple profiles at once, select a profile from incoming Signal metadata, and keep session stores, memory, skills, permissions, lifecycle, observability, and platform routing isolated per profile. That is a much wider maintenance surface than this repo needs. `signal-hermes-router` keeps Hermes unchanged: it consumes Signal once, routes by explicit group or direct-message targets, and talks to each profile through the stable ACP subprocess boundary.

## What the router does not provide (that the Hermes Signal gateway does)

### Outbound media of any kind

The Hermes gateway can send native Signal attachments:

- `send_image_file` / `send_multiple_images` - PNG, JPEG, GIF, WebP
- `send_voice` - OGG, MP3, WAV, M4A, AAC
- `send_video` - MP4
- `send_document` - any file type (PDF, ZIP, etc.)

The router only sends text out today. A narrow router-owned outbound media contract (so a profile-side plugin can hand the router a validated local attachment reference to deliver alongside the reply) is planned future work, but not implemented - see the README intro for the shape.

### Rich text in replies

The Hermes gateway converts supported markdown - `**bold**`, `*italic*`, `` `code` ``, `~~strike~~`, headings - into Signal `bodyRanges`, so the message renders with formatting in Signal clients.

The router sends plain text. Any markdown in the reply is delivered verbatim as characters.

### Reactions

The Hermes gateway can react to messages via the Signal reaction API. The router does not send reactions.

### Native reply quoting

The Hermes gateway posts replies that natively quote the original message, and surfaces inbound reply-quote relationships to the agent. The router's Signal reply calls pass only the target (`groupId` for groups, `recipient` for explicit direct routes) plus `message`, and the normalised event shape (`NormalizedEvent` in [src/signal_hermes_router/models.py](../src/signal_hermes_router/models.py)) carries no quote metadata, so quote relationships are neither sent nor surfaced.

### Inbound voice transcription

If Whisper is configured, the Hermes gateway transcribes incoming voice messages before handing them to the agent. The router stores audio attachments under the media tree and surfaces them to Hermes as attachment manifest text; any transcription is a profile-side concern, not a router concern.

### DM (1:1) message support

The Hermes gateway handles both DMs and groups, with separate allowlists (`SIGNAL_ALLOWED_USERS`, DM pairing codes, "Note to Self" support with echo-back protection).

The router is **explicit-route only by design**. Group routes map a Signal group ID to a Hermes profile. Direct-message routes are opt-in, require an exact configured sender identity, and do not provide a wildcard DM fallback. DMs from non-matching senders are discarded before attachment parsing, media storage, dedupe, or ACP delivery.

### Interactive setup and pairing flow

The Hermes gateway provides `hermes gateway setup` (interactive wizard for platform selection, signal-cli verification, HTTP URL, allowed users, access policies), `hermes gateway install` / `--system`, and `hermes pairing approve signal CODE`.

The router has no equivalent. It is configured by editing YAML files (`config.yaml`, `routes.yaml`) and started directly as a long-running process, typically under systemd. signal-cli linking is performed out of band (`signal-cli link -n "..."`) - the same as for the Hermes gateway, but the router does not wrap or automate the wizard.

## Transport basics the router provides

The router is not a regression on the common transport basics:

- **signal-cli daemon HTTP/SSE transport** - same upstream protocol (`GET /api/v1/events`, `POST /api/v1/rpc`).
- **SSE auto-reconnect with exponential backoff** - `2s → 60s`, matching the Hermes adapter (`SignalHttpClient` in [src/signal_hermes_router/signal.py](../src/signal_hermes_router/signal.py)).
- **Typing indicators while a reply is composed** - via `sendTyping` JSON-RPC, started before the ACP turn and stopped afterward.
- **Phone number redaction in logs** - applied through the router-wide redaction layer ([src/signal_hermes_router/redaction.py](../src/signal_hermes_router/redaction.py)).
- **Long-reply chunking with `[i/m]` markers** - applied automatically in [src/signal_hermes_router/outbound.py](../src/signal_hermes_router/outbound.py). The Hermes Signal docs do not describe long-text chunking behavior.

## Streaming replies: not a Signal-side difference

Hermes's platform comparison table lists "streaming/progressive updates" as supported, which might suggest the built-in gateway streams a reply into a Signal chat token by token while the router does not. It does not, and this is worth stating explicitly so the table doesn't mislead.

Hermes's gateway has a `StreamConsumer` (`gateway/stream_consumer.py`) that progressively edits a single message in place as the agent generates tokens - but only on platforms that support message editing (Telegram, Discord, Matrix). Signal is not one of them. The Hermes Signal adapter sets `SUPPORTS_MESSAGE_EDITING = False` ("Signal has no real edit API for already-sent messages") and returns no editable message id, so the stream consumer takes its "Editing not supported - skip intermediate updates" branch and delivers the finished reply once via the non-edit fallback path (chunked by `MAX_MESSAGE_LENGTH` if long). A Signal user of the built-in gateway sees a typing indicator during the turn, then the completed reply as a single (length-chunked) message - not text that grows token by token.

The router behaves the same way, and structurally so. `ACPProfile.prompt` ([src/signal_hermes_router/acp.py](../src/signal_hermes_router/acp.py)) awaits the full `session/prompt` JSON-RPC response, *then* drains the buffered ACP `session/update` notifications and concatenates them into one reply. The streaming `session/update` channel does arrive incrementally over stdio while the turn runs - each notification is buffered into a per-session queue - but the router never consumes it mid-turn; it reads the queue once, after the turn completes.

So neither side delivers token-by-token replies to Signal. The only in-turn liveness cue either offers is the typing indicator, which the router replicates.

## What the router adds over the Hermes Signal gateway

These are the reasons to choose the router in the first place, summarised here for completeness:

- **Multiple Hermes profiles from a single Signal number**, with one supervised `hermes -p <profile> acp` subprocess per active profile.
- **Route states** - `shadow`, `active`, `maintenance`, `disabled` - controlling whether a route ingests, replies, or only logs.
- **Per-route session policies** - `persistent_route`, `persistent_sender`, `ephemeral` - controlling whether a group shares one ACP session, splits per sender, or starts fresh each turn.
- **Per-route circuit breaker**, with configurable maintenance/failure replies.
- **Route-scoped event dedupe** in a sqlite store: turns the router completed (`handled`) can't be re-played across a router restart. Claims left `processing` by a crash mid-turn are reclaimed at the next startup (the router owns the state DB exclusively, so no turn is in flight then). The resulting at-least-once behaviour applies to externally retried synthetic work with a stable idempotency key - the reclaimed claim lets the caller's retry deliver - a deliberate trade against silently dropping those retries. Signal-origin work carries no router-owned replay guarantee: a Signal turn interrupted by a crash or abandoned at the shutdown deadline is redelivered only if upstream `signal-cli` happens to replay the event.
- **Route-context preamble** injected into each prompt so a profile knows which route target the turn belongs to (prompt-safe keys only; see [docs/route-context.md](route-context.md)).
- **Static ACP permission allowlist** that answers `session/request_permission` from config, so a long-running agent doesn't block on operator input mid-turn (see [docs/permissions.md](permissions.md)).
- **Scheduled synthetic route events** through a local control socket, so host timers can trigger a route-owned turn without a second Signal gateway, direct `signal-cli` send, or profile-side ACP session split.

## Summary

If you run one Hermes profile against one Signal number and want every Signal feature out of the box (image/voice/video replies, native quotes, reactions, markdown formatting, voice transcription, DM support), use the **Hermes Signal gateway**.

If you need more than one Hermes profile behind a single Signal number - at the cost of text-only replies for now, no reactions, no native quoting, and explicit-route-only ingest - use **`signal-hermes-router`**.
