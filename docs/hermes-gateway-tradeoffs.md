# Trade-offs vs. the Hermes Signal Gateway

Hermes ships a built-in Signal gateway. `signal-hermes-router` is **not** a drop-in replacement for it - the two have different scopes. The router fans one Signal account out to many independent Hermes profiles, each running as its own supervised `hermes acp` subprocess. Since Hermes `v2026.6.19`, the built-in gateway can also route one shared Signal account's groups to multiple profiles natively, via the opt-in multiplexing gateway (`gateway.multiplex_profiles`) and `gateway.profile_routes` - see [The upstream multiplexing gateway](#the-upstream-multiplexing-gateway-v2026619) below. The remaining difference is the execution boundary and the policy surface, not the routing shape itself.

This page is a grounded comparison so an operator picking between the two knows what they're gaining and giving up. Behaviour described for the Hermes gateway is taken from the upstream [Signal integration docs](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/signal), the upstream [multi-profile gateway docs](https://hermes-agent.nousresearch.com/docs/user-guide/multi-profile-gateways), and upstream source links; behaviour described for the router is taken from this repo's source. The multiplexing-gateway section reflects upstream as of July 2026; check the upstream docs for later changes.

## Why this router exists

The router is justified by a narrow gap in the upstream Hermes gateway model:

- **Profiles own gateways.** Hermes's profile docs say "Each profile runs its own gateway as a separate process", and the same page's token-lock section lists Signal as a protected platform ([Hermes profiles docs](https://hermes-agent.nousresearch.com/docs/user-guide/profiles/)).
- **The Signal adapter is single-account shaped.** The adapter requires one configured account, stores it as `self.account`, subscribes to `signal-cli` events with that account, and includes that same account in outbound JSON-RPC calls ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py)).
- **Account sharing is deliberately blocked.** The Signal adapter acquires a `signal-phone` scoped lock for the configured account, and Hermes's scoped-lock helper says these locks prevent multiple gateways using "the same external identity at once" ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py), [status.py](https://github.com/NousResearch/hermes-agent/blob/main/gateway/status.py#L578-L583)).
- **Group allowlisting is not profile routing in the default (non-multiplexed) mode.** The Signal adapter filters group messages through `SIGNAL_GROUP_ALLOWED_USERS`, but accepted groups continue into the same adapter and therefore the same profile ([signal.py](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/gateway/platforms/signal.py)).
- **Upstream now ships a multiplexing gateway that closes the routing gap.** Hermes `v2026.6.19` added `gateway.multiplex_profiles: true` (one default-profile gateway process serving every profile) and `gateway.profile_routes` (per-chat routing to named profiles on a shared credential, on every platform adapter including Signal). The design discussion this repo originally cited as open is now resolved and implemented ([Hermes multi-profile gateway docs](https://hermes-agent.nousresearch.com/docs/user-guide/multi-profile-gateways), [NousResearch/hermes-agent#23735](https://github.com/NousResearch/hermes-agent/issues/23735)).

That shipped mode changes the honest justification for this router. The original gap - "one Signal number cannot natively feed many Hermes profiles" - no longer exists. What the router still offers is a different boundary: each profile runs as its own OS process behind the stable ACP protocol, rather than co-resident inside one Hermes gateway process. The sections below compare the two shapes directly.

## The upstream multiplexing gateway (v2026.6.19+)

Since `v2026.6.19`, Hermes supports the shape this router was built for, natively. Setting `gateway.multiplex_profiles: true` on the default profile turns its gateway into a single inbound process that serves every profile on the host; `gateway.profile_routes` then routes specific chats to named profiles even when they share one bot credential:

```yaml
gateway:
  multiplex_profiles: true
  profile_routes:
    - name: group-a
      platform: signal
      chat_id: "<signal-group-id>"
      profile: profile-a
```

Per the upstream [multi-profile gateway docs](https://hermes-agent.nousresearch.com/docs/user-guide/multi-profile-gateways): routes match most-specific-first (`thread_id` > `chat_id` > `guild_id`), work on every platform adapter (Signal included), each routed turn resolves that profile's own config, skills, memory, and credentials, and session keys are namespaced per profile. This is a real, shipped alternative to this router, and for many deployments it is now the simpler choice.

The honest differences that remain, in both directions:

**Where the multiplexing gateway is the better choice**

- **One less moving part.** No separate router process, no ACP layer, no second config surface - routing policy lives in Hermes's own config, and upgrades ship with `hermes update`.
- **Full Signal feature set.** Everything in [What the router does not provide](#what-the-router-does-not-provide-that-the-hermes-signal-gateway-does) below - rich outbound media, markdown formatting, reactions, native quoting, voice transcription, general DM support - is available on every routed profile.
- **No ACP permission question.** The gateway is the agent, so the router's static permission allowlist and preflight machinery (an artifact of being an ACP client) simply doesn't exist.

**Where the router still differs**

- **Per-profile process isolation.** The multiplexer runs every profile inside one OS process; the upstream docs themselves recommend one-process-per-profile when you want "separate memory footprints, independent crash domains, the ability to restart one profile without touching the others." The router keeps that isolation - one supervised `hermes -p <profile> acp` subprocess per active profile - *while* sharing a single Signal account, which the upstream one-process-per-profile shape cannot do (the `signal-phone` lock forbids it).
- **Agent-agnostic boundary.** The router speaks ACP over stdio; the profile side can be any agent that implements the ACP server contract, not only Hermes. The multiplexing gateway is Hermes-only, in-process.
- **Explicit-route-only ingest.** In multiplex mode, a message that matches no `profile_routes` entry falls through to the default/active profile. The router has no fallback: Signal traffic that matches no configured route is discarded before media storage, dedupe, or ACP delivery. Which posture you want depends on whether an unrouted group should get silence or the default agent.
- **Router-owned policy surface.** Route states (`shadow` / `active` / `maintenance` / `disabled`), per-route session policies, per-route circuit breakers, sqlite route-scoped dedupe with crash reclaim, live `routes.yaml` reload, and the local control socket for synthetic turns (`trigger-job`, `notify-route`) have no direct equivalent in the multiplexed gateway. (Hermes has its own cron and webhook facilities; whether they cover a given synthetic-event use case is a per-deployment comparison this page does not attempt.)

One caveat we have not verified: how the multiplexed Signal adapter keys `chat_id` for groups (group ID format, and whether direct chats route as expected) should be confirmed against a live deployment before migrating routes.

## What the router does not provide (that the Hermes Signal gateway does)

### Outbound media

The Hermes gateway can send native Signal attachments of many kinds:

- `send_image_file` / `send_multiple_images` - PNG, JPEG, GIF, WebP
- `send_voice` - OGG, MP3, WAV, M4A, AAC
- `send_video` - MP4
- `send_document` - any file type (PDF, ZIP, etc.)

The router sends text for all replies, and may send **one outbound image per notification** via `notify-route --attachment`. The image is validated by the router, frozen into a private `.outbound` artifact, and delivered with the first Signal reply chunk. See [docs/media.md](media.md#outbound-notification-images) for the full staging contract. General outbound media (voice, video, documents, multiple images) is not supported.

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

These are the reasons to choose the router in the first place, summarised here for completeness. Note that the first bullet is now shared with the upstream multiplexing gateway (see [The upstream multiplexing gateway](#the-upstream-multiplexing-gateway-v2026619)) - the router's distinctive part of it is the per-profile subprocess, not the routing itself:

- **Multiple Hermes profiles from a single Signal number**, with one supervised `hermes -p <profile> acp` subprocess per active profile - so each profile keeps its own OS process, memory footprint, and crash domain while sharing the one Signal identity.
- **Route states** - `shadow`, `active`, `maintenance`, `disabled` - controlling whether a route ingests, replies, or only logs.
- **Per-route session policies** - `persistent_route`, `persistent_sender`, `ephemeral` - controlling whether a group shares one ACP session, splits per sender, or starts fresh each turn.
- **Per-route circuit breaker**, with configurable maintenance/failure replies.
- **Route-scoped event dedupe** in a sqlite store: turns the router completed (`handled`) can't be re-played across a router restart. Claims left `processing` by a crash mid-turn are reclaimed at the next startup (the router owns the state DB exclusively, so no turn is in flight then). The resulting at-least-once behaviour applies to externally retried synthetic work with a stable idempotency key - the reclaimed claim lets the caller's retry deliver - a deliberate trade against silently dropping those retries. Signal-origin work carries no router-owned replay guarantee: a Signal turn interrupted by a crash or abandoned at the shutdown deadline is redelivered only if upstream `signal-cli` happens to replay the event.
- **Route-context preamble** injected into each prompt so a profile knows which route target the turn belongs to (prompt-safe keys only; see [docs/route-context.md](route-context.md)).
- **Static ACP permission allowlist** that answers `session/request_permission` from config, so a long-running agent doesn't block on operator input mid-turn (see [docs/permissions.md](permissions.md)).
- **Scheduled synthetic route events** through a local control socket, so host timers can trigger a route-owned turn without a second Signal gateway, direct `signal-cli` send, or profile-side ACP session split.
- **Live `routes.yaml` reload** without process restart, via `signal-hermes-router reload-config` (see [docs/configuration.md](configuration.md#live-configuration-reload-routes-only)).
- **Outbound notification images** for trusted local automation, staged under `router.media_root` and validated before delivery (see [docs/media.md](media.md#outbound-notification-images)).

## Summary

If you run one Hermes profile against one Signal number and want every Signal feature out of the box (image/voice/video replies, native quotes, reactions, markdown formatting, voice transcription, DM support), use the **Hermes Signal gateway**.

If you need more than one Hermes profile behind a single Signal number and are comfortable running all profiles inside one Hermes process, use the **multiplexing gateway** (`gateway.multiplex_profiles` + `gateway.profile_routes`, Hermes `v2026.6.19`+). It is the native, lower-maintenance path and keeps the full Signal feature set on every profile.

If you need more than one profile behind a single Signal number **and** per-profile OS-process isolation (independent crash domains and memory footprints), an agent-agnostic ACP boundary, explicit-route-only ingest with no default-profile fallback, or the router's policy surface (route states, session policies, circuit breakers, dedupe, synthetic control-socket events) - at the cost of text-only ordinary replies (single outbound notification images are supported), no reactions, no native quoting, and a second config/process surface to maintain - use **`signal-hermes-router`**.
