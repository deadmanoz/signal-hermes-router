# No-reply sentinel

A Hermes profile can deliberately stay silent on a turn by emitting a fixed
sentinel string as its whole reply. The router honors the marker on transport
out by suppressing the outbound Signal send while still completing and
recording the turn normally. Chattiness policy - deciding *when* to stay
silent - lives in the Hermes profile (system prompt, skills, model behaviour);
the router only honors the marker, analogous to the outbound-media contract.

## The sentinel

```
[[signal-hermes-router:no-reply]]
```

The value is a code-controlled constant
(`signal_hermes_router.outbound.NO_REPLY_SENTINEL`). It is never configurable,
on the same principle as the prompt-safe context key allowlist: a fixed,
collision-resistant contract that profiles can rely on across deployments.

## Router semantics

- **Whole-reply match only.** The reply is suppressed when, after stripping
  leading and trailing whitespace, it equals the sentinel exactly. A sentinel
  embedded in a longer reply is a near-miss and the reply is delivered
  verbatim.
- **No Signal send.** Nothing is sent to the group or direct recipient. On a
  `notify-route` turn carrying a router-validated outbound image attachment,
  the attachment is suppressed too: deliberate silence wins over the
  attachment-only fallback text.
- **The turn still completes normally.** The event is marked handled in the
  dedupe store, the circuit breaker records a success, session state is
  unchanged, and control-socket responses report `status: delivered` with
  `reply_sent: false`.
- **One redaction-safe log line.** The router logs a single INFO line
  (`suppressing Signal reply for route_<ref>: profile emitted no-reply
  sentinel`) with the stable route ref, never the raw group ID or reply
  content.
- **The busy notice is outside this contract.** The router-owned busy notice
  (`router.busy_notice_after_seconds`) fires while the profile is still
  generating, before the router can know the reply will be the sentinel. On a
  deployment with busy notices enabled, a turn that runs past the threshold
  and then ends in the sentinel will already have posted the configured
  "still working" message; the sentinel suppresses only the turn's reply.
  Operators who need strict silence on routes whose profiles use the sentinel
  should set `busy_notice_after_seconds` at or above
  `acp_prompt_timeout_seconds` so the notice can never fire before the turn
  resolves.

## Profile adoption

Instruct the profile (in its own configuration, not in this repo) to emit the
sentinel as its entire reply whenever it decides a turn needs no response, for
example in busy groups where most messages are not addressed to it. The
sentinel must be the whole reply: any surrounding prose defeats the match by
design, so a model that "explains" its silence will be delivered, not
suppressed.
