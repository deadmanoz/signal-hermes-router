# Route context

`route_context` is private deployment JSON attached to each route. Most keys are operator metadata and are **never** sent to Hermes. Before each turn, `signal_hermes_router.context.context_for_prompt` filters the route's context dict down to a code-reviewed, prompt-safe key allowlist; that filtered subset is then handed to `render_route_context` and emitted as the first ACP text block on every turn:

```
[route_context:<nonce>]{"example":"value"}[/route_context:<nonce>]
```

## Prompt-safe key allowlist

The set of prompt-visible keys is deliberately code-controlled in `signal_hermes_router.context.PROMPT_SAFE_CONTEXT_KEYS`. Current keys:

- `purpose`
- `route_alias`

Adding a new prompt-safe key exposes that value to the LLM prompt preamble and should be reviewed like any other prompt-injection-adjacent surface. **Do not make the prompt-safe key set deployment-configurable.**

## Router-consumed switches

Some `route_context` keys are read by the router to shape transport behaviour and
are **never** serialised into the prompt preamble. They are deliberately kept out
of `PROMPT_SAFE_CONTEXT_KEYS`.

- `attachment_tool_paths` (boolean): when set to an explicit `true`, the router
  exposes the stored attachment path as a `tool_path` field inside prompt-visible
  attachment manifests (see [media.md](media.md)). The check is strict boolean
  identity, so truthy non-boolean values such as the string `"false"` do **not**
  opt in. Do not add this key to `PROMPT_SAFE_CONTEXT_KEYS`; it is router-consumed,
  not prompt-emitted, and the key name itself never appears in the preamble.

## Nonce and escaping

The nonce changes per turn (`signal_hermes_router.context.new_context_nonce`). User text is sent as a separate ACP content block, and route-context delimiter lookalikes in user text are escaped (`signal_hermes_router.context.escape_user_text`) before delivery.

Profiles should treat prompt-visible route context as trusted deployment context, not as user text. Keep raw Signal route target identifiers, friendly names, imported source labels, canary reply prefixes, and other private metadata in `routes.yaml`; they are not prompt-visible unless the public code allowlist is expanded.
