# Permissions

## What this is, and what it isn't

The router speaks ACP to each `hermes -p <profile> acp` subprocess. ACP requires the *client* (the router) to answer `session/request_permission` whenever the agent considers a tool call privileged. Without an answerer, the agent blocks waiting for one.

The router's permission handler exists **only to satisfy that contract**. It is a static, route-scoped allowlist that lets the operator answer permission prompts ahead of time rather than getting interactive prompts over Signal mid-turn.

Profile safety is owned by the Hermes profile config and the [pre-activation audit checklist](profile-audit-checklist.md). The router's allowlist is a deployment-side gate that runs on top of that, not a replacement for it.

## Static allowlist shape

The router denies by default. A route may allow only explicit `(tool, argument predicate)` shapes:

```yaml
permissions:
  - tool: "read_file"
    arguments:
      path:
        prefix: "/private/deployment/read-only/"
```

Argument predicates supported by `signal_hermes_router.permissions.ArgPredicate`:

- `equals` — exact value match
- `prefix` — string prefix match; for path-like argument names — `path`,
  `file_path`/`filepath`, `cwd`, and any argument whose name ends in `_path`
  (e.g. `output_path`, `source_path`) — the router canonicalises both paths
  and requires the requested path to resolve under the configured absolute
  prefix
- `one_of` — membership in a tuple
- `regex` — full-string regex match
- `present` — argument key presence (`true`/`false`)

Path-like `prefix` values must be absolute. Traversal such as
`/private/deployment/read-only/../secret.txt` is rejected even though it has the
same raw string prefix.

Denylists (`deny:` or `denylist:` in config) are rejected at parse time. Permission policy is allowlist-only.

When a tool call matches the allowlist, the router selects only ACP `allow_once`
options. It deliberately does not select `allow_always`, so Hermes cannot turn a
route-scoped static answer into a persistent permission grant. Denied tool calls
prefer `reject_once` and may fall back to `reject_always`.

## Pre-activation requirement

Create a private [profile audit checklist](profile-audit-checklist.md) record before activating or changing profile skills, Hermes version, or allowlists. Inspect the `hermes-acp` toolset for the exact tools exposed by the pinned Hermes version before activation.
