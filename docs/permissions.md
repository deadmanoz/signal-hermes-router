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

## Permission preflight

Allowlisting a tool name does not create that tool in the Hermes profile. Before
activating a route or changing its allowlist, compare the configured permission
tool names with the profile's ACP tool surface. Offline checks use a private
recorded contract:

```bash
signal-hermes-router \
  --config /path/to/private/config.yaml \
  --routes /path/to/private/routes.yaml \
  preflight-permissions \
  --active-only \
  --probe-contract-file /path/to/private/probe-contract.json \
  --json
```

The probe contract is private deployment evidence. The router accepts either of
these equivalent JSON shapes:

```json
{
  "profiles": {
    "example-profile": {
      "tools": ["read_file", "web_search"]
    },
    "another-profile": ["read_file"]
  }
}
```

Live checks can use profiles managed by the running router's ACP supervisor:

```bash
signal-hermes-router \
  --config /path/to/private/config.yaml \
  preflight-permissions \
  --active-only \
  --control-socket /path/to/private/control.sock \
  --json
```

The running-router probe is intentionally structured. It reads
`agentCapabilities._meta` fields such as `toolSurface`, `tool_surface`, `tools`,
or `tool_names`, including the same fields nested under `signalHermesRouter`,
`signal-hermes-router`, or `signal_hermes_router`. If no metadata surface is
present, the router tries an optional JSON-RPC extension method named
`_tool_surface/list`.
Agents that do not expose either source report `probe_unsupported`; that means
the agent needs a structured tool-surface integration before live preflight can
validate it.

An explicit empty surface such as `_meta: {"tools": []}` is treated as
authoritative. Any configured allowlist tools for that profile are reported as
missing rather than falling back to `_tool_surface/list`.

Because the live path uses the router's normal `ProfileSupervisor`, probing an
idle profile can start that profile's ACP subprocess. Supervisor cooldowns and
startup failures are reported as probe errors. If a profile is already handling
a turn when preflight starts probing it, live preflight reports
`probe_profile_busy` instead of waiting behind that turn.

The report uses only safe route references: `route:<name>` when a route has a
configured name, otherwise `routes[<index>]`. It never reports raw group IDs,
direct sender IDs, phone numbers, route keys, permission argument predicates,
or secret values.

Selector flags narrow the check:

- `--active-only` checks only active routes; by default active and shadow
  routes are checked and maintenance/disabled routes are skipped.
- `--route <name>` can be repeated to check named routes.
- `--route-index <index>` can be repeated to check unnamed routes by
  zero-based `routes.yaml` index.
- `--profile <profile>` can be repeated to check only selected Hermes profiles.

Running the offline command without `--probe-contract-file` returns a failed
report with `probe_contract_required`; that is a clear operator prompt, not a
successful validation.

## Pre-activation requirement

Create a private [profile audit checklist](profile-audit-checklist.md) record before activating or changing profile skills, Hermes version, or allowlists. Inspect the `hermes-acp` toolset for the exact tools exposed by the pinned Hermes version before activation.
