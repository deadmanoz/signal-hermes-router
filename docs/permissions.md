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

The probe contract is private deployment evidence. Version 1 requires one
document-wide `full_callable` scope that applies to every profile entry:

```json
{
  "schema_version": 1,
  "scope": "full_callable",
  "profiles": {
    "example-profile": {
      "tools": ["read_file", "web_search"]
    },
    "another-profile": {
      "tools": ["read_file"]
    }
  }
}
```

`full_callable` means every tool the profile can dispatch by name, including
tools deferred behind Tool Search or another progressive-disclosure mechanism.
A compressed or model-facing list containing bridge tools is not a callable
catalog and must use a different scope. Permission preflight rejects that scope
instead of reporting its omitted tools as missing.

Live checks can use profiles managed by the running router's ACP supervisor:

```bash
signal-hermes-router \
  --config /path/to/private/config.yaml \
  preflight-permissions \
  --active-only \
  --control-socket /path/to/private/control.sock \
  --json
```

The running-router probe is intentionally structured. It reads a tool-surface
candidate from `agentCapabilities._meta`, including candidates nested under
`signalHermesRouter`, `signal-hermes-router`, or `signal_hermes_router`. If no
metadata surface is present, the router tries an optional JSON-RPC extension
method named `_tool_surface/list`. Either source must return this envelope:

```json
{
  "schema_version": 1,
  "scope": "full_callable",
  "tools": ["read_file", "web_search"]
}
```

The producer must assemble `tools` before Tool Search compression. The router
does not import Hermes or inspect its private implementation; it validates only
this producer contract.
Agents that do not expose either source report `probe_unsupported`; that means
the agent needs a structured tool-surface integration before live preflight can
validate it.

An explicit v1 empty surface with `"tools": []` is authoritative. Any
configured allowlist tools for that profile are reported as missing rather than
falling back to `_tool_surface/list`.

Missing versions, unsupported versions, missing scopes, model-facing scopes,
ambiguous metadata candidates, and malformed contracts produce specific
`probe_contract_*` errors. They never produce missing-tool findings. Present but
invalid capability metadata also does not fall through to `_tool_surface/list`.
Redundant aliases count as ambiguous, including a v1 `toolSurface` alongside a
legacy or model-facing `tools` field; producers must publish exactly one
candidate.

### Version 1 transition checklist

1. Generate a private recorded contract from a catalog independently verified
   to contain every callable tool, then add `schema_version: 1` and
   `scope: full_callable`.
2. Run offline permission preflight with that file during the rollout.
3. Update the Hermes ACP producer to wrap its pre-Tool-Search full catalog in
   the v1 envelope shown above.
4. Deploy the producer update, then resume live control-socket preflight.

Keep using the verified offline v1 contract until every live producer is
updated; mixed deployments with unversioned producers intentionally fail live
preflight rather than guessing at catalog completeness.
Regenerate and verify the recorded contract whenever a profile's callable tool
catalog changes during that transition.

Unversioned recorded files are rejected before preflight with a validation
message that names the required `schema_version=1` field. Unversioned live
Hermes responses fail with `probe_contract_version_missing`. The router does
not grandfather either shape because an unversioned list cannot prove whether
it is complete or compressed.

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
