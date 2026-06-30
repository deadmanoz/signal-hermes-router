# signal-hermes-router

Transport router that owns one Signal account and fans Signal groups out to independent Hermes profiles over ACP.

## Scope discipline

The router does exactly four runtime things, and only these four. Anything outside this scope — agent behaviour, model decisions, profile-side policy, skills, media interpretation — belongs in a Hermes profile, not here.

1. **Transport in** — consume Signal events from upstream `signal-cli` over HTTP/SSE, normalise, dedupe, store attachments to disk, and accept configured local synthetic turns through the private control socket.
2. **Route** — map a Signal group to a Hermes profile; supervise one `hermes -p <profile> acp` subprocess per active profile; call `session/new` or `session/resume` per the route's session policy.
3. **Speak ACP** — JSON-RPC over stdio: `initialize`, `session/new`, `session/resume` (when the profile advertises it), `session/prompt`, and the *required* client-side `session/request_permission` handler. The router also registers reject-all handlers for the `fs/*` and `terminal/*` client methods (matching the zero `clientCapabilities` it advertises) so capability-ignoring agents fail loudly.
4. **Transport out** — send the agent's reply text and router-validated notification image attachments back to Signal via `signal-cli` JSON-RPC.

Operator/deployment preflight tooling is also in scope when it validates those
router-owned runtime contracts without duplicating Hermes behaviour. Permission
preflight may inspect configured route allowlists and compare tool names with a
recorded or live ACP tool-surface contract, but it must not implement tools,
skills, prompts, transcription, OCR, web search, or model policy.

If a proposed runtime change does not fall into one of those four buckets, push
back before implementing. If a proposed operator tool does not validate those
runtime contracts, push back before implementing.

### Common misreadings to push back on

- **`permissions.py` is not a security-policy authority.** It exists *only* because ACP forces the client to answer `session/request_permission` or the agent blocks. The static allowlist is the "answer ahead of time from config, don't prompt the operator over Signal mid-turn" choice. Profile safety is owned by the Hermes profile config and the pre-activation audit checklist.
- **Permission preflight validates wiring, not capability policy.** It may report that a configured allowlist names tools missing from a profile's ACP surface. It must not infer whether a route should have a tool, invent missing tools, or inspect private argument values in public output.
- **The route-context preamble is routing payload, not prompt engineering.** It tells the profile which route/group this turn belongs to. The prompt-safe key allowlist (`signal_hermes_router.context.PROMPT_SAFE_CONTEXT_KEYS`) is code-controlled by design — never make it config-driven.
- **No agent behaviour.** No tool implementations, no model prompts, no skills, no transcription/OCR/summarisation. Those belong in Hermes profiles.
- **Exposing attachment `tool_path` is transport metadata, not agent behaviour.** The opt-in `route_context.attachment_tool_paths` switch only tells the router to surface the stored file path it already manages; what a profile's tools do with that path is profile behaviour.

## Public/private boundary

This tree is **intentionally generic and intended to be publishable**. It must contain no real Signal group IDs, phone numbers, hostnames, personal names, friendly group names, profile-specific route context, credential identifiers, or route-specific audit artefacts. Those live in a separate private deployment repo.

The canonical config/route files in this repo are `config.example.yaml` and `routes.example.yaml` — edit these. Their non-example counterparts (`config.yaml`, `routes.yaml`) belong to the private deployment repo and must never be committed here.

Root-level `*.local.md` files and `/private/` are deployment-local operator state. The deploy helper preserves them during `rsync --delete` source syncs, and they must not be committed.

If you see a real-looking identifier in `config.example.yaml`, `routes.example.yaml`, `tests/`, or `tests/fixtures/`, treat it as a leak.

## Architecture map

```
signal-cli daemon → signal.py (HTTP/SSE client)
                  → events.py (normalise to NormalizedEvent)
                  → dedupe.py (sqlite route-scoped event claims)
                  → router.py (route-state gate → media → prompt → reply)
local automation → cli.py/control socket (trigger-job, notify-route, preflight-permissions)
                  → router.py (same route-state/session/reply path)
                       │
                       ├─ media.py       attachment fetch + per-route storage (write_attachment, MediaManifest)
                       ├─ outbound_media.py  validate notify-route outbound image attachments
                       ├─ context.py     build_prompt_blocks/build_synthetic_prompt_blocks (route-context preamble + escaped text + media/payload blocks)
                       ├─ sessions.py    ProfileSupervisor (one hermes subprocess per profile) + SessionRegistry (per session-policy)
                       │   └─ acp.py    JsonRpcStdioPeer ↔ `hermes -p <profile> acp`
                       │       └─ permissions.py   answers session/request_permission from static config
                       └─ outbound.py    prepare_outgoing_message + chunk_for_signal_bytes
                            └─ signal.py     send_group/send_direct (reply + attachment path)
```

Cross-cutting: `models.py` (shared dataclasses/enums: `NormalizedEvent`, `MediaManifest`, `OutboundAttachment`, `TurnResult`, `SessionPolicy`, `RouteState`, `SyntheticTurnKind`), `payloads.py` (compact control JSON and notification payload canonicalization), `preflight.py` (permission allowlist/tool-surface reporting with safe route refs), `mime.py` (content-type normalisation + extension mapping shared by `media.py`, `outbound_media.py`, and `context.py`), `private_fs.py` (0700/0600 perm enforcement on the router-managed roots `media_root`/`work_root` and the files written under them, including the dedupe DB), `redaction.py` (log redaction), `circuit.py` (per-route circuit breaker), `secrets.py` (`file://`/`env://`/`op://`/`systemd-credential://` resolvers), `config.py`, and `cli.py`.

## Commands

Tests need no Hermes — they use a fake ACP subprocess.

Dependency management is [uv](https://docs.astral.sh/uv/)-first. `hatchling` remains the PEP 517 build backend; `uv` is the installer/resolver and `uv.lock` is the source of truth for the dev environment.

```bash
# create venv + install project (editable) + dev extras from uv.lock
uv sync --extra dev
. .venv/bin/activate

# full test suite
PYTHONPATH=src python -m unittest discover -s tests -v

# single test
PYTHONPATH=src python -m unittest tests.test_router.TestRouter.test_<name>
```

Hermes is not a Python dependency of this package. The router supervises the `hermes` CLI as a black-box subprocess at runtime. Do not `import hermes_agent` here.

## Commits

Conventional commits (`type(scope): description`), scope = module name (e.g. `feat(router):`, `fix(sessions):`). One logical change per commit. Never reference AI tools in messages.

## Documentation

User-facing operational docs live under [docs/](docs/). The [profile audit checklist](docs/profile-audit-checklist.md) must be filled out (in the *private* deployment repo) before a route moves to `active`, including the permission preflight report when route allowlists or profile tool surfaces change.

`CHANGELOG.md` is owned by Release Please and generated from Conventional Commit titles — do not hand-edit it, and in particular do **not** add an `[Unreleased]` section (any global "update the changelog under `[Unreleased]`" convention does not apply here). Describe a change through its `type(scope): description` commit title; Release Please writes the changelog entry when the commit lands on `main`. See [docs/releasing.md](docs/releasing.md) for the release flow.
