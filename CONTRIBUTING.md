# Contributing

Thanks for considering a contribution. **Read [AGENTS.md](AGENTS.md) first** — it covers project scope, build/test commands, and commit conventions. This file adds the PR-specific policy on top.

## Scope

The router does exactly four things:

1. **Transport in** — consume Signal events from upstream `signal-cli`, normalise, dedupe, store attachments, and accept configured local synthetic turns through the private control socket.
2. **Route** — map a Signal group to a Hermes profile; supervise one ACP subprocess per active profile.
3. **Speak ACP** — JSON-RPC over stdio with the Hermes profile (`initialize`, `session/new`/`resume`, `session/prompt`, `session/request_permission`).
4. **Transport out** — send the agent's reply text and router-validated notification image attachments back to Signal.

See [AGENTS.md](AGENTS.md#scope-discipline) for the full rationale, including what is *not* in scope (agent behaviour, model prompts, skills, media interpretation — those belong in Hermes profiles).

## What a good PR looks like

- **Ties to one of the four scope buckets above.** Changes that sit outside this scope will be asked to move to the relevant Hermes profile repo, or closed.
- **Includes a one-paragraph "why"** in the description: the problem this solves, alternatives considered, tradeoffs. Demonstrate that you understand the change.
- **Passes CI.** Every PR runs `ruff check`, `ruff format --check`, and the unittest suite under coverage (95% floor) via [.github/workflows/ci.yml](.github/workflows/ci.yml). Run them locally before pushing to avoid a red round-trip: `ruff check . && ruff format --check . && PYTHONPATH=src python -m unittest discover -s tests`. If tests don't cover the change, describe how you verified it manually in the PR body and add a test where reasonable.
- **Keeps the public/private boundary clean.** Run `python scripts/check-public-boundary.py` before opening a PR; CI runs the same check.
- **Is atomic.** One logical change per PR. Mixed refactor + feature + style changes will be asked to split.
- **Follows conventional commits.** `type(scope): description` with scope = module name (e.g. `feat(router):`, `fix(sessions):`).

## On AI-assisted contributions

Use whatever tools you like — the bar is that **you understand and stand behind every line of the diff**. PRs that appear unreviewed by their submitter (plausible-looking changes whose author can't answer questions about them, sweeping refactors with no stated motivation, or generated boilerplate that doesn't fit the project) will be closed without detailed review. If you used an assistant, you are still the author; treat its output as a draft, not a deliverable.

## Public/private boundary

This repo is intentionally generic and publishable. **Never commit** real Signal group IDs, phone numbers, hostnames, personal names, friendly group names, profile-specific route context, credential identifiers, or route-specific audit artefacts — these belong to a separate private deployment repo. Edit `config.example.yaml` / `routes.example.yaml`, never their non-example counterparts. See [AGENTS.md](AGENTS.md#publicprivate-boundary) for details.

## Dependency policy

CI and local development use `uv.lock` as the tested dependency set. Run
`uv sync --extra dev --locked` for a release-equivalent test environment, and
refresh the lock intentionally with `uv lock` when changing dependencies. The
lower bounds in `pyproject.toml` are package metadata for downstream installers;
they are not the tested application deployment set.

## Versioning and releases

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html); pre-1.0 minor bumps may include breaking changes. See [docs/releasing.md](docs/releasing.md) for the full policy and release procedure. Release Please owns [CHANGELOG.md](CHANGELOG.md): it generates each entry from the Conventional Commit (squash) titles that land on `main`, so do not hand-edit the changelog or add an `[Unreleased]` section. Make your change describe itself through a clear `type(scope): description` title instead.
