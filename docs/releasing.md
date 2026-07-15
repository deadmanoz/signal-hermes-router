# Releasing

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). The single source of truth for the version is the `version` field in [pyproject.toml](../pyproject.toml); the matching `vX.Y.Z` annotated git tag is the release marker. Release Please owns changelog generation, version bumps, git tags, and GitHub Releases.

## Pre-1.0 (0.x.y) contract

The public surface -- CLI flags, config schema, route file format, on-disk attachment layout, dedupe DB schema, and the route-context preamble contract -- may change between minor versions. Patch bumps (`0.x.Y -> 0.x.Y+1`) are bug fixes and backward-compatible additions only. Every breaking change must be called out under its version in [CHANGELOG.md](../CHANGELOG.md).

## What counts as breaking (pre-1.0)

Treat any of the following as breaking and bump the minor (`0.x.0`):

- Removing or renaming a CLI flag, subcommand, or `signal-hermes-router` config key.
- Changing the meaning or default of an existing config key.
- Changing the route file format (`routes.yaml` schema).
- Changing the dedupe sqlite schema in a way that requires migration of an existing DB.
- Changing the on-disk attachment storage layout or manifest format.
- Changing the route-context preamble contract (delimiters, escaping rules, the prompt-safe key allowlist semantics).
- Changing the set of ACP methods the router implements or expects.

Patch bumps cover bug fixes, log/metric changes, internal refactors with no public-surface effect, runtime dependency bumps that do not change behaviour, and additive changes (a new optional config key with a backward-compatible default).

## Automated release flow

1. Merge PRs with Conventional Commit squash titles.
2. Release Please opens or updates a draft release PR after releasable commits land on `main`. It generates `CHANGELOG.md`, updates `pyproject.toml`, and updates the matching project version in `uv.lock` from the same release metadata.
3. The release workflow requires `main` to remain at the SHA that triggered Release Please. If `main` advances, the current job leaves the PR draft so the queued run regenerates all release metadata from the newer base instead of merging that base into stale metadata. The workflow then checks the PR diff through the GitHub API and requires exactly modified `CHANGELOG.md`, `.release-please-manifest.json`, `pyproject.toml`, and `uv.lock` files before checkout. It checks out the inspected SHA without persisted credentials and validates the Release Please transformations against the triggering base: versions must agree, only version fields may change in the project metadata and lockfile, and the changelog must prepend the matching release section without rewriting history. It also runs `uv lock --check`, the stdlib-only public-boundary scanner without dependency sync, and a clean-tree check for all generated release files.
4. Immediately before changing the PR to ready, the workflow confirms that the validated checkout is still the live PR head, GitHub reports it as mergeable rather than conflicting, and the compare API reports `behind_by=0`. These signals remain meaningful while the PR is draft.
5. Only that validated head is marked ready, approved, and armed for squash auto-merge.
6. When the release PR auto-merges, Release Please creates the `vX.Y.Z` tag and GitHub Release.

GitHub may report the draft as `BLOCKED` while approval or required checks are pending. The workflow permits that state because it adds approval and arms auto-merge only after validating the live head; `BEHIND`, `DIRTY`, and persistent `UNKNOWN` states still fail closed.

The canonical `release-please--branches--main` branch is automation-owned. Do not push to it manually. Release-workflow concurrency serializes automated writers, publication rechecks the live SHA around the ready transition, and auto-merge is bound to the validated head with `--match-head-commit`.

The workflow also verifies that `main` has strict required-status-check protection (`required_status_checks.strict=true`) before publishing. That repository rule atomically prevents auto-merge if `main` advances after the final compare check. Any failure after the PR becomes ready triggers an exit trap that returns it to draft.

`RELEASE_PLEASE_TOKEN` must be a classic personal access token with `repo` scope, or a fine-grained token with repository Contents, Pull requests, and Issues read/write permissions plus Administration read permission. Administration read is required only to inspect strict status-check protection; the workflow does not change repository settings.

The repository Actions setting **Allow GitHub Actions to create and approve pull requests** must remain enabled. The release PR is created by `RELEASE_PLEASE_TOKEN`, then the separate `GITHUB_TOKEN` identity supplies the required approval after validation. An approval failure leaves or returns the release PR to draft.

An existing ready release PR is moved back to draft before Release Please updates it. Even when Release Please generates no new files, the pending PR runs through the same synchronization, validation, live-head, and publication gates. This also lets a rerun recover an automation-owned draft left by a previous failed run. A successful no-op preserves a ready PR's previous auto-merge setting; a recovered draft resumes the normal auto-merge path.

If release validation fails, leave the PR in draft. Inspect the failed workflow step, fix the generating configuration or release process on `main`, and rerun the release workflow. Do not repair, mark ready, or merge the generated release branch by hand.

Do not hand-edit [CHANGELOG.md](../CHANGELOG.md) or add `[Unreleased]` entries. Release Please generates changelog entries from merged commit titles.

## PR title policy

Only squash merges are enabled, so the PR title becomes the commit that Release Please parses:

- Use `fix(...)` for bug fixes, compatible runtime dependency updates, and other patch-worthy product changes.
- Use `feat(...)` for additive changes that should still become patch releases during pre-1.0.
- Use `!` or a `BREAKING CHANGE:` footer for changes that require the next `0.x.0` release.
- Use `chore(...)` for CI, docs-only, dev-tool, and other non-shipping maintenance changes.
- Manually retitle major runtime dependency PRs before merging if the bump is behaviour-affecting, for example `feat(deps)!: bump httpx`.

## Local verification

Before merging release-affecting changes, verify:

```bash
ruff check . && ruff format --check . && PYTHONPATH=src python -m unittest discover -s tests
python scripts/check-public-boundary.py
```

## Path to 1.0

1.0 will be cut when the config schema, CLI surface, ACP supervision contract, dedupe schema, and route-context preamble contract are stable enough to commit to strict SemVer compatibility. Until then, expect minor bumps to occasionally require config or migration changes; these will always be documented in [CHANGELOG.md](../CHANGELOG.md).
