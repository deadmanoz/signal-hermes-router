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
2. Release Please opens or updates a release PR after releasable commits land on `main`.
3. The release workflow refreshes `uv.lock` on the release PR branch so `uv sync --locked` stays green.
4. When the release PR auto-merges, Release Please creates the `vX.Y.Z` tag and GitHub Release.

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
