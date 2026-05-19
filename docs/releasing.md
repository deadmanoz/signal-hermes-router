# Releasing

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). The single source of truth for the version is the `version` field in [pyproject.toml](../pyproject.toml); the matching `vX.Y.Z` annotated git tag is the release marker.

## Pre-1.0 (0.x.y) contract

The public surface — CLI flags, config schema, route file format, on-disk attachment layout, dedupe DB schema, and the route-context preamble contract — may change between minor versions. Patch bumps (`0.x.Y → 0.x.Y+1`) are bug fixes and backward-compatible additions only. Every breaking change must be called out under its version in [CHANGELOG.md](../CHANGELOG.md).

## What counts as breaking (pre-1.0)

Treat any of the following as breaking and bump the minor (`0.x.0`):

- Removing or renaming a CLI flag, subcommand, or `signal-hermes-router` config key.
- Changing the meaning or default of an existing config key.
- Changing the route file format (`routes.yaml` schema).
- Changing the dedupe sqlite schema in a way that requires migration of an existing DB.
- Changing the on-disk attachment storage layout or manifest format.
- Changing the route-context preamble contract (delimiters, escaping rules, the prompt-safe key allowlist semantics).
- Changing the set of ACP methods the router implements or expects.

Patch bumps cover bug fixes, log/metric changes, internal refactors with no public-surface effect, dependency bumps that don't change behaviour, and additive changes (a new optional config key with a backward-compatible default).

## Cutting a release

1. Move entries out of `[Unreleased]` in [CHANGELOG.md](../CHANGELOG.md) into a new `[X.Y.Z] - YYYY-MM-DD` section.
2. Bump `version` in [pyproject.toml](../pyproject.toml).
3. Verify the suite is green locally:
   ```bash
   ruff check . && ruff format --check . && PYTHONPATH=src python -m unittest discover -s tests
   python scripts/check-public-boundary.py
   ```
4. Commit as `chore(release): vX.Y.Z`.
5. Tag: `git tag -a vX.Y.Z -m "vX.Y.Z"`.
6. Push commit and tag: `git push && git push --tags`.

## Path to 1.0

1.0 will be cut when the config schema, CLI surface, ACP supervision contract, dedupe schema, and route-context preamble contract are stable enough to commit to strict SemVer compatibility. Until then, expect minor bumps to occasionally require config or migration changes — these will always be documented in [CHANGELOG.md](../CHANGELOG.md).
