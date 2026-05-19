# Deployment

This public repo does not contain deployment-specific hostnames, config paths,
Signal identifiers, or profile names. Keep those in the private deployment repo
or operator runbook.

## Source Sync

Use the deploy helper for code syncs:

```bash
scripts/deploy-service-tree.sh HOST /absolute/remote/service/path
```

For a preview:

```bash
scripts/deploy-service-tree.sh --dry-run HOST /absolute/remote/service/path
```

The helper uses `rsync --delete` with the repo `.gitignore` merged in as a
filter, and adds explicit excludes for `/.venv/`, `/.git/`, `/.claude/`, and
`/.beads/`. The `/.venv/` carve-out is the safety-critical one and must not
be removed: `.gitignore` merge rules can hide local virtualenv files without
protecting the remote virtualenv from `--delete`.

After syncing, run the host-side environment refresh without removing
deployment-local tools:

```bash
cd /absolute/remote/service/path
uv sync --locked --inexact
```

The `--inexact` flag is intentional. The router supervises the `hermes` CLI at
runtime, but Hermes is installed separately and is not part of this package's
lockfile.
