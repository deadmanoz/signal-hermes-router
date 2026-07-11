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
filter, and adds explicit excludes for root-level `*.local.md` files,
`/private/`, `/.venv/`, `/.git/`, `/.claude/`, and `/.beads/`. The `/.venv/`
carve-out is the safety-critical one and must not be removed: `.gitignore`
merge rules can hide local virtualenv files without protecting the remote
virtualenv from `--delete`.

Use root-level `*.local.md` files and `/private/` for deployment-local operator
notes or private artefacts that should survive source syncs but never be
published in this repo.

After syncing, run the host-side environment refresh without removing
deployment-local tools:

```bash
cd /absolute/remote/service/path
uv sync --locked --inexact
```

The `--inexact` flag is intentional. The router supervises the `hermes` CLI at
runtime, but Hermes is installed separately and is not part of this package's
lockfile.

## Restarts and Upgrades

Run exactly one router process per state DB, and stop the old process fully
before starting its replacement (systemd's default stop-then-start restart
behaviour is correct; avoid overlapping start-before-stop schemes). Two live
routers over the same Signal account double-consume events, and the dedupe
store's startup reclaim of orphaned `processing` claims assumes exclusive
ownership of the state DB.

The store enforces that ownership by holding an exclusive sqlite lock for the
router's lifetime, so a second current-version router fails loudly at startup.
A router built before this lock existed releases its file locks between
writes, so that enforcement cannot protect a mixed-version overlap: when
upgrading from such a version, confirm the old process has exited before
starting the new one.

## Hermes Subprocess Crash Detection

The router watches every supervised Hermes subprocess and notices an
unexpected exit the moment it happens, instead of discovering it on the next
turn's failed write. A crash produces an ERROR log with the (redacted) profile
reference, the child's returncode, and the respawn marker
`will respawn on next acquisition`, plus a second ERROR line carrying the last
stderr the child wrote before dying (bounded, credential-shapes masked, ANSI
control sequences stripped).

Recovery is deliberately lazy: the dead profile is marked for respawn and the
next turn on any of its routes transparently starts a fresh subprocess - there
is no eager restart loop, so a crash-looping profile only respawns as fast as
traffic arrives, and a profile whose respawn itself fails still trips the
existing failed-start cooldown. Graceful shutdown, `restart_profile` recovery,
and upgrades do not produce these logs; only a child that died on its own does.

## Graceful Shutdown

The serve process handles SIGTERM itself. The first SIGTERM fences new work
(new control requests get a `busy` / `router_shutting_down` response and the
control listener stops accepting), drains in-flight turns so they finish
through the normal delivery path, then closes the Signal client, terminates
each supervised Hermes subprocess gracefully (SIGTERM, a 5 second grace, then
SIGKILL), removes the control socket file, and releases the exclusive state-DB
lock before exiting 0. A second SIGTERM forces immediate exit for an operator
who cannot wait out the drain.

Shutdown is deadline-bounded by code constants, not configuration: up to 15
seconds of graceful drain, up to 5 more seconds settling work that had to be
cancelled at the drain deadline, and a supervisor-close phase that never gets
less than 10 seconds so the per-child terminate grace is not cut short. Worst
case is roughly 30 seconds; if any cleanup cannot settle even then, the
process logs the incomplete cleanup and exits hard rather than hanging, so a
stop never reaches systemd's `TimeoutStopSec` under the defaults.

Turns interrupted at the hard deadline follow the crash semantics: an
externally retried synthetic request with a stable idempotency key is
at-least-once (the replacement router reclaims the orphaned claim and the
retry delivers), but a Signal-origin turn abandoned at the deadline may be
lost unless upstream `signal-cli` happens to replay the event - the router
keeps no replay queue.

This design assumes the service unit delivers the stop signal to the router
process only, leaving the Hermes children alive for the router's own graceful
drain and terminate sequence:

- `KillMode=mixed` is required. The systemd default (`control-group`) sends
  SIGTERM to every process in the cgroup at once, so Hermes children would be
  signalled concurrently with the router and the drain-before-terminate
  ordering cannot hold.
- `TimeoutStopSec` must comfortably exceed the roughly 30 second worst-case
  budget; the 90 second default qualifies. systemd's final SIGKILL sweep at
  that timeout remains the backstop for an externally wedged process.

### Shutdown smoke test

After deploying, verify the graceful path on the real unit:

1. Inspect the unit contract first:
   `systemctl --user show <unit> -p KillMode -p TimeoutStopUSec` and confirm
   `KillMode=mixed` and a stop timeout above the shutdown budget.
2. With the router running and at least one route exercised, record the
   supervised Hermes child PIDs (`systemctl --user status <unit>` shows the
   control group).
3. `systemctl --user restart <unit>`.
4. Confirm the old child PIDs are gone, `journalctl --user -u <unit>` shows a
   clean stop (no `SIGKILL` cleanup entries for the old main process or its
   children), and the replacement router started and recreated its control
   socket.
