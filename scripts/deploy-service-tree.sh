#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: scripts/deploy-service-tree.sh [--dry-run] HOST REMOTE_DIR

Sync this public source tree to a remote service directory.

The remote virtualenv is deliberately protected. It may contain deployment-local
tools such as the Hermes CLI that are not part of this package's uv.lock.
Root-level *.local.md files and private/ are also preserved as deployment-local
operator state.
USAGE
}

rsync_args=(-av --delete)
if [[ "${1:-}" == "--dry-run" ]]; then
  rsync_args+=(--dry-run)
  shift
fi

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

host=$1
remote_dir=$2

if [[ "$host" == *:* ]]; then
  echo "HOST must not include a path; pass REMOTE_DIR separately" >&2
  exit 2
fi

if [[ "$remote_dir" != /* ]]; then
  echo "REMOTE_DIR must be absolute" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

rsync "${rsync_args[@]}" \
  --filter=':- .gitignore' \
  --exclude='/*.local.md' \
  --exclude='/private/' \
  --exclude='/.venv/' \
  --exclude='/.git/' \
  --exclude='/.git' \
  --exclude='/.claude/' \
  --exclude='/.beads/' \
  "$repo_root"/ "$host:$remote_dir"/
