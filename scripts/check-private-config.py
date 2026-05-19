#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from signal_hermes_router.config import load_app_config  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate private signal-hermes-router config without printing secrets."
    )
    parser.add_argument("config_path", type=Path)
    parser.add_argument("routes_path", type=Path)
    args = parser.parse_args(argv)

    try:
        cfg = load_app_config(args.config_path, args.routes_path)
    except Exception as exc:
        print(f"private config validation failed: {exc.__class__.__name__}", file=sys.stderr)
        return 1

    states = Counter(route.state.value for route in cfg.routes)
    states_summary = {state: states[state] for state in sorted(states)}
    print(
        "routes_parsed=%d states=%s remote_signal_base_url=%s"
        % (len(cfg.routes), states_summary, cfg.router.allow_remote_signal_base_url)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
