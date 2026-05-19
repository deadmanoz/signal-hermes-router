from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import logging
from pathlib import Path

from .config import load_app_config
from .router import SignalHermesRouter


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="signal-hermes-router")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--routes", type=Path, default=Path("routes.yaml"))
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    asyncio.run(_run(args.config, args.routes))


async def _run(config_path: Path, routes_path: Path) -> None:
    config = load_app_config(config_path, routes_path)
    route_counts = Counter(route.state.value for route in config.routes)
    logging.info(
        "starting signal-hermes-router routes=%s remote_signal_base_url=%s",
        dict(route_counts),
        config.router.allow_remote_signal_base_url,
    )
    router = SignalHermesRouter(config)
    try:
        await router.run_forever()
    finally:
        await router.close()


if __name__ == "__main__":
    main()
