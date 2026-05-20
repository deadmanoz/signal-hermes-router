"""Signal to Hermes ACP router."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("signal-hermes-router")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0+unknown"
