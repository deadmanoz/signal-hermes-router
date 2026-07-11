"""Token-bucket rate limiting for inbound routed turns.

The bucket never reads a clock itself: callers pass epoch milliseconds so the
router's injectable ``clock_ms`` drives refill deterministically in tests.
"""

from __future__ import annotations


class TokenBucket:
    """Classic token bucket: ``capacity`` burst, ``refill_per_second`` sustained."""

    def __init__(self, capacity: float, refill_per_second: float) -> None:
        if capacity < 1.0:
            raise ValueError("token bucket capacity must be >= 1")
        if refill_per_second <= 0.0:
            raise ValueError("token bucket refill rate must be positive")
        self._capacity = float(capacity)
        self._refill_per_second = float(refill_per_second)
        self._tokens = float(capacity)
        self._last_refill_ms: int | None = None

    def try_acquire(self, now_ms: int) -> bool:
        """Refill for elapsed time, then consume one token if available."""
        self._refill(now_ms)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def refund(self) -> None:
        """Return one token, e.g. when an admitted turn never ran."""
        self._tokens = min(self._capacity, self._tokens + 1.0)

    def _refill(self, now_ms: int) -> None:
        if self._last_refill_ms is None:
            self._last_refill_ms = now_ms
            return
        elapsed_ms = now_ms - self._last_refill_ms
        if elapsed_ms <= 0:
            # A clock that stalls or steps backwards must never mint tokens.
            return
        self._tokens = min(
            self._capacity,
            self._tokens + (elapsed_ms / 1000.0) * self._refill_per_second,
        )
        self._last_refill_ms = now_ms
