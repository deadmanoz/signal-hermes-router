from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class CircuitTrip:
    route_key: str
    failures: int
    window_seconds: float


class CircuitBreaker:
    def __init__(self, *, failures: int, window_seconds: float) -> None:
        self.failures = failures
        self.window_seconds = window_seconds
        self._failures: dict[str, deque[float]] = defaultdict(deque)

    def record_success(self, route_key: str) -> None:
        self._failures.pop(route_key, None)

    def record_failure(self, route_key: str, now: float | None = None) -> CircuitTrip | None:
        now = now if now is not None else time.monotonic()
        failures = self._failures[route_key]
        failures.append(now)
        cutoff = now - self.window_seconds
        while failures and failures[0] < cutoff:
            failures.popleft()
        if len(failures) >= self.failures:
            return CircuitTrip(route_key, len(failures), self.window_seconds)
        return None

    def failure_count(self, route_key: str, now: float | None = None) -> int:
        failures = self._failures.get(route_key)
        if not failures:
            return 0
        now = now if now is not None else time.monotonic()
        cutoff = now - self.window_seconds
        while failures and failures[0] < cutoff:
            failures.popleft()
        return len(failures)
