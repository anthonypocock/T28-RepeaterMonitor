"""Passive advertisement source contract and restart backoff.

The production source is HCI/mgmt passive LE scan (hcitool lescan --passive
equivalent). AdvertisementMonitor1 is not required. If passive cannot be set,
the service fails closed. Do not fall back to Bleak or StartDiscovery.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Optional, Protocol

from .engine import AdvertisementObservation

ObservationCallback = Callable[[AdvertisementObservation], Awaitable[None] | None]
LostCallback = Callable[[str], Awaitable[None] | None]


class PassiveScanUnavailable(RuntimeError):
    """HCI/mgmt cannot set a passive LE scan. Do not fall back to active scan."""


class PassiveAdvertisementSource(Protocol):
    async def run(
        self,
        on_advertisement: ObservationCallback,
        on_device_lost: Optional[LostCallback] = None,
    ) -> None:
        """Run until cancelled. Must not start an active LE scan."""


class BoundedBackoff:
    def __init__(self, initial: float = 1.0, maximum: float = 30.0, factor: float = 2.0) -> None:
        if initial <= 0 or maximum < initial or factor < 1:
            raise ValueError("invalid backoff")
        self.initial = initial
        self.maximum = maximum
        self.factor = factor
        self._current = initial

    def peek(self) -> float:
        return self._current

    def next_delay(self) -> float:
        delay = self._current
        self._current = min(self.maximum, self._current * self.factor)
        return delay

    def reset(self) -> None:
        self._current = self.initial


async def run_with_backoff(
    connect: Callable[[], Awaitable[None]],
    *,
    backoff: Optional[BoundedBackoff] = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    should_continue: Callable[[], bool] | None = None,
) -> None:
    """Reconnect a flaky BlueZ session with a bounded delay. Never grows without limit."""
    delays = backoff or BoundedBackoff()
    while should_continue is None or should_continue():
        try:
            await connect()
            delays.reset()
            continue
        except asyncio.CancelledError:
            raise
        except PassiveScanUnavailable:
            raise
        except Exception:
            pass
        await sleep(delays.next_delay())
