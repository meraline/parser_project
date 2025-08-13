import asyncio
import random
import time
from dataclasses import dataclass


@dataclass
class DelayManager:
    """Helper class for handling delays between requests.

    Parameters
    ----------
    min_delay: float
        Minimal delay (seconds) used after successful operations.
    max_delay: float
        Maximal delay (seconds) used after successful operations.
    error_delay: float
        Fixed delay (seconds) used after a failed operation before retrying.
    """

    min_delay: float = 1.0
    max_delay: float = 3.0
    error_delay: float = 10.0

    def _random_delay(self) -> float:
        """Return a random delay within the configured bounds."""
        if self.max_delay <= self.min_delay:
            return self.min_delay
        return random.uniform(self.min_delay, self.max_delay)

    def sleep(self) -> None:
        """Sleep synchronously for a random delay."""
        time.sleep(self._random_delay())

    def sleep_error(self) -> None:
        """Sleep synchronously for the error delay."""
        time.sleep(self.error_delay)

    async def wait(self) -> None:
        """Asynchronously wait for a random delay."""
        await asyncio.sleep(self._random_delay())

    async def wait_error(self) -> None:
        """Asynchronously wait for the error delay."""
        await asyncio.sleep(self.error_delay)
