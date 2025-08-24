import random
import time
from dataclasses import dataclass


class DelayManager:
    """Helper class for handling delays between requests."""

    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 3.0,
        error_delay_seconds: float = 10.0,
    ):
        """
        Initialize delay manager.

        Args:
            min_delay: Minimal delay (seconds) used after successful operations
            max_delay: Maximal delay (seconds) used after successful operations
            error_delay_seconds: Fixed delay (seconds) used after failures
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.error_delay_seconds = error_delay_seconds

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

    def apply_delay(
        self, min_delay: float | None = None, max_delay: float | None = None
    ) -> None:
        """Apply a random delay within configured bounds."""
        if min_delay is not None:
            self.min_delay = min_delay
        if max_delay is not None:
            self.max_delay = max_delay
        time.sleep(self._random_delay())

    def apply_error_delay(self) -> None:
        """Apply the configured error delay."""
        time.sleep(self.error_delay_seconds)
