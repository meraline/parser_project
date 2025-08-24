"""Metrics collection utilities."""

from dataclasses import dataclass, field
from typing import Dict, List
import time


@dataclass
class ParserMetrics:
    """Container for parser performance metrics."""

    parse_times: Dict[str, List[float]] = field(default_factory=dict)
    reviews_count: Dict[str, List[int]] = field(default_factory=dict)
    saved_count: Dict[str, List[int]] = field(default_factory=dict)
    error_count: Dict[str, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    def record_parse_time(self, duration: float, parser: str) -> None:
        """Record the time taken to parse a page."""
        if parser not in self.parse_times:
            self.parse_times[parser] = []
        self.parse_times[parser].append(duration)

    def record_reviews_count(self, total: int, saved: int, parser: str) -> None:
        """Record the number of reviews found and saved."""
        if parser not in self.reviews_count:
            self.reviews_count[parser] = []
            self.saved_count[parser] = []
        self.reviews_count[parser].append(total)
        self.saved_count[parser].append(saved)

    def record_error(self, parser: str) -> None:
        """Record a parsing error."""
        self.error_count[parser] = self.error_count.get(parser, 0) + 1

    def get_metrics(self) -> Dict:
        """Get collected metrics."""
        metrics = {
            "uptime": time.time() - self.start_time,
            "error_count": sum(self.error_count.values()),
        }

        for parser, times in self.parse_times.items():
            if times:
                metrics[f"{parser}_avg_parse_time"] = sum(times) / len(times)
                metrics[f"{parser}_min_parse_time"] = min(times)
                metrics[f"{parser}_max_parse_time"] = max(times)

        for parser, counts in self.reviews_count.items():
            if counts:
                metrics[f"{parser}_total_reviews"] = sum(counts)
                metrics[f"{parser}_avg_reviews_per_page"] = (
                    sum(counts) / len(counts)
                )

        for parser, counts in self.saved_count.items():
            if counts:
                metrics[f"{parser}_saved_reviews"] = sum(counts)
                metrics[f"{parser}_save_rate"] = (
                    sum(counts) / sum(self.reviews_count[parser])
                )

        return metrics
