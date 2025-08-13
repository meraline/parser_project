"""Utilities for exposing Prometheus metrics for parsers."""
from __future__ import annotations

from functools import wraps
from typing import Any, Callable, List

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Metrics definitions
reviews_parsed = Counter(
    "reviews_parsed_total",
    "Number of reviews successfully parsed",
    ["source"],
)

parse_duration = Histogram(
    "parse_duration_seconds",
    "Time spent parsing reviews",
    ["source"],
)

parse_errors = Counter(
    "parse_errors_total",
    "Number of parsing errors",
    ["source"],
)

active_parsers = Gauge(
    "active_parsers",
    "Currently active parser tasks",
    ["source"],
)

_METRICS_STARTED = False


def setup_metrics(port: int = 8000) -> None:
    """Start HTTP server for exposing metrics if not already running."""
    global _METRICS_STARTED
    if not _METRICS_STARTED:
        start_http_server(port)
        _METRICS_STARTED = True


def track_parsing(source: str) -> Callable[[Callable[..., List[Any]]], Callable[..., List[Any]]]:
    """Decorator for tracking parsing metrics for a given source."""

    def decorator(func: Callable[..., List[Any]]) -> Callable[..., List[Any]]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[Any]:
            active_parsers.labels(source=source).inc()
            try:
                with parse_duration.labels(source=source).time():
                    result = func(*args, **kwargs)
                if isinstance(result, list):
                    reviews_parsed.labels(source=source).inc(len(result))
                return result
            except Exception:
                parse_errors.labels(source=source).inc()
                raise
            finally:
                active_parsers.labels(source=source).dec()

        return wrapper

    return decorator
