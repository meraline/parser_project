import re
from typing import Any


def validate_non_empty_string(value: Any, field_name: str) -> str:
    """Ensure value is a non-empty string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def validate_positive_int(value: Any, field_name: str) -> int:
    """Ensure value is a positive integer (zero allowed)."""
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def validate_url(url: str) -> str:
    """Basic URL validation."""
    pattern = re.compile(r"^https?://")
    if not isinstance(url, str) or not pattern.match(url):
        raise ValueError("Invalid URL format")
    return url


def validate_not_none(value: Any, field_name: str) -> Any:
    """Ensure value is not None."""
    if value is None:
        raise ValueError(f"{field_name} cannot be None")
    return value
