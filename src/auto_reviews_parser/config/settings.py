"""Application settings module."""

from dataclasses import dataclass


@dataclass
class Settings:
    """Simple application settings.

    Currently it only exposes the path to the SQLite database. More
    configuration options can be added later without affecting the
    existing code.
    """

    db_path: str = "auto_reviews.db"
    redis_url: str = "redis://localhost:6379/0"


__all__ = ["Settings"]

