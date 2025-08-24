from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    db_path: str = "auto_reviews.db"

    # Monitoring
    prometheus_port: int = 8000

    # Caching
    redis_url: str = "redis://localhost:6379/0"

    # Parallelism
    max_workers: int = 4

    # Delays (in seconds)
    min_delay: int = 5
    max_delay: int = 15
    error_delay: int = 30
    rate_limit_delay: int = 300

    # Limits
    max_retries: int = 3
    pages_per_session: int = 50
    max_reviews_per_model: int = 1000

    # User agents for rotation
    user_agents: List[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def load_targets(path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Load brand/model targets from a YAML file."""
    path = path or Path(__file__).with_name("targets.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


settings = Settings()
TARGET_BRANDS = load_targets()
