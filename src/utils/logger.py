import json
import logging
import os
from pathlib import Path
from typing import Optional

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
DEFAULT_LEVEL = logging.INFO
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


class JSONFormatter(logging.Formatter):
    """Format log records as JSON strings."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - simple json serialization
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


def get_logger(
    name: str,
    level: int = DEFAULT_LEVEL,
    log_file: Optional[str] = None,
    json_format: Optional[bool] = None,
) -> logging.Logger:
    """Return a configured logger with console and file handlers."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    if json_format is None:
        fmt = os.getenv("LOG_FORMAT", "").lower()
        if fmt:
            json_format = fmt == "json"
        else:
            json_format = os.getenv("ENV", "").lower() == "production"

    logger.setLevel(level)
    formatter: logging.Formatter = (
        JSONFormatter() if json_format else logging.Formatter(LOG_FORMAT)
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    suffix = "json" if json_format else "log"
    file_name = log_file or f"{name}.{suffix}"
    file_handler = logging.FileHandler(LOG_DIR / file_name, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
