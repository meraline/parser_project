#!/usr/bin/env python
"""
run_debug.py — запускает короткую сессию парсинга и юнит‑тесты
с подробным логированием для диагностики на реальных данных.
"""
import logging
import os
import pytest
from auto_reviews_parser.services.parser_service import ParserService


LOG_FILE = "parser_debug.log"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def run_parser() -> None:
    max_sources = int(os.getenv("MAX_SOURCES", "5"))
    session_hours = int(os.getenv("SESSION_HOURS", "2"))
    service = ParserService()
    logging.info("Starting parsing session: %s sources", max_sources)
    service.parser.run_parsing_session(
        max_sources=max_sources, session_duration_hours=session_hours
    )
    service.show_status()
    logging.info("Parsing session finished")


def run_tests() -> None:
    logging.info("Running pytest")
    pytest.main(["-q", "--log-cli-level=INFO"])


if __name__ == "__main__":
    configure_logging()
    run_parser()
    run_tests()
