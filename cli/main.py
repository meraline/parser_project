#!/usr/bin/env python3
"""CLI для парсера авто-обзоров."""

import os
import argparse
import sys
from typing import List, Optional

from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.parsers.drive2 import Drive2Parser
from auto_reviews_parser.services.parser_service import ParserService
from auto_reviews_parser.services.parallel_parser import ParallelParserService


def get_db_path() -> str:
    """Получить путь к файлу БД."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "..", "auto_reviews.db")


def init_parser() -> ParallelParserService:
    """Инициализация парсера."""
    db = Database(get_db_path())
    review_repo = ReviewRepository(db)

    drom_parser = DromParser()
    drive2_parser = Drive2Parser()

    parser_service = ParserService(review_repo)

    parsers = {"drom": drom_parser, "drive2": drive2_parser}

    parallel_parser = ParallelParserService(parsers=parsers)

    return parallel_parser


def create_argparser() -> argparse.ArgumentParser:
    """Создание парсера аргументов командной строки."""
    parser = argparse.ArgumentParser(description="Парсер авто-обзоров")

    # Подкоманды
    subparsers = parser.add_subparsers(dest="command")

    # Команда init
    subparsers.add_parser("init", help="Инициализация БД")

    # Команда parse
    parse_parser = subparsers.add_parser("parse", help="Запуск парсинга")
    parse_parser.add_argument(
        "--source", choices=["drom", "drive2"], help="Источник данных", required=True
    )
    parse_parser.add_argument("--brand", help="Марка авто")
    parse_parser.add_argument("--model", help="Модель авто")

    return parser


def run(argv: Optional[List[str]] = None) -> int:
    """Запуск CLI."""
    parser = create_argparser()
    args = parser.parse_args(argv)

    if args.command == "init":
        from cli.init_db import init_database

        return init_database(get_db_path())

    elif args.command == "parse":
        parser = init_parser()
        if args.brand and args.model:
            sources = [(args.brand, args.model, args.source)]
            parser.parse_multiple_sources(sources)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(run())
