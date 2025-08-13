"""Command line interface for the project.

This module configures the dependency injection container using the
``dependency_injector`` package and exposes a small CLI that delegates
work to :class:`ParserManager` from ``auto_reviews_parser``.
"""

from __future__ import annotations

import argparse

from dependency_injector import containers, providers

from auto_reviews_parser import AutoReviewsParser, ParserManager, ReviewsDatabase
from parsers import DromParser, Drive2Parser
from review_repository import ReviewRepository
from config.settings import Settings


class Container(containers.DeclarativeContainer):
    """Application dependency injection container."""

    settings = providers.Singleton(Settings)

    # Core components
    database = providers.Singleton(ReviewsDatabase, db_path=settings.provided.db_path)
    review_repository = providers.Singleton(ReviewRepository, db=database)

    # Parsers
    drom_parser = providers.Factory(DromParser, db=database)
    drive2_parser = providers.Factory(Drive2Parser, db=database)

    # Services
    auto_reviews_parser = providers.Factory(
        AutoReviewsParser,
        db=database,
        drom_parser=drom_parser,
        drive2_parser=drive2_parser,
    )
    parser_manager = providers.Factory(ParserManager, parser=auto_reviews_parser)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Парсер отзывов автомобилей")
    parser.add_argument(
        "command",
        choices=["init", "parse", "continuous", "status", "export"],
        help="Команда для выполнения",
    )
    parser.add_argument(
        "--sources",
        type=int,
        default=10,
        help="Количество источников за сессию (по умолчанию: 10)",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=4,
        help="Количество сессий в день для непрерывного режима (по умолчанию: 4)",
    )
    parser.add_argument(
        "--format",
        default="xlsx",
        choices=["xlsx", "json"],
        help="Формат экспорта данных (по умолчанию: xlsx)",
    )
    return parser


def main() -> None:
    arg_parser = create_parser()
    args = arg_parser.parse_args()

    container = Container()
    manager = container.parser_manager()

    if args.command == "init":
        print("🚀 Инициализация парсера...")
        manager.reset_queue()
        print("✅ Парсер готов к работе!")
    elif args.command == "parse":
        print("🎯 Запуск разовой сессии парсинга...")
        manager.parser.run_parsing_session(max_sources=args.sources)
    elif args.command == "continuous":
        print("🔄 Запуск непрерывного парсинга...")
        manager.parser.run_continuous_parsing(
            daily_sessions=args.sessions, session_sources=args.sources
        )
    elif args.command == "status":
        manager.show_status()
    elif args.command == "export":
        manager.export_data(output_format=args.format)


if __name__ == "__main__":
    main()

